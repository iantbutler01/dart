use crate::locking::OptimisticLockCouplingWriteGuard;
use crate::node::*;
use bincode::{Decode, Encode};
use crossbeam::atomic::AtomicCell;
use crossbeam::utils::Backoff;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::errors::Errors;
use crate::errors::OptimisticLockCouplingErrorType;

pub struct Dart<T: 'static + Encode + Decode> {
    pub(crate) root: AtomicCell<NodePtr>,
    cache: Arc<NodeCache<T>>,
    pub generation: AtomicUsize,
    pub count: AtomicUsize,
    pub levels: AtomicUsize,
    pub ops: AtomicUsize,
}

impl<T: Encode + Decode> Dart<T>
where
    T: Clone,
{
    pub fn new(
        max_lru_cap: u64,
        num_lru_segments: usize,
        disk_path: String,
        wal_path: String,
    ) -> Self {
        let tree = Self {
            root: AtomicCell::new(NodePtr::sentinel_node()),
            cache: Arc::new(NodeCache::<T>::new(
                max_lru_cap,
                num_lru_segments,
                disk_path,
                wal_path,
            )),
            generation: AtomicUsize::new(0),
            count: AtomicUsize::new(0),
            levels: AtomicUsize::new(1),
            ops: AtomicUsize::new(0),
        };

        let root = tree.new_node(&[], NodeSize::TwoFiftySix);

        tree.root.swap(root);

        tree
    }

    pub fn insert(&self, key: &[u8], value: T) -> Result<(), Errors> {
        let backoff = Backoff::new();
        loop {
            match self.insert_internal(key, value.clone()) {
                Ok(_) => return Ok(()),
                Err(insert_error) => match insert_error {
                    Errors::LockingError(e) => match e {
                        OptimisticLockCouplingErrorType::Outdated
                        | OptimisticLockCouplingErrorType::Poisoned => return Err(insert_error),
                        _ => {
                            backoff.spin();
                            continue;
                        }
                    },
                    e => return Err(e),
                },
            }
        }
    }

    pub fn remove(&self, key: &[u8]) -> Result<(), Errors> {
        let backoff = Backoff::new();
        loop {
            match self.remove_internal(key) {
                Ok(_) => return Ok(()),
                Err(remove_error) => match remove_error {
                    Errors::LockingError(e) => match e {
                        OptimisticLockCouplingErrorType::Outdated
                        | OptimisticLockCouplingErrorType::Poisoned => return Err(remove_error),
                        _ => {
                            backoff.spin();
                            continue;
                        }
                    },
                    e => return Err(e),
                },
            }
        }
    }

    fn remove_internal(&self, key: &[u8]) -> Result<T, Errors> {
        let root_ptr = self.root.load();
        let root_wrapper = self.cache.get(&(root_ptr.id as u64));
        let root = root_wrapper.data.read()?;

        let mut node_ptr = root.get_child(key[0]);

        if node_ptr == NodePtr::sentinel_node() {
            return Err(Errors::NonExistantError(
                "Key does not exist in tree".to_string(),
            ));
        }

        let mut level = 0usize;
        let mut next_node: NodePtr;
        let mut parent: Option<NodeWrapper<T>> = Some(root_wrapper);

        loop {
            let node_wrapper = self.cache.get(&(node_ptr.id as u64));
            let read_guard = node_wrapper.data.read()?;
            let mut node = read_guard;

            if node.is_leaf() {
                if parent.is_some() {
                    let mut parent_mut = parent.as_ref().unwrap().data.write()?;

                    if key == node.leaf_data_ref().key {
                        parent_mut.remove(key[level]);
                    }

                    if parent_mut.internal_data_ref().prefix != [] && parent_mut.should_shrink() {
                        parent_mut.shrink();
                    }
                }

                self.remove_node(node_ptr);

                return Ok(node.leaf_data_ref().value.clone());
            }

            // Implement insertion starting with finding the nextNode if the prefix matches the current node for the given level.
            let overlap = self.overlapping_prefix_len(&node, key, level);
            if overlap != node.internal_data_ref().prefix.len() {
                return Err(Errors::NonExistantError(
                    "Unable to find key in tree.".to_string(),
                ));
            }

            level = level + overlap;

            node.try_sync()?;
            node = node_wrapper.data.read()?;
            if level >= key.len() && node.internal_data_ref().terminal != NodePtr::sentinel_node() {
                let terminal_wrapper = self
                    .cache
                    .get(&(node.internal_data_ref().terminal.id as u64));

                let terminal = terminal_wrapper.data.read()?;

                let mut node_mut = node_wrapper.data.write()?;
                node_mut.internal_data_mut().terminal = NodePtr::sentinel_node();
                self.cache.remove(terminal.id as u64);

                return Ok(terminal.leaf_data_ref().value.clone());
            }

            next_node = node.get_child(key[level]);
            if next_node == NodePtr::sentinel_node() {
                if key == node.internal_data_ref().prefix
                    && node.internal_data_ref().terminal != NodePtr::sentinel_node()
                {
                    let terminal_ptr = node.internal_data_ref().terminal;

                    let terminal_wrapper = self.cache.get(&(terminal_ptr.id as u64));
                    let terminal = terminal_wrapper
                        .data
                        .read()
                        .expect("Expected to read terminal.");
                    let value = terminal.leaf_data_ref().value.clone();

                    self.cache.remove(terminal_ptr.id as u64);

                    return Ok(value);
                } else {
                    return Err(Errors::NonExistantError(
                        "Unable to find key in tree.".to_string(),
                    ));
                }
            }

            node_ptr = next_node;
            parent = Some(node_wrapper);
        }
    }

    fn insert_internal(&self, key: &[u8], value: T) -> Result<(), Errors> {
        // println!("TREE COUNT: {:?}", self.count.load(Ordering::SeqCst));
        // println!("INSERT KEY: {:?}", key);

        let root_ptr = self.root.load();
        let root_wrapper = self.cache.get(&(root_ptr.id as u64));
        let root = root_wrapper.data.read()?;

        let mut node_ptr = root.get_child(key[0]);

        if node_ptr == NodePtr::sentinel_node() {
            let mut root = root_wrapper.data.write()?;
            let ptr = self.new_leaf(key, value);
            root.insert(key[0], ptr);

            return Ok(());
        }

        let mut parent = Some(root_wrapper);
        let mut level = 0usize;
        let mut next_node: NodePtr;

        loop {
            let node_wrapper = self.cache.get(&(node_ptr.id as u64));
            let read_guard = node_wrapper.data.read()?;
            let mut node = read_guard;

            if node.is_leaf() {
                if parent.is_some() {
                    parent.as_ref().unwrap().data.write()?;
                }

                let mut mut_node = node_wrapper.data.write()?;

                if key == node.leaf_data_ref().key {
                    mut_node.leaf_data_mut().value = value;
                    return Ok(());
                }

                let key2 = mut_node.leaf_data_ref().key.clone();
                let mut i = level;
                let mut new_prefix = Vec::<u8>::new();

                // The leaf's key is the entire prefix of key being inserted.
                if level >= key2.len() {
                    new_prefix = vec![*key2.last().unwrap()];
                } else {
                    loop {
                        if i >= key2.len() || key[i] != key2[i] {
                            break;
                        }

                        new_prefix.push(key[i]);

                        i += 1;
                    }
                    level = level + new_prefix.len();
                }

                let new_node_ptr = self.new_node(new_prefix.as_slice(), NodeSize::Four);
                let new_node = self.fetch_node_wrapper(new_node_ptr);
                let mut new_mut_node = new_node.data.write()?;

                let new_leaf = self.new_leaf(key, value);
                new_mut_node.insert(key[level], new_leaf);

                // Handle cases where there is a leaf for a key that is also a prefix. e.g. Key "a" and Key "ap", in this case "a" is both a prefix and a valid terminal.
                if level == key2.len() {
                    new_mut_node.internal_data_mut().terminal = new_node_ptr;
                } else {
                    // Insert a pointer to its self, this is because we are about to swap the new node's ptr in the NodeCache and WAL with the parent, i.e. the current node.
                    // This means ultimately it will point to the correct node after the swap.
                    new_mut_node.insert(key2[level], new_node_ptr);
                }

                self.swap_node(
                    mut_node,
                    node_wrapper.clone(),
                    new_mut_node,
                    new_node.clone(),
                );

                self.levels.fetch_max(level, Ordering::SeqCst);
                return Ok(());
            }

            let overlap = self.overlapping_prefix_len(&node, key, level);

            if overlap != node.internal_data_ref().prefix.len() {
                let _mut_node = node_wrapper.data.write()?;
                let new_node = self.new_node(&key[0..overlap], NodeSize::Four);
                let new_node_wrapper = self.fetch_node_wrapper(new_node);
                let mut new_mut_node = new_node_wrapper.data.write()?;
                let new_leaf = self.new_leaf(key, value);
                new_mut_node.insert(key[overlap + level], new_leaf);
                new_mut_node.insert(node.internal_data_ref().prefix[overlap], node_ptr);
                return Ok(());
            }

            node.try_sync()?;
            node = node_wrapper.data.read()?;
            level = level + overlap;

            if level >= key.len() && node.internal_data_ref().terminal == NodePtr::sentinel_node() {
                let mut node_mut = node_wrapper.data.write()?;
                let new_leaf_node = self.new_leaf(key, value);

                node_mut.internal_data_mut().terminal = new_leaf_node;

                return Ok(());
            }

            next_node = node.get_child(key[level]);

            if next_node == NodePtr::sentinel_node() {
                if node.is_full() {
                    if parent.is_some() {
                        parent.as_ref().unwrap().data.write()?;
                    }
                    let mut mut_node = node_wrapper.data.write()?;
                    mut_node.expand();
                }
                let new_leaf = self.new_leaf(key.into(), value);

                let mut mut_node = node_wrapper.data.write()?;
                mut_node.insert(key[level], new_leaf);

                return Ok(());
            }

            level = level;
            node_ptr = next_node;
            parent = Some(node_wrapper);
        }
    }

    fn swap_node(
        &self,
        mut parent: OptimisticLockCouplingWriteGuard<ArtNode<T>>,
        parent_wrapper: NodeWrapper<T>,
        mut child: OptimisticLockCouplingWriteGuard<ArtNode<T>>,
        child_wrapper: NodeWrapper<T>,
    ) {
        let pid = parent.id;
        let cid = child.id;

        child.id = pid;
        parent.id = cid;

        self.cache.insert(pid as u64, child_wrapper);
        self.cache.insert(cid as u64, parent_wrapper);
    }

    fn new_leaf(&self, key: &[u8], value: T) -> NodePtr {
        let node = ArtNode::<T>::new_leaf(
            key.into(),
            value,
            self.count.fetch_add(1, Ordering::SeqCst),
            self.count.load(Ordering::SeqCst),
        );

        let ptr = NodePtr {
            id: node.id,
            generation: node.generation,
        };

        self.cache
            .as_ref()
            .insert(node.id as u64, NodeWrapper::new(node));

        ptr
    }
    fn new_node(&self, prefix: &[u8], size: NodeSize) -> NodePtr {
        let node = ArtNode::<T>::new(
            prefix,
            size,
            self.count.fetch_add(1, Ordering::SeqCst),
            self.count.load(Ordering::SeqCst),
        );

        let ptr = NodePtr {
            id: node.id,
            generation: node.generation,
        };
        self.cache
            .as_ref()
            .insert(node.id as u64, NodeWrapper::new(node));

        ptr
    }

    fn remove_node(&self, ptr: NodePtr) {
        self.cache.as_ref().remove(ptr.id as u64);
    }

    pub(crate) fn fetch_node_wrapper(&self, ptr: NodePtr) -> NodeWrapper<T> {
        self.cache.as_ref().get(&(ptr.id as u64))
    }

    pub fn get(&self, key: &[u8]) -> Result<T, Errors> {
        self.get_or_update_driver(key, None)
    }

    pub fn update(
        &self,
        key: &[u8],
        callback: &mut Box<
            dyn FnMut(OptimisticLockCouplingWriteGuard<ArtNode<T>>) -> Result<T, Errors>,
        >,
    ) -> Result<T, Errors> {
        self.get_or_update_driver(key, Some(callback))
    }

    fn get_or_update_driver(
        &self,
        key: &[u8],
        mut callback: Option<
            &mut Box<dyn FnMut(OptimisticLockCouplingWriteGuard<ArtNode<T>>) -> Result<T, Errors>>,
        >,
    ) -> Result<T, Errors> {
        let backoff = Backoff::new();
        loop {
            match self.get_or_update_internal(key, &mut callback) {
                Ok(value) => return Ok(value),
                Err(get_error) => match get_error {
                    Errors::LockingError(e) => match e {
                        OptimisticLockCouplingErrorType::Outdated
                        | OptimisticLockCouplingErrorType::Poisoned => return Err(get_error),
                        _ => {
                            backoff.spin();
                            continue;
                        }
                    },
                    e => return Err(e),
                },
            }
        }
    }

    fn get_or_update_internal(
        &self,
        key: &[u8],
        callback: &mut Option<
            &mut Box<dyn FnMut(OptimisticLockCouplingWriteGuard<ArtNode<T>>) -> Result<T, Errors>>,
        >,
    ) -> Result<T, Errors> {
        let mut node_ptr = self.root.load();

        let root_ptr = self.root.load();
        let root_wrapper = self.cache.get(&(root_ptr.id as u64));
        let root = root_wrapper.data.read()?;

        let mut node_ptr = root.get_child(key[0]);

        if node_ptr == NodePtr::sentinel_node() {
            return Err(Errors::NonExistantError(
                "Key does not exist in tree!".to_string(),
            ));
        }

        let mut level = 0usize;
        let mut next_node: NodePtr;

        loop {
            let node_wrapper = self.cache.get(&(node_ptr.id as u64));
            let read_guard = node_wrapper.data.read()?;
            let mut node = read_guard;

            if node.is_leaf() {
                if key == node.leaf_data_ref().key {
                    return match callback {
                        Some(cb) => {
                            let write_lock = node_wrapper
                                .data
                                .write()
                                .expect("Expected to acquire write lock.");
                            cb(write_lock)
                        }
                        None => Ok(node.leaf_data_ref().value.clone()),
                    };
                } else {
                    return Err(Errors::NonExistantError(
                        "Key not found in tree.".to_string(),
                    ));
                }
            }

            // Implement insertion starting with finding the nextNode if the prefix matches the current node for the given level.
            let overlap = self.overlapping_prefix_len(&node, key, level);
            if overlap != node.internal_data_ref().prefix.len() {
                return Err(Errors::NonExistantError(
                    "Unable to find key in tree.".to_string(),
                ));
            }
            level = level + node.internal_data_ref().prefix.len();

            node.try_sync()?;
            node = node_wrapper.data.read()?;

            next_node = node.get_child(key[level]);
            if next_node == NodePtr::sentinel_node() {
                if key == node.internal_data_ref().prefix
                    && node.internal_data_ref().terminal != NodePtr::sentinel_node()
                {
                    let data_wrapper = self
                        .cache
                        .get(&(node.internal_data_ref().terminal.id as u64));
                    let data_node = data_wrapper.data.read().unwrap();

                    return match callback {
                        Some(cb) => {
                            let write_lock = data_wrapper
                                .data
                                .write()
                                .expect("Expected to acquire write lock.");
                            cb(write_lock)
                        }
                        None => Ok(data_node.leaf_data_ref().value.clone()),
                    };
                } else {
                    return Err(Errors::NonExistantError(
                        "Unable to find key in tree.".to_string(),
                    ));
                }
            }

            node_ptr = next_node;
        }
    }

    pub(crate) fn overlapping_prefix_len(
        &self,
        node: &ArtNode<T>,
        key: &[u8],
        level: usize,
    ) -> usize {
        let mut count = 0;
        for i in 0..core::cmp::min(node.internal_data_ref().prefix.len(), key.len()) {
            if node.internal_data_ref().prefix[i] == key[i + level] {
                count += 1;
            } else {
                break;
            }
        }

        count
    }
}

pub struct GDart<T: 'static + Encode + Decode>
where
    T: Send + Clone,
{
    current_generation: Dart<T>,
    past_generations: Vec<Dart<T>>,
}

#[cfg(test)]
mod tests {
    use rand::Rng;
    use std::sync::atomic::Ordering;
    use std::thread::current;

    use crate::errors::Errors;
    use crate::tree::Dart;
    use crate::utils::in_temp_dir;

    #[test]
    fn insert_at_root() {
        in_temp_dir!(|path: String| {
            let tree = Dart::<u64>::new(100, 1, path.clone() + "/tree", path.clone() + "/wal");

            let key = "abc".as_bytes();
            let value = 64;

            let res = tree
                .insert(key, value)
                .expect("Expected no error on insert.");
            assert_eq!(res, ());
        })
    }

    #[test]
    fn insert_at_child() {
        in_temp_dir!(|path: String| {
            let tree = Dart::<u64>::new(100, 1, path.clone() + "/tree", path + "/wal");

            let key = "aba".as_bytes();
            let value = 64;

            let res = tree
                .insert(key, value)
                .expect("Expected no error on insert.");
            assert_eq!(res, ());

            let key = "abb".as_bytes();
            let value = 65;
            tree.insert(key, value)
                .expect("Expected no error on resize to 4.");
            assert_eq!(res, ());
            assert_eq!(tree.levels.load(Ordering::Relaxed), 2);
        })
    }

    #[test]
    fn insert_non_primitive() {
        use std::collections::HashMap;

        in_temp_dir!(|path: String| {
            let tree =
                Dart::<HashMap<String, String>>::new(100, 1, path.clone() + "/tree", path + "/wal");

            let key = "aba".as_bytes();
            let value = HashMap::<String, String>::new();

            let res = tree
                .insert(key, value)
                .expect("Expected no error on insert.");
            assert_eq!(res, ());

            let key = "abb".as_bytes();
            let value = HashMap::<String, String>::new();
            tree.insert(key, value)
                .expect("Expected no error on resize to 4.");
            assert_eq!(res, ());
            assert_eq!(tree.levels.load(Ordering::Relaxed), 2);
        })
    }

    #[test]
    fn insert_at_5th() {
        in_temp_dir!(|path: String| {
            let tree = Dart::<u64>::new(100, 1, path.clone() + "/tree", path + "/wal");

            let key = "aba".as_bytes();
            let value = 64;

            let res = tree
                .insert(key, value)
                .expect("Expected no error on insert.");
            assert_eq!(res, ());

            for i in 1..5 {
                let part = (i as u8) as char;
                let fmt = format!("ab{}", part);
                let key = fmt.as_bytes();
                let value = i as u64;

                let res = tree
                    .insert(key, value)
                    .expect(&format!("Expected no error on {i}th insert."));
                assert_eq!(res, ());
            }
            assert_eq!(tree.levels.load(Ordering::Relaxed), 2);
        })
    }

    #[test]
    fn remove_at_5th() {
        in_temp_dir!(|path: String| {
            let tree = Dart::<u64>::new(100, 1, path.clone() + "/tree", path + "/wal");

            let mut key: &[u8] = &[];
            let mut fmt: String;

            for i in 0..5 {
                let part = (i as u8) as char;
                fmt = format!("ab{}", part);
                key = fmt.as_bytes().clone();
                let value = i as u64;

                let res = tree
                    .insert(key, value)
                    .expect(&format!("Expected no error on {i}th insert."));
                assert_eq!(res, ());
            }

            tree.remove(key).expect("Expected this to not explode.");
        })
    }

    #[test]
    fn insert_at_17th() {
        in_temp_dir!(|path: String| {
            let tree = Dart::<u64>::new(100, 1, path.clone() + "/tree", path + "/wal");

            for i in 0..=17 {
                let part = (i as u8) as char;
                let fmt = format!("ab{}", part);
                let key = fmt.as_bytes();
                let value = i as u64;

                let res = tree
                    .insert(key, value)
                    .expect(&format!("Expected no error on {i}th insert."));
                assert_eq!(res, ());
            }
            assert_eq!(tree.levels.load(Ordering::Relaxed), 2);
        })
    }

    #[test]
    fn remove_at_17th() {
        in_temp_dir!(|path: String| {
            let tree = Dart::<u64>::new(100, 1, path.clone() + "/tree", path + "/wal");

            let mut key: &[u8] = &[];
            let mut fmt: String;

            for i in 0..17 {
                let part = (i as u8) as char;
                fmt = format!("ab{}", part);
                key = fmt.as_bytes().clone();
                let value = i as u64;

                let res = tree
                    .insert(key, value)
                    .expect(&format!("Expected no error on {i}th insert."));
                assert_eq!(res, ());
            }

            tree.remove(key).expect("Expected this to not explode.");
        })
    }

    #[test]
    fn insert_at_49th() {
        in_temp_dir!(|path: String| {
            let tree = Dart::<u64>::new(100, 1, path.clone() + "/tree", path + "/wal");

            let key = "aba".as_bytes();
            let value = 64;

            let res = tree
                .insert(key, value)
                .expect("Expected no error on insert.");
            assert_eq!(res, ());

            for i in 0..49 {
                let part = (i as u8) as char;
                let fmt = format!("ab{}", part);
                let key = fmt.as_bytes();
                let value = i as u64;

                let res = tree
                    .insert(key, value)
                    .expect(&format!("Expected no error on {i}th insert."));
                assert_eq!(res, ());
            }

            assert_eq!(tree.levels.load(Ordering::Relaxed), 2);
        })
    }

    #[test]
    fn remove_at_49th() {
        in_temp_dir!(|path: String| {
            let tree = Dart::<u64>::new(100, 1, path.clone() + "/tree", path + "/wal");

            let mut key: &[u8] = &[];
            let mut fmt: String;

            for i in 0..49 {
                let part = (i as u8) as char;
                fmt = format!("ab{}", part);
                key = fmt.as_bytes().clone();
                let value = i as u64;

                let res = tree
                    .insert(key, value)
                    .expect(&format!("Expected no error on {i}th insert."));
                assert_eq!(res, ());
            }

            tree.remove(key).expect("Expected this to not explode.");

            assert_eq!(tree.levels.load(Ordering::Relaxed), 2);
        })
    }

    #[test]
    fn insert_multi_level() {
        in_temp_dir!(|path: String| {
            let tree = Dart::<u64>::new(100, 1, path.clone() + "/tree", path + "/wal");

            let key = vec![1, 2, 3];
            let value = 64;

            let res = tree
                .insert(&key, value)
                .expect("Expected no error on insert.");
            assert_eq!(res, ());

            let key2 = vec![1, 2, 4];
            let value = 65;
            let res = tree.insert(&key2, value).expect("Expected no error.");
            assert_eq!(res, ());

            for i in 0..4 {
                let mut key = key.clone();
                key.push(i);
                let value = i as u64;

                let res = tree
                    .insert(&key, value)
                    .expect(&format!("Expected no error on {i}th insert."));
                assert_eq!(res, ());
            }

            for i in 0..4 {
                let mut key = key2.clone();
                key.push(i);
                let value = i as u64;

                let res = tree
                    .insert(&key, value)
                    .expect(&format!("Expected no error on {i}th insert."));
                assert_eq!(res, ());
            }
            assert_eq!(tree.levels.load(Ordering::Relaxed), 3);
        })
    }

    #[test]
    fn insert_words() {
        in_temp_dir!(|path: String| {
            let tree = Dart::<u64>::new(100, 1, path.clone() + "/tree", path + "/wal");

            tree.insert("apple".as_bytes(), 1).unwrap();
            tree.insert("appetizer".as_bytes(), 2).unwrap();
            tree.insert("apply".as_bytes(), 3).unwrap();
            tree.insert("apt".as_bytes(), 4).unwrap();
            tree.insert("arrange".as_bytes(), 5).unwrap();
            tree.insert("art".as_bytes(), 5).unwrap();
            tree.insert("archaic".as_bytes(), 5).unwrap();
            tree.insert("arthropod".as_bytes(), 5).unwrap();
            tree.insert("arc".as_bytes(), 5).unwrap();
            tree.insert("bar".as_bytes(), 5).unwrap();
            tree.insert("bark".as_bytes(), 5).unwrap();
            tree.insert("bet".as_bytes(), 5).unwrap();
            assert_eq!(tree.levels.load(Ordering::Relaxed), 4);
        })
    }

    #[test]
    fn insert_deep() {
        in_temp_dir!(|path: String| {
            let tree = Dart::<u64>::new(100, 1, path.clone() + "/tree", path + "/wal");

            let mut prev_key = Vec::<u8>::new();

            for i in 0..=255 {
                prev_key.push(i);
                let value = i as u64;

                let res = tree
                    .insert(&prev_key, value)
                    .expect(&format!("Expected no error on {i}th insert."));
                assert_eq!(res, ());
            }
        })
    }

    #[test]
    fn insert_deep_and_wide() {
        in_temp_dir!(|path: String| {
            let tree = Dart::<u64>::new(100, 1, path.clone() + "/tree", path + "/wal");

            for i in 0..=255 {
                let level_vec: Vec<u8> = (0..i).collect();

                for i in 0..=255 {
                    let mut current_vec = level_vec.clone();
                    current_vec.push(i);
                    let value = i as u64;

                    let res = tree
                        .insert(&current_vec, value)
                        .expect(&format!("Expected no error on {i}th insert."));
                    assert_eq!(res, ());
                }
            }
        })
    }

    #[test]
    fn get_element_when_exists() {
        in_temp_dir!(|path: String| {
            let tree = Dart::<u64>::new(100, 1, path.clone() + "/tree", path + "/wal");

            for i in 0..10 {
                let level_vec: Vec<u8> = (0..i).collect();

                for j in 0..10 {
                    let mut key = level_vec.clone();
                    key.push(j);
                    let value = j as u64;

                    let res = tree
                        .insert(&key, value)
                        .expect(&format!("Expected no error on {j}th insert."));
                    assert_eq!(res, ());
                }
            }

            let res = tree.get(&[0, 1, 2, 3, 9]).expect("Expected result.");

            assert_eq!(res, 9)
        })
    }

    #[test]
    fn get_element_when_not_exists() {
        in_temp_dir!(|path: String| {
            let tree = Dart::<u64>::new(100, 1, path.clone() + "/tree", path + "/wal");

            for i in 0..10 {
                let level_vec: Vec<u8> = (0..i).collect();

                for j in 0..10 {
                    let mut key = level_vec.clone();
                    key.push(j);
                    let value = j as u64;

                    let res = tree
                        .insert(&key, value)
                        .expect(&format!("Expected no error on {j}th insert."));
                    assert_eq!(res, ());
                }
            }

            let res = tree.get(&[0, 1, 2, 3, 11]);

            assert_eq!(res.is_err(), true);

            match res {
                Err(e) => match e {
                    Errors::NonExistantError(_) => true,
                    _ => panic!("Expected a NonExistantError"),
                },
                _ => panic!("Expected key not to be found!"),
            }
        })
    }

    #[test]
    fn insert_terminal_remove_terminal_insert_terminal() {
        in_temp_dir!(|path: String| {
            let tree = Dart::<u64>::new(100, 1, path.clone() + "/tree", path.clone() + "/wal");

            for i in 0..=10 {
                let level_vec: Vec<u8> = (0..i).collect();

                for i in 0..=10 {
                    let mut current_vec = level_vec.clone();
                    current_vec.push(i);
                    let value = i as u64;

                    let res = tree
                        .insert(&current_vec, value)
                        .expect(&format!("Expected no error on {i}th insert."));
                    assert_eq!(res, ());
                }
            }

            let test_key = &[0, 1, 2, 3, 4, 5];

            tree.remove(test_key)
                .expect("Expected this to remove correctly.");

            tree.insert(test_key, 64)
                .expect("Expected this to insert correctly.");
        })
    }

    #[test]
    fn insert_many_random_removal() {
        in_temp_dir!(|path: String| {
            let tree = Dart::<u64>::new(100, 1, path.clone() + "/tree", path.clone() + "/wal");

            for i in 0..=255 {
                let level_vec: Vec<u8> = (0..i).collect();

                for i in 0..=255 {
                    let mut current_vec = level_vec.clone();
                    current_vec.push(i);
                    let value = i as u64;

                    let res = tree
                        .insert(&current_vec, value)
                        .expect(&format!("Expected no error on {i}th insert."));
                    assert_eq!(res, ());
                }
            }

            for i in 0..=255 {
                let level_vec: Vec<u8> = (0..i).collect();

                for i in 0..=255 {
                    let mut rng = rand::thread_rng();
                    let remove: f64 = rng.gen();

                    if remove > 0.9 {
                        let mut current_vec = level_vec.clone();
                        current_vec.push(i);

                        let res = tree
                            .remove(&current_vec)
                            .expect(&format!("Expected no error on {i}th insert."));
                        assert_eq!(res, ());
                    }
                }
            }
        })
    }
}
