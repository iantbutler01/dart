use crate::errors::Errors;
use crate::errors::OptimisticLockCouplingErrorType;
use crate::locking::OptimisticLockCouplingWriteGuard;
use crate::node::*;
use bincode::{Decode, Encode};
use crossbeam::atomic::AtomicCell;
use crossbeam::utils::Backoff;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;

pub struct Dart<T: 'static + Encode + Decode> {
    pub(crate) root: AtomicCell<NodePtr>,
    pub(crate) cache: Arc<Mutex<NodeCache<T>>>,
    pub generation: AtomicU64,
    pub count: AtomicU64,
    pub levels: AtomicUsize,
    pub ops: AtomicUsize,
}

unsafe impl<T: 'static + Encode + Decode> Sync for Dart<T> {}
unsafe impl<T: 'static + Encode + Decode> Send for Dart<T> {}

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
            cache: Arc::new(Mutex::new(NodeCache::<T>::new(
                max_lru_cap,
                num_lru_segments,
                disk_path,
                wal_path,
            ))),
            generation: AtomicU64::new(0),
            count: AtomicU64::new(1),
            levels: AtomicUsize::new(0),
            ops: AtomicUsize::new(0),
        };

        let root = tree.new_node(&[], NodeSize::TwoFiftySix);

        tree.root.swap(root);

        tree
    }

    pub fn insert(&self, key: &[u8], value: T) -> Result<Option<T>, Errors> {
        let op = Op::new(OpType::Upsert, key.into(), Some(value));

        self.execute_with_retry(op)
    }

    fn execute_with_retry(&self, op: Op<T>) -> Result<Option<T>, Errors> {
        let backoff = Backoff::new();
        loop {
            match self.execute(op.clone()) {
                Ok(res) => {
                    return Ok(res);
                }
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
        let root_wrapper = self.cache.lock().unwrap().get(&(root_ptr.id as u64));
        let root = root_wrapper.data.read()?;

        let mut node_ptr = root.get_child(key[0]);

        if node_ptr == NodePtr::sentinel_node() {
            return Err(Errors::NonExistantError);
        }

        let mut level = 0usize;
        let mut next_node: NodePtr;
        let mut parent: NodeWrapper<T> = root_wrapper;

        loop {
            let node_wrapper = self.cache.lock().unwrap().get(&(node_ptr.id as u64));
            let read_guard = node_wrapper.data.read()?;
            let mut node = read_guard;

            if node.is_leaf() {
                self.remove_node(node_ptr);

                let mut parent_mut = parent.data.write()?;

                if key == node.leaf_data_ref().key {
                    parent_mut.remove(key[level]);
                }

                if parent_mut.internal_data_ref().prefix != [] && parent_mut.should_shrink() {
                    parent_mut.shrink();
                }

                return Ok(node.leaf_data_ref().value.clone());
            }

            // Implement insertion starting with finding the nextNode if the prefix matches the current node for the given level.
            let overlap = self.overlapping_prefix_len(&node, key, level);

            if overlap != node.internal_data_ref().prefix.len() {
                return Err(Errors::NonExistantError);
            }

            level = level + overlap;

            node.try_sync()?;
            node = node_wrapper.data.read()?;

            if level >= key.len() {
                if node.internal_data_ref().terminal != NodePtr::sentinel_node() {
                    let cache = self.cache.lock().unwrap();

                    let terminal_wrapper =
                        cache.get(&(node.internal_data_ref().terminal.id as u64));

                    let terminal = terminal_wrapper.data.read()?;

                    let mut node_mut = node_wrapper.data.write()?;
                    node_mut.internal_data_mut().terminal = NodePtr::sentinel_node();
                    cache.remove(terminal.id as u64);

                    return Ok(terminal.leaf_data_ref().value.clone());
                }
            }

            node.try_sync()?;
            node = node_wrapper.data.read()?;
            next_node = node.get_child(key[level]);
            if next_node == NodePtr::sentinel_node() {
                if key == node.internal_data_ref().prefix
                    && node.internal_data_ref().terminal != NodePtr::sentinel_node()
                {
                    let terminal_ptr = node.internal_data_ref().terminal;
                    let mut mut_node = node_wrapper.data.write()?;
                    mut_node.internal_data_mut().terminal = NodePtr::sentinel_node();

                    let cache = self.cache.lock().unwrap();

                    let terminal_wrapper = cache.get(&(terminal_ptr.id as u64));
                    let terminal = terminal_wrapper
                        .data
                        .read()
                        .expect("Expected to read terminal.");
                    let value = terminal.leaf_data_ref().value.clone();

                    cache.remove(terminal_ptr.id as u64);

                    return Ok(value);
                } else {
                    return Err(Errors::NonExistantError);
                }
            }

            node_ptr = next_node;
            parent = node_wrapper;
        }
    }

    fn execute(&self, op: Op<T>) -> Result<Option<T>, Errors> {
        let key = op.key.as_slice();
        let root_ptr = self.root.load();
        let root_wrapper = self.cache.lock().unwrap().get(&(root_ptr.id as u64));
        let root = root_wrapper.data.read()?;
        let mut node_ptr = root.get_child(key[0]);

        if node_ptr == NodePtr::sentinel_node() {
            root.try_sync()?;

            match op.optype {
                OpType::Upsert => {
                    let mut root = root_wrapper.data.write()?;
                    let ptr = self.new_leaf(key, op.value.unwrap());

                    root.insert(key[0], ptr);
                    self.cache
                        .lock()
                        .unwrap()
                        .write_disk(root_ptr.id as u64, Some(root.clone()))
                        .expect("Expected to write insert to root to disk.");
                    return Ok(None);
                }
                _ => {
                    println!("THIS SHOULDN'T APPEAR 0");
                    return Err(Errors::NonExistantError);
                }
            }
        }

        root.try_sync()?;

        let mut parent = root_wrapper;
        let mut level = 0usize;
        let mut next_node: NodePtr;
        let mut parent_read = parent.data.read()?;

        loop {
            let node_wrapper = self.cache.lock().unwrap().get(&(node_ptr.id as u64));
            let mut node = node_wrapper.data.read()?;

            // println!("Checking leaf.");
            if node.is_leaf() {
                match op.optype {
                    OpType::Upsert => {
                        node.try_sync()?;
                        parent_read.try_sync()?;
                        // println!("After sync leaf");
                        let mut mut_node = node_wrapper.data.write()?;

                        if key == mut_node.leaf_data_ref().key {
                            //TODO(IAN) Write this to disk.
                            let old_value = mut_node.leaf_data_ref().value.clone();
                            mut_node.leaf_data_mut().value = op.value.unwrap().clone();
                            return Ok(Some(old_value));
                        }

                        let key2 = mut_node.leaf_data_ref().key.clone();
                        let mut i = level;
                        let mut new_prefix = Vec::<u8>::new();

                        loop {
                            if i >= key2.len() || i >= key.len() || key[i] != key2[i] {
                                break;
                            }

                            new_prefix.push(key[i]);

                            i += 1;
                        }
                        level = level + new_prefix.len();
                        if new_prefix == [] {
                            return Err(Errors::UnrecoverableError("The new prefix for a leaf expansion was empty, tree is degenerate.".to_string()));
                        }

                        let new_node_ptr = self.new_node(new_prefix.as_slice(), NodeSize::Four);
                        let new_leaf = self.new_leaf(key, op.value.unwrap());

                        let cache = self.cache.lock().unwrap();
                        let new_node = cache.get(&new_node_ptr.id);
                        let mut new_mut_node = new_node.data.write()?;

                        if level >= key.len() {
                            new_mut_node.internal_data_mut().terminal = new_leaf;
                        } else {
                            new_mut_node.insert(key[level], new_leaf);
                        }

                        // Handle cases where there is a leaf for a key that is also a prefix. e.g. Key "a" and Key "ap", in this case "a" is both a prefix and a valid terminal.
                        if level == key2.len() {
                            new_mut_node.internal_data_mut().terminal = new_node_ptr;
                        } else {
                            // Insert a pointer to its self, this is because we are about to swap the new node's ptr in the NodeCache and WAL with the parent, i.e. the current node.
                            // This means ultimately it will point to the correct node after the swap.
                            new_mut_node.insert(key2[level], new_node_ptr);
                        }

                        cache
                            .swap(
                                mut_node,
                                node_wrapper.clone(),
                                new_mut_node,
                                new_node.clone(),
                            )
                            .expect("Expected swap and write to disk.");

                        drop(cache);

                        self.levels.fetch_max(level, Ordering::SeqCst);
                        return Ok(None);
                    }
                    OpType::Get => {
                        parent_read.try_sync()?;
                        if node.leaf_data_ref().key == key {
                            return Ok(Some(node.leaf_data_ref().value.clone()));
                        } else {
                            println!("{:?}", node.leaf_data_ref().key);
                            node.try_sync()?;
                            println!("THIS SHOULDN'T APPEAR 1");
                            return Err(Errors::NonExistantError);
                        }
                    }
                    _ => {
                        return Err(Errors::UnrecoverableError(
                            "Op not implemented yet.".to_string(),
                        ))
                    }
                }
            }

            let overlap = self.overlapping_prefix_len(&node, key, level);

            if overlap != node.internal_data_ref().prefix.len() {
                parent_read.try_sync()?;
                match op.optype {
                    OpType::Upsert => {
                        let parent_lock = parent.data.write()?;
                        let mut _mut_node = node_wrapper.data.write()?;

                        let new_prefix = &key[level..level + overlap];

                        if new_prefix == [] {
                            return Err(Errors::UnrecoverableError("The new prefix for an overlap mismatch was empty, tree is degenerate.".to_string()));
                        }

                        let new_node_ptr = self.new_node(new_prefix, NodeSize::Four);
                        let new_leaf = self.new_leaf(key, op.value.unwrap());

                        let cache = self.cache.lock().unwrap();
                        let new_node_wrapper = cache.get(&new_node_ptr.id);
                        let mut new_mut_node = new_node_wrapper.data.write()?;

                        new_mut_node.insert(key[overlap + level], new_leaf);
                        // Insert a pointer to itself because it will be swapped directly after.
                        new_mut_node
                            .insert(_mut_node.internal_data_ref().prefix[overlap], new_node_ptr);
                        let updated_prefix: Vec<u8> =
                            _mut_node.internal_data_mut().prefix[overlap..].into();

                        _mut_node.internal_data_mut().prefix = updated_prefix;

                        cache
                            .swap(
                                _mut_node,
                                node_wrapper.clone(),
                                new_mut_node,
                                new_node_wrapper.clone(),
                            )
                            .expect("Expected swap and write to disk.");

                        drop(cache);
                        drop(parent_lock);
                        return Ok(None);
                    }
                    _ => {
                        let prefix = node.internal_data_ref().prefix.clone();
                        node.try_sync()?;
                        println!(
                            "THIS SHOULDN'T APPEAR 2 {:?} | {:?} | {:?}",
                            overlap, key, prefix
                        );
                        return Err(Errors::NonExistantError);
                    }
                }
            }

            level = level + overlap;

            if level >= key.len() {
                parent_read.try_sync()?;
                match op.optype {
                    OpType::Upsert => {
                        // println!("IN TERMINAL");
                        let parent_lock = parent.data.write()?;
                        let mut mut_node = node_wrapper.data.write()?;
                        let cache = self.cache.lock().unwrap();

                        let ret =
                            if mut_node.internal_data_ref().terminal != NodePtr::sentinel_node() {
                                let terminal_node =
                                    cache.get(&mut_node.internal_data_ref().terminal.id);
                                let mut terminal_node_mut = terminal_node.data.write()?;
                                let old_value = terminal_node_mut.leaf_data_ref().value.clone();
                                terminal_node_mut.leaf_data_mut().value = op.value.unwrap();

                                cache
                                    .write_disk(
                                        mut_node.internal_data_ref().terminal.id,
                                        Some(terminal_node_mut.clone()),
                                    )
                                    .expect("Expected write to disk.");

                                Some(old_value)
                            } else {
                                let new_leaf_node = self.new_leaf(key, op.value.unwrap());

                                mut_node.internal_data_mut().terminal = new_leaf_node;

                                cache
                                    .write_disk(mut_node.id, Some(mut_node.clone()))
                                    .expect("Expected write to disk.");

                                None
                            };

                        drop(cache);
                        drop(parent_lock);

                        return Ok(ret);
                    }
                    OpType::Get => {
                        if node.internal_data_ref().terminal != NodePtr::sentinel_node() {
                            let terminal_node = self
                                .cache
                                .lock()
                                .unwrap()
                                .get(&node.internal_data_ref().terminal.id);
                            let terminal_node = terminal_node.data.read()?;

                            if terminal_node.leaf_data_ref().key != key {
                                node.try_sync()?;
                                return Err(Errors::UnrecoverableError("Terminal key did not match op key, this should never happen. Tree is degenerate.".to_string()));
                            }

                            return Ok(Some(terminal_node.leaf_data_ref().value.clone()));
                        } else {
                            node.try_sync()?;
                            println!("This shouldn't appear. 3");
                            return Err(Errors::NonExistantError);
                        }
                    }
                    _ => {
                        return Err(Errors::UnrecoverableError(
                            "Op not implemented.".to_string(),
                        ))
                    }
                }
            }

            next_node = node.get_child(key[level]);

            node.try_sync()?;
            node = node_wrapper.data.read()?;

            if next_node == NodePtr::sentinel_node() {
                match op.optype {
                    OpType::Upsert => {
                        if node.is_full() {
                            node.try_sync()?;
                            parent_read.try_sync()?;
                            let parent_lock = parent.data.write()?;
                            let mut mut_node = node_wrapper.data.write()?;
                            let new_leaf = self.new_leaf(key.into(), op.value.unwrap());

                            let cache = self.cache.lock().unwrap();

                            mut_node.insert(key[level], new_leaf);
                            mut_node.expand();

                            cache
                                .write_disk(mut_node.id, Some(mut_node.clone()))
                                .expect("Expected write.");

                            // cache.insert(mut_node.id, node_wrapper);
                            drop(mut_node);
                            drop(parent_lock);
                            drop(cache);
                        } else {
                            let mut mut_node = node_wrapper.data.write()?;
                            let new_leaf = self.new_leaf(key.into(), op.value.unwrap());
                            let cache = self.cache.lock().unwrap();

                            parent_read.try_sync()?;

                            mut_node.insert(key[level], new_leaf);
                            cache
                                .write_disk(mut_node.id as u64, Some(mut_node.clone()))
                                .expect("Expected write.");

                            drop(cache)
                        }

                        return Ok(None);
                    }
                    _ => {
                        node.try_sync()?;
                        println!("This shouldn't appear. 4");
                        return Err(Errors::NonExistantError);
                    }
                }
            }

            // println!("After standard syncs.");
            level = level;
            // println!("{:?}", level);
            node_ptr = next_node;
            parent = node_wrapper;
            parent_read = parent.data.read()?;
            // println!("After parent read.");
        }
    }

    fn swap_node(
        &self,
        parent: OptimisticLockCouplingWriteGuard<ArtNode<T>>,
        parent_wrapper: NodeWrapper<T>,
        child: OptimisticLockCouplingWriteGuard<ArtNode<T>>,
        child_wrapper: NodeWrapper<T>,
    ) {
        self.cache
            .lock()
            .unwrap()
            .swap(parent, parent_wrapper, child, child_wrapper)
            .expect("Expected SWAP to complete.");
    }

    fn new_leaf(&self, key: &[u8], value: T) -> NodePtr {
        let node = ArtNode::<T>::new_leaf(
            key.into(),
            value.clone(),
            self.count.fetch_add(1, Ordering::SeqCst),
            self.count.load(Ordering::SeqCst),
        );

        let ptr = NodePtr {
            id: node.id,
            generation: node.generation,
        };

        let cache = self.cache.lock().unwrap();

        cache
            .write_disk(node.id as u64, Some(node.clone()))
            .expect("Expected write on node swap.");

        cache.insert(node.id as u64, NodeWrapper::new(node.clone()));

        drop(cache);

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

        let cache = self.cache.lock().unwrap();

        cache
            .write_disk(node.id as u64, Some(node.clone()))
            .expect("Expected write on node swap.");

        cache.insert(node.id as u64, NodeWrapper::new(node.clone()));

        drop(cache);

        ptr
    }

    fn remove_node(&self, ptr: NodePtr) {
        self.cache.lock().unwrap().remove(ptr.id as u64);
    }

    pub fn get(&self, key: &[u8]) -> Result<T, Errors> {
        let op = Op::new(OpType::Get, key.into(), None);

        self.execute_with_retry(op).map(|val| val.unwrap())
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
    use std::time::Duration;

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
            assert_eq!(res, None);
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
            assert_eq!(res, None);

            let key = "abb".as_bytes();
            let value = 65;
            tree.insert(key, value)
                .expect("Expected no error on resize to 4.");
            assert_eq!(res, None);
            assert_eq!(tree.levels.load(Ordering::Relaxed), 2);
        })
    }

    #[test]
    fn insert_same_key_many_times() {
        in_temp_dir!(|path: String| {
            let tree = Dart::<u64>::new(100, 1, path.clone() + "/tree", path + "/wal");

            let key = "aba".as_bytes();
            let value = 64;

            let res = tree
                .insert(key, value)
                .expect("Expected no error on insert.");
            assert_eq!(res, None);

            let key = "abb".as_bytes();
            let value = 65;
            tree.insert(key, value)
                .expect("Expected no error on resize to 4.");
            assert_eq!(res, None);
            assert_eq!(tree.levels.load(Ordering::Relaxed), 2);

            for i in 0..100 {
                let key = "abc".as_bytes();
                let value = i;
                tree.insert(key, value)
                    .expect("Expected no error on resize to 4.");
                assert_eq!(res, None);
                assert_eq!(tree.levels.load(Ordering::Relaxed), 2);
            }

            for i in 0..100 {
                let key = "abd".as_bytes();
                let value = i;
                tree.insert(key, value)
                    .expect("Expected no error on resize to 4.");
                assert_eq!(res, None);
                assert_eq!(tree.levels.load(Ordering::Relaxed), 2);
            }
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
            assert_eq!(res, None);

            let key = "abb".as_bytes();
            let value = HashMap::<String, String>::new();
            tree.insert(key, value)
                .expect("Expected no error on resize to 4.");
            assert_eq!(res, None);
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
            assert_eq!(res, None);

            for i in 1..5 {
                let part = (i as u8) as char;
                let fmt = format!("ab{}", part);
                let key = fmt.as_bytes();
                let value = i as u64;

                let res = tree
                    .insert(key, value)
                    .expect(&format!("Expected no error on {i}th insert."));
                assert_eq!(res, None);
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
                assert_eq!(res, None);
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
                assert_eq!(res, None);
            }
            assert_eq!(tree.levels.load(Ordering::Relaxed), 2);
        })
    }

    #[test]
    fn remove_at_17th() {
        in_temp_dir!(|path: String| {
            let tree = Dart::<u64>::new(10, 1, path.clone() + "/tree", path + "/wal");

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
                assert_eq!(res, None);
            }

            tree.remove(key).expect("Expected this to not explode.");
        })
    }

    #[test]
    fn insert_at_49th() {
        in_temp_dir!(|path: String| {
            let tree = Dart::<u64>::new(10, 1, path.clone() + "/tree", path + "/wal");

            let key = "aba".as_bytes();
            let value = 64;

            let res = tree
                .insert(key, value)
                .expect("Expected no error on insert.");
            assert_eq!(res, None);

            for i in 0..49 {
                let part = (i as u8) as char;
                let fmt = format!("ab{}", part);
                let key = fmt.as_bytes();
                let value = i as u64;

                let res = tree
                    .insert(key, value)
                    .expect(&format!("Expected no error on {i}th insert."));
                assert_eq!(res, None);
            }

            assert_eq!(tree.levels.load(Ordering::Relaxed), 2);
        })
    }

    #[test]
    fn remove_at_49th() {
        in_temp_dir!(|path: String| {
            let tree = Dart::<u64>::new(10, 1, path.clone() + "/tree", path + "/wal");

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
                assert_eq!(res, None);
            }

            tree.remove(key).expect("Expected this to not explode.");

            assert_eq!(tree.levels.load(Ordering::Relaxed), 2);
        })
    }

    #[test]
    fn insert_multi_level() {
        in_temp_dir!(|path: String| {
            let tree = Dart::<u64>::new(10, 1, path.clone() + "/tree", path + "/wal");

            let key = vec![1, 2, 3];
            let value = 64;

            let res = tree
                .insert(&key, value)
                .expect("Expected no error on insert.");
            assert_eq!(res, None);

            let key2 = vec![1, 2, 4];
            let value = 65;
            let res = tree.insert(&key2, value).expect("Expected no error.");
            assert_eq!(res, None);

            for i in 0..4 {
                let mut key = key.clone();
                key.push(i);
                let value = i as u64;

                let res = tree
                    .insert(&key, value)
                    .expect(&format!("Expected no error on {i}th insert."));
                assert_eq!(res, None);
            }

            for i in 0..4 {
                let mut key = key2.clone();
                key.push(i);
                let value = i as u64;

                let res = tree
                    .insert(&key, value)
                    .expect(&format!("Expected no error on {i}th insert."));
                assert_eq!(res, None);
            }
            assert_eq!(tree.levels.load(Ordering::Relaxed), 3);
        })
    }

    #[test]
    fn insert_words() {
        in_temp_dir!(|path: String| {
            for _ in 0..10 {
                let tree = Dart::<u64>::new(5, 5, path.clone() + "/tree", path.clone() + "/wal");

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
                // println!("------------------------------");
                tree.get("apple".as_bytes())
                    .expect("Expected to get apple.");
                tree.get("appetizer".as_bytes())
                    .expect("Expected to get appetizer.");
                tree.get("apply".as_bytes())
                    .expect("Expected to get apply.");
                tree.get("apt".as_bytes()).expect("Expected to get apt.");
                // println!("^------------------------------^");
                tree.get("arrange".as_bytes())
                    .expect("Expected to get arrange.");
                // println!("V------------------------------V");
                tree.get("art".as_bytes()).expect("Expected to get art.");
                tree.get("archaic".as_bytes())
                    .expect("Expected to get archaic.");
                tree.get("arthropod".as_bytes())
                    .expect("Expected to get arthropod.");
                tree.get("arc".as_bytes()).expect("Expected to get arc.");
                tree.get("bar".as_bytes()).expect("Expected to get bar.");
                tree.get("bark".as_bytes()).expect("Expected to get bark.");
                tree.get("bet".as_bytes()).expect("Expected to get bet.");
                assert_eq!(tree.levels.load(Ordering::Relaxed), 4);
            }
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

                // println!("-----------S");
                // println!("INS: {:?}", i);
                let res = tree
                    .insert(&prev_key, value)
                    .expect(&format!("Expected no error on {i}th insert."));
                // println!("GET: {:?}", i);
                assert_eq!(res, None);
                // println!("-----------E");
            }
        })
    }

    #[test]
    fn threaded_inserts() {
        use std::sync::Arc;
        use std::thread;
        in_temp_dir!(|path: String| {
            for i in 0..100000 {
                let tree = Arc::new(Dart::<u64>::new(
                    10,
                    1,
                    path.clone() + "/tree",
                    path.clone() + "/wal",
                ));

                let ref1 = tree.clone();
                let ref2 = tree.clone();

                let h1 = thread::spawn(move || {
                    let tree = ref1;
                    tree.insert("apple".as_bytes(), 1).unwrap();
                    tree.insert("apply".as_bytes(), 3).unwrap();
                    tree.insert("apt".as_bytes(), 4).unwrap();
                    tree.insert("bark".as_bytes(), 100).unwrap();
                    tree.insert("arrange".as_bytes(), 5).unwrap();
                });

                let h2 = thread::spawn(move || {
                    let tree = ref2;
                    tree.insert("appetizer".as_bytes(), 2).unwrap();
                    tree.insert("art".as_bytes(), 5).unwrap();
                    tree.insert("archaic".as_bytes(), 5).unwrap();
                    tree.insert("arthropod".as_bytes(), 5).unwrap();
                    tree.insert("arc".as_bytes(), 5).unwrap();
                    tree.insert("bar".as_bytes(), 5).unwrap();
                    tree.insert("bet".as_bytes(), 5).unwrap();
                });

                h1.join()
                    .expect("Expected first thread to exit successfully.");
                h2.join()
                    .expect("Expected second thread to exit successfully.");
                thread::sleep(Duration::from_millis(100));

                tree.get("appetizer".as_bytes())
                    .expect("Expected to get appetizer.");
                tree.get("art".as_bytes()).expect("Expected to get art.");
                tree.get("archaic".as_bytes())
                    .expect("Expected to get archaic.");
                tree.get("arthropod".as_bytes())
                    .expect("Expected to get arthropod.");
                tree.get("apple".as_bytes())
                    .expect("Expected to get apple.");
                tree.get("arc".as_bytes()).expect("Expected to get arc.");
                tree.get("bar".as_bytes()).expect("Expected to get bar.");
                tree.get("bark".as_bytes()).expect("Expected to get bark.");
                tree.get("apple".as_bytes())
                    .expect("Expected to get apple.");
                tree.get("apply".as_bytes())
                    .expect("Expected to get apply.");
                tree.get("apt".as_bytes()).expect("Expected to get apt.");
                tree.get("bark".as_bytes()).expect("Expected to get bark.");
                tree.get("arrange".as_bytes())
                    .expect("Expected to get arrange.");
                tree.get("bet".as_bytes()).expect("Expected to get bet.");
                assert_eq!(tree.levels.load(Ordering::Relaxed), 4);
                println!("DONE ONE LOOP {:?}", i);
            }
        })
    }

    #[test]
    fn insert_words_permutations() {
        for _ in 0..100 {
            in_temp_dir!(|path: String| {
                let tree = Dart::<u64>::new(1, 1, path.clone() + "/tree", path.clone() + "/wal");
                let mut words = vec![
                    "apple",
                    "apply",
                    "apt",
                    "bark",
                    "arrange",
                    "appetizer",
                    "art",
                    "archaic",
                    "arthropod",
                    "arc",
                    "bar",
                    "bet",
                ];

                let mut rng = rand::thread_rng();
                for _ in 0..12 {
                    let index: usize = rng.gen_range(0..words.len());
                    let word = words.remove(index);

                    tree.insert(word.as_bytes(), 1).unwrap();
                }

                std::thread::sleep(Duration::from_millis(100));
                tree.get("appetizer".as_bytes())
                    .expect("Expected to get appetizer.");
                tree.get("art".as_bytes()).expect("Expected to get art.");
                tree.get("archaic".as_bytes())
                    .expect("Expected to get archaic.");
                tree.get("arthropod".as_bytes())
                    .expect("Expected to get arthropod.");
                tree.get("apple".as_bytes())
                    .expect("Expected to get apple.");
                tree.get("arc".as_bytes()).expect("Expected to get arc.");
                tree.get("bar".as_bytes()).expect("Expected to get bar.");
                tree.get("bark".as_bytes()).expect("Expected to get bark.");
                tree.get("apple".as_bytes())
                    .expect("Expected to get apple.");
                tree.get("apply".as_bytes())
                    .expect("Expected to get apply.");
                tree.get("apt".as_bytes()).expect("Expected to get apt.");
                tree.get("bark".as_bytes()).expect("Expected to get bark.");
                tree.get("arrange".as_bytes())
                    .expect("Expected to get arrage.");
                tree.get("bet".as_bytes()).expect("Expected to get bet.");
                assert_eq!(tree.levels.load(Ordering::Relaxed), 4);
            })
        }
    }

    #[test]
    fn insert_breaking() {
        in_temp_dir!(|path: String| {
            let tree = Dart::<u64>::new(1, 1, path.clone() + "/tree", path + "/wal");
            tree.insert("bark".as_bytes(), 1).unwrap();
            tree.insert("arrange".as_bytes(), 1).unwrap();
            tree.insert("arc".as_bytes(), 1).unwrap();
            tree.insert("apply".as_bytes(), 1).unwrap();
            tree.insert("apple".as_bytes(), 1).unwrap();
            tree.insert("archaic".as_bytes(), 5).unwrap();
            tree.insert("appetizer".as_bytes(), 5).unwrap();
            tree.insert("art".as_bytes(), 1).unwrap();
            tree.insert("apt".as_bytes(), 1).unwrap();
            tree.insert("arthropod".as_bytes(), 1).unwrap();
            tree.insert("bet".as_bytes(), 100).unwrap();
            tree.insert("bar".as_bytes(), 5).unwrap();

            tree.get("appetizer".as_bytes())
                .expect("Expected to get appetizer.");
            tree.get("art".as_bytes()).expect("Expected to get art.");
            tree.get("archaic".as_bytes())
                .expect("Expected to get archaic.");
            tree.get("arthropod".as_bytes())
                .expect("Expected to get arthropod.");
            tree.get("apple".as_bytes())
                .expect("Expected to get apple.");
            tree.get("arc".as_bytes()).expect("Expected to get arc.");
            tree.get("bar".as_bytes()).expect("Expected to get bar.");
            tree.get("bark".as_bytes()).expect("Expected to get bark.");
            tree.get("apple".as_bytes())
                .expect("Expected to get apple.");
            tree.get("apply".as_bytes())
                .expect("Expected to get apply.");
            tree.get("apt".as_bytes()).expect("Expected to get apt.");
            tree.get("bark".as_bytes()).expect("Expected to get bark.");
            tree.get("arrange".as_bytes())
                .expect("Expected to get arrage.");
            tree.get("bet".as_bytes()).expect("Expected to get bet.");
        });
    }

    #[test]
    fn insert_deep_and_wide() {
        in_temp_dir!(|path: String| {
            let tree = Dart::<u64>::new(10000, 5, path.clone() + "/tree", path + "/wal");

            for i in 0..=255 {
                let level_vec: Vec<u8> = (0..=i).collect();

                for i in 0..=255 {
                    let mut current_vec = level_vec.clone();
                    current_vec.push(i);
                    let value = i as u64;

                    let res = tree
                        .insert(&current_vec, value)
                        .expect(&format!("Expected no error on {i}th insert."));
                    assert_eq!(res, None);
                }
            }
        })
    }

    #[test]
    fn get_element_when_exists() {
        in_temp_dir!(|path: String| {
            let tree = Dart::<u64>::new(100, 1, path.clone() + "/tree", path + "/wal");

            for i in 0..10 {
                let level_vec: Vec<u8> = (0..=i).collect();

                for j in 0..100 {
                    let mut key = level_vec.clone();
                    key.push(j);
                    let value = j as u64;

                    let res = tree
                        .insert(&key, value)
                        .expect(&format!("Expected no error on {j}th insert."));
                    assert_eq!(res, None);
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
                let level_vec: Vec<u8> = (0..=i).collect();

                for j in 0..10 {
                    let mut key = level_vec.clone();
                    key.push(j);
                    let value = j as u64;

                    let res = tree
                        .insert(&key, value)
                        .expect(&format!("Expected no error on {j}th insert."));
                    assert_eq!(res, None);
                }
            }

            let res = tree.get(&[0, 1, 2, 3, 11]);

            assert_eq!(res.is_err(), true);

            match res {
                Err(e) => match e {
                    Errors::NonExistantError => true,
                    _ => panic!("Expected a NonExistantError"),
                },
                _ => panic!("Expected key not to be found!"),
            }
        })
    }

    #[test]
    fn insert_terminal_remove_terminal_insert_terminal() {
        in_temp_dir!(|path: String| {
            let tree = Dart::<u64>::new(5, 1, path.clone() + "/tree", path.clone() + "/wal");

            for i in 0..=10 {
                let level_vec: Vec<u8> = (0..i).collect();

                for i in 0..=10 {
                    let mut current_vec = level_vec.clone();
                    current_vec.push(i);
                    let value = i as u64;

                    let res = tree
                        .insert(&current_vec, value)
                        .expect(&format!("Expected no error on {i}th insert."));
                    assert_eq!(res, None);
                }
            }

            let test_key = &[0, 1, 2, 3, 4, 5];

            tree.remove(test_key)
                .expect("Expected this to remove correctly.");

            tree.insert(test_key, 20)
                .expect("Expected this to insert correctly.");

            tree.insert(test_key, 64)
                .expect("Expected this to insert correctly.");

            assert_eq!(tree.get(test_key).unwrap(), 64);
        })
    }

    #[test]
    fn get_on_insert() {
        in_temp_dir!(|path: String| {
            let tree = Dart::<u64>::new(10000, 5, path.clone() + "/tree", path + "/wal");

            for i in 0..=255 {
                let level_vec: Vec<u8> = (0..=i).collect();

                for i in 0..=255 {
                    let mut current_vec = level_vec.clone();
                    current_vec.push(i);
                    let value = i as u64;

                    let res = tree
                        .insert(&current_vec, value)
                        .expect(&format!("Expected no error on {i}th insert."));
                    assert_eq!(res, None);
                    let err = format!(
                        "Expected to get value just inserted with key: {:?}.",
                        current_vec
                    );
                    let res = tree.get(&current_vec).expect(err.as_str());

                    assert_eq!(res, value);
                    println!("END");
                }
            }

            for i in 0..=255 {
                let level_vec: Vec<u8> = (0..=i).collect();

                for i in 0..=255 {
                    let mut current_vec = level_vec.clone();
                    current_vec.push(i);
                    let value = i as u64;

                    let res = tree.get(&current_vec).expect("Expected to get value.");

                    assert_eq!(res, value);
                }
            }
        })
    }

    #[test]
    fn insert_many_random_removal() {
        in_temp_dir!(|path: String| {
            let tree = Dart::<u64>::new(60000, 5, path.clone() + "/tree", path.clone() + "/wal");

            for i in 0..=255 {
                let level_vec: Vec<u8> = (0..=i).collect();

                for i in 0..=255 {
                    let mut current_vec = level_vec.clone();
                    current_vec.push(i);
                    let value = i as u64;

                    let res = tree
                        .insert(&current_vec, value)
                        .expect(&format!("Expected no error on {i}th insert."));
                    assert_eq!(res, None);
                }
            }

            std::thread::sleep(Duration::from_millis(1000));

            let mut rng = rand::thread_rng();
            for i in 0..=255 {
                let level_vec: Vec<u8> = (0..=i).collect();

                for j in 0..=255 {
                    let remove: f64 = rng.gen();

                    if remove > 0.9 {
                        let mut current_vec = level_vec.clone();
                        current_vec.push(j);

                        let res = tree.remove(&current_vec);

                        let res = res.expect(&format!("Expected no error on {i}/{j} th removal."));

                        assert_eq!(res, ());
                    }
                }
            }
        })
    }
}
