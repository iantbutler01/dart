use crate::errors::Errors;
use crate::errors::OptimisticLockCouplingErrorType;
use crate::node::*;
use bincode::{Decode, Encode};
use crossbeam::atomic::AtomicCell;
use crossbeam::utils::Backoff;
use rayon::prelude::IndexedParallelIterator;
use rayon::range;
use std::collections::VecDeque;
use std::ops::Index;
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

pub(crate) struct NodeIter<T: Clone + Encode + Decode> {
    iter: Box<dyn Iterator<Item = Option<ArtNode<T>>>>,
}

impl<T: 'static + Clone + Encode + Decode> NodeIter<T> {
    pub(crate) fn new(vec: Vec<Option<ArtNode<T>>>) -> Self {
        Self {
            iter: Box::new(vec.clone().into_iter()),
        }
    }
}

impl<T: Clone + Encode + Decode> Iterator for NodeIter<T> {
    type Item = Option<ArtNode<T>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
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

    pub fn upsert(&self, key: &[u8], value: T) -> Result<Option<T>, Errors> {
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

    pub fn remove(&self, key: &[u8]) -> Result<Option<T>, Errors> {
        let op = Op::new(OpType::Remove, key.into(), None);

        self.execute_with_retry(op)
    }

    pub fn clean_up(&self) -> () {
        let node_iter = self.iter_nodes();

        let cache = self.cache.lock().unwrap();

        for node_opt in node_iter {
            if node_opt.is_some() {
                let node =
                    node_opt.expect("Expected node to exist after literally just checking it did.");
                cache.remove(node.id);
                cache
                    .write_disk(node.id, None)
                    .expect("Expected write to disk to succeed.");
            }
        }

        cache
            .disk_maintenance()
            .expect("Expected disk maintenence to return success");
    }

    fn execute(&self, op: Op<T>) -> Result<Option<T>, Errors> {
        let key = op.key.as_slice();
        let root_ptr = self.root.load();
        let root_wrapper_opt = self.cache.lock().unwrap().get(&(root_ptr.id as u64));
        if root_wrapper_opt.is_none() {
            return Ok(None);
        }
        let root_wrapper = root_wrapper_opt.unwrap();
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
                _ => return Ok(None),
            }
        }

        root.try_sync()?;

        let mut parent = root_wrapper;
        let mut level = 0usize;
        let mut next_node: NodePtr;
        let mut parent_read = parent.data.read()?;

        loop {
            let node_wrapper_opt = self.cache.lock().unwrap().get(&(node_ptr.id as u64));
            if node_wrapper_opt.is_none() {
                match op.optype {
                    _ => {
                        return Ok(None);
                    }
                }
            }
            let node_wrapper = node_wrapper_opt.unwrap();

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
                            let cache = self.cache.lock().unwrap();
                            let old_value = mut_node.leaf_data_ref().value.clone();
                            mut_node.leaf_data_mut().value = op.value.unwrap().clone();
                            cache
                                .write_disk(mut_node.id, Some(mut_node.clone()))
                                .expect("Expected write to disk.");

                            drop(cache);

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
                            let parent_read = parent.data.read()?;
                            println!(
                                "LEAF EXPANSION {:?} | {:?} | {:?} | {:?} | {:?} | {:?} | {:?} | {:?}",
                                level,
                                i,
                                key,
                                key2,
                                new_prefix,
                                parent_read.internal_data_ref(),
                                parent_read.get_child(key[level]),
                                parent_read.get_child(key[level]).id == mut_node.id
                            );

                            return Err(Errors::UnrecoverableError("The new prefix for a leaf expansion was empty, tree is degenerate.".to_string()));
                        }

                        let new_old_leaf_ptr =
                            self.new_leaf(&key2, mut_node.leaf_data_ref().value.clone());

                        let new_leaf = self.new_leaf(key, op.value.unwrap());

                        let _ = mut_node.expand();

                        if level >= key.len() {
                            mut_node.internal_data_mut().terminal = new_leaf;
                        } else {
                            mut_node.insert(key[level], new_leaf);
                        }

                        mut_node.internal_data_mut().prefix = new_prefix;

                        // Handle cases where there is a leaf for a key that is also a prefix. e.g. Key "a" and Key "ap", in this case "a" is both a prefix and a valid terminal.
                        if level == key2.len() {
                            mut_node.internal_data_mut().terminal = new_old_leaf_ptr;
                        } else {
                            mut_node.insert(key2[level], new_old_leaf_ptr);
                        }

                        let cache = self
                            .cache
                            .lock()
                            .expect("Expected to acquire lock for cache.");

                        cache
                            .write_disk(mut_node.id, Some(mut_node.clone()))
                            .expect("Expected to write to disk.");
                        drop(cache);

                        self.levels.fetch_max(level, Ordering::SeqCst);
                        return Ok(None);
                    }
                    OpType::Get => {
                        parent_read.try_sync()?;
                        if node.leaf_data_ref().key == key {
                            return Ok(Some(node.leaf_data_ref().value.clone()));
                        } else {
                            node.try_sync()?;
                            return Ok(None);
                        }
                    }
                    OpType::Remove => {
                        if node.leaf_data_ref().key != key {
                            return Ok(None);
                        }
                        let cache = self.cache.lock().unwrap();

                        let mut parent_mut = parent.data.write()?;

                        cache.remove(node_ptr.id);
                        cache
                            .write_disk(node_ptr.id, None)
                            .expect("Expected write to disk.");

                        if key == node.leaf_data_ref().key {
                            parent_mut.remove(key[level]);
                        }

                        if parent_mut.internal_data_ref().prefix != [] && parent_mut.should_shrink()
                        {
                            parent_mut.shrink();
                        }

                        cache
                            .write_disk(parent_mut.id, Some(parent_mut.clone()))
                            .expect("Expected write to disk.");

                        drop(cache);

                        return Ok(Some(node.leaf_data_ref().value.clone()));
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
                        let old_prefix = _mut_node.internal_data_ref().prefix.clone();

                        let new_prefix = &key[level..level + overlap];

                        if new_prefix == [] {
                            println!(
                                "OVERLAP EXPANSION: {:?} | {:?} | {:?} | {:?} | {:?} | {:?}",
                                level,
                                overlap + level,
                                key,
                                _mut_node.internal_data_ref().prefix,
                                _mut_node.internal_data_ref().terminal,
                                new_prefix
                            );
                            return Err(Errors::UnrecoverableError("The new prefix for an overlap mismatch was empty, tree is degenerate.".to_string()));
                        }

                        if _mut_node.is_full() {
                            println!("EXPANDING IN OVERLAP");
                            _mut_node.expand();
                        }

                        let new_node_ptr = self.new_node(
                            _mut_node.internal_data_ref().prefix.as_slice(),
                            _mut_node.size,
                        );
                        let new_node_wrapper =
                            self.cache.lock().unwrap().get(&new_node_ptr.id).unwrap();
                        let mut new_node_mut = new_node_wrapper.data.write()?;

                        let original_data = _mut_node.copy_internal_contents_to(&mut new_node_mut);

                        let new_leaf_ptr = self.new_leaf(key, op.value.unwrap());

                        if key.len() <= overlap + level {
                            println!(
                                "OVERLAP EXPANSION: {:?} | {:?} | {:?} | {:?} | {:?} | {:?}",
                                level,
                                overlap + level,
                                key,
                                _mut_node.internal_data_ref().prefix,
                                _mut_node.internal_data_ref().terminal,
                                new_prefix
                            )
                        }

                        _mut_node.internal_data_mut().idx = original_data.idx;
                        _mut_node.internal_data_mut().children = original_data.children;
                        _mut_node.internal_data_mut().count = original_data.count;
                        _mut_node.internal_data_mut().next_pos = original_data.next_pos;
                        _mut_node.internal_data_mut().terminal = original_data.terminal;

                        if overlap + level == key.len() {
                            _mut_node.internal_data_mut().terminal = new_leaf_ptr
                        } else {
                            _mut_node.insert(key[overlap + level], new_leaf_ptr);
                        }
                        _mut_node.insert(old_prefix[overlap], new_node_ptr);

                        let updated_prefix: Vec<u8> =
                            _mut_node.internal_data_mut().prefix[overlap..].into();

                        new_node_mut.internal_data_mut().prefix = updated_prefix;

                        _mut_node.internal_data_mut().prefix = new_prefix.into();

                        let cache = self
                            .cache
                            .lock()
                            .expect("Expected to acquire lock on cache.");

                        cache
                            .write_disk(_mut_node.id, Some(_mut_node.clone()))
                            .expect("Expected to write to disk.");
                        cache
                            .write_disk(new_node_mut.id, Some(new_node_mut.clone()))
                            .expect("Expected to write to disk.");

                        drop(cache);
                        drop(parent_lock);
                        return Ok(None);
                    }
                    _ => {
                        node.try_sync()?;
                        return Ok(None);
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

                        let ret =
                            if mut_node.internal_data_ref().terminal != NodePtr::sentinel_node() {
                                let cache = self.cache.lock().unwrap();
                                let terminal_node_opt =
                                    cache.get(&mut_node.internal_data_ref().terminal.id);
                                if terminal_node_opt.is_none() {
                                    return Ok(None);
                                }
                                let terminal_node = terminal_node_opt.unwrap();

                                let mut terminal_node_mut = terminal_node.data.write()?;
                                let old_value = terminal_node_mut.leaf_data_ref().value.clone();
                                terminal_node_mut.leaf_data_mut().value = op.value.unwrap();

                                cache
                                    .write_disk(
                                        mut_node.internal_data_ref().terminal.id,
                                        Some(terminal_node_mut.clone()),
                                    )
                                    .expect("Expected write to disk.");

                                drop(cache);
                                Some(old_value)
                            } else {
                                let new_leaf_node = self.new_leaf(key, op.value.unwrap());

                                let cache = self.cache.lock().unwrap();
                                mut_node.internal_data_mut().terminal = new_leaf_node;

                                cache
                                    .write_disk(mut_node.id, Some(mut_node.clone()))
                                    .expect("Expected write to disk.");

                                drop(cache);

                                None
                            };

                        drop(parent_lock);

                        return Ok(ret);
                    }
                    OpType::Get => {
                        if node.internal_data_ref().terminal != NodePtr::sentinel_node() {
                            let terminal_node_opt = self
                                .cache
                                .lock()
                                .unwrap()
                                .get(&node.internal_data_ref().terminal.id);
                            if terminal_node_opt.is_none() {
                                return Ok(None);
                            }
                            let terminal_node = terminal_node_opt.unwrap();

                            let terminal_node = terminal_node.data.read()?;

                            if terminal_node.leaf_data_ref().key != key {
                                node.try_sync()?;
                                return Err(Errors::UnrecoverableError("Terminal key did not match op key, this should never happen. Tree is degenerate.".to_string()));
                            }

                            return Ok(Some(terminal_node.leaf_data_ref().value.clone()));
                        } else {
                            node.try_sync()?;
                            println!("This shouldn't appear. 3");
                            return Ok(None);
                        }
                    }
                    OpType::Remove => {
                        if node.internal_data_ref().terminal != NodePtr::sentinel_node() {
                            let cache = self.cache.lock().unwrap();

                            let terminal_wrapper_opt =
                                cache.get(&(node.internal_data_ref().terminal.id as u64));

                            if terminal_wrapper_opt.is_none() {
                                return Ok(None);
                            }

                            let terminal_wrapper = terminal_wrapper_opt.unwrap();

                            let terminal = terminal_wrapper.data.read()?;

                            let mut node_mut = node_wrapper.data.write()?;
                            node_mut.internal_data_mut().terminal = NodePtr::sentinel_node();
                            cache
                                .write_disk(node_mut.id, Some(node_mut.clone()))
                                .expect("Expected write to disk.");

                            cache.remove(terminal.id as u64);
                            cache
                                .write_disk(terminal.id, None)
                                .expect("Expected write to disk.");

                            drop(cache);

                            return Ok(Some(terminal.leaf_data_ref().value.clone()));
                        } else {
                            return Ok(None);
                        }
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

                            mut_node.expand();
                            mut_node.insert(key[level], new_leaf);

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
                        return Ok(None);
                    }
                }
            }

            // println!("{:?}", level);
            node_ptr = next_node;
            parent = node_wrapper;
            parent_read = parent.data.read()?;
            // println!("After parent read.");
        }
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

    pub(crate) fn iter_nodes(&self) -> impl Iterator<Item = Option<ArtNode<T>>> {
        let cache = self.cache.lock().expect("Expected to acquire cache lock.");

        let mut queue: VecDeque<NodePtr> = VecDeque::new();
        let mut node_vec: Vec<Option<ArtNode<T>>> = Vec::new();

        let root_ptr = self.root.load();
        if root_ptr == NodePtr::sentinel_node() {
            return NodeIter::new(Vec::new());
        }

        queue.push_back(root_ptr);

        while queue.len() > 0 {
            let node_ptr = queue.pop_front().unwrap();
            let node_opt = cache.get_node_raw(&(node_ptr.id as u64));

            if node_opt.is_none() {
                continue;
            }

            let node = node_opt.unwrap();

            if node.is_leaf() {
                node_vec.push(Some(node.clone()));
                continue;
            }

            if node.internal_data_ref().terminal != NodePtr::sentinel_node() {
                queue.push_back(node.internal_data_ref().terminal)
            }

            for ptr in node.internal_data_ref().children.iter() {
                queue.push_back(*ptr)
            }

            continue;
        }

        NodeIter::new(node_vec)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<T>, Errors> {
        let op = Op::new(OpType::Get, key.into(), None);

        self.execute_with_retry(op)
    }

    pub(crate) fn overlapping_prefix_len(
        &self,
        node: &ArtNode<T>,
        key: &[u8],
        level: usize,
    ) -> usize {
        let range_max = core::cmp::min(node.internal_data_ref().prefix.len(), key.len());

        // println!(
        //     "{:?} | {:?} | {:?} | {:?}",
        //     level,
        //     key.len(),
        //     key[level],
        //     node.internal_data_ref().prefix
        // );
        let mut i = 0;
        while i < range_max && i + level < key.len() {
            if node.internal_data_ref().prefix[i] == key[i + level] {
                i += 1;
            } else {
                break;
            }
        }
        i
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use crate::tree::Dart;
    use crate::utils::in_temp_dir;

    #[test]
    fn insert_at_root() {
        in_temp_dir!(|path: String| {
            let tree = Dart::<u64>::new(100, 1, path.clone() + "/tree", path.clone() + "/wal");

            let key = "abc".as_bytes();
            let value = 64;

            let res = tree
                .upsert(key, value)
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
                .upsert(key, value)
                .expect("Expected no error on insert.");
            assert_eq!(res, None);

            let key = "abb".as_bytes();
            let value = 65;
            tree.upsert(key, value)
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
                .upsert(key, value)
                .expect("Expected no error on insert.");
            assert_eq!(res, None);

            let key = "abb".as_bytes();
            let value = 65;
            tree.upsert(key, value)
                .expect("Expected no error on resize to 4.");
            assert_eq!(res, None);
            assert_eq!(tree.levels.load(Ordering::Relaxed), 2);

            for i in 0..100 {
                let key = "abc".as_bytes();
                let value = i;
                tree.upsert(key, value)
                    .expect("Expected no error on resize to 4.");
                assert_eq!(res, None);
                assert_eq!(tree.levels.load(Ordering::Relaxed), 2);
            }

            for i in 0..100 {
                let key = "abd".as_bytes();
                let value = i;
                tree.upsert(key, value)
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
                .upsert(key, value)
                .expect("Expected no error on insert.");
            assert_eq!(res, None);

            let key = "abb".as_bytes();
            let value = HashMap::<String, String>::new();
            tree.upsert(key, value)
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
                .upsert(key, value)
                .expect("Expected no error on insert.");
            assert_eq!(res, None);

            for i in 1..5 {
                let part = (i as u8) as char;
                let fmt = format!("ab{}", part);
                let key = fmt.as_bytes();
                let value = i as u64;

                let res = tree
                    .upsert(key, value)
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
                key = fmt.as_bytes();
                let value = i as u64;

                let res = tree
                    .upsert(key, value)
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
                    .upsert(key, value)
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
                key = fmt.as_bytes();
                let value = i as u64;

                let res = tree
                    .upsert(key, value)
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
                .upsert(key, value)
                .expect("Expected no error on insert.");
            assert_eq!(res, None);

            for i in 0..49 {
                let part = (i as u8) as char;
                let fmt = format!("ab{}", part);
                let key = fmt.as_bytes();
                let value = i as u64;

                let res = tree
                    .upsert(key, value)
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
                key = fmt.as_bytes();
                let value = i as u64;

                let res = tree
                    .upsert(key, value)
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
                .upsert(&key, value)
                .expect("Expected no error on insert.");
            assert_eq!(res, None);

            let key2 = vec![1, 2, 4];
            let value = 65;
            let res = tree.upsert(&key2, value).expect("Expected no error.");
            assert_eq!(res, None);

            for i in 0..4 {
                let mut key = key.clone();
                key.push(i);
                let value = i as u64;

                let res = tree
                    .upsert(&key, value)
                    .expect(&format!("Expected no error on {i}th insert."));
                assert_eq!(res, None);
            }

            for i in 0..4 {
                let mut key = key2.clone();
                key.push(i);
                let value = i as u64;

                let res = tree
                    .upsert(&key, value)
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

                tree.upsert("apple".as_bytes(), 1).unwrap();
                tree.upsert("appetizer".as_bytes(), 2).unwrap();
                tree.upsert("apply".as_bytes(), 3).unwrap();
                tree.upsert("apt".as_bytes(), 4).unwrap();
                tree.upsert("arrange".as_bytes(), 5).unwrap();
                tree.upsert("art".as_bytes(), 5).unwrap();
                tree.upsert("archaic".as_bytes(), 5).unwrap();
                tree.upsert("arthropod".as_bytes(), 5).unwrap();
                tree.upsert("arc".as_bytes(), 5).unwrap();
                tree.upsert("bar".as_bytes(), 5).unwrap();
                tree.upsert("bark".as_bytes(), 5).unwrap();
                tree.upsert("bet".as_bytes(), 5).unwrap();
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
            }
        })
    }

    #[test]
    fn insert_overlap_at_extremes() {
        in_temp_dir!(|path: String| {
            let tree = Dart::<u64>::new(100, 1, path.clone() + "/tree", path + "/wal");

            let k1 = &[0, 1, 2, 3, 4];
            let k2 = &[0, 1, 2, 3, 4, 5, 6, 7];
            let k3 = &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 7];
            let k4 = &[0, 1, 2, 3, 4, 5];

            tree.upsert(k1, 1).expect("Expect upsert to succeed.");
            tree.upsert(k2, 1).expect("Expect upsert to succeed.");
            tree.upsert(k3, 1).expect("Expect upsert to succeed.");
            tree.upsert(k4, 1).expect("Expect upsert to succeed.");
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
                    .upsert(&prev_key, value)
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
            for _i in 0..10 {
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
                    tree.upsert("apple".as_bytes(), 1).unwrap();
                    tree.upsert("apply".as_bytes(), 3).unwrap();
                    tree.upsert("apt".as_bytes(), 4).unwrap();
                    tree.upsert("bark".as_bytes(), 100).unwrap();
                    tree.upsert("arrange".as_bytes(), 5).unwrap();
                });

                let h2 = thread::spawn(move || {
                    let tree = ref2;
                    tree.upsert("appetizer".as_bytes(), 2).unwrap();
                    tree.upsert("art".as_bytes(), 5).unwrap();
                    tree.upsert("archaic".as_bytes(), 5).unwrap();
                    tree.upsert("arthropod".as_bytes(), 5).unwrap();
                    tree.upsert("arc".as_bytes(), 5).unwrap();
                    tree.upsert("bar".as_bytes(), 5).unwrap();
                    tree.upsert("bet".as_bytes(), 5).unwrap();
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

                    tree.upsert(word.as_bytes(), 1).unwrap();
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

                println!("----------------------");
            })
        }
    }

    #[test]
    fn insert_breaking() {
        in_temp_dir!(|path: String| {
            let tree = Dart::<u64>::new(1, 1, path.clone() + "/tree", path + "/wal");
            tree.upsert("bark".as_bytes(), 1).unwrap();
            tree.upsert("arrange".as_bytes(), 1).unwrap();
            tree.upsert("arc".as_bytes(), 1).unwrap();
            tree.upsert("apply".as_bytes(), 1).unwrap();
            tree.upsert("apple".as_bytes(), 1).unwrap();
            tree.upsert("archaic".as_bytes(), 5).unwrap();
            tree.upsert("appetizer".as_bytes(), 5).unwrap();
            tree.upsert("art".as_bytes(), 1).unwrap();
            tree.upsert("apt".as_bytes(), 1).unwrap();
            tree.upsert("arthropod".as_bytes(), 1).unwrap();
            tree.upsert("bet".as_bytes(), 100).unwrap();
            tree.upsert("bar".as_bytes(), 5).unwrap();

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
                        .upsert(&current_vec, value)
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
                        .upsert(&key, value)
                        .expect(&format!("Expected no error on {j}th insert."));
                    assert_eq!(res, None);
                }
            }

            let res = tree.get(&[0, 1, 2, 3, 9]).expect("Expected result.");
            assert_eq!(res.unwrap(), 9)
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
                        .upsert(&key, value)
                        .expect(&format!("Expected no error on {j}th insert."));
                    assert_eq!(res, None);
                }
            }

            let res = tree.get(&[0, 1, 2, 3, 11]);

            match res {
                Ok(None) => true,
                Err(e) => match e {
                    _ => panic!("Expected no error."),
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
                        .upsert(&current_vec, value)
                        .expect(&format!("Expected no error on {i}th insert."));
                    assert_eq!(res, None);
                }
            }

            let test_key = &[0, 1, 2, 3, 4, 5];

            tree.remove(test_key)
                .expect("Expected this to remove correctly.");

            tree.upsert(test_key, 20)
                .expect("Expected this to insert correctly.");

            tree.upsert(test_key, 64)
                .expect("Expected this to insert correctly.");

            assert_eq!(tree.get(test_key).unwrap().unwrap(), 64);
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
                        .upsert(&current_vec, value)
                        .expect(&format!("Expected no error on {i}th insert."));
                    assert_eq!(res, None);
                    let err = format!(
                        "Expected to get value just inserted with key: {:?}.",
                        current_vec
                    );
                    let res = tree.get(&current_vec).expect(err.as_str());
                    if res.is_none() {
                        println!("{:?} | {:?}", current_vec, i)
                    }
                    assert_eq!(res.unwrap(), value);
                }
            }

            for i in 0..=255 {
                let level_vec: Vec<u8> = (0..=i).collect();

                for i in 0..=255 {
                    let mut current_vec = level_vec.clone();
                    current_vec.push(i);
                    let value = i as u64;

                    let res = tree.get(&current_vec).expect("Expected to get value.");

                    assert_eq!(res.unwrap(), value);
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
                        .upsert(&current_vec, value)
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

                        assert_eq!(res, Some(j as u64));
                    }
                }
            }
        })
    }
}
