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
    root: AtomicCell<NodePtr>,
    cache: Arc<NodeCache<T>>,
    generation: AtomicUsize,
    count: AtomicUsize,
}

impl<T: Encode + Decode> Dart<T>
where
    T: Clone,
{
    pub fn insert(&self, key: &[u8], value: T) -> Result<(), Errors> {
        let backoff = Backoff::new();
        loop {
            match self.insert_internal(key, value.clone(), None) {
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
        let mut node_ptr = self.root.load();

        if node_ptr == NodePtr::sentinel_node() {
            return Err(Errors::EmptyTreeError);
        }

        let mut level = 0usize;
        let mut next_node: NodePtr;
        let mut parent: Option<NodeWrapper<T>> = None;

        loop {
            let node_wrapper = self.cache.get(&(node_ptr.id as u64));
            let read_guard = node_wrapper.data.read()?;
            let mut node = read_guard;

            // Implement insertion starting with finding the nextNode if the prefix matches the current node for the given level.
            let overlap = self.overlapping_prefix_len(&node, key);
            if overlap != node.internal_data_ref().prefix.len() {
                return Err(Errors::NonExistantError(
                    "Unable to find key in tree.".to_string(),
                ));
            }
            node.try_sync()?;

            node = node_wrapper.data.read()?;

            if node.size == NodeSize::One {
                if parent.is_some() {
                    let mut parent_mut = parent.as_ref().unwrap().data.write()?;
                    parent_mut.remove(key[level]);
                } else {
                    self.root.swap(NodePtr::sentinel_node());
                    self.count.swap(0, Ordering::SeqCst);
                }

                self.remove_node(node_ptr);

                return Ok(node.leaf_data_ref().value.clone());
            }

            next_node = node.get_child(key[level]);
            if next_node == NodePtr::sentinel_node() {
                return Err(Errors::NonExistantError(
                    "Unable to find key in tree.".to_string(),
                ));
            }

            level = level + 1;
            node_ptr = next_node;
            parent = Some(node_wrapper);
        }
    }
    fn insert_internal(
        &self,
        key: &[u8],
        value: T,
        mut parent: Option<NodeWrapper<T>>,
    ) -> Result<(), Errors> {
        let mut node_ptr = self.root.load();

        if node_ptr == NodePtr::sentinel_node() {
            let ptr = self.new_leaf(key, value);
            self.root.swap(ptr);
            return Ok(());
        }

        let mut level = 0usize;
        let mut next_node: NodePtr;

        loop {
            let node_wrapper = self.cache.get(&(node_ptr.id as u64));
            let read_guard = node_wrapper.data.read()?;
            let mut node = read_guard;

            let overlap = self.overlapping_prefix_len(&node, key);
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

            if node.size == NodeSize::One {
                if parent.is_some() {
                    parent.as_ref().unwrap().data.write()?;
                }
                let new_node = self.new_node(key, NodeSize::Four);
                let mut_node = node_wrapper.data.write()?;
                let new_node = self.fetch_node_wrapper(new_node);
                let mut new_mut_node = new_node.data.write()?;
                let key2 = mut_node.leaf_data_ref().key.clone();

                let mut i = level;
                loop {
                    if key[i] != key2[i] {
                        break;
                    }

                    new_mut_node.internal_data_mut().prefix[i - level] = key[i];
                    level = level + new_mut_node.internal_data_ref().prefix.len();

                    i += 1;
                }

                let new_leaf = self.new_leaf(key, value);
                new_mut_node.insert(key[level], new_leaf);
                new_mut_node.insert(key2[level], node_ptr);
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

            level = level + 1;
            node_ptr = next_node;
            parent = Some(node_wrapper);
        }
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

    fn fetch_node_wrapper(&self, ptr: NodePtr) -> NodeWrapper<T> {
        self.cache.as_ref().get(&(ptr.id as u64))
    }

    pub fn get(&self, key: &[u8]) -> Result<(), Errors> {
        let backoff = Backoff::new();
        loop {
            match self.get_internal(key) {
                Ok(_) => return Ok(()),
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
    fn get_internal(&self, key: &[u8]) -> Result<T, Errors> {
        let mut node_ptr = self.root.load();

        if node_ptr == NodePtr::sentinel_node() {
            return Err(Errors::EmptyTreeError);
        }

        let mut level = 0usize;
        let mut next_node: NodePtr;

        loop {
            let node_wrapper = self.cache.get(&(node_ptr.id as u64));
            let read_guard = node_wrapper.data.read()?;
            let mut node = read_guard;

            // Implement insertion starting with finding the nextNode if the prefix matches the current node for the given level.
            let overlap = self.overlapping_prefix_len(&node, key);
            if overlap != node.internal_data_ref().prefix.len() {
                return Err(Errors::NonExistantError(
                    "Unable to find key in tree.".to_string(),
                ));
            }
            node.try_sync()?;

            node = node_wrapper.data.read()?;

            if node.size == NodeSize::One {
                return Ok(node.leaf_data_ref().value.clone());
            }

            next_node = node.get_child(key[level]);
            if next_node == NodePtr::sentinel_node() {
                return Err(Errors::NonExistantError(
                    "Unable to find key in tree.".to_string(),
                ));
            }

            level = level + 1;
            node_ptr = next_node;
        }
    }

    fn overlapping_prefix_len(&self, node: &ArtNode<T>, key: &[u8]) -> usize {
        let mut count = 0;
        for i in 0..core::cmp::min(node.internal_data_ref().prefix.len(), key.len()) {
            if node.internal_data_ref().prefix[i] == key[i] {
                count += 1;
            } else {
                break;
            }
        }

        count
    }
}

pub struct GDart<T: 'static + Encode + Decode> {
    current_generation: Dart<T>,
    past_generations: Vec<Dart<T>>,
}
