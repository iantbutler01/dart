use crate::locking::*;
use bincode::{config, Decode, Encode};
use marble::Marble;
use moka::sync::SegmentedCache;
use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::MutexGuard;
use std::sync::TryLockError;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use uuid::Uuid;

pub(crate) const EMPTY_SENTINEL_IDX: u16 = 256;
pub(crate) const EMPTY_POINTER_ID: u64 = 0;

#[derive(Encode, Decode, PartialEq, Debug, Copy, Clone)]
pub(crate) enum NodeSize {
    One = 1,
    Four = 4,
    Sixteen = 16,
    FourtyEight = 48,
    TwoFiftySix = 256,
}
#[derive(Debug)]
pub(crate) struct NodeWrapper<T: 'static> {
    pub data: Arc<OptimisticLockCoupling<ArtNode<T>>>,
}

impl<T> NodeWrapper<T> {
    pub(crate) fn new(data: ArtNode<T>) -> Self {
        Self {
            data: Arc::new(OptimisticLockCoupling::new(data)),
        }
    }
}

impl<T> Clone for NodeWrapper<T> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
        }
    }
}

unsafe impl<T> Send for NodeWrapper<T> {}
unsafe impl<T> Sync for NodeWrapper<T> {}

#[derive(Encode, Decode, Clone)]
pub(crate) enum OpType {
    Upsert,
    Get,
    Remove,
}

#[derive(Encode, Decode, Clone)]
pub(crate) struct Op<T> {
    pub(crate) optype: OpType,
    pub(crate) value: Option<T>,
    pub(crate) key: Vec<u8>,
}

impl<T: Encode + Decode> Op<T> {
    pub(crate) fn new(optype: OpType, key: Vec<u8>, value: Option<T>) -> Self {
        Self { optype, key, value }
    }
}

pub struct NodeCacheConfig {
    pub max_lru_cap: u64,
    pub num_lru_segments: usize,
    pub disk_path: String,
}

impl NodeCacheConfig {
    pub fn new(max_lru_cap: u64, num_lru_segments: usize, disk_path: String) -> Self {
        Self {
            max_lru_cap,
            num_lru_segments,
            disk_path,
        }
    }
}

impl Default for NodeCacheConfig {
    fn default() -> Self {
        let uuid = Uuid::new_v4();
        Self {
            max_lru_cap: 1000,
            num_lru_segments: 5,
            disk_path: "~/.dart/".to_owned() + &uuid.to_string(),
        }
    }
}

pub(crate) struct NodeCache<T: 'static> {
    pub(crate) lru: Arc<SegmentedCache<u64, NodeWrapper<T>>>,
    pub(crate) config: NodeCacheConfig,
    write_cache: Arc<Mutex<VecDeque<(u64, Option<Vec<u8>>)>>>,
    disk: Marble,
    thread_exit_signal: Arc<AtomicBool>,
    threads: [Option<JoinHandle<()>>; 1],
}

impl<T: 'static> Drop for NodeCache<T> {
    fn drop(&mut self) {
        self.thread_exit_signal
            .store(true, std::sync::atomic::Ordering::SeqCst);

        for thread in self.threads.iter_mut() {
            thread
                .take()
                .expect("Expected thread handle.")
                .join()
                .expect("Expected thread join not to fail.");
        }
    }
}

impl<T: 'static + Encode + Decode + Clone> Default for NodeCache<T> {
    fn default() -> Self {
        NodeCache::<T>::new(NodeCacheConfig::default())
    }
}

impl<T: 'static + Encode + Decode> NodeCache<T>
where
    T: Clone,
{
    pub(crate) fn new(config: NodeCacheConfig) -> Self {
        let exit_signal = Arc::new(AtomicBool::new(false));
        let signal_clone2 = exit_signal.clone();

        let write_cache = Arc::new(Mutex::new(
            VecDeque::<(u64, Option<Vec<u8>>)>::with_capacity(1000),
        ));
        let wc_ref = write_cache.clone();

        let disk = marble::open(config.disk_path.clone()).unwrap();
        let disk_ref = disk.clone();

        let write_handle = thread::spawn(move || {
            let disk = disk_ref;
            let exit_signal = signal_clone2;
            let mut count = 0;
            loop {
                let wc_lock_res = wc_ref.try_lock();

                match wc_lock_res {
                    Ok(mut wc_lock) => {
                        let cache_empty = wc_lock.is_empty();

                        if !cache_empty {
                            disk.write_batch(wc_lock.iter().cloned().rev().collect::<Vec<(
                                u64,
                                Option<Vec<u8>>,
                            )>>(
                            ))
                            .expect("Expect write to disk to succeed.");
                            wc_lock.clear();

                            count += 1;
                        }
                        drop(wc_lock);
                    }
                    Err(TryLockError::WouldBlock) => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    _ => {
                        panic!("Stuff borked.");
                    }
                }

                if count >= 10000 {
                    count = 0;
                    disk.maintenance()
                        .expect("Expected disk maintenence to complete.");
                }

                if exit_signal.load(std::sync::atomic::Ordering::SeqCst) {
                    break;
                }

                thread::sleep(Duration::from_millis(10));
            }
        });

        let lru = SegmentedCache::builder(config.num_lru_segments)
            .max_capacity(config.max_lru_cap)
            // .eviction_listener(eviction_listener)
            .build();

        Self {
            lru: Arc::new(lru),
            write_cache,
            disk,
            config,
            threads: [Some(write_handle)],
            thread_exit_signal: exit_signal.clone(),
        }
    }

    pub(crate) fn write_disk(
        &self,
        id: u64,
        node: Option<ArtNode<T>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let encoded = node.map(|x| {
            bincode::encode_to_vec(x, config::standard())
                .expect("Expected encoding for disk write to succeed.")
        });

        self.write_cache
            .lock()
            .expect("Expected to acquire lock.")
            .push_front((id, encoded));

        Ok(())
    }

    pub(crate) fn disk_maintenance(&self) -> Result<usize, std::io::Error> {
        self.disk.maintenance()
    }

    pub(crate) fn get(&self, id: &u64) -> Option<NodeWrapper<T>> {
        self.get_internal(id, true)
    }

    fn get_internal(&self, id: &u64, insert_lru: bool) -> Option<NodeWrapper<T>> {
        let wc_lock = self.write_cache.lock().unwrap();
        let wrapper_opt = self.lru.get(id);

        if wrapper_opt.is_none() {
            let wrapper = self.load(id.clone(), wc_lock);

            if wrapper.is_some() {
                let wrapper = wrapper.unwrap();
                if !self.lru.contains_key(id) && insert_lru {
                    self.lru.insert(*id, wrapper.clone());
                }
                return Some(wrapper);
            } else {
                return None;
            }
        } else {
            drop(wc_lock);
            return wrapper_opt;
        }
    }

    pub(crate) fn get_node_raw(&self, id: &u64) -> Option<ArtNode<T>> {
        let wrapper = self.get_internal(id, false);

        wrapper.map(|w| w.data.write().unwrap().clone())
    }

    fn load(
        &self,
        id: u64,
        wc_mutex: MutexGuard<'_, VecDeque<(u64, Option<Vec<u8>>)>>,
    ) -> Option<NodeWrapper<T>> {
        // println!("ID: {:?}", id);
        let ele = wc_mutex.iter().find(|(wc_id, _)| *wc_id == id).clone();

        let bytes_opt = match ele {
            Some((_, node_opt)) => {
                if node_opt.is_some() {
                    // println!("AM BYTES");
                    Some(Vec::into_boxed_slice(node_opt.as_ref().unwrap().clone()))
                } else {
                    println!("AM NOT");
                    None
                }
            }
            None => self.disk.read(id).expect("Expected to read node"),
        };
        drop(wc_mutex);

        bytes_opt.map(|bytes| {
            let (node, _): (ArtNode<T>, _) = bincode::decode_from_slice(&bytes, config::standard())
                .expect("Expected bincode deserialization to succeed!");

            NodeWrapper::new(node)
        })
    }

    pub(crate) fn remove(&self, id: u64) -> Option<NodeWrapper<T>> {
        self.lru.remove(&id)
    }

    pub(crate) fn insert(&self, id: u64, value: NodeWrapper<T>) {
        self.lru.insert(id, value)
    }
}

#[derive(Encode, Decode, Debug, Clone)]
pub(crate) struct InternalData {
    pub(crate) children: Vec<NodePtr>,
    pub(crate) prefix: Vec<u8>,
    pub(crate) idx: Option<Vec<u16>>,
    pub(crate) count: usize,
    pub(crate) next_pos: u8,
    pub(crate) terminal: NodePtr,
}

#[derive(Encode, Decode, Debug, Clone)]
pub(crate) struct LeafData<T> {
    pub(crate) value: T,
    pub(crate) key: Vec<u8>,
}

#[derive(Encode, Decode, Debug, Clone)]
pub(crate) enum NodeData<T> {
    Leaf(LeafData<T>),
    Internal(InternalData),
}

#[derive(Encode, Decode, Debug, Clone)]
pub struct ArtNode<T> {
    pub(crate) size: NodeSize,
    pub(crate) id: u64,
    pub(crate) generation: u64,
    data: NodeData<T>,
}

impl<T: Clone> ArtNode<T> {
    pub(crate) fn new(prefix: &[u8], size: NodeSize, id: u64, generation: u64) -> Self {
        let mut node = Self {
            data: NodeData::Internal(InternalData {
                prefix: prefix.into(),
                idx: Some(vec![EMPTY_SENTINEL_IDX; NodeSize::Four as usize]),
                children: vec![NodePtr::sentinel_node(); NodeSize::Four as usize],
                next_pos: 0,
                count: 0,
                terminal: NodePtr::sentinel_node(),
            }),
            id,
            generation,
            size: NodeSize::Four,
        };

        loop {
            if node.size == size {
                return node;
            } else {
                node.expand();
            }
        }
    }

    pub(crate) fn new_leaf(key: Vec<u8>, value: T, id: u64, generation: u64) -> Self {
        Self {
            id,
            generation,
            size: NodeSize::One,
            data: NodeData::Leaf(LeafData { key, value }),
        }
    }

    pub(crate) fn internal_data_ref(&self) -> &InternalData {
        let NodeData::Internal(ref data) = self.data else {
            panic!("Leaf node!");
        };

        data
    }

    pub(crate) fn internal_data_mut(&mut self) -> &mut InternalData {
        let NodeData::Internal(ref mut data) = self.data else {
            panic!("Leaf node!");
        };

        data
    }

    pub(crate) fn leaf_data_mut(&mut self) -> &mut LeafData<T> {
        let NodeData::Leaf(ref mut data) = self.data else {
            panic!("Internal node!");
        };

        data
    }

    pub(crate) fn leaf_data_ref(&self) -> &LeafData<T> {
        let NodeData::Leaf(ref data) = self.data else {
            panic!("Internal node!");
        };

        data
    }

    pub(crate) fn expand(&mut self) -> Option<(Vec<u8>, T)> {
        match self.newsize(true) {
            NodeSize::TwoFiftySix => {
                self.expand_256();
                None
            }
            NodeSize::FourtyEight => {
                self.expand_48();
                None
            }
            NodeSize::Sixteen => {
                let NodeData::Internal(ref mut data) = self.data else {
                    panic!("Leaf node!");
                };

                let mut new_idx = vec![EMPTY_SENTINEL_IDX; 16];
                let mut new_child = vec![NodePtr::sentinel_node(); 16];

                for (idx, key) in data.idx.as_ref().unwrap().iter().enumerate() {
                    if *key != EMPTY_SENTINEL_IDX {
                        new_idx[idx] = *key;
                        new_child[idx] = data.children[idx]
                    }
                }

                self.size = NodeSize::Sixteen;
                data.idx = Some(new_idx);
                data.children = new_child;

                None
            }
            NodeSize::Four => {
                let data: NodeData<T> = NodeData::Internal(InternalData {
                    prefix: vec![],
                    idx: Some(vec![EMPTY_SENTINEL_IDX; NodeSize::Four as usize]),
                    children: vec![NodePtr::sentinel_node(); NodeSize::Four as usize],
                    next_pos: 0,
                    count: 0,
                    terminal: NodePtr::sentinel_node(),
                });

                let mut ret = None;

                if self.is_leaf() {
                    let leaf_key = self.leaf_data_ref().key.clone();
                    let value = self.leaf_data_ref().value.clone();

                    ret = Some((leaf_key, value))
                }

                self.data = data;
                self.size = NodeSize::Four;

                ret
            }
            _ => panic!("New size should never be less than 4 on expand."),
        }
    }

    pub(crate) fn copy_internal_contents_to(
        &mut self,
        node: &mut OptimisticLockCouplingWriteGuard<ArtNode<T>>,
    ) -> InternalData {
        let original_data = node.internal_data_mut().clone();
        let node_data_mut = self.internal_data_mut();

        let data: NodeData<T> = NodeData::Internal(InternalData {
            prefix: node_data_mut.prefix.clone(),
            idx: node_data_mut.idx.clone(),
            children: node_data_mut.children.clone(),
            next_pos: node_data_mut.next_pos,
            count: node_data_mut.count,
            terminal: node_data_mut.terminal,
        });

        node.data = data;
        node.size = self.size;
        original_data
    }

    pub(crate) fn shrink(&mut self) {
        match self.newsize(false) {
            NodeSize::FourtyEight => self.shrink_48(),
            NodeSize::Sixteen => self.shrink_16(),
            NodeSize::One => self.shrink_1(),
            size => {
                let data = self.internal_data_mut();

                let idx_mut = data.idx.as_mut().unwrap();
                idx_mut.shrink_to(NodeSize::Four as usize);

                data.children.shrink_to(NodeSize::Four as usize);
                self.size = size;
            }
        }
    }

    pub(crate) fn is_full(&self) -> bool {
        if self.size == NodeSize::One {
            return true;
        }

        let data = self.internal_data_ref();
        data.count == (self.size as usize)
    }

    pub(crate) fn should_shrink(&self) -> bool {
        let downsize = self.newsize(false);

        let NodeData::Internal(ref data) = self.data else {
            panic!("Leaf node!");
        };

        data.count <= downsize as usize
    }

    fn newsize(&self, updown: bool) -> NodeSize {
        let mut old_size = self.size as isize;
        if old_size == 48 {
            old_size = 64
        }

        let new_size = if updown { old_size * 4 } else { old_size / 4 };

        if new_size == 64 {
            return NodeSize::FourtyEight;
        } else {
            return match new_size {
                4 => NodeSize::Four,
                16 => NodeSize::Sixteen,
                256 => NodeSize::TwoFiftySix,
                _ => panic!("Expected match to have already been made in size conversion."),
            };
        }
    }

    fn shrink_1(&mut self) {
        //TODO deal with merging for nodes that shink to 1.
    }

    fn shrink_16(&mut self) {
        let NodeData::Internal(ref mut data) = self.data else {
            panic!("Leaf node!");
        };
        let idx_mut = data.idx.as_mut().unwrap();
        idx_mut.shrink_to(NodeSize::Sixteen as usize);
        data.children.shrink_to(NodeSize::Sixteen as usize);

        let mut intermediate = data
            .children
            .iter()
            .enumerate()
            .map(|(x, y)| (x, y))
            .collect::<Vec<_>>();

        intermediate.sort_unstable_by_key(|(idx, _)| idx_mut[*idx]);

        data.children = intermediate
            .iter()
            .cloned()
            .map(|(_, ele)| *ele)
            .collect::<Vec<_>>();

        idx_mut.sort_unstable();
    }

    fn shrink_48(&mut self) {
        let NodeData::Internal(ref mut data) = self.data else {
            panic!("Leaf node!");
        };
        let mut new_idx = vec![EMPTY_SENTINEL_IDX; 256];
        let mut new_children = vec![NodePtr::sentinel_node(); NodeSize::FourtyEight as usize]; // Vec::<NodePtr>::with_capacity(48);
        let mut pos = 0;

        for (key, value) in data.children.iter().enumerate() {
            if *value != NodePtr::sentinel_node() {
                new_idx[key] = pos;

                new_children[pos as usize] = *value;
                pos += 1;
            }
        }

        data.next_pos = pos as u8;

        data.idx = Some(new_idx);
        data.children = new_children;
        self.size = NodeSize::FourtyEight;
    }

    pub(crate) fn get_child(&self, id: u8) -> NodePtr {
        let NodeData::Internal(ref data) = self.data else {
            panic!("Leaf node!");
        };
        match self.size {
            NodeSize::TwoFiftySix => data.children[id as usize],
            NodeSize::FourtyEight => {
                let pos = data.idx.as_ref().unwrap()[id as usize];

                if pos >= NodeSize::FourtyEight as u16 {
                    NodePtr::sentinel_node()
                } else {
                    data.children[pos as usize]
                }
            }
            NodeSize::One => {
                panic!("Leaf nodes do not have children.")
            }
            _ => {
                let pos_opt = data
                    .idx
                    .as_ref()
                    .unwrap()
                    .iter()
                    .position(|x| *x == id.into());

                if pos_opt.is_none() {
                    NodePtr::sentinel_node()
                } else {
                    data.children[pos_opt.unwrap()]
                }
            }
        }
    }

    pub(crate) fn is_leaf(&self) -> bool {
        self.size == NodeSize::One
    }

    fn expand_48(&mut self) {
        let NodeData::Internal(ref mut data) = self.data else {
            panic!("Leaf node!");
        };
        let mut new_idx = vec![EMPTY_SENTINEL_IDX; 256];
        let mut new_children = vec![NodePtr::sentinel_node(); NodeSize::FourtyEight as usize]; // Vec::<NodePtr>::with_capacity(48);
        let mut pos = 0;

        for key in data.idx.as_ref().unwrap().iter() {
            if *key != EMPTY_SENTINEL_IDX {
                new_idx[*key as usize] = pos;

                let pos_opt = data.idx.as_ref().unwrap().iter().position(|x| *x == *key);

                let child = if pos_opt.is_none() {
                    NodePtr::sentinel_node()
                } else {
                    data.children[pos_opt.unwrap()]
                };
                new_children[pos as usize] = child;
                pos += 1;
            }
        }

        data.next_pos = pos as u8;

        data.idx = Some(new_idx);
        data.children = new_children;
        self.size = NodeSize::FourtyEight;
    }

    fn expand_256(&mut self) {
        if self.size != NodeSize::FourtyEight {
            panic!("Node should only be flattened if converting from 48 to 256.");
        }
        let NodeData::Internal(ref mut data) = self.data else {
            panic!("Leaf node!");
        };

        let mut new_vec = vec![NodePtr::sentinel_node(); NodeSize::TwoFiftySix as usize];

        for (key, pos) in data.idx.as_ref().unwrap().iter().enumerate() {
            if *pos != EMPTY_SENTINEL_IDX {
                let child = data.children[*pos as usize];

                new_vec[key] = child;
            };
        }

        data.idx = None;
        data.children = new_vec;
        self.size = NodeSize::TwoFiftySix;
    }

    pub(crate) fn remove(&mut self, key: u8) -> NodePtr {
        if self.size == NodeSize::One {
            panic!("Tried removing from a Leaf!");
        }
        let (ptr, _size) = match self.size {
            NodeSize::TwoFiftySix => self.remove256(key),
            NodeSize::FourtyEight => self.remove48(key),
            NodeSize::One => panic!("Trying to remove from a leaf!"),
            _ => self.remove_sleq16(key),
        };

        ptr
    }

    fn remove_sleq16(&mut self, key: u8) -> (NodePtr, usize) {
        let NodeData::Internal(ref mut data) = self.data else {
            panic!("Leaf node!");
        };

        let pos = data
            .idx
            .as_ref()
            .unwrap()
            .iter()
            .position(|x| *x == key.into())
            .unwrap();

        data.idx.as_mut().unwrap()[pos] = 0;
        let old = data.children[pos];
        data.children[pos] = NodePtr::sentinel_node();

        data.next_pos = pos as u8;
        data.count -= 1;

        (old, data.count)
    }

    fn remove48(&mut self, key: u8) -> (NodePtr, usize) {
        let NodeData::Internal(ref mut data) = self.data else {
            panic!("Leaf node!");
        };
        let pos = data.idx.as_ref().unwrap()[key as usize];

        let old = data.children[pos as usize];
        data.children[pos as usize] = NodePtr::sentinel_node();

        data.next_pos = pos as u8;
        data.count -= 1;

        (old, data.count)
    }

    fn remove256(&mut self, key: u8) -> (NodePtr, usize) {
        let NodeData::Internal(ref mut data) = self.data else {
            panic!("Leaf node!");
        };

        let old = data.children[key as usize];
        data.children[key as usize] = NodePtr::sentinel_node();

        data.count -= 1;

        (old, data.count)
    }

    pub(crate) fn insert(&mut self, key: u8, ptr: NodePtr) {
        match self.size {
            NodeSize::Four => self._insert(key, ptr),
            NodeSize::Sixteen => self._insert(key, ptr),
            NodeSize::FourtyEight => self.insert48(key, ptr),
            NodeSize::TwoFiftySix => self.insert256(key, ptr),
            _ => (),
        }
        let NodeData::Internal(ref mut data) = self.data else {
            panic!("Leaf node!");
        };

        data.count += 1;
    }

    fn insert48(&mut self, key: u8, ptr: NodePtr) {
        let NodeData::Internal(ref mut data) = self.data else {
            panic!("Leaf node!");
        };

        let mut pos = data.next_pos;

        loop {
            if data.children[data.next_pos as usize] != NodePtr::sentinel_node() {
                pos += 1;
            } else {
                data.idx.as_mut().unwrap()[key as usize] = pos as u16;
                data.children[pos as usize] = ptr;
                break;
            }
        }

        data.next_pos = pos + 1;
    }

    fn _insert(&mut self, key: u8, ptr: NodePtr) {
        let NodeData::Internal(ref mut data) = self.data else {
            panic!("Leaf node!");
        };
        let idx_mut = data.idx.as_mut().unwrap();
        let mut pos: usize = 0;

        while pos < data.count {
            if idx_mut[pos] != EMPTY_SENTINEL_IDX {
                pos += 1;
            } else {
                break;
            }
        }

        idx_mut[pos] = key as u16;
        data.children[pos] = ptr;
    }

    fn insert256(&mut self, key: u8, ptr: NodePtr) {
        let NodeData::Internal(ref mut data) = self.data else {
            panic!("Leaf node!");
        };
        data.children[key as usize] = ptr
    }
}

#[derive(Encode, Decode, Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct NodePtr {
    pub(crate) id: u64,
    pub(crate) generation: u64,
}

impl NodePtr {
    pub(crate) fn sentinel_node() -> NodePtr {
        Self {
            id: EMPTY_POINTER_ID,
            generation: EMPTY_POINTER_ID,
        }
    }
}
