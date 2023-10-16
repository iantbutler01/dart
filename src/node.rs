use crate::errors::Errors;
use crate::errors::OptimisticLockCouplingErrorType;
use crate::locking::*;
use bincode::{config, Decode, Encode};
use core::num;
use crossbeam::utils::Backoff;
use marble::Marble;
use moka::notification::RemovalCause;
use moka::sync::SegmentedCache;
use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::TryRecvError;
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle, Thread};
use std::time::Duration;

pub(crate) const EMPTY_SENTINEL_IDX: u16 = 256;
pub(crate) const EMPTY_POINTER_ID: usize = 0;

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

#[derive(Encode, Decode)]
pub(crate) enum OpType {
    Insert,
    Remove,
}

#[derive(Encode, Decode)]
pub(crate) struct Op<T> {
    pub(crate) optype: OpType,
    pub(crate) value: Option<T>,
    pub(crate) key: Vec<u8>,
}

impl<T: Encode + Decode> Op<T> {
    fn new(optype: OpType, key: Vec<u8>, value: Option<T>) -> Self {
        Self { optype, key, value }
    }
}

pub(crate) struct WriteAheadLog {
    count: usize,
    ops: Vec<(u64, Vec<u8>)>,
    flushat: usize,
    receiver: mpsc::Receiver<Vec<u8>>,
    disk: Marble,
}

impl WriteAheadLog {
    fn new(flushat: usize, path: String, receiver: mpsc::Receiver<Vec<u8>>) -> Self {
        Self {
            count: 0,
            ops: Vec::new(),
            flushat,
            receiver,
            disk: marble::open(path).expect("Expected WAL disk path to open without issue."),
        }
    }

    fn append(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        match self.receiver.try_recv() {
            Ok(op) => {
                self.ops.push((self.count as u64, op));

                self.count += 1;
            }
            Err(recv_error) => match recv_error {
                mpsc::TryRecvError::Empty => (),
                mpsc::TryRecvError::Disconnected => return Err(Box::new(recv_error)),
            },
        }

        Ok(())
    }

    fn flush(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.ops.len() >= self.flushat {
            let batch = self.ops.iter().map(|(id, op)| (*id as u64, Some(op)));
            self.disk.write_batch(batch)?;
            if self.count % 100 == 0 {
                self.disk.maintenance()?;
            }

            if self.count % 1000 == 0 {
                let range_iter = ((self.count - 1000)..self.count).into_iter();

                let batch: Vec<(u64, Option<Vec<u8>>)> =
                    range_iter.map(|i| (i as u64, None)).collect();

                self.disk.write_batch(batch)?;
            }
        }
        Ok(())
    }
}

pub(crate) struct NodeCache<T: 'static> {
    pub(crate) lru: Arc<Mutex<SegmentedCache<u64, NodeWrapper<T>>>>,
    log: mpsc::Sender<Vec<u8>>,
    write_cache: Arc<Mutex<VecDeque<(u64, Option<Vec<u8>>)>>>,
    disk: Marble,
    thread_exit_signal: Arc<AtomicBool>,
    threads: [Option<JoinHandle<()>>; 2],
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

impl<T: 'static + Encode + Decode> NodeCache<T>
where
    T: Clone,
{
    pub(crate) fn new(
        max_lru_cap: u64,
        num_lru_segments: usize,
        disk_path: String,
        wal_path: String,
    ) -> Self {
        let exit_signal = Arc::new(AtomicBool::new(false));
        let signal_clone = exit_signal.clone();
        let signal_clone2 = exit_signal.clone();

        let (tx, rx) = mpsc::channel();
        let wal_handle = thread::spawn(move || {
            let exit_signal = signal_clone;
            let mut wal = WriteAheadLog::new(100, wal_path, rx);
            loop {
                match wal.append() {
                    Ok(_) => (),
                    Err(e) => {
                        let recv_error = e.downcast_ref::<mpsc::TryRecvError>();

                        if recv_error.is_some() {
                            break;
                        }

                        panic!("Expected wal append to succeed!")
                    }
                }
                wal.flush().expect("Expected flushing WAL to complete.");

                if exit_signal.load(std::sync::atomic::Ordering::SeqCst) {
                    break;
                }
            }
        });
        let write_cache = Arc::new(Mutex::new(
            VecDeque::<(u64, Option<Vec<u8>>)>::with_capacity(1000),
        ));
        let wc_ref = write_cache.clone();

        let disk = marble::open(disk_path).unwrap();
        let disk_ref = disk.clone();

        let write_handle = thread::spawn(move || {
            let disk = disk_ref;
            let exit_signal = signal_clone2;
            let mut count = 0;
            loop {
                let mut wc_lock = wc_ref.lock().expect("Expected to acquire write lock");
                let cache_empty = wc_lock.is_empty();

                if !cache_empty {
                    disk.write_batch(
                        wc_lock
                            .iter()
                            .cloned()
                            .rev()
                            .collect::<Vec<(u64, Option<Vec<u8>>)>>(),
                    )
                    .expect("Expect write to disk to succeed.");
                    wc_lock.clear();

                    count += 1;
                }
                drop(wc_lock);

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

        let evl_write_cache_handle = write_cache.clone();

        let eviction_listener = move |k: Arc<u64>, v: NodeWrapper<T>, cause| {
            if cause == RemovalCause::Replaced {
                return;
            }
            let backoff = Backoff::new();
            loop {
                match v.data.write() {
                    Ok(lock) => {
                        let node = lock.clone();
                        // println!("WC ID: {:?} NODE: {:?}", k, node.id);
                        let option = Some(node);

                        let encoded = option.map(|x| {
                            bincode::encode_to_vec(x.clone(), config::standard())
                                .expect("Expected encoding for disk write to succeed.")
                        });
                        let mut cache_lock = evl_write_cache_handle.lock().unwrap();
                        cache_lock.push_front((*k, encoded));
                        break;
                    }
                    Err(insert_error) => match insert_error {
                        OptimisticLockCouplingErrorType::Blocked => {
                            break;
                        }
                        e => {
                            println!("E: {:?}", e);
                            backoff.spin();
                            continue;
                        }
                    },
                }
            }
        };

        let lru = SegmentedCache::builder(num_lru_segments)
            .max_capacity(max_lru_cap)
            .eviction_listener(eviction_listener)
            .build();

        Self {
            lru: Arc::new(Mutex::new(lru)),
            write_cache,
            log: tx,
            disk,
            threads: [Some(wal_handle), Some(write_handle)],
            thread_exit_signal: exit_signal.clone(),
        }
    }

    pub(crate) fn write_wal(&self, op: Op<T>) -> Result<(), Box<dyn std::error::Error>> {
        let encoded = bincode::encode_to_vec(op, config::standard())?;

        self.log.send(encoded)?;

        Ok(())
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

    pub(crate) fn get(&self, id: &u64) -> NodeWrapper<T> {
        self.lru
            .lock()
            .unwrap()
            .get(id)
            .unwrap_or_else(|| self.load(id.clone()))
    }

    fn load(&self, id: u64) -> NodeWrapper<T> {
        // println!("ID: {:?}", id);
        loop {
            let wc_rg_res = self.write_cache.try_lock();

            if wc_rg_res.is_err() {
                continue;
            }

            let wc_rg = wc_rg_res.unwrap();
            let ele = wc_rg.iter().find(|(wc_id, _)| *wc_id == id);

            let bytes_opt = match ele {
                Some((_, node_opt)) => {
                    if node_opt.is_some() {
                        // println!("AM BYTES");
                        Some(Vec::into_boxed_slice(node_opt.as_ref().unwrap().clone()))
                    } else {
                        println!("AM NOT");
                        self.disk.read(id).expect("Expected to read node")
                    }
                }
                None => self.disk.read(id).expect("Expected to read node"),
            };

            drop(wc_rg);

            let err = format!("Expected {:?} to exist on disk or write cache.", id);

            let bytes = bytes_opt.expect(&err);

            let (node, _): (ArtNode<T>, _) = bincode::decode_from_slice(&bytes, config::standard())
                .expect("Expected bincode deserialization to succeed!");

            let wrapper = NodeWrapper::new(node);
            self.lru.lock().unwrap().insert(id, wrapper.clone());
            return wrapper.clone();
        }
    }

    pub(crate) fn remove(&self, id: u64) -> Option<NodeWrapper<T>> {
        self.lru.lock().unwrap().remove(&id)
    }

    pub(crate) fn insert(&self, id: u64, value: NodeWrapper<T>) {
        self.lru.lock().unwrap().insert(id, value)
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
    pub(crate) id: usize,
    pub(crate) generation: usize,
    data: NodeData<T>,
}

impl<T> ArtNode<T> {
    pub(crate) fn new(prefix: &[u8], size: NodeSize, id: usize, generation: usize) -> Self {
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
                node.expand()
            }
        }
    }

    pub(crate) fn new_leaf(key: Vec<u8>, value: T, id: usize, generation: usize) -> Self {
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

    pub(crate) fn expand(&mut self) {
        match self.newsize(true) {
            NodeSize::TwoFiftySix => self.expand_256(),
            NodeSize::FourtyEight => self.expand_48(),
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
            }
            NodeSize::Four => {
                let NodeData::Internal(ref mut data) = self.data else {
                    panic!("Leaf node!");
                };

                let new_idx = vec![EMPTY_SENTINEL_IDX; 4];
                let new_child = vec![NodePtr::sentinel_node(); 4];

                self.size = NodeSize::Four;
                data.idx = Some(new_idx);
                data.children = new_child;
            }
            _ => panic!("Attempted to expand Leaf"),
        }
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
    pub(crate) id: usize,
    pub(crate) generation: usize,
}

impl NodePtr {
    pub(crate) fn sentinel_node() -> NodePtr {
        Self {
            id: EMPTY_POINTER_ID,
            generation: EMPTY_POINTER_ID,
        }
    }
}
