use crate::locking::*;
use bincode::{config, Decode, Encode};
use marble::Marble;
use moka::sync::SegmentedCache;
use std::sync::Arc;

pub(crate) const EMPTY_SENTINEL: u8 = 255;
pub(crate) const EMPTY_POINTER_ID: usize = 0;

#[derive(Encode, Decode, PartialEq, Debug, Copy, Clone)]
pub(crate) enum NodeSize {
    One = 1,
    Four = 4,
    Sixteen = 16,
    FourtyEight = 48,
    TwoFiftySix = 256,
}

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

pub(crate) struct NodeCache<T: 'static> {
    lru: Arc<SegmentedCache<u64, NodeWrapper<T>>>,
    disk: Arc<Marble>,
}

impl<T: 'static + Encode + Decode> NodeCache<T> {
    pub fn get(&self, id: &u64) -> NodeWrapper<T> {
        self.lru.get(id).unwrap_or(self.load(id.clone()))
    }

    fn load(&self, id: u64) -> NodeWrapper<T> {
        let node = self
            .disk
            .read(id)
            .and_then(|bytes_opt| {
                let bytes = bytes_opt.expect("Expected item to exist on disk!");

                let (node, _): (ArtNode<T>, _) =
                    bincode::decode_from_slice(&bytes, config::standard())
                        .expect("Expected bincode deserialization to succeed!");

                Ok(node)
            })
            .expect("Expected reading from disk to succeed!");

        NodeWrapper::new(node)
    }

    pub(crate) fn remove(&self, key: u64) -> Option<NodeWrapper<T>> {
        self.lru.remove(&key)
    }

    pub(crate) fn insert(&self, key: u64, value: NodeWrapper<T>) {
        self.lru.insert(key, value)
    }
}

#[derive(Encode, Decode)]
pub(crate) struct InternalData {
    pub(crate) children: Vec<NodePtr>,
    pub(crate) prefix: Vec<u8>,
    pub(crate) idx: Option<Vec<u8>>,
    pub(crate) count: usize,
    pub(crate) next_pos: u8,
}

#[derive(Encode, Decode)]
pub(crate) struct LeafData<T> {
    pub(crate) value: T,
    pub(crate) key: Vec<u8>,
}

#[derive(Encode, Decode)]
pub(crate) enum NodeData<T> {
    Leaf(LeafData<T>),
    Internal(InternalData),
}

#[derive(Encode, Decode)]
pub(crate) struct ArtNode<T> {
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
                idx: None,
                children: Vec::with_capacity(NodeSize::One as usize),
                next_pos: 0,
                count: 0,
            }),
            id,
            generation,
            size: NodeSize::One,
        };

        loop {
            if node.size == size {
                return node;
            } else {
                node.expand()
            }
        }
    }

    pub(crate) fn set_prefix(&mut self, p: &[u8]) {
        let NodeData::Internal(ref mut data) = self.data else {
            panic!("Leaf node!");
        };
        data.prefix = p.into();
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
            size => {
                let NodeData::Internal(ref mut data) = self.data else {
                    panic!("Leaf node!");
                };
                let idx_mut = data.idx.as_mut().unwrap();
                idx_mut.reserve_exact(size as usize - self.size as usize);

                data.children
                    .reserve_exact(size as usize - self.size as usize);
                self.size = size;
            }
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

    fn newsize(&mut self, updown: bool) -> NodeSize {
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
        let mut count = 0;
        let mut new_idx = Vec::<u8>::with_capacity(NodeSize::TwoFiftySix as usize);
        let mut new_children = Vec::<NodePtr>::with_capacity(NodeSize::TwoFiftySix as usize);

        for (key, node) in data.children.iter().enumerate() {
            if node.id != EMPTY_POINTER_ID {
                new_idx[key as usize] = count;
                new_children[count as usize] = *node;
                count += 1;
            }
        }

        data.idx = Some(new_idx);
        data.children = new_children;
    }

    pub(crate) fn get_child(&self, id: u8) -> NodePtr {
        let NodeData::Internal(ref data) = self.data else {
            panic!("Leaf node!");
        };
        match self.size {
            NodeSize::TwoFiftySix => data.children[id as usize],
            NodeSize::FourtyEight => {
                let pos = data.idx.as_ref().unwrap()[id as usize];
                data.children[pos as usize]
            }
            NodeSize::One => {
                panic!("Leaf nodes do not have children.")
            }
            _ => {
                let pos = data
                    .idx
                    .as_ref()
                    .unwrap()
                    .iter()
                    .position(|x| *x == id)
                    .unwrap();
                data.children[pos]
            }
        }
    }

    fn expand_48(&mut self) {
        let NodeData::Internal(ref mut data) = self.data else {
            panic!("Leaf node!");
        };
        let mut new_idx = Vec::<u8>::with_capacity(256);
        let mut new_children = Vec::<NodePtr>::with_capacity(48);
        let mut pos = 0;

        for (idx, key) in data.idx.as_ref().unwrap().iter().enumerate() {
            new_idx[*key as usize] = pos;
            new_children[pos as usize] = data.children[idx];
            pos += 1;
        }

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

        let mut new_vec = vec![NodePtr::sentinel_node(); 256];

        for (key, pos) in data.idx.as_ref().unwrap().iter().enumerate() {
            if *pos != EMPTY_SENTINEL {
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
        let (ptr, size) = match self.size {
            NodeSize::TwoFiftySix => self.remove256(key),
            NodeSize::FourtyEight => self.remove48(key),
            NodeSize::One => panic!("Trying to remove from a leaf!"),
            _ => self.remove_sleq16(key),
        };

        if size <= self.newsize(false) as usize {
            self.shrink()
        }

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
            .position(|x| *x == key)
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

        data.next_pos = pos;
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
        data.idx.as_mut().unwrap()[key as usize] = data.next_pos;
        data.children[data.next_pos as usize] = ptr;
    }

    fn _insert(&mut self, key: u8, ptr: NodePtr) {
        let NodeData::Internal(ref mut data) = self.data else {
            panic!("Leaf node!");
        };
        let idx_mut = data.idx.as_mut().unwrap();
        idx_mut.push(key);
        //This needs to be sorted, pick up on it tomorrow.
        let mut pos: usize = 0;

        while pos < data.count {
            if idx_mut[pos] < key {
                pos += 1;
                continue;
            } else {
                break;
            }
        }

        unsafe {
            std::ptr::copy(
                idx_mut.as_ptr().add(pos),
                idx_mut.as_mut_ptr().add(pos + 1),
                data.count - pos,
            );

            std::ptr::copy(
                data.children.as_ptr().add(pos),
                data.children.as_mut_ptr().add(pos + 1),
                data.count - pos,
            );
        }

        idx_mut[pos] = key;
        data.children[pos] = ptr;
    }

    fn insert256(&mut self, key: u8, ptr: NodePtr) {
        let NodeData::Internal(ref mut data) = self.data else {
            panic!("Leaf node!");
        };
        data.children[key as usize] = ptr
    }
}

#[derive(Encode, Decode, Clone, Copy, PartialEq, Eq)]
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
