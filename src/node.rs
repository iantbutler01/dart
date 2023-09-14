use crate::locking::*;
use bincode::{config, Decode, Encode};
use marble::Marble;
use moka::sync::SegmentedCache;
use std::sync::Arc;

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

pub(crate) struct NodeCache<T: 'static> {
    lru: Arc<SegmentedCache<u64, NodeWrapper<T>>>,
    disk: Arc<Marble>,
}

impl<T: 'static + Encode + Decode> NodeCache<T> {
    pub fn new(max_lru_cap: u64, num_lru_segments: usize, disk_location: String) -> Self {
        Self {
            lru: Arc::new(SegmentedCache::new(max_lru_cap, num_lru_segments)),
            disk: Arc::new(marble::open(disk_location).unwrap()),
        }
    }

    pub fn get(&self, id: &u64) -> NodeWrapper<T> {
        self.lru.get(id).unwrap_or_else(|| self.load(id.clone()))
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

    pub(crate) fn remove(&self, id: u64) -> Option<NodeWrapper<T>> {
        self.lru.remove(&id)
    }

    pub(crate) fn insert(&self, id: u64, value: NodeWrapper<T>) {
        self.lru.insert(id, value)
    }
}

#[derive(Encode, Decode, Debug)]
pub(crate) struct InternalData {
    pub(crate) children: Vec<NodePtr>,
    pub(crate) prefix: Vec<u8>,
    pub(crate) idx: Option<Vec<u16>>,
    pub(crate) count: usize,
    pub(crate) next_pos: u8,
    pub(crate) terminal: NodePtr,
}

#[derive(Encode, Decode, Debug)]
pub(crate) struct LeafData<T> {
    pub(crate) value: T,
    pub(crate) key: Vec<u8>,
}

#[derive(Encode, Decode, Debug)]
pub(crate) enum NodeData<T> {
    Leaf(LeafData<T>),
    Internal(InternalData),
}

#[derive(Encode, Decode, Debug)]
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
                idx: Some(vec![0; NodeSize::Four as usize]),
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

    // pub(crate) fn set_prefix(&mut self, p: &[u8]) {
    //     let NodeData::Internal(ref mut data) = self.data else {
    //         panic!("Leaf node!");
    //     };
    //     data.prefix = p.into();
    // }

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
        println!("We callin, we ballin.");
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

        println!("{:?}", downsize);

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
        let mut count = 0;
        let mut new_idx = Vec::<u16>::with_capacity(NodeSize::TwoFiftySix as usize);
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
        let (ptr, size) = match self.size {
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
            if idx_mut[pos] < key as u16 {
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
