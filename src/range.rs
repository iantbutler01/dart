use crate::errors::Errors;
use crate::node::*;
use crate::tree::Dart;
use bincode::{Decode, Encode};
use crossbeam::atomic::AtomicCell;

struct RangeState {
    current_node: AtomicCell<NodePtr>,
    unseen: Vec<NodePtr>,
    stop: bool,
}

#[derive(Clone)]
pub struct ScanResult<T> {
    pub matches: Vec<(Vec<u8>, T)>,
}

pub struct Range<'a> {
    start: &'a [u8],
    end: &'a [u8],
    state: Option<RangeState>,
}

impl<'a> Range<'a> {
    pub fn new(start: &'a [u8], end: &'a [u8]) -> Self {
        Self {
            start,
            end,
            state: None,
        }
    }

    pub fn scan<T>(&mut self, tree: &Dart<T>) -> Result<ScanResult<T>, Errors>
    where
        T: Encode + Decode + Clone + Send,
    {
        let root = tree.root.load();
        let state = RangeState {
            current_node: AtomicCell::new(root),
            unseen: Vec::new(),
            stop: false,
        };

        self.state = Some(state);

        let mut result = ScanResult {
            matches: Vec::new(),
        };

        loop {
            if self.state.as_ref().unwrap().stop {
                break;
            }

            let node_ptr = self.state.as_mut().unwrap().current_node.load();
            let node_wrapper = tree.cache.lock().unwrap().get(&node_ptr.id);

            let node = node_wrapper.data.read()?;

            if node.is_leaf() {
                let leaf_data_ref = node.leaf_data_ref();
                if leaf_data_ref.key.as_slice() > self.start
                    && leaf_data_ref.key.as_slice() < self.end
                {
                    result
                        .matches
                        .push((leaf_data_ref.key.to_vec(), leaf_data_ref.value.clone()))
                }
            }

            if self.start <= node.internal_data_ref().prefix.as_slice()
                && node.internal_data_ref().prefix.as_slice() <= self.end
            {
                for child in node.internal_data_ref().children.iter() {
                    if child.id != EMPTY_POINTER_ID {
                        self.state.as_mut().unwrap().unseen.push(child.clone())
                    }
                }
            }

            if node.internal_data_ref().prefix.as_slice() > self.end {
                self.state.as_mut().unwrap().stop = true;
            }
        }

        Ok(result)
    }
}
