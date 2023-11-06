use crate::{errors::Errors, tree::Dart};
use bincode::{Decode, Encode};
use rayon::prelude::*;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::RwLock;
use std::thread::JoinHandle;

pub struct GDart<T: 'static + Encode + Decode>
where
    T: Send + Sync + Clone + Debug,
{
    current_generation: Arc<Dart<T>>,
    current_generation_number: AtomicU64,
    past_generations: Arc<RwLock<VecDeque<Arc<Dart<T>>>>>,
    max_single_generation_size: u64,
    max_single_generation_lru_size: u64,
    num_single_generation_lru_segments: usize,
    write_path: String,
    wal_path: String,
    merge_thread: Option<JoinHandle<()>>,
}

impl<T: 'static + Encode + Decode + Send + Sync + Clone + Debug> GDart<T> {
    pub fn new(
        max_single_generation_size: u64,
        max_single_generation_lru_size: u64,
        num_single_generation_lru_segments: usize,
        write_path: Option<String>,
    ) -> Self {
        let write_path = write_path.unwrap_or(".gdart".to_string());
        let first_generation_write_path = write_path.clone() + "/0";
        Self {
            max_single_generation_size,
            write_path: write_path.clone(),
            wal_path: write_path.clone(),
            past_generations: Arc::new(RwLock::new(VecDeque::<_>::with_capacity(100))),
            max_single_generation_lru_size,
            num_single_generation_lru_segments,
            current_generation_number: AtomicU64::new(0),
            current_generation: Arc::new(Dart::<T>::new(
                max_single_generation_lru_size,
                num_single_generation_lru_segments,
                first_generation_write_path.clone() + "/disk",
                first_generation_write_path.clone() + "/wal",
            )),
            merge_thread: None,
        }
    }

    fn merge_past_generations(&mut self) {
        let past_gen_arc = self.past_generations.clone();

        if self.merge_thread.is_some() {
            self.merge_thread
                .take()
                .unwrap()
                .join()
                .expect("Expected to join merge thread.");
        }

        let merged_gen = self.new_generation();
        let handle = std::thread::spawn(move || {
            let past_generations_reader = past_gen_arc
                .read()
                .expect("Expected to acquire past generation vector reader.");

            if past_generations_reader.len() >= 5 {
                drop(past_generations_reader);

                let mut past_generations_writer =
                    past_gen_arc.write().expect("Expected to acquire writer.");

                for gen in past_generations_writer.iter() {
                    for node_opt in gen.iter_nodes() {
                        match node_opt {
                            Some(node) => {
                                if node.is_leaf() {
                                    merged_gen
                                        .upsert(
                                            node.leaf_data_ref().key.as_slice(),
                                            node.leaf_data_ref().value.clone(),
                                        )
                                        .expect(
                                            "Expected insert into a merged generation to succeed.",
                                        );
                                } else {
                                    if node.internal_data_ref().terminal
                                        != crate::node::NodePtr::sentinel_node()
                                    {
                                        let cache = gen
                                            .cache
                                            .lock()
                                            .expect("Expected to lock past gen node cache.");

                                        let node = cache
                                            .get_node_raw(&node.internal_data_ref().terminal.id)
                                            .expect("Expected terminal to exist.");

                                        merged_gen
                                        .upsert(
                                            node.leaf_data_ref().key.as_slice(),
                                            node.leaf_data_ref().value.clone(),
                                        )
                                        .expect(
                                            "Expected insert into a merged generation to succeed.",
                                        );
                                    }
                                }
                            }
                            None => (),
                        }
                    }
                }

                while past_generations_writer.len() > 0 {
                    let gen = past_generations_writer.pop_back().unwrap();
                    gen.clean_up();
                }

                past_generations_writer.push_back(merged_gen);

                drop(past_generations_writer);
            } else {
                drop(past_generations_reader);
            };
        });

        self.merge_thread = Some(handle);
    }

    fn new_generation(&mut self) -> Arc<Dart<T>> {
        let next_gen_number = self
            .current_generation_number
            .fetch_add(1 as u64, Ordering::SeqCst)
            + 1;

        let new_gen = Dart::<T>::new(
            self.max_single_generation_lru_size,
            self.num_single_generation_lru_segments,
            self.write_path.clone() + "/" + next_gen_number.clone().to_string().as_str() + "/disk",
            self.wal_path.clone() + "/" + next_gen_number.to_string().as_str() + "/wal",
        );

        Arc::new(new_gen)
    }

    fn next_generation(&mut self) -> Arc<Dart<T>> {
        let mut past_gen_write = self
            .past_generations
            .write()
            .expect("Expected to acquire past generation vector write lock.");

        past_gen_write.push_front(self.current_generation.clone());
        drop(past_gen_write);

        self.merge_past_generations();

        self.current_generation = self.new_generation();
        self.current_generation.clone()
    }

    pub fn upsert(&mut self, key: &[u8], value: T) -> Result<Option<T>, Errors> {
        let gen_count = self
            .current_generation
            .count
            .load(std::sync::atomic::Ordering::SeqCst);

        if gen_count >= self.max_single_generation_size {
            self.next_generation();
        }

        self.current_generation.upsert(key, value)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<T>, Errors> {
        let initial_res = self
            .current_generation
            .get(key)
            .expect("Expected to execute get without error.");

        let final_res = match initial_res {
            Some(v) => Some(v),
            None => {
                let mut results = Vec::new();
                self.past_generations
                    .read()
                    .expect("Expected to acquire reader for past generations.")
                    .par_iter()
                    .map(|gen| {
                        gen.get(key)
                            .expect("Expected get to complete without error.")
                    })
                    .collect_into_vec(&mut results);

                results
                    .iter()
                    .cloned()
                    .find(|i| i.is_some())
                    .map(|opt| opt.unwrap())
            }
        };

        Ok(final_res)
    }

    pub fn remove(&self, key: &[u8]) -> Result<Option<T>, Errors> {
        let initial_res = self
            .current_generation
            .remove(key)
            .expect("Expected to execute get without error.");

        let mut results = Vec::new();
        self.past_generations
            .read()
            .expect("Expected to acquire reader for past generation vector.")
            .par_iter()
            .map(|gen| {
                gen.remove(key)
                    .expect("Expected get to complete without error.")
            })
            .collect_into_vec(&mut results);

        let final_res = results
            .iter()
            .cloned()
            .find(|i| i.is_some())
            .map(|opt| opt.unwrap());

        let res = match initial_res {
            Some(_) => initial_res,
            _ => final_res,
        };

        Ok(res)
    }
}

impl<T: Send + Sync + Clone + Encode + Decode + Debug> Drop for GDart<T> {
    fn drop(&mut self) {
        if self.merge_thread.is_some() {
            self.merge_thread
                .take()
                .unwrap()
                .join()
                .expect("Expected to join merge thread.");
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{generational::GDart, utils::in_temp_dir};

    #[test]
    fn create_gdart() {
        in_temp_dir!(|path| {
            let gdart: GDart<u64> = GDart::new(100, 50, 5, Some(path));

            assert_eq!(
                gdart
                    .current_generation_number
                    .load(std::sync::atomic::Ordering::SeqCst),
                0
            )
        });
    }

    #[test]
    fn insert_words() {
        in_temp_dir!(|path: String| {
            let mut tree = GDart::<u64>::new(3, 10, 1, Some(path));

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

            tree.get("apple".as_bytes())
                .expect("Expected to get apple.");
            tree.get("appetizer".as_bytes())
                .expect("Expected to get appetizer.");
            tree.get("apply".as_bytes())
                .expect("Expected to get apply.");
            tree.get("apt".as_bytes()).expect("Expected to get apt.");
            tree.get("arrange".as_bytes())
                .expect("Expected to get arrange.");
            tree.get("art".as_bytes()).expect("Expected to get art.");
            tree.get("archaic".as_bytes())
                .expect("Expected to get archaic.");
            tree.get("arthropod".as_bytes())
                .expect("Expected to get arthropod.");
            tree.get("arc".as_bytes()).expect("Expected to get arc.");
            tree.get("bar".as_bytes()).expect("Expected to get bar.");
            tree.get("bark".as_bytes()).expect("Expected to get bark.");
            tree.get("bet".as_bytes()).expect("Expected to get bet.");
        })
    }

    #[test]
    fn insert_deep_and_wide() {
        in_temp_dir!(|path: String| {
            let mut tree = GDart::<u64>::new(5000, 1000, 5, Some(path.clone()));

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
    fn get_on_insert() {
        in_temp_dir!(|path: String| {
            let mut tree = GDart::<u64>::new(5000, 1000, 5, Some(path));

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
}
