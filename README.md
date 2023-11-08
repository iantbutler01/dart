<img src="https://github.com/iantbutler01/dart/assets/6426407/d811d567-982f-4645-b6b3-bc548dba4651" width="400" height="400">

### DART (Disk [backed] Adaptive Radix Tree)

A Rust implementation of the [Adaptive Radix Tree](https://db.in.tum.de/~leis/papers/ART.pdf) with [optimistic locking modification](https://db.in.tum.de/~leis/papers/artsync.pdf) for thread safe concurrency.

What makes DART unique is it serves operations from a configurable LRU cache and if the entry has been evicted will fall back to disk. This modification allows you to serve operations with a near constant memory footprint with a trade off for performance. This was developed with full text search in mind and in such cases there will undoutebdly be a set of common or hot paths through the tree. Hot paths will be served from the LRU while everything else will be fetched from disk.

This idea was inspired by a set of blog posts by [Julien Lemoine](https://www.linkedin.com/in/julienlemoine/) where he discussed the original indexing structure for Algolia, a well known search as a service company.

Further I wrote this to act as a drop in replacement for my naive inverted index implementation in my own Full Text Search Extension for postgresql [Quria](https://github.com/iantbutler01/Quria) which is still early and experimental and I have not yet completed the swap.

## Example

```rust
use dart::tree::{Dart, NodeCacheConfig};

//LRU Size, LRU Segments, Path for Disk Writes
let cache_config = NodeCacheConfig::new(100, 1, path.clone() + "/tree")
let tree = Dart::<u64>::new(cache_config);

tree.upsert(&[1, 2, 3, 4], 64)
let res = tree.get(&[1, 2, 3, 4]);

assert_eq!(res, Some(64));
tree.upsert(&[1, 2, 3, 4], 65)

let res = tree.get(&[1, 2, 3, 4]);
assert_eq!(res, Some(65));
let res = tree.get(&[1, 2, 3, 5]);
assert_eq!(res, None);

let res = tree.remove(&[1, 2, 3, 4]);
assert_eq!(res, Some(65))
let res = tree.remove(&[1, 2, 3, 4]);
assert_eq!(res, None)
```

## GDART

If all of the above wasn't enough I created a generational version of DART -- GDART which takes all of the above goodness and implements generations[0] on top of it. This allows you to keep the size of the trees small, keeping recent searches and upserts quick. Operations are also parallelized over generations so older content can be quickly found.

[0] A generational datastructure is basically a versioned datastructure, such that after some size cutoff a new version is created and the older data structure is appended to a history. Upserts are directed to the latest generation, and searches/removals are first done on the latest generation and then parallelized over historical generations, if nothing is found in the current generation. The benefit of this is by keeping smaller tree sizes you speed up operations on single generations, as well as the parallelizability of operations over multiple generations.

### Example

```rust
use dart::generational::{GDart, GDartConfig};

//Max generation size, LRU Size for a generation, LRU Segments, Path for all writes
let config = GDartConfig::new(5000, 1000, 5, Some(path));
let mut tree = GDart::<u64>::new(config);

tree.upsert(&[1, 2, 3, 4], 64)
let res = tree.get(&[1, 2, 3, 4]);

assert_eq!(res, Some(64));
tree.upsert(&[1, 2, 3, 4], 65)

let res = tree.get(&[1, 2, 3, 4]);
assert_eq!(res, Some(65));
let res = tree.get(&[1, 2, 3, 5]);
assert_eq!(res, None);

let res = tree.remove(&[1, 2, 3, 4]);
assert_eq!(res, Some(65))
let res = tree.remove(&[1, 2, 3, 4]);
assert_eq!(res, None)
```

Dart and GDart's interfaces are largely the same with the notable fact that tree must be mut for upsert and removal.
