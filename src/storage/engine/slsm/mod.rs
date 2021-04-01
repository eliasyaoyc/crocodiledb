//! Instructions in advance, if you need to written a introduction about your code what you should create a file named `README.md` and written in.
//! Strangely, i written in `mod.rs` but you don't ask me why i do that.
//! LSM the full-name is Log Structure Merge Tree that has good write performance greater than B+Tree, because LSM written in direct order,
//! so what is the basic performance cost for behavior but it's a waste of space serious, so we need to compress regularly and the related code in `compressor.rs`.
//! In this i will not impl write ahead logging(WAL) to ensure the security of the data because at the high-level all requests go through the RAFT protocol that it will
//! written in disk first.
//!
//! The most important thing is i'm not implementation the pure-lsm, i reference a paper named 『The SkipList-Based LSM Tree』that techniques such as cache-aware skiplist、
//! bloom filter、fence pointer、fast k-way merging algorithm and multi-thread. **It has the two important components built-in：**
//! * In-Memory: mainly used fot quick inserts and queries for buffered data，achieved by skiplist.
//! * In-Disk: mainly stores data in a hierarchical way and provides the operation of merge.
//! * In-Memory data structures support queries for cached data and queries for recent data.
//! * The storage on disk is hierarchical and immediate.
//!
//! **The related techniques designs:**
//! * Memory Buffer:
//! > The memory buffer contains of R runs, indexed by r. In the sLSM，one run corresponds to one skiplist. Only one run is active at any one time, denoted by index ra. Each
//! > run can contain up to Rn elements.
//! >
//! > An insert occurs as followers:
//! > 1. If the current run is full, make the current run a new,empty one
//! > 2. If there is no space for a new run:
//! >     * merge a proportion m of the existing runs in the buffer to secondary storage(disk).
//! >     * set the active run to a new,empty one.
//! >     * insert the k-v pair into the active run.
//! >
//! > A lookup occus as followers:
//! > 1. starting from the newest run and moving towards the oldest,search the skiplist for the given key.
//! > 2. return the first one found(as it is the newest). If not found, then search disk storage.
//! * SkipLists:
//! > * Fast Random Levels: p setup 0.5 in practice.
//! > * Vertical Arrays, Horizontal Pointer: traversal optimization. In skiplist, each node contains an array, which mainly stores the pointer to the next node
//! > of the node at each level.This design can save a lot of memory.
//! * Bloom Filter
//! > mainly provides the read performance optimization. if queries key not hit in bloom filter then return.
//! * Fence Pointer:
//! > used for disk-storage, mainly record the maximum and minimum values built in each SST. Since the key ranges of multiple SSTs in each layer do not overlap
//! > each other, it is possible to narrow the search scope for a single key to singe SST.
//! * Merge:
//! > merge a proportion m of the existing runs in the buffer to disk when memory-buffer is full. use the heap to optimize the merge operation,even with a minimum heap for the merge operation.
mod bloom;
mod checksum;
mod error;
mod levels;
pub mod skl;
pub mod table;
mod wal;
mod compressor;
