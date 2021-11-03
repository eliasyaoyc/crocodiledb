#[macro_use]
extern crate num_derive;
#[macro_use]
extern crate log;

mod error;
mod db;
mod util;
mod sstable;
pub mod opt;
mod cache;
mod filter;
mod memtable;
pub mod iterator;
mod wal;
mod version;
mod compaction;
mod storage;
pub mod snapshot;
mod table_cache;
mod batch;

pub use error::{Error, IResult};
pub use memtable::skiplist;

pub type DefaultHashBuilder = std::hash::BuildHasherDefault<fxhash::FxHasher>;
pub type HashSet<K> = std::collections::HashSet<K, DefaultHashBuilder>;