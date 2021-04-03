//! The current version that just support two kinds of engines, e.g memory and lsm
//! * Memory : all operation what action on the DRAM. The advantage is that it is very fast, but the data is easy to lose and  underlying data structure is B+Tree,
//!            as is known to all that the B+Tree write method is more serious, but it has better read performance(Ologn).
//! * SLSM : Log Structured Merged Tree given up a part of read performance be compared to B+Tree，but SLSM has very good write performance
//!         because it's going to written in direct order.
//! * WISCLSM : Not yet support in the short term.
pub mod memory;
pub mod slsm;
pub mod wisclsm;
