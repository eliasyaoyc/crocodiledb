# 💨💨💨 CrocodileDB (WIP)

## Overview
CrocodileDB is a distributed SQL database in Rust, written as a learning project but i hope it can be used in production.
the components including:
* Linearization based on Raft distributed consensus algorithm
* ACID-compliant transaction engine with MVCC-based snapshot isolation.
* Alternative storage engines with B+tree and log-structured.
* SQL layer satisfies generality, e.g. join,aggregates and transactions etc. 

## CrocodileDB Architecture
in making....

## Usage
First, should ensure you have Rust compiler.

* Start Server
```bash
cd clusters/local && ./run.sh
```

* Start Client
```bash
cargo run
```

## Bench
