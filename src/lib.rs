mod config;
mod error;
mod kv;
mod raft;
mod sql;
mod storage;
mod types;
mod util;
mod admin;
mod metrics;
mod tracing;


pub trait NewService<T> {
    type Service;

    fn new_service(&mut self, target: T) -> Self::Service;
}

impl<F, T, S> NewService<T> for F
    where
        F: Fn(T) -> S,
{
    type Service = S;

    fn new_service(&mut self, target: T) -> Self::Service {
        (self)(target)
    }
}