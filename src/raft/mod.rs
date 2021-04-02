mod node;
mod error;
mod config;
mod serve;

#[derive(Debug, Clone)]
pub enum Role {
    Visitor,
    Follower,
    Candidate,
    Leader,
}