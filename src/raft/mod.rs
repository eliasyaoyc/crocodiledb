mod config;
mod error;
mod node;
mod serve;

#[derive(Debug, Clone)]
pub enum Role {
    Visitor,
    Follower,
    Candidate,
    Leader,
}
