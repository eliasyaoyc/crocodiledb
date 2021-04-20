mod config;
mod node;
mod serve;

#[derive(Debug, Clone)]
pub enum Role {
    Visitor,
    Follower,
    Candidate,
    Leader,
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
