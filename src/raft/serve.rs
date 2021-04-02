#[derive(Debug, Clone)]
pub struct Serve {
    pub peers: Vec<String>,
}

impl Serve {
    /// Returns true represents cluster setup otherwise not.
    #[inline]
    fn is_cluster(&self) -> bool {
        if !self.peers.is_empty() && self.peers.len() > 1 {
            return true;
        }
        false
    }
}