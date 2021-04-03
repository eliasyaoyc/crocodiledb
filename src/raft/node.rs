use crate::raft::Role;

#[derive(Debug, Clone)]
pub struct Node {
    pub id: usize,
    pub name: String,
    pub role: Role,
}

impl Node {
    pub fn create(&self) -> Self {
        self.init_node();
        Self {
            id: 0,
            name: "1".into(),
            role: Role::Visitor,
        }
    }

    fn init_node(&self) {}
}
