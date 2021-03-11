use crate::storage::error::Error::ParamCapacityrErr;
use crate::storage::error::{Error, Result};
use crate::storage::storage::{Range, Scan};
use crate::storage::Storage;
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::mem::replace;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, RwLock};

/// The default B+tree capacity, i.e maximum number of children per node.
const DEFAULT_CAPACITY: usize = 8;

/// In-memory key-value store using the B+tree. The B+tree is a variant of a binary search tree with
/// lookup keys in inner nodes and actual key-value pair on the leaf nodes. Each nodes has several
/// children and search keys, to make use of cache locality, up to a maximum known as the tree's
/// capacity. Leaf and inner nodes contain between capacity/2 and capacity items, and will be split, rotated,
/// or merged as appropriate, while the root node can have between 0 and capacity children.
///
/// This implementation differs from a standard B+tree in that leaf nodes do not have pointers to
/// the sibling leaf nodes. Iterators traversal is instead done via lookups from the root node. This
/// has O(log n) complexity rather than O(1) for iterators, but is left as a future performance
/// optimization if it is shown to be necessary.
/// TODO added leaf nodes has pointers to the sibling leaf nods, and optimize iterator traversal performance.
pub struct Memory {
    /// The tree node , guarded by an RwLock to support multiple iterators across it.
    root: Arc<RwLock<Node>>,
}

impl Display for Memory {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "memory")
    }
}

impl Memory {
    /// Creates a new in-memory store using the default capacity.
    pub fn new() -> Self {
        Self::new_with_capacity(DEFAULT_CAPACITY).unwrap()
    }

    /// Creates a new in-memory store using the given capacity.
    pub fn new_with_capacity(capacity: usize) -> Result<Self> {
        if capacity < 2 {
            return Err(ParamCapacityrErr);
        }
        Ok(Self {
            root: Arc::new(RwLock::new(Node::Root(Children::new(DEFAULT_CAPACITY)))),
        })
    }
}

impl Storage for Memory {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.root.read()?.get(key))
    }

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.root.write()?.set(key, value);
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.root.write()?.delete(key);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        // TODO the item store in the memory in current version, so don't need to implement this method for now.
        Ok(())
    }

    fn scan(&self, range: Range) -> Scan {
        unimplemented!()
    }
}

/// B-tree node variants. Most internal logic is delegated to contained Children/Values structs,
/// while this outer structure manages the overall tree, particularly root special-casing.
///
/// All nodes in a tree have the same capacity (i.e. the same maximum number of children/values). The
/// root node can contain anywhere between 0 and the maximum number of items, while inner and leaf
/// node try to stay between capacity/2 and capacity items.
#[derive(Debug, PartialEq)]
enum Node {
    Root(Children),
    Inner(Children),
    Leaf(Values),
}

impl Node {
    /// Deletes a key from the node, if it exists.
    fn delete(&mut self, key: &[u8]) {
        match self {
            Self::Root(children) => {
                children.delete(key);
                // If we now have a single child, pull it up into the root.
                while children.len() == 1 && matches!(children[0], Node::Inner{..}) {
                    if let Node::Inner(c) = children.remove(0) {
                        *children = c;
                    }
                }
                // If we have a single empty child, remove it.
                if children.len() == 1 && children[0].size() == 0 {
                    children.remove(0);
                }
            }
            Self::Inner(children) => children.delete(key),
            Self::Leaf(values) => values.delete(key),
        }
    }

    /// Returns a value for a key, if it exists.
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self {
            Self::Root(children) | Self::Inner(children) => children.get(key),
            Self::Leaf(values) => values.get(key),
        }
    }

    /// Returns the first key/value pair, if any.
    fn get_first(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        match self {
            Self::Root(children) | Self::Inner(children) => children.get_first(),
            Self::Leaf(values) => values.get_first(),
        }
    }

    /// Returns the last key/value pair, if any.
    fn get_last(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        match self {
            Self::Root(children) | Self::Inner(children) => children.get_last(),
            Self::Leaf(values) => values.get_last(),
        }
    }

    /// Returns the next key/value pair after the given key.
    fn get_next(&self, key: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
        match self {
            Self::Root(children) | Self::Inner(children) => children.get_next(key),
            Self::Leaf(values) => values.get_next(key),
        }
    }

    /// Returns the previous key/value pair before the given key.
    fn get_prev(&self, key: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
        match self {
            Self::Root(children) | Self::Inner(children) => children.get_prev(key),
            Self::Leaf(values) => values.get_prev(key),
        }
    }

    /// Sets a key to a value in the node, inserting or updating the key as appropriate. If the
    /// node splits, return the split key and new (right) node.
    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Option<(Vec<u8>, Node)> {
        match self {
            Self::Root(ref mut children) => {
                // Set the key/value pair in the children. If the children split, create a new
                // child set for the root node with two new inner nodes for the split children.
                if let Some((split_key, split_children)) = children.set(key, value) {
                    // If the children split, we should rebuild the root node,so the next step is to insert the old root node and insert split node.
                    let mut root_children = Children::new(children.capacity());
                    root_children.keys.push(split_key);
                    root_children
                        .nodes
                        .push(Node::Inner(replace(children, Children::empty())));
                    root_children.nodes.push(Node::Inner(split_children));
                    *children = root_children;
                }
                None
            }
            Self::Inner(children) => children.set(key, value).map(|(sk, c)| (sk, Node::Inner(c))),
            Self::Leaf(values) => values.set(key, value).map(|(sk, v)| (sk, Node::Leaf(v))),
        }
    }

    /// Returns the order (i.e. capacity) of the node.
    fn capacity(&self) -> usize {
        match self {
            Self::Root(children) | Self::Inner(children) => children.capacity(),
            Self::Leaf(values) => values.capacity(),
        }
    }

    /// Returns the size (number of items) of the node.
    fn size(&self) -> usize {
        match self {
            Self::Root(children) | Self::Inner(children) => children.len(),
            Self::Leaf(values) => values.len(),
        }
    }
}

/// Root node and inner node children. The child set (node) determines the maximum number of
/// child nodes, which is tracked via the interval vector capacity. Derefs to the child node vector.
///
/// The keys are used to guide lookups. There is always one key less than children, where the key at
/// index 1 (and all keys up to the one at i+1) is contained within the child at index i+1.
///
/// For example:
///
/// Index   Keys  Nodes
/// 0       d     a=1,b=2,c=3        Keys:                d         f
/// 1       f     d=4,e=5            Node:    a=1,b=2,c=3 | d=4,e=5 | f=6,g=7
/// 2             f=6,g=7
#[derive(Debug, Default, PartialEq)]
struct Children {
    keys: Vec<Vec<u8>>,
    nodes: Vec<Node>,
}

impl Deref for Children {
    type Target = Vec<Node>;
    fn deref(&self) -> &Self::Target {
        &self.nodes
    }
}

impl DerefMut for Children {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.nodes
    }
}

impl Children {
    /// Creates a new child set of the given order (maximum capacity).
    fn new(order: usize) -> Self {
        Self {
            keys: Vec::with_capacity(order - 1),
            nodes: Vec::with_capacity(order),
        }
    }

    /// Creates an empty child set, for use with replace().
    fn empty() -> Self {
        Children::default()
    }

    /// Deletes a key from the children, if it exists.
    fn delete(&mut self, key: &[u8]) {
        if self.is_empty() {
            return;
        }


        // Delete the key in the relevant child.
        let (i, child) = self.lookup_mut(key);
        child.delete(key);

        // If the child dose not underflow, or it has no siblings, we're done.
        if child.size() >= (child.capacity() + 1) / 2 || self.len() == 1 {
            return;
        }

        // Attempt to rotate or merge with the left or right siblings.
        let (siz, cap) = (self[i].size(), self[i].capacity());
        let (lsiz, lcap) = if i > 0 {
            (self[i - 1].size(), self[i - 1].capacity())
        } else {
            (0, 0)
        };

        let (rsiz, rcap) = if i < self.len() - 1 {
            (self[i + 1].size(), self[i + 1].capacity())
        } else {
            (0, 0)
        };

        if lsiz > (lcap + 1) / 2 {
            self.rotate_right(i - 1);
        } else if rsiz > (rcap + 1) / 2 {
            self.rotate_left(i + 1);
        } else if lsiz + siz <= lcap {
            self.merge(i - 1);
        } else if rsiz + siz <= cap {
            self.merge(i);
        }
    }

    /// Returns a value for a key, if it exists.
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        if self.is_empty() {
            None
        } else {
            self.lookup(key).1.get(key)
        }
    }

    /// Returns the first key/value pair, if any.
    fn get_first(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.nodes.first().and_then(|n| n.get_first())
    }

    /// Returns the last key/value pair, if any.
    fn get_last(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.nodes.last().and_then(|n| n.get_last())
    }

    /// Returns the next key/value pair after the given key, if it exists.
    fn get_next(&self, key: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
        None
    }

    /// Returns the previous key/value pair before the given key, if it exists.
    fn get_prev(&self, key: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
        None
    }

    /// Looks up the child responsible for a given key. This can only be called on non-empty
    /// child sets, which should be all child sets except for the initial root node.
    fn lookup(&self, key: &[u8]) -> (usize, &Node) {
        let i = self.keys.iter().position(|k| k.deref() > key).unwrap_or_else(|| self.keys.len());
        (i, &self[i])
    }

    /// Looks up the child responsible for a given key, and returns a mutable reference to it. This
    /// can only be called on non-empty child sets, which should be all child sets except for the
    /// initial root node.
    fn lookup_mut(&mut self, key: &[u8]) -> (usize, &mut Node) {
        let i = self.keys.iter().position(|k| k.deref() > key).unwrap_or_else(|| self.keys.len());
        (i, &mut self[i])
    }

    /// Merges the node at index i with it's right sibling.
    fn merge(&mut self, i: usize) {}

    fn rebalance(&mut self) {}
    /// Rotates children to the left, by transferring items from the node at the given index to
    /// its left sibling and adjusting the separator key.
    fn rotate_left(&mut self, i: usize) {}

    /// Rotates children to the right, by transferring items from the node at the given index to
    /// its right sibling and adjusting the separator key.
    fn rotate_right(&mut self, i: usize) {}

    /// Sets a key to a value in the children, delegating to the child responsible. If the node
    /// splits, returns the split key and new (right) node.
    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Option<(Vec<u8>, Children)> {
        None
    }
}

/// Leaf node key/value pairs. The value set (leaf node) order determines the maximum number
/// of key/value items, which is tracked via the internal vector capacity. Items are ordered by key,
/// and looked up via linear search due to the low cardinality. Derefs to the inner vec.
#[derive(Debug, PartialEq)]
struct Values(Vec<(Vec<u8>, Vec<u8>)>);

impl Deref for Values {
    type Target = Vec<(Vec<u8>, Vec<u8>)>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Values {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Values {
    /// Creates a new value set with the given order (maximum capacity).
    fn new(order: usize) -> Self {
        Self(Vec::with_capacity(order))
    }

    /// Deletes a key from the set, if it exists.
    fn delete(&mut self, key: &[u8]) {}

    /// Returns a value from the set, if the key exists.
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        None
    }

    /// Returns the first key/value pair from the set, if any.
    fn get_first(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.0.first().cloned()
    }

    /// Returns the last key/value pair from the set, if any.
    fn get_last(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.0.last().cloned()
    }

    /// Returns the next value after the given key, if it exists.
    fn get_next(&self, key: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
        None
    }

    /// Returns the previous value before the given key, if it exists.
    fn get_prev(&self, key: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
        None
    }

    /// Sets a key to a value, inserting of updating it. If the value set is full, it is split
    /// in the middle and the split key and right values are returned.
    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Option<(Vec<u8>, Values)> {
        None
    }
}

struct Iter {
    root: Arc<RwLock<Node>>,
    range: Range,
    front_cursor: Option<Vec<u8>>,
    back_cursor: Option<Vec<u8>>,
}

impl Iter {
    fn new(root: Arc<RwLock<Node>>, range: Range) -> Self {
        Self {
            root,
            range,
            front_cursor: None,
            back_cursor: None,
        }
    }

    fn try_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        Ok(None)
    }

    fn try_next_back(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        Ok(None)
    }
}

impl Iterator for Iter {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

impl DoubleEndedIterator for Iter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.try_next_back().transpose()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn is_work() {
        println!("11");
    }
}
