mod edges;
mod neighbors;
mod nodes;
mod relation;
mod source_node;
mod target_node;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum EdgeDirection {
    Incoming,
    Outgoing,
    Both,
}

pub use edges::EdgesOperation;
pub use neighbors::NeighborsOperation;
pub use nodes::NodesOperation;
pub use relation::{Relation, RelationOperation, SelectRelationOperation};
pub use source_node::EdgeSource;
pub use target_node::EdgeTarget;
