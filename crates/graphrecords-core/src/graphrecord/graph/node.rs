use super::{Attributes, EdgeIndex};
use graphrecords_utils::aliases::GrHashSet;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Node {
    pub(crate) attributes: Attributes,
    pub(crate) outgoing_edge_indices: GrHashSet<EdgeIndex>,
    pub(crate) incoming_edge_indices: GrHashSet<EdgeIndex>,
}

impl Node {
    pub fn new(attributes: Attributes) -> Self {
        Self {
            attributes,
            outgoing_edge_indices: GrHashSet::new(),
            incoming_edge_indices: GrHashSet::new(),
        }
    }
}
