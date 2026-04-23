use super::super::intern_table::{HandleAttributes, NodeHandle};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Edge {
    pub(crate) attributes: HandleAttributes,
    pub(crate) source_node_handle: NodeHandle,
    pub(crate) target_node_handle: NodeHandle,
}

impl Edge {
    pub const fn new(
        attributes: HandleAttributes,
        source_node_handle: NodeHandle,
        target_node_handle: NodeHandle,
    ) -> Self {
        Self {
            attributes,
            source_node_handle,
            target_node_handle,
        }
    }
}
