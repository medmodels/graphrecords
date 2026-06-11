use super::{AttributeMap, NodeIndex};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Edge {
    pub(crate) attributes: AttributeMap,
    pub(crate) source_node_index: NodeIndex,
    pub(crate) target_node_index: NodeIndex,
}

impl Edge {
    pub const fn new(
        attributes: AttributeMap,
        source_node_index: NodeIndex,
        target_node_index: NodeIndex,
    ) -> Self {
        Self {
            attributes,
            source_node_index,
            target_node_index,
        }
    }
}
