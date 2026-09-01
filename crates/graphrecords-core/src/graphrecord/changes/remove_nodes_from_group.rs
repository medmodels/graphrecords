#[cfg(feature = "plugins")]
use super::Changes;
use super::{Change, sealed::Sealed};
#[cfg(feature = "plugins")]
use crate::graphrecord::{GraphRecord, plugins::Plugin};
use crate::{
    errors::{GraphRecordError, GraphRecordResult},
    graphrecord::{
        datatypes::{GroupIndex, NodeIndex},
        state::GraphState,
    },
};
use graphrecords_utils::distinct::Distinct;

pub struct RemoveNodesFromGroup {
    group_index: GroupIndex,
    node_indices: Vec<NodeIndex>,
}

impl RemoveNodesFromGroup {
    #[must_use]
    pub fn new(node_indices: Vec<NodeIndex>, group_index: GroupIndex) -> Self {
        let node_indices: Vec<_> = node_indices.into_iter().collect::<Distinct<_>>().into();

        Self {
            group_index,
            node_indices,
        }
    }

    #[must_use]
    pub const fn group_index(&self) -> &GroupIndex {
        &self.group_index
    }

    #[must_use]
    pub fn node_indices(&self) -> &[NodeIndex] {
        &self.node_indices
    }
}

impl Sealed for RemoveNodesFromGroup {}

impl Change for RemoveNodesFromGroup {
    fn apply(&self, mut state: GraphState) -> GraphRecordResult<GraphState> {
        let group_address = state
            .resolve_group_address(&self.group_index)
            .ok_or_else(|| GraphRecordError::GroupNotFound {
                group_index: self.group_index.clone(),
            })?;

        for node_index in &self.node_indices {
            let node_address = state.resolve_node_address(node_index).ok_or_else(|| {
                GraphRecordError::NodeNotFound {
                    node_index: node_index.clone(),
                }
            })?;

            state.remove_node_from_group(node_address, group_address)?;
        }

        Ok(state)
    }

    #[cfg(feature = "plugins")]
    fn pre_dispatch(
        self: Box<Self>,
        plugin: &dyn Plugin,
        record: &GraphRecord,
    ) -> GraphRecordResult<Changes> {
        plugin.pre_remove_nodes_from_group(record, *self)
    }

    #[cfg(feature = "plugins")]
    fn post_dispatch(
        &self,
        plugin: &dyn Plugin,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        plugin.post_remove_nodes_from_group(previous, candidate, self)
    }
}

#[cfg(test)]
mod test {
    use super::RemoveNodesFromGroup;
    use crate::graphrecord::datatypes::{GroupIndex, NodeIndex};

    #[test]
    fn test_new() {
        let removal = RemoveNodesFromGroup::new(
            vec!["lorem".into(), "ipsum".into(), "lorem".into()],
            "dolor".into(),
        );

        assert_eq!(&GroupIndex::from("dolor"), removal.group_index());
        assert_eq!(
            vec![NodeIndex::from("lorem"), NodeIndex::from("ipsum")],
            removal.node_indices()
        );
    }
}
