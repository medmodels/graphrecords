#[cfg(feature = "plugins")]
use super::Changes;
use super::{Change, sealed::Sealed};
#[cfg(feature = "plugins")]
use crate::graphrecord::{GraphRecord, plugins::Plugin};
use crate::{
    errors::{GraphRecordError, GraphRecordResult},
    graphrecord::{
        datatypes::{Group, NodeIndex},
        state::GraphState,
    },
};
use graphrecords_utils::distinct::Distinct;

pub struct RemoveNodesFromGroup {
    group: Group,
    node_indices: Vec<NodeIndex>,
}

impl RemoveNodesFromGroup {
    #[must_use]
    pub fn new(group: Group, node_indices: Vec<NodeIndex>) -> Self {
        let node_indices: Vec<_> = node_indices.into_iter().collect::<Distinct<_>>().into();

        Self {
            group,
            node_indices,
        }
    }

    #[must_use]
    pub const fn group(&self) -> &Group {
        &self.group
    }

    #[must_use]
    pub fn node_indices(&self) -> &[NodeIndex] {
        &self.node_indices
    }
}

impl Sealed for RemoveNodesFromGroup {}

impl Change for RemoveNodesFromGroup {
    fn apply(self: Box<Self>, mut state: GraphState) -> GraphRecordResult<GraphState> {
        let Self {
            group,
            node_indices,
        } = *self;

        let group_address = state
            .resolve_group_address(&group)
            .ok_or(GraphRecordError::GroupNotFound { group })?;

        for node_index in node_indices {
            let node_address = state
                .resolve_node_address(&node_index)
                .ok_or(GraphRecordError::NodeNotFound { node_index })?;

            state.remove_node_from_group(node_address, group_address)?;
        }

        Ok(state)
    }

    #[cfg(feature = "plugins")]
    fn dispatch(
        self: Box<Self>,
        plugin: &dyn Plugin,
        record: &GraphRecord,
    ) -> GraphRecordResult<Changes> {
        plugin.on_remove_nodes_from_group(record, *self)
    }

    #[cfg(feature = "plugins")]
    fn post_dispatch_hook(
        &self,
    ) -> fn(
        plugin: &dyn Plugin,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        |plugin, previous, candidate| plugin.post_remove_nodes_from_group(previous, candidate)
    }
}

#[cfg(test)]
mod test {
    use super::RemoveNodesFromGroup;
    use crate::graphrecord::datatypes::{Group, NodeIndex};

    #[test]
    fn test_new() {
        let removal = RemoveNodesFromGroup::new(
            "dolor".into(),
            vec!["lorem".into(), "ipsum".into(), "lorem".into()],
        );

        assert_eq!(&Group::from("dolor"), removal.group());
        assert_eq!(
            vec![NodeIndex::from("lorem"), NodeIndex::from("ipsum")],
            removal.node_indices()
        );
    }
}
