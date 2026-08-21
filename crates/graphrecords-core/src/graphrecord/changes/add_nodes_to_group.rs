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

pub struct AddNodesToGroup {
    group_index: GroupIndex,
    node_indices: Vec<NodeIndex>,
}

impl AddNodesToGroup {
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

impl Sealed for AddNodesToGroup {}

impl Change for AddNodesToGroup {
    fn apply(self: Box<Self>, mut state: GraphState) -> GraphRecordResult<GraphState> {
        let Self {
            group_index,
            node_indices,
        } = *self;

        let group_address = state
            .resolve_group_address(&group_index)
            .ok_or(GraphRecordError::GroupNotFound { group_index })?;

        for node_index in node_indices {
            let node_address = state
                .resolve_node_address(&node_index)
                .ok_or(GraphRecordError::NodeNotFound { node_index })?;

            state.add_node_to_group(node_address, group_address)?;
        }

        Ok(state)
    }

    #[cfg(feature = "plugins")]
    fn dispatch(
        self: Box<Self>,
        plugin: &dyn Plugin,
        record: &GraphRecord,
    ) -> GraphRecordResult<Changes> {
        plugin.on_add_nodes_to_group(record, *self)
    }

    #[cfg(feature = "plugins")]
    fn post_dispatch_hook(
        &self,
    ) -> fn(
        plugin: &dyn Plugin,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        |plugin, previous, candidate| plugin.post_add_nodes_to_group(previous, candidate)
    }
}

#[cfg(test)]
mod test {
    use super::AddNodesToGroup;
    use crate::graphrecord::datatypes::{GroupIndex, NodeIndex};

    #[test]
    fn test_new() {
        let addition = AddNodesToGroup::new(
            vec!["lorem".into(), "ipsum".into(), "lorem".into()],
            "dolor".into(),
        );

        assert_eq!(&GroupIndex::from("dolor"), addition.group_index());
        assert_eq!(
            vec![NodeIndex::from("lorem"), NodeIndex::from("ipsum")],
            addition.node_indices()
        );
    }
}
