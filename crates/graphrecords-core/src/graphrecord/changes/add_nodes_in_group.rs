#[cfg(feature = "plugins")]
use super::Changes;
use super::{Change, sealed::Sealed};
#[cfg(feature = "plugins")]
use crate::graphrecord::{GraphRecord, plugins::Plugin};
use crate::{
    errors::GraphRecordResult,
    graphrecord::{batch::NodeBatch, datatypes::GroupIndex, state::GraphState},
};

pub struct AddNodesInGroup {
    batch: NodeBatch,
    group_index: GroupIndex,
}

impl AddNodesInGroup {
    #[must_use]
    pub const fn new(batch: NodeBatch, group_index: GroupIndex) -> Self {
        Self { batch, group_index }
    }

    #[must_use]
    pub const fn batch(&self) -> &NodeBatch {
        &self.batch
    }

    #[must_use]
    pub const fn group_index(&self) -> &GroupIndex {
        &self.group_index
    }
}

impl Sealed for AddNodesInGroup {}

impl Change for AddNodesInGroup {
    fn apply(&self, mut state: GraphState) -> GraphRecordResult<GraphState> {
        let group_address = match state.resolve_group_address(&self.group_index) {
            Some(group_address) => group_address,
            None => state.insert_group(&self.group_index)?,
        };

        for (node_index, attributes) in self.batch.iter() {
            state.insert_node(node_index, attributes, &[group_address])?;
        }

        Ok(state)
    }

    #[cfg(feature = "plugins")]
    fn pre_dispatch(
        self: Box<Self>,
        plugin: &dyn Plugin,
        record: &GraphRecord,
    ) -> GraphRecordResult<Changes> {
        plugin.pre_add_nodes_in_group(record, *self)
    }

    #[cfg(feature = "plugins")]
    fn post_dispatch(
        &self,
        plugin: &dyn Plugin,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        plugin.post_add_nodes_in_group(previous, candidate, self)
    }
}

#[cfg(test)]
mod test {
    use super::AddNodesInGroup;
    use crate::graphrecord::{AttributeMap, batch::NodeBatch, datatypes::GroupIndex};

    #[test]
    fn test_new() {
        let addition = AddNodesInGroup::new(
            NodeBatch::from(vec![("lorem".into(), AttributeMap::new())]),
            "dolor".into(),
        );

        assert_eq!(1, addition.batch().len());
        assert_eq!(&GroupIndex::from("dolor"), addition.group_index());
    }
}
