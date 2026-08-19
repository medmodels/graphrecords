#[cfg(feature = "plugins")]
use super::Changes;
use super::{Change, sealed::Sealed};
#[cfg(feature = "plugins")]
use crate::graphrecord::{GraphRecord, plugins::Plugin};
use crate::{
    errors::{GraphRecordError, GraphRecordResult},
    graphrecord::{batch::NodeBatch, datatypes::Group, state::GraphState},
};
use graphrecords_utils::distinct::Distinct;

pub struct AddNodesInGroups {
    batch: NodeBatch,
    groups: Vec<Group>,
}

impl AddNodesInGroups {
    #[must_use]
    pub fn new(batch: NodeBatch, groups: Vec<Group>) -> Self {
        let groups: Vec<_> = groups.into_iter().collect::<Distinct<_>>().into();

        Self { batch, groups }
    }

    #[must_use]
    pub const fn batch(&self) -> &NodeBatch {
        &self.batch
    }

    #[must_use]
    pub fn groups(&self) -> &[Group] {
        &self.groups
    }
}

impl Sealed for AddNodesInGroups {}

impl Change for AddNodesInGroups {
    fn apply(self: Box<Self>, mut state: GraphState) -> GraphRecordResult<GraphState> {
        let Self { batch, groups } = *self;

        let group_addresses: Vec<_> = groups
            .into_iter()
            .map(|group| {
                state
                    .resolve_group_address(&group)
                    .ok_or(GraphRecordError::GroupNotFound { group })
            })
            .collect::<GraphRecordResult<_>>()?;

        for (node_index, attributes) in batch {
            state.insert_node(node_index, &attributes, &group_addresses)?;
        }

        Ok(state)
    }

    #[cfg(feature = "plugins")]
    fn dispatch(
        self: Box<Self>,
        plugin: &dyn Plugin,
        record: &GraphRecord,
    ) -> GraphRecordResult<Changes> {
        plugin.on_add_nodes_in_groups(record, *self)
    }

    #[cfg(feature = "plugins")]
    fn post_dispatch_hook(
        &self,
    ) -> fn(
        plugin: &dyn Plugin,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        |plugin, previous, candidate| plugin.post_add_nodes_in_groups(previous, candidate)
    }
}

#[cfg(test)]
mod test {
    use super::AddNodesInGroups;
    use crate::graphrecord::{AttributeMap, batch::NodeBatch, datatypes::Group};

    #[test]
    fn test_new() {
        let addition = AddNodesInGroups::new(
            NodeBatch::from(vec![("lorem".into(), AttributeMap::new())]),
            vec!["dolor".into(), "sed".into(), "dolor".into()],
        );

        assert_eq!(1, addition.batch().len());
        assert_eq!(
            vec![Group::from("dolor"), Group::from("sed")],
            addition.groups()
        );
    }
}
