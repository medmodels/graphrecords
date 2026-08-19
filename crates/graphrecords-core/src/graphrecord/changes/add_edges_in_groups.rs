#[cfg(feature = "plugins")]
use super::Changes;
use super::{Change, sealed::Sealed};
#[cfg(feature = "plugins")]
use crate::graphrecord::{GraphRecord, plugins::Plugin};
use crate::{
    errors::{GraphRecordError, GraphRecordResult},
    graphrecord::{batch::EdgeBatch, datatypes::Group, state::GraphState},
};
use graphrecords_utils::distinct::Distinct;

pub struct AddEdgesInGroups {
    batch: EdgeBatch,
    groups: Vec<Group>,
}

impl AddEdgesInGroups {
    #[must_use]
    pub fn new(batch: EdgeBatch, groups: Vec<Group>) -> Self {
        let groups: Vec<_> = groups.into_iter().collect::<Distinct<_>>().into();

        Self { batch, groups }
    }

    #[must_use]
    pub const fn batch(&self) -> &EdgeBatch {
        &self.batch
    }

    #[must_use]
    pub fn groups(&self) -> &[Group] {
        &self.groups
    }
}

impl Sealed for AddEdgesInGroups {}

impl Change for AddEdgesInGroups {
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

        let resolved_edges: Vec<_> = batch
            .into_iter()
            .map(|(source_node_index, target_node_index, attributes)| {
                let source_address = state.resolve_node_address(&source_node_index).ok_or(
                    GraphRecordError::NodeNotFound {
                        node_index: source_node_index,
                    },
                )?;
                let target_address = state.resolve_node_address(&target_node_index).ok_or(
                    GraphRecordError::NodeNotFound {
                        node_index: target_node_index,
                    },
                )?;

                Ok((source_address, target_address, attributes))
            })
            .collect::<GraphRecordResult<_>>()?;

        state.insert_edges(resolved_edges, &group_addresses)?;

        Ok(state)
    }

    #[cfg(feature = "plugins")]
    fn dispatch(
        self: Box<Self>,
        plugin: &dyn Plugin,
        record: &GraphRecord,
    ) -> GraphRecordResult<Changes> {
        plugin.on_add_edges_in_groups(record, *self)
    }

    #[cfg(feature = "plugins")]
    fn post_dispatch_hook(
        &self,
    ) -> fn(
        plugin: &dyn Plugin,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        |plugin, previous, candidate| plugin.post_add_edges_in_groups(previous, candidate)
    }
}

#[cfg(test)]
mod test {
    use super::AddEdgesInGroups;
    use crate::graphrecord::{AttributeMap, batch::EdgeBatch, datatypes::Group};

    #[test]
    fn test_new() {
        let addition = AddEdgesInGroups::new(
            EdgeBatch::from(vec![("lorem".into(), "ipsum".into(), AttributeMap::new())]),
            vec!["dolor".into(), "sed".into(), "dolor".into()],
        );

        assert_eq!(1, addition.batch().len());
        assert_eq!(
            vec![Group::from("dolor"), Group::from("sed")],
            addition.groups()
        );
    }
}
