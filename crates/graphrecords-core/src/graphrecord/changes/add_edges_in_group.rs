#[cfg(feature = "plugins")]
use super::Changes;
use super::{Change, sealed::Sealed};
#[cfg(feature = "plugins")]
use crate::graphrecord::{GraphRecord, plugins::Plugin};
use crate::{
    errors::{GraphRecordError, GraphRecordResult},
    graphrecord::{batch::EdgeBatch, datatypes::GroupIndex, state::GraphState},
};

pub struct AddEdgesInGroup {
    batch: EdgeBatch,
    group_index: GroupIndex,
}

impl AddEdgesInGroup {
    #[must_use]
    pub const fn new(batch: EdgeBatch, group_index: GroupIndex) -> Self {
        Self { batch, group_index }
    }

    #[must_use]
    pub const fn batch(&self) -> &EdgeBatch {
        &self.batch
    }

    #[must_use]
    pub const fn group_index(&self) -> &GroupIndex {
        &self.group_index
    }
}

impl Sealed for AddEdgesInGroup {}

impl Change for AddEdgesInGroup {
    fn apply(&self, mut state: GraphState) -> GraphRecordResult<GraphState> {
        let group_address = match state.resolve_group_address(&self.group_index) {
            Some(group_address) => group_address,
            None => state.insert_group(&self.group_index)?,
        };

        let resolved_edges: Vec<_> = self
            .batch
            .iter()
            .map(|(source_node_index, target_node_index, attributes)| {
                let source_address =
                    state
                        .resolve_node_address(source_node_index)
                        .ok_or_else(|| GraphRecordError::NodeNotFound {
                            node_index: source_node_index.clone(),
                        })?;
                let target_address =
                    state
                        .resolve_node_address(target_node_index)
                        .ok_or_else(|| GraphRecordError::NodeNotFound {
                            node_index: target_node_index.clone(),
                        })?;

                Ok((source_address, target_address, attributes))
            })
            .collect::<GraphRecordResult<_>>()?;

        state.insert_edges(resolved_edges, &[group_address])?;

        Ok(state)
    }

    #[cfg(feature = "plugins")]
    fn pre_dispatch(
        self: Box<Self>,
        plugin: &dyn Plugin,
        record: &GraphRecord,
    ) -> GraphRecordResult<Changes> {
        plugin.pre_add_edges_in_group(record, *self)
    }

    #[cfg(feature = "plugins")]
    fn post_dispatch(
        &self,
        plugin: &dyn Plugin,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        plugin.post_add_edges_in_group(previous, candidate, self)
    }
}

#[cfg(test)]
mod test {
    use super::AddEdgesInGroup;
    use crate::graphrecord::{AttributeMap, batch::EdgeBatch, datatypes::GroupIndex};

    #[test]
    fn test_new() {
        let addition = AddEdgesInGroup::new(
            EdgeBatch::from(vec![("lorem".into(), "ipsum".into(), AttributeMap::new())]),
            "dolor".into(),
        );

        assert_eq!(1, addition.batch().len());
        assert_eq!(&GroupIndex::from("dolor"), addition.group_index());
    }
}
