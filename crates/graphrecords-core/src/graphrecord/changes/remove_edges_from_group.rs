#[cfg(feature = "plugins")]
use super::Changes;
use super::{Change, sealed::Sealed};
#[cfg(feature = "plugins")]
use crate::graphrecord::{GraphRecord, plugins::Plugin};
use crate::{
    errors::{GraphRecordError, GraphRecordResult},
    graphrecord::{
        datatypes::{EdgeIndex, GroupIndex},
        state::GraphState,
    },
};
use graphrecords_utils::distinct::Distinct;

pub struct RemoveEdgesFromGroup {
    group_index: GroupIndex,
    edge_indices: Vec<EdgeIndex>,
}

impl RemoveEdgesFromGroup {
    #[must_use]
    pub fn new(edge_indices: Vec<EdgeIndex>, group_index: GroupIndex) -> Self {
        let edge_indices: Vec<_> = edge_indices.into_iter().collect::<Distinct<_>>().into();

        Self {
            group_index,
            edge_indices,
        }
    }

    #[must_use]
    pub const fn group_index(&self) -> &GroupIndex {
        &self.group_index
    }

    #[must_use]
    pub fn edge_indices(&self) -> &[EdgeIndex] {
        &self.edge_indices
    }
}

impl Sealed for RemoveEdgesFromGroup {}

impl Change for RemoveEdgesFromGroup {
    fn apply(&self, mut state: GraphState) -> GraphRecordResult<GraphState> {
        let group_address = state
            .resolve_group_address(&self.group_index)
            .ok_or_else(|| GraphRecordError::GroupNotFound {
                group_index: self.group_index.clone(),
            })?;

        for edge_index in &self.edge_indices {
            let edge_address = state.resolve_edge_address(edge_index).ok_or_else(|| {
                GraphRecordError::EdgeNotFound {
                    edge_index: *edge_index,
                }
            })?;

            state.remove_edge_from_group(edge_address, group_address)?;
        }

        Ok(state)
    }

    #[cfg(feature = "plugins")]
    fn pre_dispatch(
        self: Box<Self>,
        plugin: &dyn Plugin,
        record: &GraphRecord,
    ) -> GraphRecordResult<Changes> {
        plugin.pre_remove_edges_from_group(record, *self)
    }

    #[cfg(feature = "plugins")]
    fn post_dispatch(
        &self,
        plugin: &dyn Plugin,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        plugin.post_remove_edges_from_group(previous, candidate, self)
    }
}

#[cfg(test)]
mod test {
    use super::RemoveEdgesFromGroup;
    use crate::graphrecord::datatypes::{EdgeIndex, GroupIndex};

    #[test]
    fn test_new() {
        let removal = RemoveEdgesFromGroup::new(
            vec![
                EdgeIndex::new(1, 0),
                EdgeIndex::new(1, 1),
                EdgeIndex::new(1, 0),
            ],
            "dolor".into(),
        );

        assert_eq!(&GroupIndex::from("dolor"), removal.group_index());
        assert_eq!(
            vec![EdgeIndex::new(1, 0), EdgeIndex::new(1, 1)],
            removal.edge_indices()
        );
    }
}
