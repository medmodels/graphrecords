#[cfg(feature = "plugins")]
use super::Changes;
use super::{Change, sealed::Sealed};
#[cfg(feature = "plugins")]
use crate::graphrecord::{GraphRecord, plugins::Plugin};
use crate::{
    errors::{GraphRecordError, GraphRecordResult},
    graphrecord::{datatypes::EdgeIndex, state::GraphState},
};
use graphrecords_utils::distinct::Distinct;

pub struct RemoveEdges {
    edge_indices: Vec<EdgeIndex>,
}

impl RemoveEdges {
    #[must_use]
    pub fn new(edge_indices: Vec<EdgeIndex>) -> Self {
        let edge_indices: Vec<_> = edge_indices.into_iter().collect::<Distinct<_>>().into();

        Self { edge_indices }
    }

    #[must_use]
    pub fn edge_indices(&self) -> &[EdgeIndex] {
        &self.edge_indices
    }
}

impl Sealed for RemoveEdges {}

impl Change for RemoveEdges {
    fn apply(&self, mut state: GraphState) -> GraphRecordResult<GraphState> {
        for edge_index in &self.edge_indices {
            let address = state.resolve_edge_address(edge_index).ok_or_else(|| {
                GraphRecordError::EdgeNotFound {
                    edge_index: *edge_index,
                }
            })?;

            state.remove_edge(address);
        }

        Ok(state)
    }

    #[cfg(feature = "plugins")]
    fn pre_dispatch(
        self: Box<Self>,
        plugin: &dyn Plugin,
        record: &GraphRecord,
    ) -> GraphRecordResult<Changes> {
        plugin.pre_remove_edges(record, *self)
    }

    #[cfg(feature = "plugins")]
    fn post_dispatch(
        &self,
        plugin: &dyn Plugin,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        plugin.post_remove_edges(previous, candidate, self)
    }
}

#[cfg(test)]
mod test {
    use super::RemoveEdges;
    use crate::graphrecord::datatypes::EdgeIndex;

    #[test]
    fn test_new() {
        let removal = RemoveEdges::new(vec![
            EdgeIndex::new(1, 0),
            EdgeIndex::new(1, 1),
            EdgeIndex::new(1, 0),
        ]);

        assert_eq!(
            vec![EdgeIndex::new(1, 0), EdgeIndex::new(1, 1)],
            removal.edge_indices()
        );
    }
}
