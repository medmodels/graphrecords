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

pub struct AddEdgesToGroup {
    group_index: GroupIndex,
    edge_indices: Vec<EdgeIndex>,
}

impl AddEdgesToGroup {
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

impl Sealed for AddEdgesToGroup {}

impl Change for AddEdgesToGroup {
    fn apply(self: Box<Self>, mut state: GraphState) -> GraphRecordResult<GraphState> {
        let Self {
            group_index,
            edge_indices,
        } = *self;

        let group_address = state
            .resolve_group_address(&group_index)
            .ok_or(GraphRecordError::GroupNotFound { group_index })?;

        for edge_index in edge_indices {
            let edge_address = state
                .resolve_edge_address(&edge_index)
                .ok_or(GraphRecordError::EdgeNotFound { edge_index })?;

            state.add_edge_to_group(edge_address, group_address)?;
        }

        Ok(state)
    }

    #[cfg(feature = "plugins")]
    fn dispatch(
        self: Box<Self>,
        plugin: &dyn Plugin,
        record: &GraphRecord,
    ) -> GraphRecordResult<Changes> {
        plugin.on_add_edges_to_group(record, *self)
    }

    #[cfg(feature = "plugins")]
    fn post_dispatch_hook(
        &self,
    ) -> fn(
        plugin: &dyn Plugin,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        |plugin, previous, candidate| plugin.post_add_edges_to_group(previous, candidate)
    }
}

#[cfg(test)]
mod test {
    use super::AddEdgesToGroup;
    use crate::graphrecord::datatypes::{EdgeIndex, GroupIndex};

    #[test]
    fn test_new() {
        let addition = AddEdgesToGroup::new(
            vec![
                EdgeIndex::new(1, 0),
                EdgeIndex::new(1, 1),
                EdgeIndex::new(1, 0),
            ],
            "dolor".into(),
        );

        assert_eq!(&GroupIndex::from("dolor"), addition.group_index());
        assert_eq!(
            vec![EdgeIndex::new(1, 0), EdgeIndex::new(1, 1)],
            addition.edge_indices()
        );
    }
}
