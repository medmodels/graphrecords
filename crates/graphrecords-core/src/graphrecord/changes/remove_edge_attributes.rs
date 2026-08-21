#[cfg(feature = "plugins")]
use super::Changes;
use super::{Change, sealed::Sealed};
#[cfg(feature = "plugins")]
use crate::graphrecord::{GraphRecord, plugins::Plugin};
use crate::{
    errors::{GraphRecordError, GraphRecordResult},
    graphrecord::{
        datatypes::{AttributeName, EdgeIndex},
        state::GraphState,
    },
};
use graphrecords_utils::distinct::Distinct;

pub struct RemoveEdgeAttributes {
    edge_indices: Vec<EdgeIndex>,
    attribute_names: Vec<AttributeName>,
}

impl RemoveEdgeAttributes {
    #[must_use]
    pub fn new(edge_indices: Vec<EdgeIndex>, attribute_names: Vec<AttributeName>) -> Self {
        let edge_indices: Vec<_> = edge_indices.into_iter().collect::<Distinct<_>>().into();
        let attribute_names: Vec<_> = attribute_names.into_iter().collect::<Distinct<_>>().into();

        Self {
            edge_indices,
            attribute_names,
        }
    }

    #[must_use]
    pub fn edge_indices(&self) -> &[EdgeIndex] {
        &self.edge_indices
    }

    #[must_use]
    pub fn attribute_names(&self) -> &[AttributeName] {
        &self.attribute_names
    }
}

impl Sealed for RemoveEdgeAttributes {}

impl Change for RemoveEdgeAttributes {
    fn apply(self: Box<Self>, mut state: GraphState) -> GraphRecordResult<GraphState> {
        let Self {
            edge_indices,
            attribute_names,
        } = *self;

        for edge_index in edge_indices {
            let address = state
                .resolve_edge_address(&edge_index)
                .ok_or(GraphRecordError::EdgeNotFound { edge_index })?;

            for attribute_name in &attribute_names {
                state.remove_edge_attribute(address, attribute_name)?;
            }
        }

        Ok(state)
    }

    #[cfg(feature = "plugins")]
    fn dispatch(
        self: Box<Self>,
        plugin: &dyn Plugin,
        record: &GraphRecord,
    ) -> GraphRecordResult<Changes> {
        plugin.on_remove_edge_attributes(record, *self)
    }

    #[cfg(feature = "plugins")]
    fn post_dispatch_hook(
        &self,
    ) -> fn(
        plugin: &dyn Plugin,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        |plugin, previous, candidate| plugin.post_remove_edge_attributes(previous, candidate)
    }
}

#[cfg(test)]
mod test {
    use super::RemoveEdgeAttributes;
    use crate::graphrecord::datatypes::{AttributeName, EdgeIndex};

    #[test]
    fn test_new() {
        let removal = RemoveEdgeAttributes::new(
            vec![
                EdgeIndex::new(1, 0),
                EdgeIndex::new(1, 1),
                EdgeIndex::new(1, 0),
            ],
            vec!["sed".into(), "amet".into(), "sed".into()],
        );

        assert_eq!(
            vec![EdgeIndex::new(1, 0), EdgeIndex::new(1, 1)],
            removal.edge_indices()
        );
        assert_eq!(
            vec![AttributeName::from("sed"), AttributeName::from("amet")],
            removal.attribute_names()
        );
    }
}
