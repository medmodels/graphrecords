#[cfg(feature = "plugins")]
use super::Changes;
use super::{Change, sealed::Sealed};
#[cfg(feature = "plugins")]
use crate::graphrecord::{GraphRecord, plugins::Plugin};
use crate::{
    errors::{GraphRecordError, GraphRecordResult},
    graphrecord::{AttributeMap, datatypes::EdgeIndex, state::GraphState},
};
use graphrecords_utils::distinct::Distinct;

pub struct SetEdgeAttributes {
    edge_indices: Vec<EdgeIndex>,
    attributes: AttributeMap,
}

impl SetEdgeAttributes {
    #[must_use]
    pub fn new(edge_indices: Vec<EdgeIndex>, attributes: AttributeMap) -> Self {
        let edge_indices: Vec<_> = edge_indices.into_iter().collect::<Distinct<_>>().into();

        Self {
            edge_indices,
            attributes,
        }
    }

    #[must_use]
    pub fn edge_indices(&self) -> &[EdgeIndex] {
        &self.edge_indices
    }

    #[must_use]
    pub const fn attributes(&self) -> &AttributeMap {
        &self.attributes
    }
}

impl Sealed for SetEdgeAttributes {}

impl Change for SetEdgeAttributes {
    fn apply(self: Box<Self>, mut state: GraphState) -> GraphRecordResult<GraphState> {
        let Self {
            edge_indices,
            attributes,
        } = *self;

        for edge_index in edge_indices {
            let address = state
                .resolve_edge_address(&edge_index)
                .ok_or(GraphRecordError::EdgeNotFound { edge_index })?;

            state.set_edge_attributes(address, &attributes)?;
        }

        Ok(state)
    }

    #[cfg(feature = "plugins")]
    fn dispatch(
        self: Box<Self>,
        plugin: &dyn Plugin,
        record: &GraphRecord,
    ) -> GraphRecordResult<Changes> {
        plugin.on_set_edge_attributes(record, *self)
    }

    #[cfg(feature = "plugins")]
    fn post_dispatch_hook(
        &self,
    ) -> fn(
        plugin: &dyn Plugin,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        |plugin, previous, candidate| plugin.post_set_edge_attributes(previous, candidate)
    }
}

#[cfg(test)]
mod test {
    use super::SetEdgeAttributes;
    use crate::graphrecord::{AttributeMap, datatypes::EdgeIndex};

    #[test]
    fn test_new() {
        let assignment = SetEdgeAttributes::new(
            vec![
                EdgeIndex::new(1, 0),
                EdgeIndex::new(1, 1),
                EdgeIndex::new(1, 0),
            ],
            AttributeMap::from([("sed".into(), 1.into())]),
        );

        assert_eq!(
            vec![EdgeIndex::new(1, 0), EdgeIndex::new(1, 1)],
            assignment.edge_indices()
        );
        assert_eq!(
            &AttributeMap::from([("sed".into(), 1.into())]),
            assignment.attributes()
        );
    }
}
