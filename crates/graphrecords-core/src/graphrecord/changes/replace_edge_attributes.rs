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

pub struct ReplaceEdgeAttributes {
    edge_indices: Vec<EdgeIndex>,
    attributes: AttributeMap,
}

impl ReplaceEdgeAttributes {
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

impl Sealed for ReplaceEdgeAttributes {}

impl Change for ReplaceEdgeAttributes {
    fn apply(&self, mut state: GraphState) -> GraphRecordResult<GraphState> {
        for edge_index in &self.edge_indices {
            let address = state.resolve_edge_address(edge_index).ok_or_else(|| {
                GraphRecordError::EdgeNotFound {
                    edge_index: *edge_index,
                }
            })?;

            state.replace_edge_attributes(address, &self.attributes)?;
        }

        Ok(state)
    }

    #[cfg(feature = "plugins")]
    fn pre_dispatch(
        self: Box<Self>,
        plugin: &dyn Plugin,
        record: &GraphRecord,
    ) -> GraphRecordResult<Changes> {
        plugin.pre_replace_edge_attributes(record, *self)
    }

    #[cfg(feature = "plugins")]
    fn post_dispatch(
        &self,
        plugin: &dyn Plugin,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        plugin.post_replace_edge_attributes(previous, candidate, self)
    }
}

#[cfg(test)]
mod test {
    use super::ReplaceEdgeAttributes;
    use crate::graphrecord::{AttributeMap, datatypes::EdgeIndex};

    #[test]
    fn test_new() {
        let replacement = ReplaceEdgeAttributes::new(
            vec![
                EdgeIndex::new(1, 0),
                EdgeIndex::new(1, 1),
                EdgeIndex::new(1, 0),
            ],
            AttributeMap::from([("sed".into(), 1.into())]),
        );

        assert_eq!(
            vec![EdgeIndex::new(1, 0), EdgeIndex::new(1, 1)],
            replacement.edge_indices()
        );
        assert_eq!(
            &AttributeMap::from([("sed".into(), 1.into())]),
            replacement.attributes()
        );
    }
}
