#[cfg(feature = "plugins")]
use super::Changes;
use super::{Change, sealed::Sealed};
#[cfg(feature = "plugins")]
use crate::graphrecord::{GraphRecord, plugins::Plugin};
use crate::{
    errors::{GraphRecordError, GraphRecordResult},
    graphrecord::{AttributeMap, datatypes::NodeIndex, state::GraphState},
};
use graphrecords_utils::distinct::Distinct;

pub struct SetNodeAttributes {
    node_indices: Vec<NodeIndex>,
    attributes: AttributeMap,
}

impl SetNodeAttributes {
    #[must_use]
    pub fn new(node_indices: Vec<NodeIndex>, attributes: AttributeMap) -> Self {
        let node_indices: Vec<_> = node_indices.into_iter().collect::<Distinct<_>>().into();

        Self {
            node_indices,
            attributes,
        }
    }

    #[must_use]
    pub fn node_indices(&self) -> &[NodeIndex] {
        &self.node_indices
    }

    #[must_use]
    pub const fn attributes(&self) -> &AttributeMap {
        &self.attributes
    }
}

impl Sealed for SetNodeAttributes {}

impl Change for SetNodeAttributes {
    fn apply(&self, mut state: GraphState) -> GraphRecordResult<GraphState> {
        for node_index in &self.node_indices {
            let address = state.resolve_node_address(node_index).ok_or_else(|| {
                GraphRecordError::NodeNotFound {
                    node_index: node_index.clone(),
                }
            })?;

            state.set_node_attributes(address, &self.attributes)?;
        }

        Ok(state)
    }

    #[cfg(feature = "plugins")]
    fn pre_dispatch(
        self: Box<Self>,
        plugin: &dyn Plugin,
        record: &GraphRecord,
    ) -> GraphRecordResult<Changes> {
        plugin.pre_set_node_attributes(record, *self)
    }

    #[cfg(feature = "plugins")]
    fn post_dispatch(
        &self,
        plugin: &dyn Plugin,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        plugin.post_set_node_attributes(previous, candidate, self)
    }
}

#[cfg(test)]
mod test {
    use super::SetNodeAttributes;
    use crate::graphrecord::{AttributeMap, datatypes::NodeIndex};

    #[test]
    fn test_new() {
        let assignment = SetNodeAttributes::new(
            vec!["lorem".into(), "ipsum".into(), "lorem".into()],
            AttributeMap::from([("sed".into(), 1.into())]),
        );

        assert_eq!(
            vec![NodeIndex::from("lorem"), NodeIndex::from("ipsum")],
            assignment.node_indices()
        );
        assert_eq!(
            &AttributeMap::from([("sed".into(), 1.into())]),
            assignment.attributes()
        );
    }
}
