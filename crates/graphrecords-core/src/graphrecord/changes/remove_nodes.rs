#[cfg(feature = "plugins")]
use super::Changes;
use super::{Change, sealed::Sealed};
#[cfg(feature = "plugins")]
use crate::graphrecord::{GraphRecord, plugins::Plugin};
use crate::{
    errors::{GraphRecordError, GraphRecordResult},
    graphrecord::{datatypes::NodeIndex, state::GraphState},
};
use graphrecords_utils::distinct::Distinct;

pub struct RemoveNodes {
    node_indices: Vec<NodeIndex>,
}

impl RemoveNodes {
    #[must_use]
    pub fn new(node_indices: Vec<NodeIndex>) -> Self {
        let node_indices: Vec<_> = node_indices.into_iter().collect::<Distinct<_>>().into();

        Self { node_indices }
    }

    #[must_use]
    pub fn node_indices(&self) -> &[NodeIndex] {
        &self.node_indices
    }
}

impl Sealed for RemoveNodes {}

impl Change for RemoveNodes {
    fn apply(&self, mut state: GraphState) -> GraphRecordResult<GraphState> {
        for node_index in &self.node_indices {
            let address = state.resolve_node_address(node_index).ok_or_else(|| {
                GraphRecordError::NodeNotFound {
                    node_index: node_index.clone(),
                }
            })?;

            state.remove_node(address);
        }

        Ok(state)
    }

    #[cfg(feature = "plugins")]
    fn pre_dispatch(
        self: Box<Self>,
        plugin: &dyn Plugin,
        record: &GraphRecord,
    ) -> GraphRecordResult<Changes> {
        plugin.pre_remove_nodes(record, *self)
    }

    #[cfg(feature = "plugins")]
    fn post_dispatch(
        &self,
        plugin: &dyn Plugin,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        plugin.post_remove_nodes(previous, candidate, self)
    }
}

#[cfg(test)]
mod test {
    use super::RemoveNodes;
    use crate::graphrecord::datatypes::NodeIndex;

    #[test]
    fn test_new() {
        let removal = RemoveNodes::new(vec!["lorem".into(), "ipsum".into(), "lorem".into()]);

        assert_eq!(
            vec![NodeIndex::from("lorem"), NodeIndex::from("ipsum")],
            removal.node_indices()
        );
    }
}
