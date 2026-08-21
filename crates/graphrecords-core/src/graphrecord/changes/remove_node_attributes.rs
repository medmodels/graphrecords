#[cfg(feature = "plugins")]
use super::Changes;
use super::{Change, sealed::Sealed};
#[cfg(feature = "plugins")]
use crate::graphrecord::{GraphRecord, plugins::Plugin};
use crate::{
    errors::{GraphRecordError, GraphRecordResult},
    graphrecord::{
        datatypes::{AttributeName, NodeIndex},
        state::GraphState,
    },
};
use graphrecords_utils::distinct::Distinct;

pub struct RemoveNodeAttributes {
    node_indices: Vec<NodeIndex>,
    attribute_names: Vec<AttributeName>,
}

impl RemoveNodeAttributes {
    #[must_use]
    pub fn new(node_indices: Vec<NodeIndex>, attribute_names: Vec<AttributeName>) -> Self {
        let node_indices: Vec<_> = node_indices.into_iter().collect::<Distinct<_>>().into();
        let attribute_names: Vec<_> = attribute_names.into_iter().collect::<Distinct<_>>().into();

        Self {
            node_indices,
            attribute_names,
        }
    }

    #[must_use]
    pub fn node_indices(&self) -> &[NodeIndex] {
        &self.node_indices
    }

    #[must_use]
    pub fn attribute_names(&self) -> &[AttributeName] {
        &self.attribute_names
    }
}

impl Sealed for RemoveNodeAttributes {}

impl Change for RemoveNodeAttributes {
    fn apply(self: Box<Self>, mut state: GraphState) -> GraphRecordResult<GraphState> {
        let Self {
            node_indices,
            attribute_names,
        } = *self;

        for node_index in node_indices {
            let address = state
                .resolve_node_address(&node_index)
                .ok_or(GraphRecordError::NodeNotFound { node_index })?;

            for attribute_name in &attribute_names {
                state.remove_node_attribute(address, attribute_name)?;
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
        plugin.on_remove_node_attributes(record, *self)
    }

    #[cfg(feature = "plugins")]
    fn post_dispatch_hook(
        &self,
    ) -> fn(
        plugin: &dyn Plugin,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        |plugin, previous, candidate| plugin.post_remove_node_attributes(previous, candidate)
    }
}

#[cfg(test)]
mod test {
    use super::RemoveNodeAttributes;
    use crate::graphrecord::datatypes::{AttributeName, NodeIndex};

    #[test]
    fn test_new() {
        let removal = RemoveNodeAttributes::new(
            vec!["lorem".into(), "ipsum".into(), "lorem".into()],
            vec!["sed".into(), "amet".into(), "sed".into()],
        );

        assert_eq!(
            vec![NodeIndex::from("lorem"), NodeIndex::from("ipsum")],
            removal.node_indices()
        );
        assert_eq!(
            vec![AttributeName::from("sed"), AttributeName::from("amet")],
            removal.attribute_names()
        );
    }
}
