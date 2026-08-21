#[cfg(feature = "plugins")]
use super::Changes;
use super::{Change, sealed::Sealed};
#[cfg(feature = "plugins")]
use crate::graphrecord::{GraphRecord, plugins::Plugin};
use crate::{
    errors::GraphRecordResult,
    graphrecord::{
        datatypes::{Identifier, NodeIndex},
        schema::{Schema, SchemaType},
        state::GraphState,
    },
};
use graphrecords_utils::aliases::GrHashSet;

pub struct SetSchema {
    schema: Schema,
}

impl SetSchema {
    #[must_use]
    pub const fn new(schema: Schema) -> Self {
        Self { schema }
    }

    #[must_use]
    pub const fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl Sealed for SetSchema {}

impl Change for SetSchema {
    fn apply(self: Box<Self>, mut state: GraphState) -> GraphRecordResult<GraphState> {
        let mut schema = self.schema;

        let mut visited_node_groups = GrHashSet::new();
        let mut ungrouped_nodes_visited = false;

        for node_address in state.node_addresses() {
            let attributes = state.node_attribute_map(node_address);
            let group_indices: Vec<_> = state
                .node_memberships(node_address)
                .filter_map(|group_address| state.group_index(group_address).cloned())
                .collect();

            if group_indices.is_empty() {
                match schema.schema_type() {
                    SchemaType::Inferred => {
                        let population_was_empty = !ungrouped_nodes_visited;

                        schema.update_node(&attributes, None, population_was_empty);
                        ungrouped_nodes_visited = true;
                    }
                    SchemaType::Provided => {
                        let identifier = Identifier::from(
                            state.node_key(node_address).expect("Node must exist."),
                        );
                        let node_index = NodeIndex::from(identifier);

                        schema.validate_node(&node_index, &attributes, None)?;
                    }
                }

                continue;
            }

            for group_index in group_indices {
                match schema.schema_type() {
                    SchemaType::Inferred => {
                        let population_was_empty = visited_node_groups.insert(group_index.clone());

                        schema.update_node(&attributes, Some(&group_index), population_was_empty);
                    }
                    SchemaType::Provided => {
                        let identifier = Identifier::from(
                            state.node_key(node_address).expect("Node must exist."),
                        );
                        let node_index = NodeIndex::from(identifier);

                        schema.validate_node(&node_index, &attributes, Some(&group_index))?;
                    }
                }
            }
        }

        let mut visited_edge_groups = GrHashSet::new();
        let mut ungrouped_edges_visited = false;

        for edge_address in state.edge_addresses() {
            let attributes = state.edge_attribute_map(edge_address);
            let group_indices: Vec<_> = state
                .edge_memberships(edge_address)
                .filter_map(|group_address| state.group_index(group_address).cloned())
                .collect();

            if group_indices.is_empty() {
                match schema.schema_type() {
                    SchemaType::Inferred => {
                        let population_was_empty = !ungrouped_edges_visited;

                        schema.update_edge(&attributes, None, population_was_empty);
                        ungrouped_edges_visited = true;
                    }
                    SchemaType::Provided => {
                        let edge_index = state.edge_index(edge_address).expect("Edge must exist.");

                        schema.validate_edge(&edge_index, &attributes, None)?;
                    }
                }

                continue;
            }

            for group_index in group_indices {
                match schema.schema_type() {
                    SchemaType::Inferred => {
                        let population_was_empty = visited_edge_groups.insert(group_index.clone());

                        schema.update_edge(&attributes, Some(&group_index), population_was_empty);
                    }
                    SchemaType::Provided => {
                        let edge_index = state.edge_index(edge_address).expect("Edge must exist.");

                        schema.validate_edge(&edge_index, &attributes, Some(&group_index))?;
                    }
                }
            }
        }

        state.replace_schema(schema);

        Ok(state)
    }

    #[cfg(feature = "plugins")]
    fn dispatch(
        self: Box<Self>,
        plugin: &dyn Plugin,
        record: &GraphRecord,
    ) -> GraphRecordResult<Changes> {
        plugin.on_set_schema(record, *self)
    }

    #[cfg(feature = "plugins")]
    fn post_dispatch_hook(
        &self,
    ) -> fn(
        plugin: &dyn Plugin,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        |plugin, previous, candidate| plugin.post_set_schema(previous, candidate)
    }
}
