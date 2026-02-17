pub mod attributes;
pub mod datatypes;
mod graph;
mod group_mapping;
pub mod overview;
#[cfg(feature = "plugins")]
pub mod plugins;
mod polars;
pub mod querying;
pub mod schema;

pub use self::{
    datatypes::{GraphRecordAttribute, GraphRecordValue},
    graph::{Attributes, EdgeIndex, NodeIndex},
    group_mapping::Group,
};
use crate::{
    errors::GraphRecordError,
    graphrecord::{
        attributes::{EdgeAttributesMut, NodeAttributesMut},
        overview::{DEFAULT_TRUNCATE_DETAILS, GroupOverview, Overview},
        plugins::PreSetSchemaContext,
        polars::DataFramesExport,
    },
};
use ::polars::frame::DataFrame;
use graph::Graph;
use graphrecords_utils::aliases::GrHashSet;
use group_mapping::GroupMapping;
#[cfg(feature = "plugins")]
use plugins::Plugin;
use polars::{dataframe_to_edges, dataframe_to_nodes};
use querying::{
    ReturnOperand, Selection, edges::EdgeOperand, nodes::NodeOperand, wrapper::Wrapper,
};
use schema::{GroupSchema, Schema, SchemaType};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "plugins")]
use std::sync::Arc;
use std::{
    collections::{HashMap, hash_map::Entry},
    fmt::{Display, Formatter},
    mem,
};
#[cfg(feature = "serde")]
use std::{fs, path::Path};

pub struct NodeDataFrameInput {
    dataframe: DataFrame,
    index_column: String,
}

pub struct EdgeDataFrameInput {
    dataframe: DataFrame,
    source_index_column: String,
    target_index_column: String,
}

impl<D, S> From<(D, S)> for NodeDataFrameInput
where
    D: Into<DataFrame>,
    S: Into<String>,
{
    fn from(val: (D, S)) -> Self {
        Self {
            dataframe: val.0.into(),
            index_column: val.1.into(),
        }
    }
}

impl<D, S> From<(D, S, S)> for EdgeDataFrameInput
where
    D: Into<DataFrame>,
    S: Into<String>,
{
    fn from(val: (D, S, S)) -> Self {
        Self {
            dataframe: val.0.into(),
            source_index_column: val.1.into(),
            target_index_column: val.2.into(),
        }
    }
}

fn node_dataframes_to_tuples(
    nodes_dataframes: impl IntoIterator<Item = impl Into<NodeDataFrameInput>>,
) -> Result<Vec<(NodeIndex, Attributes)>, GraphRecordError> {
    let nodes = nodes_dataframes
        .into_iter()
        .map(|dataframe_input| {
            let dataframe_input = dataframe_input.into();

            dataframe_to_nodes(dataframe_input.dataframe, &dataframe_input.index_column)
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect();

    Ok(nodes)
}

#[allow(clippy::type_complexity)]
fn dataframes_to_tuples(
    nodes_dataframes: impl IntoIterator<Item = impl Into<NodeDataFrameInput>>,
    edges_dataframes: impl IntoIterator<Item = impl Into<EdgeDataFrameInput>>,
) -> Result<
    (
        Vec<(NodeIndex, Attributes)>,
        Vec<(NodeIndex, NodeIndex, Attributes)>,
    ),
    GraphRecordError,
> {
    let nodes = node_dataframes_to_tuples(nodes_dataframes)?;

    let edges = edges_dataframes
        .into_iter()
        .map(|dataframe_input| {
            let dataframe_input = dataframe_input.into();

            dataframe_to_edges(
                dataframe_input.dataframe,
                &dataframe_input.source_index_column,
                &dataframe_input.target_index_column,
            )
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect();

    Ok((nodes, edges))
}

#[derive(Default, Debug, Clone)]
#[allow(clippy::unsafe_derive_deserialize)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct GraphRecord {
    graph: Graph,
    group_mapping: GroupMapping,
    schema: Schema,
    #[cfg(feature = "plugins")]
    plugins: Arc<Vec<Box<dyn Plugin>>>,
}

impl Display for GraphRecord {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let overview = Overview::new(self, Some(DEFAULT_TRUNCATE_DETAILS))
            .map_err(|_| std::fmt::Error)?
            .to_string();

        write!(f, "{overview}")
    }
}

impl GraphRecord {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_schema(schema: Schema) -> Self {
        Self {
            schema,
            ..Default::default()
        }
    }

    #[must_use]
    pub fn with_capacity(nodes: usize, edges: usize, schema: Option<Schema>) -> Self {
        Self {
            graph: Graph::with_capacity(nodes, edges),
            schema: schema.unwrap_or_default(),
            ..Default::default()
        }
    }

    #[cfg(feature = "plugins")]
    #[must_use]
    pub fn with_plugins(plugins: Vec<Box<dyn Plugin>>) -> Self {
        let mut graphrecord = Self {
            plugins: Arc::new(plugins),
            ..Default::default()
        };

        let plugins = graphrecord.plugins.clone();

        plugins
            .iter()
            .for_each(|plugin| plugin.initialize(&mut graphrecord));

        graphrecord
    }

    pub fn from_tuples(
        nodes: Vec<(NodeIndex, Attributes)>,
        edges: Option<Vec<(NodeIndex, NodeIndex, Attributes)>>,
        schema: Option<Schema>,
    ) -> Result<Self, GraphRecordError> {
        let mut graphrecord = Self::with_capacity(
            nodes.len(),
            edges.as_ref().map_or(0, std::vec::Vec::len),
            schema,
        );

        for (node_index, attributes) in nodes {
            graphrecord.add_node(node_index, attributes)?;
        }

        if let Some(edges) = edges {
            for (source_node_index, target_node_index, attributes) in edges {
                graphrecord.add_edge(source_node_index, target_node_index, attributes)?;
            }
        }

        Ok(graphrecord)
    }

    pub fn from_dataframes(
        nodes_dataframes: impl IntoIterator<Item = impl Into<NodeDataFrameInput>>,
        edges_dataframes: impl IntoIterator<Item = impl Into<EdgeDataFrameInput>>,
        schema: Option<Schema>,
    ) -> Result<Self, GraphRecordError> {
        let (nodes, edges) = dataframes_to_tuples(nodes_dataframes, edges_dataframes)?;

        Self::from_tuples(nodes, Some(edges), schema)
    }

    pub fn from_nodes_dataframes(
        nodes_dataframes: impl IntoIterator<Item = impl Into<NodeDataFrameInput>>,
        schema: Option<Schema>,
    ) -> Result<Self, GraphRecordError> {
        let nodes = node_dataframes_to_tuples(nodes_dataframes)?;

        Self::from_tuples(nodes, None, schema)
    }

    #[cfg(feature = "serde")]
    pub fn from_ron<P>(path: P) -> Result<Self, GraphRecordError>
    where
        P: AsRef<Path>,
    {
        let file = fs::read_to_string(&path)
            .map_err(|_| GraphRecordError::ConversionError("Failed to read file".to_string()))?;

        ron::from_str(&file).map_err(|_| {
            GraphRecordError::ConversionError(
                "Failed to create GraphRecord from contents from file".to_string(),
            )
        })
    }

    #[cfg(feature = "serde")]
    pub fn to_ron<P>(&self, path: P) -> Result<(), GraphRecordError>
    where
        P: AsRef<Path>,
    {
        let ron_string = ron::to_string(self).map_err(|_| {
            GraphRecordError::ConversionError("Failed to convert GraphRecord to ron".to_string())
        })?;

        if let Some(parent) = path.as_ref().parent() {
            fs::create_dir_all(parent).map_err(|_| {
                GraphRecordError::ConversionError(
                    "Failed to create folders to GraphRecord save path".to_string(),
                )
            })?;
        }

        fs::write(&path, ron_string).map_err(|_| {
            GraphRecordError::ConversionError(
                "Failed to save GraphRecord due to file error".to_string(),
            )
        })
    }

    pub fn to_dataframes(&self) -> Result<DataFramesExport, GraphRecordError> {
        DataFramesExport::new(self)
    }

    #[allow(clippy::too_many_lines)]
    pub fn set_schema_bypass_plugins(
        &mut self,
        mut schema: Schema,
    ) -> Result<(), GraphRecordError> {
        let mut nodes_group_cache = HashMap::<&Group, usize>::new();
        let mut nodes_ungrouped_visited = false;
        let mut edges_group_cache = HashMap::<&Group, usize>::new();
        let mut edges_ungrouped_visited = false;

        for (node_index, node) in &self.graph.nodes {
            #[expect(clippy::missing_panics_doc, reason = "infallible")]
            let groups_of_node: Vec<_> = self
                .groups_of_node(node_index)
                .expect("groups of node must exist")
                .collect();

            if groups_of_node.is_empty() {
                match schema.schema_type() {
                    SchemaType::Inferred => {
                        let nodes_in_groups = self.group_mapping.nodes_in_group.len();

                        let nodes_not_in_groups = self.graph.node_count() - nodes_in_groups;

                        schema.update_node(
                            &node.attributes,
                            None,
                            nodes_not_in_groups == 0 || !nodes_ungrouped_visited,
                        );

                        nodes_ungrouped_visited = true;
                    }
                    SchemaType::Provided => {
                        schema.validate_node(node_index, &node.attributes, None)?;
                    }
                }
            } else {
                for group in groups_of_node {
                    match schema.schema_type() {
                        SchemaType::Inferred => match nodes_group_cache.entry(group) {
                            Entry::Occupied(entry) => {
                                schema.update_node(
                                    &node.attributes,
                                    Some(group),
                                    *entry.get() == 0,
                                );
                            }
                            Entry::Vacant(entry) => {
                                entry.insert(
                                    self.group_mapping
                                        .nodes_in_group
                                        .get(group)
                                        .map_or(0, GrHashSet::len),
                                );

                                schema.update_node(&node.attributes, Some(group), true);
                            }
                        },
                        SchemaType::Provided => {
                            schema.validate_node(node_index, &node.attributes, Some(group))?;
                        }
                    }
                }
            }
        }

        for (edge_index, edge) in &self.graph.edges {
            #[expect(clippy::missing_panics_doc, reason = "infallible")]
            let groups_of_edge: Vec<_> = self
                .groups_of_edge(edge_index)
                .expect("groups of edge must exist")
                .collect();

            if groups_of_edge.is_empty() {
                match schema.schema_type() {
                    SchemaType::Inferred => {
                        let edges_in_groups = self.group_mapping.edges_in_group.len();

                        let edges_not_in_groups = self.graph.edge_count() - edges_in_groups;

                        schema.update_edge(
                            &edge.attributes,
                            None,
                            edges_not_in_groups == 0 || !edges_ungrouped_visited,
                        );

                        edges_ungrouped_visited = true;
                    }
                    SchemaType::Provided => {
                        schema.validate_edge(edge_index, &edge.attributes, None)?;
                    }
                }
            } else {
                for group in groups_of_edge {
                    match schema.schema_type() {
                        SchemaType::Inferred => match edges_group_cache.entry(group) {
                            Entry::Occupied(entry) => {
                                schema.update_edge(
                                    &edge.attributes,
                                    Some(group),
                                    *entry.get() == 0,
                                );
                            }
                            Entry::Vacant(entry) => {
                                entry.insert(
                                    self.group_mapping
                                        .edges_in_group
                                        .get(group)
                                        .map_or(0, GrHashSet::len),
                                );

                                schema.update_edge(&edge.attributes, Some(group), true);
                            }
                        },
                        SchemaType::Provided => {
                            schema.validate_edge(edge_index, &edge.attributes, Some(group))?;
                        }
                    }
                }
            }
        }

        mem::swap(&mut self.schema, &mut schema);

        Ok(())
    }

    pub fn set_schema(&mut self, schema: Schema) -> Result<(), GraphRecordError> {
        let context = PreSetSchemaContext { schema };
        let plugins = self.plugins.clone();
        let context = plugins.iter().fold(context, |context, plugin| {
            plugin.pre_set_schema(context, self)
        });

        self.set_schema_bypass_plugins(context.schema)?;

        plugins
            .iter()
            .for_each(|plugin| plugin.post_set_schema(self));

        Ok(())
    }

    /// # Safety
    ///
    /// This function should only be used if the data has been validated against the schema.
    /// Using this function with invalid data may lead to undefined behavior.
    /// This function does not run any plugin hooks.
    pub const unsafe fn set_schema_unchecked(&mut self, schema: &mut Schema) {
        mem::swap(&mut self.schema, schema);
    }

    #[must_use]
    pub const fn get_schema(&self) -> &Schema {
        &self.schema
    }

    pub const fn freeze_schema(&mut self) {
        self.schema.freeze();
    }

    pub const fn unfreeze_schema(&mut self) {
        self.schema.unfreeze();
    }

    pub fn node_indices(&self) -> impl Iterator<Item = &NodeIndex> {
        self.graph.node_indices()
    }

    pub fn node_attributes(&self, node_index: &NodeIndex) -> Result<&Attributes, GraphRecordError> {
        self.graph
            .node_attributes(node_index)
            .map_err(GraphRecordError::from)
    }

    pub fn node_attributes_mut<'a>(
        &'a mut self,
        node_index: &'a NodeIndex,
    ) -> Result<NodeAttributesMut<'a>, GraphRecordError> {
        NodeAttributesMut::new(node_index, self)
    }

    pub fn outgoing_edges(
        &self,
        node_index: &NodeIndex,
    ) -> Result<impl Iterator<Item = &EdgeIndex> + use<'_>, GraphRecordError> {
        self.graph
            .outgoing_edges(node_index)
            .map_err(GraphRecordError::from)
    }

    pub fn incoming_edges(
        &self,
        node_index: &NodeIndex,
    ) -> Result<impl Iterator<Item = &EdgeIndex> + use<'_>, GraphRecordError> {
        self.graph
            .incoming_edges(node_index)
            .map_err(GraphRecordError::from)
    }

    pub fn edge_indices(&self) -> impl Iterator<Item = &EdgeIndex> {
        self.graph.edge_indices()
    }

    pub fn edge_attributes(&self, edge_index: &EdgeIndex) -> Result<&Attributes, GraphRecordError> {
        self.graph
            .edge_attributes(edge_index)
            .map_err(GraphRecordError::from)
    }

    pub fn edge_attributes_mut<'a>(
        &'a mut self,
        edge_index: &'a EdgeIndex,
    ) -> Result<EdgeAttributesMut<'a>, GraphRecordError> {
        EdgeAttributesMut::new(edge_index, self)
    }

    pub fn edge_endpoints(
        &self,
        edge_index: &EdgeIndex,
    ) -> Result<(&NodeIndex, &NodeIndex), GraphRecordError> {
        self.graph
            .edge_endpoints(edge_index)
            .map_err(GraphRecordError::from)
    }

    pub fn edges_connecting<'a>(
        &'a self,
        outgoing_node_indices: Vec<&'a NodeIndex>,
        incoming_node_indices: Vec<&'a NodeIndex>,
    ) -> impl Iterator<Item = &'a EdgeIndex> + 'a {
        self.graph
            .edges_connecting(outgoing_node_indices, incoming_node_indices)
    }

    pub fn edges_connecting_undirected<'a>(
        &'a self,
        first_node_indices: Vec<&'a NodeIndex>,
        second_node_indices: Vec<&'a NodeIndex>,
    ) -> impl Iterator<Item = &'a EdgeIndex> + 'a {
        self.graph
            .edges_connecting_undirected(first_node_indices, second_node_indices)
    }

    pub fn add_node(
        &mut self,
        node_index: NodeIndex,
        attributes: Attributes,
    ) -> Result<(), GraphRecordError> {
        match self.schema.schema_type() {
            SchemaType::Inferred => {
                let nodes_in_groups = self.group_mapping.nodes_in_group.len();

                let nodes_not_in_groups = self.graph.node_count() - nodes_in_groups;

                self.schema
                    .update_node(&attributes, None, nodes_not_in_groups == 0);
            }
            SchemaType::Provided => {
                self.schema.validate_node(&node_index, &attributes, None)?;
            }
        }

        self.graph
            .add_node(node_index, attributes)
            .map_err(GraphRecordError::from)
    }

    // TODO: Add tests
    #[allow(clippy::needless_pass_by_value)]
    pub fn add_node_with_group(
        &mut self,
        node_index: NodeIndex,
        attributes: Attributes,
        group: Group,
    ) -> Result<(), GraphRecordError> {
        match self.schema.schema_type() {
            SchemaType::Inferred => {
                let nodes_in_group = self
                    .group_mapping
                    .nodes_in_group
                    .get(&group)
                    .map_or(0, GrHashSet::len);

                self.schema
                    .update_node(&attributes, Some(&group), nodes_in_group == 0);
            }
            SchemaType::Provided => {
                self.schema
                    .validate_node(&node_index, &attributes, Some(&group))?;
            }
        }

        self.graph
            .add_node(node_index.clone(), attributes)
            .map_err(GraphRecordError::from)?;

        self.group_mapping
            .add_node_to_group(group, node_index.clone())
            .inspect_err(|_| {
                #[expect(clippy::missing_panics_doc, reason = "infallible")]
                self.graph
                    .remove_node(&node_index, &mut self.group_mapping)
                    .expect("Node must exist");
            })
    }

    pub fn remove_node(&mut self, node_index: &NodeIndex) -> Result<Attributes, GraphRecordError> {
        self.group_mapping.remove_node(node_index);

        self.graph
            .remove_node(node_index, &mut self.group_mapping)
            .map_err(GraphRecordError::from)
    }

    pub fn add_nodes(
        &mut self,
        nodes: Vec<(NodeIndex, Attributes)>,
    ) -> Result<(), GraphRecordError> {
        for (node_index, attributes) in nodes {
            self.add_node(node_index, attributes)?;
        }

        Ok(())
    }

    // TODO: Add tests
    #[allow(clippy::needless_pass_by_value)]
    pub fn add_nodes_with_group(
        &mut self,
        nodes: Vec<(NodeIndex, Attributes)>,
        group: Group,
    ) -> Result<(), GraphRecordError> {
        if !self.contains_group(&group) {
            self.add_group(group.clone(), None, None)?;
        }

        for (node_index, attributes) in nodes {
            self.add_node_with_group(node_index, attributes, group.clone())?;
        }

        Ok(())
    }

    pub fn add_nodes_dataframes(
        &mut self,
        nodes_dataframes: impl IntoIterator<Item = impl Into<NodeDataFrameInput>>,
    ) -> Result<(), GraphRecordError> {
        let nodes = nodes_dataframes
            .into_iter()
            .map(|dataframe_input| {
                let dataframe_input = dataframe_input.into();

                dataframe_to_nodes(dataframe_input.dataframe, &dataframe_input.index_column)
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect();

        self.add_nodes(nodes)
    }

    // TODO: Add tests
    pub fn add_nodes_dataframes_with_group(
        &mut self,
        nodes_dataframes: impl IntoIterator<Item = impl Into<NodeDataFrameInput>>,
        group: Group,
    ) -> Result<(), GraphRecordError> {
        let nodes = nodes_dataframes
            .into_iter()
            .map(|dataframe_input| {
                let dataframe_input = dataframe_input.into();

                dataframe_to_nodes(dataframe_input.dataframe, &dataframe_input.index_column)
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect();

        self.add_nodes_with_group(nodes, group)
    }

    #[allow(clippy::needless_pass_by_value)]
    pub fn add_edge(
        &mut self,
        source_node_index: NodeIndex,
        target_node_index: NodeIndex,
        attributes: Attributes,
    ) -> Result<EdgeIndex, GraphRecordError> {
        let edge_index = self
            .graph
            .add_edge(source_node_index, target_node_index, attributes.clone())
            .map_err(GraphRecordError::from)?;

        match self.schema.schema_type() {
            SchemaType::Inferred => {
                let edges_in_groups = self.group_mapping.edges_in_group.len();

                let edges_not_in_groups = self.graph.edge_count() - edges_in_groups;

                self.schema
                    .update_edge(&attributes, None, edges_not_in_groups <= 1);

                Ok(edge_index)
            }
            SchemaType::Provided => {
                match self.schema.validate_edge(&edge_index, &attributes, None) {
                    Ok(()) => Ok(edge_index),
                    Err(e) => {
                        #[expect(clippy::missing_panics_doc, reason = "infallible")]
                        self.graph
                            .remove_edge(&edge_index)
                            .expect("Edge must exist");

                        Err(e.into())
                    }
                }
            }
        }
    }

    // TODO: Add tests
    #[allow(clippy::needless_pass_by_value)]
    pub fn add_edge_with_group(
        &mut self,
        source_node_index: NodeIndex,
        target_node_index: NodeIndex,
        attributes: Attributes,
        group: Group,
    ) -> Result<EdgeIndex, GraphRecordError> {
        let edge_index = self
            .graph
            .add_edge(source_node_index, target_node_index, attributes.clone())
            .map_err(GraphRecordError::from)?;

        match self.schema.schema_type() {
            SchemaType::Inferred => {
                let edges_in_group = self
                    .group_mapping
                    .edges_in_group
                    .get(&group)
                    .map_or(0, GrHashSet::len);

                self.schema
                    .update_edge(&attributes, Some(&group), edges_in_group == 0);
            }
            SchemaType::Provided => {
                self.schema
                    .validate_edge(&edge_index, &attributes, Some(&group))
                    .inspect_err(|_| {
                        #[expect(clippy::missing_panics_doc, reason = "infallible")]
                        self.graph
                            .remove_edge(&edge_index)
                            .expect("Edge must exist");
                    })?;
            }
        }

        self.group_mapping
            .add_edge_to_group(group, edge_index)
            .inspect_err(|_| {
                #[expect(clippy::missing_panics_doc, reason = "infallible")]
                self.graph
                    .remove_edge(&edge_index)
                    .expect("Edge must exist");
            })?;

        Ok(edge_index)
    }

    pub fn remove_edge(&mut self, edge_index: &EdgeIndex) -> Result<Attributes, GraphRecordError> {
        self.group_mapping.remove_edge(edge_index);

        self.graph
            .remove_edge(edge_index)
            .map_err(GraphRecordError::from)
    }

    pub fn add_edges(
        &mut self,
        edges: Vec<(NodeIndex, NodeIndex, Attributes)>,
    ) -> Result<Vec<EdgeIndex>, GraphRecordError> {
        edges
            .into_iter()
            .map(|(source_edge_index, target_node_index, attributes)| {
                self.add_edge(source_edge_index, target_node_index, attributes)
            })
            .collect()
    }

    // TODO: Add tests
    pub fn add_edges_with_group(
        &mut self,
        edges: Vec<(NodeIndex, NodeIndex, Attributes)>,
        group: &Group,
    ) -> Result<Vec<EdgeIndex>, GraphRecordError> {
        if !self.contains_group(group) {
            self.add_group(group.clone(), None, None)?;
        }

        edges
            .into_iter()
            .map(|(source_edge_index, target_node_index, attributes)| {
                self.add_edge_with_group(
                    source_edge_index,
                    target_node_index,
                    attributes,
                    group.clone(),
                )
            })
            .collect()
    }

    pub fn add_edges_dataframes(
        &mut self,
        edges_dataframes: impl IntoIterator<Item = impl Into<EdgeDataFrameInput>>,
    ) -> Result<Vec<EdgeIndex>, GraphRecordError> {
        let edges = edges_dataframes
            .into_iter()
            .map(|dataframe_input| {
                let dataframe_input = dataframe_input.into();

                dataframe_to_edges(
                    dataframe_input.dataframe,
                    &dataframe_input.source_index_column,
                    &dataframe_input.target_index_column,
                )
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect();

        self.add_edges(edges)
    }

    // TODO: Add tests
    pub fn add_edges_dataframes_with_group(
        &mut self,
        edges_dataframes: impl IntoIterator<Item = impl Into<EdgeDataFrameInput>>,
        group: &Group,
    ) -> Result<Vec<EdgeIndex>, GraphRecordError> {
        let edges = edges_dataframes
            .into_iter()
            .map(|dataframe_input| {
                let dataframe_input = dataframe_input.into();

                dataframe_to_edges(
                    dataframe_input.dataframe,
                    &dataframe_input.source_index_column,
                    &dataframe_input.target_index_column,
                )
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect();

        self.add_edges_with_group(edges, group)
    }

    pub fn add_group(
        &mut self,
        group: Group,
        node_indices: Option<Vec<NodeIndex>>,
        edge_indices: Option<Vec<EdgeIndex>>,
    ) -> Result<(), GraphRecordError> {
        if self.group_mapping.contains_group(&group) {
            return Err(GraphRecordError::AssertionError(format!(
                "Group {group} already exists"
            )));
        }

        if let Some(ref node_indices) = node_indices {
            for node_index in node_indices {
                if !self.graph.contains_node(node_index) {
                    return Err(GraphRecordError::IndexError(format!(
                        "Cannot find node with index {node_index}",
                    )));
                }
            }
        }

        if let Some(ref edge_indices) = edge_indices {
            for edge_index in edge_indices {
                if !self.graph.contains_edge(edge_index) {
                    return Err(GraphRecordError::IndexError(format!(
                        "Cannot find edge with index {edge_index}",
                    )));
                }
            }
        }

        match self.schema.schema_type() {
            SchemaType::Inferred => {
                if !self.schema.groups().contains_key(&group) {
                    self.schema
                        .add_group(group.clone(), GroupSchema::default())?;
                }

                if let Some(ref node_indices) = node_indices {
                    let mut empty = true;

                    for node_index in node_indices {
                        let node_attributes = self.graph.node_attributes(node_index)?;

                        self.schema
                            .update_node(node_attributes, Some(&group), empty);

                        empty = false;
                    }
                }

                if let Some(ref edge_indices) = edge_indices {
                    let mut empty = true;

                    for edge_index in edge_indices {
                        let edge_attributes = self.graph.edge_attributes(edge_index)?;

                        self.schema
                            .update_edge(edge_attributes, Some(&group), empty);

                        empty = false;
                    }
                }
            }
            SchemaType::Provided => {
                if !self.schema.groups().contains_key(&group) {
                    return Err(GraphRecordError::SchemaError(format!(
                        "Group {group} is not defined in the schema"
                    )));
                }

                if let Some(ref node_indices) = node_indices {
                    for node_index in node_indices {
                        let node_attributes = self.graph.node_attributes(node_index)?;

                        self.schema
                            .validate_node(node_index, node_attributes, Some(&group))?;
                    }
                }

                if let Some(ref edge_indices) = edge_indices {
                    for edge_index in edge_indices {
                        let edge_attributes = self.graph.edge_attributes(edge_index)?;

                        self.schema
                            .validate_edge(edge_index, edge_attributes, Some(&group))?;
                    }
                }
            }
        }

        #[expect(clippy::missing_panics_doc, reason = "infallible")]
        self.group_mapping
            .add_group(group, node_indices, edge_indices)
            .expect("Group must not exist");

        Ok(())
    }

    pub fn remove_group(&mut self, group: &Group) -> Result<(), GraphRecordError> {
        self.group_mapping.remove_group(group)
    }

    pub fn add_node_to_group(
        &mut self,
        group: Group,
        node_index: NodeIndex,
    ) -> Result<(), GraphRecordError> {
        let node_attributes = self.graph.node_attributes(&node_index)?;

        match self.schema.schema_type() {
            SchemaType::Inferred => {
                let nodes_in_group = self
                    .group_mapping
                    .nodes_in_group
                    .get(&group)
                    .map_or(0, GrHashSet::len);

                self.schema
                    .update_node(node_attributes, Some(&group), nodes_in_group == 0);
            }
            SchemaType::Provided => {
                self.schema
                    .validate_node(&node_index, node_attributes, Some(&group))?;
            }
        }

        self.group_mapping.add_node_to_group(group, node_index)
    }

    pub fn add_edge_to_group(
        &mut self,
        group: Group,
        edge_index: EdgeIndex,
    ) -> Result<(), GraphRecordError> {
        let edge_attributes = self.graph.edge_attributes(&edge_index)?;

        match self.schema.schema_type() {
            SchemaType::Inferred => {
                let edges_in_group = self
                    .group_mapping
                    .edges_in_group
                    .get(&group)
                    .map_or(0, GrHashSet::len);

                self.schema
                    .update_edge(edge_attributes, Some(&group), edges_in_group == 0);
            }
            SchemaType::Provided => {
                self.schema
                    .validate_edge(&edge_index, edge_attributes, Some(&group))?;
            }
        }

        self.group_mapping.add_edge_to_group(group, edge_index)
    }

    pub fn remove_node_from_group(
        &mut self,
        group: &Group,
        node_index: &NodeIndex,
    ) -> Result<(), GraphRecordError> {
        if !self.graph.contains_node(node_index) {
            return Err(GraphRecordError::IndexError(format!(
                "Cannot find node with index {node_index}",
            )));
        }

        self.group_mapping.remove_node_from_group(group, node_index)
    }

    pub fn remove_edge_from_group(
        &mut self,
        group: &Group,
        edge_index: &EdgeIndex,
    ) -> Result<(), GraphRecordError> {
        if !self.graph.contains_edge(edge_index) {
            return Err(GraphRecordError::IndexError(format!(
                "Cannot find edge with index {edge_index}",
            )));
        }

        self.group_mapping.remove_edge_from_group(group, edge_index)
    }

    pub fn groups(&self) -> impl Iterator<Item = &Group> {
        self.group_mapping.groups()
    }

    pub fn nodes_in_group(
        &self,
        group: &Group,
    ) -> Result<impl Iterator<Item = &NodeIndex> + use<'_>, GraphRecordError> {
        self.group_mapping.nodes_in_group(group)
    }

    pub fn ungrouped_nodes(&self) -> impl Iterator<Item = &NodeIndex> {
        let nodes_in_groups: GrHashSet<_> = self
            .groups()
            .flat_map(|group| {
                #[expect(clippy::missing_panics_doc, reason = "infallible")]
                self.nodes_in_group(group).expect("Group must exist")
            })
            .collect();

        self.graph
            .node_indices()
            .filter(move |node_index| !nodes_in_groups.contains(*node_index))
    }

    pub fn edges_in_group(
        &self,
        group: &Group,
    ) -> Result<impl Iterator<Item = &EdgeIndex> + use<'_>, GraphRecordError> {
        self.group_mapping.edges_in_group(group)
    }

    pub fn ungrouped_edges(&self) -> impl Iterator<Item = &EdgeIndex> {
        let edges_in_groups: GrHashSet<_> = self
            .groups()
            .flat_map(|group| {
                #[expect(clippy::missing_panics_doc, reason = "infallible")]
                self.edges_in_group(group).expect("Group must exist")
            })
            .collect();

        self.graph
            .edge_indices()
            .filter(move |edge_index| !edges_in_groups.contains(*edge_index))
    }

    pub fn groups_of_node(
        &self,
        node_index: &NodeIndex,
    ) -> Result<impl Iterator<Item = &Group> + use<'_>, GraphRecordError> {
        if !self.graph.contains_node(node_index) {
            return Err(GraphRecordError::IndexError(format!(
                "Cannot find node with index {node_index}",
            )));
        }

        Ok(self.group_mapping.groups_of_node(node_index))
    }

    pub fn groups_of_edge(
        &self,
        edge_index: &EdgeIndex,
    ) -> Result<impl Iterator<Item = &Group> + use<'_>, GraphRecordError> {
        if !self.graph.contains_edge(edge_index) {
            return Err(GraphRecordError::IndexError(format!(
                "Cannot find edge with index {edge_index}",
            )));
        }

        Ok(self.group_mapping.groups_of_edge(edge_index))
    }

    #[must_use]
    pub fn node_count(&self) -> usize {
        self.graph.node_count()
    }

    #[must_use]
    pub fn edge_count(&self) -> usize {
        self.graph.edge_count()
    }

    #[must_use]
    pub fn group_count(&self) -> usize {
        self.group_mapping.group_count()
    }

    #[must_use]
    pub fn contains_node(&self, node_index: &NodeIndex) -> bool {
        self.graph.contains_node(node_index)
    }

    #[must_use]
    pub fn contains_edge(&self, edge_index: &EdgeIndex) -> bool {
        self.graph.contains_edge(edge_index)
    }

    #[must_use]
    pub fn contains_group(&self, group: &Group) -> bool {
        self.group_mapping.contains_group(group)
    }

    pub fn neighbors_outgoing(
        &self,
        node_index: &NodeIndex,
    ) -> Result<impl Iterator<Item = &NodeIndex> + use<'_>, GraphRecordError> {
        self.graph
            .neighbors_outgoing(node_index)
            .map_err(GraphRecordError::from)
    }

    // TODO: Add tests
    pub fn neighbors_incoming(
        &self,
        node_index: &NodeIndex,
    ) -> Result<impl Iterator<Item = &NodeIndex> + use<'_>, GraphRecordError> {
        self.graph
            .neighbors_incoming(node_index)
            .map_err(GraphRecordError::from)
    }

    pub fn neighbors_undirected(
        &self,
        node_index: &NodeIndex,
    ) -> Result<impl Iterator<Item = &NodeIndex> + use<'_>, GraphRecordError> {
        self.graph
            .neighbors_undirected(node_index)
            .map_err(GraphRecordError::from)
    }

    pub fn clear(&mut self) {
        self.graph.clear();
        self.group_mapping.clear();
    }

    pub fn query_nodes<'a, Q, R>(&'a self, query: Q) -> Selection<'a, R>
    where
        Q: FnOnce(&Wrapper<NodeOperand>) -> R,
        R: ReturnOperand<'a>,
    {
        Selection::new_node(self, query)
    }

    pub fn query_edges<'a, Q, R>(&'a self, query: Q) -> Selection<'a, R>
    where
        Q: FnOnce(&Wrapper<EdgeOperand>) -> R,
        R: ReturnOperand<'a>,
    {
        Selection::new_edge(self, query)
    }

    pub fn overview(&self, truncate_details: Option<usize>) -> Result<Overview, GraphRecordError> {
        Overview::new(self, truncate_details)
    }

    pub fn group_overview(
        &self,
        group: &Group,
        truncate_details: Option<usize>,
    ) -> Result<GroupOverview, GraphRecordError> {
        GroupOverview::new(self, Some(group), truncate_details)
    }
}

#[cfg(test)]
mod test {
    use super::{Attributes, GraphRecord, GraphRecordAttribute, NodeIndex};
    use crate::{
        errors::GraphRecordError,
        graphrecord::{
            SchemaType,
            datatypes::DataType,
            schema::{AttributeSchema, GroupSchema, Schema},
        },
    };
    use polars::prelude::{DataFrame, NamedFrom, PolarsError, Series};
    use std::collections::HashMap;
    #[cfg(feature = "serde")]
    use std::fs;

    fn create_nodes() -> Vec<(NodeIndex, Attributes)> {
        vec![
            (
                "0".into(),
                HashMap::from([("lorem".into(), "ipsum".into())]),
            ),
            (
                "1".into(),
                HashMap::from([("amet".into(), "consectetur".into())]),
            ),
            (
                "2".into(),
                HashMap::from([("adipiscing".into(), "elit".into())]),
            ),
            ("3".into(), HashMap::new()),
        ]
    }

    fn create_edges() -> Vec<(NodeIndex, NodeIndex, Attributes)> {
        vec![
            (
                "0".into(),
                "1".into(),
                HashMap::from([
                    ("sed".into(), "do".into()),
                    ("eiusmod".into(), "tempor".into()),
                ]),
            ),
            (
                "1".into(),
                "0".into(),
                HashMap::from([
                    ("sed".into(), "do".into()),
                    ("eiusmod".into(), "tempor".into()),
                ]),
            ),
            (
                "1".into(),
                "2".into(),
                HashMap::from([("incididunt".into(), "ut".into())]),
            ),
            ("0".into(), "2".into(), HashMap::new()),
        ]
    }

    fn create_nodes_dataframe() -> Result<DataFrame, PolarsError> {
        let s0 = Series::new("index".into(), &["0", "1"]);
        let s1 = Series::new("attribute".into(), &[1, 2]);
        DataFrame::new(2, vec![s0.into(), s1.into()])
    }

    fn create_edges_dataframe() -> Result<DataFrame, PolarsError> {
        let s0 = Series::new("from".into(), &["0", "1"]);
        let s1 = Series::new("to".into(), &["1", "0"]);
        let s2 = Series::new("attribute".into(), &[1, 2]);
        DataFrame::new(2, vec![s0.into(), s1.into(), s2.into()])
    }

    fn create_graphrecord() -> GraphRecord {
        let nodes = create_nodes();
        let edges = create_edges();

        GraphRecord::from_tuples(nodes, Some(edges), None).unwrap()
    }

    #[test]
    fn test_from_tuples() {
        let graphrecord = create_graphrecord();

        assert_eq!(4, graphrecord.node_count());
        assert_eq!(4, graphrecord.edge_count());
    }

    #[test]
    fn test_invalid_from_tuples() {
        let nodes = create_nodes();

        // Adding an edge pointing to a non-existing node should fail
        assert!(
            GraphRecord::from_tuples(
                nodes.clone(),
                Some(vec![("0".into(), "50".into(), HashMap::new())]),
                None
            )
            .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );

        // Adding an edge from a non-existing should fail
        assert!(
            GraphRecord::from_tuples(
                nodes,
                Some(vec![("50".into(), "0".into(), HashMap::new())]),
                None
            )
            .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );
    }

    #[test]
    fn test_from_dataframes() {
        let nodes_dataframe = create_nodes_dataframe().unwrap();
        let edges_dataframe = create_edges_dataframe().unwrap();

        let graphrecord = GraphRecord::from_dataframes(
            vec![(nodes_dataframe, "index".to_string())],
            vec![(edges_dataframe, "from".to_string(), "to".to_string())],
            None,
        )
        .unwrap();

        assert_eq!(2, graphrecord.node_count());
        assert_eq!(2, graphrecord.edge_count());
    }

    #[test]
    fn test_from_nodes_dataframes() {
        let nodes_dataframe = create_nodes_dataframe().unwrap();

        let graphrecord =
            GraphRecord::from_nodes_dataframes(vec![(nodes_dataframe, "index".to_string())], None)
                .unwrap();

        assert_eq!(2, graphrecord.node_count());
    }

    #[test]
    #[cfg(feature = "serde")]
    fn test_ron() {
        let graphrecord = create_graphrecord();

        let mut file_path = std::env::temp_dir().into_os_string();
        file_path.push("/graphrecord_test/");

        fs::create_dir_all(&file_path).unwrap();

        file_path.push("test.ron");

        graphrecord.to_ron(&file_path).unwrap();

        let loaded_graphrecord = GraphRecord::from_ron(&file_path).unwrap();

        assert_eq!(graphrecord.node_count(), loaded_graphrecord.node_count());
        assert_eq!(graphrecord.edge_count(), loaded_graphrecord.edge_count());
    }

    #[test]
    fn test_set_schema() {
        let mut graphrecord = GraphRecord::new();

        let group_schema = GroupSchema::new(
            AttributeSchema::from([("attribute".into(), DataType::Int.into())]),
            AttributeSchema::from([("attribute".into(), DataType::Int.into())]),
        );

        graphrecord
            .add_node("0".into(), HashMap::from([("attribute".into(), 1.into())]))
            .unwrap();
        graphrecord
            .add_node("1".into(), HashMap::from([("attribute".into(), 1.into())]))
            .unwrap();
        graphrecord
            .add_edge(
                "0".into(),
                "1".into(),
                HashMap::from([("attribute".into(), 1.into())]),
            )
            .unwrap();

        let schema = Schema::new_provided(HashMap::default(), group_schema.clone());

        assert!(graphrecord.set_schema(schema.clone()).is_ok());

        assert_eq!(schema, *graphrecord.get_schema());

        let mut graphrecord = GraphRecord::new();

        graphrecord
            .add_node("0".into(), HashMap::from([("attribute".into(), 1.into())]))
            .unwrap();
        graphrecord
            .add_node("1".into(), HashMap::from([("attribute".into(), 1.into())]))
            .unwrap();
        graphrecord
            .add_node("2".into(), HashMap::from([("attribute".into(), 1.into())]))
            .unwrap();
        graphrecord
            .add_edge(
                "0".into(),
                "1".into(),
                HashMap::from([("attribute".into(), 1.into())]),
            )
            .unwrap();
        graphrecord
            .add_edge(
                "0".into(),
                "1".into(),
                HashMap::from([("attribute".into(), 1.into())]),
            )
            .unwrap();
        graphrecord
            .add_edge(
                "0".into(),
                "1".into(),
                HashMap::from([("attribute".into(), 1.into())]),
            )
            .unwrap();

        let schema = Schema::new_inferred(
            HashMap::from([
                ("0".into(), group_schema.clone()),
                ("1".into(), group_schema.clone()),
            ]),
            group_schema,
        );

        graphrecord
            .add_group(
                "0".into(),
                Some(vec!["0".into(), "1".into()]),
                Some(vec![0, 1]),
            )
            .unwrap();
        graphrecord
            .add_group(
                "1".into(),
                Some(vec!["0".into(), "1".into()]),
                Some(vec![0, 1]),
            )
            .unwrap();

        let inferred_schema = Schema::new_inferred(HashMap::default(), GroupSchema::default());

        assert!(graphrecord.set_schema(inferred_schema).is_ok());

        assert_eq!(schema, *graphrecord.get_schema());
    }

    #[test]
    fn test_invalid_set_schema() {
        let mut graphrecord = GraphRecord::new();

        graphrecord
            .add_node("0".into(), HashMap::from([("attribute2".into(), 1.into())]))
            .unwrap();

        let schema = Schema::new_provided(
            HashMap::default(),
            GroupSchema::new(
                AttributeSchema::from([("attribute".into(), DataType::Int.into())]),
                AttributeSchema::from([("attribute".into(), DataType::Int.into())]),
            ),
        );

        let previous_schema = graphrecord.get_schema().clone();

        assert!(
            graphrecord
                .set_schema(schema.clone())
                .is_err_and(|e| { matches!(e, GraphRecordError::SchemaError(_)) })
        );

        assert_eq!(previous_schema, *graphrecord.get_schema());

        let mut graphrecord = GraphRecord::new();

        graphrecord
            .add_node("0".into(), HashMap::from([("attribute".into(), 1.into())]))
            .unwrap();
        graphrecord
            .add_node("1".into(), HashMap::from([("attribute".into(), 1.into())]))
            .unwrap();
        graphrecord
            .add_edge(
                "0".into(),
                "1".into(),
                HashMap::from([("attribute2".into(), 1.into())]),
            )
            .unwrap();

        let previous_schema = graphrecord.get_schema().clone();

        assert!(
            graphrecord
                .set_schema(schema)
                .is_err_and(|e| { matches!(e, GraphRecordError::SchemaError(_)) })
        );

        assert_eq!(previous_schema, *graphrecord.get_schema());
    }

    #[test]
    fn test_freeze_schema() {
        let mut graphrecord = GraphRecord::new();

        assert_eq!(
            SchemaType::Inferred,
            *graphrecord.get_schema().schema_type()
        );

        graphrecord.freeze_schema();

        assert_eq!(
            SchemaType::Provided,
            *graphrecord.get_schema().schema_type()
        );
    }

    #[test]
    fn test_unfreeze_schema() {
        let schema = Schema::new_provided(HashMap::default(), GroupSchema::default());
        let mut graphrecord = GraphRecord::with_schema(schema);

        assert_eq!(
            *graphrecord.get_schema().schema_type(),
            SchemaType::Provided
        );

        graphrecord.unfreeze_schema();

        assert_eq!(
            *graphrecord.get_schema().schema_type(),
            SchemaType::Inferred
        );
    }

    #[test]
    fn test_node_indices() {
        let graphrecord = create_graphrecord();

        let node_indices: Vec<_> = create_nodes()
            .into_iter()
            .map(|(node_index, _)| node_index)
            .collect();

        for node_index in graphrecord.node_indices() {
            assert!(node_indices.contains(node_index));
        }
    }

    #[test]
    fn test_node_attributes() {
        let graphrecord = create_graphrecord();

        let attributes = graphrecord.node_attributes(&"0".into()).unwrap();

        assert_eq!(&create_nodes()[0].1, attributes);
    }

    #[test]
    fn test_invalid_node_attributes() {
        let graphrecord = create_graphrecord();

        // Querying a non-existing node should fail
        assert!(
            graphrecord
                .node_attributes(&"50".into())
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );
    }

    #[test]
    fn test_node_attributes_mut() {
        let mut graphrecord = create_graphrecord();

        let node_index = "0".into();
        let mut attributes = graphrecord.node_attributes_mut(&node_index).unwrap();

        let new_attributes = HashMap::from([("0".into(), "1".into()), ("2".into(), "3".into())]);

        attributes
            .replace_attributes(new_attributes.clone())
            .unwrap();

        assert_eq!(
            &new_attributes,
            graphrecord.node_attributes(&node_index).unwrap()
        );
    }

    #[test]
    fn test_invalid_node_attributes_mut() {
        let mut graphrecord = create_graphrecord();

        // Accessing the node attributes of a non-existing node should fail
        assert!(
            graphrecord
                .node_attributes_mut(&"50".into())
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );
    }

    #[test]
    fn test_outgoing_edges() {
        let graphrecord = create_graphrecord();

        let edges = graphrecord.outgoing_edges(&"0".into()).unwrap();

        assert_eq!(2, edges.count());
    }

    #[test]
    fn test_invalid_outgoing_edges() {
        let graphrecord = create_graphrecord();

        assert!(
            graphrecord
                .outgoing_edges(&"50".into())
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );
    }

    #[test]
    fn test_incoming_edges() {
        let graphrecord = create_graphrecord();

        let edges = graphrecord.incoming_edges(&"2".into()).unwrap();

        assert_eq!(2, edges.count());
    }

    #[test]
    fn test_invalid_incoming_edges() {
        let graphrecord = create_graphrecord();

        assert!(
            graphrecord
                .incoming_edges(&"50".into())
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );
    }

    #[test]
    fn test_edge_indices() {
        let graphrecord = create_graphrecord();
        let edges = [0, 1, 2, 3];

        for edge in graphrecord.edge_indices() {
            assert!(edges.contains(edge));
        }
    }

    #[test]
    fn test_edge_attributes() {
        let graphrecord = create_graphrecord();

        let attributes = graphrecord.edge_attributes(&0).unwrap();

        assert_eq!(&create_edges()[0].2, attributes);
    }

    #[test]
    fn test_invalid_edge_attributes() {
        let graphrecord = create_graphrecord();

        // Querying a non-existing node should fail
        assert!(
            graphrecord
                .edge_attributes(&50)
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );
    }

    #[test]
    fn test_edge_attributes_mut() {
        let mut graphrecord = create_graphrecord();

        let mut attributes = graphrecord.edge_attributes_mut(&0).unwrap();

        let new_attributes = HashMap::from([("0".into(), "1".into()), ("2".into(), "3".into())]);

        attributes
            .replace_attributes(new_attributes.clone())
            .unwrap();

        assert_eq!(&new_attributes, graphrecord.edge_attributes(&0).unwrap());
    }

    #[test]
    fn test_invalid_edge_attributes_mut() {
        let mut graphrecord = create_graphrecord();

        // Accessing the edge attributes of a non-existing edge should fail
        assert!(
            graphrecord
                .edge_attributes_mut(&50)
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );
    }

    #[test]
    fn test_edge_endpoints() {
        let graphrecord = create_graphrecord();

        let edge = &create_edges()[0];

        let endpoints = graphrecord.edge_endpoints(&0).unwrap();

        assert_eq!(&edge.0, endpoints.0);

        assert_eq!(&edge.1, endpoints.1);
    }

    #[test]
    fn test_invalid_edge_endpoints() {
        let graphrecord = create_graphrecord();

        // Accessing the edge endpoints of a non-existing edge should fail
        assert!(
            graphrecord
                .edge_endpoints(&50)
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );
    }

    #[test]
    fn test_edges_connecting() {
        let graphrecord = create_graphrecord();

        let first_index = "0".into();
        let second_index = "1".into();
        let edges_connecting =
            graphrecord.edges_connecting(vec![&first_index], vec![&second_index]);

        assert_eq!(vec![&0], edges_connecting.collect::<Vec<_>>());

        let first_index = "0".into();
        let second_index = "3".into();
        let edges_connecting =
            graphrecord.edges_connecting(vec![&first_index], vec![&second_index]);

        assert_eq!(0, edges_connecting.count());

        let first_index = "0".into();
        let second_index = "1".into();
        let third_index = "2".into();
        let mut edges_connecting: Vec<_> = graphrecord
            .edges_connecting(vec![&first_index, &second_index], vec![&third_index])
            .collect();

        edges_connecting.sort();
        assert_eq!(vec![&2, &3], edges_connecting);

        let first_index = "0".into();
        let second_index = "1".into();
        let third_index = "2".into();
        let fourth_index = "3".into();
        let mut edges_connecting: Vec<_> = graphrecord
            .edges_connecting(
                vec![&first_index, &second_index],
                vec![&third_index, &fourth_index],
            )
            .collect();

        edges_connecting.sort();
        assert_eq!(vec![&2, &3], edges_connecting);
    }

    #[test]
    fn test_edges_connecting_undirected() {
        let graphrecord = create_graphrecord();

        let first_index = "0".into();
        let second_index = "1".into();
        let mut edges_connecting: Vec<_> = graphrecord
            .edges_connecting_undirected(vec![&first_index], vec![&second_index])
            .collect();

        edges_connecting.sort();
        assert_eq!(vec![&0, &1], edges_connecting);
    }

    #[test]
    fn test_add_node() {
        let mut graphrecord = GraphRecord::new();

        assert_eq!(0, graphrecord.node_count());

        graphrecord.add_node("0".into(), HashMap::new()).unwrap();

        assert_eq!(1, graphrecord.node_count());

        graphrecord.freeze_schema();

        graphrecord.add_node("1".into(), HashMap::new()).unwrap();

        assert_eq!(2, graphrecord.node_count());
    }

    #[test]
    fn test_invalid_add_node() {
        let mut graphrecord = create_graphrecord();

        assert!(
            graphrecord
                .add_node("0".into(), HashMap::new())
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );

        graphrecord.freeze_schema();

        assert!(
            graphrecord
                .add_node("4".into(), HashMap::from([("attribute".into(), 1.into())]))
                .is_err_and(|e| matches!(e, GraphRecordError::SchemaError(_)))
        );
    }

    #[test]
    fn test_remove_node() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group("group".into(), Some(vec!["0".into()]), Some(vec![0]))
            .unwrap();

        let nodes = create_nodes();

        assert_eq!(4, graphrecord.node_count());
        assert_eq!(4, graphrecord.edge_count());
        assert_eq!(
            1,
            graphrecord
                .nodes_in_group(&("group".into()))
                .unwrap()
                .count()
        );
        assert_eq!(
            1,
            graphrecord
                .edges_in_group(&("group".into()))
                .unwrap()
                .count()
        );

        assert_eq!(nodes[0].1, graphrecord.remove_node(&"0".into()).unwrap());

        assert_eq!(3, graphrecord.node_count());
        assert_eq!(1, graphrecord.edge_count());
        assert_eq!(
            0,
            graphrecord
                .nodes_in_group(&("group".into()))
                .unwrap()
                .count()
        );
        assert_eq!(
            0,
            graphrecord
                .edges_in_group(&("group".into()))
                .unwrap()
                .count()
        );

        let mut graphrecord = GraphRecord::new();

        graphrecord.add_node(0.into(), HashMap::new()).unwrap();
        graphrecord
            .add_edge(0.into(), 0.into(), HashMap::new())
            .unwrap();

        assert_eq!(1, graphrecord.node_count());
        assert_eq!(1, graphrecord.edge_count());

        assert!(graphrecord.remove_node(&0.into()).is_ok());

        assert_eq!(0, graphrecord.node_count());
        assert_eq!(0, graphrecord.edge_count());
    }

    #[test]
    fn test_invalid_remove_node() {
        let mut graphrecord = create_graphrecord();

        // Removing a non-existing node should fail
        assert!(
            graphrecord
                .remove_node(&"50".into())
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );
    }

    #[test]
    fn test_add_nodes() {
        let mut graphrecord = GraphRecord::new();

        assert_eq!(0, graphrecord.node_count());

        let nodes = create_nodes();

        graphrecord.add_nodes(nodes).unwrap();

        assert_eq!(4, graphrecord.node_count());
    }

    #[test]
    fn test_invalid_add_nodes() {
        let mut graphrecord = create_graphrecord();

        let nodes = create_nodes();

        assert!(
            graphrecord
                .add_nodes(nodes)
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
    }

    #[test]
    fn test_add_nodes_dataframe() {
        let mut graphrecord = GraphRecord::new();

        assert_eq!(0, graphrecord.node_count());

        let nodes_dataframe = create_nodes_dataframe().unwrap();

        graphrecord
            .add_nodes_dataframes(vec![(nodes_dataframe, "index".to_string())])
            .unwrap();

        assert_eq!(2, graphrecord.node_count());
    }

    #[test]
    fn test_add_edge() {
        let mut graphrecord = create_graphrecord();

        assert_eq!(4, graphrecord.edge_count());

        graphrecord
            .add_edge("0".into(), "3".into(), HashMap::new())
            .unwrap();

        assert_eq!(5, graphrecord.edge_count());

        graphrecord.freeze_schema();

        graphrecord
            .add_edge("0".into(), "3".into(), HashMap::new())
            .unwrap();

        assert_eq!(6, graphrecord.edge_count());
    }

    #[test]
    fn test_invalid_add_edge() {
        let mut graphrecord = GraphRecord::new();

        let nodes = create_nodes();

        graphrecord.add_nodes(nodes).unwrap();

        // Adding an edge pointing to a non-existing node should fail
        assert!(
            graphrecord
                .add_edge("0".into(), "50".into(), HashMap::new())
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );

        // Adding an edge from a non-existing node should fail
        assert!(
            graphrecord
                .add_edge("50".into(), "0".into(), HashMap::new())
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );

        graphrecord.freeze_schema();

        assert!(
            graphrecord
                .add_edge(
                    "0".into(),
                    "3".into(),
                    HashMap::from([("attribute".into(), 1.into())])
                )
                .is_err_and(|e| matches!(e, GraphRecordError::SchemaError(_)))
        );
    }

    #[test]
    fn test_remove_edge() {
        let mut graphrecord = create_graphrecord();

        let edges = create_edges();

        assert_eq!(edges[0].2, graphrecord.remove_edge(&0).unwrap());
    }

    #[test]
    fn test_invalid_remove_edge() {
        let mut graphrecord = create_graphrecord();

        // Removing a non-existing edge should fail
        assert!(
            graphrecord
                .remove_edge(&50)
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );
    }

    #[test]
    fn test_add_edges() {
        let mut graphrecord = GraphRecord::new();

        let nodes = create_nodes();

        graphrecord.add_nodes(nodes).unwrap();

        assert_eq!(0, graphrecord.edge_count());

        let edges = create_edges();

        graphrecord.add_edges(edges).unwrap();

        assert_eq!(4, graphrecord.edge_count());
    }

    #[test]
    fn test_add_edges_dataframe() {
        let mut graphrecord = GraphRecord::new();

        let nodes = create_nodes();

        graphrecord.add_nodes(nodes).unwrap();

        assert_eq!(0, graphrecord.edge_count());

        let edges = create_edges_dataframe().unwrap();

        graphrecord
            .add_edges_dataframes(vec![(edges, "from", "to")])
            .unwrap();

        assert_eq!(2, graphrecord.edge_count());
    }

    #[test]
    fn test_add_group() {
        let mut graphrecord = create_graphrecord();

        assert_eq!(0, graphrecord.group_count());

        graphrecord.add_group("0".into(), None, None).unwrap();

        assert_eq!(1, graphrecord.group_count());

        graphrecord
            .add_group("1".into(), Some(vec!["0".into(), "1".into()]), None)
            .unwrap();

        assert_eq!(2, graphrecord.group_count());

        assert_eq!(2, graphrecord.nodes_in_group(&"1".into()).unwrap().count());
    }

    #[test]
    fn test_invalid_add_group() {
        let mut graphrecord = create_graphrecord();

        // Adding a group with a non-existing node should fail
        assert!(
            graphrecord
                .add_group("0".into(), Some(vec!["50".into()]), None)
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );

        // Adding a group with a non-existing edge should fail
        assert!(
            graphrecord
                .add_group("0".into(), None, Some(vec![50]))
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );

        graphrecord.add_group("0".into(), None, None).unwrap();

        // Adding an already existing group should fail
        assert!(
            graphrecord
                .add_group("0".into(), None, None)
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );

        graphrecord.freeze_schema();

        assert!(
            graphrecord
                .add_group("2".into(), None, None)
                .is_err_and(|e| matches!(e, GraphRecordError::SchemaError(_)))
        );

        graphrecord.remove_group(&"0".into()).unwrap();

        assert!(
            graphrecord
                .add_group("0".into(), Some(vec!["0".into()]), None)
                .is_err_and(|e| matches!(e, GraphRecordError::SchemaError(_)))
        );
        assert!(
            graphrecord
                .add_group("0".into(), None, Some(vec![0]))
                .is_err_and(|e| matches!(e, GraphRecordError::SchemaError(_)))
        );
    }

    #[test]
    fn test_remove_group() {
        let mut graphrecord = create_graphrecord();

        graphrecord.add_group("0".into(), None, None).unwrap();

        assert_eq!(1, graphrecord.group_count());

        graphrecord.remove_group(&"0".into()).unwrap();

        assert_eq!(0, graphrecord.group_count());
    }

    #[test]
    fn test_invalid_remove_group() {
        let mut graphrecord = GraphRecord::new();

        // Removing a non-existing group should fail
        assert!(
            graphrecord
                .remove_group(&"0".into())
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );
    }

    #[test]
    fn test_add_node_to_group() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group("0".into(), Some(vec!["0".into(), "1".into()]), None)
            .unwrap();

        assert_eq!(2, graphrecord.nodes_in_group(&"0".into()).unwrap().count());

        graphrecord
            .add_node_to_group("0".into(), "2".into())
            .unwrap();

        assert_eq!(3, graphrecord.nodes_in_group(&"0".into()).unwrap().count());

        graphrecord
            .add_node("4".into(), HashMap::from([("test".into(), "test".into())]))
            .unwrap();

        graphrecord
            .add_group("1".into(), Some(vec!["4".into()]), None)
            .unwrap();

        graphrecord.freeze_schema();

        graphrecord
            .add_node("5".into(), HashMap::from([("test".into(), "test".into())]))
            .unwrap();

        assert!(
            graphrecord
                .add_node_to_group("1".into(), "5".into())
                .is_ok()
        );

        assert_eq!(2, graphrecord.nodes_in_group(&"1".into()).unwrap().count());
    }

    #[test]
    fn test_invalid_add_node_to_group() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group("0".into(), Some(vec!["0".into()]), None)
            .unwrap();

        // Adding a non-existing node to a group should fail
        assert!(
            graphrecord
                .add_node_to_group("0".into(), "50".into())
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );

        // Adding a node to a group that already is in the group should fail
        assert!(
            graphrecord
                .add_node_to_group("0".into(), "0".into())
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );

        let mut graphrecord = GraphRecord::new();

        graphrecord
            .add_node("0".into(), HashMap::from([("test".into(), "test".into())]))
            .unwrap();
        graphrecord.add_group("group".into(), None, None).unwrap();

        graphrecord.freeze_schema();

        assert!(
            graphrecord
                .add_node_to_group("group".into(), "0".into())
                .is_err_and(|e| matches!(e, GraphRecordError::SchemaError(_)))
        );
    }

    #[test]
    fn test_add_edge_to_group() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group("0".into(), None, Some(vec![0, 1]))
            .unwrap();

        assert_eq!(2, graphrecord.edges_in_group(&"0".into()).unwrap().count());

        graphrecord.add_edge_to_group("0".into(), 2).unwrap();

        assert_eq!(3, graphrecord.edges_in_group(&"0".into()).unwrap().count());

        graphrecord
            .add_edge("0".into(), "1".into(), HashMap::new())
            .unwrap();

        graphrecord
            .add_group("1".into(), None, Some(vec![3]))
            .unwrap();

        graphrecord.freeze_schema();

        let edge_index = graphrecord
            .add_edge("0".into(), "1".into(), HashMap::new())
            .unwrap();

        assert!(
            graphrecord
                .add_edge_to_group("1".into(), edge_index)
                .is_ok()
        );

        assert_eq!(2, graphrecord.edges_in_group(&"1".into()).unwrap().count());
    }

    #[test]
    fn test_invalid_add_edge_to_group() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group("0".into(), None, Some(vec![0]))
            .unwrap();

        // Adding a non-existing edge to a group should fail
        assert!(
            graphrecord
                .add_edge_to_group("0".into(), 50)
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );

        // Adding an edge to a group that already is in the group should fail
        assert!(
            graphrecord
                .add_edge_to_group("0".into(), 0)
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );

        let mut graphrecord = GraphRecord::new();

        graphrecord.add_node("0".into(), HashMap::new()).unwrap();
        graphrecord
            .add_edge(
                "0".into(),
                "0".into(),
                HashMap::from([("test".into(), "test".into())]),
            )
            .unwrap();
        graphrecord.add_group("group".into(), None, None).unwrap();

        graphrecord.freeze_schema();

        assert!(
            graphrecord
                .add_edge_to_group("group".into(), 0)
                .is_err_and(|e| matches!(e, GraphRecordError::SchemaError(_)))
        );
    }

    #[test]
    fn test_remove_node_from_group() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group("0".into(), Some(vec!["0".into(), "1".into()]), None)
            .unwrap();

        assert_eq!(2, graphrecord.nodes_in_group(&"0".into()).unwrap().count());

        graphrecord
            .remove_node_from_group(&"0".into(), &"0".into())
            .unwrap();

        assert_eq!(1, graphrecord.nodes_in_group(&"0".into()).unwrap().count());
    }

    #[test]
    fn test_invalid_remove_node_from_group() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group("0".into(), Some(vec!["0".into()]), None)
            .unwrap();

        // Removing a node from a non-existing group should fail
        assert!(
            graphrecord
                .remove_node_from_group(&"50".into(), &"0".into())
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );

        // Removing a non-existing node from a group should fail
        assert!(
            graphrecord
                .remove_node_from_group(&"0".into(), &"50".into())
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );

        // Removing a node from a group it is not in should fail
        assert!(
            graphrecord
                .remove_node_from_group(&"0".into(), &"1".into())
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
    }

    #[test]
    fn test_remove_edge_from_group() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group("0".into(), None, Some(vec![0, 1]))
            .unwrap();

        assert_eq!(2, graphrecord.edges_in_group(&"0".into()).unwrap().count());

        graphrecord.remove_edge_from_group(&"0".into(), &0).unwrap();

        assert_eq!(1, graphrecord.edges_in_group(&"0".into()).unwrap().count());
    }

    #[test]
    fn test_invalid_remove_edge_from_group() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group("0".into(), None, Some(vec![0]))
            .unwrap();

        // Removing an edge from a non-existing group should fail
        assert!(
            graphrecord
                .remove_edge_from_group(&"50".into(), &0)
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );

        // Removing a non-existing edge from a group should fail
        assert!(
            graphrecord
                .remove_edge_from_group(&"0".into(), &50)
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );

        // Removing an edge from a group it is not in should fail
        assert!(
            graphrecord
                .remove_edge_from_group(&"0".into(), &1)
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
    }

    #[test]
    fn test_groups() {
        let mut graphrecord = create_graphrecord();

        graphrecord.add_group("0".into(), None, None).unwrap();

        let groups: Vec<_> = graphrecord.groups().collect();

        assert_eq!(vec![&(GraphRecordAttribute::from("0"))], groups);
    }

    #[test]
    fn test_nodes_in_group() {
        let mut graphrecord = create_graphrecord();

        graphrecord.add_group("0".into(), None, None).unwrap();

        assert_eq!(0, graphrecord.nodes_in_group(&"0".into()).unwrap().count());

        graphrecord
            .add_group("1".into(), Some(vec!["0".into()]), None)
            .unwrap();

        assert_eq!(1, graphrecord.nodes_in_group(&"1".into()).unwrap().count());
    }

    #[test]
    fn test_invalid_nodes_in_group() {
        let graphrecord = create_graphrecord();

        // Querying a non-existing group should fail
        assert!(
            graphrecord
                .nodes_in_group(&"0".into())
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );
    }

    #[test]
    fn test_edges_in_group() {
        let mut graphrecord = create_graphrecord();

        graphrecord.add_group("0".into(), None, None).unwrap();

        assert_eq!(0, graphrecord.edges_in_group(&"0".into()).unwrap().count());

        graphrecord
            .add_group("1".into(), None, Some(vec![0]))
            .unwrap();

        assert_eq!(1, graphrecord.edges_in_group(&"1".into()).unwrap().count());
    }

    #[test]
    fn test_invalid_edges_in_group() {
        let graphrecord = create_graphrecord();

        // Querying a non-existing group should fail
        assert!(
            graphrecord
                .edges_in_group(&"0".into())
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );
    }

    #[test]
    fn test_groups_of_node() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group("0".into(), Some(vec!["0".into()]), None)
            .unwrap();

        assert_eq!(1, graphrecord.groups_of_node(&"0".into()).unwrap().count());
    }

    #[test]
    fn test_invalid_groups_of_node() {
        let graphrecord = create_graphrecord();

        // Queyring the groups of a non-existing node should fail
        assert!(
            graphrecord
                .groups_of_node(&"50".into())
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );
    }

    #[test]
    fn test_groups_of_edge() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group("0".into(), None, Some(vec![0]))
            .unwrap();

        assert_eq!(1, graphrecord.groups_of_edge(&0).unwrap().count());
    }

    #[test]
    fn test_invalid_groups_of_edge() {
        let graphrecord = create_graphrecord();

        // Queyring the groups of a non-existing edge should fail
        assert!(
            graphrecord
                .groups_of_edge(&50)
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );
    }

    #[test]
    fn test_node_count() {
        let mut graphrecord = GraphRecord::new();

        assert_eq!(0, graphrecord.node_count());

        graphrecord.add_node("0".into(), HashMap::new()).unwrap();

        assert_eq!(1, graphrecord.node_count());
    }

    #[test]
    fn test_edge_count() {
        let mut graphrecord = GraphRecord::new();

        graphrecord.add_node("0".into(), HashMap::new()).unwrap();
        graphrecord.add_node("1".into(), HashMap::new()).unwrap();

        assert_eq!(0, graphrecord.edge_count());

        graphrecord
            .add_edge("0".into(), "1".into(), HashMap::new())
            .unwrap();

        assert_eq!(1, graphrecord.edge_count());
    }

    #[test]
    fn test_group_count() {
        let mut graphrecord = create_graphrecord();

        assert_eq!(0, graphrecord.group_count());

        graphrecord.add_group("0".into(), None, None).unwrap();

        assert_eq!(1, graphrecord.group_count());
    }

    #[test]
    fn test_contains_node() {
        let graphrecord = create_graphrecord();

        assert!(graphrecord.contains_node(&"0".into()));

        assert!(!graphrecord.contains_node(&"50".into()));
    }

    #[test]
    fn test_contains_edge() {
        let graphrecord = create_graphrecord();

        assert!(graphrecord.contains_edge(&0));

        assert!(!graphrecord.contains_edge(&50));
    }

    #[test]
    fn test_contains_group() {
        let mut graphrecord = create_graphrecord();

        assert!(!graphrecord.contains_group(&"0".into()));

        graphrecord.add_group("0".into(), None, None).unwrap();

        assert!(graphrecord.contains_group(&"0".into()));
    }

    #[test]
    fn test_neighbors() {
        let graphrecord = create_graphrecord();

        let neighbors = graphrecord.neighbors_outgoing(&"0".into()).unwrap();

        assert_eq!(2, neighbors.count());
    }

    #[test]
    fn test_invalid_neighbors() {
        let graphrecord = GraphRecord::new();

        // Querying neighbors of a non-existing node sohuld fail
        assert!(
            graphrecord
                .neighbors_outgoing(&"0".into())
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );
    }

    #[test]
    fn test_neighbors_undirected() {
        let graphrecord = create_graphrecord();

        let neighbors = graphrecord.neighbors_outgoing(&"2".into()).unwrap();
        assert_eq!(0, neighbors.count());

        let neighbors = graphrecord.neighbors_undirected(&"2".into()).unwrap();
        assert_eq!(2, neighbors.count());
    }

    #[test]
    fn test_invalid_neighbors_undirected() {
        let graphrecord = create_graphrecord();

        assert!(
            graphrecord
                .neighbors_undirected(&"50".into())
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );
    }

    #[test]
    fn test_clear() {
        let mut graphrecord = create_graphrecord();

        graphrecord.clear();

        assert_eq!(0, graphrecord.node_count());
        assert_eq!(0, graphrecord.edge_count());
        assert_eq!(0, graphrecord.group_count());
    }
}
