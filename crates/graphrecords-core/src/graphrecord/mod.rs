pub mod attributes;
#[cfg(feature = "connectors")]
pub mod connector;
pub mod datatypes;
mod graph;
mod group_mapping;
mod intern_table;
mod lookup;
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
    intern_table::{
        AttributeHandle, AttributeNameKind, AttributesView, GroupHandle, GroupKind, Handle,
        HandleKind, NodeHandle, NodeIndexKind,
    },
    lookup::{AsAttributeName, AsLookup, HandleLookup},
};
use crate::errors::GraphRecordResult;
#[cfg(feature = "plugins")]
use crate::graphrecord::plugins::{Plugin, PluginName};
use crate::{
    errors::GraphRecordError,
    graphrecord::{
        attributes::{EdgeAttributesMut, NodeAttributesMut},
        overview::{DEFAULT_TRUNCATE_DETAILS, GroupOverview, Overview},
        polars::DataFramesExport,
    },
};
use ::polars::frame::DataFrame;
use graph::Graph;
#[cfg(feature = "plugins")]
use graphrecords_utils::aliases::GrHashMap;
use graphrecords_utils::aliases::GrHashSet;
use group_mapping::GroupMapping;
use lookup::resolve_all;
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

#[derive(Debug, Clone)]
pub struct NodeDataFrameInput {
    pub dataframe: DataFrame,
    pub index_column: String,
}

#[derive(Debug, Clone)]
pub struct EdgeDataFrameInput {
    pub dataframe: DataFrame,
    pub source_index_column: String,
    pub target_index_column: String,
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
) -> GraphRecordResult<Vec<(NodeIndex, Attributes)>> {
    let nodes = nodes_dataframes
        .into_iter()
        .map(|dataframe_input| {
            let dataframe_input = dataframe_input.into();

            dataframe_to_nodes(dataframe_input.dataframe, &dataframe_input.index_column)
        })
        .collect::<GraphRecordResult<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect();

    Ok(nodes)
}

fn edge_dataframes_to_tuples(
    edges_dataframes: impl IntoIterator<Item = impl Into<EdgeDataFrameInput>>,
) -> GraphRecordResult<Vec<(NodeIndex, NodeIndex, Attributes)>> {
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
        .collect::<GraphRecordResult<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect();

    Ok(edges)
}

#[allow(clippy::type_complexity)]
fn dataframes_to_tuples(
    nodes_dataframes: impl IntoIterator<Item = impl Into<NodeDataFrameInput>>,
    edges_dataframes: impl IntoIterator<Item = impl Into<EdgeDataFrameInput>>,
) -> GraphRecordResult<(
    Vec<(NodeIndex, Attributes)>,
    Vec<(NodeIndex, NodeIndex, Attributes)>,
)> {
    let nodes = node_dataframes_to_tuples(nodes_dataframes)?;
    let edges = edge_dataframes_to_tuples(edges_dataframes)?;

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
    plugins: Arc<GrHashMap<PluginName, Box<dyn Plugin>>>,
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

    pub fn from_tuples(
        nodes: Vec<(NodeIndex, Attributes)>,
        edges: Option<Vec<(NodeIndex, NodeIndex, Attributes)>>,
        schema: Option<Schema>,
    ) -> GraphRecordResult<Self> {
        let mut graphrecord = Self::with_capacity(
            nodes.len(),
            edges.as_ref().map_or(0, std::vec::Vec::len),
            schema,
        );

        for (node_index, attributes) in nodes {
            graphrecord.add_node_impl(node_index, attributes)?;
        }

        if let Some(edges) = edges {
            for (source_node_index, target_node_index, attributes) in edges {
                let source_handle = source_node_index.resolve(&graphrecord)?;
                let target_handle = target_node_index.resolve(&graphrecord)?;

                graphrecord.add_edge_impl(source_handle, target_handle, attributes)?;
            }
        }

        Ok(graphrecord)
    }

    pub fn from_dataframes(
        nodes_dataframes: impl IntoIterator<Item = impl Into<NodeDataFrameInput>>,
        edges_dataframes: impl IntoIterator<Item = impl Into<EdgeDataFrameInput>>,
        schema: Option<Schema>,
    ) -> GraphRecordResult<Self> {
        let (nodes, edges) = dataframes_to_tuples(nodes_dataframes, edges_dataframes)?;

        Self::from_tuples(nodes, Some(edges), schema)
    }

    pub fn from_nodes_dataframes(
        nodes_dataframes: impl IntoIterator<Item = impl Into<NodeDataFrameInput>>,
        schema: Option<Schema>,
    ) -> GraphRecordResult<Self> {
        let nodes = node_dataframes_to_tuples(nodes_dataframes)?;

        Self::from_tuples(nodes, None, schema)
    }

    #[cfg(feature = "serde")]
    pub fn from_ron<P>(path: P) -> GraphRecordResult<Self>
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
    pub fn to_ron<P>(&self, path: P) -> GraphRecordResult<()>
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

    pub fn to_dataframes(&self) -> GraphRecordResult<DataFramesExport> {
        DataFramesExport::new(self)
    }

    #[allow(clippy::too_many_lines)]
    fn set_schema_impl(&mut self, mut schema: Schema) -> GraphRecordResult<()> {
        let mut nodes_group_cache = HashMap::<GroupHandle, usize>::new();
        let mut nodes_ungrouped_visited = false;
        let mut edges_group_cache = HashMap::<GroupHandle, usize>::new();
        let mut edges_ungrouped_visited = false;

        for (node_handle, node) in &self.graph.nodes {
            let node_index = self.graph.node_index_table.resolve(*node_handle);
            let node_attributes =
                AttributesView::new(&self.graph.attribute_name_table, &node.attributes);

            let groups_of_node: Vec<GroupHandle> =
                self.group_mapping.groups_of_node(*node_handle).collect();

            if groups_of_node.is_empty() {
                match schema.schema_type() {
                    SchemaType::Inferred => {
                        let nodes_in_groups = self.group_mapping.nodes_in_group.len();

                        let nodes_not_in_groups = self.graph.node_count() - nodes_in_groups;

                        schema.update_node(
                            &node_attributes,
                            None,
                            nodes_not_in_groups == 0 || !nodes_ungrouped_visited,
                        );

                        nodes_ungrouped_visited = true;
                    }
                    SchemaType::Provided => {
                        schema.validate_node(node_index, &node_attributes, None)?;
                    }
                }
            } else {
                for group_handle in groups_of_node {
                    let group = self.graph.group_name_table.resolve(group_handle);
                    match schema.schema_type() {
                        SchemaType::Inferred => match nodes_group_cache.entry(group_handle) {
                            Entry::Occupied(entry) => {
                                schema.update_node(
                                    &node_attributes,
                                    Some(group),
                                    *entry.get() == 0,
                                );
                            }
                            Entry::Vacant(entry) => {
                                entry.insert(
                                    self.group_mapping
                                        .nodes_in_group
                                        .get(&group_handle)
                                        .map_or(0, GrHashSet::len),
                                );

                                schema.update_node(&node_attributes, Some(group), true);
                            }
                        },
                        SchemaType::Provided => {
                            schema.validate_node(node_index, &node_attributes, Some(group))?;
                        }
                    }
                }
            }
        }

        for (edge_index, edge) in &self.graph.edges {
            let edge_attributes =
                AttributesView::new(&self.graph.attribute_name_table, &edge.attributes);

            let groups_of_edge: Vec<GroupHandle> =
                self.group_mapping.groups_of_edge(edge_index).collect();

            if groups_of_edge.is_empty() {
                match schema.schema_type() {
                    SchemaType::Inferred => {
                        let edges_in_groups = self.group_mapping.edges_in_group.len();

                        let edges_not_in_groups = self.graph.edge_count() - edges_in_groups;

                        schema.update_edge(
                            &edge_attributes,
                            None,
                            edges_not_in_groups == 0 || !edges_ungrouped_visited,
                        );

                        edges_ungrouped_visited = true;
                    }
                    SchemaType::Provided => {
                        schema.validate_edge(edge_index, &edge_attributes, None)?;
                    }
                }
            } else {
                for group_handle in groups_of_edge {
                    let group = self.graph.group_name_table.resolve(group_handle);
                    match schema.schema_type() {
                        SchemaType::Inferred => match edges_group_cache.entry(group_handle) {
                            Entry::Occupied(entry) => {
                                schema.update_edge(
                                    &edge_attributes,
                                    Some(group),
                                    *entry.get() == 0,
                                );
                            }
                            Entry::Vacant(entry) => {
                                entry.insert(
                                    self.group_mapping
                                        .edges_in_group
                                        .get(&group_handle)
                                        .map_or(0, GrHashSet::len),
                                );

                                schema.update_edge(&edge_attributes, Some(group), true);
                            }
                        },
                        SchemaType::Provided => {
                            schema.validate_edge(edge_index, &edge_attributes, Some(group))?;
                        }
                    }
                }
            }
        }

        mem::swap(&mut self.schema, &mut schema);

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

    const fn freeze_schema_impl(&mut self) {
        self.schema.freeze();
    }

    const fn unfreeze_schema_impl(&mut self) {
        self.schema.unfreeze();
    }

    pub fn node_indices(&self) -> impl Iterator<Item = &NodeIndex> {
        self.graph.node_indices()
    }

    pub fn node_handles(&self) -> impl Iterator<Item = NodeHandle> + use<'_> {
        self.graph.node_handles()
    }

    pub fn node_attributes(
        &self,
        node: impl AsLookup<NodeIndexKind>,
    ) -> GraphRecordResult<AttributesView<'_>> {
        let handle = node.resolve(self)?;

        self.graph
            .node_attributes(handle)
            .map_err(GraphRecordError::from)
    }

    pub fn node_attributes_mut(
        &mut self,
        node: impl AsLookup<NodeIndexKind>,
    ) -> GraphRecordResult<NodeAttributesMut<'_>> {
        NodeAttributesMut::new(node, self)
    }

    pub fn outgoing_edges<L: AsLookup<NodeIndexKind>>(
        &self,
        node: L,
    ) -> GraphRecordResult<impl Iterator<Item = &EdgeIndex> + use<'_, L>> {
        let handle = node.resolve(self)?;

        self.graph
            .outgoing_edges(handle)
            .map_err(GraphRecordError::from)
    }

    pub fn incoming_edges<L: AsLookup<NodeIndexKind>>(
        &self,
        node: L,
    ) -> GraphRecordResult<impl Iterator<Item = &EdgeIndex> + use<'_, L>> {
        let handle = node.resolve(self)?;

        self.graph
            .incoming_edges(handle)
            .map_err(GraphRecordError::from)
    }

    pub fn edge_indices(&self) -> impl Iterator<Item = &EdgeIndex> {
        self.graph.edge_indices()
    }

    pub fn edge_attributes(&self, edge_index: &EdgeIndex) -> GraphRecordResult<AttributesView<'_>> {
        self.graph
            .edge_attributes(edge_index)
            .map_err(GraphRecordError::from)
    }

    pub fn edge_attributes_mut<'a>(
        &'a mut self,
        edge_index: &'a EdgeIndex,
    ) -> GraphRecordResult<EdgeAttributesMut<'a>> {
        EdgeAttributesMut::new(edge_index, self)
    }

    pub fn edge_endpoint_handles(
        &self,
        edge_index: &EdgeIndex,
    ) -> GraphRecordResult<(NodeHandle, NodeHandle)> {
        self.graph
            .edge_endpoint_handles(edge_index)
            .map_err(GraphRecordError::from)
    }

    pub fn edge_endpoints(
        &self,
        edge_index: &EdgeIndex,
    ) -> GraphRecordResult<(&NodeIndex, &NodeIndex)> {
        self.graph
            .edge_endpoints(edge_index)
            .map_err(GraphRecordError::from)
    }

    pub fn edges_connecting(
        &self,
        outgoing_nodes: impl IntoIterator<Item = impl AsLookup<NodeIndexKind>>,
        incoming_nodes: impl IntoIterator<Item = impl AsLookup<NodeIndexKind>>,
    ) -> GraphRecordResult<impl Iterator<Item = &EdgeIndex>> {
        let outgoing_handles = resolve_all(self, outgoing_nodes)?;
        let incoming_handles = resolve_all(self, incoming_nodes)?;

        Ok(self
            .graph
            .edges_connecting(outgoing_handles, incoming_handles))
    }

    pub fn edges_connecting_undirected(
        &self,
        first_nodes: impl IntoIterator<Item = impl AsLookup<NodeIndexKind>>,
        second_nodes: impl IntoIterator<Item = impl AsLookup<NodeIndexKind>>,
    ) -> GraphRecordResult<impl Iterator<Item = &EdgeIndex>> {
        let first_handles = resolve_all(self, first_nodes)?;
        let second_handles = resolve_all(self, second_nodes)?;

        Ok(self
            .graph
            .edges_connecting_undirected(first_handles, second_handles))
    }

    fn add_node_impl(
        &mut self,
        node_index: NodeIndex,
        attributes: Attributes,
    ) -> GraphRecordResult<NodeHandle> {
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
    fn add_node_with_group_impl(
        &mut self,
        node_index: NodeIndex,
        attributes: Attributes,
        group_handle: GroupHandle,
    ) -> GraphRecordResult<NodeHandle> {
        let group = self.graph.group_name_table.resolve(group_handle).clone();

        match self.schema.schema_type() {
            SchemaType::Inferred => {
                let nodes_in_group = self
                    .group_mapping
                    .nodes_in_group
                    .get(&group_handle)
                    .map_or(0, GrHashSet::len);

                self.schema
                    .update_node(&attributes, Some(&group), nodes_in_group == 0);
            }
            SchemaType::Provided => {
                self.schema
                    .validate_node(&node_index, &attributes, Some(&group))?;
            }
        }

        let node_handle = self
            .graph
            .add_node(node_index, attributes)
            .map_err(GraphRecordError::from)?;

        self.group_mapping
            .add_node_to_group(group_handle, node_handle)
            .inspect_err(|_| {
                self.graph
                    .remove_node(node_handle, &mut self.group_mapping)
                    .expect("Node must exist");
            })?;

        Ok(node_handle)
    }

    fn add_node_with_groups_impl(
        &mut self,
        node_index: NodeIndex,
        attributes: Attributes,
        group_handles: &[GroupHandle],
    ) -> GraphRecordResult<NodeHandle> {
        match group_handles.split_first() {
            None => self.add_node_impl(node_index, attributes),
            Some((first, rest)) => {
                let node_handle = self.add_node_with_group_impl(node_index, attributes, *first)?;

                for group_handle in rest {
                    self.add_node_to_group_impl(*group_handle, node_handle)
                        .inspect_err(|_| {
                            self.graph
                                .remove_node(node_handle, &mut self.group_mapping)
                                .expect("Node must exist");
                        })?;
                }

                Ok(node_handle)
            }
        }
    }

    fn remove_node_impl(&mut self, node_handle: NodeHandle) -> GraphRecordResult<Attributes> {
        self.graph
            .remove_node(node_handle, &mut self.group_mapping)
            .map_err(GraphRecordError::from)
    }

    fn add_nodes_impl(
        &mut self,
        nodes: Vec<(NodeIndex, Attributes)>,
    ) -> GraphRecordResult<Vec<NodeHandle>> {
        nodes
            .into_iter()
            .map(|(node_index, attributes)| self.add_node_impl(node_index, attributes))
            .collect()
    }

    // TODO: Add tests
    fn add_nodes_with_group_impl(
        &mut self,
        nodes: Vec<(NodeIndex, Attributes)>,
        group_handle: GroupHandle,
    ) -> GraphRecordResult<Vec<NodeHandle>> {
        nodes
            .into_iter()
            .map(|(node_index, attributes)| {
                self.add_node_with_group_impl(node_index, attributes, group_handle)
            })
            .collect()
    }

    fn add_nodes_with_groups_impl(
        &mut self,
        nodes: Vec<(NodeIndex, Attributes)>,
        group_handles: &[GroupHandle],
    ) -> GraphRecordResult<Vec<NodeHandle>> {
        nodes
            .into_iter()
            .map(|(node_index, attributes)| {
                self.add_node_with_groups_impl(node_index, attributes, group_handles)
            })
            .collect()
    }

    fn add_nodes_dataframes_impl(
        &mut self,
        nodes_dataframes: impl IntoIterator<Item = impl Into<NodeDataFrameInput>>,
    ) -> GraphRecordResult<Vec<NodeHandle>> {
        self.add_nodes_impl(node_dataframes_to_tuples(nodes_dataframes)?)
    }

    // TODO: Add tests
    fn add_nodes_dataframes_with_group_impl(
        &mut self,
        nodes_dataframes: impl IntoIterator<Item = impl Into<NodeDataFrameInput>>,
        group_handle: GroupHandle,
    ) -> GraphRecordResult<Vec<NodeHandle>> {
        self.add_nodes_with_group_impl(node_dataframes_to_tuples(nodes_dataframes)?, group_handle)
    }

    fn add_nodes_dataframes_with_groups_impl(
        &mut self,
        nodes_dataframes: impl IntoIterator<Item = impl Into<NodeDataFrameInput>>,
        group_handles: &[GroupHandle],
    ) -> GraphRecordResult<Vec<NodeHandle>> {
        self.add_nodes_with_groups_impl(node_dataframes_to_tuples(nodes_dataframes)?, group_handles)
    }

    #[allow(clippy::needless_pass_by_value)]
    fn add_edge_impl(
        &mut self,
        source_handle: NodeHandle,
        target_handle: NodeHandle,
        attributes: Attributes,
    ) -> GraphRecordResult<EdgeIndex> {
        let edge_index = self
            .graph
            .add_edge(source_handle, target_handle, attributes.clone())
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
    fn add_edge_with_group_impl(
        &mut self,
        source_handle: NodeHandle,
        target_handle: NodeHandle,
        attributes: Attributes,
        group_handle: GroupHandle,
    ) -> GraphRecordResult<EdgeIndex> {
        let edge_index = self
            .graph
            .add_edge(source_handle, target_handle, attributes.clone())
            .map_err(GraphRecordError::from)?;

        let group = self.graph.group_name_table.resolve(group_handle).clone();

        match self.schema.schema_type() {
            SchemaType::Inferred => {
                let edges_in_group = self
                    .group_mapping
                    .edges_in_group
                    .get(&group_handle)
                    .map_or(0, GrHashSet::len);

                self.schema
                    .update_edge(&attributes, Some(&group), edges_in_group == 0);
            }
            SchemaType::Provided => {
                self.schema
                    .validate_edge(&edge_index, &attributes, Some(&group))
                    .inspect_err(|_| {
                        self.graph
                            .remove_edge(&edge_index)
                            .expect("Edge must exist");
                    })?;
            }
        }

        self.group_mapping
            .add_edge_to_group(group_handle, edge_index)
            .inspect_err(|_| {
                self.graph
                    .remove_edge(&edge_index)
                    .expect("Edge must exist");
            })?;

        Ok(edge_index)
    }

    fn add_edge_with_groups_impl(
        &mut self,
        source_handle: NodeHandle,
        target_handle: NodeHandle,
        attributes: Attributes,
        group_handles: &[GroupHandle],
    ) -> GraphRecordResult<EdgeIndex> {
        match group_handles.split_first() {
            None => self.add_edge_impl(source_handle, target_handle, attributes),
            Some((first, rest)) => {
                let edge_index = self.add_edge_with_group_impl(
                    source_handle,
                    target_handle,
                    attributes,
                    *first,
                )?;

                for group_handle in rest {
                    self.add_edge_to_group_impl(*group_handle, edge_index)
                        .inspect_err(|_| {
                            self.graph
                                .remove_edge(&edge_index)
                                .expect("Edge must exist");
                        })?;
                }

                Ok(edge_index)
            }
        }
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    fn remove_edge_impl(&mut self, edge_index: &EdgeIndex) -> GraphRecordResult<Attributes> {
        self.group_mapping.remove_edge(edge_index);

        self.graph
            .remove_edge(edge_index)
            .map_err(GraphRecordError::from)
    }

    fn add_edges_impl<S, T>(
        &mut self,
        edges: Vec<(S, T, Attributes)>,
    ) -> GraphRecordResult<Vec<EdgeIndex>>
    where
        S: AsLookup<NodeIndexKind>,
        T: AsLookup<NodeIndexKind>,
    {
        edges
            .into_iter()
            .map(|(source, target, attributes)| {
                let source_handle = source.resolve(self)?;
                let target_handle = target.resolve(self)?;
                self.add_edge_impl(source_handle, target_handle, attributes)
            })
            .collect()
    }

    // TODO: Add tests
    fn add_edges_with_group_impl<S, T>(
        &mut self,
        edges: Vec<(S, T, Attributes)>,
        group_handle: GroupHandle,
    ) -> GraphRecordResult<Vec<EdgeIndex>>
    where
        S: AsLookup<NodeIndexKind>,
        T: AsLookup<NodeIndexKind>,
    {
        edges
            .into_iter()
            .map(|(source, target, attributes)| {
                let source_handle = source.resolve(self)?;
                let target_handle = target.resolve(self)?;
                self.add_edge_with_group_impl(
                    source_handle,
                    target_handle,
                    attributes,
                    group_handle,
                )
            })
            .collect()
    }

    fn add_edges_with_groups_impl<S, T>(
        &mut self,
        edges: Vec<(S, T, Attributes)>,
        group_handles: &[GroupHandle],
    ) -> GraphRecordResult<Vec<EdgeIndex>>
    where
        S: AsLookup<NodeIndexKind>,
        T: AsLookup<NodeIndexKind>,
    {
        edges
            .into_iter()
            .map(|(source, target, attributes)| {
                let source_handle = source.resolve(self)?;
                let target_handle = target.resolve(self)?;
                self.add_edge_with_groups_impl(
                    source_handle,
                    target_handle,
                    attributes,
                    group_handles,
                )
            })
            .collect()
    }

    fn add_edges_dataframes_impl(
        &mut self,
        edges_dataframes: impl IntoIterator<Item = impl Into<EdgeDataFrameInput>>,
    ) -> GraphRecordResult<Vec<EdgeIndex>> {
        self.add_edges_impl(edge_dataframes_to_tuples(edges_dataframes)?)
    }

    // TODO: Add tests
    fn add_edges_dataframes_with_group_impl(
        &mut self,
        edges_dataframes: impl IntoIterator<Item = impl Into<EdgeDataFrameInput>>,
        group_handle: GroupHandle,
    ) -> GraphRecordResult<Vec<EdgeIndex>> {
        self.add_edges_with_group_impl(edge_dataframes_to_tuples(edges_dataframes)?, group_handle)
    }

    fn add_edges_dataframes_with_groups_impl(
        &mut self,
        edges_dataframes: impl IntoIterator<Item = impl Into<EdgeDataFrameInput>>,
        group_handles: &[GroupHandle],
    ) -> GraphRecordResult<Vec<EdgeIndex>> {
        self.add_edges_with_groups_impl(edge_dataframes_to_tuples(edges_dataframes)?, group_handles)
    }

    fn add_group_impl(
        &mut self,
        group: Group,
        node_handles: Option<Vec<NodeHandle>>,
        edge_indices: Option<Vec<EdgeIndex>>,
    ) -> GraphRecordResult<()> {
        if self.contains_group(&group) {
            return Err(GraphRecordError::AssertionError(format!(
                "Group {group} already exists"
            )));
        }

        if let Some(ref node_handles) = node_handles {
            for node_handle in node_handles {
                if !self.graph.contains_node(*node_handle) {
                    return Err(GraphRecordError::IndexError(format!(
                        "Cannot find node for handle {node_handle:?}",
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

                if let Some(ref node_handles) = node_handles {
                    let mut empty = true;

                    for node_handle in node_handles {
                        let node_attributes = self.graph.node_attributes(*node_handle)?;

                        self.schema
                            .update_node(&node_attributes, Some(&group), empty);

                        empty = false;
                    }
                }

                if let Some(ref edge_indices) = edge_indices {
                    let mut empty = true;

                    for edge_index in edge_indices {
                        let edge_attributes = self.graph.edge_attributes(edge_index)?;

                        self.schema
                            .update_edge(&edge_attributes, Some(&group), empty);

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

                if let Some(ref node_handles) = node_handles {
                    for node_handle in node_handles {
                        let node_index = self.graph.node_index_table.resolve(*node_handle).clone();
                        let node_attributes = self.graph.node_attributes(*node_handle)?;

                        self.schema
                            .validate_node(&node_index, &node_attributes, Some(&group))?;
                    }
                }

                if let Some(ref edge_indices) = edge_indices {
                    for edge_index in edge_indices {
                        let edge_attributes = self.graph.edge_attributes(edge_index)?;

                        self.schema
                            .validate_edge(edge_index, &edge_attributes, Some(&group))?;
                    }
                }
            }
        }

        let group_handle = self.graph.group_name_table.intern_owned(group);

        self.group_mapping
            .add_group(group_handle, node_handles, edge_indices)
            .expect("Group must not exist");

        Ok(())
    }

    fn remove_group_impl(&mut self, group_handle: GroupHandle) -> GraphRecordResult<()> {
        self.group_mapping.remove_group(group_handle)
    }

    fn add_node_to_group_impl(
        &mut self,
        group_handle: GroupHandle,
        node_handle: NodeHandle,
    ) -> GraphRecordResult<()> {
        let node_attributes = self.graph.node_attributes(node_handle)?;
        let group = self.graph.group_name_table.resolve(group_handle).clone();

        match self.schema.schema_type() {
            SchemaType::Inferred => {
                let nodes_in_group = self
                    .group_mapping
                    .nodes_in_group
                    .get(&group_handle)
                    .map_or(0, GrHashSet::len);

                self.schema
                    .update_node(&node_attributes, Some(&group), nodes_in_group == 0);
            }
            SchemaType::Provided => {
                let node_index = self.graph.node_index_table.resolve(node_handle).clone();

                self.schema
                    .validate_node(&node_index, &node_attributes, Some(&group))?;
            }
        }

        self.group_mapping
            .add_node_to_group(group_handle, node_handle)
    }

    fn add_node_to_groups_impl(
        &mut self,
        group_handles: &[GroupHandle],
        node_handle: NodeHandle,
    ) -> GraphRecordResult<()> {
        group_handles
            .iter()
            .try_for_each(|group_handle| self.add_node_to_group_impl(*group_handle, node_handle))
    }

    fn add_nodes_to_groups_impl(
        &mut self,
        group_handles: &[GroupHandle],
        node_handles: Vec<NodeHandle>,
    ) -> GraphRecordResult<()> {
        node_handles
            .into_iter()
            .try_for_each(|node_handle| self.add_node_to_groups_impl(group_handles, node_handle))
    }

    fn add_edge_to_group_impl(
        &mut self,
        group_handle: GroupHandle,
        edge_index: EdgeIndex,
    ) -> GraphRecordResult<()> {
        let edge_attributes = self.graph.edge_attributes(&edge_index)?;
        let group = self.graph.group_name_table.resolve(group_handle).clone();

        match self.schema.schema_type() {
            SchemaType::Inferred => {
                let edges_in_group = self
                    .group_mapping
                    .edges_in_group
                    .get(&group_handle)
                    .map_or(0, GrHashSet::len);

                self.schema
                    .update_edge(&edge_attributes, Some(&group), edges_in_group == 0);
            }
            SchemaType::Provided => {
                self.schema
                    .validate_edge(&edge_index, &edge_attributes, Some(&group))?;
            }
        }

        self.group_mapping
            .add_edge_to_group(group_handle, edge_index)
    }

    fn add_edge_to_groups_impl(
        &mut self,
        group_handles: &[GroupHandle],
        edge_index: EdgeIndex,
    ) -> GraphRecordResult<()> {
        group_handles
            .iter()
            .try_for_each(|group_handle| self.add_edge_to_group_impl(*group_handle, edge_index))
    }

    fn add_edges_to_groups_impl(
        &mut self,
        group_handles: &[GroupHandle],
        edge_indices: Vec<EdgeIndex>,
    ) -> GraphRecordResult<()> {
        edge_indices
            .into_iter()
            .try_for_each(|edge_index| self.add_edge_to_groups_impl(group_handles, edge_index))
    }

    fn remove_node_from_group_impl(
        &mut self,
        group_handle: GroupHandle,
        node_handle: NodeHandle,
    ) -> GraphRecordResult<()> {
        self.group_mapping
            .remove_node_from_group(group_handle, node_handle)
    }

    fn remove_node_from_groups_impl(
        &mut self,
        group_handles: &[GroupHandle],
        node_handle: NodeHandle,
    ) -> GraphRecordResult<()> {
        group_handles.iter().try_for_each(|group_handle| {
            self.remove_node_from_group_impl(*group_handle, node_handle)
        })
    }

    fn remove_nodes_from_groups_impl(
        &mut self,
        group_handles: &[GroupHandle],
        node_handles: &[NodeHandle],
    ) -> GraphRecordResult<()> {
        node_handles.iter().try_for_each(|node_handle| {
            self.remove_node_from_groups_impl(group_handles, *node_handle)
        })
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    fn remove_edge_from_group_impl(
        &mut self,
        group_handle: GroupHandle,
        edge_index: &EdgeIndex,
    ) -> GraphRecordResult<()> {
        if !self.graph.contains_edge(edge_index) {
            return Err(GraphRecordError::IndexError(format!(
                "Cannot find edge with index {edge_index}",
            )));
        }

        self.group_mapping
            .remove_edge_from_group(group_handle, edge_index)
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    fn remove_edge_from_groups_impl(
        &mut self,
        group_handles: &[GroupHandle],
        edge_index: &EdgeIndex,
    ) -> GraphRecordResult<()> {
        group_handles.iter().try_for_each(|group_handle| {
            self.remove_edge_from_group_impl(*group_handle, edge_index)
        })
    }

    fn remove_edges_from_groups_impl(
        &mut self,
        group_handles: &[GroupHandle],
        edge_indices: &[EdgeIndex],
    ) -> GraphRecordResult<()> {
        edge_indices
            .iter()
            .try_for_each(|edge_index| self.remove_edge_from_groups_impl(group_handles, edge_index))
    }

    pub fn groups(&self) -> impl Iterator<Item = &Group> {
        self.group_mapping
            .groups()
            .map(|handle| self.graph.group_name_table.resolve(handle))
    }

    pub fn group_handles(&self) -> impl Iterator<Item = GroupHandle> + use<'_> {
        self.group_mapping.groups()
    }

    pub fn nodes_in_group<L: AsLookup<GroupKind>>(
        &self,
        group: L,
    ) -> GraphRecordResult<impl Iterator<Item = &NodeIndex> + use<'_, L>> {
        let group_handle = group.resolve(self)?;
        Ok(self
            .group_mapping
            .nodes_in_group(group_handle)?
            .map(|handle| self.graph.node_index_table.resolve(handle)))
    }

    pub fn node_handles_in_group<L: AsLookup<GroupKind>>(
        &self,
        group: L,
    ) -> GraphRecordResult<impl Iterator<Item = NodeHandle> + use<'_, L>> {
        let group_handle = group.resolve(self)?;
        self.group_mapping.nodes_in_group(group_handle)
    }

    pub fn ungrouped_nodes(&self) -> impl Iterator<Item = &NodeIndex> {
        let nodes_in_groups: GrHashSet<_> = self
            .group_mapping
            .nodes_in_group
            .values()
            .flat_map(|handles| handles.iter().copied())
            .collect();

        self.graph
            .nodes
            .keys()
            .filter(move |handle| !nodes_in_groups.contains(*handle))
            .map(|handle| self.graph.node_index_table.resolve(*handle))
    }

    pub fn ungrouped_node_handles(&self) -> impl Iterator<Item = NodeHandle> + use<'_> {
        let nodes_in_groups: GrHashSet<_> = self
            .group_mapping
            .nodes_in_group
            .values()
            .flat_map(|handles| handles.iter().copied())
            .collect();

        self.graph
            .nodes
            .keys()
            .filter(move |handle| !nodes_in_groups.contains(*handle))
            .copied()
    }

    pub fn edges_in_group<L: AsLookup<GroupKind>>(
        &self,
        group: L,
    ) -> GraphRecordResult<impl Iterator<Item = &EdgeIndex> + use<'_, L>> {
        let group_handle = group.resolve(self)?;
        self.group_mapping.edges_in_group(group_handle)
    }

    pub fn ungrouped_edges(&self) -> impl Iterator<Item = &EdgeIndex> {
        let edges_in_groups: GrHashSet<_> = self
            .group_mapping
            .edges_in_group
            .values()
            .flatten()
            .collect();

        self.graph
            .edge_indices()
            .filter(move |edge_index| !edges_in_groups.contains(*edge_index))
    }

    pub fn groups_of_node<L: AsLookup<NodeIndexKind>>(
        &self,
        node: L,
    ) -> GraphRecordResult<impl Iterator<Item = &Group> + use<'_, L>> {
        let node_handle = node.resolve(self)?;

        if !self.graph.contains_node(node_handle) {
            let node_index = self.graph.node_index_table.resolve(node_handle);
            return Err(GraphRecordError::IndexError(format!(
                "Cannot find node with index {node_index}"
            )));
        }

        Ok(self
            .group_mapping
            .groups_of_node(node_handle)
            .map(|handle| self.graph.group_name_table.resolve(handle)))
    }

    pub fn group_handles_of_node<L: AsLookup<NodeIndexKind>>(
        &self,
        node: L,
    ) -> GraphRecordResult<impl Iterator<Item = GroupHandle> + use<'_, L>> {
        let node_handle = node.resolve(self)?;

        if !self.graph.contains_node(node_handle) {
            let node_index = self.graph.node_index_table.resolve(node_handle);
            return Err(GraphRecordError::IndexError(format!(
                "Cannot find node with index {node_index}"
            )));
        }

        Ok(self.group_mapping.groups_of_node(node_handle))
    }

    pub fn groups_of_edge(
        &self,
        edge_index: &EdgeIndex,
    ) -> GraphRecordResult<impl Iterator<Item = &Group> + use<'_>> {
        if !self.graph.contains_edge(edge_index) {
            return Err(GraphRecordError::IndexError(format!(
                "Cannot find edge with index {edge_index}",
            )));
        }

        Ok(self
            .group_mapping
            .groups_of_edge(edge_index)
            .map(|handle| self.graph.group_name_table.resolve(handle)))
    }

    pub fn group_handles_of_edge(
        &self,
        edge_index: &EdgeIndex,
    ) -> GraphRecordResult<impl Iterator<Item = GroupHandle> + use<'_>> {
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
    pub fn contains_node(&self, node: impl AsLookup<NodeIndexKind>) -> bool {
        node.resolve(self)
            .is_ok_and(|handle| self.graph.contains_node(handle))
    }

    #[must_use]
    pub fn contains_edge(&self, edge_index: &EdgeIndex) -> bool {
        self.graph.contains_edge(edge_index)
    }

    #[must_use]
    pub fn contains_group(&self, group: impl AsLookup<GroupKind>) -> bool {
        group
            .resolve(self)
            .is_ok_and(|handle| self.group_mapping.contains_group(handle))
    }

    pub fn outgoing_neighbors<L: AsLookup<NodeIndexKind>>(
        &self,
        node: L,
    ) -> GraphRecordResult<impl Iterator<Item = &NodeIndex> + use<'_, L>> {
        let handle = node.resolve(self)?;

        self.graph
            .outgoing_neighbors(handle)
            .map_err(GraphRecordError::from)
    }

    pub fn outgoing_neighbor_handles<L: AsLookup<NodeIndexKind>>(
        &self,
        node: L,
    ) -> GraphRecordResult<impl Iterator<Item = NodeHandle> + use<'_, L>> {
        let handle = node.resolve(self)?;

        self.graph
            .outgoing_neighbor_handles(handle)
            .map_err(GraphRecordError::from)
    }

    // TODO: Add tests
    pub fn incoming_neighbors<L: AsLookup<NodeIndexKind>>(
        &self,
        node: L,
    ) -> GraphRecordResult<impl Iterator<Item = &NodeIndex> + use<'_, L>> {
        let handle = node.resolve(self)?;

        self.graph
            .incoming_neighbors(handle)
            .map_err(GraphRecordError::from)
    }

    pub fn incoming_neighbor_handles<L: AsLookup<NodeIndexKind>>(
        &self,
        node: L,
    ) -> GraphRecordResult<impl Iterator<Item = NodeHandle> + use<'_, L>> {
        let handle = node.resolve(self)?;

        self.graph
            .incoming_neighbor_handles(handle)
            .map_err(GraphRecordError::from)
    }

    pub fn neighbors<L: AsLookup<NodeIndexKind>>(
        &self,
        node: L,
    ) -> GraphRecordResult<impl Iterator<Item = &NodeIndex> + use<'_, L>> {
        let handle = node.resolve(self)?;

        self.graph.neighbors(handle).map_err(GraphRecordError::from)
    }

    pub fn neighbor_handles<L: AsLookup<NodeIndexKind>>(
        &self,
        node: L,
    ) -> GraphRecordResult<impl Iterator<Item = NodeHandle> + use<'_, L>> {
        let handle = node.resolve(self)?;

        self.graph
            .neighbor_handles(handle)
            .map_err(GraphRecordError::from)
    }

    fn clear_impl(&mut self) {
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

    pub fn overview(&self, truncate_details: Option<usize>) -> GraphRecordResult<Overview> {
        Overview::new(self, truncate_details)
    }

    pub fn group_overview(
        &self,
        group: impl AsLookup<GroupKind>,
        truncate_details: Option<usize>,
    ) -> GraphRecordResult<GroupOverview> {
        let group_handle = group.resolve(self)?;
        let group_name = self.graph.group_name_table.resolve(group_handle);

        GroupOverview::new(self, Some(group_name), truncate_details)
    }
}

#[cfg(not(feature = "plugins"))]
impl GraphRecord {
    pub fn set_schema(&mut self, schema: Schema) -> GraphRecordResult<()> {
        self.set_schema_impl(schema)
    }

    pub const fn freeze_schema(&mut self) -> GraphRecordResult<()> {
        self.freeze_schema_impl();

        Ok(())
    }

    pub const fn unfreeze_schema(&mut self) -> GraphRecordResult<()> {
        self.unfreeze_schema_impl();

        Ok(())
    }

    pub fn add_node(
        &mut self,
        node_index: NodeIndex,
        attributes: Attributes,
    ) -> GraphRecordResult<NodeHandle> {
        self.add_node_impl(node_index, attributes)
    }

    pub fn add_node_with_group(
        &mut self,
        node_index: NodeIndex,
        attributes: Attributes,
        group: impl AsLookup<GroupKind>,
    ) -> GraphRecordResult<NodeHandle> {
        self.add_node_with_group_impl(node_index, attributes, group.resolve(self)?)
    }

    pub fn add_node_with_groups(
        &mut self,
        node_index: NodeIndex,
        attributes: Attributes,
        groups: impl IntoIterator<Item = impl AsLookup<GroupKind>>,
    ) -> GraphRecordResult<NodeHandle> {
        self.add_node_with_groups_impl(node_index, attributes, &resolve_all(self, groups)?)
    }

    pub fn remove_node(
        &mut self,
        node: impl AsLookup<NodeIndexKind>,
    ) -> GraphRecordResult<Attributes> {
        self.remove_node_impl(node.resolve(self)?)
    }

    pub fn add_nodes(
        &mut self,
        nodes: Vec<(NodeIndex, Attributes)>,
    ) -> GraphRecordResult<Vec<NodeHandle>> {
        self.add_nodes_impl(nodes)
    }

    pub fn add_nodes_with_group(
        &mut self,
        nodes: Vec<(NodeIndex, Attributes)>,
        group: impl AsLookup<GroupKind>,
    ) -> GraphRecordResult<Vec<NodeHandle>> {
        self.add_nodes_with_group_impl(nodes, group.resolve(self)?)
    }

    pub fn add_nodes_with_groups(
        &mut self,
        nodes: Vec<(NodeIndex, Attributes)>,
        groups: impl IntoIterator<Item = impl AsLookup<GroupKind>>,
    ) -> GraphRecordResult<Vec<NodeHandle>> {
        self.add_nodes_with_groups_impl(nodes, &resolve_all(self, groups)?)
    }

    pub fn add_nodes_dataframes(
        &mut self,
        nodes_dataframes: impl IntoIterator<Item = impl Into<NodeDataFrameInput>>,
    ) -> GraphRecordResult<Vec<NodeHandle>> {
        self.add_nodes_dataframes_impl(nodes_dataframes)
    }

    pub fn add_nodes_dataframes_with_group(
        &mut self,
        nodes_dataframes: impl IntoIterator<Item = impl Into<NodeDataFrameInput>>,
        group: impl AsLookup<GroupKind>,
    ) -> GraphRecordResult<Vec<NodeHandle>> {
        self.add_nodes_dataframes_with_group_impl(nodes_dataframes, group.resolve(self)?)
    }

    pub fn add_nodes_dataframes_with_groups(
        &mut self,
        nodes_dataframes: impl IntoIterator<Item = impl Into<NodeDataFrameInput>>,
        groups: impl IntoIterator<Item = impl AsLookup<GroupKind>>,
    ) -> GraphRecordResult<Vec<NodeHandle>> {
        self.add_nodes_dataframes_with_groups_impl(nodes_dataframes, &resolve_all(self, groups)?)
    }

    pub fn add_edge(
        &mut self,
        source: impl AsLookup<NodeIndexKind>,
        target: impl AsLookup<NodeIndexKind>,
        attributes: Attributes,
    ) -> GraphRecordResult<EdgeIndex> {
        self.add_edge_impl(source.resolve(self)?, target.resolve(self)?, attributes)
    }

    pub fn add_edge_with_group(
        &mut self,
        source: impl AsLookup<NodeIndexKind>,
        target: impl AsLookup<NodeIndexKind>,
        attributes: Attributes,
        group: impl AsLookup<GroupKind>,
    ) -> GraphRecordResult<EdgeIndex> {
        self.add_edge_with_group_impl(
            source.resolve(self)?,
            target.resolve(self)?,
            attributes,
            group.resolve(self)?,
        )
    }

    pub fn add_edge_with_groups(
        &mut self,
        source: impl AsLookup<NodeIndexKind>,
        target: impl AsLookup<NodeIndexKind>,
        attributes: Attributes,
        groups: impl IntoIterator<Item = impl AsLookup<GroupKind>>,
    ) -> GraphRecordResult<EdgeIndex> {
        self.add_edge_with_groups_impl(
            source.resolve(self)?,
            target.resolve(self)?,
            attributes,
            &resolve_all(self, groups)?,
        )
    }

    pub fn remove_edge(&mut self, edge_index: &EdgeIndex) -> GraphRecordResult<Attributes> {
        self.remove_edge_impl(edge_index)
    }

    pub fn add_edges<S, T>(
        &mut self,
        edges: Vec<(S, T, Attributes)>,
    ) -> GraphRecordResult<Vec<EdgeIndex>>
    where
        S: AsLookup<NodeIndexKind>,
        T: AsLookup<NodeIndexKind>,
    {
        self.add_edges_impl(edges)
    }

    pub fn add_edges_with_group<S, T>(
        &mut self,
        edges: Vec<(S, T, Attributes)>,
        group: impl AsLookup<GroupKind>,
    ) -> GraphRecordResult<Vec<EdgeIndex>>
    where
        S: AsLookup<NodeIndexKind>,
        T: AsLookup<NodeIndexKind>,
    {
        self.add_edges_with_group_impl(edges, group.resolve(self)?)
    }

    pub fn add_edges_with_groups<S, T>(
        &mut self,
        edges: Vec<(S, T, Attributes)>,
        groups: impl IntoIterator<Item = impl AsLookup<GroupKind>>,
    ) -> GraphRecordResult<Vec<EdgeIndex>>
    where
        S: AsLookup<NodeIndexKind>,
        T: AsLookup<NodeIndexKind>,
    {
        self.add_edges_with_groups_impl(edges, &resolve_all(self, groups)?)
    }

    pub fn add_edges_dataframes(
        &mut self,
        edges_dataframes: impl IntoIterator<Item = impl Into<EdgeDataFrameInput>>,
    ) -> GraphRecordResult<Vec<EdgeIndex>> {
        self.add_edges_dataframes_impl(edges_dataframes)
    }

    pub fn add_edges_dataframes_with_group(
        &mut self,
        edges_dataframes: impl IntoIterator<Item = impl Into<EdgeDataFrameInput>>,
        group: impl AsLookup<GroupKind>,
    ) -> GraphRecordResult<Vec<EdgeIndex>> {
        self.add_edges_dataframes_with_group_impl(edges_dataframes, group.resolve(self)?)
    }

    pub fn add_edges_dataframes_with_groups(
        &mut self,
        edges_dataframes: impl IntoIterator<Item = impl Into<EdgeDataFrameInput>>,
        groups: impl IntoIterator<Item = impl AsLookup<GroupKind>>,
    ) -> GraphRecordResult<Vec<EdgeIndex>> {
        self.add_edges_dataframes_with_groups_impl(edges_dataframes, &resolve_all(self, groups)?)
    }

    pub fn add_group(
        &mut self,
        group: Group,
        nodes: Option<Vec<impl AsLookup<NodeIndexKind>>>,
        edge_indices: Option<Vec<EdgeIndex>>,
    ) -> GraphRecordResult<()> {
        let node_handles = match nodes {
            None => None,
            Some(nodes) => Some(resolve_all(self, nodes)?),
        };

        self.add_group_impl(group, node_handles, edge_indices)
    }

    pub fn remove_group(&mut self, group: impl AsLookup<GroupKind>) -> GraphRecordResult<()> {
        self.remove_group_impl(group.resolve(self)?)
    }

    pub fn add_node_to_group(
        &mut self,
        group: impl AsLookup<GroupKind>,
        node: impl AsLookup<NodeIndexKind>,
    ) -> GraphRecordResult<()> {
        self.add_node_to_group_impl(group.resolve(self)?, node.resolve(self)?)
    }

    pub fn add_node_to_groups(
        &mut self,
        groups: impl IntoIterator<Item = impl AsLookup<GroupKind>>,
        node: impl AsLookup<NodeIndexKind>,
    ) -> GraphRecordResult<()> {
        self.add_node_to_groups_impl(&resolve_all(self, groups)?, node.resolve(self)?)
    }

    pub fn add_nodes_to_groups(
        &mut self,
        groups: impl IntoIterator<Item = impl AsLookup<GroupKind>>,
        nodes: impl IntoIterator<Item = impl AsLookup<NodeIndexKind>>,
    ) -> GraphRecordResult<()> {
        self.add_nodes_to_groups_impl(&resolve_all(self, groups)?, resolve_all(self, nodes)?)
    }

    pub fn add_edge_to_group(
        &mut self,
        group: impl AsLookup<GroupKind>,
        edge_index: EdgeIndex,
    ) -> GraphRecordResult<()> {
        self.add_edge_to_group_impl(group.resolve(self)?, edge_index)
    }

    pub fn add_edge_to_groups(
        &mut self,
        groups: impl IntoIterator<Item = impl AsLookup<GroupKind>>,
        edge_index: EdgeIndex,
    ) -> GraphRecordResult<()> {
        self.add_edge_to_groups_impl(&resolve_all(self, groups)?, edge_index)
    }

    pub fn add_edges_to_groups(
        &mut self,
        groups: impl IntoIterator<Item = impl AsLookup<GroupKind>>,
        edge_indices: Vec<EdgeIndex>,
    ) -> GraphRecordResult<()> {
        self.add_edges_to_groups_impl(&resolve_all(self, groups)?, edge_indices)
    }

    pub fn remove_node_from_group(
        &mut self,
        group: impl AsLookup<GroupKind>,
        node: impl AsLookup<NodeIndexKind>,
    ) -> GraphRecordResult<()> {
        self.remove_node_from_group_impl(group.resolve(self)?, node.resolve(self)?)
    }

    pub fn remove_node_from_groups(
        &mut self,
        groups: impl IntoIterator<Item = impl AsLookup<GroupKind>>,
        node: impl AsLookup<NodeIndexKind>,
    ) -> GraphRecordResult<()> {
        self.remove_node_from_groups_impl(&resolve_all(self, groups)?, node.resolve(self)?)
    }

    pub fn remove_nodes_from_groups(
        &mut self,
        groups: impl IntoIterator<Item = impl AsLookup<GroupKind>>,
        nodes: impl IntoIterator<Item = impl AsLookup<NodeIndexKind>>,
    ) -> GraphRecordResult<()> {
        self.remove_nodes_from_groups_impl(&resolve_all(self, groups)?, &resolve_all(self, nodes)?)
    }

    pub fn remove_edge_from_group(
        &mut self,
        group: impl AsLookup<GroupKind>,
        edge_index: &EdgeIndex,
    ) -> GraphRecordResult<()> {
        self.remove_edge_from_group_impl(group.resolve(self)?, edge_index)
    }

    pub fn remove_edge_from_groups(
        &mut self,
        groups: impl IntoIterator<Item = impl AsLookup<GroupKind>>,
        edge_index: &EdgeIndex,
    ) -> GraphRecordResult<()> {
        self.remove_edge_from_groups_impl(&resolve_all(self, groups)?, edge_index)
    }

    pub fn remove_edges_from_groups(
        &mut self,
        groups: impl IntoIterator<Item = impl AsLookup<GroupKind>>,
        edge_indices: &[EdgeIndex],
    ) -> GraphRecordResult<()> {
        self.remove_edges_from_groups_impl(&resolve_all(self, groups)?, edge_indices)
    }

    pub fn clear(&mut self) -> GraphRecordResult<()> {
        self.clear_impl();

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::{
        Attributes, EdgeDataFrameInput, GraphRecord, GraphRecordAttribute, Group, HandleLookup,
        NodeDataFrameInput, NodeIndex, NodeIndexKind,
    };
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
                &NodeIndex::from("0"),
                &NodeIndex::from("1"),
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
                &NodeIndex::from("0"),
                &NodeIndex::from("1"),
                HashMap::from([("attribute".into(), 1.into())]),
            )
            .unwrap();
        graphrecord
            .add_edge(
                &NodeIndex::from("0"),
                &NodeIndex::from("1"),
                HashMap::from([("attribute".into(), 1.into())]),
            )
            .unwrap();
        graphrecord
            .add_edge(
                &NodeIndex::from("0"),
                &NodeIndex::from("1"),
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
                NodeIndex::from("0"),
                Some(vec![NodeIndex::from("0"), NodeIndex::from("1")]),
                Some(vec![0, 1]),
            )
            .unwrap();
        graphrecord
            .add_group(
                NodeIndex::from("1"),
                Some(vec![NodeIndex::from("0"), NodeIndex::from("1")]),
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
                &NodeIndex::from("0"),
                &NodeIndex::from("1"),
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

        graphrecord.freeze_schema().unwrap();

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

        graphrecord.unfreeze_schema().unwrap();

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

        let node_index: NodeIndex = "0".into();
        let attributes = graphrecord.node_attributes(&node_index).unwrap();

        assert_eq!(&create_nodes()[0].1, attributes);
    }

    #[test]
    fn test_invalid_node_attributes() {
        let graphrecord = create_graphrecord();

        let node_index: NodeIndex = "50".into();

        // Querying a non-existing node should fail
        assert!(
            graphrecord
                .node_attributes(&node_index)
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );
    }

    #[test]
    fn test_node_attributes_mut() {
        let mut graphrecord = create_graphrecord();

        let node_index = NodeIndex::from("0");
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
                .node_attributes_mut(&NodeIndex::from("50"))
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );
    }

    #[test]
    fn test_outgoing_edges() {
        let graphrecord = create_graphrecord();

        let node_index: NodeIndex = "0".into();
        let edges = graphrecord.outgoing_edges(&node_index).unwrap();

        assert_eq!(2, edges.count());
    }

    #[test]
    fn test_invalid_outgoing_edges() {
        let graphrecord = create_graphrecord();

        let node_index: NodeIndex = "50".into();

        assert!(
            graphrecord
                .outgoing_edges(&node_index)
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );
    }

    #[test]
    fn test_incoming_edges() {
        let graphrecord = create_graphrecord();

        let node_index: NodeIndex = "2".into();
        let edges = graphrecord.incoming_edges(&node_index).unwrap();

        assert_eq!(2, edges.count());
    }

    #[test]
    fn test_invalid_incoming_edges() {
        let graphrecord = create_graphrecord();

        let node_index: NodeIndex = "50".into();

        assert!(
            graphrecord
                .incoming_edges(&node_index)
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

        let (source_node_index, target_node_index) = graphrecord.edge_endpoints(&0).unwrap();

        assert_eq!(&edge.0, source_node_index);
        assert_eq!(&edge.1, target_node_index);
    }

    #[test]
    fn test_edge_endpoint_handles() {
        let graphrecord = create_graphrecord();

        let edge = &create_edges()[0];

        let (source_handle, target_handle) = graphrecord.edge_endpoint_handles(&0).unwrap();

        assert_eq!(
            &edge.0,
            <GraphRecord as HandleLookup<NodeIndexKind>>::resolve_handle(
                &graphrecord,
                source_handle
            )
            .unwrap()
        );

        assert_eq!(
            &edge.1,
            <GraphRecord as HandleLookup<NodeIndexKind>>::resolve_handle(
                &graphrecord,
                target_handle
            )
            .unwrap()
        );
    }

    #[test]
    fn test_invalid_edge_endpoint_handles() {
        let graphrecord = create_graphrecord();

        assert!(
            graphrecord
                .edge_endpoint_handles(&50)
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );
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

        let first_index = NodeIndex::from("0");
        let second_index = NodeIndex::from("1");
        let edges_connecting = graphrecord
            .edges_connecting(vec![&first_index], vec![&second_index])
            .unwrap();

        assert_eq!(vec![&0], edges_connecting.collect::<Vec<_>>());

        let first_index = NodeIndex::from("0");
        let second_index = NodeIndex::from("3");
        let edges_connecting = graphrecord
            .edges_connecting(vec![&first_index], vec![&second_index])
            .unwrap();

        assert_eq!(0, edges_connecting.count());

        let first_index = NodeIndex::from("0");
        let second_index = NodeIndex::from("1");
        let third_index = NodeIndex::from("2");
        let mut edges_connecting: Vec<_> = graphrecord
            .edges_connecting(vec![&first_index, &second_index], vec![&third_index])
            .unwrap()
            .collect();

        edges_connecting.sort();
        assert_eq!(vec![&2, &3], edges_connecting);

        let first_index = NodeIndex::from("0");
        let second_index = NodeIndex::from("1");
        let third_index = NodeIndex::from("2");
        let fourth_index = NodeIndex::from("3");
        let mut edges_connecting: Vec<_> = graphrecord
            .edges_connecting(
                vec![&first_index, &second_index],
                vec![&third_index, &fourth_index],
            )
            .unwrap()
            .collect();

        edges_connecting.sort();
        assert_eq!(vec![&2, &3], edges_connecting);
    }

    #[test]
    fn test_invalid_edges_connecting() {
        let graphrecord = create_graphrecord();

        let missing = NodeIndex::from("50");
        let valid = NodeIndex::from("0");

        assert!(
            graphrecord
                .edges_connecting(vec![&missing], vec![&valid])
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );
    }

    #[test]
    fn test_edges_connecting_undirected() {
        let graphrecord = create_graphrecord();

        let first_index = NodeIndex::from("0");
        let second_index = NodeIndex::from("1");
        let mut edges_connecting: Vec<_> = graphrecord
            .edges_connecting_undirected(vec![&first_index], vec![&second_index])
            .unwrap()
            .collect();

        edges_connecting.sort();
        assert_eq!(vec![&0, &1], edges_connecting);
    }

    #[test]
    fn test_invalid_edges_connecting_undirected() {
        let graphrecord = create_graphrecord();

        let missing = NodeIndex::from("50");
        let valid = NodeIndex::from("0");

        assert!(
            graphrecord
                .edges_connecting_undirected(vec![&missing], vec![&valid])
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );
    }

    #[test]
    fn test_add_node() {
        let mut graphrecord = GraphRecord::new();

        assert_eq!(0, graphrecord.node_count());

        graphrecord.add_node("0".into(), HashMap::new()).unwrap();

        assert_eq!(1, graphrecord.node_count());

        graphrecord.freeze_schema().unwrap();

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

        graphrecord.freeze_schema().unwrap();

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
            .add_group(
                Group::from("group"),
                Some(vec![NodeIndex::from("0")]),
                Some(vec![0]),
            )
            .unwrap();

        let nodes = create_nodes();

        assert_eq!(4, graphrecord.node_count());
        assert_eq!(4, graphrecord.edge_count());
        assert_eq!(
            1,
            graphrecord
                .nodes_in_group(&Group::from("group"))
                .unwrap()
                .count()
        );
        assert_eq!(
            1,
            graphrecord
                .edges_in_group(&Group::from("group"))
                .unwrap()
                .count()
        );

        assert_eq!(
            nodes[0].1,
            graphrecord.remove_node(&NodeIndex::from("0")).unwrap()
        );

        assert_eq!(3, graphrecord.node_count());
        assert_eq!(1, graphrecord.edge_count());
        assert_eq!(
            0,
            graphrecord
                .nodes_in_group(&Group::from("group"))
                .unwrap()
                .count()
        );
        assert_eq!(
            0,
            graphrecord
                .edges_in_group(&Group::from("group"))
                .unwrap()
                .count()
        );

        let mut graphrecord = GraphRecord::new();

        graphrecord.add_node(0.into(), HashMap::new()).unwrap();
        graphrecord
            .add_edge(&NodeIndex::from(0), &NodeIndex::from(0), HashMap::new())
            .unwrap();

        assert_eq!(1, graphrecord.node_count());
        assert_eq!(1, graphrecord.edge_count());

        assert!(graphrecord.remove_node(&NodeIndex::from(0)).is_ok());

        assert_eq!(0, graphrecord.node_count());
        assert_eq!(0, graphrecord.edge_count());
    }

    #[test]
    fn test_invalid_remove_node() {
        let mut graphrecord = create_graphrecord();

        // Removing a non-existing node should fail
        assert!(
            graphrecord
                .remove_node(&NodeIndex::from("50"))
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
            .add_edge(&NodeIndex::from("0"), &NodeIndex::from("3"), HashMap::new())
            .unwrap();

        assert_eq!(5, graphrecord.edge_count());

        graphrecord.freeze_schema().unwrap();

        graphrecord
            .add_edge(&NodeIndex::from("0"), &NodeIndex::from("3"), HashMap::new())
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
                .add_edge(
                    &NodeIndex::from("0"),
                    &NodeIndex::from("50"),
                    HashMap::new()
                )
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );

        // Adding an edge from a non-existing node should fail
        assert!(
            graphrecord
                .add_edge(
                    &NodeIndex::from("50"),
                    &NodeIndex::from("0"),
                    HashMap::new()
                )
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );

        graphrecord.freeze_schema().unwrap();

        assert!(
            graphrecord
                .add_edge(
                    &NodeIndex::from("0"),
                    &NodeIndex::from("3"),
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

        graphrecord
            .add_group(Group::from("0"), None::<Vec<NodeIndex>>, None)
            .unwrap();

        assert_eq!(1, graphrecord.group_count());

        graphrecord
            .add_group(
                Group::from("1"),
                Some(vec![NodeIndex::from("0"), NodeIndex::from("1")]),
                None,
            )
            .unwrap();

        assert_eq!(2, graphrecord.group_count());

        assert_eq!(
            2,
            graphrecord
                .nodes_in_group(&Group::from("1"))
                .unwrap()
                .count()
        );
    }

    #[test]
    fn test_invalid_add_group() {
        let mut graphrecord = create_graphrecord();

        // Adding a group with a non-existing node should fail
        assert!(
            graphrecord
                .add_group(Group::from("0"), Some(vec![NodeIndex::from("50")]), None)
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );

        // Adding a group with a non-existing edge should fail
        assert!(
            graphrecord
                .add_group(Group::from("0"), None::<Vec<NodeIndex>>, Some(vec![50]))
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );

        graphrecord
            .add_group(Group::from("0"), None::<Vec<NodeIndex>>, None)
            .unwrap();

        // Adding an already existing group should fail
        assert!(
            graphrecord
                .add_group(Group::from("0"), None::<Vec<NodeIndex>>, None)
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );

        graphrecord.freeze_schema().unwrap();

        assert!(
            graphrecord
                .add_group(Group::from("2"), None::<Vec<NodeIndex>>, None)
                .is_err_and(|e| matches!(e, GraphRecordError::SchemaError(_)))
        );

        graphrecord.remove_group(&Group::from("0")).unwrap();

        assert!(
            graphrecord
                .add_group(Group::from("0"), Some(vec![NodeIndex::from("0")]), None)
                .is_err_and(|e| matches!(e, GraphRecordError::SchemaError(_)))
        );
        assert!(
            graphrecord
                .add_group(Group::from("0"), None::<Vec<NodeIndex>>, Some(vec![0]))
                .is_err_and(|e| matches!(e, GraphRecordError::SchemaError(_)))
        );
    }

    #[test]
    fn test_remove_group() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group(Group::from("0"), None::<Vec<NodeIndex>>, None)
            .unwrap();

        assert_eq!(1, graphrecord.group_count());

        graphrecord.remove_group(&Group::from("0")).unwrap();

        assert_eq!(0, graphrecord.group_count());
    }

    #[test]
    fn test_invalid_remove_group() {
        let mut graphrecord = GraphRecord::new();

        // Removing a non-existing group should fail
        assert!(
            graphrecord
                .remove_group(&Group::from("0"))
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );
    }

    #[test]
    fn test_add_node_to_group() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group(
                Group::from("0"),
                Some(vec![NodeIndex::from("0"), NodeIndex::from("1")]),
                None,
            )
            .unwrap();

        assert_eq!(
            2,
            graphrecord
                .nodes_in_group(&Group::from("0"))
                .unwrap()
                .count()
        );

        graphrecord
            .add_node_to_group(&Group::from("0"), &NodeIndex::from("2"))
            .unwrap();

        assert_eq!(
            3,
            graphrecord
                .nodes_in_group(&Group::from("0"))
                .unwrap()
                .count()
        );

        graphrecord
            .add_node("4".into(), HashMap::from([("test".into(), "test".into())]))
            .unwrap();

        graphrecord
            .add_group(Group::from("1"), Some(vec![NodeIndex::from("4")]), None)
            .unwrap();

        graphrecord.freeze_schema().unwrap();

        graphrecord
            .add_node("5".into(), HashMap::from([("test".into(), "test".into())]))
            .unwrap();

        assert!(
            graphrecord
                .add_node_to_group(&Group::from("1"), &NodeIndex::from("5"))
                .is_ok()
        );

        assert_eq!(
            2,
            graphrecord
                .nodes_in_group(&Group::from("1"))
                .unwrap()
                .count()
        );
    }

    #[test]
    fn test_invalid_add_node_to_group() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group(Group::from("0"), Some(vec![NodeIndex::from("0")]), None)
            .unwrap();

        // Adding a non-existing node to a group should fail
        assert!(
            graphrecord
                .add_node_to_group(&Group::from("0"), &NodeIndex::from("50"))
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );

        // Adding a node to a group that already is in the group should fail
        assert!(
            graphrecord
                .add_node_to_group(&Group::from("0"), &NodeIndex::from("0"))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );

        let mut graphrecord = GraphRecord::new();

        graphrecord
            .add_node("0".into(), HashMap::from([("test".into(), "test".into())]))
            .unwrap();
        graphrecord
            .add_group(Group::from("group"), None::<Vec<NodeIndex>>, None)
            .unwrap();

        graphrecord.freeze_schema().unwrap();

        assert!(
            graphrecord
                .add_node_to_group(&Group::from("group"), &NodeIndex::from("0"))
                .is_err_and(|e| matches!(e, GraphRecordError::SchemaError(_)))
        );
    }

    #[test]
    fn test_add_node_to_groups() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group(Group::from("0"), Some(vec![NodeIndex::from("0")]), None)
            .unwrap();
        graphrecord
            .add_group(Group::from("1"), Some(vec![NodeIndex::from("1")]), None)
            .unwrap();

        graphrecord
            .add_node_to_groups(&[Group::from("0"), Group::from("1")], &NodeIndex::from("2"))
            .unwrap();

        assert_eq!(
            2,
            graphrecord
                .nodes_in_group(&Group::from("0"))
                .unwrap()
                .count()
        );
        assert_eq!(
            2,
            graphrecord
                .nodes_in_group(&Group::from("1"))
                .unwrap()
                .count()
        );
    }

    #[test]
    fn test_invalid_add_node_to_groups() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group(Group::from("0"), Some(vec![NodeIndex::from("0")]), None)
            .unwrap();
        graphrecord
            .add_group(Group::from("1"), Some(vec![NodeIndex::from("1")]), None)
            .unwrap();

        assert!(
            graphrecord
                .add_node_to_groups(
                    &[Group::from("0"), Group::from("1")],
                    &NodeIndex::from("50")
                )
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );

        assert!(
            graphrecord
                .add_node_to_groups(&[Group::from("0"), Group::from("1")], &NodeIndex::from("0"))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );

        let mut graphrecord = GraphRecord::new();

        graphrecord
            .add_node("0".into(), HashMap::from([("test".into(), "test".into())]))
            .unwrap();
        graphrecord
            .add_group(Group::from("group"), None::<Vec<NodeIndex>>, None)
            .unwrap();
        graphrecord
            .add_group(Group::from("group2"), None::<Vec<NodeIndex>>, None)
            .unwrap();

        graphrecord.freeze_schema().unwrap();

        assert!(
            graphrecord
                .add_node_to_groups(
                    &[Group::from("group"), Group::from("group2")],
                    &NodeIndex::from("0")
                )
                .is_err_and(|e| matches!(e, GraphRecordError::SchemaError(_)))
        );
    }

    #[test]
    fn test_add_edge_to_group() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group(Group::from("0"), None::<Vec<NodeIndex>>, Some(vec![0, 1]))
            .unwrap();

        assert_eq!(
            2,
            graphrecord
                .edges_in_group(&Group::from("0"))
                .unwrap()
                .count()
        );

        graphrecord.add_edge_to_group(&Group::from("0"), 2).unwrap();

        assert_eq!(
            3,
            graphrecord
                .edges_in_group(&Group::from("0"))
                .unwrap()
                .count()
        );

        graphrecord
            .add_edge(&NodeIndex::from("0"), &NodeIndex::from("1"), HashMap::new())
            .unwrap();

        graphrecord
            .add_group(Group::from("1"), None::<Vec<NodeIndex>>, Some(vec![3]))
            .unwrap();

        graphrecord.freeze_schema().unwrap();

        let edge_index = graphrecord
            .add_edge(&NodeIndex::from("0"), &NodeIndex::from("1"), HashMap::new())
            .unwrap();

        assert!(
            graphrecord
                .add_edge_to_group(&Group::from("1"), edge_index)
                .is_ok()
        );

        assert_eq!(
            2,
            graphrecord
                .edges_in_group(&Group::from("1"))
                .unwrap()
                .count()
        );
    }

    #[test]
    fn test_invalid_add_edge_to_group() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group(Group::from("0"), None::<Vec<NodeIndex>>, Some(vec![0]))
            .unwrap();

        // Adding a non-existing edge to a group should fail
        assert!(
            graphrecord
                .add_edge_to_group(&Group::from("0"), 50)
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );

        // Adding an edge to a group that already is in the group should fail
        assert!(
            graphrecord
                .add_edge_to_group(&Group::from("0"), 0)
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );

        let mut graphrecord = GraphRecord::new();

        graphrecord.add_node("0".into(), HashMap::new()).unwrap();
        graphrecord
            .add_edge(
                &NodeIndex::from("0"),
                &NodeIndex::from("0"),
                HashMap::from([("test".into(), "test".into())]),
            )
            .unwrap();
        graphrecord
            .add_group(Group::from("group"), None::<Vec<NodeIndex>>, None)
            .unwrap();

        graphrecord.freeze_schema().unwrap();

        assert!(
            graphrecord
                .add_edge_to_group(&Group::from("group"), 0)
                .is_err_and(|e| matches!(e, GraphRecordError::SchemaError(_)))
        );
    }

    #[test]
    fn test_add_edge_to_groups() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group(Group::from("0"), None::<Vec<NodeIndex>>, Some(vec![0]))
            .unwrap();
        graphrecord
            .add_group(Group::from("1"), None::<Vec<NodeIndex>>, Some(vec![1]))
            .unwrap();

        graphrecord
            .add_edge_to_groups(&[Group::from("0"), Group::from("1")], 2)
            .unwrap();

        assert_eq!(
            2,
            graphrecord
                .edges_in_group(&Group::from("0"))
                .unwrap()
                .count()
        );
        assert_eq!(
            2,
            graphrecord
                .edges_in_group(&Group::from("1"))
                .unwrap()
                .count()
        );
    }

    #[test]
    fn test_invalid_add_edge_to_groups() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group(Group::from("0"), None::<Vec<NodeIndex>>, Some(vec![0]))
            .unwrap();
        graphrecord
            .add_group(Group::from("1"), None::<Vec<NodeIndex>>, Some(vec![1]))
            .unwrap();

        assert!(
            graphrecord
                .add_edge_to_groups(&[Group::from("0"), Group::from("1")], 50)
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );

        assert!(
            graphrecord
                .add_edge_to_groups(&[Group::from("0"), Group::from("1")], 0)
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );

        let mut graphrecord = GraphRecord::new();

        graphrecord.add_node("0".into(), HashMap::new()).unwrap();
        graphrecord
            .add_edge(
                &NodeIndex::from("0"),
                &NodeIndex::from("0"),
                HashMap::from([("test".into(), "test".into())]),
            )
            .unwrap();
        graphrecord
            .add_group(Group::from("group"), None::<Vec<NodeIndex>>, None)
            .unwrap();
        graphrecord
            .add_group(Group::from("group2"), None::<Vec<NodeIndex>>, None)
            .unwrap();

        graphrecord.freeze_schema().unwrap();

        assert!(
            graphrecord
                .add_edge_to_groups(&[Group::from("group"), Group::from("group2")], 0)
                .is_err_and(|e| matches!(e, GraphRecordError::SchemaError(_)))
        );
    }

    #[test]
    fn test_remove_node_from_group() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group(
                Group::from("0"),
                Some(vec![NodeIndex::from("0"), NodeIndex::from("1")]),
                None,
            )
            .unwrap();

        assert_eq!(
            2,
            graphrecord
                .nodes_in_group(&Group::from("0"))
                .unwrap()
                .count()
        );

        graphrecord
            .remove_node_from_group(&Group::from("0"), &NodeIndex::from("0"))
            .unwrap();

        assert_eq!(
            1,
            graphrecord
                .nodes_in_group(&Group::from("0"))
                .unwrap()
                .count()
        );
    }

    #[test]
    fn test_invalid_remove_node_from_group() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group(Group::from("0"), Some(vec![NodeIndex::from("0")]), None)
            .unwrap();

        // Removing a node from a non-existing group should fail
        assert!(
            graphrecord
                .remove_node_from_group(&Group::from("50"), &NodeIndex::from("0"))
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );

        // Removing a non-existing node from a group should fail
        assert!(
            graphrecord
                .remove_node_from_group(&Group::from("0"), &NodeIndex::from("50"))
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );

        // Removing a node from a group it is not in should fail
        assert!(
            graphrecord
                .remove_node_from_group(&Group::from("0"), &NodeIndex::from("1"))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
    }

    #[test]
    fn test_remove_node_from_groups() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group(
                Group::from("0"),
                Some(vec![NodeIndex::from("0"), NodeIndex::from("1")]),
                None,
            )
            .unwrap();
        graphrecord
            .add_group(
                Group::from("1"),
                Some(vec![NodeIndex::from("0"), NodeIndex::from("2")]),
                None,
            )
            .unwrap();

        graphrecord
            .remove_node_from_groups(&[Group::from("0"), Group::from("1")], &NodeIndex::from("0"))
            .unwrap();

        assert_eq!(
            1,
            graphrecord
                .nodes_in_group(&Group::from("0"))
                .unwrap()
                .count()
        );
        assert_eq!(
            1,
            graphrecord
                .nodes_in_group(&Group::from("1"))
                .unwrap()
                .count()
        );
    }

    #[test]
    fn test_invalid_remove_node_from_groups() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group(Group::from("0"), Some(vec![NodeIndex::from("0")]), None)
            .unwrap();
        graphrecord
            .add_group(Group::from("1"), Some(vec![NodeIndex::from("1")]), None)
            .unwrap();

        assert!(
            graphrecord
                .remove_node_from_groups(
                    &[Group::from("0"), Group::from("1")],
                    &NodeIndex::from("50")
                )
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );

        assert!(
            graphrecord
                .remove_node_from_groups(
                    &[Group::from("0"), Group::from("1")],
                    &NodeIndex::from("1")
                )
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
    }

    #[test]
    fn test_remove_edge_from_group() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group(Group::from("0"), None::<Vec<NodeIndex>>, Some(vec![0, 1]))
            .unwrap();

        assert_eq!(
            2,
            graphrecord
                .edges_in_group(&Group::from("0"))
                .unwrap()
                .count()
        );

        graphrecord
            .remove_edge_from_group(&Group::from("0"), &0)
            .unwrap();

        assert_eq!(
            1,
            graphrecord
                .edges_in_group(&Group::from("0"))
                .unwrap()
                .count()
        );
    }

    #[test]
    fn test_invalid_remove_edge_from_group() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group(Group::from("0"), None::<Vec<NodeIndex>>, Some(vec![0]))
            .unwrap();

        // Removing an edge from a non-existing group should fail
        assert!(
            graphrecord
                .remove_edge_from_group(&Group::from("50"), &0)
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );

        // Removing a non-existing edge from a group should fail
        assert!(
            graphrecord
                .remove_edge_from_group(&Group::from("0"), &50)
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );

        // Removing an edge from a group it is not in should fail
        assert!(
            graphrecord
                .remove_edge_from_group(&Group::from("0"), &1)
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
    }

    #[test]
    fn test_remove_edge_from_groups() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group(Group::from("0"), None::<Vec<NodeIndex>>, Some(vec![0, 1]))
            .unwrap();
        graphrecord
            .add_group(Group::from("1"), None::<Vec<NodeIndex>>, Some(vec![0, 2]))
            .unwrap();

        graphrecord
            .remove_edge_from_groups(&[Group::from("0"), Group::from("1")], &0)
            .unwrap();

        assert_eq!(
            1,
            graphrecord
                .edges_in_group(&Group::from("0"))
                .unwrap()
                .count()
        );
        assert_eq!(
            1,
            graphrecord
                .edges_in_group(&Group::from("1"))
                .unwrap()
                .count()
        );
    }

    #[test]
    fn test_invalid_remove_edge_from_groups() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group(Group::from("0"), None::<Vec<NodeIndex>>, Some(vec![0]))
            .unwrap();
        graphrecord
            .add_group(Group::from("1"), None::<Vec<NodeIndex>>, Some(vec![1]))
            .unwrap();

        assert!(
            graphrecord
                .remove_edge_from_groups(&[Group::from("0"), Group::from("1")], &50)
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );

        assert!(
            graphrecord
                .remove_edge_from_groups(&[Group::from("0"), Group::from("1")], &1)
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
    }

    #[test]
    fn test_add_nodes_to_groups() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group(Group::from("0"), Some(vec![NodeIndex::from("0")]), None)
            .unwrap();
        graphrecord
            .add_group(Group::from("1"), Some(vec![NodeIndex::from("1")]), None)
            .unwrap();

        graphrecord
            .add_nodes_to_groups(
                &[Group::from("0"), Group::from("1")],
                vec![NodeIndex::from("2"), NodeIndex::from("3")],
            )
            .unwrap();

        assert_eq!(
            3,
            graphrecord
                .nodes_in_group(&Group::from("0"))
                .unwrap()
                .count()
        );
        assert_eq!(
            3,
            graphrecord
                .nodes_in_group(&Group::from("1"))
                .unwrap()
                .count()
        );
    }

    #[test]
    fn test_invalid_add_nodes_to_groups() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group(Group::from("0"), Some(vec![NodeIndex::from("0")]), None)
            .unwrap();
        graphrecord
            .add_group(Group::from("1"), Some(vec![NodeIndex::from("1")]), None)
            .unwrap();

        assert!(
            graphrecord
                .add_nodes_to_groups(
                    &[Group::from("0"), Group::from("1")],
                    vec![NodeIndex::from("50")]
                )
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );

        assert!(
            graphrecord
                .add_nodes_to_groups(
                    &[Group::from("0"), Group::from("1")],
                    vec![NodeIndex::from("0")]
                )
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );

        let mut graphrecord = GraphRecord::new();

        graphrecord
            .add_node("0".into(), HashMap::from([("test".into(), "test".into())]))
            .unwrap();
        graphrecord
            .add_group(Group::from("group"), None::<Vec<NodeIndex>>, None)
            .unwrap();
        graphrecord
            .add_group(Group::from("group2"), None::<Vec<NodeIndex>>, None)
            .unwrap();

        graphrecord.freeze_schema().unwrap();

        assert!(
            graphrecord
                .add_nodes_to_groups(
                    &[Group::from("group"), Group::from("group2")],
                    vec![NodeIndex::from("0")]
                )
                .is_err_and(|e| matches!(e, GraphRecordError::SchemaError(_)))
        );
    }

    #[test]
    fn test_add_edges_to_groups() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group(Group::from("0"), None::<Vec<NodeIndex>>, Some(vec![0]))
            .unwrap();
        graphrecord
            .add_group(Group::from("1"), None::<Vec<NodeIndex>>, Some(vec![1]))
            .unwrap();

        graphrecord
            .add_edges_to_groups(&[Group::from("0"), Group::from("1")], vec![2, 3])
            .unwrap();

        assert_eq!(
            3,
            graphrecord
                .edges_in_group(&Group::from("0"))
                .unwrap()
                .count()
        );
        assert_eq!(
            3,
            graphrecord
                .edges_in_group(&Group::from("1"))
                .unwrap()
                .count()
        );
    }

    #[test]
    fn test_invalid_add_edges_to_groups() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group(Group::from("0"), None::<Vec<NodeIndex>>, Some(vec![0]))
            .unwrap();
        graphrecord
            .add_group(Group::from("1"), None::<Vec<NodeIndex>>, Some(vec![1]))
            .unwrap();

        assert!(
            graphrecord
                .add_edges_to_groups(&[Group::from("0"), Group::from("1")], vec![50])
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );

        assert!(
            graphrecord
                .add_edges_to_groups(&[Group::from("0"), Group::from("1")], vec![0])
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );

        let mut graphrecord = GraphRecord::new();

        graphrecord.add_node("0".into(), HashMap::new()).unwrap();
        graphrecord
            .add_edge(
                &NodeIndex::from("0"),
                &NodeIndex::from("0"),
                HashMap::from([("test".into(), "test".into())]),
            )
            .unwrap();
        graphrecord
            .add_group(Group::from("group"), None::<Vec<NodeIndex>>, None)
            .unwrap();
        graphrecord
            .add_group(Group::from("group2"), None::<Vec<NodeIndex>>, None)
            .unwrap();

        graphrecord.freeze_schema().unwrap();

        assert!(
            graphrecord
                .add_edges_to_groups(&[Group::from("group"), Group::from("group2")], vec![0])
                .is_err_and(|e| matches!(e, GraphRecordError::SchemaError(_)))
        );
    }

    #[test]
    fn test_remove_nodes_from_groups() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group(
                NodeIndex::from("0"),
                Some(vec![
                    NodeIndex::from("0"),
                    NodeIndex::from("1"),
                    NodeIndex::from("2"),
                ]),
                None,
            )
            .unwrap();
        graphrecord
            .add_group(
                NodeIndex::from("1"),
                Some(vec![
                    NodeIndex::from("0"),
                    NodeIndex::from("1"),
                    NodeIndex::from("2"),
                ]),
                None,
            )
            .unwrap();

        graphrecord
            .remove_nodes_from_groups(
                &[Group::from("0"), Group::from("1")],
                &[Group::from("0"), Group::from("1")],
            )
            .unwrap();

        assert_eq!(
            1,
            graphrecord
                .nodes_in_group(&Group::from("0"))
                .unwrap()
                .count()
        );
        assert_eq!(
            1,
            graphrecord
                .nodes_in_group(&Group::from("1"))
                .unwrap()
                .count()
        );
    }

    #[test]
    fn test_invalid_remove_nodes_from_groups() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group(Group::from("0"), Some(vec![NodeIndex::from("0")]), None)
            .unwrap();
        graphrecord
            .add_group(Group::from("1"), Some(vec![NodeIndex::from("1")]), None)
            .unwrap();

        assert!(
            graphrecord
                .remove_nodes_from_groups(
                    &[Group::from("0"), Group::from("1")],
                    &[Group::from("50")],
                )
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );

        assert!(
            graphrecord
                .remove_nodes_from_groups(
                    &[Group::from("0"), Group::from("1")],
                    &[Group::from("1")],
                )
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
    }

    #[test]
    fn test_remove_edges_from_groups() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group(
                Group::from("0"),
                None::<Vec<NodeIndex>>,
                Some(vec![0, 1, 2]),
            )
            .unwrap();
        graphrecord
            .add_group(
                Group::from("1"),
                None::<Vec<NodeIndex>>,
                Some(vec![0, 1, 2]),
            )
            .unwrap();

        graphrecord
            .remove_edges_from_groups(&[Group::from("0"), Group::from("1")], &[0, 1])
            .unwrap();

        assert_eq!(
            1,
            graphrecord
                .edges_in_group(&Group::from("0"))
                .unwrap()
                .count()
        );
        assert_eq!(
            1,
            graphrecord
                .edges_in_group(&Group::from("1"))
                .unwrap()
                .count()
        );
    }

    #[test]
    fn test_invalid_remove_edges_from_groups() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group(Group::from("0"), None::<Vec<NodeIndex>>, Some(vec![0]))
            .unwrap();
        graphrecord
            .add_group(Group::from("1"), None::<Vec<NodeIndex>>, Some(vec![1]))
            .unwrap();

        assert!(
            graphrecord
                .remove_edges_from_groups(&[Group::from("0"), Group::from("1")], &[50])
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );

        assert!(
            graphrecord
                .remove_edges_from_groups(&[Group::from("0"), Group::from("1")], &[1])
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
    }

    #[test]
    fn test_add_node_with_groups() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group(Group::from("0"), None::<Vec<NodeIndex>>, None)
            .unwrap();
        graphrecord
            .add_group(Group::from("1"), None::<Vec<NodeIndex>>, None)
            .unwrap();

        graphrecord
            .add_node_with_groups(
                "4".into(),
                HashMap::from([("lorem".into(), "ipsum".into())]),
                &[Group::from("0"), Group::from("1")],
            )
            .unwrap();

        assert_eq!(5, graphrecord.node_count());
        assert_eq!(
            1,
            graphrecord
                .nodes_in_group(&Group::from("0"))
                .unwrap()
                .count()
        );
        assert_eq!(
            1,
            graphrecord
                .nodes_in_group(&Group::from("1"))
                .unwrap()
                .count()
        );
    }

    #[test]
    fn test_invalid_add_node_with_groups() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group(Group::from("0"), None::<Vec<NodeIndex>>, None)
            .unwrap();
        graphrecord
            .add_group(Group::from("1"), None::<Vec<NodeIndex>>, None)
            .unwrap();

        assert!(
            graphrecord
                .add_node_with_groups(
                    "0".into(),
                    HashMap::new(),
                    &[Group::from("0"), Group::from("1")],
                )
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
    }

    #[test]
    fn test_add_edge_with_groups() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group(Group::from("0"), None::<Vec<NodeIndex>>, None)
            .unwrap();
        graphrecord
            .add_group(Group::from("1"), None::<Vec<NodeIndex>>, None)
            .unwrap();

        let edge_index = graphrecord
            .add_edge_with_groups(
                &NodeIndex::from("0"),
                &NodeIndex::from("1"),
                HashMap::from([("sed".into(), "do".into())]),
                &[Group::from("0"), Group::from("1")],
            )
            .unwrap();

        assert_eq!(5, graphrecord.edge_count());
        assert_eq!(4, edge_index);
        assert_eq!(
            1,
            graphrecord
                .edges_in_group(&Group::from("0"))
                .unwrap()
                .count()
        );
        assert_eq!(
            1,
            graphrecord
                .edges_in_group(&Group::from("1"))
                .unwrap()
                .count()
        );
    }

    #[test]
    fn test_invalid_add_edge_with_groups() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group(Group::from("0"), None::<Vec<NodeIndex>>, None)
            .unwrap();
        graphrecord
            .add_group(Group::from("1"), None::<Vec<NodeIndex>>, None)
            .unwrap();

        assert!(
            graphrecord
                .add_edge_with_groups(
                    &NodeIndex::from("50"),
                    &NodeIndex::from("0"),
                    HashMap::new(),
                    &[Group::from("0"), Group::from("1")],
                )
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );

        assert!(
            graphrecord
                .add_edge_with_groups(
                    &NodeIndex::from("0"),
                    &NodeIndex::from("50"),
                    HashMap::new(),
                    &[Group::from("0"), Group::from("1")],
                )
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );
    }

    #[test]
    fn test_add_nodes_with_groups() {
        let mut graphrecord = GraphRecord::new();

        graphrecord
            .add_group(Group::from("0"), None::<Vec<NodeIndex>>, None)
            .unwrap();
        graphrecord
            .add_group(Group::from("1"), None::<Vec<NodeIndex>>, None)
            .unwrap();

        graphrecord
            .add_nodes_with_groups(
                vec![
                    (
                        "0".into(),
                        HashMap::from([("lorem".into(), "ipsum".into())]),
                    ),
                    (
                        "1".into(),
                        HashMap::from([("amet".into(), "consectetur".into())]),
                    ),
                ],
                &[Group::from("0"), Group::from("1")],
            )
            .unwrap();

        assert_eq!(2, graphrecord.node_count());
        assert_eq!(
            2,
            graphrecord
                .nodes_in_group(&Group::from("0"))
                .unwrap()
                .count()
        );
        assert_eq!(
            2,
            graphrecord
                .nodes_in_group(&Group::from("1"))
                .unwrap()
                .count()
        );
    }

    #[test]
    fn test_invalid_add_nodes_with_groups() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group(Group::from("0"), None::<Vec<NodeIndex>>, None)
            .unwrap();
        graphrecord
            .add_group(Group::from("1"), None::<Vec<NodeIndex>>, None)
            .unwrap();

        assert!(
            graphrecord
                .add_nodes_with_groups(
                    vec![("0".into(), HashMap::new())],
                    &[Group::from("0"), Group::from("1")],
                )
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
    }

    #[test]
    fn test_add_edges_with_groups() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group(Group::from("0"), None::<Vec<NodeIndex>>, None)
            .unwrap();
        graphrecord
            .add_group(Group::from("1"), None::<Vec<NodeIndex>>, None)
            .unwrap();

        let edge_indices = graphrecord
            .add_edges_with_groups(
                vec![
                    (
                        NodeIndex::from("0"),
                        NodeIndex::from("1"),
                        HashMap::from([("sed".into(), "do".into())]),
                    ),
                    (
                        NodeIndex::from("1"),
                        NodeIndex::from("0"),
                        HashMap::from([("sed".into(), "do".into())]),
                    ),
                ],
                &[Group::from("0"), Group::from("1")],
            )
            .unwrap();

        assert_eq!(6, graphrecord.edge_count());
        assert_eq!(vec![4, 5], edge_indices);
        assert_eq!(
            2,
            graphrecord
                .edges_in_group(&Group::from("0"))
                .unwrap()
                .count()
        );
        assert_eq!(
            2,
            graphrecord
                .edges_in_group(&Group::from("1"))
                .unwrap()
                .count()
        );
    }

    #[test]
    fn test_invalid_add_edges_with_groups() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group(Group::from("0"), None::<Vec<NodeIndex>>, None)
            .unwrap();
        graphrecord
            .add_group(Group::from("1"), None::<Vec<NodeIndex>>, None)
            .unwrap();

        assert!(
            graphrecord
                .add_edges_with_groups(
                    vec![(NodeIndex::from("50"), NodeIndex::from("0"), HashMap::new())],
                    &[Group::from("0"), Group::from("1")],
                )
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );

        assert!(
            graphrecord
                .add_edges_with_groups(
                    vec![(NodeIndex::from("0"), NodeIndex::from("50"), HashMap::new())],
                    &[Group::from("0"), Group::from("1")],
                )
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );
    }

    #[test]
    fn test_add_nodes_dataframes_with_groups() {
        let mut graphrecord = GraphRecord::new();

        graphrecord
            .add_group(Group::from("0"), None::<Vec<NodeIndex>>, None)
            .unwrap();
        graphrecord
            .add_group(Group::from("1"), None::<Vec<NodeIndex>>, None)
            .unwrap();

        let nodes_dataframe = create_nodes_dataframe().unwrap();

        graphrecord
            .add_nodes_dataframes_with_groups(
                vec![NodeDataFrameInput {
                    dataframe: nodes_dataframe,
                    index_column: "index".to_string(),
                }],
                &[Group::from("0"), Group::from("1")],
            )
            .unwrap();

        assert_eq!(2, graphrecord.node_count());
        assert_eq!(
            2,
            graphrecord
                .nodes_in_group(&Group::from("0"))
                .unwrap()
                .count()
        );
        assert_eq!(
            2,
            graphrecord
                .nodes_in_group(&Group::from("1"))
                .unwrap()
                .count()
        );
    }

    #[test]
    fn test_add_edges_dataframes_with_groups() {
        let mut graphrecord = GraphRecord::new();

        let nodes = create_nodes();

        graphrecord.add_nodes(nodes).unwrap();

        graphrecord
            .add_group(Group::from("0"), None::<Vec<NodeIndex>>, None)
            .unwrap();
        graphrecord
            .add_group(Group::from("1"), None::<Vec<NodeIndex>>, None)
            .unwrap();

        let edges_dataframe = create_edges_dataframe().unwrap();

        let edge_indices = graphrecord
            .add_edges_dataframes_with_groups(
                vec![EdgeDataFrameInput {
                    dataframe: edges_dataframe,
                    source_index_column: "from".to_string(),
                    target_index_column: "to".to_string(),
                }],
                &[Group::from("0"), Group::from("1")],
            )
            .unwrap();

        assert_eq!(2, graphrecord.edge_count());
        assert_eq!(2, edge_indices.len());
        assert_eq!(
            2,
            graphrecord
                .edges_in_group(&Group::from("0"))
                .unwrap()
                .count()
        );
        assert_eq!(
            2,
            graphrecord
                .edges_in_group(&Group::from("1"))
                .unwrap()
                .count()
        );
    }

    #[test]
    fn test_groups() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group(Group::from("0"), None::<Vec<NodeIndex>>, None)
            .unwrap();

        let groups: Vec<_> = graphrecord.groups().collect();

        assert_eq!(vec![&(GraphRecordAttribute::from("0"))], groups);
    }

    #[test]
    fn test_nodes_in_group() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group(Group::from("0"), None::<Vec<NodeIndex>>, None)
            .unwrap();

        assert_eq!(
            0,
            graphrecord
                .nodes_in_group(&Group::from("0"))
                .unwrap()
                .count()
        );

        graphrecord
            .add_group(Group::from("1"), Some(vec![NodeIndex::from("0")]), None)
            .unwrap();

        assert_eq!(
            1,
            graphrecord
                .nodes_in_group(&Group::from("1"))
                .unwrap()
                .count()
        );
    }

    #[test]
    fn test_invalid_nodes_in_group() {
        let graphrecord = create_graphrecord();

        // Querying a non-existing group should fail
        assert!(
            graphrecord
                .nodes_in_group(&Group::from("0"))
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );
    }

    #[test]
    fn test_edges_in_group() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group(Group::from("0"), None::<Vec<NodeIndex>>, None)
            .unwrap();

        assert_eq!(
            0,
            graphrecord
                .edges_in_group(&Group::from("0"))
                .unwrap()
                .count()
        );

        graphrecord
            .add_group(Group::from("1"), None::<Vec<NodeIndex>>, Some(vec![0]))
            .unwrap();

        assert_eq!(
            1,
            graphrecord
                .edges_in_group(&Group::from("1"))
                .unwrap()
                .count()
        );
    }

    #[test]
    fn test_invalid_edges_in_group() {
        let graphrecord = create_graphrecord();

        // Querying a non-existing group should fail
        assert!(
            graphrecord
                .edges_in_group(&Group::from("0"))
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );
    }

    #[test]
    fn test_groups_of_node() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group(Group::from("0"), Some(vec![NodeIndex::from("0")]), None)
            .unwrap();

        assert_eq!(
            1,
            graphrecord
                .groups_of_node(&NodeIndex::from("0"))
                .unwrap()
                .count()
        );
    }

    #[test]
    fn test_invalid_groups_of_node() {
        let graphrecord = create_graphrecord();

        // Queyring the groups of a non-existing node should fail
        assert!(
            graphrecord
                .groups_of_node(&NodeIndex::from("50"))
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );
    }

    #[test]
    fn test_groups_of_edge() {
        let mut graphrecord = create_graphrecord();

        graphrecord
            .add_group(Group::from("0"), None::<Vec<NodeIndex>>, Some(vec![0]))
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
            .add_edge(&NodeIndex::from("0"), &NodeIndex::from("1"), HashMap::new())
            .unwrap();

        assert_eq!(1, graphrecord.edge_count());
    }

    #[test]
    fn test_group_count() {
        let mut graphrecord = create_graphrecord();

        assert_eq!(0, graphrecord.group_count());

        graphrecord
            .add_group(Group::from("0"), None::<Vec<NodeIndex>>, None)
            .unwrap();

        assert_eq!(1, graphrecord.group_count());
    }

    #[test]
    fn test_contains_node() {
        let graphrecord = create_graphrecord();

        let existing: NodeIndex = "0".into();
        assert!(graphrecord.contains_node(&existing));

        let missing: NodeIndex = "50".into();
        assert!(!graphrecord.contains_node(&missing));
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

        let group: Group = "0".into();
        assert!(!graphrecord.contains_group(&group));

        graphrecord
            .add_group(Group::from("0"), None::<Vec<NodeIndex>>, None)
            .unwrap();

        assert!(graphrecord.contains_group(&group));
    }

    #[test]
    fn test_outgoing_neighbors() {
        let graphrecord = create_graphrecord();

        let node_index: NodeIndex = "0".into();
        let neighbors = graphrecord.outgoing_neighbors(&node_index).unwrap();

        assert_eq!(2, neighbors.count());
    }

    #[test]
    fn test_invalid_outgoing_neighbors() {
        let graphrecord = GraphRecord::new();

        let node_index: NodeIndex = "0".into();

        assert!(
            graphrecord
                .outgoing_neighbors(&node_index)
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );
    }

    #[test]
    fn test_neighbors() {
        let graphrecord = create_graphrecord();

        let node_index: NodeIndex = "2".into();

        let neighbors = graphrecord.outgoing_neighbors(&node_index).unwrap();
        assert_eq!(0, neighbors.count());

        let neighbors = graphrecord.neighbors(&node_index).unwrap();
        assert_eq!(2, neighbors.count());
    }

    #[test]
    fn test_invalid_neighbors() {
        let graphrecord = create_graphrecord();

        let node_index: NodeIndex = "50".into();

        assert!(
            graphrecord
                .neighbors(&node_index)
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );
    }

    #[test]
    fn test_clear() {
        let mut graphrecord = create_graphrecord();

        graphrecord.clear().unwrap();

        assert_eq!(0, graphrecord.node_count());
        assert_eq!(0, graphrecord.edge_count());
        assert_eq!(0, graphrecord.group_count());
    }
}
