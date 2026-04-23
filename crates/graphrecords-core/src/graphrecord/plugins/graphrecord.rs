use graphrecords_utils::aliases::GrHashMap;

use super::{
    Plugin, PostAddEdgeContext, PostAddEdgeToGroupContext, PostAddEdgeToGroupsContext,
    PostAddEdgeWithGroupContext, PostAddEdgeWithGroupsContext, PostAddEdgesContext,
    PostAddEdgesDataframesContext, PostAddEdgesDataframesWithGroupContext,
    PostAddEdgesDataframesWithGroupsContext, PostAddEdgesToGroupsContext,
    PostAddEdgesWithGroupContext, PostAddEdgesWithGroupsContext, PostAddGroupContext,
    PostAddNodeContext, PostAddNodeToGroupContext, PostAddNodeToGroupsContext,
    PostAddNodeWithGroupContext, PostAddNodeWithGroupsContext, PostAddNodesContext,
    PostAddNodesDataframesContext, PostAddNodesDataframesWithGroupContext,
    PostAddNodesDataframesWithGroupsContext, PostAddNodesToGroupsContext,
    PostAddNodesWithGroupContext, PostAddNodesWithGroupsContext, PostRemoveEdgeContext,
    PostRemoveEdgeFromGroupContext, PostRemoveEdgeFromGroupsContext,
    PostRemoveEdgesFromGroupsContext, PostRemoveGroupContext, PostRemoveNodeContext,
    PostRemoveNodeFromGroupContext, PostRemoveNodeFromGroupsContext,
    PostRemoveNodesFromGroupsContext, PreAddEdgeContext, PreAddEdgeToGroupContext,
    PreAddEdgeToGroupsContext, PreAddEdgeWithGroupContext, PreAddEdgeWithGroupsContext,
    PreAddEdgesContext, PreAddEdgesDataframesContext, PreAddEdgesDataframesWithGroupContext,
    PreAddEdgesDataframesWithGroupsContext, PreAddEdgesToGroupsContext,
    PreAddEdgesWithGroupContext, PreAddEdgesWithGroupsContext, PreAddGroupContext,
    PreAddNodeContext, PreAddNodeToGroupContext, PreAddNodeToGroupsContext,
    PreAddNodeWithGroupContext, PreAddNodeWithGroupsContext, PreAddNodesContext,
    PreAddNodesDataframesContext, PreAddNodesDataframesWithGroupContext,
    PreAddNodesDataframesWithGroupsContext, PreAddNodesToGroupsContext,
    PreAddNodesWithGroupContext, PreAddNodesWithGroupsContext, PreRemoveEdgeContext,
    PreRemoveEdgeFromGroupContext, PreRemoveEdgeFromGroupsContext, PreRemoveEdgesFromGroupsContext,
    PreRemoveGroupContext, PreRemoveNodeContext, PreRemoveNodeFromGroupContext,
    PreRemoveNodeFromGroupsContext, PreRemoveNodesFromGroupsContext, PreSetSchemaContext,
};
use crate::{
    errors::{GraphRecordError, GraphRecordResult},
    graphrecord::{
        AsLookup, EdgeDataFrameInput, GraphRecord, GroupHandle, GroupKind, NodeDataFrameInput,
        NodeHandle, NodeIndexKind,
        lookup::{resolve_all, resolve_edge_handles},
    },
    prelude::{Attributes, EdgeIndex, GraphRecordAttribute, Group, NodeIndex, Schema},
};
use std::sync::Arc;

pub type PluginName = GraphRecordAttribute;

impl GraphRecord {
    pub fn with_plugins(
        plugins: GrHashMap<PluginName, Box<dyn Plugin>>,
    ) -> GraphRecordResult<Self> {
        let mut graphrecord = Self {
            plugins: Arc::new(plugins),
            ..Default::default()
        };

        let plugins = graphrecord.plugins.clone();

        plugins
            .iter()
            .try_for_each(|(_, plugin)| plugin.initialize(&mut graphrecord))?;

        Ok(graphrecord)
    }

    pub fn add_plugin(
        &mut self,
        name: PluginName,
        plugin: Box<dyn Plugin>,
    ) -> GraphRecordResult<()> {
        if self.plugins.contains_key(&name) {
            return Err(GraphRecordError::KeyError(format!(
                "Plugin with name '{name}' already exists"
            )));
        }

        plugin.initialize(self)?;

        let plugins = Arc::make_mut(&mut self.plugins);

        plugins.insert(name, plugin);

        Ok(())
    }

    pub fn remove_plugin(&mut self, name: &PluginName) -> GraphRecordResult<()> {
        let plugin = {
            let plugins = Arc::make_mut(&mut self.plugins);

            plugins.remove(name).ok_or_else(|| {
                GraphRecordError::KeyError(format!("Plugin with name '{name}' does not exist"))
            })?
        };

        plugin.finalize(self)?;

        Ok(())
    }

    pub fn plugin_names(&self) -> impl Iterator<Item = &PluginName> {
        self.plugins.keys()
    }

    pub fn set_schema(&mut self, schema: Schema) -> GraphRecordResult<()> {
        let plugins = self.plugins.clone();

        let pre_context = PreSetSchemaContext { schema };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_set_schema(self, pre_context)
            })?;

        self.set_schema_impl(pre_context.schema)?;

        plugins
            .iter()
            .try_for_each(|(_, plugin)| plugin.post_set_schema(self))?;

        Ok(())
    }

    pub fn set_schema_bypass_plugins(&mut self, schema: Schema) -> GraphRecordResult<()> {
        self.set_schema_impl(schema)
    }

    pub fn freeze_schema(&mut self) -> GraphRecordResult<()> {
        let plugins = self.plugins.clone();
        for (_, plugin) in plugins.iter() {
            plugin.pre_freeze_schema(self)?;
        }

        self.freeze_schema_impl();

        for (_, plugin) in plugins.iter() {
            plugin.post_freeze_schema(self)?;
        }

        Ok(())
    }

    pub const fn freeze_schema_bypass_plugins(&mut self) -> GraphRecordResult<()> {
        self.freeze_schema_impl();

        Ok(())
    }

    pub fn unfreeze_schema(&mut self) -> GraphRecordResult<()> {
        let plugins = self.plugins.clone();
        for (_, plugin) in plugins.iter() {
            plugin.pre_unfreeze_schema(self)?;
        }

        self.unfreeze_schema_impl();

        for (_, plugin) in plugins.iter() {
            plugin.post_unfreeze_schema(self)?;
        }

        Ok(())
    }

    pub const fn unfreeze_schema_bypass_plugins(&mut self) -> GraphRecordResult<()> {
        self.unfreeze_schema_impl();

        Ok(())
    }

    pub fn add_node(
        &mut self,
        node_index: NodeIndex,
        attributes: Attributes,
    ) -> GraphRecordResult<NodeHandle> {
        let plugins = self.plugins.clone();

        let pre_context = PreAddNodeContext {
            node_index: node_index.clone(),
            attributes,
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_add_node(self, pre_context)
            })?;

        let node_handle = self.add_node_impl(pre_context.node_index, pre_context.attributes)?;

        let post_context = PostAddNodeContext { node_index };

        plugins
            .iter()
            .try_for_each(|(_, plugin)| plugin.post_add_node(self, post_context.clone()))?;

        Ok(node_handle)
    }

    pub fn add_node_bypass_plugins(
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
        let plugins = self.plugins.clone();

        let pre_context = PreAddNodeWithGroupContext {
            node_index,
            attributes,
            group_handle: group.resolve(self)?,
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_add_node_with_group(self, pre_context)
            })?;

        let node_index = pre_context.node_index.clone();
        let group_handle = pre_context.group_handle;

        let node_handle = self.add_node_with_group_impl(
            pre_context.node_index,
            pre_context.attributes,
            pre_context.group_handle,
        )?;

        let post_context = PostAddNodeWithGroupContext {
            node_index,
            group_handle,
        };

        plugins.iter().try_for_each(|(_, plugin)| {
            plugin.post_add_node_with_group(self, post_context.clone())
        })?;

        Ok(node_handle)
    }

    pub fn add_node_with_group_bypass_plugins(
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
        let plugins = self.plugins.clone();

        let pre_context = PreAddNodeWithGroupsContext {
            node_index,
            attributes,
            group_handles: resolve_all(self, groups)?,
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_add_node_with_groups(self, pre_context)
            })?;

        let node_handle = self.add_node_with_groups_impl(
            pre_context.node_index.clone(),
            pre_context.attributes,
            &pre_context.group_handles,
        )?;

        let post_context = PostAddNodeWithGroupsContext {
            node_index: pre_context.node_index,
            group_handles: pre_context.group_handles,
        };

        plugins.iter().try_for_each(|(_, plugin)| {
            plugin.post_add_node_with_groups(self, post_context.clone())
        })?;

        Ok(node_handle)
    }

    pub fn add_node_with_groups_bypass_plugins(
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
        let plugins = self.plugins.clone();

        let pre_context = PreRemoveNodeContext {
            node_handle: node.resolve(self)?,
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_remove_node(self, pre_context)
            })?;

        let attributes = self.remove_node_impl(pre_context.node_handle)?;

        let post_context = PostRemoveNodeContext {
            node_handle: pre_context.node_handle,
        };

        plugins
            .iter()
            .try_for_each(|(_, plugin)| plugin.post_remove_node(self, post_context.clone()))?;

        Ok(attributes)
    }

    pub fn remove_node_bypass_plugins(
        &mut self,
        node: impl AsLookup<NodeIndexKind>,
    ) -> GraphRecordResult<Attributes> {
        self.remove_node_impl(node.resolve(self)?)
    }

    pub fn add_nodes(
        &mut self,
        nodes: Vec<(NodeIndex, Attributes)>,
    ) -> GraphRecordResult<Vec<NodeHandle>> {
        let plugins = self.plugins.clone();

        let pre_context = PreAddNodesContext { nodes };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_add_nodes(self, pre_context)
            })?;

        let node_handles = self.add_nodes_impl(pre_context.nodes.clone())?;

        let post_context = PostAddNodesContext {
            nodes: pre_context.nodes,
        };

        plugins
            .iter()
            .try_for_each(|(_, plugin)| plugin.post_add_nodes(self, post_context.clone()))?;

        Ok(node_handles)
    }

    pub fn add_nodes_bypass_plugins(
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
        let plugins = self.plugins.clone();

        let pre_context = PreAddNodesWithGroupContext {
            nodes,
            group_handle: group.resolve(self)?,
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_add_nodes_with_group(self, pre_context)
            })?;

        let node_handles =
            self.add_nodes_with_group_impl(pre_context.nodes.clone(), pre_context.group_handle)?;

        let post_context = PostAddNodesWithGroupContext {
            nodes: pre_context.nodes,
            group_handle: pre_context.group_handle,
        };

        plugins.iter().try_for_each(|(_, plugin)| {
            plugin.post_add_nodes_with_group(self, post_context.clone())
        })?;

        Ok(node_handles)
    }

    pub fn add_nodes_with_group_bypass_plugins(
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
        let plugins = self.plugins.clone();

        let pre_context = PreAddNodesWithGroupsContext {
            nodes,
            group_handles: resolve_all(self, groups)?,
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_add_nodes_with_groups(self, pre_context)
            })?;

        let node_handles =
            self.add_nodes_with_groups_impl(pre_context.nodes.clone(), &pre_context.group_handles)?;

        let post_context = PostAddNodesWithGroupsContext {
            nodes: pre_context.nodes,
            group_handles: pre_context.group_handles,
        };

        plugins.iter().try_for_each(|(_, plugin)| {
            plugin.post_add_nodes_with_groups(self, post_context.clone())
        })?;

        Ok(node_handles)
    }

    pub fn add_nodes_with_groups_bypass_plugins(
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
        let plugins = self.plugins.clone();

        let pre_context = PreAddNodesDataframesContext {
            nodes_dataframes: nodes_dataframes.into_iter().map(Into::into).collect(),
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_add_nodes_dataframes(self, pre_context)
            })?;

        let node_handles = self.add_nodes_dataframes_impl(pre_context.nodes_dataframes.clone())?;

        let post_context = PostAddNodesDataframesContext {
            nodes_dataframes: pre_context.nodes_dataframes,
        };

        plugins.iter().try_for_each(|(_, plugin)| {
            plugin.post_add_nodes_dataframes(self, post_context.clone())
        })?;

        Ok(node_handles)
    }

    pub fn add_nodes_dataframes_bypass_plugins(
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
        let plugins = self.plugins.clone();

        let pre_context = PreAddNodesDataframesWithGroupContext {
            nodes_dataframes: nodes_dataframes.into_iter().map(Into::into).collect(),
            group_handle: group.resolve(self)?,
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_add_nodes_dataframes_with_group(self, pre_context)
            })?;

        let node_handles = self.add_nodes_dataframes_with_group_impl(
            pre_context.nodes_dataframes.clone(),
            pre_context.group_handle,
        )?;

        let post_context = PostAddNodesDataframesWithGroupContext {
            nodes_dataframes: pre_context.nodes_dataframes,
            group_handle: pre_context.group_handle,
        };

        plugins.iter().try_for_each(|(_, plugin)| {
            plugin.post_add_nodes_dataframes_with_group(self, post_context.clone())
        })?;

        Ok(node_handles)
    }

    pub fn add_nodes_dataframes_with_group_bypass_plugins(
        &mut self,
        nodes_dataframes: impl IntoIterator<Item = impl Into<NodeDataFrameInput>>,
        group: impl AsLookup<GroupKind>,
    ) -> GraphRecordResult<Vec<NodeHandle>> {
        self.add_nodes_dataframes_with_group_impl(nodes_dataframes, group.resolve(self)?)
    }

    pub fn add_nodes_dataframes_with_groups(
        &mut self,
        nodes_dataframes: Vec<NodeDataFrameInput>,
        groups: impl IntoIterator<Item = impl AsLookup<GroupKind>>,
    ) -> GraphRecordResult<Vec<NodeHandle>> {
        let plugins = self.plugins.clone();

        let pre_context = PreAddNodesDataframesWithGroupsContext {
            nodes_dataframes,
            group_handles: resolve_all(self, groups)?,
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_add_nodes_dataframes_with_groups(self, pre_context)
            })?;

        let node_handles = self.add_nodes_dataframes_with_groups_impl(
            pre_context.nodes_dataframes.clone(),
            &pre_context.group_handles,
        )?;

        let post_context = PostAddNodesDataframesWithGroupsContext {
            nodes_dataframes: pre_context.nodes_dataframes,
            group_handles: pre_context.group_handles,
        };

        plugins.iter().try_for_each(|(_, plugin)| {
            plugin.post_add_nodes_dataframes_with_groups(self, post_context.clone())
        })?;

        Ok(node_handles)
    }

    pub fn add_nodes_dataframes_with_groups_bypass_plugins(
        &mut self,
        nodes_dataframes: Vec<NodeDataFrameInput>,
        groups: impl IntoIterator<Item = impl AsLookup<GroupKind>>,
    ) -> GraphRecordResult<Vec<NodeHandle>> {
        self.add_nodes_dataframes_with_groups_impl(nodes_dataframes, &resolve_all(self, groups)?)
    }

    pub fn add_edge<S, T>(
        &mut self,
        source: S,
        target: T,
        attributes: Attributes,
    ) -> GraphRecordResult<EdgeIndex>
    where
        S: AsLookup<NodeIndexKind>,
        T: AsLookup<NodeIndexKind>,
    {
        let plugins = self.plugins.clone();

        let pre_context = PreAddEdgeContext {
            source_node_handle: source.resolve(self)?,
            target_node_handle: target.resolve(self)?,
            attributes,
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_add_edge(self, pre_context)
            })?;

        let edge_index = self.add_edge_impl(
            pre_context.source_node_handle,
            pre_context.target_node_handle,
            pre_context.attributes,
        )?;

        let post_context = PostAddEdgeContext { edge_index };

        plugins
            .iter()
            .try_for_each(|(_, plugin)| plugin.post_add_edge(self, post_context.clone()))?;

        Ok(edge_index)
    }

    pub fn add_edge_bypass_plugins<S, T>(
        &mut self,
        source: S,
        target: T,
        attributes: Attributes,
    ) -> GraphRecordResult<EdgeIndex>
    where
        S: AsLookup<NodeIndexKind>,
        T: AsLookup<NodeIndexKind>,
    {
        self.add_edge_impl(source.resolve(self)?, target.resolve(self)?, attributes)
    }

    pub fn add_edge_with_group<S, T>(
        &mut self,
        source: S,
        target: T,
        attributes: Attributes,
        group: impl AsLookup<GroupKind>,
    ) -> GraphRecordResult<EdgeIndex>
    where
        S: AsLookup<NodeIndexKind>,
        T: AsLookup<NodeIndexKind>,
    {
        let plugins = self.plugins.clone();

        let pre_context = PreAddEdgeWithGroupContext {
            source_node_handle: source.resolve(self)?,
            target_node_handle: target.resolve(self)?,
            attributes,
            group_handle: group.resolve(self)?,
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_add_edge_with_group(self, pre_context)
            })?;

        let edge_index = self.add_edge_with_group_impl(
            pre_context.source_node_handle,
            pre_context.target_node_handle,
            pre_context.attributes,
            pre_context.group_handle,
        )?;

        let post_context = PostAddEdgeWithGroupContext {
            edge_index,
            group_handle: pre_context.group_handle,
        };

        plugins.iter().try_for_each(|(_, plugin)| {
            plugin.post_add_edge_with_group(self, post_context.clone())
        })?;

        Ok(edge_index)
    }

    pub fn add_edge_with_group_bypass_plugins<S, T>(
        &mut self,
        source: S,
        target: T,
        attributes: Attributes,
        group: impl AsLookup<GroupKind>,
    ) -> GraphRecordResult<EdgeIndex>
    where
        S: AsLookup<NodeIndexKind>,
        T: AsLookup<NodeIndexKind>,
    {
        self.add_edge_with_group_impl(
            source.resolve(self)?,
            target.resolve(self)?,
            attributes,
            group.resolve(self)?,
        )
    }

    pub fn add_edge_with_groups<S, T>(
        &mut self,
        source: S,
        target: T,
        attributes: Attributes,
        groups: impl IntoIterator<Item = impl AsLookup<GroupKind>>,
    ) -> GraphRecordResult<EdgeIndex>
    where
        S: AsLookup<NodeIndexKind>,
        T: AsLookup<NodeIndexKind>,
    {
        let plugins = self.plugins.clone();

        let pre_context = PreAddEdgeWithGroupsContext {
            source_node_handle: source.resolve(self)?,
            target_node_handle: target.resolve(self)?,
            attributes,
            group_handles: resolve_all(self, groups)?,
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_add_edge_with_groups(self, pre_context)
            })?;

        let edge_index = self.add_edge_with_groups_impl(
            pre_context.source_node_handle,
            pre_context.target_node_handle,
            pre_context.attributes,
            &pre_context.group_handles,
        )?;

        let post_context = PostAddEdgeWithGroupsContext {
            edge_index,
            group_handles: pre_context.group_handles,
        };

        plugins.iter().try_for_each(|(_, plugin)| {
            plugin.post_add_edge_with_groups(self, post_context.clone())
        })?;

        Ok(edge_index)
    }

    pub fn add_edge_with_groups_bypass_plugins<S, T>(
        &mut self,
        source: S,
        target: T,
        attributes: Attributes,
        groups: impl IntoIterator<Item = impl AsLookup<GroupKind>>,
    ) -> GraphRecordResult<EdgeIndex>
    where
        S: AsLookup<NodeIndexKind>,
        T: AsLookup<NodeIndexKind>,
    {
        self.add_edge_with_groups_impl(
            source.resolve(self)?,
            target.resolve(self)?,
            attributes,
            &resolve_all(self, groups)?,
        )
    }

    pub fn remove_edge(&mut self, edge_index: &EdgeIndex) -> GraphRecordResult<Attributes> {
        let plugins = self.plugins.clone();

        let pre_context = PreRemoveEdgeContext {
            edge_index: *edge_index,
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_remove_edge(self, pre_context)
            })?;

        let attributes = self.remove_edge_impl(&pre_context.edge_index)?;

        let post_context = PostRemoveEdgeContext {
            edge_index: pre_context.edge_index,
        };

        plugins
            .iter()
            .try_for_each(|(_, plugin)| plugin.post_remove_edge(self, post_context.clone()))?;

        Ok(attributes)
    }

    pub fn remove_edge_bypass_plugins(
        &mut self,
        edge_index: &EdgeIndex,
    ) -> GraphRecordResult<Attributes> {
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
        let edges = resolve_edge_handles(self, edges)?;

        let plugins = self.plugins.clone();

        let pre_context = PreAddEdgesContext { edges };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_add_edges(self, pre_context)
            })?;

        let edge_indices = self.add_edges_impl(pre_context.edges)?;

        let post_context = PostAddEdgesContext {
            edge_indices: edge_indices.clone(),
        };

        plugins
            .iter()
            .try_for_each(|(_, plugin)| plugin.post_add_edges(self, post_context.clone()))?;

        Ok(edge_indices)
    }

    pub fn add_edges_bypass_plugins<S, T>(
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
        let edges = resolve_edge_handles(self, edges)?;

        let plugins = self.plugins.clone();

        let pre_context = PreAddEdgesWithGroupContext {
            edges,
            group_handle: group.resolve(self)?,
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_add_edges_with_group(self, pre_context)
            })?;

        let group_handle = pre_context.group_handle;
        let edge_indices = self.add_edges_with_group_impl(pre_context.edges, group_handle)?;

        let post_context = PostAddEdgesWithGroupContext {
            edge_indices: edge_indices.clone(),
            group_handle,
        };

        plugins.iter().try_for_each(|(_, plugin)| {
            plugin.post_add_edges_with_group(self, post_context.clone())
        })?;

        Ok(edge_indices)
    }

    pub fn add_edges_with_group_bypass_plugins<S, T>(
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
        let edges = resolve_edge_handles(self, edges)?;

        let plugins = self.plugins.clone();

        let pre_context = PreAddEdgesWithGroupsContext {
            edges,
            group_handles: resolve_all(self, groups)?,
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_add_edges_with_groups(self, pre_context)
            })?;

        let edge_indices =
            self.add_edges_with_groups_impl(pre_context.edges.clone(), &pre_context.group_handles)?;

        let post_context = PostAddEdgesWithGroupsContext {
            edge_indices: edge_indices.clone(),
            group_handles: pre_context.group_handles,
        };

        plugins.iter().try_for_each(|(_, plugin)| {
            plugin.post_add_edges_with_groups(self, post_context.clone())
        })?;

        Ok(edge_indices)
    }

    pub fn add_edges_with_groups_bypass_plugins<S, T>(
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
        let plugins = self.plugins.clone();

        let pre_context = PreAddEdgesDataframesContext {
            edges_dataframes: edges_dataframes.into_iter().map(Into::into).collect(),
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_add_edges_dataframes(self, pre_context)
            })?;

        let edge_indices = self.add_edges_dataframes_impl(pre_context.edges_dataframes.clone())?;

        let post_context = PostAddEdgesDataframesContext {
            edges_dataframes: pre_context.edges_dataframes,
        };

        plugins.iter().try_for_each(|(_, plugin)| {
            plugin.post_add_edges_dataframes(self, post_context.clone())
        })?;

        Ok(edge_indices)
    }

    pub fn add_edges_dataframes_bypass_plugins(
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
        let plugins = self.plugins.clone();

        let pre_context = PreAddEdgesDataframesWithGroupContext {
            edges_dataframes: edges_dataframes.into_iter().map(Into::into).collect(),
            group_handle: group.resolve(self)?,
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_add_edges_dataframes_with_group(self, pre_context)
            })?;

        let edge_indices = self.add_edges_dataframes_with_group_impl(
            pre_context.edges_dataframes.clone(),
            pre_context.group_handle,
        )?;

        let post_context = PostAddEdgesDataframesWithGroupContext {
            edges_dataframes: pre_context.edges_dataframes,
            group_handle: pre_context.group_handle,
        };

        plugins.iter().try_for_each(|(_, plugin)| {
            plugin.post_add_edges_dataframes_with_group(self, post_context.clone())
        })?;

        Ok(edge_indices)
    }

    pub fn add_edges_dataframes_with_group_bypass_plugins(
        &mut self,
        edges_dataframes: impl IntoIterator<Item = impl Into<EdgeDataFrameInput>>,
        group: impl AsLookup<GroupKind>,
    ) -> GraphRecordResult<Vec<EdgeIndex>> {
        self.add_edges_dataframes_with_group_impl(edges_dataframes, group.resolve(self)?)
    }

    pub fn add_edges_dataframes_with_groups(
        &mut self,
        edges_dataframes: Vec<EdgeDataFrameInput>,
        groups: impl IntoIterator<Item = impl AsLookup<GroupKind>>,
    ) -> GraphRecordResult<Vec<EdgeIndex>> {
        let plugins = self.plugins.clone();

        let pre_context = PreAddEdgesDataframesWithGroupsContext {
            edges_dataframes,
            group_handles: resolve_all(self, groups)?,
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_add_edges_dataframes_with_groups(self, pre_context)
            })?;

        let edge_indices = self.add_edges_dataframes_with_groups_impl(
            pre_context.edges_dataframes.clone(),
            &pre_context.group_handles,
        )?;

        let post_context = PostAddEdgesDataframesWithGroupsContext {
            edges_dataframes: pre_context.edges_dataframes,
            group_handles: pre_context.group_handles,
        };

        plugins.iter().try_for_each(|(_, plugin)| {
            plugin.post_add_edges_dataframes_with_groups(self, post_context.clone())
        })?;

        Ok(edge_indices)
    }

    pub fn add_edges_dataframes_with_groups_bypass_plugins(
        &mut self,
        edges_dataframes: Vec<EdgeDataFrameInput>,
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
        let plugins = self.plugins.clone();

        let node_handles = match nodes {
            None => None,
            Some(nodes) => Some(resolve_all(self, nodes)?),
        };

        let pre_context = PreAddGroupContext {
            group,
            node_handles,
            edge_indices,
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_add_group(self, pre_context)
            })?;

        self.add_group_impl(
            pre_context.group.clone(),
            pre_context.node_handles.clone(),
            pre_context.edge_indices.clone(),
        )?;

        let post_context = PostAddGroupContext {
            group: pre_context.group,
            node_handles: pre_context.node_handles,
            edge_indices: pre_context.edge_indices,
        };

        plugins
            .iter()
            .try_for_each(|(_, plugin)| plugin.post_add_group(self, post_context.clone()))?;

        Ok(())
    }

    pub fn add_group_bypass_plugins(
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
        let plugins = self.plugins.clone();

        let pre_context = PreRemoveGroupContext {
            group_handle: group.resolve(self)?,
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_remove_group(self, pre_context)
            })?;

        let group_handle = pre_context.group_handle;

        self.remove_group_impl(group_handle)?;

        let post_context = PostRemoveGroupContext { group_handle };

        plugins
            .iter()
            .try_for_each(|(_, plugin)| plugin.post_remove_group(self, post_context.clone()))?;

        Ok(())
    }

    pub fn remove_group_bypass_plugins(
        &mut self,
        group: impl AsLookup<GroupKind>,
    ) -> GraphRecordResult<()> {
        self.remove_group_impl(group.resolve(self)?)
    }

    pub fn add_node_to_group(
        &mut self,
        group: impl AsLookup<GroupKind>,
        node: impl AsLookup<NodeIndexKind>,
    ) -> GraphRecordResult<()> {
        let plugins = self.plugins.clone();

        let group_handle = group.resolve(self)?;
        let node_handle = node.resolve(self)?;

        let pre_context = PreAddNodeToGroupContext {
            group_handle,
            node_handle,
        };

        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_add_node_to_group(self, pre_context)
            })?;

        self.add_node_to_group_impl(pre_context.group_handle, pre_context.node_handle)?;

        let post_context = PostAddNodeToGroupContext {
            group_handle: pre_context.group_handle,
            node_handle: pre_context.node_handle,
        };

        plugins.iter().try_for_each(|(_, plugin)| {
            plugin.post_add_node_to_group(self, post_context.clone())
        })?;

        Ok(())
    }

    pub fn add_node_to_group_bypass_plugins(
        &mut self,
        group: impl AsLookup<GroupKind>,
        node: impl AsLookup<NodeIndexKind>,
    ) -> GraphRecordResult<()> {
        let group_handle = group.resolve(self)?;
        let node_handle = node.resolve(self)?;
        self.add_node_to_group_impl(group_handle, node_handle)
    }

    pub fn add_node_to_groups(
        &mut self,
        groups: impl IntoIterator<Item = impl AsLookup<GroupKind>>,
        node: impl AsLookup<NodeIndexKind>,
    ) -> GraphRecordResult<()> {
        let plugins = self.plugins.clone();

        let group_handles: Vec<GroupHandle> = groups
            .into_iter()
            .map(|group| group.resolve(self))
            .collect::<GraphRecordResult<_>>()?;
        let node_handle = node.resolve(self)?;

        let pre_context = PreAddNodeToGroupsContext {
            group_handles,
            node_handle,
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_add_node_to_groups(self, pre_context)
            })?;

        self.add_node_to_groups_impl(&pre_context.group_handles, pre_context.node_handle)?;

        let post_context = PostAddNodeToGroupsContext {
            group_handles: pre_context.group_handles,
            node_handle: pre_context.node_handle,
        };

        plugins.iter().try_for_each(|(_, plugin)| {
            plugin.post_add_node_to_groups(self, post_context.clone())
        })?;

        Ok(())
    }

    pub fn add_node_to_groups_bypass_plugins(
        &mut self,
        groups: impl IntoIterator<Item = impl AsLookup<GroupKind>>,
        node: impl AsLookup<NodeIndexKind>,
    ) -> GraphRecordResult<()> {
        let group_handles: Vec<GroupHandle> = groups
            .into_iter()
            .map(|group| group.resolve(self))
            .collect::<GraphRecordResult<_>>()?;
        let node_handle = node.resolve(self)?;
        self.add_node_to_groups_impl(&group_handles, node_handle)
    }

    pub fn add_nodes_to_groups(
        &mut self,
        groups: impl IntoIterator<Item = impl AsLookup<GroupKind>>,
        nodes: impl IntoIterator<Item = impl AsLookup<NodeIndexKind>>,
    ) -> GraphRecordResult<()> {
        let plugins = self.plugins.clone();

        let group_handles: Vec<GroupHandle> = groups
            .into_iter()
            .map(|group| group.resolve(self))
            .collect::<GraphRecordResult<_>>()?;
        let node_handles: Vec<NodeHandle> = nodes
            .into_iter()
            .map(|node| node.resolve(self))
            .collect::<GraphRecordResult<_>>()?;

        let pre_context = PreAddNodesToGroupsContext {
            group_handles,
            node_handles,
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_add_nodes_to_groups(self, pre_context)
            })?;

        self.add_nodes_to_groups_impl(
            &pre_context.group_handles,
            pre_context.node_handles.clone(),
        )?;

        let post_context = PostAddNodesToGroupsContext {
            group_handles: pre_context.group_handles,
            node_handles: pre_context.node_handles,
        };

        plugins.iter().try_for_each(|(_, plugin)| {
            plugin.post_add_nodes_to_groups(self, post_context.clone())
        })?;

        Ok(())
    }

    pub fn add_nodes_to_groups_bypass_plugins(
        &mut self,
        groups: impl IntoIterator<Item = impl AsLookup<GroupKind>>,
        nodes: impl IntoIterator<Item = impl AsLookup<NodeIndexKind>>,
    ) -> GraphRecordResult<()> {
        let group_handles: Vec<GroupHandle> = groups
            .into_iter()
            .map(|group| group.resolve(self))
            .collect::<GraphRecordResult<_>>()?;
        let node_handles: Vec<NodeHandle> = nodes
            .into_iter()
            .map(|node| node.resolve(self))
            .collect::<GraphRecordResult<_>>()?;
        self.add_nodes_to_groups_impl(&group_handles, node_handles)
    }

    pub fn add_edge_to_group(
        &mut self,
        group: impl AsLookup<GroupKind>,
        edge_index: EdgeIndex,
    ) -> GraphRecordResult<()> {
        let plugins = self.plugins.clone();

        let group_handle = group.resolve(self)?;

        let pre_context = PreAddEdgeToGroupContext {
            group_handle,
            edge_index,
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_add_edge_to_group(self, pre_context)
            })?;

        self.add_edge_to_group_impl(pre_context.group_handle, pre_context.edge_index)?;

        let post_context = PostAddEdgeToGroupContext {
            group_handle: pre_context.group_handle,
            edge_index: pre_context.edge_index,
        };

        plugins.iter().try_for_each(|(_, plugin)| {
            plugin.post_add_edge_to_group(self, post_context.clone())
        })?;

        Ok(())
    }

    pub fn add_edge_to_group_bypass_plugins(
        &mut self,
        group: impl AsLookup<GroupKind>,
        edge_index: EdgeIndex,
    ) -> GraphRecordResult<()> {
        let group_handle = group.resolve(self)?;
        self.add_edge_to_group_impl(group_handle, edge_index)
    }

    pub fn add_edge_to_groups(
        &mut self,
        groups: impl IntoIterator<Item = impl AsLookup<GroupKind>>,
        edge_index: EdgeIndex,
    ) -> GraphRecordResult<()> {
        let plugins = self.plugins.clone();

        let group_handles: Vec<GroupHandle> = groups
            .into_iter()
            .map(|group| group.resolve(self))
            .collect::<GraphRecordResult<_>>()?;

        let pre_context = PreAddEdgeToGroupsContext {
            group_handles,
            edge_index,
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_add_edge_to_groups(self, pre_context)
            })?;

        self.add_edge_to_groups_impl(&pre_context.group_handles, pre_context.edge_index)?;

        let post_context = PostAddEdgeToGroupsContext {
            group_handles: pre_context.group_handles,
            edge_index: pre_context.edge_index,
        };

        plugins.iter().try_for_each(|(_, plugin)| {
            plugin.post_add_edge_to_groups(self, post_context.clone())
        })?;

        Ok(())
    }

    pub fn add_edge_to_groups_bypass_plugins(
        &mut self,
        groups: impl IntoIterator<Item = impl AsLookup<GroupKind>>,
        edge_index: EdgeIndex,
    ) -> GraphRecordResult<()> {
        let group_handles: Vec<GroupHandle> = groups
            .into_iter()
            .map(|group| group.resolve(self))
            .collect::<GraphRecordResult<_>>()?;
        self.add_edge_to_groups_impl(&group_handles, edge_index)
    }

    pub fn add_edges_to_groups(
        &mut self,
        groups: impl IntoIterator<Item = impl AsLookup<GroupKind>>,
        edge_indices: Vec<EdgeIndex>,
    ) -> GraphRecordResult<()> {
        let plugins = self.plugins.clone();

        let group_handles: Vec<GroupHandle> = groups
            .into_iter()
            .map(|group| group.resolve(self))
            .collect::<GraphRecordResult<_>>()?;

        let pre_context = PreAddEdgesToGroupsContext {
            group_handles,
            edge_indices,
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_add_edges_to_groups(self, pre_context)
            })?;

        self.add_edges_to_groups_impl(
            &pre_context.group_handles,
            pre_context.edge_indices.clone(),
        )?;

        let post_context = PostAddEdgesToGroupsContext {
            group_handles: pre_context.group_handles,
            edge_indices: pre_context.edge_indices,
        };

        plugins.iter().try_for_each(|(_, plugin)| {
            plugin.post_add_edges_to_groups(self, post_context.clone())
        })?;

        Ok(())
    }

    pub fn add_edges_to_groups_bypass_plugins(
        &mut self,
        groups: impl IntoIterator<Item = impl AsLookup<GroupKind>>,
        edge_indices: Vec<EdgeIndex>,
    ) -> GraphRecordResult<()> {
        let group_handles: Vec<GroupHandle> = groups
            .into_iter()
            .map(|group| group.resolve(self))
            .collect::<GraphRecordResult<_>>()?;
        self.add_edges_to_groups_impl(&group_handles, edge_indices)
    }

    pub fn remove_node_from_group(
        &mut self,
        group: impl AsLookup<GroupKind>,
        node: impl AsLookup<NodeIndexKind>,
    ) -> GraphRecordResult<()> {
        let plugins = self.plugins.clone();

        let group_handle = group.resolve(self)?;
        let node_handle = node.resolve(self)?;

        let pre_context = PreRemoveNodeFromGroupContext {
            group_handle,
            node_handle,
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_remove_node_from_group(self, pre_context)
            })?;

        self.remove_node_from_group_impl(pre_context.group_handle, pre_context.node_handle)?;

        let post_context = PostRemoveNodeFromGroupContext {
            group_handle: pre_context.group_handle,
            node_handle: pre_context.node_handle,
        };

        plugins.iter().try_for_each(|(_, plugin)| {
            plugin.post_remove_node_from_group(self, post_context.clone())
        })?;

        Ok(())
    }

    pub fn remove_node_from_group_bypass_plugins(
        &mut self,
        group: impl AsLookup<GroupKind>,
        node: impl AsLookup<NodeIndexKind>,
    ) -> GraphRecordResult<()> {
        let group_handle = group.resolve(self)?;
        let node_handle = node.resolve(self)?;
        self.remove_node_from_group_impl(group_handle, node_handle)
    }

    pub fn remove_node_from_groups(
        &mut self,
        groups: impl IntoIterator<Item = impl AsLookup<GroupKind>>,
        node: impl AsLookup<NodeIndexKind>,
    ) -> GraphRecordResult<()> {
        let plugins = self.plugins.clone();

        let group_handles: Vec<GroupHandle> = groups
            .into_iter()
            .map(|group| group.resolve(self))
            .collect::<GraphRecordResult<_>>()?;
        let node_handle = node.resolve(self)?;

        let pre_context = PreRemoveNodeFromGroupsContext {
            group_handles,
            node_handle,
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_remove_node_from_groups(self, pre_context)
            })?;

        self.remove_node_from_groups_impl(&pre_context.group_handles, pre_context.node_handle)?;

        let post_context = PostRemoveNodeFromGroupsContext {
            group_handles: pre_context.group_handles,
            node_handle: pre_context.node_handle,
        };

        plugins.iter().try_for_each(|(_, plugin)| {
            plugin.post_remove_node_from_groups(self, post_context.clone())
        })?;

        Ok(())
    }

    pub fn remove_node_from_groups_bypass_plugins(
        &mut self,
        groups: impl IntoIterator<Item = impl AsLookup<GroupKind>>,
        node: impl AsLookup<NodeIndexKind>,
    ) -> GraphRecordResult<()> {
        let group_handles: Vec<GroupHandle> = groups
            .into_iter()
            .map(|group| group.resolve(self))
            .collect::<GraphRecordResult<_>>()?;
        let node_handle = node.resolve(self)?;
        self.remove_node_from_groups_impl(&group_handles, node_handle)
    }

    pub fn remove_nodes_from_groups(
        &mut self,
        groups: impl IntoIterator<Item = impl AsLookup<GroupKind>>,
        nodes: impl IntoIterator<Item = impl AsLookup<NodeIndexKind>>,
    ) -> GraphRecordResult<()> {
        let plugins = self.plugins.clone();

        let group_handles: Vec<GroupHandle> = groups
            .into_iter()
            .map(|group| group.resolve(self))
            .collect::<GraphRecordResult<_>>()?;
        let node_handles: Vec<NodeHandle> = nodes
            .into_iter()
            .map(|node| node.resolve(self))
            .collect::<GraphRecordResult<_>>()?;

        let pre_context = PreRemoveNodesFromGroupsContext {
            group_handles,
            node_handles,
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_remove_nodes_from_groups(self, pre_context)
            })?;

        self.remove_nodes_from_groups_impl(&pre_context.group_handles, &pre_context.node_handles)?;

        let post_context = PostRemoveNodesFromGroupsContext {
            group_handles: pre_context.group_handles,
            node_handles: pre_context.node_handles,
        };

        plugins.iter().try_for_each(|(_, plugin)| {
            plugin.post_remove_nodes_from_groups(self, post_context.clone())
        })?;

        Ok(())
    }

    pub fn remove_nodes_from_groups_bypass_plugins(
        &mut self,
        groups: impl IntoIterator<Item = impl AsLookup<GroupKind>>,
        nodes: impl IntoIterator<Item = impl AsLookup<NodeIndexKind>>,
    ) -> GraphRecordResult<()> {
        let group_handles: Vec<GroupHandle> = groups
            .into_iter()
            .map(|group| group.resolve(self))
            .collect::<GraphRecordResult<_>>()?;
        let node_handles: Vec<NodeHandle> = nodes
            .into_iter()
            .map(|node| node.resolve(self))
            .collect::<GraphRecordResult<_>>()?;
        self.remove_nodes_from_groups_impl(&group_handles, &node_handles)
    }

    pub fn remove_edge_from_group(
        &mut self,
        group: impl AsLookup<GroupKind>,
        edge_index: &EdgeIndex,
    ) -> GraphRecordResult<()> {
        let plugins = self.plugins.clone();

        let group_handle = group.resolve(self)?;

        let pre_context = PreRemoveEdgeFromGroupContext {
            group_handle,
            edge_index: *edge_index,
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_remove_edge_from_group(self, pre_context)
            })?;

        self.remove_edge_from_group_impl(pre_context.group_handle, &pre_context.edge_index)?;

        let post_context = PostRemoveEdgeFromGroupContext {
            group_handle: pre_context.group_handle,
            edge_index: pre_context.edge_index,
        };

        plugins.iter().try_for_each(|(_, plugin)| {
            plugin.post_remove_edge_from_group(self, post_context.clone())
        })?;

        Ok(())
    }

    pub fn remove_edge_from_group_bypass_plugins(
        &mut self,
        group: impl AsLookup<GroupKind>,
        edge_index: &EdgeIndex,
    ) -> GraphRecordResult<()> {
        let group_handle = group.resolve(self)?;
        self.remove_edge_from_group_impl(group_handle, edge_index)
    }

    pub fn remove_edge_from_groups(
        &mut self,
        groups: impl IntoIterator<Item = impl AsLookup<GroupKind>>,
        edge_index: &EdgeIndex,
    ) -> GraphRecordResult<()> {
        let plugins = self.plugins.clone();

        let group_handles: Vec<GroupHandle> = groups
            .into_iter()
            .map(|group| group.resolve(self))
            .collect::<GraphRecordResult<_>>()?;

        let pre_context = PreRemoveEdgeFromGroupsContext {
            group_handles,
            edge_index: *edge_index,
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_remove_edge_from_groups(self, pre_context)
            })?;

        self.remove_edge_from_groups_impl(&pre_context.group_handles, &pre_context.edge_index)?;

        let post_context = PostRemoveEdgeFromGroupsContext {
            group_handles: pre_context.group_handles,
            edge_index: pre_context.edge_index,
        };

        plugins.iter().try_for_each(|(_, plugin)| {
            plugin.post_remove_edge_from_groups(self, post_context.clone())
        })?;

        Ok(())
    }

    pub fn remove_edge_from_groups_bypass_plugins(
        &mut self,
        groups: impl IntoIterator<Item = impl AsLookup<GroupKind>>,
        edge_index: &EdgeIndex,
    ) -> GraphRecordResult<()> {
        let group_handles: Vec<GroupHandle> = groups
            .into_iter()
            .map(|group| group.resolve(self))
            .collect::<GraphRecordResult<_>>()?;
        self.remove_edge_from_groups_impl(&group_handles, edge_index)
    }

    pub fn remove_edges_from_groups(
        &mut self,
        groups: impl IntoIterator<Item = impl AsLookup<GroupKind>>,
        edge_indices: &[EdgeIndex],
    ) -> GraphRecordResult<()> {
        let plugins = self.plugins.clone();

        let pre_context = PreRemoveEdgesFromGroupsContext {
            group_handles: resolve_all(self, groups)?,
            edge_indices: edge_indices.to_vec(),
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_remove_edges_from_groups(self, pre_context)
            })?;

        self.remove_edges_from_groups_impl(&pre_context.group_handles, &pre_context.edge_indices)?;

        let post_context = PostRemoveEdgesFromGroupsContext {
            group_handles: pre_context.group_handles,
            edge_indices: pre_context.edge_indices,
        };

        plugins.iter().try_for_each(|(_, plugin)| {
            plugin.post_remove_edges_from_groups(self, post_context.clone())
        })?;

        Ok(())
    }

    pub fn remove_edges_from_groups_bypass_plugins(
        &mut self,
        groups: impl IntoIterator<Item = impl AsLookup<GroupKind>>,
        edge_indices: &[EdgeIndex],
    ) -> GraphRecordResult<()> {
        self.remove_edges_from_groups_impl(&resolve_all(self, groups)?, edge_indices)
    }

    pub fn clear(&mut self) -> GraphRecordResult<()> {
        let plugins = self.plugins.clone();

        for (_, plugin) in plugins.iter() {
            plugin.pre_clear(self)?;
        }

        self.clear_impl();

        for (_, plugin) in plugins.iter() {
            plugin.post_clear(self)?;
        }

        Ok(())
    }

    pub fn clear_bypass_plugins(&mut self) -> GraphRecordResult<()> {
        self.clear_impl();

        Ok(())
    }
}
