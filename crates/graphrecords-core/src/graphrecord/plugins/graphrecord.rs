use graphrecords_utils::aliases::GrHashMap;

use super::{
    Plugin, PostAddEdgeContext, PostAddEdgeToGroupContext, PostAddEdgeWithGroupContext,
    PostAddEdgesContext, PostAddEdgesDataframesContext, PostAddEdgesDataframesWithGroupContext,
    PostAddEdgesWithGroupContext, PostAddGroupContext, PostAddNodeContext,
    PostAddNodeToGroupContext, PostAddNodeWithGroupContext, PostAddNodesContext,
    PostAddNodesDataframesContext, PostAddNodesDataframesWithGroupContext,
    PostAddNodesWithGroupContext, PostRemoveEdgeContext, PostRemoveEdgeFromGroupContext,
    PostRemoveGroupContext, PostRemoveNodeContext, PostRemoveNodeFromGroupContext,
    PreAddEdgeContext, PreAddEdgeToGroupContext, PreAddEdgeWithGroupContext, PreAddEdgesContext,
    PreAddEdgesDataframesContext, PreAddEdgesDataframesWithGroupContext,
    PreAddEdgesWithGroupContext, PreAddGroupContext, PreAddNodeContext, PreAddNodeToGroupContext,
    PreAddNodeWithGroupContext, PreAddNodesContext, PreAddNodesDataframesContext,
    PreAddNodesDataframesWithGroupContext, PreAddNodesWithGroupContext, PreRemoveEdgeContext,
    PreRemoveEdgeFromGroupContext, PreRemoveGroupContext, PreRemoveNodeContext,
    PreRemoveNodeFromGroupContext, PreSetSchemaContext,
};
use crate::{
    errors::{GraphRecordError, GraphRecordResult},
    graphrecord::{EdgeDataFrameInput, GraphRecord, NodeDataFrameInput},
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
    ) -> GraphRecordResult<()> {
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

        self.add_node_impl(pre_context.node_index, pre_context.attributes)?;

        let post_context = PostAddNodeContext { node_index };

        plugins
            .iter()
            .try_for_each(|(_, plugin)| plugin.post_add_node(self, post_context.clone()))?;

        Ok(())
    }

    pub fn add_node_bypass_plugins(
        &mut self,
        node_index: NodeIndex,
        attributes: Attributes,
    ) -> GraphRecordResult<()> {
        self.add_node_impl(node_index, attributes)
    }

    #[allow(clippy::needless_pass_by_value)]
    pub fn add_node_with_group(
        &mut self,
        node_index: NodeIndex,
        attributes: Attributes,
        group: Group,
    ) -> GraphRecordResult<()> {
        let plugins = self.plugins.clone();

        let pre_context = PreAddNodeWithGroupContext {
            node_index: node_index.clone(),
            attributes,
            group: group.clone(),
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_add_node_with_group(self, pre_context)
            })?;

        self.add_node_with_group_impl(
            pre_context.node_index,
            pre_context.attributes,
            pre_context.group,
        )?;

        let post_context = PostAddNodeWithGroupContext { node_index, group };

        plugins.iter().try_for_each(|(_, plugin)| {
            plugin.post_add_node_with_group(self, post_context.clone())
        })?;

        Ok(())
    }

    pub fn add_node_with_group_bypass_plugins(
        &mut self,
        node_index: NodeIndex,
        attributes: Attributes,
        group: Group,
    ) -> GraphRecordResult<()> {
        self.add_node_with_group_impl(node_index, attributes, group)
    }

    pub fn remove_node(&mut self, node_index: &NodeIndex) -> GraphRecordResult<Attributes> {
        let plugins = self.plugins.clone();

        let pre_context = PreRemoveNodeContext {
            node_index: node_index.clone(),
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_remove_node(self, pre_context)
            })?;

        let attributes = self.remove_node_impl(&pre_context.node_index)?;

        let post_context = PostRemoveNodeContext {
            node_index: pre_context.node_index,
        };

        plugins
            .iter()
            .try_for_each(|(_, plugin)| plugin.post_remove_node(self, post_context.clone()))?;

        Ok(attributes)
    }

    pub fn remove_node_bypass_plugins(
        &mut self,
        node_index: &NodeIndex,
    ) -> GraphRecordResult<Attributes> {
        self.remove_node_impl(node_index)
    }

    pub fn add_nodes(&mut self, nodes: Vec<(NodeIndex, Attributes)>) -> GraphRecordResult<()> {
        let plugins = self.plugins.clone();

        let pre_context = PreAddNodesContext { nodes };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_add_nodes(self, pre_context)
            })?;

        self.add_nodes_impl(pre_context.nodes.clone())?;

        let post_context = PostAddNodesContext {
            nodes: pre_context.nodes,
        };

        plugins
            .iter()
            .try_for_each(|(_, plugin)| plugin.post_add_nodes(self, post_context.clone()))?;

        Ok(())
    }

    pub fn add_nodes_bypass_plugins(
        &mut self,
        nodes: Vec<(NodeIndex, Attributes)>,
    ) -> GraphRecordResult<()> {
        self.add_nodes_impl(nodes)
    }

    #[allow(clippy::needless_pass_by_value)]
    pub fn add_nodes_with_group(
        &mut self,
        nodes: Vec<(NodeIndex, Attributes)>,
        group: Group,
    ) -> GraphRecordResult<()> {
        let plugins = self.plugins.clone();

        let pre_context = PreAddNodesWithGroupContext { nodes, group };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_add_nodes_with_group(self, pre_context)
            })?;

        self.add_nodes_with_group_impl(pre_context.nodes.clone(), pre_context.group.clone())?;

        let post_context = PostAddNodesWithGroupContext {
            nodes: pre_context.nodes,
            group: pre_context.group,
        };

        plugins.iter().try_for_each(|(_, plugin)| {
            plugin.post_add_nodes_with_group(self, post_context.clone())
        })?;

        Ok(())
    }

    pub fn add_nodes_with_group_bypass_plugins(
        &mut self,
        nodes: Vec<(NodeIndex, Attributes)>,
        group: Group,
    ) -> GraphRecordResult<()> {
        self.add_nodes_with_group_impl(nodes, group)
    }

    pub fn add_nodes_dataframes(
        &mut self,
        nodes_dataframes: impl IntoIterator<Item = impl Into<NodeDataFrameInput>>,
    ) -> GraphRecordResult<()> {
        let plugins = self.plugins.clone();

        let pre_context = PreAddNodesDataframesContext {
            nodes_dataframes: nodes_dataframes.into_iter().map(Into::into).collect(),
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_add_nodes_dataframes(self, pre_context)
            })?;

        self.add_nodes_dataframes_impl(pre_context.nodes_dataframes.clone())?;

        let post_context = PostAddNodesDataframesContext {
            nodes_dataframes: pre_context.nodes_dataframes,
        };

        plugins.iter().try_for_each(|(_, plugin)| {
            plugin.post_add_nodes_dataframes(self, post_context.clone())
        })?;

        Ok(())
    }

    pub fn add_nodes_dataframes_bypass_plugins(
        &mut self,
        nodes_dataframes: impl IntoIterator<Item = impl Into<NodeDataFrameInput>>,
    ) -> GraphRecordResult<()> {
        self.add_nodes_dataframes_impl(nodes_dataframes)
    }

    pub fn add_nodes_dataframes_with_group(
        &mut self,
        nodes_dataframes: impl IntoIterator<Item = impl Into<NodeDataFrameInput>>,
        group: Group,
    ) -> GraphRecordResult<()> {
        let plugins = self.plugins.clone();

        let pre_context = PreAddNodesDataframesWithGroupContext {
            nodes_dataframes: nodes_dataframes.into_iter().map(Into::into).collect(),
            group,
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_add_nodes_dataframes_with_group(self, pre_context)
            })?;

        self.add_nodes_dataframes_with_group_impl(
            pre_context.nodes_dataframes.clone(),
            pre_context.group.clone(),
        )?;

        let post_context = PostAddNodesDataframesWithGroupContext {
            nodes_dataframes: pre_context.nodes_dataframes,
            group: pre_context.group,
        };

        plugins.iter().try_for_each(|(_, plugin)| {
            plugin.post_add_nodes_dataframes_with_group(self, post_context.clone())
        })?;

        Ok(())
    }

    pub fn add_nodes_dataframes_with_group_bypass_plugins(
        &mut self,
        nodes_dataframes: impl IntoIterator<Item = impl Into<NodeDataFrameInput>>,
        group: Group,
    ) -> GraphRecordResult<()> {
        self.add_nodes_dataframes_with_group_impl(nodes_dataframes, group)
    }

    #[allow(clippy::needless_pass_by_value)]
    pub fn add_edge(
        &mut self,
        source_node_index: NodeIndex,
        target_node_index: NodeIndex,
        attributes: Attributes,
    ) -> GraphRecordResult<EdgeIndex> {
        let plugins = self.plugins.clone();

        let pre_context = PreAddEdgeContext {
            source_node_index,
            target_node_index,
            attributes,
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_add_edge(self, pre_context)
            })?;

        let edge_index = self.add_edge_impl(
            pre_context.source_node_index,
            pre_context.target_node_index,
            pre_context.attributes,
        )?;

        let post_context = PostAddEdgeContext { edge_index };

        plugins
            .iter()
            .try_for_each(|(_, plugin)| plugin.post_add_edge(self, post_context.clone()))?;

        Ok(edge_index)
    }

    pub fn add_edge_bypass_plugins(
        &mut self,
        source_node_index: NodeIndex,
        target_node_index: NodeIndex,
        attributes: Attributes,
    ) -> GraphRecordResult<EdgeIndex> {
        self.add_edge_impl(source_node_index, target_node_index, attributes)
    }

    #[allow(clippy::needless_pass_by_value)]
    pub fn add_edge_with_group(
        &mut self,
        source_node_index: NodeIndex,
        target_node_index: NodeIndex,
        attributes: Attributes,
        group: Group,
    ) -> GraphRecordResult<EdgeIndex> {
        let plugins = self.plugins.clone();

        let pre_context = PreAddEdgeWithGroupContext {
            source_node_index,
            target_node_index,
            attributes,
            group,
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_add_edge_with_group(self, pre_context)
            })?;

        let edge_index = self.add_edge_with_group_impl(
            pre_context.source_node_index,
            pre_context.target_node_index,
            pre_context.attributes,
            pre_context.group,
        )?;

        let post_context = PostAddEdgeWithGroupContext { edge_index };

        plugins.iter().try_for_each(|(_, plugin)| {
            plugin.post_add_edge_with_group(self, post_context.clone())
        })?;

        Ok(edge_index)
    }

    pub fn add_edge_with_group_bypass_plugins(
        &mut self,
        source_node_index: NodeIndex,
        target_node_index: NodeIndex,
        attributes: Attributes,
        group: Group,
    ) -> GraphRecordResult<EdgeIndex> {
        self.add_edge_with_group_impl(source_node_index, target_node_index, attributes, group)
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

    pub fn add_edges(
        &mut self,
        edges: Vec<(NodeIndex, NodeIndex, Attributes)>,
    ) -> GraphRecordResult<Vec<EdgeIndex>> {
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

    pub fn add_edges_bypass_plugins(
        &mut self,
        edges: Vec<(NodeIndex, NodeIndex, Attributes)>,
    ) -> GraphRecordResult<Vec<EdgeIndex>> {
        self.add_edges_impl(edges)
    }

    pub fn add_edges_with_group(
        &mut self,
        edges: Vec<(NodeIndex, NodeIndex, Attributes)>,
        group: &Group,
    ) -> GraphRecordResult<Vec<EdgeIndex>> {
        let plugins = self.plugins.clone();

        let pre_context = PreAddEdgesWithGroupContext {
            edges,
            group: group.clone(),
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_add_edges_with_group(self, pre_context)
            })?;

        let edge_indices = self.add_edges_with_group_impl(pre_context.edges, &pre_context.group)?;

        let post_context = PostAddEdgesWithGroupContext {
            edge_indices: edge_indices.clone(),
        };

        plugins.iter().try_for_each(|(_, plugin)| {
            plugin.post_add_edges_with_group(self, post_context.clone())
        })?;

        Ok(edge_indices)
    }

    pub fn add_edges_with_group_bypass_plugins(
        &mut self,
        edges: Vec<(NodeIndex, NodeIndex, Attributes)>,
        group: &Group,
    ) -> GraphRecordResult<Vec<EdgeIndex>> {
        self.add_edges_with_group_impl(edges, group)
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
        group: &Group,
    ) -> GraphRecordResult<Vec<EdgeIndex>> {
        let plugins = self.plugins.clone();

        let pre_context = PreAddEdgesDataframesWithGroupContext {
            edges_dataframes: edges_dataframes.into_iter().map(Into::into).collect(),
            group: group.clone(),
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_add_edges_dataframes_with_group(self, pre_context)
            })?;

        let edge_indices = self.add_edges_dataframes_with_group_impl(
            pre_context.edges_dataframes.clone(),
            &pre_context.group,
        )?;

        let post_context = PostAddEdgesDataframesWithGroupContext {
            edges_dataframes: pre_context.edges_dataframes,
            group: pre_context.group,
        };

        plugins.iter().try_for_each(|(_, plugin)| {
            plugin.post_add_edges_dataframes_with_group(self, post_context.clone())
        })?;

        Ok(edge_indices)
    }

    pub fn add_edges_dataframes_with_group_bypass_plugins(
        &mut self,
        edges_dataframes: impl IntoIterator<Item = impl Into<EdgeDataFrameInput>>,
        group: &Group,
    ) -> GraphRecordResult<Vec<EdgeIndex>> {
        self.add_edges_dataframes_with_group_impl(edges_dataframes, group)
    }

    pub fn add_group(
        &mut self,
        group: Group,
        node_indices: Option<Vec<NodeIndex>>,
        edge_indices: Option<Vec<EdgeIndex>>,
    ) -> GraphRecordResult<()> {
        let plugins = self.plugins.clone();

        let pre_context = PreAddGroupContext {
            group,
            node_indices,
            edge_indices,
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_add_group(self, pre_context)
            })?;

        self.add_group_impl(
            pre_context.group.clone(),
            pre_context.node_indices.clone(),
            pre_context.edge_indices.clone(),
        )?;

        let post_context = PostAddGroupContext {
            group: pre_context.group,
            node_indices: pre_context.node_indices,
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
        node_indices: Option<Vec<NodeIndex>>,
        edge_indices: Option<Vec<EdgeIndex>>,
    ) -> GraphRecordResult<()> {
        self.add_group_impl(group, node_indices, edge_indices)
    }

    pub fn remove_group(&mut self, group: &Group) -> GraphRecordResult<()> {
        let plugins = self.plugins.clone();

        let pre_context = PreRemoveGroupContext {
            group: group.clone(),
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_remove_group(self, pre_context)
            })?;

        self.remove_group_impl(&pre_context.group)?;

        let post_context = PostRemoveGroupContext {
            group: pre_context.group,
        };

        plugins
            .iter()
            .try_for_each(|(_, plugin)| plugin.post_remove_group(self, post_context.clone()))?;

        Ok(())
    }

    pub fn remove_group_bypass_plugins(&mut self, group: &Group) -> GraphRecordResult<()> {
        self.remove_group_impl(group)
    }

    pub fn add_node_to_group(
        &mut self,
        group: Group,
        node_index: NodeIndex,
    ) -> GraphRecordResult<()> {
        let plugins = self.plugins.clone();

        let pre_context = PreAddNodeToGroupContext { group, node_index };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_add_node_to_group(self, pre_context)
            })?;

        self.add_node_to_group_impl(pre_context.group.clone(), pre_context.node_index.clone())?;

        let post_context = PostAddNodeToGroupContext {
            group: pre_context.group,
            node_index: pre_context.node_index,
        };

        plugins.iter().try_for_each(|(_, plugin)| {
            plugin.post_add_node_to_group(self, post_context.clone())
        })?;

        Ok(())
    }

    pub fn add_node_to_group_bypass_plugins(
        &mut self,
        group: Group,
        node_index: NodeIndex,
    ) -> GraphRecordResult<()> {
        self.add_node_to_group_impl(group, node_index)
    }

    pub fn add_edge_to_group(
        &mut self,
        group: Group,
        edge_index: EdgeIndex,
    ) -> GraphRecordResult<()> {
        let plugins = self.plugins.clone();

        let pre_context = PreAddEdgeToGroupContext { group, edge_index };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_add_edge_to_group(self, pre_context)
            })?;

        self.add_edge_to_group_impl(pre_context.group.clone(), pre_context.edge_index)?;

        let post_context = PostAddEdgeToGroupContext {
            group: pre_context.group,
            edge_index: pre_context.edge_index,
        };

        plugins.iter().try_for_each(|(_, plugin)| {
            plugin.post_add_edge_to_group(self, post_context.clone())
        })?;

        Ok(())
    }

    pub fn add_edge_to_group_bypass_plugins(
        &mut self,
        group: Group,
        edge_index: EdgeIndex,
    ) -> GraphRecordResult<()> {
        self.add_edge_to_group_impl(group, edge_index)
    }

    pub fn remove_node_from_group(
        &mut self,
        group: &Group,
        node_index: &NodeIndex,
    ) -> GraphRecordResult<()> {
        let plugins = self.plugins.clone();

        let pre_context = PreRemoveNodeFromGroupContext {
            group: group.clone(),
            node_index: node_index.clone(),
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_remove_node_from_group(self, pre_context)
            })?;

        self.remove_node_from_group_impl(&pre_context.group, &pre_context.node_index)?;

        let post_context = PostRemoveNodeFromGroupContext {
            group: pre_context.group,
            node_index: pre_context.node_index,
        };

        plugins.iter().try_for_each(|(_, plugin)| {
            plugin.post_remove_node_from_group(self, post_context.clone())
        })?;

        Ok(())
    }

    pub fn remove_node_from_group_bypass_plugins(
        &mut self,
        group: &Group,
        node_index: &NodeIndex,
    ) -> GraphRecordResult<()> {
        self.remove_node_from_group_impl(group, node_index)
    }

    pub fn remove_edge_from_group(
        &mut self,
        group: &Group,
        edge_index: &EdgeIndex,
    ) -> GraphRecordResult<()> {
        let plugins = self.plugins.clone();

        let pre_context = PreRemoveEdgeFromGroupContext {
            group: group.clone(),
            edge_index: *edge_index,
        };
        let pre_context = plugins
            .iter()
            .try_fold(pre_context, |pre_context, (_, plugin)| {
                plugin.pre_remove_edge_from_group(self, pre_context)
            })?;

        self.remove_edge_from_group_impl(&pre_context.group, &pre_context.edge_index)?;

        let post_context = PostRemoveEdgeFromGroupContext {
            group: pre_context.group,
            edge_index: pre_context.edge_index,
        };

        plugins.iter().try_for_each(|(_, plugin)| {
            plugin.post_remove_edge_from_group(self, post_context.clone())
        })?;

        Ok(())
    }

    pub fn remove_edge_from_group_bypass_plugins(
        &mut self,
        group: &Group,
        edge_index: &EdgeIndex,
    ) -> GraphRecordResult<()> {
        self.remove_edge_from_group_impl(group, edge_index)
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
