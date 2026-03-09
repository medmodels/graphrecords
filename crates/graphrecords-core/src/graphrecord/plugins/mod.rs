mod graphrecord;

use crate::{
    errors::GraphRecordResult,
    graphrecord::{EdgeDataFrameInput, NodeDataFrameInput},
    prelude::{Attributes, EdgeIndex, Group, NodeIndex, Schema},
};
pub use graphrecord::{PluginGraphRecord, PluginName};
use std::fmt::Debug;

#[derive(Debug, Clone)]
pub struct PreSetSchemaContext {
    pub schema: Schema,
}

#[derive(Debug, Clone)]
pub struct PreAddNodeContext {
    pub node_index: NodeIndex,
    pub attributes: Attributes,
}

#[derive(Debug, Clone)]
pub struct PostAddNodeContext {
    pub node_index: NodeIndex,
}

#[derive(Debug, Clone)]
pub struct PreAddNodeWithGroupContext {
    pub node_index: NodeIndex,
    pub attributes: Attributes,
    pub group: Group,
}

#[derive(Debug, Clone)]
pub struct PostAddNodeWithGroupContext {
    pub node_index: NodeIndex,
    pub group: Group,
}

#[derive(Debug, Clone)]
pub struct PreRemoveNodeContext {
    pub node_index: NodeIndex,
}

#[derive(Debug, Clone)]
pub struct PostRemoveNodeContext {
    pub node_index: NodeIndex,
}

#[derive(Debug, Clone)]
pub struct PreAddNodesContext {
    pub nodes: Vec<(NodeIndex, Attributes)>,
}

#[derive(Debug, Clone)]
pub struct PostAddNodesContext {
    pub nodes: Vec<(NodeIndex, Attributes)>,
}

#[derive(Debug, Clone)]
pub struct PreAddNodesWithGroupContext {
    pub nodes: Vec<(NodeIndex, Attributes)>,
    pub group: Group,
}

#[derive(Debug, Clone)]
pub struct PostAddNodesWithGroupContext {
    pub nodes: Vec<(NodeIndex, Attributes)>,
    pub group: Group,
}

#[derive(Debug, Clone)]
pub struct PreAddNodesDataframesContext {
    pub nodes_dataframes: Vec<NodeDataFrameInput>,
}

#[derive(Debug, Clone)]
pub struct PostAddNodesDataframesContext {
    pub nodes_dataframes: Vec<NodeDataFrameInput>,
}

#[derive(Debug, Clone)]
pub struct PreAddNodesDataframesWithGroupContext {
    pub nodes_dataframes: Vec<NodeDataFrameInput>,
    pub group: Group,
}

#[derive(Debug, Clone)]
pub struct PostAddNodesDataframesWithGroupContext {
    pub nodes_dataframes: Vec<NodeDataFrameInput>,
    pub group: Group,
}

#[derive(Debug, Clone)]
pub struct PreAddEdgeContext {
    pub source_node_index: NodeIndex,
    pub target_node_index: NodeIndex,
    pub attributes: Attributes,
}

#[derive(Debug, Clone)]
pub struct PostAddEdgeContext {
    pub edge_index: EdgeIndex,
}

#[derive(Debug, Clone)]
pub struct PreAddEdgeWithGroupContext {
    pub source_node_index: NodeIndex,
    pub target_node_index: NodeIndex,
    pub attributes: Attributes,
    pub group: Group,
}

#[derive(Debug, Clone)]
pub struct PostAddEdgeWithGroupContext {
    pub edge_index: EdgeIndex,
}

#[derive(Debug, Clone)]
pub struct PreRemoveEdgeContext {
    pub edge_index: EdgeIndex,
}

#[derive(Debug, Clone)]
pub struct PostRemoveEdgeContext {
    pub edge_index: EdgeIndex,
}

#[derive(Debug, Clone)]
pub struct PreAddEdgesContext {
    pub edges: Vec<(NodeIndex, NodeIndex, Attributes)>,
}

#[derive(Debug, Clone)]
pub struct PostAddEdgesContext {
    pub edge_indices: Vec<EdgeIndex>,
}

#[derive(Debug, Clone)]
pub struct PreAddEdgesWithGroupContext {
    pub edges: Vec<(NodeIndex, NodeIndex, Attributes)>,
    pub group: Group,
}

#[derive(Debug, Clone)]
pub struct PostAddEdgesWithGroupContext {
    pub edge_indices: Vec<EdgeIndex>,
}

#[derive(Debug, Clone)]
pub struct PreAddEdgesDataframesContext {
    pub edges_dataframes: Vec<EdgeDataFrameInput>,
}

#[derive(Debug, Clone)]
pub struct PostAddEdgesDataframesContext {
    pub edges_dataframes: Vec<EdgeDataFrameInput>,
}

#[derive(Debug, Clone)]
pub struct PreAddEdgesDataframesWithGroupContext {
    pub edges_dataframes: Vec<EdgeDataFrameInput>,
    pub group: Group,
}

#[derive(Debug, Clone)]
pub struct PostAddEdgesDataframesWithGroupContext {
    pub edges_dataframes: Vec<EdgeDataFrameInput>,
    pub group: Group,
}

#[derive(Debug, Clone)]
pub struct PreAddGroupContext {
    pub group: Group,
    pub node_indices: Option<Vec<NodeIndex>>,
    pub edge_indices: Option<Vec<EdgeIndex>>,
}

#[derive(Debug, Clone)]
pub struct PostAddGroupContext {
    pub group: Group,
    pub node_indices: Option<Vec<NodeIndex>>,
    pub edge_indices: Option<Vec<EdgeIndex>>,
}

#[derive(Debug, Clone)]
pub struct PreRemoveGroupContext {
    pub group: Group,
}

#[derive(Debug, Clone)]
pub struct PostRemoveGroupContext {
    pub group: Group,
}

#[derive(Debug, Clone)]
pub struct PreAddNodeToGroupContext {
    pub group: Group,
    pub node_index: NodeIndex,
}

#[derive(Debug, Clone)]
pub struct PostAddNodeToGroupContext {
    pub group: Group,
    pub node_index: NodeIndex,
}

#[derive(Debug, Clone)]
pub struct PreAddEdgeToGroupContext {
    pub group: Group,
    pub edge_index: EdgeIndex,
}

#[derive(Debug, Clone)]
pub struct PostAddEdgeToGroupContext {
    pub group: Group,
    pub edge_index: EdgeIndex,
}

#[derive(Debug, Clone)]
pub struct PreRemoveNodeFromGroupContext {
    pub group: Group,
    pub node_index: NodeIndex,
}

#[derive(Debug, Clone)]
pub struct PostRemoveNodeFromGroupContext {
    pub group: Group,
    pub node_index: NodeIndex,
}

#[derive(Debug, Clone)]
pub struct PreRemoveEdgeFromGroupContext {
    pub group: Group,
    pub edge_index: EdgeIndex,
}

#[derive(Debug, Clone)]
pub struct PostRemoveEdgeFromGroupContext {
    pub group: Group,
    pub edge_index: EdgeIndex,
}

#[cfg_attr(feature = "serde", typetag::serde(tag = "type"))]
#[allow(unused_variables)]
pub trait Plugin: Debug + Send + Sync {
    fn clone_box(&self) -> Box<dyn Plugin>;

    fn initialize(&self, graphrecord: &mut PluginGraphRecord) -> GraphRecordResult<()> {
        Ok(())
    }

    fn finalize(&self, graphrecord: &mut PluginGraphRecord) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_set_schema(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PreSetSchemaContext,
    ) -> GraphRecordResult<PreSetSchemaContext> {
        Ok(context)
    }

    fn post_set_schema(&self, graphrecord: &mut PluginGraphRecord) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_freeze_schema(&self, graphrecord: &mut PluginGraphRecord) -> GraphRecordResult<()> {
        Ok(())
    }

    fn post_freeze_schema(&self, graphrecord: &mut PluginGraphRecord) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_unfreeze_schema(&self, graphrecord: &mut PluginGraphRecord) -> GraphRecordResult<()> {
        Ok(())
    }

    fn post_unfreeze_schema(&self, graphrecord: &mut PluginGraphRecord) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_node(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PreAddNodeContext,
    ) -> GraphRecordResult<PreAddNodeContext> {
        Ok(context)
    }

    fn post_add_node(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PostAddNodeContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_node_with_group(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PreAddNodeWithGroupContext,
    ) -> GraphRecordResult<PreAddNodeWithGroupContext> {
        Ok(context)
    }

    fn post_add_node_with_group(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PostAddNodeWithGroupContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_remove_node(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PreRemoveNodeContext,
    ) -> GraphRecordResult<PreRemoveNodeContext> {
        Ok(context)
    }

    fn post_remove_node(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PostRemoveNodeContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_nodes(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PreAddNodesContext,
    ) -> GraphRecordResult<PreAddNodesContext> {
        Ok(context)
    }

    fn post_add_nodes(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PostAddNodesContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_nodes_with_group(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PreAddNodesWithGroupContext,
    ) -> GraphRecordResult<PreAddNodesWithGroupContext> {
        Ok(context)
    }

    fn post_add_nodes_with_group(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PostAddNodesWithGroupContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_nodes_dataframes(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PreAddNodesDataframesContext,
    ) -> GraphRecordResult<PreAddNodesDataframesContext> {
        Ok(context)
    }

    fn post_add_nodes_dataframes(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PostAddNodesDataframesContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_nodes_dataframes_with_group(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PreAddNodesDataframesWithGroupContext,
    ) -> GraphRecordResult<PreAddNodesDataframesWithGroupContext> {
        Ok(context)
    }

    fn post_add_nodes_dataframes_with_group(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PostAddNodesDataframesWithGroupContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_edge(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PreAddEdgeContext,
    ) -> GraphRecordResult<PreAddEdgeContext> {
        Ok(context)
    }

    fn post_add_edge(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PostAddEdgeContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_edge_with_group(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PreAddEdgeWithGroupContext,
    ) -> GraphRecordResult<PreAddEdgeWithGroupContext> {
        Ok(context)
    }

    fn post_add_edge_with_group(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PostAddEdgeWithGroupContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_remove_edge(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PreRemoveEdgeContext,
    ) -> GraphRecordResult<PreRemoveEdgeContext> {
        Ok(context)
    }

    fn post_remove_edge(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PostRemoveEdgeContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_edges(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PreAddEdgesContext,
    ) -> GraphRecordResult<PreAddEdgesContext> {
        Ok(context)
    }

    fn post_add_edges(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PostAddEdgesContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_edges_with_group(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PreAddEdgesWithGroupContext,
    ) -> GraphRecordResult<PreAddEdgesWithGroupContext> {
        Ok(context)
    }

    fn post_add_edges_with_group(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PostAddEdgesWithGroupContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_edges_dataframes(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PreAddEdgesDataframesContext,
    ) -> GraphRecordResult<PreAddEdgesDataframesContext> {
        Ok(context)
    }

    fn post_add_edges_dataframes(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PostAddEdgesDataframesContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_edges_dataframes_with_group(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PreAddEdgesDataframesWithGroupContext,
    ) -> GraphRecordResult<PreAddEdgesDataframesWithGroupContext> {
        Ok(context)
    }

    fn post_add_edges_dataframes_with_group(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PostAddEdgesDataframesWithGroupContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_group(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PreAddGroupContext,
    ) -> GraphRecordResult<PreAddGroupContext> {
        Ok(context)
    }

    fn post_add_group(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PostAddGroupContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_remove_group(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PreRemoveGroupContext,
    ) -> GraphRecordResult<PreRemoveGroupContext> {
        Ok(context)
    }

    fn post_remove_group(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PostRemoveGroupContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_node_to_group(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PreAddNodeToGroupContext,
    ) -> GraphRecordResult<PreAddNodeToGroupContext> {
        Ok(context)
    }

    fn post_add_node_to_group(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PostAddNodeToGroupContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_edge_to_group(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PreAddEdgeToGroupContext,
    ) -> GraphRecordResult<PreAddEdgeToGroupContext> {
        Ok(context)
    }

    fn post_add_edge_to_group(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PostAddEdgeToGroupContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_remove_node_from_group(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PreRemoveNodeFromGroupContext,
    ) -> GraphRecordResult<PreRemoveNodeFromGroupContext> {
        Ok(context)
    }

    fn post_remove_node_from_group(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PostRemoveNodeFromGroupContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_remove_edge_from_group(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PreRemoveEdgeFromGroupContext,
    ) -> GraphRecordResult<PreRemoveEdgeFromGroupContext> {
        Ok(context)
    }

    fn post_remove_edge_from_group(
        &self,
        graphrecord: &mut PluginGraphRecord,
        context: PostRemoveEdgeFromGroupContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_clear(&self, graphrecord: &mut PluginGraphRecord) -> GraphRecordResult<()> {
        Ok(())
    }

    fn post_clear(&self, graphrecord: &mut PluginGraphRecord) -> GraphRecordResult<()> {
        Ok(())
    }
}

impl Clone for Box<dyn Plugin> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}
