mod graphrecord;

use crate::{
    GraphRecord,
    errors::GraphRecordResult,
    graphrecord::{EdgeDataFrameInput, GroupHandle, NodeDataFrameInput, NodeHandle},
    prelude::{Attributes, EdgeIndex, Group, NodeIndex, Schema},
};
pub use graphrecord::PluginName;
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
    pub group_handle: GroupHandle,
}

#[derive(Debug, Clone)]
pub struct PostAddNodeWithGroupContext {
    pub node_index: NodeIndex,
    pub group_handle: GroupHandle,
}

#[derive(Debug, Clone)]
pub struct PreAddNodeWithGroupsContext {
    pub node_index: NodeIndex,
    pub attributes: Attributes,
    pub group_handles: Vec<GroupHandle>,
}

#[derive(Debug, Clone)]
pub struct PostAddNodeWithGroupsContext {
    pub node_index: NodeIndex,
    pub group_handles: Vec<GroupHandle>,
}

#[derive(Debug, Clone)]
pub struct PreRemoveNodeContext {
    pub node_handle: NodeHandle,
}

#[derive(Debug, Clone)]
pub struct PostRemoveNodeContext {
    pub node_handle: NodeHandle,
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
    pub group_handle: GroupHandle,
}

#[derive(Debug, Clone)]
pub struct PostAddNodesWithGroupContext {
    pub nodes: Vec<(NodeIndex, Attributes)>,
    pub group_handle: GroupHandle,
}

#[derive(Debug, Clone)]
pub struct PreAddNodesWithGroupsContext {
    pub nodes: Vec<(NodeIndex, Attributes)>,
    pub group_handles: Vec<GroupHandle>,
}

#[derive(Debug, Clone)]
pub struct PostAddNodesWithGroupsContext {
    pub nodes: Vec<(NodeIndex, Attributes)>,
    pub group_handles: Vec<GroupHandle>,
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
    pub group_handle: GroupHandle,
}

#[derive(Debug, Clone)]
pub struct PostAddNodesDataframesWithGroupContext {
    pub nodes_dataframes: Vec<NodeDataFrameInput>,
    pub group_handle: GroupHandle,
}

#[derive(Debug, Clone)]
pub struct PreAddNodesDataframesWithGroupsContext {
    pub nodes_dataframes: Vec<NodeDataFrameInput>,
    pub group_handles: Vec<GroupHandle>,
}

#[derive(Debug, Clone)]
pub struct PostAddNodesDataframesWithGroupsContext {
    pub nodes_dataframes: Vec<NodeDataFrameInput>,
    pub group_handles: Vec<GroupHandle>,
}

#[derive(Debug, Clone)]
pub struct PreAddEdgeContext {
    pub source_node_handle: NodeHandle,
    pub target_node_handle: NodeHandle,
    pub attributes: Attributes,
}

#[derive(Debug, Clone)]
pub struct PostAddEdgeContext {
    pub edge_index: EdgeIndex,
}

#[derive(Debug, Clone)]
pub struct PreAddEdgeWithGroupContext {
    pub source_node_handle: NodeHandle,
    pub target_node_handle: NodeHandle,
    pub attributes: Attributes,
    pub group_handle: GroupHandle,
}

#[derive(Debug, Clone)]
pub struct PostAddEdgeWithGroupContext {
    pub edge_index: EdgeIndex,
    pub group_handle: GroupHandle,
}

#[derive(Debug, Clone)]
pub struct PreAddEdgeWithGroupsContext {
    pub source_node_handle: NodeHandle,
    pub target_node_handle: NodeHandle,
    pub attributes: Attributes,
    pub group_handles: Vec<GroupHandle>,
}

#[derive(Debug, Clone)]
pub struct PostAddEdgeWithGroupsContext {
    pub edge_index: EdgeIndex,
    pub group_handles: Vec<GroupHandle>,
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
    pub edges: Vec<(NodeHandle, NodeHandle, Attributes)>,
}

#[derive(Debug, Clone)]
pub struct PostAddEdgesContext {
    pub edge_indices: Vec<EdgeIndex>,
}

#[derive(Debug, Clone)]
pub struct PreAddEdgesWithGroupContext {
    pub edges: Vec<(NodeHandle, NodeHandle, Attributes)>,
    pub group_handle: GroupHandle,
}

#[derive(Debug, Clone)]
pub struct PostAddEdgesWithGroupContext {
    pub edge_indices: Vec<EdgeIndex>,
    pub group_handle: GroupHandle,
}

#[derive(Debug, Clone)]
pub struct PreAddEdgesWithGroupsContext {
    pub edges: Vec<(NodeHandle, NodeHandle, Attributes)>,
    pub group_handles: Vec<GroupHandle>,
}

#[derive(Debug, Clone)]
pub struct PostAddEdgesWithGroupsContext {
    pub edge_indices: Vec<EdgeIndex>,
    pub group_handles: Vec<GroupHandle>,
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
    pub group_handle: GroupHandle,
}

#[derive(Debug, Clone)]
pub struct PostAddEdgesDataframesWithGroupContext {
    pub edges_dataframes: Vec<EdgeDataFrameInput>,
    pub group_handle: GroupHandle,
}

#[derive(Debug, Clone)]
pub struct PreAddEdgesDataframesWithGroupsContext {
    pub edges_dataframes: Vec<EdgeDataFrameInput>,
    pub group_handles: Vec<GroupHandle>,
}

#[derive(Debug, Clone)]
pub struct PostAddEdgesDataframesWithGroupsContext {
    pub edges_dataframes: Vec<EdgeDataFrameInput>,
    pub group_handles: Vec<GroupHandle>,
}

#[derive(Debug, Clone)]
pub struct PreAddGroupContext {
    pub group: Group,
    pub node_handles: Option<Vec<NodeHandle>>,
    pub edge_indices: Option<Vec<EdgeIndex>>,
}

#[derive(Debug, Clone)]
pub struct PostAddGroupContext {
    pub group: Group,
    pub node_handles: Option<Vec<NodeHandle>>,
    pub edge_indices: Option<Vec<EdgeIndex>>,
}

#[derive(Debug, Clone)]
pub struct PreRemoveGroupContext {
    pub group_handle: GroupHandle,
}

#[derive(Debug, Clone)]
pub struct PostRemoveGroupContext {
    pub group_handle: GroupHandle,
}

#[derive(Debug, Clone)]
pub struct PreAddNodeToGroupContext {
    pub group_handle: GroupHandle,
    pub node_handle: NodeHandle,
}

#[derive(Debug, Clone)]
pub struct PostAddNodeToGroupContext {
    pub group_handle: GroupHandle,
    pub node_handle: NodeHandle,
}

#[derive(Debug, Clone)]
pub struct PreAddNodeToGroupsContext {
    pub group_handles: Vec<GroupHandle>,
    pub node_handle: NodeHandle,
}

#[derive(Debug, Clone)]
pub struct PostAddNodeToGroupsContext {
    pub group_handles: Vec<GroupHandle>,
    pub node_handle: NodeHandle,
}

#[derive(Debug, Clone)]
pub struct PreAddNodesToGroupsContext {
    pub group_handles: Vec<GroupHandle>,
    pub node_handles: Vec<NodeHandle>,
}

#[derive(Debug, Clone)]
pub struct PostAddNodesToGroupsContext {
    pub group_handles: Vec<GroupHandle>,
    pub node_handles: Vec<NodeHandle>,
}

#[derive(Debug, Clone)]
pub struct PreAddEdgeToGroupContext {
    pub group_handle: GroupHandle,
    pub edge_index: EdgeIndex,
}

#[derive(Debug, Clone)]
pub struct PostAddEdgeToGroupContext {
    pub group_handle: GroupHandle,
    pub edge_index: EdgeIndex,
}

#[derive(Debug, Clone)]
pub struct PreAddEdgeToGroupsContext {
    pub group_handles: Vec<GroupHandle>,
    pub edge_index: EdgeIndex,
}

#[derive(Debug, Clone)]
pub struct PostAddEdgeToGroupsContext {
    pub group_handles: Vec<GroupHandle>,
    pub edge_index: EdgeIndex,
}

#[derive(Debug, Clone)]
pub struct PreAddEdgesToGroupsContext {
    pub group_handles: Vec<GroupHandle>,
    pub edge_indices: Vec<EdgeIndex>,
}

#[derive(Debug, Clone)]
pub struct PostAddEdgesToGroupsContext {
    pub group_handles: Vec<GroupHandle>,
    pub edge_indices: Vec<EdgeIndex>,
}

#[derive(Debug, Clone)]
pub struct PreRemoveNodeFromGroupContext {
    pub group_handle: GroupHandle,
    pub node_handle: NodeHandle,
}

#[derive(Debug, Clone)]
pub struct PostRemoveNodeFromGroupContext {
    pub group_handle: GroupHandle,
    pub node_handle: NodeHandle,
}

#[derive(Debug, Clone)]
pub struct PreRemoveNodeFromGroupsContext {
    pub group_handles: Vec<GroupHandle>,
    pub node_handle: NodeHandle,
}

#[derive(Debug, Clone)]
pub struct PostRemoveNodeFromGroupsContext {
    pub group_handles: Vec<GroupHandle>,
    pub node_handle: NodeHandle,
}

#[derive(Debug, Clone)]
pub struct PreRemoveNodesFromGroupsContext {
    pub group_handles: Vec<GroupHandle>,
    pub node_handles: Vec<NodeHandle>,
}

#[derive(Debug, Clone)]
pub struct PostRemoveNodesFromGroupsContext {
    pub group_handles: Vec<GroupHandle>,
    pub node_handles: Vec<NodeHandle>,
}

#[derive(Debug, Clone)]
pub struct PreRemoveEdgeFromGroupContext {
    pub group_handle: GroupHandle,
    pub edge_index: EdgeIndex,
}

#[derive(Debug, Clone)]
pub struct PostRemoveEdgeFromGroupContext {
    pub group_handle: GroupHandle,
    pub edge_index: EdgeIndex,
}

#[derive(Debug, Clone)]
pub struct PreRemoveEdgeFromGroupsContext {
    pub group_handles: Vec<GroupHandle>,
    pub edge_index: EdgeIndex,
}

#[derive(Debug, Clone)]
pub struct PostRemoveEdgeFromGroupsContext {
    pub group_handles: Vec<GroupHandle>,
    pub edge_index: EdgeIndex,
}

#[derive(Debug, Clone)]
pub struct PreRemoveEdgesFromGroupsContext {
    pub group_handles: Vec<GroupHandle>,
    pub edge_indices: Vec<EdgeIndex>,
}

#[derive(Debug, Clone)]
pub struct PostRemoveEdgesFromGroupsContext {
    pub group_handles: Vec<GroupHandle>,
    pub edge_indices: Vec<EdgeIndex>,
}

#[cfg_attr(feature = "serde", typetag::serde(tag = "type"))]
#[allow(unused_variables)]
pub trait Plugin: Debug + Send + Sync {
    fn clone_box(&self) -> Box<dyn Plugin>;

    fn initialize(&self, graphrecord: &mut GraphRecord) -> GraphRecordResult<()> {
        Ok(())
    }

    fn finalize(&self, graphrecord: &mut GraphRecord) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_set_schema(
        &self,
        graphrecord: &mut GraphRecord,
        context: PreSetSchemaContext,
    ) -> GraphRecordResult<PreSetSchemaContext> {
        Ok(context)
    }

    fn post_set_schema(&self, graphrecord: &mut GraphRecord) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_freeze_schema(&self, graphrecord: &mut GraphRecord) -> GraphRecordResult<()> {
        Ok(())
    }

    fn post_freeze_schema(&self, graphrecord: &mut GraphRecord) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_unfreeze_schema(&self, graphrecord: &mut GraphRecord) -> GraphRecordResult<()> {
        Ok(())
    }

    fn post_unfreeze_schema(&self, graphrecord: &mut GraphRecord) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_node(
        &self,
        graphrecord: &mut GraphRecord,
        context: PreAddNodeContext,
    ) -> GraphRecordResult<PreAddNodeContext> {
        Ok(context)
    }

    fn post_add_node(
        &self,
        graphrecord: &mut GraphRecord,
        context: PostAddNodeContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_node_with_group(
        &self,
        graphrecord: &mut GraphRecord,
        context: PreAddNodeWithGroupContext,
    ) -> GraphRecordResult<PreAddNodeWithGroupContext> {
        Ok(context)
    }

    fn post_add_node_with_group(
        &self,
        graphrecord: &mut GraphRecord,
        context: PostAddNodeWithGroupContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_node_with_groups(
        &self,
        graphrecord: &mut GraphRecord,
        context: PreAddNodeWithGroupsContext,
    ) -> GraphRecordResult<PreAddNodeWithGroupsContext> {
        Ok(context)
    }

    fn post_add_node_with_groups(
        &self,
        graphrecord: &mut GraphRecord,
        context: PostAddNodeWithGroupsContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_remove_node(
        &self,
        graphrecord: &mut GraphRecord,
        context: PreRemoveNodeContext,
    ) -> GraphRecordResult<PreRemoveNodeContext> {
        Ok(context)
    }

    fn post_remove_node(
        &self,
        graphrecord: &mut GraphRecord,
        context: PostRemoveNodeContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_nodes(
        &self,
        graphrecord: &mut GraphRecord,
        context: PreAddNodesContext,
    ) -> GraphRecordResult<PreAddNodesContext> {
        Ok(context)
    }

    fn post_add_nodes(
        &self,
        graphrecord: &mut GraphRecord,
        context: PostAddNodesContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_nodes_with_group(
        &self,
        graphrecord: &mut GraphRecord,
        context: PreAddNodesWithGroupContext,
    ) -> GraphRecordResult<PreAddNodesWithGroupContext> {
        Ok(context)
    }

    fn post_add_nodes_with_group(
        &self,
        graphrecord: &mut GraphRecord,
        context: PostAddNodesWithGroupContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_nodes_with_groups(
        &self,
        graphrecord: &mut GraphRecord,
        context: PreAddNodesWithGroupsContext,
    ) -> GraphRecordResult<PreAddNodesWithGroupsContext> {
        Ok(context)
    }

    fn post_add_nodes_with_groups(
        &self,
        graphrecord: &mut GraphRecord,
        context: PostAddNodesWithGroupsContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_nodes_dataframes(
        &self,
        graphrecord: &mut GraphRecord,
        context: PreAddNodesDataframesContext,
    ) -> GraphRecordResult<PreAddNodesDataframesContext> {
        Ok(context)
    }

    fn post_add_nodes_dataframes(
        &self,
        graphrecord: &mut GraphRecord,
        context: PostAddNodesDataframesContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_nodes_dataframes_with_group(
        &self,
        graphrecord: &mut GraphRecord,
        context: PreAddNodesDataframesWithGroupContext,
    ) -> GraphRecordResult<PreAddNodesDataframesWithGroupContext> {
        Ok(context)
    }

    fn post_add_nodes_dataframes_with_group(
        &self,
        graphrecord: &mut GraphRecord,
        context: PostAddNodesDataframesWithGroupContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_nodes_dataframes_with_groups(
        &self,
        graphrecord: &mut GraphRecord,
        context: PreAddNodesDataframesWithGroupsContext,
    ) -> GraphRecordResult<PreAddNodesDataframesWithGroupsContext> {
        Ok(context)
    }

    fn post_add_nodes_dataframes_with_groups(
        &self,
        graphrecord: &mut GraphRecord,
        context: PostAddNodesDataframesWithGroupsContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_edge(
        &self,
        graphrecord: &mut GraphRecord,
        context: PreAddEdgeContext,
    ) -> GraphRecordResult<PreAddEdgeContext> {
        Ok(context)
    }

    fn post_add_edge(
        &self,
        graphrecord: &mut GraphRecord,
        context: PostAddEdgeContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_edge_with_group(
        &self,
        graphrecord: &mut GraphRecord,
        context: PreAddEdgeWithGroupContext,
    ) -> GraphRecordResult<PreAddEdgeWithGroupContext> {
        Ok(context)
    }

    fn post_add_edge_with_group(
        &self,
        graphrecord: &mut GraphRecord,
        context: PostAddEdgeWithGroupContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_edge_with_groups(
        &self,
        graphrecord: &mut GraphRecord,
        context: PreAddEdgeWithGroupsContext,
    ) -> GraphRecordResult<PreAddEdgeWithGroupsContext> {
        Ok(context)
    }

    fn post_add_edge_with_groups(
        &self,
        graphrecord: &mut GraphRecord,
        context: PostAddEdgeWithGroupsContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_remove_edge(
        &self,
        graphrecord: &mut GraphRecord,
        context: PreRemoveEdgeContext,
    ) -> GraphRecordResult<PreRemoveEdgeContext> {
        Ok(context)
    }

    fn post_remove_edge(
        &self,
        graphrecord: &mut GraphRecord,
        context: PostRemoveEdgeContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_edges(
        &self,
        graphrecord: &mut GraphRecord,
        context: PreAddEdgesContext,
    ) -> GraphRecordResult<PreAddEdgesContext> {
        Ok(context)
    }

    fn post_add_edges(
        &self,
        graphrecord: &mut GraphRecord,
        context: PostAddEdgesContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_edges_with_group(
        &self,
        graphrecord: &mut GraphRecord,
        context: PreAddEdgesWithGroupContext,
    ) -> GraphRecordResult<PreAddEdgesWithGroupContext> {
        Ok(context)
    }

    fn post_add_edges_with_group(
        &self,
        graphrecord: &mut GraphRecord,
        context: PostAddEdgesWithGroupContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_edges_with_groups(
        &self,
        graphrecord: &mut GraphRecord,
        context: PreAddEdgesWithGroupsContext,
    ) -> GraphRecordResult<PreAddEdgesWithGroupsContext> {
        Ok(context)
    }

    fn post_add_edges_with_groups(
        &self,
        graphrecord: &mut GraphRecord,
        context: PostAddEdgesWithGroupsContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_edges_dataframes(
        &self,
        graphrecord: &mut GraphRecord,
        context: PreAddEdgesDataframesContext,
    ) -> GraphRecordResult<PreAddEdgesDataframesContext> {
        Ok(context)
    }

    fn post_add_edges_dataframes(
        &self,
        graphrecord: &mut GraphRecord,
        context: PostAddEdgesDataframesContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_edges_dataframes_with_group(
        &self,
        graphrecord: &mut GraphRecord,
        context: PreAddEdgesDataframesWithGroupContext,
    ) -> GraphRecordResult<PreAddEdgesDataframesWithGroupContext> {
        Ok(context)
    }

    fn post_add_edges_dataframes_with_group(
        &self,
        graphrecord: &mut GraphRecord,
        context: PostAddEdgesDataframesWithGroupContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_edges_dataframes_with_groups(
        &self,
        graphrecord: &mut GraphRecord,
        context: PreAddEdgesDataframesWithGroupsContext,
    ) -> GraphRecordResult<PreAddEdgesDataframesWithGroupsContext> {
        Ok(context)
    }

    fn post_add_edges_dataframes_with_groups(
        &self,
        graphrecord: &mut GraphRecord,
        context: PostAddEdgesDataframesWithGroupsContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_group(
        &self,
        graphrecord: &mut GraphRecord,
        context: PreAddGroupContext,
    ) -> GraphRecordResult<PreAddGroupContext> {
        Ok(context)
    }

    fn post_add_group(
        &self,
        graphrecord: &mut GraphRecord,
        context: PostAddGroupContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_remove_group(
        &self,
        graphrecord: &mut GraphRecord,
        context: PreRemoveGroupContext,
    ) -> GraphRecordResult<PreRemoveGroupContext> {
        Ok(context)
    }

    fn post_remove_group(
        &self,
        graphrecord: &mut GraphRecord,
        context: PostRemoveGroupContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_node_to_group(
        &self,
        graphrecord: &mut GraphRecord,
        context: PreAddNodeToGroupContext,
    ) -> GraphRecordResult<PreAddNodeToGroupContext> {
        Ok(context)
    }

    fn post_add_node_to_group(
        &self,
        graphrecord: &mut GraphRecord,
        context: PostAddNodeToGroupContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_node_to_groups(
        &self,
        graphrecord: &mut GraphRecord,
        context: PreAddNodeToGroupsContext,
    ) -> GraphRecordResult<PreAddNodeToGroupsContext> {
        Ok(context)
    }

    fn post_add_node_to_groups(
        &self,
        graphrecord: &mut GraphRecord,
        context: PostAddNodeToGroupsContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_nodes_to_groups(
        &self,
        graphrecord: &mut GraphRecord,
        context: PreAddNodesToGroupsContext,
    ) -> GraphRecordResult<PreAddNodesToGroupsContext> {
        Ok(context)
    }

    fn post_add_nodes_to_groups(
        &self,
        graphrecord: &mut GraphRecord,
        context: PostAddNodesToGroupsContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_edge_to_group(
        &self,
        graphrecord: &mut GraphRecord,
        context: PreAddEdgeToGroupContext,
    ) -> GraphRecordResult<PreAddEdgeToGroupContext> {
        Ok(context)
    }

    fn post_add_edge_to_group(
        &self,
        graphrecord: &mut GraphRecord,
        context: PostAddEdgeToGroupContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_edge_to_groups(
        &self,
        graphrecord: &mut GraphRecord,
        context: PreAddEdgeToGroupsContext,
    ) -> GraphRecordResult<PreAddEdgeToGroupsContext> {
        Ok(context)
    }

    fn post_add_edge_to_groups(
        &self,
        graphrecord: &mut GraphRecord,
        context: PostAddEdgeToGroupsContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_add_edges_to_groups(
        &self,
        graphrecord: &mut GraphRecord,
        context: PreAddEdgesToGroupsContext,
    ) -> GraphRecordResult<PreAddEdgesToGroupsContext> {
        Ok(context)
    }

    fn post_add_edges_to_groups(
        &self,
        graphrecord: &mut GraphRecord,
        context: PostAddEdgesToGroupsContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_remove_node_from_group(
        &self,
        graphrecord: &mut GraphRecord,
        context: PreRemoveNodeFromGroupContext,
    ) -> GraphRecordResult<PreRemoveNodeFromGroupContext> {
        Ok(context)
    }

    fn post_remove_node_from_group(
        &self,
        graphrecord: &mut GraphRecord,
        context: PostRemoveNodeFromGroupContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_remove_node_from_groups(
        &self,
        graphrecord: &mut GraphRecord,
        context: PreRemoveNodeFromGroupsContext,
    ) -> GraphRecordResult<PreRemoveNodeFromGroupsContext> {
        Ok(context)
    }

    fn post_remove_node_from_groups(
        &self,
        graphrecord: &mut GraphRecord,
        context: PostRemoveNodeFromGroupsContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_remove_nodes_from_groups(
        &self,
        graphrecord: &mut GraphRecord,
        context: PreRemoveNodesFromGroupsContext,
    ) -> GraphRecordResult<PreRemoveNodesFromGroupsContext> {
        Ok(context)
    }

    fn post_remove_nodes_from_groups(
        &self,
        graphrecord: &mut GraphRecord,
        context: PostRemoveNodesFromGroupsContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_remove_edge_from_group(
        &self,
        graphrecord: &mut GraphRecord,
        context: PreRemoveEdgeFromGroupContext,
    ) -> GraphRecordResult<PreRemoveEdgeFromGroupContext> {
        Ok(context)
    }

    fn post_remove_edge_from_group(
        &self,
        graphrecord: &mut GraphRecord,
        context: PostRemoveEdgeFromGroupContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_remove_edge_from_groups(
        &self,
        graphrecord: &mut GraphRecord,
        context: PreRemoveEdgeFromGroupsContext,
    ) -> GraphRecordResult<PreRemoveEdgeFromGroupsContext> {
        Ok(context)
    }

    fn post_remove_edge_from_groups(
        &self,
        graphrecord: &mut GraphRecord,
        context: PostRemoveEdgeFromGroupsContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_remove_edges_from_groups(
        &self,
        graphrecord: &mut GraphRecord,
        context: PreRemoveEdgesFromGroupsContext,
    ) -> GraphRecordResult<PreRemoveEdgesFromGroupsContext> {
        Ok(context)
    }

    fn post_remove_edges_from_groups(
        &self,
        graphrecord: &mut GraphRecord,
        context: PostRemoveEdgesFromGroupsContext,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    fn pre_clear(&self, graphrecord: &mut GraphRecord) -> GraphRecordResult<()> {
        Ok(())
    }

    fn post_clear(&self, graphrecord: &mut GraphRecord) -> GraphRecordResult<()> {
        Ok(())
    }
}

impl Clone for Box<dyn Plugin> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}
