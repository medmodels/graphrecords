use crate::{
    GraphRecord,
    prelude::{Attributes, EdgeIndex, Group, NodeIndex, Schema},
};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

pub enum MutatingPluginResultPayload {
    AddNode(NodeIndex, Attributes),
    AddEdge(NodeIndex, NodeIndex, Attributes),
    AddGroup(Group),
    AddNodeToGroup(NodeIndex, Group),
    AddEdgeToGroup(EdgeIndex, Group),
    RemoveNode(NodeIndex),
    RemoveEdge(EdgeIndex),
    RemoveGroup(Group),
    RemoveNodeFromGroup(NodeIndex, Group),
    RemoveEdgeFromGroup(EdgeIndex, Group),
    SetSchema(Schema),
}

pub type MutatingPluginResult = Vec<MutatingPluginResultPayload>;

#[cfg_attr(feature = "serde", typetag::serde(tag = "type"))]
#[allow(unused)]
pub trait Plugin: Send + Sync + Debug {
    fn clone_box(&self) -> Box<dyn Plugin>;

    fn to_dataframes(&self, graphrecord: &GraphRecord) {}

    fn set_schema(
        &self,
        schema: Schema,
        graphrecord: &GraphRecord,
    ) -> Option<MutatingPluginResult> {
        None
    }

    fn set_schema_unchecked(
        &self,
        schema: Schema,
        graphrecord: &GraphRecord,
    ) -> Option<MutatingPluginResult> {
        None
    }

    fn get_schema(&self, schema: &Schema, graphrecord: &GraphRecord) -> Option<Schema> {
        None
    }

    fn add_node<'a>(
        &self,
        node_index: NodeIndex,
        attributes: Attributes,
        graphrecord: &GraphRecord,
    ) -> Option<MutatingPluginResult> {
        None
    }
}

impl Clone for Box<dyn Plugin> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DefaultPlugin;
