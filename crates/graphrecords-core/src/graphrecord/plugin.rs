use crate::{
    GraphRecord,
    prelude::{Attributes, EdgeIndex, NodeIndex},
};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Default, Debug, Clone)]
pub struct PluginResponse<'a> {
    pub added_nodes: Option<Vec<(NodeIndex, Attributes)>>,
    pub added_edges: Option<Vec<(NodeIndex, NodeIndex, Attributes)>>,
    pub removed_nodes: Option<Vec<&'a NodeIndex>>,
    pub removed_edges: Option<Vec<&'a EdgeIndex>>,
}

pub trait PluginRoot {}

impl<T: Plugin> PluginRoot for T {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DefaultPlugin;

impl PluginRoot for DefaultPlugin {}

#[allow(unused)]
pub trait Plugin: Send + Sync + Debug {
    fn clone_box(&self) -> Box<dyn Plugin>;

    fn add_node<'a>(
        &self,
        node_index: NodeIndex,
        attributes: Attributes,
        graphrecord: &'a GraphRecord,
    ) -> Option<PluginResponse<'a>> {
        None
    }
}

impl Clone for Box<dyn Plugin> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}
