use crate::{
    BoxedIterator,
    nodes::{NodeContext, NodeOperandContext},
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult, graphrecord::NodeIndex};

pub struct AllNodes;

impl From<AllNodes> for NodeContext {
    fn from(context: AllNodes) -> Self {
        Self::AllNodes(context)
    }
}

impl NodeOperandContext for AllNodes {
    fn evaluate<'a>(
        &self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, &'a NodeIndex>> {
        Ok(Box::new(graphrecord.node_indices()))
    }
}
