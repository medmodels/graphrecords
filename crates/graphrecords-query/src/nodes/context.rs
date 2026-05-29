use crate::{BoxedIterator, nodes::Where};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult, graphrecord::NodeIndex};

pub trait NodeOperandContext {
    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, &'a NodeIndex>>;
}

pub(crate) enum NodeContext {
    AllNodes(AllNodes),
    Where(Where),
    Custom(Box<dyn NodeOperandContext>),
}

impl NodeContext {
    pub(super) fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, &'a NodeIndex>> {
        match self {
            NodeContext::AllNodes(context) => context.evaluate(graphrecord),
            NodeContext::Where(context) => context.evaluate(graphrecord),
            NodeContext::Custom(context) => context.evaluate(graphrecord),
        }
    }
}

pub(crate) struct AllNodes;

impl From<AllNodes> for NodeContext {
    fn from(context: AllNodes) -> Self {
        NodeContext::AllNodes(context)
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
