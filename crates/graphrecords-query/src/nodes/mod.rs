mod filter;
mod scan;

use crate::{BoxedIterator, RootOperand};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult, graphrecord::NodeIndex};
pub(crate) use scan::AllNodes;
use std::sync::Arc;

#[derive(Clone)]
pub struct NodeOperand {
    context: Arc<NodeContext>,
}

impl RootOperand for NodeOperand {
    type Index<'a> = &'a NodeIndex;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, Self::Index<'a>>> {
        self.context.evaluate(graphrecord)
    }
}

impl NodeOperand {
    pub(crate) fn new<C: Into<NodeContext>>(context: C) -> Self {
        Self {
            context: Arc::new(context.into()),
        }
    }

    pub fn custom_context<C: NodeOperandContext + 'static>(context: C) -> Self {
        Self {
            context: Arc::new(NodeContext::Custom(Box::new(context))),
        }
    }
}

pub trait NodeOperandContext: Send + Sync {
    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, &'a NodeIndex>>;
}

pub(crate) enum NodeContext {
    AllNodes(AllNodes),
    Where(filter::Where),
    Custom(Box<dyn NodeOperandContext>),
}

impl NodeContext {
    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, &'a NodeIndex>> {
        match self {
            Self::AllNodes(context) => context.evaluate(graphrecord),
            Self::Where(context) => context.evaluate(graphrecord),
            Self::Custom(context) => context.evaluate(graphrecord),
        }
    }
}
