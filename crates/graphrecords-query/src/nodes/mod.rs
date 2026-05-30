mod attribute;
mod filter;
mod in_group;
mod scan;

use crate::{BoxedIterator, RootOperand};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult, graphrecord::NodeIndex};
pub(crate) use scan::AllNodes;
use std::sync::Arc;

pub trait NodeOperandContext: 'static + Send + Sync {
    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, &'a NodeIndex>>;
}

#[derive(Clone)]
pub struct NodeOperand {
    context: Arc<dyn NodeOperandContext>,
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
    pub fn new<C: NodeOperandContext>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }
}
