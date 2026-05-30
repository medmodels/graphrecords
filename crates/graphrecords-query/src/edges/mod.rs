mod attribute;
mod filter;
mod in_group;
mod scan;

use crate::{BoxedIterator, RootOperand};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult, graphrecord::EdgeIndex};
pub(crate) use scan::AllEdges;
use std::sync::Arc;

pub trait EdgeOperandContext: 'static + Send + Sync {
    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, &'a EdgeIndex>>;
}

#[derive(Clone)]
pub struct EdgeOperand {
    context: Arc<dyn EdgeOperandContext>,
}

impl RootOperand for EdgeOperand {
    type Index<'a> = &'a EdgeIndex;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, Self::Index<'a>>> {
        self.context.evaluate(graphrecord)
    }
}

impl EdgeOperand {
    pub fn new<C: EdgeOperandContext>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }
}
