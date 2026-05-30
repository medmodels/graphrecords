mod filter;
mod scan;

use crate::{BoxedIterator, RootOperand, edges::filter::Where};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult, graphrecord::EdgeIndex};
pub(crate) use scan::AllEdges;
use std::sync::Arc;

#[derive(Clone)]
pub struct EdgeOperand {
    context: Arc<EdgeContext>,
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
    pub(crate) fn new<C: Into<EdgeContext>>(context: C) -> Self {
        Self {
            context: Arc::new(context.into()),
        }
    }

    pub fn custom_context<C: EdgeOperandContext + 'static>(context: C) -> Self {
        Self {
            context: Arc::new(EdgeContext::Custom(Box::new(context))),
        }
    }
}

pub trait EdgeOperandContext: Send + Sync {
    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, &'a EdgeIndex>>;
}

pub(crate) enum EdgeContext {
    AllEdges(AllEdges),
    Where(Where),
    Custom(Box<dyn EdgeOperandContext>),
}

impl EdgeContext {
    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, &'a EdgeIndex>> {
        match self {
            Self::AllEdges(context) => context.evaluate(graphrecord),
            Self::Where(context) => context.evaluate(graphrecord),
            Self::Custom(context) => context.evaluate(graphrecord),
        }
    }
}
