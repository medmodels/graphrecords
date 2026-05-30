mod scan;

use crate::{BoxedIterator, RootOperand};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult, graphrecord::EdgeIndex};
pub(crate) use scan::AllEdges;
use std::sync::Arc;

#[derive(Clone)]
pub struct EdgeOperand {
    context: Arc<EdgeContext>,
}

impl RootOperand for EdgeOperand {
    type Index<'a> = &'a EdgeIndex;
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

    pub(crate) fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, &'a EdgeIndex>> {
        self.context.evaluate(graphrecord)
    }
}

pub trait EdgeOperandContext: Send + Sync {
    fn evaluate<'a>(
        &self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, &'a EdgeIndex>>;
}

pub(crate) enum EdgeContext {
    AllEdges(AllEdges),
    Custom(Box<dyn EdgeOperandContext>),
}

impl EdgeContext {
    pub(crate) fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, &'a EdgeIndex>> {
        match self {
            Self::AllEdges(context) => context.evaluate(graphrecord),
            Self::Custom(context) => context.evaluate(graphrecord),
        }
    }
}
