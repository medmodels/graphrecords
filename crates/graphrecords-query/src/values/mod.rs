mod attribute;

use crate::BoxedIterator;
use graphrecords_core::{GraphRecord, errors::GraphRecordResult, graphrecord::GraphRecordValue};
use std::sync::Arc;

#[derive(Clone)]
pub struct MultipleValuesOperand<I> {
    context: Arc<MultipleValuesContext<I>>,
}

impl<I> MultipleValuesOperand<I> {
    pub(crate) fn new<C: Into<MultipleValuesContext<I>>>(context: C) -> Self {
        Self {
            context: Arc::new(context.into()),
        }
    }

    pub fn custom_context<C: MultipleValuesOperandContext<Index = I> + 'static>(
        context: C,
    ) -> Self {
        Self {
            context: Arc::new(MultipleValuesContext::Custom(Box::new(context))),
        }
    }

    pub fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, (&'a I, GraphRecordValue)>> {
        self.context.evaluate(graphrecord)
    }
}

pub trait MultipleValuesOperandContext: Send + Sync {
    type Index;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, (&'a Self::Index, GraphRecordValue)>>;
}

pub(crate) enum MultipleValuesContext<I> {
    RootOperand(Box<dyn MultipleValuesOperandContext<Index = I>>),
    Custom(Box<dyn MultipleValuesOperandContext<Index = I>>),
}

impl<I> MultipleValuesContext<I> {
    pub(crate) fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, (&'a I, GraphRecordValue)>> {
        match self {
            Self::RootOperand(context) | Self::Custom(context) => context.evaluate(graphrecord),
        }
    }
}
