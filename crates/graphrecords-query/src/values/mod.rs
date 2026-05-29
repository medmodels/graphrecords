mod context;
mod group_by;

use crate::BoxedIterator;
pub(crate) use context::MultipleValuesContext;
pub use context::MultipleValuesOperandContext;
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
