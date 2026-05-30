use crate::BoxedIterator;
use graphrecords_core::{GraphRecord, errors::GraphRecordResult, graphrecord::GraphRecordValue};
use std::sync::Arc;

pub trait MultipleValuesOperandContext: Send + Sync {
    type Index;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, (&'a Self::Index, GraphRecordValue)>>;
}

#[derive(Clone)]
pub struct MultipleValuesOperand<I> {
    context: Arc<dyn MultipleValuesOperandContext<Index = I>>,
}

impl<I> MultipleValuesOperand<I> {
    pub fn new<C: MultipleValuesOperandContext<Index = I> + 'static>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }

    pub fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, (&'a I, GraphRecordValue)>> {
        self.context.evaluate(graphrecord)
    }
}
