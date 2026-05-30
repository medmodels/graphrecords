use crate::{BoxedIterator, RootOperand};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult, graphrecord::GraphRecordValue};
use std::sync::Arc;

pub trait MultipleValuesOperandContext: 'static + Send + Sync {
    type Operand: RootOperand;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<
        BoxedIterator<'a, (<Self::Operand as RootOperand>::Index<'a>, GraphRecordValue)>,
    >;
}

#[derive(Clone)]
pub struct MultipleValuesOperand<O: RootOperand> {
    context: Arc<dyn MultipleValuesOperandContext<Operand = O>>,
}

impl<O: RootOperand> MultipleValuesOperand<O> {
    pub fn new<C: MultipleValuesOperandContext<Operand = O>>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }

    pub fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, (<O as RootOperand>::Index<'a>, GraphRecordValue)>>
    {
        self.context.evaluate(graphrecord)
    }
}
