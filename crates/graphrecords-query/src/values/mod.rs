use crate::{
    BoxedIterator, Operand, RootOperand,
    execution::ExecutionContext,
    optimizer::{Cardinality, OptimizeInputs, PlanNode},
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult, graphrecord::GraphRecordValue};
use std::sync::Arc;

pub trait MultipleValuesOperandContext:
    PlanNode + OptimizeInputs<Output = MultipleValuesOperand<Self::Operand>> + Cardinality
{
    type Operand: RootOperand;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<
        BoxedIterator<'a, (<Self::Operand as RootOperand>::Index<'a>, GraphRecordValue)>,
    >;
}

#[derive(Operand)]
#[operand(crate = "crate")]
pub struct MultipleValuesOperand<O: RootOperand> {
    #[operand(context)]
    context: Arc<dyn MultipleValuesOperandContext<Operand = O, Output = Self>>,
}

impl<O: RootOperand> Clone for MultipleValuesOperand<O> {
    fn clone(&self) -> Self {
        Self {
            context: Arc::clone(&self.context),
        }
    }
}

impl<O: RootOperand> MultipleValuesOperand<O> {
    #[must_use]
    pub fn new<C: MultipleValuesOperandContext<Operand = O>>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }

    pub fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<BoxedIterator<'a, (<O as RootOperand>::Index<'a>, GraphRecordValue)>>
    {
        self.context.evaluate(graphrecord, context)
    }
}
