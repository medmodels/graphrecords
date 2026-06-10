use crate::{
    BoxedIterator, Explain, Operand, RootOperand,
    execution::ExecutionContext,
    optimizer::{Cardinality, OptimizeInputs, PlanNode},
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult};
use std::sync::Arc;

pub trait IndicesOperandContext:
    PlanNode + OptimizeInputs<Output = IndicesOperand<Self::Operand>> + Cardinality + Explain
{
    type Operand: RootOperand;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<BoxedIterator<'a, <Self::Operand as RootOperand>::Index<'a>>>;
}

#[derive(Operand)]
pub struct IndicesOperand<O: RootOperand> {
    #[operand(context)]
    context: Arc<dyn IndicesOperandContext<Operand = O, Output = Self>>,
}

impl<O: RootOperand> IndicesOperand<O> {
    #[must_use]
    pub fn new<C: IndicesOperandContext<Operand = O>>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }

    pub fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<BoxedIterator<'a, <O as RootOperand>::Index<'a>>> {
        self.context.evaluate(graphrecord, context)
    }
}

pub trait IndexOperandContext:
    PlanNode + OptimizeInputs<Output = IndexOperand<Self::Operand>> + Explain
{
    type Operand: RootOperand;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<Option<<Self::Operand as RootOperand>::Index<'a>>>;
}

#[derive(Operand)]
pub struct IndexOperand<O: RootOperand> {
    #[operand(context)]
    context: Arc<dyn IndexOperandContext<Operand = O, Output = Self>>,
}

impl<O: RootOperand> IndexOperand<O> {
    #[must_use]
    pub fn new<C: IndexOperandContext<Operand = O>>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }

    pub fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<Option<<O as RootOperand>::Index<'a>>> {
        self.context.evaluate(graphrecord, context)
    }
}
