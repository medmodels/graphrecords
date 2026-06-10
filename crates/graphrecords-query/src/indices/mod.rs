use crate::{
    BoxedIterator, EvaluateContext, EvaluateOperand, Explain, Operand, RootOperand,
    execution::ExecutionContext,
    optimizer::{Cardinality, OptimizeInputs, PlanNode},
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult};
use std::sync::Arc;

pub trait IndicesOperandContext:
    PlanNode
    + OptimizeInputs<Output = IndicesOperand<Self::RootOperand>>
    + Cardinality
    + Explain
    + EvaluateContext<Operand = IndicesOperand<Self::RootOperand>>
{
    type RootOperand: RootOperand;
}

#[derive(Operand)]
pub struct IndicesOperand<O: RootOperand> {
    #[operand(context)]
    context: Arc<dyn IndicesOperandContext<RootOperand = O, Output = Self, Operand = Self>>,
}

impl<O: RootOperand> Clone for IndicesOperand<O> {
    fn clone(&self) -> Self {
        Self {
            context: Arc::clone(&self.context),
        }
    }
}

impl<O: RootOperand> EvaluateOperand for IndicesOperand<O> {
    type ReturnValue<'a> = BoxedIterator<'a, <O as RootOperand>::Index<'a>>;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<Self::ReturnValue<'a>> {
        self.context.evaluate(graphrecord, context)
    }
}

impl<O: RootOperand> IndicesOperand<O> {
    #[must_use]
    pub fn new<C: IndicesOperandContext<RootOperand = O>>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }
}

pub trait IndexOperandContext:
    PlanNode
    + OptimizeInputs<Output = IndexOperand<Self::RootOperand>>
    + Explain
    + EvaluateContext<Operand = IndexOperand<Self::RootOperand>>
{
    type RootOperand: RootOperand;
}

#[derive(Operand)]
pub struct IndexOperand<O: RootOperand> {
    #[operand(context)]
    context: Arc<dyn IndexOperandContext<RootOperand = O, Output = Self, Operand = Self>>,
}

impl<O: RootOperand> Clone for IndexOperand<O> {
    fn clone(&self) -> Self {
        Self {
            context: Arc::clone(&self.context),
        }
    }
}

impl<O: RootOperand> EvaluateOperand for IndexOperand<O> {
    type ReturnValue<'a> = Option<<O as RootOperand>::Index<'a>>;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<Self::ReturnValue<'a>> {
        self.context.evaluate(graphrecord, context)
    }
}

impl<O: RootOperand> IndexOperand<O> {
    #[must_use]
    pub fn new<C: IndexOperandContext<RootOperand = O>>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }
}
