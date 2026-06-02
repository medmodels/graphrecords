mod logic;

use crate::{
    BoxedIterator, Operand, RootOperand,
    execution::ExecutionContext,
    optimizer::{OptimizeInputs, PlanNode, Selectivity},
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult};
pub use logic::{AndContext, NotContext, OrContext, XorContext};
use std::sync::Arc;

pub trait BoolMaskOperandContext:
    PlanNode + OptimizeInputs<Output = BoolMaskOperand<Self::Operand>> + Selectivity
{
    type Operand: RootOperand;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<BoxedIterator<'a, (<Self::Operand as RootOperand>::Index<'a>, bool)>>;
}

#[derive(Operand)]
#[operand(crate = "crate")]
pub struct BoolMaskOperand<O: RootOperand> {
    #[operand(context)]
    context: Arc<dyn BoolMaskOperandContext<Operand = O, Output = Self>>,
}

impl<O: RootOperand> Clone for BoolMaskOperand<O> {
    fn clone(&self) -> Self {
        Self {
            context: Arc::clone(&self.context),
        }
    }
}

impl<O: RootOperand> BoolMaskOperand<O> {
    #[must_use]
    pub fn new<C: BoolMaskOperandContext<Operand = O>>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }

    pub fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<BoxedIterator<'a, (<O as RootOperand>::Index<'a>, bool)>> {
        self.context.evaluate(graphrecord, context)
    }
}

pub trait BoolOperandContext:
    PlanNode + OptimizeInputs<Output = BoolOperand<Self::Operand>> + Selectivity
{
    type Operand: RootOperand;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<(<Self::Operand as RootOperand>::Index<'a>, bool)>;
}

#[derive(Operand)]
#[operand(crate = "crate")]
pub struct BoolOperand<O: RootOperand> {
    #[operand(context)]
    context: Arc<dyn BoolOperandContext<Operand = O, Output = Self>>,
}

impl<O: RootOperand> Clone for BoolOperand<O> {
    fn clone(&self) -> Self {
        Self {
            context: Arc::clone(&self.context),
        }
    }
}

impl<O: RootOperand> BoolOperand<O> {
    pub fn new<C: BoolOperandContext<Operand = O>>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }

    pub fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<(<O as RootOperand>::Index<'a>, bool)> {
        self.context.evaluate(graphrecord, context)
    }
}
