mod logic;

use crate::{
    BoxedIterator, EvaluateContext, EvaluateOperand, Explain, Operand, RootOperand,
    execution::ExecutionContext,
    optimizer::{OptimizeInputs, PlanNode, Selectivity},
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult};
use graphrecords_utils::aliases::GrHashMap;
pub use logic::{AndContext, NotContext, OrContext, XorContext};
use std::sync::Arc;

pub type NestedBoolMaskIterator<'a, O, T> =
    BoxedIterator<'a, (<O as RootOperand>::Index<'a>, GrHashMap<T, bool>)>;

pub trait NestedBoolMaskOperandContext:
    PlanNode
    + OptimizeInputs<Output = NestedBoolMaskOperand<Self::RootOperand, Self::TreeType>>
    + Selectivity
    + Explain
    + EvaluateContext<Operand = NestedBoolMaskOperand<Self::RootOperand, Self::TreeType>>
{
    type RootOperand: RootOperand;
    type TreeType;
}

#[derive(Operand)]
pub struct NestedBoolMaskOperand<O: RootOperand, T: 'static> {
    #[operand(context)]
    context: Arc<
        dyn NestedBoolMaskOperandContext<
                RootOperand = O,
                TreeType = T,
                Output = Self,
                Operand = Self,
            >,
    >,
}

impl<O: RootOperand, T: 'static> Clone for NestedBoolMaskOperand<O, T> {
    fn clone(&self) -> Self {
        Self {
            context: Arc::clone(&self.context),
        }
    }
}

impl<O: RootOperand, T: 'static> EvaluateOperand for NestedBoolMaskOperand<O, T> {
    type ReturnValue<'a> = NestedBoolMaskIterator<'a, O, T>;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<Self::ReturnValue<'a>> {
        self.context.evaluate(graphrecord, context)
    }
}

impl<O: RootOperand, T: 'static> NestedBoolMaskOperand<O, T> {
    #[must_use]
    pub fn new<C: NestedBoolMaskOperandContext<RootOperand = O, TreeType = T>>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }
}

pub trait BoolMaskOperandContext:
    PlanNode
    + OptimizeInputs<Output = BoolMaskOperand<Self::RootOperand>>
    + Selectivity
    + Explain
    + EvaluateContext<Operand = BoolMaskOperand<Self::RootOperand>>
{
    type RootOperand: RootOperand;
}

#[derive(Operand)]
pub struct BoolMaskOperand<O: RootOperand> {
    #[operand(context)]
    context: Arc<dyn BoolMaskOperandContext<RootOperand = O, Output = Self, Operand = Self>>,
}

impl<O: RootOperand> Clone for BoolMaskOperand<O> {
    fn clone(&self) -> Self {
        Self {
            context: Arc::clone(&self.context),
        }
    }
}

impl<O: RootOperand> EvaluateOperand for BoolMaskOperand<O> {
    type ReturnValue<'a> = BoxedIterator<'a, (<O as RootOperand>::Index<'a>, bool)>;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<Self::ReturnValue<'a>> {
        self.context.evaluate(graphrecord, context)
    }
}

impl<O: RootOperand> BoolMaskOperand<O> {
    #[must_use]
    pub fn new<C: BoolMaskOperandContext<RootOperand = O>>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }
}

pub trait BoolOperandContext:
    PlanNode
    + OptimizeInputs<Output = BoolOperand<Self::RootOperand>>
    + Selectivity
    + Explain
    + EvaluateContext<Operand = BoolOperand<Self::RootOperand>>
{
    type RootOperand: RootOperand;
}

#[derive(Operand)]
pub struct BoolOperand<O: RootOperand> {
    #[operand(context)]
    context: Arc<dyn BoolOperandContext<RootOperand = O, Output = Self, Operand = Self>>,
}

impl<O: RootOperand> Clone for BoolOperand<O> {
    fn clone(&self) -> Self {
        Self {
            context: Arc::clone(&self.context),
        }
    }
}

impl<O: RootOperand> EvaluateOperand for BoolOperand<O> {
    type ReturnValue<'a> = (<O as RootOperand>::Index<'a>, bool);

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<Self::ReturnValue<'a>> {
        self.context.evaluate(graphrecord, context)
    }
}

impl<O: RootOperand> BoolOperand<O> {
    pub fn new<C: BoolOperandContext<RootOperand = O>>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }
}
