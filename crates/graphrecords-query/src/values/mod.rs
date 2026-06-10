use crate::{
    BoxedIterator, EvaluateContext, EvaluateOperand, Explain, Operand, RootOperand,
    execution::ExecutionContext,
    optimizer::{Cardinality, OptimizeInputs, PlanNode},
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult, graphrecord::GraphRecordValue};
use std::sync::Arc;

pub trait ValuesOperandContext:
    PlanNode
    + OptimizeInputs<Output = ValuesOperand<Self::RootOperand>>
    + Cardinality
    + Explain
    + EvaluateContext<Operand = ValuesOperand<Self::RootOperand>>
{
    type RootOperand: RootOperand;
}

#[derive(Operand)]
pub struct ValuesOperand<O: RootOperand> {
    #[operand(context)]
    context: Arc<dyn ValuesOperandContext<RootOperand = O, Output = Self, Operand = Self>>,
}

impl<O: RootOperand> Clone for ValuesOperand<O> {
    fn clone(&self) -> Self {
        Self {
            context: Arc::clone(&self.context),
        }
    }
}

impl<O: RootOperand> EvaluateOperand for ValuesOperand<O> {
    type ReturnValue<'a> = BoxedIterator<'a, (<O as RootOperand>::Index<'a>, GraphRecordValue)>;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<Self::ReturnValue<'a>> {
        self.context.evaluate(graphrecord, context)
    }
}

impl<O: RootOperand> ValuesOperand<O> {
    #[must_use]
    pub fn new<C: ValuesOperandContext<RootOperand = O>>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }
}

pub trait BareValuesOperandContext:
    PlanNode
    + OptimizeInputs<Output = BareValuesOperand>
    + Cardinality
    + Explain
    + EvaluateContext<Operand = BareValuesOperand>
{
}

#[derive(Clone, Operand)]
pub struct BareValuesOperand {
    #[operand(context)]
    context: Arc<dyn BareValuesOperandContext<Output = Self>>,
}

impl EvaluateOperand for BareValuesOperand {
    type ReturnValue<'a> = BoxedIterator<'a, GraphRecordValue>;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<Self::ReturnValue<'a>> {
        self.context.evaluate(graphrecord, context)
    }
}

impl BareValuesOperand {
    #[must_use]
    pub fn new<C: BareValuesOperandContext>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }
}

pub trait ValueOperandContext:
    PlanNode
    + OptimizeInputs<Output = ValueOperand<Self::RootOperand>>
    + Explain
    + EvaluateContext<Operand = ValueOperand<Self::RootOperand>>
{
    type RootOperand: RootOperand;
}

#[derive(Operand)]
pub struct ValueOperand<O: RootOperand> {
    #[operand(context)]
    context: Arc<dyn ValueOperandContext<RootOperand = O, Output = Self, Operand = Self>>,
}

impl<O: RootOperand> Clone for ValueOperand<O> {
    fn clone(&self) -> Self {
        Self {
            context: Arc::clone(&self.context),
        }
    }
}

impl<O: RootOperand> EvaluateOperand for ValueOperand<O> {
    type ReturnValue<'a> = Option<(<O as RootOperand>::Index<'a>, GraphRecordValue)>;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<Self::ReturnValue<'a>> {
        self.context.evaluate(graphrecord, context)
    }
}

impl<O: RootOperand> ValueOperand<O> {
    #[must_use]
    pub fn new<C: ValueOperandContext<RootOperand = O>>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }
}

pub trait BareValueOperandContext:
    PlanNode
    + OptimizeInputs<Output = BareValueOperand>
    + Explain
    + EvaluateContext<Operand = BareValueOperand>
{
}

#[derive(Clone, Operand)]
pub struct BareValueOperand {
    #[operand(context)]
    context: Arc<dyn BareValueOperandContext<Output = Self>>,
}

impl EvaluateOperand for BareValueOperand {
    type ReturnValue<'a> = Option<GraphRecordValue>;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<Self::ReturnValue<'a>> {
        self.context.evaluate(graphrecord, context)
    }
}

impl BareValueOperand {
    #[must_use]
    pub fn new<C: BareValueOperandContext>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }
}
