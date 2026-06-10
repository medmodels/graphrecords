use crate::{
    BoxedIterator, Evaluate, Explain, Operand, RootOperand,
    execution::ExecutionContext,
    optimizer::{Cardinality, OptimizeInputs, PlanNode},
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult, graphrecord::GraphRecordValue};
use std::sync::Arc;

pub trait ValuesOperandContext:
    PlanNode + OptimizeInputs<Output = ValuesOperand<Self::Operand>> + Cardinality + Explain
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
pub struct ValuesOperand<O: RootOperand> {
    #[operand(context)]
    context: Arc<dyn ValuesOperandContext<Operand = O, Output = Self>>,
}

impl<O: RootOperand> Clone for ValuesOperand<O> {
    fn clone(&self) -> Self {
        Self {
            context: Arc::clone(&self.context),
        }
    }
}

impl<O: RootOperand> Evaluate for ValuesOperand<O> {
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
    pub fn new<C: ValuesOperandContext<Operand = O>>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }
}

pub trait BareValuesOperandContext:
    PlanNode + OptimizeInputs<Output = BareValuesOperand> + Cardinality + Explain
{
    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<BoxedIterator<'a, GraphRecordValue>>;
}

#[derive(Clone, Operand)]
pub struct BareValuesOperand {
    #[operand(context)]
    context: Arc<dyn BareValuesOperandContext<Output = Self>>,
}

impl Evaluate for BareValuesOperand {
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
    PlanNode + OptimizeInputs<Output = ValueOperand<Self::Operand>> + Explain
{
    type Operand: RootOperand;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<Option<(<Self::Operand as RootOperand>::Index<'a>, GraphRecordValue)>>;
}

#[derive(Operand)]
pub struct ValueOperand<O: RootOperand> {
    #[operand(context)]
    context: Arc<dyn ValueOperandContext<Operand = O, Output = Self>>,
}

impl<O: RootOperand> Clone for ValueOperand<O> {
    fn clone(&self) -> Self {
        Self {
            context: Arc::clone(&self.context),
        }
    }
}

impl<O: RootOperand> Evaluate for ValueOperand<O> {
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
    pub fn new<C: ValueOperandContext<Operand = O>>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }
}

pub trait BareValueOperandContext:
    PlanNode + OptimizeInputs<Output = BareValueOperand> + Explain
{
    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<Option<GraphRecordValue>>;
}

#[derive(Clone, Operand)]
pub struct BareValueOperand {
    #[operand(context)]
    context: Arc<dyn BareValueOperandContext<Output = Self>>,
}

impl Evaluate for BareValueOperand {
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
