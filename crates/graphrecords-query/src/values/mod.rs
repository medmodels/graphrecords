use crate::{
    BoxedIterator, Operand, RootOperand,
    execution::ExecutionContext,
    optimizer::{Cardinality, OptimizeInputs, PlanNode},
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult, graphrecord::GraphRecordValue};
use std::sync::Arc;

pub trait ValuesOperandContext:
    PlanNode + OptimizeInputs<Output = ValuesOperand<Self::Operand>> + Cardinality
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

impl<O: RootOperand> ValuesOperand<O> {
    #[must_use]
    pub fn new<C: ValuesOperandContext<Operand = O>>(context: C) -> Self {
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

pub trait BareValuesOperandContext:
    PlanNode + OptimizeInputs<Output = BareValuesOperand> + Cardinality
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

impl BareValuesOperand {
    #[must_use]
    pub fn new<C: BareValuesOperandContext>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }

    pub fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<BoxedIterator<'a, GraphRecordValue>> {
        self.context.evaluate(graphrecord, context)
    }
}

pub trait ValueOperandContext:
    PlanNode + OptimizeInputs<Output = ValueOperand<Self::Operand>> + Cardinality
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

impl<O: RootOperand> ValueOperand<O> {
    #[must_use]
    pub fn new<C: ValueOperandContext<Operand = O>>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }

    pub fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<Option<(<O as RootOperand>::Index<'a>, GraphRecordValue)>> {
        self.context.evaluate(graphrecord, context)
    }
}

pub trait BareValueOperandContext:
    PlanNode + OptimizeInputs<Output = BareValueOperand> + Cardinality
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

impl BareValueOperand {
    #[must_use]
    pub fn new<C: BareValueOperandContext>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }

    pub fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<Option<GraphRecordValue>> {
        self.context.evaluate(graphrecord, context)
    }
}
