use crate::{
    BoxedIterator, Operand, RootOperand,
    execution::ExecutionContext,
    optimizer::{OptimizeInputs, PlanNode},
};
use graphrecords_core::{
    GraphRecord, errors::GraphRecordResult, graphrecord::GraphRecordAttribute,
};
use graphrecords_utils::aliases::GrHashSet;
use std::sync::Arc;

pub type NestedAttributesIterator<'a, O> = BoxedIterator<
    'a,
    (
        <O as RootOperand>::Index<'a>,
        GrHashSet<GraphRecordAttribute>,
    ),
>;

pub trait NestedAttributesOperandContext:
    PlanNode + OptimizeInputs<Output = NestedAttributesOperand<Self::Operand>>
{
    type Operand: RootOperand;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<NestedAttributesIterator<'a, Self::Operand>>;
}

#[derive(Operand)]
pub struct NestedAttributesOperand<O: RootOperand> {
    #[operand(context)]
    context: Arc<dyn NestedAttributesOperandContext<Operand = O, Output = Self>>,
}

impl<O: RootOperand> Clone for NestedAttributesOperand<O> {
    fn clone(&self) -> Self {
        Self {
            context: Arc::clone(&self.context),
        }
    }
}

impl<O: RootOperand> NestedAttributesOperand<O> {
    pub fn new<C: NestedAttributesOperandContext<Operand = O>>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }

    pub fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<NestedAttributesIterator<'a, O>> {
        self.context.evaluate(graphrecord, context)
    }
}

pub trait AttributesOperandContext:
    PlanNode + OptimizeInputs<Output = AttributesOperand<Self::Operand>>
{
    type Operand: RootOperand;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<
        BoxedIterator<
            'a,
            (
                <Self::Operand as RootOperand>::Index<'a>,
                GraphRecordAttribute,
            ),
        >,
    >;
}

#[derive(Operand)]
pub struct AttributesOperand<O: RootOperand> {
    #[operand(context)]
    context: Arc<dyn AttributesOperandContext<Operand = O, Output = Self>>,
}

impl<O: RootOperand> Clone for AttributesOperand<O> {
    fn clone(&self) -> Self {
        Self {
            context: Arc::clone(&self.context),
        }
    }
}

impl<O: RootOperand> AttributesOperand<O> {
    pub fn new<C: AttributesOperandContext<Operand = O>>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }

    pub fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<BoxedIterator<'a, (<O as RootOperand>::Index<'a>, GraphRecordAttribute)>>
    {
        self.context.evaluate(graphrecord, context)
    }
}

pub trait BareAttributesOperandContext:
    PlanNode + OptimizeInputs<Output = BareAttributesOperand>
{
    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<BoxedIterator<'a, GraphRecordAttribute>>;
}

#[derive(Clone, Operand)]
pub struct BareAttributesOperand {
    #[operand(context)]
    context: Arc<dyn BareAttributesOperandContext<Output = Self>>,
}

impl BareAttributesOperand {
    pub fn new<C: BareAttributesOperandContext>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }

    pub fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<BoxedIterator<'a, GraphRecordAttribute>> {
        self.context.evaluate(graphrecord, context)
    }
}

pub trait AttributeOperandContext:
    PlanNode + OptimizeInputs<Output = AttributeOperand<Self::Operand>>
{
    type Operand: RootOperand;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<
        Option<(
            <Self::Operand as RootOperand>::Index<'a>,
            GraphRecordAttribute,
        )>,
    >;
}

#[derive(Operand)]
pub struct AttributeOperand<O: RootOperand> {
    #[operand(context)]
    context: Arc<dyn AttributeOperandContext<Operand = O, Output = Self>>,
}

impl<O: RootOperand> Clone for AttributeOperand<O> {
    fn clone(&self) -> Self {
        Self {
            context: Arc::clone(&self.context),
        }
    }
}

impl<O: RootOperand> AttributeOperand<O> {
    pub fn new<C: AttributeOperandContext<Operand = O>>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }

    pub fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<Option<(<O as RootOperand>::Index<'a>, GraphRecordAttribute)>> {
        self.context.evaluate(graphrecord, context)
    }
}

pub trait BareAttributeOperandContext:
    PlanNode + OptimizeInputs<Output = BareAttributeOperand>
{
    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<Option<GraphRecordAttribute>>;
}

#[derive(Clone, Operand)]
pub struct BareAttributeOperand {
    #[operand(context)]
    context: Arc<dyn BareAttributeOperandContext<Output = Self>>,
}

impl BareAttributeOperand {
    pub fn new<C: BareAttributeOperandContext>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }

    pub fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<Option<GraphRecordAttribute>> {
        self.context.evaluate(graphrecord, context)
    }
}
