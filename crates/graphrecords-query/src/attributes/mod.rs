use crate::{
    BoxedIterator, Evaluate, Explain, Operand, RootOperand,
    execution::ExecutionContext,
    optimizer::{Cardinality, OptimizeInputs, PlanNode},
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
    PlanNode + OptimizeInputs<Output = NestedAttributesOperand<Self::Operand>> + Cardinality + Explain
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

impl<O: RootOperand> Evaluate for NestedAttributesOperand<O> {
    type ReturnValue<'a> = NestedAttributesIterator<'a, O>;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<Self::ReturnValue<'a>> {
        self.context.evaluate(graphrecord, context)
    }
}

impl<O: RootOperand> NestedAttributesOperand<O> {
    pub fn new<C: NestedAttributesOperandContext<Operand = O>>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }
}

pub trait AttributesOperandContext:
    PlanNode + OptimizeInputs<Output = AttributesOperand<Self::Operand>> + Cardinality + Explain
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

impl<O: RootOperand> Evaluate for AttributesOperand<O> {
    type ReturnValue<'a> = BoxedIterator<'a, (<O as RootOperand>::Index<'a>, GraphRecordAttribute)>;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<Self::ReturnValue<'a>> {
        self.context.evaluate(graphrecord, context)
    }
}

impl<O: RootOperand> AttributesOperand<O> {
    pub fn new<C: AttributesOperandContext<Operand = O>>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }
}

pub trait BareAttributesOperandContext:
    PlanNode + OptimizeInputs<Output = BareAttributesOperand> + Cardinality + Explain
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

impl Evaluate for BareAttributesOperand {
    type ReturnValue<'a> = BoxedIterator<'a, GraphRecordAttribute>;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<Self::ReturnValue<'a>> {
        self.context.evaluate(graphrecord, context)
    }
}

impl BareAttributesOperand {
    pub fn new<C: BareAttributesOperandContext>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }
}

pub trait AttributeOperandContext:
    PlanNode + OptimizeInputs<Output = AttributeOperand<Self::Operand>> + Explain
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

impl<O: RootOperand> Evaluate for AttributeOperand<O> {
    type ReturnValue<'a> = Option<(<O as RootOperand>::Index<'a>, GraphRecordAttribute)>;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<Self::ReturnValue<'a>> {
        self.context.evaluate(graphrecord, context)
    }
}

impl<O: RootOperand> AttributeOperand<O> {
    pub fn new<C: AttributeOperandContext<Operand = O>>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }
}

pub trait BareAttributeOperandContext:
    PlanNode + OptimizeInputs<Output = BareAttributeOperand> + Explain
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

impl Evaluate for BareAttributeOperand {
    type ReturnValue<'a> = Option<GraphRecordAttribute>;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<Self::ReturnValue<'a>> {
        self.context.evaluate(graphrecord, context)
    }
}

impl BareAttributeOperand {
    pub fn new<C: BareAttributeOperandContext>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }
}
