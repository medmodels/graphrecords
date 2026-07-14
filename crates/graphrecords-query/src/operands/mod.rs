mod attributes;
mod bool;
mod edges;
mod group;
mod indices;
mod nodes;
mod values;

use crate::{
    BoxedIterator, IndexDomain, OperandContext, QueryResult,
    execution::EvaluationCache,
    explain::Explanation,
    optimizer::{Estimate, Estimated, PlanNode, Stats},
    sealed::Sealed,
};
pub use attributes::{
    AttributeOperand, AttributesOperand, BareAttributeOperand, BareAttributesOperand,
    NestedAttributesIterator, NestedAttributesOperand,
};
pub use bool::{BoolMaskOperand, BoolOperand, NestedBoolMaskIterator, NestedBoolMaskOperand};
pub use edges::{AllEdges, EdgeOperand};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{GraphRecordAttribute, GraphRecordValue},
};
use graphrecords_utils::aliases::{GrHashMap, GrHashSet};
pub use group::{GroupOperand, Grouped, GroupedIterator, try_partition_by};
pub use indices::{IndexOperand, IndicesOperand, ReferenceOperand};
pub use nodes::{AllNodes, NodeOperand};
use std::{marker::PhantomData, sync::Arc};
pub use values::{BareValueOperand, BareValuesOperand, ValueOperand, ValuesOperand};

pub trait ValueType: 'static {
    type Value<'a>: 'a + Clone
    where
        Self: 'a;
}

pub struct Scalar;
pub struct Mask;
pub struct AttributeName;
pub struct Unit;
pub struct MaskMap<T: 'static + Clone>(PhantomData<T>);
pub struct AttributeSet;
pub struct IndexValue<I: IndexDomain>(PhantomData<I>);

impl ValueType for Scalar {
    type Value<'a> = GraphRecordValue;
}
impl ValueType for Mask {
    type Value<'a> = bool;
}
impl ValueType for AttributeName {
    type Value<'a> = GraphRecordAttribute;
}
impl ValueType for Unit {
    type Value<'a> = ();
}
impl<T: 'static + Clone> ValueType for MaskMap<T> {
    type Value<'a> = GrHashMap<T, bool>;
}
impl ValueType for AttributeSet {
    type Value<'a> = GrHashSet<GraphRecordAttribute>;
}
impl<I: IndexDomain> ValueType for IndexValue<I> {
    type Value<'a> = I::Index<'a>;
}

pub trait ElementShape: 'static {
    type Element<'a>: 'a;
}

pub struct Indexed<K: IndexDomain, V: ValueType>(PhantomData<(K, V)>);
pub struct Bare<V: ValueType>(PhantomData<V>);

impl<K: IndexDomain, V: ValueType> ElementShape for Indexed<K, V> {
    type Element<'a> = (K::Index<'a>, QueryResult<V::Value<'a>>);
}
impl<V: ValueType> ElementShape for Bare<V> {
    type Element<'a> = QueryResult<V::Value<'a>>;
}

pub trait Arity: 'static {
    type Container<'a, X: 'a>: 'a;
}

pub trait OrderState: 'static {}

pub struct Ordered;
pub struct Unordered;

impl OrderState for Ordered {}
impl OrderState for Unordered {}

pub struct Multiple<O: OrderState>(PhantomData<O>);
pub struct Single;
pub struct Definite;

impl<O: OrderState> Arity for Multiple<O> {
    type Container<'a, X: 'a> = BoxedIterator<'a, X>;
}
impl Arity for Single {
    type Container<'a, X: 'a> = Option<X>;
}
impl Arity for Definite {
    type Container<'a, X: 'a> = X;
}

pub type Return<'a, S, C> = <C as Arity>::Container<'a, <S as ElementShape>::Element<'a>>;

pub trait EvaluateOperand {
    type ReturnValue<'a>: 'a
    where
        Self: 'a;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::ReturnValue<'a>>;
}

pub trait Operand: 'static + Sized + Clone + EvaluateOperand + Sealed {
    fn context(&self) -> &dyn OperandContext<Self>;

    fn as_plan_node(&self) -> &dyn PlanNode;

    fn from_context(context: Arc<dyn OperandContext<Self>>) -> Self;

    #[must_use]
    fn new<C>(context: C) -> Self
    where
        C: OperandContext<Self>,
    {
        Self::from_context(Arc::new(context))
    }

    fn downcast<T: PlanNode>(&self) -> Option<&T> {
        self.as_plan_node().downcast::<T>()
    }

    fn explain(&self) -> Explanation<'_> {
        Explanation::new(self)
    }
}

pub struct OperandHandle<S: ElementShape, C: Arity> {
    context: Arc<dyn OperandContext<Self>>,
}

impl<S: ElementShape, C: Arity> Clone for OperandHandle<S, C> {
    fn clone(&self) -> Self {
        Self {
            context: Arc::clone(&self.context),
        }
    }
}

impl<S: ElementShape, C: Arity> EvaluateOperand for OperandHandle<S, C> {
    type ReturnValue<'a> = Return<'a, S, C>;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::ReturnValue<'a>> {
        self.context.evaluate(graphrecord, cache)
    }
}

impl<S: ElementShape, C: Arity> Estimated for OperandHandle<S, C> {
    fn estimate(&self, stats: &Stats) -> Estimate {
        self.context().estimate(stats)
    }
}

impl<S: ElementShape, C: Arity> Operand for OperandHandle<S, C> {
    fn context(&self) -> &dyn OperandContext<Self> {
        self.context.as_ref()
    }

    fn as_plan_node(&self) -> &dyn PlanNode {
        self.context.as_ref()
    }

    fn from_context(context: Arc<dyn OperandContext<Self>>) -> Self {
        Self { context }
    }
}

impl<S: ElementShape, C: Arity> Sealed for OperandHandle<S, C> {}
