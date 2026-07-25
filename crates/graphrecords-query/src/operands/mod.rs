mod attributes;
mod bool;
mod edges;
mod elements;
mod errors;
mod group;
mod indices;
mod nodes;
mod references;
mod values;

use crate::{
    BoxedIterator, EntityDomain, Failure, FailureKind, IndexDomain, OperandContext, QueryResult,
    ToOwnedValue,
    execution::EvaluationCache,
    explain::Explanation,
    operations::{Absent, Alignment, ArgumentSource, Keyed, Lookup, Prepare, Preserving},
    optimizer::{Estimate, Estimated, PlanNode, Stats},
    sealed::Sealed,
};
pub use attributes::{
    AttributeOperand, AttributesOperand, BareAttributeOperand, BareAttributesOperand,
    BareNestedAttributeOperand, BareNestedAttributesOperand, DefiniteAttributeOperand,
    DefiniteBareAttributeOperand, DefiniteBareNestedAttributeOperand,
    DefiniteNestedAttributeOperand, NestedAttributeOperand, NestedAttributesIterator,
    NestedAttributesOperand,
};
pub use bool::{
    BareBoolMaskOperand, BareBoolOperand, BareNestedBoolMaskOperand, BareNestedBoolOperand,
    BoolMaskOperand, BoolOperand, DefiniteBareBoolOperand, DefiniteBareNestedBoolOperand,
    DefiniteBoolOperand, DefiniteNestedBoolOperand, NestedBoolMaskIterator, NestedBoolMaskOperand,
    NestedBoolOperand,
};
pub use edges::{AllEdges, DefiniteEdgeOperand, EdgeOperand, EdgesOperand};
pub use elements::{DefiniteElementOperand, ElementOperand, ElementsOperand};
pub use errors::{
    BareFailureKindOperand, BareFailureKindsOperand, BareFailureOperand, BareFailuresOperand,
    DefiniteBareFailureKindOperand, DefiniteBareFailureOperand, DefiniteFailureKindOperand,
    DefiniteFailureOperand, FailureKindOperand, FailureKindsOperand, FailureOperand,
    FailuresOperand,
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{GraphRecordAttribute, GraphRecordValue},
};
use graphrecords_utils::aliases::{GrHashMap, GrHashSet};
pub use group::{GroupOperand, Grouped, GroupedIterator, try_partition_by};
pub use indices::{
    BareIndexOperand, BareIndicesOperand, DefiniteBareIndexOperand, DefiniteIndexOperand,
    IndexOperand, IndicesOperand,
};
pub use nodes::{AllNodes, DefiniteNodeOperand, NodeOperand, NodesOperand};
pub use references::{
    BareReferenceOperand, BareReferencesOperand, DefiniteBareReferenceOperand,
    DefiniteReferenceIndexOperand, DefiniteReferenceOperand, ReferenceIndexOperand,
    ReferenceIndicesOperand, ReferenceOperand, ReferencesOperand,
};
use std::{marker::PhantomData, sync::Arc};
pub use values::{
    BareValueOperand, BareValuesOperand, DefiniteBareValueOperand, DefiniteValueOperand,
    ValueOperand, ValuesOperand,
};

pub trait ValueType: 'static {
    type Value<'a>: 'a + Clone + ToOwnedValue
    where
        Self: 'a;
}

pub trait ReturnValueType: ValueType {}

pub struct Scalar;
pub struct Mask;
pub struct AttributeName;
pub struct Unit;
pub struct MaskMap<T: 'static + Clone>(PhantomData<T>);
pub struct AttributeSet;
pub struct IndexValue<I: IndexDomain>(PhantomData<I>);
pub struct EntityReference<E: EntityDomain>(PhantomData<E>);
pub struct FailureValue;
pub struct FailureKindValue;

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
    type Value<'a> = I::Owned;
}
impl<E: EntityDomain> ValueType for EntityReference<E> {
    type Value<'a> = E::Index<'a>;
}
impl ValueType for FailureValue {
    type Value<'a> = Failure;
}
impl ValueType for FailureKindValue {
    type Value<'a> = FailureKind;
}

impl ReturnValueType for Scalar {}
impl ReturnValueType for Mask {}
impl ReturnValueType for AttributeName {}
impl<T: 'static + Clone> ReturnValueType for MaskMap<T> {}
impl ReturnValueType for AttributeSet {}
impl<I: IndexDomain> ReturnValueType for IndexValue<I> {}
impl<E: EntityDomain> ReturnValueType for EntityReference<E> {}
impl ReturnValueType for FailureValue {}
impl ReturnValueType for FailureKindValue {}

pub trait ElementShape: 'static {
    type Element<'a>: 'a;
}

pub trait ReturnShape: ElementShape {
    type ReturnElement<'a>: 'a;

    fn into_return_element(element: Self::Element<'_>) -> Self::ReturnElement<'_>;
}

pub struct Indexed<K: IndexDomain, V: ValueType>(PhantomData<(K, V)>);
pub struct Bare<V: ValueType>(PhantomData<V>);

impl<K: IndexDomain, V: ValueType> ElementShape for Indexed<K, V> {
    type Element<'a> = (K::Index<'a>, QueryResult<V::Value<'a>>);
}
impl<V: ValueType> ElementShape for Bare<V> {
    type Element<'a> = QueryResult<V::Value<'a>>;
}

impl<K: IndexDomain, V: ReturnValueType> ReturnShape for Indexed<K, V> {
    type ReturnElement<'a> = (K::Index<'a>, QueryResult<V::Value<'a>>);

    fn into_return_element(element: Self::Element<'_>) -> Self::ReturnElement<'_> {
        element
    }
}

impl<K: IndexDomain> ReturnShape for Indexed<K, Unit> {
    type ReturnElement<'a> = QueryResult<K::Owned>;

    fn into_return_element(element: Self::Element<'_>) -> Self::ReturnElement<'_> {
        let (index, value) = element;

        value.map(|()| index.to_owned_value())
    }
}

impl<V: ReturnValueType> ReturnShape for Bare<V> {
    type ReturnElement<'a> = QueryResult<V::Value<'a>>;

    fn into_return_element(element: Self::Element<'_>) -> Self::ReturnElement<'_> {
        element
    }
}

pub trait Arity: 'static {
    type Container<'a, X: 'a>: 'a;
    type AfterDrop: Arity;

    fn map_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        function: impl FnMut(X) -> Y + 'a,
    ) -> Self::Container<'a, Y>;

    fn filter_map_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        function: impl FnMut(X) -> Option<Y> + 'a,
    ) -> <Self::AfterDrop as Arity>::Container<'a, Y>;
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
    type AfterDrop = Self;
    type Container<'a, X: 'a> = BoxedIterator<'a, X>;

    fn map_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        function: impl FnMut(X) -> Y + 'a,
    ) -> Self::Container<'a, Y> {
        Box::new(container.map(function))
    }

    fn filter_map_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        function: impl FnMut(X) -> Option<Y> + 'a,
    ) -> <Self::AfterDrop as Arity>::Container<'a, Y> {
        Box::new(container.filter_map(function))
    }
}
impl Arity for Single {
    type AfterDrop = Self;
    type Container<'a, X: 'a> = Option<X>;

    fn map_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        function: impl FnMut(X) -> Y + 'a,
    ) -> Self::Container<'a, Y> {
        container.map(function)
    }

    fn filter_map_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        function: impl FnMut(X) -> Option<Y> + 'a,
    ) -> <Self::AfterDrop as Arity>::Container<'a, Y> {
        container.and_then(function)
    }
}
impl Arity for Definite {
    type AfterDrop = Single;
    type Container<'a, X: 'a> = X;

    fn map_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        mut function: impl FnMut(X) -> Y + 'a,
    ) -> Self::Container<'a, Y> {
        function(container)
    }

    fn filter_map_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        mut function: impl FnMut(X) -> Option<Y> + 'a,
    ) -> <Self::AfterDrop as Arity>::Container<'a, Y> {
        function(container)
    }
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

impl<I: IndexDomain, V: ValueType, O: OrderState> Prepare
    for OperandHandle<Indexed<I, V>, Multiple<O>>
{
    type Prepared<'a>
        = Arc<GrHashMap<I::Index<'a>, QueryResult<V::Value<'a>>>>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(Arc::new(self.evaluate(graphrecord, cache)?.collect()))
    }
}

impl<I: IndexDomain, V: ValueType, O: OrderState> ArgumentSource<Keyed<I>>
    for OperandHandle<Indexed<I, V>, Multiple<O>>
{
    type Retention = Preserving;
    type Value<'a> = V::Value<'a>;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        address: &<Keyed<I> as Alignment>::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<Self::Value<'a>>>
    where
        Self: 'a,
    {
        match prepared.get(address) {
            Some(value) => Lookup::Present(value),
            None => Lookup::Absent(Absent::Uncovered),
        }
    }
}

impl<I: IndexDomain, V: ValueType> Prepare for OperandHandle<Indexed<I, V>, Single> {
    type Prepared<'a>
        = Option<QueryResult<V::Value<'a>>>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self
            .evaluate(graphrecord, cache)?
            .map(|(_index, value)| value))
    }
}

impl<A: Alignment, I: IndexDomain, V: ValueType> ArgumentSource<A>
    for OperandHandle<Indexed<I, V>, Single>
{
    type Retention = Preserving;
    type Value<'a> = V::Value<'a>;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        _address: &A::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<Self::Value<'a>>>
    where
        Self: 'a,
    {
        match prepared {
            Some(value) => Lookup::Present(value),
            None => Lookup::Absent(Absent::Empty),
        }
    }
}

impl<I: IndexDomain, V: ValueType> Prepare for OperandHandle<Indexed<I, V>, Definite> {
    type Prepared<'a>
        = QueryResult<V::Value<'a>>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        let (_index, value) = self.evaluate(graphrecord, cache)?;

        Ok(value)
    }
}

impl<V: ValueType> Prepare for OperandHandle<Bare<V>, Single> {
    type Prepared<'a>
        = Option<QueryResult<V::Value<'a>>>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.evaluate(graphrecord, cache)
    }
}

impl<A: Alignment, V: ValueType> ArgumentSource<A> for OperandHandle<Bare<V>, Single> {
    type Retention = Preserving;
    type Value<'a> = V::Value<'a>;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        _address: &A::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<Self::Value<'a>>>
    where
        Self: 'a,
    {
        match prepared {
            Some(value) => Lookup::Present(value),
            None => Lookup::Absent(Absent::Empty),
        }
    }
}

impl<V: ValueType> Prepare for OperandHandle<Bare<V>, Definite> {
    type Prepared<'a>
        = QueryResult<V::Value<'a>>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.evaluate(graphrecord, cache)
    }
}

impl<A: Alignment, V: ValueType> ArgumentSource<A> for OperandHandle<Bare<V>, Definite> {
    type Retention = Preserving;
    type Value<'a> = V::Value<'a>;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        _address: &A::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<Self::Value<'a>>>
    where
        Self: 'a,
    {
        Lookup::Present(prepared)
    }
}

impl<A: Alignment, I: IndexDomain, V: ValueType> ArgumentSource<A>
    for OperandHandle<Indexed<I, V>, Definite>
{
    type Retention = Preserving;
    type Value<'a> = V::Value<'a>;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        _address: &A::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<Self::Value<'a>>>
    where
        Self: 'a,
    {
        Lookup::Present(prepared)
    }
}
