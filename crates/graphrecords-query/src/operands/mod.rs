mod attributes;
mod bool;
mod edges;
mod elements;
mod errors;
mod expanded;
mod group;
mod indices;
mod nodes;
mod references;
mod values;

use crate::{
    BoxedIterator, DuplicateIndex, EntityDomain, Failure, FailureKind, IndexDomain, OperandContext,
    QueryResult,
    execution::EvaluationCache,
    explain::Explanation,
    operations::{
        Absent, AlignableArity, Alignment, ArgumentSource, EnumerableArity, IndexedElementSource,
        Keyed, Lookup, Prepare, PreparedArity, Preserving, SetArity, SetSource,
    },
    optimizer::{Estimate, Estimated, PlanNode, Stats},
    sealed::Sealed,
};
pub use attributes::{
    AttributeOperand, AttributesOperand, BareAttributeOperand, BareAttributesOperand,
    DefiniteAttributeOperand, DefiniteBareAttributeOperand,
};
pub use bool::{
    BareBoolMaskOperand, BareBoolOperand, BoolMaskOperand, BoolOperand, DefiniteBareBoolOperand,
    DefiniteBoolOperand,
};
pub use edges::{AllEdges, DefiniteEdgeOperand, EdgeOperand, EdgesOperand};
pub use elements::{DefiniteElementOperand, ElementOperand, ElementsOperand};
pub use errors::{
    BareFailureKindOperand, BareFailureKindsOperand, BareFailureOperand, BareFailuresOperand,
    DefiniteBareFailureKindOperand, DefiniteBareFailureOperand, DefiniteFailureKindOperand,
    DefiniteFailureOperand, FailureKindOperand, FailureKindsOperand, FailureOperand,
    FailuresOperand,
};
pub use expanded::{
    DuplicateExpandedChildIndex, ExpandedChild, ExpandedIndex, ExpandedIndexOwned,
    ExpandedIndexReference, NoChildIndex,
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{GraphRecordAttribute, GraphRecordValue},
};
use graphrecords_utils::aliases::{GrHashMap, GrHashSet};
pub use group::{
    Bucket, BucketChange, BucketOwned, GroupOperand, InvalidPartitionBucketArity, KeyFailure,
    KeyFailureChange, KeyFailureOwned, Partition, PartitionArity, PartitionBucketParts,
    PartitionBuilder, PartitionClassification, PartitionKeyFailureParts, PartitionOwned,
    PartitionOwnedParts, PartitionParts, PartitionShape, ReturnBucket, ReturnKeyFailure,
    ReturnPartition,
};
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
use std::{hash::Hash, marker::PhantomData, sync::Arc};
pub use values::{
    BareValueOperand, BareValuesOperand, DefiniteBareValueOperand, DefiniteValueOperand,
    ValueOperand, ValuesOperand,
};

pub trait ValueType: 'static {
    type Value<'a>: 'a + Clone
    where
        Self: 'a;

    type Owned: 'static + Clone;

    fn into_owned(value: Self::Value<'_>) -> Self::Owned;

    fn from_owned(owned: &Self::Owned) -> Self::Value<'_>;
}

pub trait ReturnValueType: ValueType {}

pub struct Scalar;
pub struct Mask;
#[derive(Clone)]
pub struct AttributeName;
pub struct Unit;
pub struct IndexValue<I: IndexDomain>(PhantomData<I>);
pub struct EntityReference<E: EntityDomain>(PhantomData<E>);
pub struct FailureValue;
pub struct FailureKindValue;

impl ValueType for Scalar {
    type Owned = GraphRecordValue;
    type Value<'a> = GraphRecordValue;

    fn into_owned(value: Self::Value<'_>) -> Self::Owned {
        value
    }

    fn from_owned(owned: &Self::Owned) -> Self::Value<'_> {
        owned.clone()
    }
}
impl ValueType for Mask {
    type Owned = bool;
    type Value<'a> = bool;

    fn into_owned(value: Self::Value<'_>) -> Self::Owned {
        value
    }

    fn from_owned(owned: &Self::Owned) -> Self::Value<'_> {
        *owned
    }
}
impl ValueType for AttributeName {
    type Owned = GraphRecordAttribute;
    type Value<'a> = GraphRecordAttribute;

    fn into_owned(value: Self::Value<'_>) -> Self::Owned {
        value
    }

    fn from_owned(owned: &Self::Owned) -> Self::Value<'_> {
        owned.clone()
    }
}
impl ValueType for Unit {
    type Owned = ();
    type Value<'a> = ();

    fn into_owned(_value: Self::Value<'_>) -> Self::Owned {}

    fn from_owned(_owned: &Self::Owned) -> Self::Value<'_> {}
}
impl<I: IndexDomain> ValueType for IndexValue<I> {
    type Owned = I::Owned;
    type Value<'a> = I::Owned;

    fn into_owned(value: Self::Value<'_>) -> Self::Owned {
        value
    }

    fn from_owned(owned: &Self::Owned) -> Self::Value<'_> {
        owned.clone()
    }
}
impl<E: EntityDomain> ValueType for EntityReference<E> {
    type Owned = E::Owned;
    type Value<'a> = E::Index<'a>;

    fn into_owned(value: Self::Value<'_>) -> Self::Owned {
        E::to_owned(&value)
    }

    fn from_owned(owned: &Self::Owned) -> Self::Value<'_> {
        E::from_owned(owned)
    }
}
impl ValueType for FailureValue {
    type Owned = Failure;
    type Value<'a> = Failure;

    fn into_owned(value: Self::Value<'_>) -> Self::Owned {
        value
    }

    fn from_owned(owned: &Self::Owned) -> Self::Value<'_> {
        owned.clone()
    }
}
impl ValueType for FailureKindValue {
    type Owned = FailureKind;
    type Value<'a> = FailureKind;

    fn into_owned(value: Self::Value<'_>) -> Self::Owned {
        value
    }

    fn from_owned(owned: &Self::Owned) -> Self::Value<'_> {
        *owned
    }
}

impl ReturnValueType for Scalar {}
impl ReturnValueType for Mask {}
impl ReturnValueType for AttributeName {}
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

        value.map(|()| K::to_owned(&index))
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
    type AfterOrderedExpansion: Arity;
    type AfterUnorderedExpansion: Arity;

    fn map_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        function: impl Fn(X) -> Y + 'a,
    ) -> Self::Container<'a, Y>;

    fn filter_map_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        function: impl Fn(X) -> Option<Y> + 'a,
    ) -> <Self::AfterDrop as Arity>::Container<'a, Y>;

    fn flat_map_ordered_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        function: impl Fn(X) -> Vec<Y> + 'a,
    ) -> <Self::AfterOrderedExpansion as Arity>::Container<'a, Y>;

    fn flat_map_unordered_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        function: impl Fn(X) -> Vec<Y> + 'a,
    ) -> <Self::AfterUnorderedExpansion as Arity>::Container<'a, Y>;
}

pub trait OrderState: Sealed + 'static {}

pub struct Ordered;
pub struct Unordered;

impl Sealed for Ordered {}
impl Sealed for Unordered {}

impl OrderState for Ordered {}

impl OrderState for Unordered {}

pub struct Multiple<O: OrderState>(PhantomData<O>);
pub struct Single;
pub struct Definite;

impl<O: OrderState> Arity for Multiple<O> {
    type AfterDrop = Self;
    type AfterOrderedExpansion = Self;
    type AfterUnorderedExpansion = Multiple<Unordered>;
    type Container<'a, X: 'a> = BoxedIterator<'a, X>;

    fn map_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        function: impl Fn(X) -> Y + 'a,
    ) -> Self::Container<'a, Y> {
        Box::new(container.map(function))
    }

    fn filter_map_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        function: impl Fn(X) -> Option<Y> + 'a,
    ) -> <Self::AfterDrop as Arity>::Container<'a, Y> {
        Box::new(container.filter_map(function))
    }

    fn flat_map_ordered_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        function: impl Fn(X) -> Vec<Y> + 'a,
    ) -> <Self::AfterOrderedExpansion as Arity>::Container<'a, Y> {
        Box::new(container.flat_map(function))
    }

    fn flat_map_unordered_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        function: impl Fn(X) -> Vec<Y> + 'a,
    ) -> <Self::AfterUnorderedExpansion as Arity>::Container<'a, Y> {
        Box::new(container.flat_map(function))
    }
}

impl Arity for Single {
    type AfterDrop = Self;
    type AfterOrderedExpansion = Multiple<Ordered>;
    type AfterUnorderedExpansion = Multiple<Unordered>;
    type Container<'a, X: 'a> = Option<X>;

    fn map_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        function: impl Fn(X) -> Y + 'a,
    ) -> Self::Container<'a, Y> {
        container.map(function)
    }

    fn filter_map_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        function: impl Fn(X) -> Option<Y> + 'a,
    ) -> <Self::AfterDrop as Arity>::Container<'a, Y> {
        container.and_then(function)
    }

    fn flat_map_ordered_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        function: impl Fn(X) -> Vec<Y> + 'a,
    ) -> <Self::AfterOrderedExpansion as Arity>::Container<'a, Y> {
        Box::new(container.into_iter().flat_map(function))
    }

    fn flat_map_unordered_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        function: impl Fn(X) -> Vec<Y> + 'a,
    ) -> <Self::AfterUnorderedExpansion as Arity>::Container<'a, Y> {
        Box::new(container.into_iter().flat_map(function))
    }
}

impl Arity for Definite {
    type AfterDrop = Single;
    type AfterOrderedExpansion = Multiple<Ordered>;
    type AfterUnorderedExpansion = Multiple<Unordered>;
    type Container<'a, X: 'a> = X;

    fn map_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        function: impl Fn(X) -> Y + 'a,
    ) -> Self::Container<'a, Y> {
        function(container)
    }

    fn filter_map_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        function: impl Fn(X) -> Option<Y> + 'a,
    ) -> <Self::AfterDrop as Arity>::Container<'a, Y> {
        function(container)
    }

    fn flat_map_ordered_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        function: impl Fn(X) -> Vec<Y> + 'a,
    ) -> <Self::AfterOrderedExpansion as Arity>::Container<'a, Y> {
        Box::new(function(container).into_iter())
    }

    fn flat_map_unordered_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        function: impl Fn(X) -> Vec<Y> + 'a,
    ) -> <Self::AfterUnorderedExpansion as Arity>::Container<'a, Y> {
        Box::new(function(container).into_iter())
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

pub trait Operand: 'static + Sized + Clone + EvaluateOperand {
    fn context(&self) -> &dyn OperandContext<Self>;

    fn as_plan_node(&self) -> &dyn PlanNode;

    fn from_context(context: Arc<dyn OperandContext<Self>>) -> Self;

    #[must_use]
    fn new<C: OperandContext<Self>>(context: C) -> Self {
        Self::from_context(Arc::new(context))
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

pub struct CheckedIndexedLaneBuilder<'a, I: IndexDomain, V: ValueType> {
    seen: GrHashSet<I::Owned>,
    elements: Vec<(I::Index<'a>, QueryResult<V::Value<'a>>)>,
}

impl<'a, I: IndexDomain, V: ValueType> CheckedIndexedLaneBuilder<'a, I, V> {
    #[must_use]
    pub fn new() -> Self {
        Self {
            seen: GrHashSet::default(),
            elements: Vec::new(),
        }
    }

    pub fn push(
        &mut self,
        index: I::Index<'a>,
        outcome: QueryResult<V::Value<'a>>,
    ) -> QueryResult<()> {
        if !self.seen.insert(I::to_owned(&index)) {
            return Err(Failure::new_at::<I, _>(
                "indexed lane construction",
                DuplicateIndex::<I>::new(I::to_owned(&index)),
                &index,
            ));
        }

        self.elements.push((index, outcome));

        Ok(())
    }

    #[must_use]
    pub fn finish(self) -> BoxedIterator<'a, (I::Index<'a>, QueryResult<V::Value<'a>>)> {
        Box::new(self.elements.into_iter())
    }
}

impl<I: IndexDomain, V: ValueType> Default for CheckedIndexedLaneBuilder<'_, I, V> {
    fn default() -> Self {
        Self::new()
    }
}

pub struct PreparedIndexedMultiple<'a, I: IndexDomain, V: ValueType> {
    elements: Vec<(I::Index<'a>, QueryResult<V::Value<'a>>)>,
    positions: GrHashMap<I::Index<'a>, usize>,
}

impl<S: ElementShape, C: PreparedArity<S>> Prepare for OperandHandle<S, C> {
    type Prepared<'a>
        = C::Prepared<'a>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        C::prepare(self.evaluate(graphrecord, cache)?)
    }
}

impl<S: ElementShape, C: AlignableArity<S, A>, A: Alignment> ArgumentSource<A>
    for OperandHandle<S, C>
{
    type OwnedValue = C::OwnedValue;
    type Retention = C::Retention;
    type Value<'a>
        = C::Value<'a>
    where
        Self: 'a;

    fn to_owned_value(value: &Self::Value<'_>) -> Self::OwnedValue {
        C::to_owned_value(value)
    }

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        address: &A::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<Self::Value<'a>>>
    where
        Self: 'a,
    {
        C::lookup(prepared, address)
    }
}

impl<S: ElementShape, C: EnumerableArity<S, I>, I: IndexDomain> IndexedElementSource<I>
    for OperandHandle<S, C>
{
    type Arity = C;
    type Value<'a>
        = C::Value<'a>
    where
        Self: 'a;

    fn elements<'a>(
        prepared: Self::Prepared<'a>,
    ) -> C::Container<'a, (I::Index<'a>, QueryResult<Self::Value<'a>>)>
    where
        Self: 'a,
    {
        C::elements(prepared)
    }
}

impl<S: ElementShape, C: SetArity<S>> SetSource for OperandHandle<S, C> {
    type Value<'a>
        = C::Value<'a>
    where
        Self: 'a;

    fn set<'a>(prepared: Self::Prepared<'a>) -> QueryResult<GrHashSet<Self::Value<'a>>>
    where
        Self: 'a,
    {
        C::set(prepared)
    }
}

impl<I: IndexDomain, V: ValueType, O: OrderState> PreparedArity<Indexed<I, V>> for Multiple<O> {
    type Prepared<'a>
        = Arc<PreparedIndexedMultiple<'a, I, V>>
    where
        Indexed<I, V>: 'a;

    fn prepare<'a>(
        container: Self::Container<'a, <Indexed<I, V> as ElementShape>::Element<'a>>,
    ) -> QueryResult<Self::Prepared<'a>>
    where
        Indexed<I, V>: 'a,
    {
        let mut elements = Vec::new();
        let mut positions = GrHashMap::default();

        for (index, outcome) in container {
            if positions.contains_key(&index) {
                return Err(Failure::new_at::<I, _>(
                    "operand preparation",
                    DuplicateIndex::<I>::new(I::to_owned(&index)),
                    &index,
                ));
            }

            positions.insert(index.clone(), elements.len());
            elements.push((index, outcome));
        }

        Ok(Arc::new(PreparedIndexedMultiple {
            elements,
            positions,
        }))
    }
}

impl<I: IndexDomain, V: ValueType, O: OrderState> AlignableArity<Indexed<I, V>, Keyed<I>>
    for Multiple<O>
{
    type OwnedValue = V::Owned;
    type Retention = Preserving;
    type Value<'a>
        = V::Value<'a>
    where
        Indexed<I, V>: 'a;

    fn to_owned_value(value: &Self::Value<'_>) -> Self::OwnedValue {
        V::into_owned(value.clone())
    }

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        address: &<Keyed<I> as Alignment>::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<Self::Value<'a>>>
    where
        Indexed<I, V>: 'a,
    {
        match prepared.positions.get(address) {
            Some(position) => Lookup::Present(&prepared.elements[*position].1),
            None => Lookup::Absent(Absent::Uncovered),
        }
    }
}

impl<I: IndexDomain, V: ValueType, O: OrderState> EnumerableArity<Indexed<I, V>, I>
    for Multiple<O>
{
    type Value<'a>
        = V::Value<'a>
    where
        Indexed<I, V>: 'a;

    fn elements<'a>(
        prepared: Self::Prepared<'a>,
    ) -> Self::Container<'a, (I::Index<'a>, QueryResult<Self::Value<'a>>)>
    where
        Indexed<I, V>: 'a,
    {
        let element_count = prepared.elements.len();

        Box::new((0..element_count).map(move |position| prepared.elements[position].clone()))
    }
}

impl<I, V, O> SetArity<Indexed<I, V>> for Multiple<O>
where
    I: IndexDomain,
    V: ValueType,
    O: OrderState,
    for<'a> V::Value<'a>: Eq + Hash,
{
    type Value<'a>
        = V::Value<'a>
    where
        Indexed<I, V>: 'a;

    fn set<'a>(prepared: Self::Prepared<'a>) -> QueryResult<GrHashSet<Self::Value<'a>>>
    where
        Indexed<I, V>: 'a,
    {
        prepared
            .elements
            .iter()
            .map(|element| element.1.clone())
            .collect()
    }
}

impl<I: IndexDomain, V: ValueType> PreparedArity<Indexed<I, V>> for Single {
    type Prepared<'a>
        = Option<(I::Index<'a>, QueryResult<V::Value<'a>>)>
    where
        Indexed<I, V>: 'a;

    fn prepare<'a>(
        container: Self::Container<'a, <Indexed<I, V> as ElementShape>::Element<'a>>,
    ) -> QueryResult<Self::Prepared<'a>>
    where
        Indexed<I, V>: 'a,
    {
        Ok(container)
    }
}

impl<A: Alignment, I: IndexDomain, V: ValueType> AlignableArity<Indexed<I, V>, A> for Single {
    type OwnedValue = V::Owned;
    type Retention = Preserving;
    type Value<'a>
        = V::Value<'a>
    where
        Indexed<I, V>: 'a;

    fn to_owned_value(value: &Self::Value<'_>) -> Self::OwnedValue {
        V::into_owned(value.clone())
    }

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        _address: &A::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<Self::Value<'a>>>
    where
        Indexed<I, V>: 'a,
    {
        match prepared {
            Some((_, outcome)) => Lookup::Present(outcome),
            None => Lookup::Absent(Absent::Empty),
        }
    }
}

impl<I: IndexDomain, V: ValueType> EnumerableArity<Indexed<I, V>, I> for Single {
    type Value<'a>
        = V::Value<'a>
    where
        Indexed<I, V>: 'a;

    fn elements<'a>(
        prepared: Self::Prepared<'a>,
    ) -> Self::Container<'a, (I::Index<'a>, QueryResult<Self::Value<'a>>)>
    where
        Indexed<I, V>: 'a,
    {
        prepared
    }
}

impl<I, V> SetArity<Indexed<I, V>> for Single
where
    I: IndexDomain,
    V: ValueType,
    for<'a> V::Value<'a>: Eq + Hash,
{
    type Value<'a>
        = V::Value<'a>
    where
        Indexed<I, V>: 'a;

    fn set<'a>(prepared: Self::Prepared<'a>) -> QueryResult<GrHashSet<Self::Value<'a>>>
    where
        Indexed<I, V>: 'a,
    {
        match prepared {
            Some(element) => Ok(std::iter::once(element.1?).collect()),
            None => Ok(GrHashSet::default()),
        }
    }
}

impl<I: IndexDomain, V: ValueType> PreparedArity<Indexed<I, V>> for Definite {
    type Prepared<'a>
        = (I::Index<'a>, QueryResult<V::Value<'a>>)
    where
        Indexed<I, V>: 'a;

    fn prepare<'a>(
        container: Self::Container<'a, <Indexed<I, V> as ElementShape>::Element<'a>>,
    ) -> QueryResult<Self::Prepared<'a>>
    where
        Indexed<I, V>: 'a,
    {
        Ok(container)
    }
}

impl<A: Alignment, I: IndexDomain, V: ValueType> AlignableArity<Indexed<I, V>, A> for Definite {
    type OwnedValue = V::Owned;
    type Retention = Preserving;
    type Value<'a>
        = V::Value<'a>
    where
        Indexed<I, V>: 'a;

    fn to_owned_value(value: &Self::Value<'_>) -> Self::OwnedValue {
        V::into_owned(value.clone())
    }

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        _address: &A::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<Self::Value<'a>>>
    where
        Indexed<I, V>: 'a,
    {
        Lookup::Present(&prepared.1)
    }
}

impl<I: IndexDomain, V: ValueType> EnumerableArity<Indexed<I, V>, I> for Definite {
    type Value<'a>
        = V::Value<'a>
    where
        Indexed<I, V>: 'a;

    fn elements<'a>(
        prepared: Self::Prepared<'a>,
    ) -> Self::Container<'a, (I::Index<'a>, QueryResult<Self::Value<'a>>)>
    where
        Indexed<I, V>: 'a,
    {
        prepared
    }
}

impl<I, V> SetArity<Indexed<I, V>> for Definite
where
    I: IndexDomain,
    V: ValueType,
    for<'a> V::Value<'a>: Eq + Hash,
{
    type Value<'a>
        = V::Value<'a>
    where
        Indexed<I, V>: 'a;

    fn set<'a>(prepared: Self::Prepared<'a>) -> QueryResult<GrHashSet<Self::Value<'a>>>
    where
        Indexed<I, V>: 'a,
    {
        Ok(std::iter::once(prepared.1?).collect())
    }
}

impl<V: ValueType, O: OrderState> PreparedArity<Bare<V>> for Multiple<O> {
    type Prepared<'a>
        = Arc<Vec<QueryResult<V::Value<'a>>>>
    where
        Bare<V>: 'a;

    fn prepare<'a>(
        container: Self::Container<'a, <Bare<V> as ElementShape>::Element<'a>>,
    ) -> QueryResult<Self::Prepared<'a>>
    where
        Bare<V>: 'a,
    {
        Ok(Arc::new(container.collect()))
    }
}

impl<V, O> SetArity<Bare<V>> for Multiple<O>
where
    V: ValueType,
    O: OrderState,
    for<'a> V::Value<'a>: Eq + Hash,
{
    type Value<'a>
        = V::Value<'a>
    where
        Bare<V>: 'a;

    fn set<'a>(prepared: Self::Prepared<'a>) -> QueryResult<GrHashSet<Self::Value<'a>>>
    where
        Bare<V>: 'a,
    {
        prepared.iter().cloned().collect()
    }
}

impl<V: ValueType> PreparedArity<Bare<V>> for Single {
    type Prepared<'a>
        = Option<QueryResult<V::Value<'a>>>
    where
        Bare<V>: 'a;

    fn prepare<'a>(
        container: Self::Container<'a, <Bare<V> as ElementShape>::Element<'a>>,
    ) -> QueryResult<Self::Prepared<'a>>
    where
        Bare<V>: 'a,
    {
        Ok(container)
    }
}

impl<A: Alignment, V: ValueType> AlignableArity<Bare<V>, A> for Single {
    type OwnedValue = V::Owned;
    type Retention = Preserving;
    type Value<'a>
        = V::Value<'a>
    where
        Bare<V>: 'a;

    fn to_owned_value(value: &Self::Value<'_>) -> Self::OwnedValue {
        V::into_owned(value.clone())
    }

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        _address: &A::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<Self::Value<'a>>>
    where
        Bare<V>: 'a,
    {
        match prepared {
            Some(value) => Lookup::Present(value),
            None => Lookup::Absent(Absent::Empty),
        }
    }
}

impl<V> SetArity<Bare<V>> for Single
where
    V: ValueType,
    for<'a> V::Value<'a>: Eq + Hash,
{
    type Value<'a>
        = V::Value<'a>
    where
        Bare<V>: 'a;

    fn set<'a>(prepared: Self::Prepared<'a>) -> QueryResult<GrHashSet<Self::Value<'a>>>
    where
        Bare<V>: 'a,
    {
        match prepared {
            Some(outcome) => Ok(std::iter::once(outcome?).collect()),
            None => Ok(GrHashSet::default()),
        }
    }
}

impl<V: ValueType> PreparedArity<Bare<V>> for Definite {
    type Prepared<'a>
        = QueryResult<V::Value<'a>>
    where
        Bare<V>: 'a;

    fn prepare<'a>(
        container: Self::Container<'a, <Bare<V> as ElementShape>::Element<'a>>,
    ) -> QueryResult<Self::Prepared<'a>>
    where
        Bare<V>: 'a,
    {
        Ok(container)
    }
}

impl<A: Alignment, V: ValueType> AlignableArity<Bare<V>, A> for Definite {
    type OwnedValue = V::Owned;
    type Retention = Preserving;
    type Value<'a>
        = V::Value<'a>
    where
        Bare<V>: 'a;

    fn to_owned_value(value: &Self::Value<'_>) -> Self::OwnedValue {
        V::into_owned(value.clone())
    }

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        _address: &A::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<Self::Value<'a>>>
    where
        Bare<V>: 'a,
    {
        Lookup::Present(prepared)
    }
}

impl<V> SetArity<Bare<V>> for Definite
where
    V: ValueType,
    for<'a> V::Value<'a>: Eq + Hash,
{
    type Value<'a>
        = V::Value<'a>
    where
        Bare<V>: 'a;

    fn set<'a>(prepared: Self::Prepared<'a>) -> QueryResult<GrHashSet<Self::Value<'a>>>
    where
        Bare<V>: 'a,
    {
        Ok(std::iter::once(prepared?).collect())
    }
}
