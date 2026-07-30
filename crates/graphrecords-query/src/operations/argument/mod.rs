mod collection;
mod constant;

use crate::{
    Arity, Bare, Definite, Diagnostic, ElementShape, EvaluateOperand, Explain, Failure,
    IndexDomain, Indexed, Multiple, OrderState, QueryResult, Single, ValueType,
    element::{ElementEmission, Preserving, Retention},
    error::{
        argument::{Absent, ArgumentAbsent},
        index::DuplicateIndex,
    },
    execution::EvaluationCache,
    operands::OperandHandle,
    optimizer::{Estimated, PlanIdentity, PlanInputs},
};
use graphrecords_core::GraphRecord;
use graphrecords_utils::aliases::{GrHashMap, GrHashSet};
use std::{hash::Hash, marker::PhantomData, sync::Arc};

pub trait Prepare: 'static {
    type Prepared<'a>: Clone + 'a
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>>;
}

pub trait Alignment: 'static {
    type Address<'a>;

    fn raise_at(
        operation: &'static str,
        cause: impl Diagnostic,
        address: &Self::Address<'_>,
    ) -> Box<Failure>;
}

pub struct Keyed<I: IndexDomain>(PhantomData<I>);

impl<I: IndexDomain> Alignment for Keyed<I> {
    type Address<'a> = I::Index<'a>;

    fn raise_at(
        operation: &'static str,
        cause: impl Diagnostic,
        address: &Self::Address<'_>,
    ) -> Box<Failure> {
        Failure::new_at::<I, _>(operation, cause, address)
    }
}

pub struct Unaligned;

impl Alignment for Unaligned {
    type Address<'a> = ();

    fn raise_at(
        operation: &'static str,
        cause: impl Diagnostic,
        _address: &Self::Address<'_>,
    ) -> Box<Failure> {
        Failure::new(operation, cause)
    }
}

pub enum Lookup<'a, W> {
    Present(&'a W),
    Absent(Absent),
}

pub trait ArgumentSource<A: Alignment>:
    Prepare + Explain + PlanIdentity + PlanInputs + Estimated
{
    type Value<'a>: Clone
    where
        Self: 'a;

    type OwnedValue: 'static;

    type Retention: Retention;

    fn to_owned_value(value: &Self::Value<'_>) -> Self::OwnedValue;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        address: &A::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<Self::Value<'a>>>
    where
        Self: 'a;

    fn resolve<'a>(
        prepared: &Self::Prepared<'a>,
        address: &A::Address<'a>,
        label: &'static str,
    ) -> <Self::Retention as ElementEmission>::Step<QueryResult<Self::Value<'a>>>
    where
        Self: 'a,
    {
        match Self::lookup(prepared, address) {
            Lookup::Present(wrapped) => Self::Retention::keep(wrapped.clone()),
            Lookup::Absent(absent) => {
                Self::Retention::absent(|| A::raise_at(label, ArgumentAbsent::new(absent), address))
            }
        }
    }
}

pub type IndexedElementContainer<'a, I, V, C> =
    <C as Arity>::Container<'a, (<I as IndexDomain>::Index<'a>, QueryResult<V>)>;

pub trait IndexedElementSource<I: IndexDomain>: Prepare {
    type Value<'a>: Clone
    where
        Self: 'a;

    type Arity: Arity;

    fn elements<'a>(
        prepared: Self::Prepared<'a>,
    ) -> IndexedElementContainer<'a, I, Self::Value<'a>, Self::Arity>
    where
        Self: 'a;
}

pub trait SetSource: Prepare + Explain + PlanIdentity + PlanInputs + Estimated {
    type Value<'a>: Eq + Hash
    where
        Self: 'a;

    fn set<'a>(prepared: Self::Prepared<'a>) -> QueryResult<GrHashSet<Self::Value<'a>>>
    where
        Self: 'a;
}

pub trait PreparedArity<S: ElementShape>: Arity {
    type Prepared<'a>: Clone + 'a
    where
        S: 'a;

    fn prepare<'a>(
        container: Self::Container<'a, S::Element<'a>>,
    ) -> QueryResult<Self::Prepared<'a>>
    where
        S: 'a;
}

pub trait AlignableArity<S: ElementShape, A: Alignment>: PreparedArity<S> {
    type Value<'a>: Clone
    where
        S: 'a;

    type OwnedValue: 'static;

    type Retention: Retention;

    fn to_owned_value(value: &Self::Value<'_>) -> Self::OwnedValue;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        address: &A::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<Self::Value<'a>>>
    where
        S: 'a;
}

pub trait EnumerableArity<S: ElementShape, I: IndexDomain>: PreparedArity<S> {
    type Value<'a>: Clone
    where
        S: 'a;

    fn elements<'a>(
        prepared: Self::Prepared<'a>,
    ) -> IndexedElementContainer<'a, I, Self::Value<'a>, Self>
    where
        S: 'a;
}

pub trait SetArity<S: ElementShape>: PreparedArity<S> {
    type Value<'a>: Eq + Hash
    where
        S: 'a;

    fn set<'a>(prepared: Self::Prepared<'a>) -> QueryResult<GrHashSet<Self::Value<'a>>>
    where
        S: 'a;
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
