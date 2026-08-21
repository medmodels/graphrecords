#[cfg(feature = "dynamic")]
use crate::dynamic::DynValue;
use crate::{
    Bare, BareValueDomain, BoxedIterator, Definite, EntityIndexDomain, EntityReference, Failure,
    FailureKindValue, FailureValue, IndexDomain, IndexValue, Indexed, Mask, Multiple, OrderState,
    QueryResult, ReturnValueDomain, Scalar, Series, Single, Unit, ValueDomain,
    element::Preserving,
    error::{argument::Absent, index::DuplicateIndex},
    execution::EvaluationCache,
    expressions::ExpressionHandle,
    operations::{
        Alignment, ArgumentSource, IndexedElementContainer, IndexedElementSource, Keyed, Lookup,
        OnMissing, Prepare, SetSource, SourceDomain,
    },
};
use graphrecords_core::{
    GraphRecord, StateView,
    graphrecord::{AttributeName, StateIdentity},
};
use graphrecords_utils::aliases::{GrHashMap, GrHashSet};
use std::{hash::Hash, iter, sync::Arc};

pub struct PreparedSeriesArgument<'a, I: IndexDomain, V: ValueDomain> {
    graphrecord: &'a GraphRecord,
    state_identity: StateIdentity,
    population: Vec<(I::Address, usize)>,
    values: Vec<QueryResult<V::Value<'a>>>,
    positions: GrHashMap<I::Address, usize>,
}

impl<'a, I: IndexDomain, V: ValueDomain> PreparedSeriesArgument<'a, I, V> {
    fn new(
        graphrecord: &'a GraphRecord,
        receiver_graphrecord: &'a GraphRecord,
        elements: BoxedIterator<'a, (I::Address, QueryResult<V::Value<'a>>)>,
    ) -> QueryResult<Arc<Self>> {
        let state_identity = StateView::of(graphrecord).state_identity();
        let aligned = StateView::of(receiver_graphrecord).state_identity() == state_identity;

        let mut population = Vec::new();
        let mut values = Vec::new();
        let mut positions = GrHashMap::default();

        for (address, outcome) in elements {
            if positions.contains_key(&address) {
                let index = I::index(graphrecord, &address);

                return Err(Failure::new_at::<I, _>(
                    DuplicateIndex::<I>::new(I::own_index(&index)),
                    &index,
                    super::LABEL,
                ));
            }

            let member = if aligned {
                Some(address.clone())
            } else {
                let identity = I::own_index(&I::index(graphrecord, &address));

                I::resolve(receiver_graphrecord, &identity, super::LABEL).ok()
            };

            if let Some(member) = member {
                population.push((member, values.len()));
            }

            positions.insert(address, values.len());
            values.push(outcome);
        }

        Ok(Arc::new(Self {
            graphrecord,
            state_identity,
            population,
            values,
            positions,
        }))
    }

    fn lookup(
        &self,
        receiver_graphrecord: &GraphRecord,
        address: &I::Address,
    ) -> Lookup<QueryResult<V::Value<'a>>> {
        let position =
            if StateView::of(receiver_graphrecord).state_identity() == self.state_identity {
                self.positions.get(address)
            } else {
                let identity = I::own_index(&I::index(receiver_graphrecord, address));

                I::resolve(self.graphrecord, &identity, super::LABEL)
                    .ok()
                    .and_then(|address| self.positions.get(&address))
            };

        match position {
            Some(position) => Lookup::Present(self.values[*position].clone()),
            None => Lookup::Absent(Absent::Uncovered),
        }
    }
}

macro_rules! series_indexed_argument {
    ($value:ty $(, $J:ident: $bound:ident)?) => {
        impl<I: IndexDomain, O: OrderState $(, $J: $bound)?> SourceDomain
            for Series<ExpressionHandle<Indexed<I, $value>, Multiple<O>>>
        {
            type ValueDomain = $value;
        }

        impl<I: IndexDomain, O: OrderState $(, $J: $bound)?> Prepare
            for Series<ExpressionHandle<Indexed<I, $value>, Multiple<O>>>
        {
            type Prepared<'a>
                = Arc<PreparedSeriesArgument<'a, I, $value>>
            where
                Self: 'a;

            fn prepare<'a>(
                &'a self,
                graphrecord: &'a GraphRecord,
                _cache: &'a EvaluationCache,
            ) -> QueryResult<Self::Prepared<'a>> {
                PreparedSeriesArgument::new(self.graphrecord(), graphrecord, self.elements()?)
            }
        }

        impl<I: IndexDomain, O: OrderState $(, $J: $bound)?> ArgumentSource<Keyed<I>, $value>
            for Series<ExpressionHandle<Indexed<I, $value>, Multiple<O>>>
        {
            type Retention = Preserving;

            fn lookup<'a>(
                graphrecord: &'a GraphRecord,
                prepared: &Self::Prepared<'a>,
                address: &<Keyed<I> as Alignment>::Address,
                _label: &'static str,
            ) -> Lookup<QueryResult<<$value as ValueDomain>::Value<'a>>>
            where
                Self: 'a,
            {
                prepared.lookup(graphrecord, address)
            }
        }

        impl<I: IndexDomain, O: OrderState $(, $J: $bound)?> OnMissing<Keyed<I>>
            for Series<ExpressionHandle<Indexed<I, $value>, Multiple<O>>>
        {
        }

        impl<I: IndexDomain, O: OrderState $(, $J: $bound)?> IndexedElementSource
            for Series<ExpressionHandle<Indexed<I, $value>, Multiple<O>>>
        {
            type IndexDomain = I;
            type Arity = Multiple<O>;

            fn elements<'a>(
                prepared: Self::Prepared<'a>,
            ) -> IndexedElementContainer<'a, I, <$value as ValueDomain>::Value<'a>, Multiple<O>>
            where
                Self: 'a,
            {
                Box::new((0..prepared.population.len()).map(move |position| {
                    let member = &prepared.population[position];

                    (member.0.clone(), prepared.values[member.1].clone())
                }))
            }
        }

        impl<I: IndexDomain, O: OrderState $(, $J: $bound)?> SetSource<$value>
            for Series<ExpressionHandle<Indexed<I, $value>, Multiple<O>>>
        {
            fn set<'a>(
                _graphrecord: &'a GraphRecord,
                prepared: Self::Prepared<'a>,
                _label: &'static str,
            ) -> QueryResult<GrHashSet<<$value as ValueDomain>::Value<'a>>>
            where
                Self: 'a,
                <$value as ValueDomain>::Value<'a>: Eq + Hash,
            {
                prepared.values.iter().cloned().collect()
            }
        }
    };
}

macro_rules! series_bare_argument {
    ($value:ty $(, $J:ident: $bound:ident)?) => {
        impl<$($J: $bound)?> SourceDomain for Series<ExpressionHandle<Bare<$value>, Single>> {
            type ValueDomain = $value;
        }

        impl<$($J: $bound)?> Prepare for Series<ExpressionHandle<Bare<$value>, Single>> {
            type Prepared<'a>
                = Option<QueryResult<<$value as ValueDomain>::Value<'a>>>
            where
                Self: 'a;

            fn prepare<'a>(
                &'a self,
                _graphrecord: &'a GraphRecord,
                _cache: &'a EvaluationCache,
            ) -> QueryResult<Self::Prepared<'a>> {
                self.elements()
            }
        }

        impl<A: Alignment $(, $J: $bound)?> ArgumentSource<A, $value>
            for Series<ExpressionHandle<Bare<$value>, Single>>
        {
            type Retention = Preserving;

            fn lookup<'a>(
                _graphrecord: &'a GraphRecord,
                prepared: &Self::Prepared<'a>,
                _address: &A::Address,
                _label: &'static str,
            ) -> Lookup<QueryResult<<$value as ValueDomain>::Value<'a>>>
            where
                Self: 'a,
            {
                match prepared {
                    Some(value) => Lookup::Present(value.clone()),
                    None => Lookup::Absent(Absent::Empty),
                }
            }
        }

        impl<A: Alignment $(, $J: $bound)?> OnMissing<A>
            for Series<ExpressionHandle<Bare<$value>, Single>>
        {
        }

        impl<$($J: $bound)?> SetSource<$value> for Series<ExpressionHandle<Bare<$value>, Single>> {
            fn set<'a>(
                _graphrecord: &'a GraphRecord,
                prepared: Self::Prepared<'a>,
                _label: &'static str,
            ) -> QueryResult<GrHashSet<<$value as ValueDomain>::Value<'a>>>
            where
                Self: 'a,
                <$value as ValueDomain>::Value<'a>: Eq + Hash,
            {
                prepared.into_iter().collect()
            }
        }
    };
}

macro_rules! series_bare_definite_argument {
    ($value:ty $(, $J:ident: $bound:ident)?) => {
        impl<$($J: $bound)?> SourceDomain for Series<ExpressionHandle<Bare<$value>, Definite>> {
            type ValueDomain = $value;
        }

        impl<$($J: $bound)?> Prepare for Series<ExpressionHandle<Bare<$value>, Definite>> {
            type Prepared<'a>
                = QueryResult<<$value as ValueDomain>::Value<'a>>
            where
                Self: 'a;

            fn prepare<'a>(
                &'a self,
                _graphrecord: &'a GraphRecord,
                _cache: &'a EvaluationCache,
            ) -> QueryResult<Self::Prepared<'a>> {
                self.elements()
            }
        }

        impl<A: Alignment $(, $J: $bound)?> ArgumentSource<A, $value>
            for Series<ExpressionHandle<Bare<$value>, Definite>>
        {
            type Retention = Preserving;

            fn lookup<'a>(
                _graphrecord: &'a GraphRecord,
                prepared: &Self::Prepared<'a>,
                _address: &A::Address,
                _label: &'static str,
            ) -> Lookup<QueryResult<<$value as ValueDomain>::Value<'a>>>
            where
                Self: 'a,
            {
                Lookup::Present(prepared.clone())
            }
        }

        impl<$($J: $bound)?> SetSource<$value>
            for Series<ExpressionHandle<Bare<$value>, Definite>>
        {
            fn set<'a>(
                _graphrecord: &'a GraphRecord,
                prepared: Self::Prepared<'a>,
                _label: &'static str,
            ) -> QueryResult<GrHashSet<<$value as ValueDomain>::Value<'a>>>
            where
                Self: 'a,
                <$value as ValueDomain>::Value<'a>: Eq + Hash,
            {
                iter::once(prepared).collect()
            }
        }
    };
}

series_indexed_argument!(Scalar);
series_indexed_argument!(Mask);
series_indexed_argument!(AttributeName);
series_indexed_argument!(Unit);
series_indexed_argument!(IndexValue<J>, J: IndexDomain);
series_indexed_argument!(EntityReference<J>, J: EntityIndexDomain);
series_indexed_argument!(FailureValue);
series_indexed_argument!(FailureKindValue);
#[cfg(feature = "dynamic")]
series_indexed_argument!(DynValue);

series_bare_argument!(Scalar);
series_bare_argument!(Mask);
series_bare_argument!(AttributeName);
series_bare_argument!(IndexValue<J>, J: IndexDomain);
series_bare_argument!(EntityReference<J>, J: EntityIndexDomain);
series_bare_argument!(FailureValue);
series_bare_argument!(FailureKindValue);
#[cfg(feature = "dynamic")]
series_bare_argument!(DynValue);

series_bare_definite_argument!(Scalar);
series_bare_definite_argument!(Mask);
series_bare_definite_argument!(AttributeName);
series_bare_definite_argument!(IndexValue<J>, J: IndexDomain);
series_bare_definite_argument!(EntityReference<J>, J: EntityIndexDomain);
series_bare_definite_argument!(FailureValue);
series_bare_definite_argument!(FailureKindValue);
#[cfg(feature = "dynamic")]
series_bare_definite_argument!(DynValue);

impl<I: IndexDomain, V: ValueDomain + ReturnValueDomain> SourceDomain
    for Series<ExpressionHandle<Indexed<I, V>, Single>>
{
    type ValueDomain = V;
}

impl<I: IndexDomain, V: ValueDomain + ReturnValueDomain> Prepare
    for Series<ExpressionHandle<Indexed<I, V>, Single>>
{
    type Prepared<'a>
        = Option<(I::Address, QueryResult<V::Value<'a>>)>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.elements()
    }
}

impl<I: IndexDomain, V: ValueDomain + ReturnValueDomain> SetSource<V>
    for Series<ExpressionHandle<Indexed<I, V>, Single>>
{
    fn set<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
        _label: &'static str,
    ) -> QueryResult<GrHashSet<V::Value<'a>>>
    where
        Self: 'a,
        V::Value<'a>: Eq + Hash,
    {
        prepared.into_iter().map(|element| element.1).collect()
    }
}

impl<I: IndexDomain, V: ValueDomain + ReturnValueDomain> SourceDomain
    for Series<ExpressionHandle<Indexed<I, V>, Definite>>
{
    type ValueDomain = V;
}

impl<I: IndexDomain, V: ValueDomain + ReturnValueDomain> Prepare
    for Series<ExpressionHandle<Indexed<I, V>, Definite>>
{
    type Prepared<'a>
        = (I::Address, QueryResult<V::Value<'a>>)
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.elements()
    }
}

impl<I: IndexDomain, V: ValueDomain + ReturnValueDomain> SetSource<V>
    for Series<ExpressionHandle<Indexed<I, V>, Definite>>
{
    fn set<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
        _label: &'static str,
    ) -> QueryResult<GrHashSet<V::Value<'a>>>
    where
        Self: 'a,
        V::Value<'a>: Eq + Hash,
    {
        iter::once(prepared.1).collect()
    }
}

impl<O: OrderState, V: BareValueDomain + ReturnValueDomain> SourceDomain
    for Series<ExpressionHandle<Bare<V>, Multiple<O>>>
{
    type ValueDomain = V;
}

impl<O: OrderState, V: BareValueDomain + ReturnValueDomain> Prepare
    for Series<ExpressionHandle<Bare<V>, Multiple<O>>>
{
    type Prepared<'a>
        = Vec<QueryResult<V::Value<'a>>>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self.elements()?.collect())
    }
}

impl<O: OrderState, V: BareValueDomain + ReturnValueDomain> SetSource<V>
    for Series<ExpressionHandle<Bare<V>, Multiple<O>>>
{
    fn set<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
        _label: &'static str,
    ) -> QueryResult<GrHashSet<V::Value<'a>>>
    where
        Self: 'a,
        V::Value<'a>: Eq + Hash,
    {
        prepared.into_iter().collect()
    }
}
