use crate::{
    Bare, BoxedIterator, Failure, FailureKindValue, FailureValue, IndexDomain, IndexValue, Indexed,
    Mask, Multiple, OrderState, QueryResult, Scalar, Series, Single, Unit, ValueDomain,
    element::Preserving,
    error::{argument::Absent, index::DuplicateIndex},
    execution::EvaluationCache,
    expressions::ExpressionHandle,
    operations::{
        Alignment, ArgumentSource, IndexedElementContainer, IndexedElementSource, Keyed, Lookup,
        OnMissing, Prepare, SetSource, SourceDomain,
    },
};
use graphrecords_core::{GraphRecord, graphrecord::AttributeName};
use graphrecords_utils::aliases::{GrHashMap, GrHashSet};
use std::{hash::Hash, sync::Arc};

pub struct PreparedSeriesArgument<'a, I: IndexDomain, V: ValueDomain> {
    graphrecord: &'a GraphRecord,
    addresses: Vec<I::Address>,
    values: Vec<QueryResult<V::Value<'a>>>,
    positions: GrHashMap<I::Address, usize>,
}

impl<'a, I: IndexDomain, V: ValueDomain> PreparedSeriesArgument<'a, I, V> {
    fn new(
        graphrecord: &'a GraphRecord,
        elements: BoxedIterator<'a, (I::Address, QueryResult<V::Value<'a>>)>,
    ) -> QueryResult<Arc<Self>> {
        let mut addresses = Vec::new();
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

            positions.insert(address.clone(), values.len());
            addresses.push(address);
            values.push(outcome);
        }

        Ok(Arc::new(Self {
            graphrecord,
            addresses,
            values,
            positions,
        }))
    }

    fn lookup(
        &self,
        receiver_graphrecord: &GraphRecord,
        address: &I::Address,
    ) -> Lookup<QueryResult<V::Value<'a>>> {
        let identity = I::own_index(&I::index(receiver_graphrecord, address));
        let position = I::resolve(self.graphrecord, &identity, super::LABEL)
            .ok()
            .and_then(|address| self.positions.get(&address));

        match position {
            Some(position) => Lookup::Present(self.values[*position].clone()),
            None => Lookup::Absent(Absent::Uncovered),
        }
    }
}

macro_rules! series_indexed_argument {
    ($value:ty $(, $J:ident)?) => {
        impl<I: IndexDomain, O: OrderState $(, $J: IndexDomain)?> SourceDomain
            for Series<ExpressionHandle<Indexed<I, $value>, Multiple<O>>>
        {
            type ValueDomain = $value;
        }

        impl<I: IndexDomain, O: OrderState $(, $J: IndexDomain)?> Prepare
            for Series<ExpressionHandle<Indexed<I, $value>, Multiple<O>>>
        {
            type Prepared<'a>
                = Arc<PreparedSeriesArgument<'a, I, $value>>
            where
                Self: 'a;

            fn prepare<'a>(
                &'a self,
                _graphrecord: &'a GraphRecord,
                _cache: &'a EvaluationCache,
            ) -> QueryResult<Self::Prepared<'a>> {
                PreparedSeriesArgument::new(self.graphrecord(), self.elements()?)
            }
        }

        impl<I: IndexDomain, O: OrderState $(, $J: IndexDomain)?> ArgumentSource<Keyed<I>, $value>
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

        impl<I: IndexDomain, O: OrderState $(, $J: IndexDomain)?> OnMissing<Keyed<I>>
            for Series<ExpressionHandle<Indexed<I, $value>, Multiple<O>>>
        {
        }

        impl<I: IndexDomain, O: OrderState $(, $J: IndexDomain)?> IndexedElementSource
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
                Box::new((0..prepared.values.len()).map(move |position| {
                    (
                        prepared.addresses[position].clone(),
                        prepared.values[position].clone(),
                    )
                }))
            }
        }

        impl<I: IndexDomain, O: OrderState $(, $J: IndexDomain)?> SetSource<$value>
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
    ($value:ty $(, $J:ident)?) => {
        impl<$($J: IndexDomain)?> SourceDomain for Series<ExpressionHandle<Bare<$value>, Single>> {
            type ValueDomain = $value;
        }

        impl<$($J: IndexDomain)?> Prepare for Series<ExpressionHandle<Bare<$value>, Single>> {
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

        impl<A: Alignment $(, $J: IndexDomain)?> ArgumentSource<A, $value>
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

        impl<A: Alignment $(, $J: IndexDomain)?> OnMissing<A>
            for Series<ExpressionHandle<Bare<$value>, Single>>
        {
        }

        impl<$($J: IndexDomain)?> SetSource<$value> for Series<ExpressionHandle<Bare<$value>, Single>> {
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

series_indexed_argument!(Scalar);
series_indexed_argument!(Mask);
series_indexed_argument!(AttributeName);
series_indexed_argument!(Unit);
series_indexed_argument!(IndexValue<J>, J);
series_indexed_argument!(FailureValue);
series_indexed_argument!(FailureKindValue);

series_bare_argument!(Scalar);
series_bare_argument!(Mask);
series_bare_argument!(AttributeName);
series_bare_argument!(IndexValue<J>, J);
series_bare_argument!(FailureValue);
series_bare_argument!(FailureKindValue);
