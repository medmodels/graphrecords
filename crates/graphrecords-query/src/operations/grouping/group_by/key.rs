use crate::{
    BoxedIterator, IndexDomain, Operand, OrderState,
    operands::{ReferenceOperand, ValuesOperand},
    operations::{ArgumentSource, Keyed, MissingPolicy, WithMissing},
    optimizer::{Cardinality, Stats},
    traits::MaybeAbsent,
};
use graphrecords_core::graphrecord::{EdgeIndex, GraphRecordValue, NodeIndex};
use std::hash::Hash;

#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be used as a grouping key",
    note = "a grouping key is an index-aligned operand",
    note = "resolve errors with `.on_error(..)` before using a stream as a key"
)]
pub trait GroupKey: 'static + Clone {
    type Key<'a>: Clone + Eq + Hash
    where
        Self: 'a;
}

pub trait KeyOperand: GroupKey + ArgumentSource<Keyed<Self::Subject>> {
    type Subject: IndexDomain;

    fn distinct_count(&self, stats: &Stats) -> Cardinality;

    fn assignments<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
    ) -> BoxedIterator<'prepared, (<Self::Subject as IndexDomain>::Index<'a>, Self::Key<'a>)>
    where
        Self: 'a;
}

impl<I: IndexDomain, O: OrderState> GroupKey for ValuesOperand<I, O> {
    type Key<'a> = GraphRecordValue;
}

impl GroupKey for NodeIndex {
    type Key<'a> = <Self as IndexDomain>::Index<'a>;
}

impl GroupKey for EdgeIndex {
    type Key<'a> = <Self as IndexDomain>::Index<'a>;
}

impl<I: IndexDomain, O: OrderState> KeyOperand for ValuesOperand<I, O> {
    type Subject = I;

    fn distinct_count(&self, stats: &Stats) -> Cardinality {
        self.context().cost(stats).distinct()
    }

    fn assignments<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
    ) -> BoxedIterator<'prepared, (I::Index<'a>, GraphRecordValue)>
    where
        Self: 'a,
    {
        Box::new(
            prepared.iter().filter_map(|(index, key)| {
                key.as_ref().ok().map(|key| (index.clone(), key.clone()))
            }),
        )
    }
}

impl<K: IndexDomain, E: IndexDomain, O: OrderState> GroupKey for ReferenceOperand<K, E, O> {
    type Key<'a> = E::Index<'a>;
}

impl<K: IndexDomain, E: IndexDomain, O: OrderState> KeyOperand for ReferenceOperand<K, E, O> {
    type Subject = K;

    fn distinct_count(&self, stats: &Stats) -> Cardinality {
        self.context().cost(stats).distinct()
    }

    fn assignments<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
    ) -> BoxedIterator<'prepared, (K::Index<'a>, E::Index<'a>)>
    where
        Self: 'a,
    {
        Box::new(
            prepared.iter().filter_map(|(index, key)| {
                key.as_ref().ok().map(|key| (index.clone(), key.clone()))
            }),
        )
    }
}

impl<I: IndexDomain, S, P> GroupKey for WithMissing<I, S, P>
where
    S: KeyOperand<Subject = I> + MaybeAbsent<I> + Clone,
    P: MissingPolicy<I, S>,
{
    type Key<'a> = S::Key<'a>;
}

impl<I: IndexDomain, S, P> KeyOperand for WithMissing<I, S, P>
where
    S: KeyOperand<Subject = I> + MaybeAbsent<I> + Clone,
    P: MissingPolicy<I, S>,
{
    type Subject = I;

    fn distinct_count(&self, stats: &Stats) -> Cardinality {
        self.inner().distinct_count(stats)
    }

    fn assignments<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
    ) -> BoxedIterator<'prepared, (I::Index<'a>, S::Key<'a>)>
    where
        Self: 'a,
    {
        S::assignments(&prepared.0)
    }
}
