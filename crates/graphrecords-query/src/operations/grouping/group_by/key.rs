use crate::{
    BoxedIterator, FailureKind, IndexDomain, OrderState,
    operands::{FailureKindsOperand, ReferenceOperand, ValuesOperand},
    operations::{ArgumentSource, Keyed, MissingPolicy, WithMissing},
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

    fn assignments<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
    ) -> BoxedIterator<'prepared, (<Self::Subject as IndexDomain>::Index<'a>, Self::Key<'a>)>
    where
        Self: 'a;
}

impl<I: IndexDomain, O: OrderState> GroupKey for ValuesOperand<I, O> {
    type Key<'a> = GraphRecordValue;
}

impl<I: IndexDomain, O: OrderState> GroupKey for FailureKindsOperand<I, O> {
    type Key<'a> = FailureKind;
}

impl GroupKey for NodeIndex {
    type Key<'a> = <Self as IndexDomain>::Index<'a>;
}

impl GroupKey for EdgeIndex {
    type Key<'a> = <Self as IndexDomain>::Index<'a>;
}

impl<I: IndexDomain, O: OrderState> KeyOperand for ValuesOperand<I, O> {
    type Subject = I;

    fn assignments<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
    ) -> BoxedIterator<'prepared, (<Self::Subject as IndexDomain>::Index<'a>, Self::Key<'a>)>
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

impl<I: IndexDomain, O: OrderState> KeyOperand for FailureKindsOperand<I, O> {
    type Subject = I;

    fn assignments<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
    ) -> BoxedIterator<'prepared, (<Self::Subject as IndexDomain>::Index<'a>, Self::Key<'a>)>
    where
        Self: 'a,
    {
        Box::new(
            prepared
                .iter()
                .filter_map(|(index, key)| key.as_ref().ok().map(|key| (index.clone(), *key))),
        )
    }
}

impl<K: IndexDomain, E: IndexDomain, O: OrderState> GroupKey for ReferenceOperand<K, E, O> {
    type Key<'a> = E::Index<'a>;
}

impl<K: IndexDomain, E: IndexDomain, O: OrderState> KeyOperand for ReferenceOperand<K, E, O> {
    type Subject = K;

    fn assignments<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
    ) -> BoxedIterator<'prepared, (<Self::Subject as IndexDomain>::Index<'a>, Self::Key<'a>)>
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

impl<I: IndexDomain, S, P> GroupKey for WithMissing<Keyed<I>, S, P>
where
    S: KeyOperand<Subject = I> + MaybeAbsent<Keyed<I>> + Clone,
    P: MissingPolicy<Keyed<I>, S>,
{
    type Key<'a> = S::Key<'a>;
}

impl<I: IndexDomain, S, P> KeyOperand for WithMissing<Keyed<I>, S, P>
where
    S: KeyOperand<Subject = I> + MaybeAbsent<Keyed<I>> + Clone,
    P: MissingPolicy<Keyed<I>, S>,
{
    type Subject = I;

    fn assignments<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
    ) -> BoxedIterator<'prepared, (<Self::Subject as IndexDomain>::Index<'a>, Self::Key<'a>)>
    where
        Self: 'a,
    {
        S::assignments(&prepared.0)
    }
}
