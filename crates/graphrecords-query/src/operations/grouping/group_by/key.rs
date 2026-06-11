use crate::{
    BoxedIterator, IndexDomain,
    operands::ValuesOperand,
    operations::{ArgumentSource, MissingPolicy, WithMissing},
    optimizer::PlanIdentity,
    traits::MaybeAbsent,
};
use graphrecords_core::graphrecord::GraphRecordValue;
use std::hash::Hash;

#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be used as a grouping key",
    note = "a grouping key is an index-aligned operand",
    note = "resolve errors with `.on_error(..)` before using a stream as a key"
)]
pub trait GroupKey: 'static + Clone {
    type Key: 'static + Clone + Eq + Hash;
}

pub trait KeyOperand: GroupKey + ArgumentSource<Self::Subject, Value = Self::Key> {
    type Subject: IndexDomain;

    fn assignments<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
    ) -> BoxedIterator<
        'prepared,
        (
            <Self::Subject as IndexDomain>::Index<'a>,
            &'prepared Self::Key,
        ),
    >
    where
        Self: 'a;
}

impl<I: IndexDomain> GroupKey for ValuesOperand<I> {
    type Key = GraphRecordValue;
}

impl<I: IndexDomain> KeyOperand for ValuesOperand<I> {
    type Subject = I;

    fn assignments<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
    ) -> BoxedIterator<'prepared, (I::Index<'a>, &'prepared GraphRecordValue)>
    where
        Self: 'a,
    {
        Box::new(
            prepared
                .iter()
                .filter_map(|(index, key)| key.as_ref().ok().map(|key| (index.clone(), key))),
        )
    }
}

impl<I: IndexDomain, S, P> GroupKey for WithMissing<I, S, P>
where
    S: KeyOperand<Subject = I> + MaybeAbsent<I> + Clone,
    S::Key: Clone + PlanIdentity,
    P: MissingPolicy<I, S::Key>,
{
    type Key = S::Key;
}

impl<I: IndexDomain, S, P> KeyOperand for WithMissing<I, S, P>
where
    S: KeyOperand<Subject = I> + MaybeAbsent<I> + Clone,
    S::Key: Clone + PlanIdentity,
    P: MissingPolicy<I, S::Key>,
{
    type Subject = I;

    fn assignments<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
    ) -> BoxedIterator<'prepared, (I::Index<'a>, &'prepared S::Key)>
    where
        Self: 'a,
    {
        S::assignments(&prepared.0)
    }
}
