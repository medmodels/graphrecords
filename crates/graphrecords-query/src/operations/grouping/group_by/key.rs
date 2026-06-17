use crate::{
    BoxedIterator, IndexDomain,
    operands::ValuesOperand,
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

impl<I: IndexDomain> GroupKey for ValuesOperand<I> {
    type Key<'a> = GraphRecordValue;
}

impl GroupKey for NodeIndex {
    type Key<'a> = <Self as IndexDomain>::Index<'a>;
}

impl GroupKey for EdgeIndex {
    type Key<'a> = <Self as IndexDomain>::Index<'a>;
}

impl<I: IndexDomain> KeyOperand for ValuesOperand<I> {
    type Subject = I;

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

    fn assignments<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
    ) -> BoxedIterator<'prepared, (I::Index<'a>, S::Key<'a>)>
    where
        Self: 'a,
    {
        S::assignments(&prepared.0)
    }
}
