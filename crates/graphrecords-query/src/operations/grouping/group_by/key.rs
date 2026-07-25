use crate::{
    BoxedIterator, EntityDomain, FailureKind, IndexDomain, OrderState,
    operands::{BoolMaskOperand, FailureKindsOperand, ReferencesOperand, ValuesOperand},
    operations::{ArgumentSource, Keyed, MissingPolicy, WithMissing},
    traits::MaybeAbsent,
};
use graphrecords_core::graphrecord::{EdgeIndex, GraphRecordValue, NodeIndex};

pub trait GroupKey: IndexDomain {}

pub type KeyAssignments<'a, 'prepared, K> = BoxedIterator<
    'prepared,
    (
        <<K as KeyOperand>::Subject as IndexDomain>::Index<'a>,
        <<K as KeyOperand>::Key as IndexDomain>::Index<'a>,
    ),
>;

#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be used as a grouping key",
    note = "a grouping key is an index-aligned operand",
    note = "resolve errors with `.on_error(..)` before using a stream as a key"
)]
pub trait KeyOperand: ArgumentSource<Keyed<Self::Subject>> {
    type Subject: IndexDomain;
    type Key: GroupKey;

    fn assignments<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
    ) -> KeyAssignments<'a, 'prepared, Self>
    where
        Self: 'a;
}

impl GroupKey for GraphRecordValue {}
impl GroupKey for bool {}
impl GroupKey for FailureKind {}
impl GroupKey for NodeIndex {}
impl GroupKey for EdgeIndex {}

impl<I: IndexDomain, O: OrderState> KeyOperand for ValuesOperand<I, O> {
    type Key = GraphRecordValue;
    type Subject = I;

    fn assignments<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
    ) -> KeyAssignments<'a, 'prepared, Self>
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

impl<I: IndexDomain, O: OrderState> KeyOperand for BoolMaskOperand<I, O> {
    type Key = bool;
    type Subject = I;

    fn assignments<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
    ) -> KeyAssignments<'a, 'prepared, Self>
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

impl<I: IndexDomain, O: OrderState> KeyOperand for FailureKindsOperand<I, O> {
    type Key = FailureKind;
    type Subject = I;

    fn assignments<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
    ) -> KeyAssignments<'a, 'prepared, Self>
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

impl<E: EntityDomain + GroupKey, I: IndexDomain, O: OrderState> KeyOperand
    for ReferencesOperand<E, I, O>
{
    type Key = E;
    type Subject = I;

    fn assignments<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
    ) -> KeyAssignments<'a, 'prepared, Self>
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

impl<I: IndexDomain, S, P> KeyOperand for WithMissing<Keyed<I>, S, P>
where
    S: KeyOperand<Subject = I> + MaybeAbsent<Keyed<I>> + Clone,
    P: MissingPolicy<Keyed<I>, S>,
{
    type Key = S::Key;
    type Subject = I;

    fn assignments<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
    ) -> KeyAssignments<'a, 'prepared, Self>
    where
        Self: 'a,
    {
        S::assignments(&prepared.0)
    }
}
