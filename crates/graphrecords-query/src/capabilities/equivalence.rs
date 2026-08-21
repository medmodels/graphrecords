use crate::{
    EntityIndexDomain, EntityRef, EntityReference, FailureKind, FailureKindValue, IndexDomain,
    IndexValue, Mask, Scalar, ValueDomain,
};
use graphrecords_core::graphrecord::{AttributeName, ValueView, datatypes::AttributeNameView};
use std::hash::Hash;

pub trait ValueEquivalence: ValueDomain {
    type Key<'a>: Eq + Hash
    where
        Self: 'a;

    fn equivalence_key<'a>(value: &Self::Value<'a>) -> Self::Key<'a>;
}

impl ValueEquivalence for Scalar {
    type Key<'a> = ValueView<'a>;

    fn equivalence_key<'a>(value: &Self::Value<'a>) -> Self::Key<'a> {
        value.clone()
    }
}

impl ValueEquivalence for Mask {
    type Key<'a> = bool;

    fn equivalence_key<'a>(value: &Self::Value<'a>) -> Self::Key<'a> {
        *value
    }
}

impl ValueEquivalence for AttributeName {
    type Key<'a> = AttributeNameView<'a>;

    fn equivalence_key<'a>(value: &Self::Value<'a>) -> Self::Key<'a> {
        value.clone()
    }
}

impl<I: IndexDomain> ValueEquivalence for IndexValue<I> {
    type Key<'a>
        = I::Index<'a>
    where
        Self: 'a;

    fn equivalence_key<'a>(value: &Self::Value<'a>) -> Self::Key<'a> {
        value.clone()
    }
}

impl<E: EntityIndexDomain> ValueEquivalence for EntityReference<E> {
    type Key<'a>
        = EntityRef<'a, E>
    where
        Self: 'a;

    fn equivalence_key<'a>(value: &Self::Value<'a>) -> Self::Key<'a> {
        value.clone()
    }
}

impl ValueEquivalence for FailureKindValue {
    type Key<'a> = FailureKind;

    fn equivalence_key<'a>(value: &Self::Value<'a>) -> Self::Key<'a> {
        *value
    }
}
