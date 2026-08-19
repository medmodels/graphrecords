use crate::{FailureKind, FailureKindValue, IndexDomain, IndexValue, Mask, Scalar, ValueDomain};
use graphrecords_core::graphrecord::{AttributeName, Value};
use std::hash::Hash;

pub trait ValueEquivalence: ValueDomain {
    type Key: Eq + Hash;

    fn equivalence_key(value: &Self::Value<'_>) -> Self::Key;
}

impl ValueEquivalence for Scalar {
    type Key = Value;

    fn equivalence_key(value: &Self::Value<'_>) -> Self::Key {
        value.clone()
    }
}

impl ValueEquivalence for Mask {
    type Key = bool;

    fn equivalence_key(value: &Self::Value<'_>) -> Self::Key {
        *value
    }
}

impl ValueEquivalence for AttributeName {
    type Key = Self;

    fn equivalence_key(value: &Self::Value<'_>) -> Self::Key {
        value.clone()
    }
}

impl<I: IndexDomain> ValueEquivalence for IndexValue<I> {
    type Key = I::Owned;

    fn equivalence_key(value: &Self::Value<'_>) -> Self::Key {
        value.clone()
    }
}

impl ValueEquivalence for FailureKindValue {
    type Key = FailureKind;

    fn equivalence_key(value: &Self::Value<'_>) -> Self::Key {
        *value
    }
}
