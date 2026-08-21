use crate::{
    EntityIndexDomain, EntityReference, FailureKind, FailureKindValue, IndexDomain, IndexValue,
    Mask, Scalar, capabilities::ValueEquivalence,
};
use graphrecords_core::graphrecord::{AttributeName, Value};

#[diagnostic::on_unimplemented(
    message = "`{Self}` values cannot be used as grouping keys",
    note = "implement `ValueGrouping` for `{Self}` to give it a group key domain"
)]
pub trait ValueGrouping: ValueEquivalence {
    type KeyDomain: IndexDomain;

    fn to_group_key(value: &Self::Value<'_>) -> <Self::KeyDomain as IndexDomain>::Owned;
}

impl ValueGrouping for Scalar {
    type KeyDomain = Value;

    fn to_group_key(value: &Self::Value<'_>) -> <Self::KeyDomain as IndexDomain>::Owned {
        Value::from(value.clone())
    }
}

impl ValueGrouping for Mask {
    type KeyDomain = bool;

    fn to_group_key(value: &Self::Value<'_>) -> <Self::KeyDomain as IndexDomain>::Owned {
        *value
    }
}

impl ValueGrouping for AttributeName {
    type KeyDomain = Self;

    fn to_group_key(value: &Self::Value<'_>) -> <Self::KeyDomain as IndexDomain>::Owned {
        Self::from(value.clone())
    }
}

impl ValueGrouping for FailureKindValue {
    type KeyDomain = FailureKind;

    fn to_group_key(value: &Self::Value<'_>) -> <Self::KeyDomain as IndexDomain>::Owned {
        *value
    }
}

impl<I: IndexDomain> ValueGrouping for IndexValue<I> {
    type KeyDomain = I;

    fn to_group_key(value: &Self::Value<'_>) -> <Self::KeyDomain as IndexDomain>::Owned {
        I::own_index(value)
    }
}

impl<E: EntityIndexDomain> ValueGrouping for EntityReference<E> {
    type KeyDomain = E;

    fn to_group_key(value: &Self::Value<'_>) -> <Self::KeyDomain as IndexDomain>::Owned {
        value.clone().into_owned()
    }
}
