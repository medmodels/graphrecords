use crate::{
    EntityDomain, EntityReference, FailureKind, FailureKindValue, IndexDomain, IndexValue, Mask,
    Scalar, ValueDomain, index::GroupKey,
};
use graphrecords_core::graphrecord::{AttributeName, Value};

#[diagnostic::on_unimplemented(
    message = "`{Self}` values cannot be used as grouping keys",
    note = "implement `GroupingValue` for `{Self}` to give it a group key domain"
)]
pub trait GroupingValue: ValueDomain {
    type Key: GroupKey;

    fn to_group_key(value: &Self::Value<'_>) -> <Self::Key as IndexDomain>::Owned;
}

impl GroupingValue for Scalar {
    type Key = Value;

    fn to_group_key(value: &Self::Value<'_>) -> <Self::Key as IndexDomain>::Owned {
        value.clone()
    }
}

impl GroupingValue for Mask {
    type Key = bool;

    fn to_group_key(value: &Self::Value<'_>) -> <Self::Key as IndexDomain>::Owned {
        *value
    }
}

impl GroupingValue for AttributeName {
    type Key = Self;

    fn to_group_key(value: &Self::Value<'_>) -> <Self::Key as IndexDomain>::Owned {
        value.clone()
    }
}

impl GroupingValue for FailureKindValue {
    type Key = FailureKind;

    fn to_group_key(value: &Self::Value<'_>) -> <Self::Key as IndexDomain>::Owned {
        *value
    }
}

impl<I: GroupKey> GroupingValue for IndexValue<I> {
    type Key = I;

    fn to_group_key(value: &Self::Value<'_>) -> <Self::Key as IndexDomain>::Owned {
        value.clone()
    }
}

impl<E: EntityDomain + GroupKey> GroupingValue for EntityReference<E> {
    type Key = E;

    fn to_group_key(value: &Self::Value<'_>) -> <Self::Key as IndexDomain>::Owned {
        E::to_owned(value)
    }
}
