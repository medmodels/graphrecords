use crate::{
    AttributeName, EntityDomain, EntityReference, FailureKind, FailureKindValue, IndexDomain,
    IndexValue, Mask, Scalar, ValueType, index::GroupKey,
};
use graphrecords_core::graphrecord::GraphRecordValue;

pub trait GroupingValue: ValueType {
    type Key: GroupKey;

    fn to_group_key(value: &Self::Value<'_>) -> <Self::Key as IndexDomain>::Owned;
}

impl GroupingValue for Scalar {
    type Key = GraphRecordValue;

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
