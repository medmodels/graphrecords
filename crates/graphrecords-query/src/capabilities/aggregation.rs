use crate::{
    AttributeName, EntityDomain, EntityReference, FailureKind, FailureKindValue, IndexDomain,
    IndexValue, Mask, ReturnValueType, Scalar, ValueType,
};
use graphrecords_core::graphrecord::{GraphRecordAttribute, GraphRecordValue};
use std::hash::Hash;

pub trait ValueMode: ReturnValueType {
    type Key: Eq + Hash;

    fn mode_key(value: &Self::Value<'_>) -> Self::Key;
}

pub trait ValueUniqueCount: ValueType {
    type Key: Eq + Hash;

    fn unique_count_key(value: &Self::Value<'_>) -> Self::Key;
}

impl ValueMode for Scalar {
    type Key = GraphRecordValue;

    fn mode_key(value: &Self::Value<'_>) -> Self::Key {
        value.clone()
    }
}

impl ValueUniqueCount for Scalar {
    type Key = GraphRecordValue;

    fn unique_count_key(value: &Self::Value<'_>) -> Self::Key {
        value.clone()
    }
}

impl ValueMode for Mask {
    type Key = bool;

    fn mode_key(value: &Self::Value<'_>) -> Self::Key {
        *value
    }
}

impl ValueUniqueCount for Mask {
    type Key = bool;

    fn unique_count_key(value: &Self::Value<'_>) -> Self::Key {
        *value
    }
}

impl ValueMode for AttributeName {
    type Key = GraphRecordAttribute;

    fn mode_key(value: &Self::Value<'_>) -> Self::Key {
        value.clone()
    }
}

impl ValueUniqueCount for AttributeName {
    type Key = GraphRecordAttribute;

    fn unique_count_key(value: &Self::Value<'_>) -> Self::Key {
        value.clone()
    }
}

impl<I: IndexDomain> ValueMode for IndexValue<I> {
    type Key = I::Owned;

    fn mode_key(value: &Self::Value<'_>) -> Self::Key {
        value.clone()
    }
}

impl<I: IndexDomain> ValueUniqueCount for IndexValue<I> {
    type Key = I::Owned;

    fn unique_count_key(value: &Self::Value<'_>) -> Self::Key {
        value.clone()
    }
}

impl<E: EntityDomain> ValueUniqueCount for EntityReference<E> {
    type Key = E::Owned;

    fn unique_count_key(value: &Self::Value<'_>) -> Self::Key {
        E::to_owned(value)
    }
}

impl ValueUniqueCount for FailureKindValue {
    type Key = FailureKind;

    fn unique_count_key(value: &Self::Value<'_>) -> Self::Key {
        *value
    }
}
