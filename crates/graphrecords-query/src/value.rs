use crate::{EntityDomain, Failure, FailureKind, IndexDomain};
use graphrecords_core::graphrecord::{GraphRecordAttribute, GraphRecordValue};
use std::marker::PhantomData;

pub trait ValueType: 'static {
    type Value<'a>: 'a + Clone
    where
        Self: 'a;

    type Owned: 'static + Clone;

    fn into_owned(value: Self::Value<'_>) -> Self::Owned;

    fn from_owned(owned: &Self::Owned) -> Self::Value<'_>;
}

pub trait BareValueType: ValueType {}

pub trait ReturnValueType: ValueType {}

pub struct Scalar;
pub struct Mask;
#[derive(Clone)]
pub struct AttributeName;
pub struct Unit;
pub struct IndexValue<I: IndexDomain>(PhantomData<I>);
pub struct EntityReference<E: EntityDomain>(PhantomData<E>);
pub struct FailureValue;
pub struct FailureKindValue;

impl ValueType for Scalar {
    type Owned = GraphRecordValue;
    type Value<'a> = GraphRecordValue;

    fn into_owned(value: Self::Value<'_>) -> Self::Owned {
        value
    }

    fn from_owned(owned: &Self::Owned) -> Self::Value<'_> {
        owned.clone()
    }
}
impl ValueType for Mask {
    type Owned = bool;
    type Value<'a> = bool;

    fn into_owned(value: Self::Value<'_>) -> Self::Owned {
        value
    }

    fn from_owned(owned: &Self::Owned) -> Self::Value<'_> {
        *owned
    }
}
impl ValueType for AttributeName {
    type Owned = GraphRecordAttribute;
    type Value<'a> = GraphRecordAttribute;

    fn into_owned(value: Self::Value<'_>) -> Self::Owned {
        value
    }

    fn from_owned(owned: &Self::Owned) -> Self::Value<'_> {
        owned.clone()
    }
}
impl ValueType for Unit {
    type Owned = ();
    type Value<'a> = ();

    fn into_owned(_value: Self::Value<'_>) -> Self::Owned {}

    fn from_owned(_owned: &Self::Owned) -> Self::Value<'_> {}
}
impl<I: IndexDomain> ValueType for IndexValue<I> {
    type Owned = I::Owned;
    type Value<'a> = I::Owned;

    fn into_owned(value: Self::Value<'_>) -> Self::Owned {
        value
    }

    fn from_owned(owned: &Self::Owned) -> Self::Value<'_> {
        owned.clone()
    }
}
impl<E: EntityDomain> ValueType for EntityReference<E> {
    type Owned = E::Owned;
    type Value<'a> = E::Index<'a>;

    fn into_owned(value: Self::Value<'_>) -> Self::Owned {
        E::to_owned(&value)
    }

    fn from_owned(owned: &Self::Owned) -> Self::Value<'_> {
        E::from_owned(owned)
    }
}
impl ValueType for FailureValue {
    type Owned = Failure;
    type Value<'a> = Failure;

    fn into_owned(value: Self::Value<'_>) -> Self::Owned {
        value
    }

    fn from_owned(owned: &Self::Owned) -> Self::Value<'_> {
        owned.clone()
    }
}
impl ValueType for FailureKindValue {
    type Owned = FailureKind;
    type Value<'a> = FailureKind;

    fn into_owned(value: Self::Value<'_>) -> Self::Owned {
        value
    }

    fn from_owned(owned: &Self::Owned) -> Self::Value<'_> {
        *owned
    }
}

impl BareValueType for Scalar {}
impl BareValueType for Mask {}
impl BareValueType for AttributeName {}
impl<I: IndexDomain> BareValueType for IndexValue<I> {}
impl<E: EntityDomain> BareValueType for EntityReference<E> {}
impl BareValueType for FailureValue {}
impl BareValueType for FailureKindValue {}

impl ReturnValueType for Scalar {}
impl ReturnValueType for Mask {}
impl ReturnValueType for AttributeName {}
impl<I: IndexDomain> ReturnValueType for IndexValue<I> {}
impl<E: EntityDomain> ReturnValueType for EntityReference<E> {}
impl ReturnValueType for FailureValue {}
impl ReturnValueType for FailureKindValue {}
