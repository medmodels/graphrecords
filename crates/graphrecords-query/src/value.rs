use crate::{EntityDomain, Failure, FailureKind, IndexDomain};
use graphrecords_core::graphrecord::{AttributeName, Value};
use std::marker::PhantomData;

pub trait ValueDomain: 'static {
    type Owned: 'static + Clone;

    type Value<'a>: 'a + Clone
    where
        Self: 'a;

    fn into_owned(value: Self::Value<'_>) -> Self::Owned;

    fn from_owned(owned: &Self::Owned) -> Self::Value<'_>;
}

pub trait BareValueDomain: ValueDomain {}

pub trait ReturnValueDomain: ValueDomain {}

pub struct Scalar;
pub struct Mask;
pub struct Unit;
pub struct IndexValue<I: IndexDomain>(PhantomData<I>);
pub struct EntityReference<E: EntityDomain>(PhantomData<E>);
pub struct FailureValue;
pub struct FailureKindValue;

impl ValueDomain for Scalar {
    type Owned = Value;
    type Value<'a> = Value;

    fn into_owned(value: Self::Value<'_>) -> Self::Owned {
        value
    }

    fn from_owned(owned: &Self::Owned) -> Self::Value<'_> {
        owned.clone()
    }
}
impl ValueDomain for Mask {
    type Owned = bool;
    type Value<'a> = bool;

    fn into_owned(value: Self::Value<'_>) -> Self::Owned {
        value
    }

    fn from_owned(owned: &Self::Owned) -> Self::Value<'_> {
        *owned
    }
}
impl ValueDomain for AttributeName {
    type Owned = Self;
    type Value<'a> = Self;

    fn into_owned(value: Self::Value<'_>) -> Self::Owned {
        value
    }

    fn from_owned(owned: &Self::Owned) -> Self::Value<'_> {
        owned.clone()
    }
}
impl ValueDomain for Unit {
    type Owned = ();
    type Value<'a> = ();

    fn into_owned(_value: Self::Value<'_>) -> Self::Owned {}

    fn from_owned(_owned: &Self::Owned) -> Self::Value<'_> {}
}
impl<I: IndexDomain> ValueDomain for IndexValue<I> {
    type Owned = I::Owned;
    type Value<'a> = I::Owned;

    fn into_owned(value: Self::Value<'_>) -> Self::Owned {
        value
    }

    fn from_owned(owned: &Self::Owned) -> Self::Value<'_> {
        owned.clone()
    }
}
impl<E: EntityDomain> ValueDomain for EntityReference<E> {
    type Owned = E::Owned;
    type Value<'a> = E::Index<'a>;

    fn into_owned(value: Self::Value<'_>) -> Self::Owned {
        E::to_owned(&value)
    }

    fn from_owned(owned: &Self::Owned) -> Self::Value<'_> {
        E::from_owned(owned)
    }
}
impl ValueDomain for FailureValue {
    type Owned = Failure;
    type Value<'a> = Failure;

    fn into_owned(value: Self::Value<'_>) -> Self::Owned {
        value
    }

    fn from_owned(owned: &Self::Owned) -> Self::Value<'_> {
        owned.clone()
    }
}
impl ValueDomain for FailureKindValue {
    type Owned = FailureKind;
    type Value<'a> = FailureKind;

    fn into_owned(value: Self::Value<'_>) -> Self::Owned {
        value
    }

    fn from_owned(owned: &Self::Owned) -> Self::Value<'_> {
        *owned
    }
}

impl BareValueDomain for Scalar {}
impl BareValueDomain for Mask {}
impl BareValueDomain for AttributeName {}
impl<I: IndexDomain> BareValueDomain for IndexValue<I> {}
impl<E: EntityDomain> BareValueDomain for EntityReference<E> {}
impl BareValueDomain for FailureValue {}
impl BareValueDomain for FailureKindValue {}

impl ReturnValueDomain for Scalar {}
impl ReturnValueDomain for Mask {}
impl ReturnValueDomain for AttributeName {}
impl<I: IndexDomain> ReturnValueDomain for IndexValue<I> {}
impl<E: EntityDomain> ReturnValueDomain for EntityReference<E> {}
impl ReturnValueDomain for FailureValue {}
impl ReturnValueDomain for FailureKindValue {}
