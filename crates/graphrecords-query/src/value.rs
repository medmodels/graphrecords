use crate::{EntityDomain, Failure, FailureKind, IndexDomain, error::QueryResult};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{AttributeName, Value, ValueView, datatypes::AttributeNameView},
};
use std::{
    hash::{Hash, Hasher},
    marker::PhantomData,
    ptr,
};

pub trait ValueDomain: 'static {
    type Owned: 'static + Clone;

    type Value<'a>: 'a + Clone
    where
        Self: 'a;

    type Cached: 'static + Send + Sync;

    fn into_owned(value: Self::Value<'_>) -> Self::Owned;

    fn from_owned<'a>(
        graphrecord: &'a GraphRecord,
        owned: &'a Self::Owned,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>>;

    fn into_cached(value: Self::Value<'_>) -> Self::Cached;

    fn from_cached<'a>(graphrecord: &'a GraphRecord, cached: &'a Self::Cached) -> Self::Value<'a>;
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

pub struct EntityRef<'a, E: EntityDomain> {
    graphrecord: &'a GraphRecord,
    address: E::Address,
}

impl<'a, E: EntityDomain> EntityRef<'a, E> {
    #[must_use]
    pub const fn new(graphrecord: &'a GraphRecord, address: E::Address) -> Self {
        Self {
            graphrecord,
            address,
        }
    }

    #[must_use]
    pub const fn graphrecord(&self) -> &'a GraphRecord {
        self.graphrecord
    }

    #[must_use]
    pub const fn address(&self) -> &E::Address {
        &self.address
    }

    #[must_use]
    pub fn index(&self) -> E::Index<'a> {
        E::index(self.graphrecord, &self.address)
    }

    #[must_use]
    pub fn into_owned(self) -> E::Owned {
        E::own_index(&E::index(self.graphrecord, &self.address))
    }
}

impl<E: EntityDomain> Clone for EntityRef<'_, E> {
    fn clone(&self) -> Self {
        Self {
            graphrecord: self.graphrecord,
            address: self.address.clone(),
        }
    }
}

impl<E: EntityDomain> PartialEq for EntityRef<'_, E> {
    fn eq(&self, other: &Self) -> bool {
        ptr::eq(self.graphrecord, other.graphrecord) && self.address == other.address
    }
}

impl<E: EntityDomain> Eq for EntityRef<'_, E> {}

impl<E: EntityDomain> Hash for EntityRef<'_, E> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        ptr::from_ref(self.graphrecord).hash(state);
        self.address.hash(state);
    }
}

impl ValueDomain for Scalar {
    type Cached = Value;
    type Owned = Value;
    type Value<'a> = ValueView<'a>;

    fn into_owned(value: Self::Value<'_>) -> Self::Owned {
        Value::from(value)
    }

    fn from_owned<'a>(
        _graphrecord: &'a GraphRecord,
        owned: &'a Self::Owned,
        _label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        Ok(ValueView::from(owned))
    }

    fn into_cached(value: Self::Value<'_>) -> Self::Cached {
        Value::from(value)
    }

    fn from_cached<'a>(_graphrecord: &'a GraphRecord, cached: &'a Self::Cached) -> Self::Value<'a> {
        ValueView::from(cached)
    }
}

impl ValueDomain for Mask {
    type Cached = bool;
    type Owned = bool;
    type Value<'a> = bool;

    fn into_owned(value: Self::Value<'_>) -> Self::Owned {
        value
    }

    fn from_owned<'a>(
        _graphrecord: &'a GraphRecord,
        owned: &'a Self::Owned,
        _label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        Ok(*owned)
    }

    fn into_cached(value: Self::Value<'_>) -> Self::Cached {
        value
    }

    fn from_cached<'a>(_graphrecord: &'a GraphRecord, cached: &'a Self::Cached) -> Self::Value<'a> {
        *cached
    }
}

impl ValueDomain for AttributeName {
    type Cached = Self;
    type Owned = Self;
    type Value<'a> = AttributeNameView<'a>;

    fn into_owned(value: Self::Value<'_>) -> Self::Owned {
        Self::from(value)
    }

    fn from_owned<'a>(
        _graphrecord: &'a GraphRecord,
        owned: &'a Self::Owned,
        _label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        Ok(AttributeNameView::from(owned.identifier()))
    }

    fn into_cached(value: Self::Value<'_>) -> Self::Cached {
        Self::from(value)
    }

    fn from_cached<'a>(_graphrecord: &'a GraphRecord, cached: &'a Self::Cached) -> Self::Value<'a> {
        AttributeNameView::from(cached.identifier())
    }
}

impl ValueDomain for Unit {
    type Cached = ();
    type Owned = ();
    type Value<'a> = ();

    fn into_owned(_value: Self::Value<'_>) -> Self::Owned {}

    fn from_owned<'a>(
        _graphrecord: &'a GraphRecord,
        _owned: &'a Self::Owned,
        _label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        Ok(())
    }

    fn into_cached(_value: Self::Value<'_>) -> Self::Cached {}

    fn from_cached<'a>(
        _graphrecord: &'a GraphRecord,
        _cached: &'a Self::Cached,
    ) -> Self::Value<'a> {
    }
}

impl<I: IndexDomain> ValueDomain for IndexValue<I> {
    type Cached = I::Owned;
    type Owned = I::Owned;
    type Value<'a> = I::Index<'a>;

    fn into_owned(value: Self::Value<'_>) -> Self::Owned {
        I::own_index(&value)
    }

    fn from_owned<'a>(
        _graphrecord: &'a GraphRecord,
        owned: &'a Self::Owned,
        _label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        Ok(I::borrow_index(owned))
    }

    fn into_cached(value: Self::Value<'_>) -> Self::Cached {
        I::own_index(&value)
    }

    fn from_cached<'a>(_graphrecord: &'a GraphRecord, cached: &'a Self::Cached) -> Self::Value<'a> {
        I::borrow_index(cached)
    }
}

impl<E: EntityDomain> ValueDomain for EntityReference<E> {
    type Cached = E::Address;
    type Owned = E::Owned;
    type Value<'a> = EntityRef<'a, E>;

    fn into_owned(value: Self::Value<'_>) -> Self::Owned {
        value.into_owned()
    }

    fn from_owned<'a>(
        graphrecord: &'a GraphRecord,
        owned: &'a Self::Owned,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        Ok(EntityRef::new(
            graphrecord,
            E::resolve(graphrecord, owned, label)?,
        ))
    }

    fn into_cached(value: Self::Value<'_>) -> Self::Cached {
        value.address
    }

    fn from_cached<'a>(graphrecord: &'a GraphRecord, cached: &'a Self::Cached) -> Self::Value<'a> {
        EntityRef::new(graphrecord, cached.clone())
    }
}

impl ValueDomain for FailureValue {
    type Cached = Failure;
    type Owned = Failure;
    type Value<'a> = Failure;

    fn into_owned(value: Self::Value<'_>) -> Self::Owned {
        value
    }

    fn from_owned<'a>(
        _graphrecord: &'a GraphRecord,
        owned: &'a Self::Owned,
        _label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        Ok(owned.clone())
    }

    fn into_cached(value: Self::Value<'_>) -> Self::Cached {
        value
    }

    fn from_cached<'a>(_graphrecord: &'a GraphRecord, cached: &'a Self::Cached) -> Self::Value<'a> {
        cached.clone()
    }
}

impl ValueDomain for FailureKindValue {
    type Cached = FailureKind;
    type Owned = FailureKind;
    type Value<'a> = FailureKind;

    fn into_owned(value: Self::Value<'_>) -> Self::Owned {
        value
    }

    fn from_owned<'a>(
        _graphrecord: &'a GraphRecord,
        owned: &'a Self::Owned,
        _label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        Ok(*owned)
    }

    fn into_cached(value: Self::Value<'_>) -> Self::Cached {
        value
    }

    fn from_cached<'a>(_graphrecord: &'a GraphRecord, cached: &'a Self::Cached) -> Self::Value<'a> {
        *cached
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
