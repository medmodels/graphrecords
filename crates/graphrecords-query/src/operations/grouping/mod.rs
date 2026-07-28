mod broadcast;
mod broadcast_via;
mod group_by;
mod having;
mod inspection;
mod keys;
mod on_bucket_error;
mod on_key_error;
mod ungroup;
mod ungroup_keyed;

use crate::{
    Arity, AttributeName, Bare, Definite, Diagnostic, ElementShape, EntityDomain, EntityReference,
    ExpandedIndex, ExpandedIndexReference, Failure, FailureKind, FailureKindValue, IndexDomain,
    IndexValue, Indexed, Mask, Multiple, OrderState, Position, Positional, QueryResult, Scalar,
    Single, ValueType,
    operands::OperandHandle,
    operations::{ArgumentSource, Keyed, MissingPolicy, WithMissing},
    traits::MaybeAbsent,
};
pub use broadcast::BroadcastOperation;
pub use broadcast_via::BroadcastViaOperation;
use graphrecords_core::{
    GraphRecord,
    graphrecord::{EdgeIndex, GraphRecordAttribute, GraphRecordValue, NodeIndex},
};
pub use group_by::GroupByOperation;
pub use having::HavingOperation;
pub use inspection::{BucketErrorsOperation, KeyErrorsOperation};
pub use keys::KeysOperation;
pub use on_bucket_error::{
    BucketErrorPolicy, BucketErrorPolicyIn, BucketErrorPolicyOf, BucketErrorPolicyWithCause,
};
pub use on_key_error::{
    KeyErrorPolicy, KeyErrorPolicyIn, KeyErrorPolicyOf, KeyErrorPolicyWithCause,
};
use std::{
    error::Error,
    fmt::{self, Display, Formatter},
};
pub use ungroup::UngroupOperation;
pub use ungroup_keyed::UngroupKeyedOperation;

pub trait GroupKey: IndexDomain {
    fn resolve_key<'a>(
        graphrecord: &'a GraphRecord,
        key: &Self::Owned,
    ) -> QueryResult<Self::Index<'a>>;
}

impl GroupKey for GraphRecordValue {
    fn resolve_key<'a>(
        _graphrecord: &'a GraphRecord,
        key: &Self::Owned,
    ) -> QueryResult<Self::Index<'a>> {
        Ok(key.clone())
    }
}
impl GroupKey for bool {
    fn resolve_key<'a>(
        _graphrecord: &'a GraphRecord,
        key: &Self::Owned,
    ) -> QueryResult<Self::Index<'a>> {
        Ok(*key)
    }
}
impl GroupKey for AttributeName {
    fn resolve_key<'a>(
        _graphrecord: &'a GraphRecord,
        key: &Self::Owned,
    ) -> QueryResult<Self::Index<'a>> {
        Ok(key.clone())
    }
}
impl GroupKey for FailureKind {
    fn resolve_key<'a>(
        _graphrecord: &'a GraphRecord,
        key: &Self::Owned,
    ) -> QueryResult<Self::Index<'a>> {
        Ok(*key)
    }
}
impl GroupKey for Positional {
    fn resolve_key<'a>(
        _graphrecord: &'a GraphRecord,
        key: &Self::Owned,
    ) -> QueryResult<Self::Index<'a>> {
        Ok(*key)
    }
}
impl GroupKey for NodeIndex {
    fn resolve_key<'a>(
        graphrecord: &'a GraphRecord,
        key: &Self::Owned,
    ) -> QueryResult<Self::Index<'a>> {
        Self::resolve_index(graphrecord, key).map_err(|error| {
            Failure::new_at::<Self, _>("group key resolution", error, &Self::from_owned(key))
        })
    }
}
impl GroupKey for EdgeIndex {
    fn resolve_key<'a>(
        graphrecord: &'a GraphRecord,
        key: &Self::Owned,
    ) -> QueryResult<Self::Index<'a>> {
        Self::resolve_index(graphrecord, key).map_err(|error| {
            Failure::new_at::<Self, _>("group key resolution", error, &Self::from_owned(key))
        })
    }
}
impl<P: GroupKey, C: GroupKey> GroupKey for ExpandedIndex<P, C> {
    fn resolve_key<'a>(
        graphrecord: &'a GraphRecord,
        key: &Self::Owned,
    ) -> QueryResult<Self::Index<'a>> {
        let parent = P::resolve_key(graphrecord, key.parent_index())?;

        match key.child_index() {
            None => Ok(ExpandedIndexReference::source(parent)),
            Some(child) => Ok(ExpandedIndexReference::child(
                parent,
                C::resolve_key(graphrecord, child)?,
            )),
        }
    }
}

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

#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot provide grouping keys for `{I}`",
    note = "a grouping key must be keyed by the subject domain or be a single constant"
)]
pub trait KeyOperand<I: IndexDomain>: ArgumentSource<Keyed<I>> {
    type Key: GroupKey;

    fn to_key(value: &Self::Value<'_>) -> <Self::Key as IndexDomain>::Owned;
}

impl<I: IndexDomain> KeyOperand<I> for GraphRecordValue {
    type Key = Self;

    fn to_key(value: &Self::Value<'_>) -> <Self::Key as IndexDomain>::Owned {
        value.clone()
    }
}
impl<I: IndexDomain> KeyOperand<I> for bool {
    type Key = Self;

    fn to_key(value: &Self::Value<'_>) -> <Self::Key as IndexDomain>::Owned {
        *value
    }
}
impl<I: IndexDomain> KeyOperand<I> for GraphRecordAttribute {
    type Key = AttributeName;

    fn to_key(value: &Self::Value<'_>) -> <Self::Key as IndexDomain>::Owned {
        value.clone()
    }
}
impl<I: IndexDomain> KeyOperand<I> for Position {
    type Key = Positional;

    fn to_key(value: &Self::Value<'_>) -> <Self::Key as IndexDomain>::Owned {
        *value
    }
}
impl<I: IndexDomain> KeyOperand<I> for EdgeIndex {
    type Key = Self;

    fn to_key(value: &Self::Value<'_>) -> <Self::Key as IndexDomain>::Owned {
        *value
    }
}

impl<I, J, V, C> KeyOperand<I> for OperandHandle<Indexed<J, V>, C>
where
    I: IndexDomain,
    J: IndexDomain,
    V: GroupingValue,
    C: Arity,
    for<'a> Self: ArgumentSource<Keyed<I>, Value<'a> = V::Value<'a>>,
{
    type Key = V::Key;

    fn to_key(value: &Self::Value<'_>) -> <Self::Key as IndexDomain>::Owned {
        V::to_group_key(value)
    }
}

impl<I, V, C> KeyOperand<I> for OperandHandle<Bare<V>, C>
where
    I: IndexDomain,
    V: GroupingValue,
    C: Arity,
    for<'a> Self: ArgumentSource<Keyed<I>, Value<'a> = V::Value<'a>>,
{
    type Key = V::Key;

    fn to_key(value: &Self::Value<'_>) -> <Self::Key as IndexDomain>::Owned {
        V::to_group_key(value)
    }
}

impl<I, S, P> KeyOperand<I> for WithMissing<Keyed<I>, S, P>
where
    I: IndexDomain,
    S: KeyOperand<I> + MaybeAbsent<Keyed<I>> + Clone,
    P: MissingPolicy<Keyed<I>, S>,
{
    type Key = S::Key;

    fn to_key(value: &Self::Value<'_>) -> <Self::Key as IndexDomain>::Owned {
        S::to_key(value)
    }
}

pub trait BucketFailureArity<S: ElementShape>: Arity {
    fn bucket_failure<'payload, 'a>(
        payload: &'payload QueryResult<Self::Container<'a, S::Element<'a>>>,
    ) -> Option<&'payload Failure>
    where
        S: 'a;
}

impl<I: IndexDomain, V: ValueType, O: OrderState> BucketFailureArity<Indexed<I, V>>
    for Multiple<O>
{
    fn bucket_failure<'payload, 'a>(
        payload: &'payload QueryResult<
            Self::Container<'a, <Indexed<I, V> as ElementShape>::Element<'a>>,
        >,
    ) -> Option<&'payload Failure>
    where
        Indexed<I, V>: 'a,
    {
        payload.as_ref().err().map(Box::as_ref)
    }
}

impl<V: ValueType, O: OrderState> BucketFailureArity<Bare<V>> for Multiple<O> {
    fn bucket_failure<'payload, 'a>(
        payload: &'payload QueryResult<Self::Container<'a, <Bare<V> as ElementShape>::Element<'a>>>,
    ) -> Option<&'payload Failure>
    where
        Bare<V>: 'a,
    {
        payload.as_ref().err().map(Box::as_ref)
    }
}

impl<I: IndexDomain, V: ValueType> BucketFailureArity<Indexed<I, V>> for Single {
    fn bucket_failure<'payload, 'a>(
        payload: &'payload QueryResult<
            Self::Container<'a, <Indexed<I, V> as ElementShape>::Element<'a>>,
        >,
    ) -> Option<&'payload Failure>
    where
        Indexed<I, V>: 'a,
    {
        match payload {
            Err(failure) | Ok(Some((_, Err(failure)))) => Some(failure),
            Ok(None | Some((_, Ok(_)))) => None,
        }
    }
}

impl<V: ValueType> BucketFailureArity<Bare<V>> for Single {
    fn bucket_failure<'payload, 'a>(
        payload: &'payload QueryResult<Self::Container<'a, <Bare<V> as ElementShape>::Element<'a>>>,
    ) -> Option<&'payload Failure>
    where
        Bare<V>: 'a,
    {
        match payload {
            Err(failure) | Ok(Some(Err(failure))) => Some(failure),
            Ok(None | Some(Ok(_))) => None,
        }
    }
}

impl<I: IndexDomain, V: ValueType> BucketFailureArity<Indexed<I, V>> for Definite {
    fn bucket_failure<'payload, 'a>(
        payload: &'payload QueryResult<
            Self::Container<'a, <Indexed<I, V> as ElementShape>::Element<'a>>,
        >,
    ) -> Option<&'payload Failure>
    where
        Indexed<I, V>: 'a,
    {
        match payload {
            Err(failure) | Ok((_, Err(failure))) => Some(failure),
            Ok((_, Ok(_))) => None,
        }
    }
}

impl<V: ValueType> BucketFailureArity<Bare<V>> for Definite {
    fn bucket_failure<'payload, 'a>(
        payload: &'payload QueryResult<Self::Container<'a, <Bare<V> as ElementShape>::Element<'a>>>,
    ) -> Option<&'payload Failure>
    where
        Bare<V>: 'a,
    {
        match payload {
            Err(failure) | Ok(Err(failure)) => Some(failure),
            Ok(Ok(_)) => None,
        }
    }
}

#[derive(Debug)]
pub struct MissingGroupAggregate;

impl Display for MissingGroupAggregate {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("no aggregate value for the element's group")
    }
}

impl Error for MissingGroupAggregate {}

impl Diagnostic for MissingGroupAggregate {
    fn name() -> &'static str {
        "MissingGroupAggregate"
    }

    fn help(&self) -> Option<String> {
        Some(
            "ensure every group produces a value or handle the gap with `on_error(...)`"
                .to_string(),
        )
    }
}

#[derive(Debug)]
pub struct UnresolvedGroupKeyFailures {
    failures: Vec<Failure>,
}

impl UnresolvedGroupKeyFailures {
    #[must_use]
    pub const fn new(failures: Vec<Failure>) -> Self {
        Self { failures }
    }

    #[must_use]
    pub fn failures(&self) -> &[Failure] {
        &self.failures
    }
}

impl Display for UnresolvedGroupKeyFailures {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} unresolved grouping-key failure(s) cannot be represented by this exit",
            self.failures.len(),
        )
    }
}

impl Error for UnresolvedGroupKeyFailures {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.failures
            .first()
            .map(|failure| failure as &(dyn Error + 'static))
    }
}

impl Diagnostic for UnresolvedGroupKeyFailures {
    fn name() -> &'static str {
        "UnresolvedGroupKeyFailures"
    }

    fn help(&self) -> Option<String> {
        Some("resolve retained key failures with `on_key_error(...)` before this exit".to_string())
    }
}

#[derive(Debug)]
pub struct UnresolvedBucketFailures {
    failures: Vec<Failure>,
}

impl UnresolvedBucketFailures {
    #[must_use]
    pub const fn new(failures: Vec<Failure>) -> Self {
        Self { failures }
    }

    #[must_use]
    pub fn failures(&self) -> &[Failure] {
        &self.failures
    }
}

impl Display for UnresolvedBucketFailures {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} unresolved bucket failure(s) cannot be represented by this exit",
            self.failures.len(),
        )
    }
}

impl Error for UnresolvedBucketFailures {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.failures
            .first()
            .map(|failure| failure as &(dyn Error + 'static))
    }
}

impl Diagnostic for UnresolvedBucketFailures {
    fn name() -> &'static str {
        "UnresolvedBucketFailures"
    }

    fn help(&self) -> Option<String> {
        Some(
            "resolve retained bucket failures with `on_bucket_error(...)` before this exit"
                .to_string(),
        )
    }
}
