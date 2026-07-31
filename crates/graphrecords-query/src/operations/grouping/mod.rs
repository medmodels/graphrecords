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
    Arity, AttributeName, Bare, BareValueType, Definite, ElementShape, Failure, FailureKind,
    IndexDomain, Indexed, Multiple, OrderState, Position, Positional, QueryResult, Single,
    ValueType,
    capabilities::GroupingValue,
    error::grouping::UnresolvedGroupKeyFailures,
    index::GroupKey,
    operands::OperandHandle,
    operations::{ArgumentSource, Keyed, MaybeAbsent, MissingPolicy, WithMissing},
};
pub use broadcast::BroadcastOperation;
pub use broadcast_via::BroadcastViaOperation;
use graphrecords_core::graphrecord::{EdgeIndex, GraphRecordAttribute, GraphRecordValue};
pub use group_by::GroupByOperation;
pub use having::HavingOperation;
pub use inspection::{BucketErrorsOperation, KeyErrorsOperation};
pub use keys::KeysOperation;
pub use on_bucket_error::{
    BucketErrorPolicy, BucketErrorPolicyIn, BucketErrorPolicyOf, BucketErrorPolicyWithCause,
    DropBucketErrors, DropBucketErrorsIn, DropBucketErrorsOf, DropBucketErrorsWithCause,
    RaiseBucketErrors, RaiseBucketErrorsIn, RaiseBucketErrorsOf, RaiseBucketErrorsWithCause,
};
pub use on_key_error::{
    DropKeyErrors, DropKeyErrorsIn, DropKeyErrorsOf, DropKeyErrorsWithCause, KeyErrorPolicy,
    KeyErrorPolicyIn, KeyErrorPolicyOf, KeyErrorPolicyWithCause, RaiseKeyErrors, RaiseKeyErrorsIn,
    RaiseKeyErrorsOf, RaiseKeyErrorsWithCause,
};
pub use ungroup::UngroupOperation;
pub use ungroup_keyed::UngroupKeyedOperation;

fn reject_key_failures<M: IndexDomain>(
    key_failures: Vec<(M::Index<'_>, Box<Failure>)>,
    label: &'static str,
) -> QueryResult<()> {
    if key_failures.is_empty() {
        return Ok(());
    }

    Err(Failure::new(
        label,
        UnresolvedGroupKeyFailures::new(
            key_failures
                .into_iter()
                .map(|key_failure| *key_failure.1)
                .collect(),
        ),
    ))
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
impl<I: IndexDomain> KeyOperand<I> for FailureKind {
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
    V: GroupingValue + BareValueType,
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

impl<V: BareValueType, O: OrderState> BucketFailureArity<Bare<V>> for Multiple<O> {
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

impl<V: BareValueType> BucketFailureArity<Bare<V>> for Single {
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

impl<V: BareValueType> BucketFailureArity<Bare<V>> for Definite {
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
