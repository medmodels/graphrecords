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
    Arity, Bare, BareValueDomain, Definite, ElementShape, Failure, IndexDomain, Indexed, Multiple,
    OrderState, QueryResult, Single, ValueDomain, error::grouping::UnresolvedGroupKeyFailures,
    expressions::PartitionKeyFailureParts, registry::OperationManifest,
};
pub use broadcast::BroadcastOperation;
pub use broadcast_via::BroadcastViaOperation;
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

pub(super) fn operation_manifests() -> Vec<OperationManifest> {
    vec![
        broadcast::operation_manifest(),
        broadcast_via::operation_manifest(),
        group_by::operation_manifest(),
        having::operation_manifest(),
        keys::operation_manifest(),
        ungroup::operation_manifest(),
        ungroup_keyed::operation_manifest(),
        inspection::bucket_errors::operation_manifest(),
        inspection::key_errors::operation_manifest(),
        on_bucket_error::drop::operation_manifest(),
        on_bucket_error::raise::operation_manifest(),
        on_key_error::drop::operation_manifest(),
        on_key_error::raise::operation_manifest(),
    ]
}

fn reject_key_failures<M: IndexDomain>(
    key_failures: Vec<PartitionKeyFailureParts<M>>,
    label: &'static str,
) -> QueryResult<()> {
    if key_failures.is_empty() {
        return Ok(());
    }

    Err(Failure::new(
        UnresolvedGroupKeyFailures::new(
            key_failures
                .into_iter()
                .map(|key_failure| *key_failure.1)
                .collect(),
        ),
        label,
    ))
}

pub trait BucketFailureArity<S: ElementShape>: Arity {
    fn bucket_failure<'payload, 'a>(
        payload: &'payload QueryResult<Self::Container<'a, S::Element<'a>>>,
    ) -> Option<&'payload Failure>
    where
        S: 'a;
}

impl<I: IndexDomain, V: ValueDomain, O: OrderState> BucketFailureArity<Indexed<I, V>>
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

impl<V: BareValueDomain, O: OrderState> BucketFailureArity<Bare<V>> for Multiple<O> {
    fn bucket_failure<'payload, 'a>(
        payload: &'payload QueryResult<Self::Container<'a, <Bare<V> as ElementShape>::Element<'a>>>,
    ) -> Option<&'payload Failure>
    where
        Bare<V>: 'a,
    {
        payload.as_ref().err().map(Box::as_ref)
    }
}

impl<I: IndexDomain, V: ValueDomain> BucketFailureArity<Indexed<I, V>> for Single {
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

impl<V: BareValueDomain> BucketFailureArity<Bare<V>> for Single {
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

impl<I: IndexDomain, V: ValueDomain> BucketFailureArity<Indexed<I, V>> for Definite {
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

impl<V: BareValueDomain> BucketFailureArity<Bare<V>> for Definite {
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
