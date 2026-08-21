mod equal_to;
mod greater_than;
mod greater_than_or_equal_to;
mod less_than;
mod less_than_or_equal_to;
mod not_equal_to;

use crate::{
    Failure, IndexDomain, Mask, ValueDomain,
    element::{BarePipeline, IndexedValuePipeline, Pipeline, Retention},
    error::comparison::IncomparableValues,
    operations::{ArgumentSource, Keyed, Unaligned},
    registry::OperationManifest,
};
pub use equal_to::EqualToOperation;
use graphrecords_core::GraphRecord;
pub use greater_than::GreaterThanOperation;
pub use greater_than_or_equal_to::GreaterThanOrEqualToOperation;
pub use less_than::LessThanOperation;
pub use less_than_or_equal_to::LessThanOrEqualToOperation;
pub use not_equal_to::NotEqualToOperation;
use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
};

pub(super) fn operation_manifests() -> Vec<OperationManifest> {
    vec![
        equal_to::operation_manifest(),
        greater_than::operation_manifest(),
        greater_than_or_equal_to::operation_manifest(),
        less_than::operation_manifest(),
        less_than_or_equal_to::operation_manifest(),
        not_equal_to::operation_manifest(),
    ]
}

fn equality_indexed<'a, I, V, A>(
    graphrecord: &'a GraphRecord,
    prepared: A::Prepared<'a>,
    equality: fn(&V::Value<'a>, &V::Value<'a>) -> bool,
    label: &'static str,
) -> IndexedValuePipeline<'a, I, V, Mask, A::Retention>
where
    I: IndexDomain,
    V: ValueDomain,
    A: ArgumentSource<Keyed<I>, V>,
    A::Prepared<'a>: 'a,
{
    Pipeline::keyed(move |address, item| {
        let value = match item {
            Ok(value) => value,
            Err(original) => {
                return A::Retention::keep(Err(original));
            }
        };

        let step = A::resolve(graphrecord, &prepared, &address, label);

        A::Retention::map_step(step, |resolved| {
            resolved.map(|argument| equality(&value, &argument))
        })
    })
}

fn equality_bare<'a, V, A>(
    graphrecord: &'a GraphRecord,
    prepared: A::Prepared<'a>,
    equality: fn(&V::Value<'a>, &V::Value<'a>) -> bool,
    label: &'static str,
) -> BarePipeline<'a, V, Mask, A::Retention>
where
    V: ValueDomain,
    A: ArgumentSource<Unaligned, V>,
    A::Prepared<'a>: 'a,
{
    Pipeline::new(move |item| {
        let value = match item {
            Ok(value) => value,
            Err(original) => {
                return A::Retention::keep(Err(original));
            }
        };

        let step = A::resolve(graphrecord, &prepared, &(), label);

        A::Retention::map_step(step, |resolved| {
            resolved.map(|argument| equality(&value, &argument))
        })
    })
}

fn ordering_indexed<'a, I, V, A>(
    graphrecord: &'a GraphRecord,
    prepared: A::Prepared<'a>,
    ordering: fn(&V::Value<'a>, &V::Value<'a>) -> Option<Ordering>,
    predicate: fn(Ordering) -> bool,
    label: &'static str,
) -> IndexedValuePipeline<'a, I, V, Mask, A::Retention>
where
    I: IndexDomain,
    V: ValueDomain,
    A: ArgumentSource<Keyed<I>, V>,
    V::Owned: Debug + Display + Send + Sync,
    A::Prepared<'a>: 'a,
{
    Pipeline::keyed(move |address, item| {
        let value = match item {
            Ok(value) => value,
            Err(original) => {
                return A::Retention::keep(Err(original));
            }
        };

        let step = A::resolve(graphrecord, &prepared, &address, label);

        A::Retention::map_step(step, |resolved| {
            resolved.and_then(|argument| match ordering(&value, &argument) {
                Some(outcome) => Ok(predicate(outcome)),
                None => Err(Failure::new_at_address::<I, _>(
                    IncomparableValues::new(V::into_owned(value), V::into_owned(argument)),
                    graphrecord,
                    &address,
                    label,
                )),
            })
        })
    })
}

fn ordering_bare<'a, V, A>(
    graphrecord: &'a GraphRecord,
    prepared: A::Prepared<'a>,
    ordering: fn(&V::Value<'a>, &V::Value<'a>) -> Option<Ordering>,
    predicate: fn(Ordering) -> bool,
    label: &'static str,
) -> BarePipeline<'a, V, Mask, A::Retention>
where
    V: ValueDomain,
    A: ArgumentSource<Unaligned, V>,
    V::Owned: Debug + Display + Send + Sync,
    A::Prepared<'a>: 'a,
{
    Pipeline::new(move |item| {
        let value = match item {
            Ok(value) => value,
            Err(original) => {
                return A::Retention::keep(Err(original));
            }
        };

        let step = A::resolve(graphrecord, &prepared, &(), label);

        A::Retention::map_step(step, |resolved| {
            resolved.and_then(|argument| match ordering(&value, &argument) {
                Some(outcome) => Ok(predicate(outcome)),
                None => Err(Failure::new(
                    IncomparableValues::new(V::into_owned(value), V::into_owned(argument)),
                    label,
                )),
            })
        })
    })
}
