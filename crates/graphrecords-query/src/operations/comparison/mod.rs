mod equal_to;
mod greater_than;
mod greater_than_or_equal_to;
mod less_than;
mod less_than_or_equal_to;
mod not_equal_to;

use crate::{
    Failure, IncomparableValues, IndexDomain, Mask, ValueType,
    element::{BarePipeline, IndexedValuePipeline, Pipeline, Retention},
    operations::{ArgumentSource, Keyed, Unaligned},
};
pub use equal_to::EqualToOperation;
pub use greater_than::GreaterThanOperation;
pub use greater_than_or_equal_to::GreaterThanOrEqualToOperation;
pub use less_than::LessThanOperation;
pub use less_than_or_equal_to::LessThanOrEqualToOperation;
pub use not_equal_to::NotEqualToOperation;
use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
};

fn equality_indexed<'a, I, V, A>(
    prepared: A::Prepared<'a>,
    label: &'static str,
    equality: fn(&V::Value<'a>, &V::Value<'a>) -> bool,
) -> IndexedValuePipeline<'a, I, V, Mask, A::Retention>
where
    I: IndexDomain,
    V: ValueType,
    A: ArgumentSource<Keyed<I>, Value<'a> = V::Value<'a>>,
    A::Prepared<'a>: 'a,
{
    Pipeline::keyed(move |index, item| {
        let value = match item {
            Ok(value) => value,
            Err(original) => {
                return <A::Retention as Retention>::keep(Err(original));
            }
        };

        let step = A::resolve(&prepared, &index, label);

        <A::Retention as Retention>::map_step(step, |resolved| {
            resolved.map(|argument| equality(&value, &argument))
        })
    })
}

fn equality_bare<'a, V, A>(
    prepared: A::Prepared<'a>,
    label: &'static str,
    equality: fn(&V::Value<'a>, &V::Value<'a>) -> bool,
) -> BarePipeline<'a, V, Mask, A::Retention>
where
    V: ValueType,
    A: ArgumentSource<Unaligned, Value<'a> = V::Value<'a>>,
    A::Prepared<'a>: 'a,
{
    Pipeline::new(move |item| {
        let value = match item {
            Ok(value) => value,
            Err(original) => {
                return <A::Retention as Retention>::keep(Err(original));
            }
        };

        let step = A::resolve(&prepared, &(), label);

        <A::Retention as Retention>::map_step(step, |resolved| {
            resolved.map(|argument| equality(&value, &argument))
        })
    })
}

fn ordering_indexed<'a, I, V, A>(
    prepared: A::Prepared<'a>,
    label: &'static str,
    ordering: fn(&V::Value<'a>, &V::Value<'a>) -> Option<Ordering>,
    predicate: fn(Ordering) -> bool,
) -> IndexedValuePipeline<'a, I, V, Mask, A::Retention>
where
    I: IndexDomain,
    V: ValueType,
    A: ArgumentSource<Keyed<I>, Value<'a> = V::Value<'a>>,
    V::Owned: Debug + Display + Send + Sync,
    A::Prepared<'a>: 'a,
{
    Pipeline::keyed(move |index, item| {
        let value = match item {
            Ok(value) => value,
            Err(original) => {
                return <A::Retention as Retention>::keep(Err(original));
            }
        };

        let step = A::resolve(&prepared, &index, label);

        <A::Retention as Retention>::map_step(step, |resolved| {
            resolved.and_then(|argument| match ordering(&value, &argument) {
                Some(outcome) => Ok(predicate(outcome)),
                None => Err(Failure::new_at::<I, _>(
                    label,
                    IncomparableValues {
                        first: V::into_owned(value),
                        second: V::into_owned(argument),
                    },
                    &index,
                )),
            })
        })
    })
}

fn ordering_bare<'a, V, A>(
    prepared: A::Prepared<'a>,
    label: &'static str,
    ordering: fn(&V::Value<'a>, &V::Value<'a>) -> Option<Ordering>,
    predicate: fn(Ordering) -> bool,
) -> BarePipeline<'a, V, Mask, A::Retention>
where
    V: ValueType,
    A: ArgumentSource<Unaligned, Value<'a> = V::Value<'a>>,
    V::Owned: Debug + Display + Send + Sync,
    A::Prepared<'a>: 'a,
{
    Pipeline::new(move |item| {
        let value = match item {
            Ok(value) => value,
            Err(original) => {
                return <A::Retention as Retention>::keep(Err(original));
            }
        };

        let step = A::resolve(&prepared, &(), label);

        <A::Retention as Retention>::map_step(step, |resolved| {
            resolved.and_then(|argument| match ordering(&value, &argument) {
                Some(outcome) => Ok(predicate(outcome)),
                None => Err(Failure::new(
                    label,
                    IncomparableValues {
                        first: V::into_owned(value),
                        second: V::into_owned(argument),
                    },
                )),
            })
        })
    })
}
