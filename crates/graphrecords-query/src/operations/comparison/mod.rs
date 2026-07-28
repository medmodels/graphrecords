mod equal_to;
mod greater_than;
mod greater_than_or_equal_to;
mod less_than;
mod less_than_or_equal_to;
mod not_equal_to;

use crate::{
    AttributeName, Failure, FailureKindValue, IncomparableValues, IndexDomain, IndexValue, Mask,
    Scalar, ValueType,
    operations::{
        ArgumentSource, BarePipeline, IndexedValuePipeline, Keyed, Pipeline, Retention, Unaligned,
    },
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

pub trait ValueEquality: ValueType {
    fn equal(value: &Self::Owned, argument: &Self::Owned) -> bool;
}

pub trait ValueOrdering: ValueEquality {
    fn ordering(value: &Self::Owned, argument: &Self::Owned) -> Option<Ordering>;
}

impl ValueEquality for Scalar {
    fn equal(value: &Self::Owned, argument: &Self::Owned) -> bool {
        value == argument
    }
}

impl ValueOrdering for Scalar {
    fn ordering(value: &Self::Owned, argument: &Self::Owned) -> Option<Ordering> {
        value.partial_cmp(argument)
    }
}

impl ValueEquality for AttributeName {
    fn equal(value: &Self::Owned, argument: &Self::Owned) -> bool {
        value == argument
    }
}

impl ValueOrdering for AttributeName {
    fn ordering(value: &Self::Owned, argument: &Self::Owned) -> Option<Ordering> {
        value.partial_cmp(argument)
    }
}

impl<I: IndexDomain> ValueEquality for IndexValue<I> {
    fn equal(value: &Self::Owned, argument: &Self::Owned) -> bool {
        value == argument
    }
}

impl<I> ValueOrdering for IndexValue<I>
where
    I: IndexDomain,
    I::Owned: PartialOrd,
{
    fn ordering(value: &Self::Owned, argument: &Self::Owned) -> Option<Ordering> {
        value.partial_cmp(argument)
    }
}

impl ValueEquality for FailureKindValue {
    fn equal(value: &Self::Owned, argument: &Self::Owned) -> bool {
        value == argument
    }
}

impl ValueOrdering for FailureKindValue {
    fn ordering(value: &Self::Owned, argument: &Self::Owned) -> Option<Ordering> {
        Some(value.cmp(argument))
    }
}

fn equality_indexed<'a, I, A, V>(
    prepared: A::Prepared<'a>,
    label: &'static str,
    equality: fn(&V::Owned, &V::Owned) -> bool,
) -> IndexedValuePipeline<'a, I, V, Mask, A::Retention>
where
    I: IndexDomain,
    A: ArgumentSource<Keyed<I>, Value<'a> = <V as ValueType>::Owned>,
    A::Prepared<'a>: 'a,
    V: ValueType<Value<'a> = <V as ValueType>::Owned>,
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

fn equality_bare<'a, A, V>(
    prepared: A::Prepared<'a>,
    label: &'static str,
    equality: fn(&V::Owned, &V::Owned) -> bool,
) -> BarePipeline<'a, V, Mask, A::Retention>
where
    A: ArgumentSource<Unaligned, Value<'a> = <V as ValueType>::Owned>,
    A::Prepared<'a>: 'a,
    V: ValueType<Value<'a> = <V as ValueType>::Owned>,
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

fn ordering_indexed<'a, I, A, V>(
    prepared: A::Prepared<'a>,
    label: &'static str,
    ordering: fn(&V::Owned, &V::Owned) -> Option<Ordering>,
    predicate: fn(Ordering) -> bool,
) -> IndexedValuePipeline<'a, I, V, Mask, A::Retention>
where
    I: IndexDomain,
    A: ArgumentSource<Keyed<I>, Value<'a> = <V as ValueType>::Owned>,
    A::Prepared<'a>: 'a,
    V: ValueType<Value<'a> = <V as ValueType>::Owned>,
    V::Owned: Debug + Display + Send + Sync,
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
                        first: value,
                        second: argument,
                    },
                    &index,
                )),
            })
        })
    })
}

fn ordering_bare<'a, A, V>(
    prepared: A::Prepared<'a>,
    label: &'static str,
    ordering: fn(&V::Owned, &V::Owned) -> Option<Ordering>,
    predicate: fn(Ordering) -> bool,
) -> BarePipeline<'a, V, Mask, A::Retention>
where
    A: ArgumentSource<Unaligned, Value<'a> = <V as ValueType>::Owned>,
    A::Prepared<'a>: 'a,
    V: ValueType<Value<'a> = <V as ValueType>::Owned>,
    V::Owned: Debug + Display + Send + Sync,
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
                        first: value,
                        second: argument,
                    },
                )),
            })
        })
    })
}
