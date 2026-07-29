use crate::{AttributeName, FailureKindValue, IndexDomain, IndexValue, Scalar, ValueType};
use std::cmp::Ordering;

pub trait ValueEquality: ValueType {
    fn equal<'a>(value: &Self::Value<'a>, argument: &Self::Value<'a>) -> bool;
}

pub trait ValueOrdering: ValueEquality {
    fn ordering<'a>(value: &Self::Value<'a>, argument: &Self::Value<'a>) -> Option<Ordering>;
}

impl ValueEquality for Scalar {
    fn equal<'a>(value: &Self::Value<'a>, argument: &Self::Value<'a>) -> bool {
        value == argument
    }
}

impl ValueOrdering for Scalar {
    fn ordering<'a>(value: &Self::Value<'a>, argument: &Self::Value<'a>) -> Option<Ordering> {
        value.partial_cmp(argument)
    }
}

impl ValueEquality for AttributeName {
    fn equal<'a>(value: &Self::Value<'a>, argument: &Self::Value<'a>) -> bool {
        value == argument
    }
}

impl ValueOrdering for AttributeName {
    fn ordering<'a>(value: &Self::Value<'a>, argument: &Self::Value<'a>) -> Option<Ordering> {
        value.partial_cmp(argument)
    }
}

impl<I: IndexDomain> ValueEquality for IndexValue<I> {
    fn equal<'a>(value: &Self::Value<'a>, argument: &Self::Value<'a>) -> bool {
        value == argument
    }
}

impl<I> ValueOrdering for IndexValue<I>
where
    I: IndexDomain,
    I::Owned: PartialOrd,
{
    fn ordering<'a>(value: &Self::Value<'a>, argument: &Self::Value<'a>) -> Option<Ordering> {
        value.partial_cmp(argument)
    }
}

impl ValueEquality for FailureKindValue {
    fn equal<'a>(value: &Self::Value<'a>, argument: &Self::Value<'a>) -> bool {
        value == argument
    }
}
