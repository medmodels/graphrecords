use crate::{AttributeName, FailureKindValue, IndexDomain, IndexValue, Scalar, ValueType};
use std::cmp::Ordering;

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
