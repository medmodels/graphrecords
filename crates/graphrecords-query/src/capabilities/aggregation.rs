use super::{ValueOrdering, incomparable_with_first};
use crate::{
    AttributeName, EntityDomain, EntityReference, Failure, FailureKind, FailureKindValue,
    IndexDomain, IndexValue, Mask, QueryResult, ReturnValueType, Scalar, ValueType,
    error::aggregation::InvalidMedianValue,
};
use chrono::TimeDelta;
use graphrecords_core::graphrecord::{GraphRecordAttribute, GraphRecordValue};
use std::hash::Hash;

const NANOSECONDS_PER_SECOND: i128 = 1_000_000_000;

pub trait ValueMedian: ValueOrdering {
    fn validate_median(label: &'static str, value: &Self::Value<'_>) -> QueryResult<()>;

    fn find_incomparable_median_values<'a, 'b>(
        values: impl Iterator<Item = &'a Self::Value<'b>>,
    ) -> Option<(usize, usize)>
    where
        Self::Value<'b>: 'a;

    fn median<'a>(
        label: &'static str,
        lower: Self::Value<'a>,
        upper: Option<Self::Value<'a>>,
    ) -> QueryResult<Self::Value<'a>>;
}

pub trait ValueMode: ReturnValueType {
    type Key: Eq + Hash;

    fn mode_key(value: &Self::Value<'_>) -> Self::Key;
}

pub trait ValueUniqueCount: ValueType {
    type Key: Eq + Hash;

    fn unique_count_key(value: &Self::Value<'_>) -> Self::Key;
}

fn validate_graphrecord_median_value(
    label: &'static str,
    value: &GraphRecordValue,
) -> QueryResult<()> {
    if matches!(
        value,
        GraphRecordValue::Int(_)
            | GraphRecordValue::Float(_)
            | GraphRecordValue::DateTime(_)
            | GraphRecordValue::Duration(_)
    ) {
        Ok(())
    } else {
        Err(Failure::new(label, InvalidMedianValue::new(value.clone())))
    }
}

fn median_graphrecord_value(
    lower: GraphRecordValue,
    upper: Option<GraphRecordValue>,
) -> GraphRecordValue {
    match (lower, upper) {
        (GraphRecordValue::Int(value), None) => GraphRecordValue::Float(value as f64),
        (GraphRecordValue::Float(value), None) => GraphRecordValue::Float(value),
        (GraphRecordValue::DateTime(value), None) => GraphRecordValue::DateTime(value),
        (GraphRecordValue::Duration(value), None) => GraphRecordValue::Duration(value),
        (GraphRecordValue::Int(lower), Some(GraphRecordValue::Int(upper))) => {
            GraphRecordValue::Float((lower as f64).midpoint(upper as f64))
        }
        (GraphRecordValue::Int(lower), Some(GraphRecordValue::Float(upper))) => {
            GraphRecordValue::Float((lower as f64).midpoint(upper))
        }
        (GraphRecordValue::Float(lower), Some(GraphRecordValue::Int(upper))) => {
            GraphRecordValue::Float(lower.midpoint(upper as f64))
        }
        (GraphRecordValue::Float(lower), Some(GraphRecordValue::Float(upper))) => {
            GraphRecordValue::Float(lower.midpoint(upper))
        }
        (GraphRecordValue::DateTime(lower), Some(GraphRecordValue::DateTime(upper))) => {
            let difference = upper.signed_duration_since(lower);
            let half = difference.checked_div(2).expect("two is a nonzero divisor");

            GraphRecordValue::DateTime(
                lower
                    .checked_add_signed(half)
                    .expect("a datetime midpoint lies between its inputs"),
            )
        }
        (GraphRecordValue::Duration(lower), Some(GraphRecordValue::Duration(upper))) => {
            let lower = i128::from(lower.num_seconds()) * NANOSECONDS_PER_SECOND
                + i128::from(lower.subsec_nanos());
            let upper = i128::from(upper.num_seconds()) * NANOSECONDS_PER_SECOND
                + i128::from(upper.subsec_nanos());
            let midpoint = lower
                .checked_add(upper)
                .expect("two durations fit within i128 nanoseconds")
                / 2;
            let seconds = midpoint.div_euclid(NANOSECONDS_PER_SECOND);
            let nanoseconds = midpoint.rem_euclid(NANOSECONDS_PER_SECOND);

            GraphRecordValue::Duration(
                TimeDelta::new(
                    i64::try_from(seconds).expect("a duration midpoint fits in i64 seconds"),
                    u32::try_from(nanoseconds).expect("subsecond nanoseconds fit in a u32"),
                )
                .expect("a duration midpoint lies between its inputs"),
            )
        }
        _ => unreachable!("median values were validated and checked for comparability"),
    }
}

impl ValueMedian for Scalar {
    fn validate_median(label: &'static str, value: &Self::Value<'_>) -> QueryResult<()> {
        validate_graphrecord_median_value(label, value)
    }

    fn find_incomparable_median_values<'a, 'b>(
        values: impl Iterator<Item = &'a Self::Value<'b>>,
    ) -> Option<(usize, usize)>
    where
        Self::Value<'b>: 'a,
    {
        incomparable_with_first(values)
    }

    fn median<'a>(
        _label: &'static str,
        lower: Self::Value<'a>,
        upper: Option<Self::Value<'a>>,
    ) -> QueryResult<Self::Value<'a>> {
        Ok(median_graphrecord_value(lower, upper))
    }
}

impl ValueMode for Scalar {
    type Key = GraphRecordValue;

    fn mode_key(value: &Self::Value<'_>) -> Self::Key {
        value.clone()
    }
}

impl ValueUniqueCount for Scalar {
    type Key = GraphRecordValue;

    fn unique_count_key(value: &Self::Value<'_>) -> Self::Key {
        value.clone()
    }
}

impl ValueMode for Mask {
    type Key = bool;

    fn mode_key(value: &Self::Value<'_>) -> Self::Key {
        *value
    }
}

impl ValueUniqueCount for Mask {
    type Key = bool;

    fn unique_count_key(value: &Self::Value<'_>) -> Self::Key {
        *value
    }
}

impl ValueMode for AttributeName {
    type Key = GraphRecordAttribute;

    fn mode_key(value: &Self::Value<'_>) -> Self::Key {
        value.clone()
    }
}

impl ValueUniqueCount for AttributeName {
    type Key = GraphRecordAttribute;

    fn unique_count_key(value: &Self::Value<'_>) -> Self::Key {
        value.clone()
    }
}

impl ValueMedian for IndexValue<GraphRecordValue> {
    fn validate_median(label: &'static str, value: &Self::Value<'_>) -> QueryResult<()> {
        validate_graphrecord_median_value(label, value)
    }

    fn find_incomparable_median_values<'a, 'b>(
        values: impl Iterator<Item = &'a Self::Value<'b>>,
    ) -> Option<(usize, usize)>
    where
        Self::Value<'b>: 'a,
    {
        incomparable_with_first(values)
    }

    fn median<'a>(
        _label: &'static str,
        lower: Self::Value<'a>,
        upper: Option<Self::Value<'a>>,
    ) -> QueryResult<Self::Value<'a>> {
        Ok(median_graphrecord_value(lower, upper))
    }
}

impl<I: IndexDomain> ValueMode for IndexValue<I> {
    type Key = I::Owned;

    fn mode_key(value: &Self::Value<'_>) -> Self::Key {
        value.clone()
    }
}

impl<I: IndexDomain> ValueUniqueCount for IndexValue<I> {
    type Key = I::Owned;

    fn unique_count_key(value: &Self::Value<'_>) -> Self::Key {
        value.clone()
    }
}

impl<E: EntityDomain> ValueUniqueCount for EntityReference<E> {
    type Key = E::Owned;

    fn unique_count_key(value: &Self::Value<'_>) -> Self::Key {
        E::to_owned(value)
    }
}

impl ValueUniqueCount for FailureKindValue {
    type Key = FailureKind;

    fn unique_count_key(value: &Self::Value<'_>) -> Self::Key {
        *value
    }
}
