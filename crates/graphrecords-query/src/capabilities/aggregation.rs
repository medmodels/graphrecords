use super::{ValueEquivalence, ValueOrdering, incomparable_with_first, value_into_view};
use crate::{
    Failure, IndexDomain, IndexValue, Mask, QueryResult, Scalar, ValueDomain,
    error::aggregation::InvalidMedianValue,
};
use chrono::TimeDelta;
use graphrecords_core::graphrecord::{AttributeName, Value};

const NANOSECONDS_PER_SECOND: i128 = 1_000_000_000;

pub trait ValueMedian: ValueOrdering {
    fn validate_median(value: &Self::Value<'_>, label: &'static str) -> QueryResult<()>;

    fn find_incomparable_median_values<'a, 'b: 'a>(
        values: impl Iterator<Item = &'a Self::Value<'b>>,
    ) -> Option<(usize, usize)>;

    fn median<'a>(
        lower: Self::Value<'a>,
        upper: Option<Self::Value<'a>>,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>>;
}

pub trait ValueMode: ValueEquivalence {}

pub trait ValueScalar: ValueDomain {
    fn into_scalar(value: Self::Value<'_>, label: &'static str) -> QueryResult<Value>;

    fn from_scalar<'a>(original: &Self::Value<'_>, value: Value) -> Self::Value<'a>;
}

fn validate_median_value(value: &Value, label: &'static str) -> QueryResult<()> {
    if matches!(
        value,
        Value::Int(_) | Value::Float(_) | Value::DateTime(_) | Value::Duration(_)
    ) {
        Ok(())
    } else {
        Err(Failure::new(InvalidMedianValue::new(value.clone()), label))
    }
}

fn median_value(lower: Value, upper: Option<Value>) -> Value {
    match (lower, upper) {
        (Value::Int(value), None) => Value::Float(value as f64),
        (Value::Float(value), None) => Value::Float(value),
        (Value::DateTime(value), None) => Value::DateTime(value),
        (Value::Duration(value), None) => Value::Duration(value),
        (Value::Int(lower), Some(Value::Int(upper))) => {
            Value::Float((lower as f64).midpoint(upper as f64))
        }
        (Value::Int(lower), Some(Value::Float(upper))) => {
            Value::Float((lower as f64).midpoint(upper))
        }
        (Value::Float(lower), Some(Value::Int(upper))) => {
            Value::Float(lower.midpoint(upper as f64))
        }
        (Value::Float(lower), Some(Value::Float(upper))) => Value::Float(lower.midpoint(upper)),
        (Value::DateTime(lower), Some(Value::DateTime(upper))) => {
            let difference = upper.signed_duration_since(lower);
            let half = difference.checked_div(2).expect("two is a nonzero divisor");

            Value::DateTime(
                lower
                    .checked_add_signed(half)
                    .expect("a datetime midpoint lies between its inputs"),
            )
        }
        (Value::Duration(lower), Some(Value::Duration(upper))) => {
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

            Value::Duration(
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
    fn validate_median(value: &Self::Value<'_>, label: &'static str) -> QueryResult<()> {
        validate_median_value(&Value::from(value.clone()), label)
    }

    fn find_incomparable_median_values<'a, 'b: 'a>(
        values: impl Iterator<Item = &'a Self::Value<'b>>,
    ) -> Option<(usize, usize)> {
        incomparable_with_first(values)
    }

    fn median<'a>(
        lower: Self::Value<'a>,
        upper: Option<Self::Value<'a>>,
        _label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        Ok(value_into_view(median_value(
            Value::from(lower),
            upper.map(Value::from),
        )))
    }
}

impl ValueScalar for Scalar {
    fn into_scalar(value: Self::Value<'_>, _label: &'static str) -> QueryResult<Value> {
        Ok(Value::from(value))
    }

    fn from_scalar<'a>(_original: &Self::Value<'_>, value: Value) -> Self::Value<'a> {
        value_into_view(value)
    }
}

impl ValueMode for Scalar {}

impl ValueMode for Mask {}

impl ValueMode for AttributeName {}

impl ValueMedian for IndexValue<Value> {
    fn validate_median(value: &Self::Value<'_>, label: &'static str) -> QueryResult<()> {
        validate_median_value(value, label)
    }

    fn find_incomparable_median_values<'a, 'b: 'a>(
        values: impl Iterator<Item = &'a Self::Value<'b>>,
    ) -> Option<(usize, usize)> {
        incomparable_with_first(values)
    }

    fn median<'a>(
        lower: Self::Value<'a>,
        upper: Option<Self::Value<'a>>,
        _label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        Ok(median_value(lower, upper))
    }
}

impl ValueScalar for IndexValue<Value> {
    fn into_scalar(value: Self::Value<'_>, _label: &'static str) -> QueryResult<Value> {
        Ok(value)
    }

    fn from_scalar<'a>(_original: &Self::Value<'_>, value: Value) -> Self::Value<'a> {
        value
    }
}

impl<I: IndexDomain> ValueMode for IndexValue<I> {}
