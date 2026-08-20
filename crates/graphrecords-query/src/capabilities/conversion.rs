use super::value_into_view;
use crate::{
    Failure, IndexValue, QueryResult, Scalar, ValueDomain,
    cast::{
        Bool as BoolTarget, CastTarget, DateTime as DateTimeTarget, Duration as DurationTarget,
        Float as FloatTarget, Int as IntTarget, String as StringTarget,
    },
    error::conversion::InvalidCast,
};
use chrono::{DateTime, NaiveDateTime, TimeDelta};
use graphrecords_core::graphrecord::{
    AttributeName, Identifier, IdentifierView, NodeIndex, NodeIndexView, Value,
    datatypes::{AttributeNameView, DataType},
};
use std::{
    fmt::{Debug, Display},
    time::Duration as StandardDuration,
};

const MILLISECONDS_PER_SECOND: u32 = 1_000;
const NANOSECONDS_PER_MILLISECOND: u32 = 1_000_000;

pub trait ValueCast<T: CastTarget>: ValueDomain {
    fn cast<'a>(
        value: Self::Value<'a>,
        target: &T,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>>;
}

fn invalid_cast<T, U>(value: T, target: DataType, label: &'static str) -> QueryResult<U>
where
    T: Debug + Display + Send + Sync + 'static,
{
    Err(Failure::new(InvalidCast::new(value, target), label))
}

fn duration_from_parts(seconds: u64, nanoseconds: u32, negative: bool) -> Option<TimeDelta> {
    let duration = TimeDelta::from_std(StandardDuration::new(seconds, nanoseconds)).ok()?;

    Some(if negative { -duration } else { duration })
}

fn duration_from_milliseconds(milliseconds: i64) -> Option<TimeDelta> {
    let magnitude = milliseconds.unsigned_abs();

    duration_from_parts(
        magnitude / u64::from(MILLISECONDS_PER_SECOND),
        ((magnitude % u64::from(MILLISECONDS_PER_SECOND)) * u64::from(NANOSECONDS_PER_MILLISECOND))
            as u32,
        milliseconds.is_negative(),
    )
}

fn duration_from_fractional_milliseconds(milliseconds: f64) -> Option<TimeDelta> {
    if !milliseconds.is_finite() {
        return None;
    }

    let duration = StandardDuration::try_from_secs_f64(
        milliseconds.abs() / f64::from(MILLISECONDS_PER_SECOND),
    )
    .ok()?;
    let duration = TimeDelta::from_std(duration).ok()?;

    Some(if milliseconds.is_sign_negative() {
        -duration
    } else {
        duration
    })
}

const fn datetime_from_duration(duration: TimeDelta) -> Option<NaiveDateTime> {
    DateTime::UNIX_EPOCH
        .naive_utc()
        .checked_add_signed(duration)
}

fn parse_duration(value: &str) -> Option<TimeDelta> {
    if value == "P0D" {
        return duration_from_parts(0, 0, false);
    }

    let (negative, value) = match value.strip_prefix('-') {
        Some(value) => (true, value),
        None => (false, value),
    };
    let value = value.strip_prefix("PT")?.strip_suffix('S')?;
    let (seconds, nanoseconds) = match value.split_once('.') {
        Some((seconds, fraction)) => {
            if fraction.is_empty()
                || fraction.len() > 9
                || !fraction.bytes().all(|character| character.is_ascii_digit())
            {
                return None;
            }

            let nanoseconds = fraction.parse::<u32>().ok()? * 10_u32.pow(9 - fraction.len() as u32);

            (seconds, nanoseconds)
        }
        None => (value, 0),
    };

    if seconds.is_empty() || !seconds.bytes().all(|character| character.is_ascii_digit()) {
        return None;
    }

    duration_from_parts(seconds.parse().ok()?, nanoseconds, negative)
}

fn cast_value_to_bool(value: Value, label: &'static str) -> QueryResult<Value> {
    match value {
        Value::String(value) => match value.parse() {
            Ok(value) => Ok(Value::Bool(value)),
            Err(_) => invalid_cast(Value::String(value), DataType::Bool, label),
        },
        Value::Int(value) => Ok(Value::Bool(value != 0)),
        Value::Float(value) => Ok(Value::Bool(value != 0.0)),
        Value::Bool(_) => Ok(value),
        value @ (Value::DateTime(_) | Value::Duration(_) | Value::Null) => {
            invalid_cast(value, DataType::Bool, label)
        }
    }
}

fn cast_value_to_datetime(value: Value, label: &'static str) -> QueryResult<Value> {
    match value {
        Value::String(value) => match value.parse().map(Value::DateTime) {
            Ok(value) => Ok(value),
            Err(_) => invalid_cast(Value::String(value), DataType::DateTime, label),
        },
        Value::Int(value) => duration_from_milliseconds(value)
            .and_then(datetime_from_duration)
            .map(Value::DateTime)
            .ok_or_else(|| {
                Failure::new(
                    InvalidCast::new(Value::Int(value), DataType::DateTime),
                    label,
                )
            }),
        Value::Float(value) => duration_from_fractional_milliseconds(value)
            .and_then(datetime_from_duration)
            .map(Value::DateTime)
            .ok_or_else(|| {
                Failure::new(
                    InvalidCast::new(Value::Float(value), DataType::DateTime),
                    label,
                )
            }),
        Value::DateTime(_) => Ok(value),
        value @ (Value::Bool(_) | Value::Duration(_) | Value::Null) => {
            invalid_cast(value, DataType::DateTime, label)
        }
    }
}

fn cast_value_to_duration(value: Value, label: &'static str) -> QueryResult<Value> {
    match value {
        Value::String(value) => parse_duration(&value).map(Value::Duration).ok_or_else(|| {
            Failure::new(
                InvalidCast::new(Value::String(value), DataType::Duration),
                label,
            )
        }),
        Value::Int(value) => duration_from_milliseconds(value)
            .map(Value::Duration)
            .ok_or_else(|| {
                Failure::new(
                    InvalidCast::new(Value::Int(value), DataType::Duration),
                    label,
                )
            }),
        Value::Float(value) => duration_from_fractional_milliseconds(value)
            .map(Value::Duration)
            .ok_or_else(|| {
                Failure::new(
                    InvalidCast::new(Value::Float(value), DataType::Duration),
                    label,
                )
            }),
        Value::Duration(_) => Ok(value),
        value @ (Value::Bool(_) | Value::DateTime(_) | Value::Null) => {
            invalid_cast(value, DataType::Duration, label)
        }
    }
}

fn cast_value_to_float(value: Value, label: &'static str) -> QueryResult<Value> {
    match value {
        Value::String(value) => match value.parse() {
            Ok(value) => Ok(Value::Float(value)),
            Err(_) => invalid_cast(Value::String(value), DataType::Float, label),
        },
        Value::Int(value) => Ok(Value::Float(value as f64)),
        Value::Float(_) => Ok(value),
        Value::Bool(value) => Ok(Value::Float(if value { 1.0 } else { 0.0 })),
        Value::DateTime(value) => {
            let datetime = value.and_utc();
            let milliseconds = datetime.timestamp_millis() as f64
                + f64::from(datetime.timestamp_subsec_nanos() % NANOSECONDS_PER_MILLISECOND)
                    / f64::from(NANOSECONDS_PER_MILLISECOND);

            Ok(Value::Float(milliseconds))
        }
        Value::Duration(value) => Ok(Value::Float(
            value.as_seconds_f64() * f64::from(MILLISECONDS_PER_SECOND),
        )),
        Value::Null => invalid_cast(Value::Null, DataType::Float, label),
    }
}

fn cast_value_to_int(value: Value, label: &'static str) -> QueryResult<Value> {
    match value {
        Value::String(value) => match value.parse() {
            Ok(value) => Ok(Value::Int(value)),
            Err(_) => invalid_cast(Value::String(value), DataType::Int, label),
        },
        Value::Int(_) => Ok(value),
        Value::Float(value)
            if value.is_finite() && value >= i64::MIN as f64 && value < -(i64::MIN as f64) =>
        {
            Ok(Value::Int(value as i64))
        }
        Value::Float(value) => invalid_cast(Value::Float(value), DataType::Int, label),
        Value::Bool(value) => Ok(Value::Int(i64::from(value))),
        Value::DateTime(value) => Ok(Value::Int(value.and_utc().timestamp_millis())),
        Value::Duration(value) => Ok(Value::Int(value.num_milliseconds())),
        Value::Null => invalid_cast(Value::Null, DataType::Int, label),
    }
}

fn cast_value_to_string(value: Value) -> Value {
    Value::String(match value {
        Value::String(value) => value,
        Value::Int(value) => value.to_string(),
        Value::Float(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        Value::DateTime(value) => value.format("%Y-%m-%dT%H:%M:%S%.f").to_string(),
        Value::Duration(value) => value.to_string(),
        Value::Null => "Null".to_string(),
    })
}

impl ValueCast<BoolTarget> for Scalar {
    fn cast<'a>(
        value: Self::Value<'a>,
        _target: &BoolTarget,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        cast_value_to_bool(Value::from(value), label).map(value_into_view)
    }
}

impl ValueCast<DateTimeTarget> for Scalar {
    fn cast<'a>(
        value: Self::Value<'a>,
        _target: &DateTimeTarget,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        cast_value_to_datetime(Value::from(value), label).map(value_into_view)
    }
}

impl ValueCast<DurationTarget> for Scalar {
    fn cast<'a>(
        value: Self::Value<'a>,
        _target: &DurationTarget,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        cast_value_to_duration(Value::from(value), label).map(value_into_view)
    }
}

impl ValueCast<FloatTarget> for Scalar {
    fn cast<'a>(
        value: Self::Value<'a>,
        _target: &FloatTarget,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        cast_value_to_float(Value::from(value), label).map(value_into_view)
    }
}

impl ValueCast<IntTarget> for Scalar {
    fn cast<'a>(
        value: Self::Value<'a>,
        _target: &IntTarget,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        cast_value_to_int(Value::from(value), label).map(value_into_view)
    }
}

impl ValueCast<StringTarget> for Scalar {
    fn cast<'a>(
        value: Self::Value<'a>,
        _target: &StringTarget,
        _label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        Ok(value_into_view(cast_value_to_string(Value::from(value))))
    }
}

impl ValueCast<IntTarget> for AttributeName {
    fn cast<'a>(
        value: Self::Value<'a>,
        _target: &IntTarget,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        match value.identifier_view() {
            IdentifierView::String(string) => match string.parse::<i64>() {
                Ok(integer) => Ok(AttributeNameView::from(IdentifierView::Int(integer))),
                Err(_) => invalid_cast(Self::from(value), DataType::Int, label),
            },
            IdentifierView::Int(_) => Ok(value),
        }
    }
}

impl ValueCast<StringTarget> for AttributeName {
    fn cast<'a>(
        value: Self::Value<'a>,
        _target: &StringTarget,
        _label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        Ok(match Identifier::from(Self::from(value)) {
            Identifier::String(string) => {
                AttributeNameView::from(IdentifierView::String(string.into()))
            }
            Identifier::Int(integer) => {
                AttributeNameView::from(IdentifierView::String(integer.to_string().into()))
            }
        })
    }
}

impl ValueCast<BoolTarget> for IndexValue<Value> {
    fn cast<'a>(
        value: Self::Value<'a>,
        _target: &BoolTarget,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        cast_value_to_bool(value, label)
    }
}

impl ValueCast<DateTimeTarget> for IndexValue<Value> {
    fn cast<'a>(
        value: Self::Value<'a>,
        _target: &DateTimeTarget,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        cast_value_to_datetime(value, label)
    }
}

impl ValueCast<DurationTarget> for IndexValue<Value> {
    fn cast<'a>(
        value: Self::Value<'a>,
        _target: &DurationTarget,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        cast_value_to_duration(value, label)
    }
}

impl ValueCast<FloatTarget> for IndexValue<Value> {
    fn cast<'a>(
        value: Self::Value<'a>,
        _target: &FloatTarget,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        cast_value_to_float(value, label)
    }
}

impl ValueCast<IntTarget> for IndexValue<Value> {
    fn cast<'a>(
        value: Self::Value<'a>,
        _target: &IntTarget,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        cast_value_to_int(value, label)
    }
}

impl ValueCast<StringTarget> for IndexValue<Value> {
    fn cast<'a>(
        value: Self::Value<'a>,
        _target: &StringTarget,
        _label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        Ok(cast_value_to_string(value))
    }
}

impl ValueCast<IntTarget> for IndexValue<NodeIndex> {
    fn cast<'a>(
        value: Self::Value<'a>,
        _target: &IntTarget,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        match value.identifier_view() {
            IdentifierView::String(string) => match string.parse::<i64>() {
                Ok(integer) => Ok(NodeIndexView::from(IdentifierView::Int(integer))),
                Err(_) => invalid_cast(NodeIndex::from(value), DataType::Int, label),
            },
            IdentifierView::Int(_) => Ok(value),
        }
    }
}

impl ValueCast<StringTarget> for IndexValue<NodeIndex> {
    fn cast<'a>(
        value: Self::Value<'a>,
        _target: &StringTarget,
        _label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        Ok(match Identifier::from(NodeIndex::from(value)) {
            Identifier::String(string) => {
                NodeIndexView::from(IdentifierView::String(string.into()))
            }
            Identifier::Int(integer) => {
                NodeIndexView::from(IdentifierView::String(integer.to_string().into()))
            }
        })
    }
}

impl ValueCast<IntTarget> for IndexValue<AttributeName> {
    fn cast<'a>(
        value: Self::Value<'a>,
        _target: &IntTarget,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        match value.identifier_view() {
            IdentifierView::String(string) => match string.parse::<i64>() {
                Ok(integer) => Ok(AttributeNameView::from(IdentifierView::Int(integer))),
                Err(_) => invalid_cast(AttributeName::from(value), DataType::Int, label),
            },
            IdentifierView::Int(_) => Ok(value),
        }
    }
}

impl ValueCast<StringTarget> for IndexValue<AttributeName> {
    fn cast<'a>(
        value: Self::Value<'a>,
        _target: &StringTarget,
        _label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        Ok(match Identifier::from(AttributeName::from(value)) {
            Identifier::String(string) => {
                AttributeNameView::from(IdentifierView::String(string.into()))
            }
            Identifier::Int(integer) => {
                AttributeNameView::from(IdentifierView::String(integer.to_string().into()))
            }
        })
    }
}
