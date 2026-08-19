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
    AttributeName, Identifier, NodeIndex, Value, datatypes::DataType,
};
use std::{
    fmt::{Debug, Display},
    time::Duration as StandardDuration,
};

pub trait ValueCast<T: CastTarget>: ValueDomain {
    fn cast<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        target: &T,
    ) -> QueryResult<Self::Value<'a>>;
}

fn invalid_cast<T, U>(label: &'static str, value: T, target: DataType) -> QueryResult<U>
where
    T: Debug + Display + Send + Sync + 'static,
{
    Err(Failure::new(label, InvalidCast::new(value, target)))
}

fn duration_from_parts(seconds: u64, nanoseconds: u32, negative: bool) -> Option<TimeDelta> {
    let duration = TimeDelta::from_std(StandardDuration::new(seconds, nanoseconds)).ok()?;

    Some(if negative { -duration } else { duration })
}

fn duration_from_milliseconds(milliseconds: i64) -> Option<TimeDelta> {
    let magnitude = milliseconds.unsigned_abs();

    duration_from_parts(
        magnitude / 1_000,
        ((magnitude % 1_000) * 1_000_000) as u32,
        milliseconds.is_negative(),
    )
}

fn duration_from_fractional_milliseconds(milliseconds: f64) -> Option<TimeDelta> {
    if !milliseconds.is_finite() {
        return None;
    }

    let duration = StandardDuration::try_from_secs_f64(milliseconds.abs() / 1_000.0).ok()?;
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

fn cast_value_to_bool(label: &'static str, value: Value) -> QueryResult<Value> {
    match value {
        Value::String(value) => match value.parse() {
            Ok(value) => Ok(Value::Bool(value)),
            Err(_) => invalid_cast(label, Value::String(value), DataType::Bool),
        },
        Value::Int(value) => Ok(Value::Bool(value != 0)),
        Value::Float(value) => Ok(Value::Bool(value != 0.0)),
        Value::Bool(_) => Ok(value),
        value @ (Value::DateTime(_) | Value::Duration(_) | Value::Null) => {
            invalid_cast(label, value, DataType::Bool)
        }
    }
}

fn cast_value_to_datetime(label: &'static str, value: Value) -> QueryResult<Value> {
    match value {
        Value::String(value) => match value.parse().map(Value::DateTime) {
            Ok(value) => Ok(value),
            Err(_) => invalid_cast(label, Value::String(value), DataType::DateTime),
        },
        Value::Int(value) => duration_from_milliseconds(value)
            .and_then(datetime_from_duration)
            .map(Value::DateTime)
            .ok_or_else(|| {
                Failure::new(
                    label,
                    InvalidCast::new(Value::Int(value), DataType::DateTime),
                )
            }),
        Value::Float(value) => duration_from_fractional_milliseconds(value)
            .and_then(datetime_from_duration)
            .map(Value::DateTime)
            .ok_or_else(|| {
                Failure::new(
                    label,
                    InvalidCast::new(Value::Float(value), DataType::DateTime),
                )
            }),
        Value::DateTime(_) => Ok(value),
        value @ (Value::Bool(_) | Value::Duration(_) | Value::Null) => {
            invalid_cast(label, value, DataType::DateTime)
        }
    }
}

fn cast_value_to_duration(label: &'static str, value: Value) -> QueryResult<Value> {
    match value {
        Value::String(value) => parse_duration(&value).map(Value::Duration).ok_or_else(|| {
            Failure::new(
                label,
                InvalidCast::new(Value::String(value), DataType::Duration),
            )
        }),
        Value::Int(value) => duration_from_milliseconds(value)
            .map(Value::Duration)
            .ok_or_else(|| {
                Failure::new(
                    label,
                    InvalidCast::new(Value::Int(value), DataType::Duration),
                )
            }),
        Value::Float(value) => duration_from_fractional_milliseconds(value)
            .map(Value::Duration)
            .ok_or_else(|| {
                Failure::new(
                    label,
                    InvalidCast::new(Value::Float(value), DataType::Duration),
                )
            }),
        Value::Duration(_) => Ok(value),
        value @ (Value::Bool(_) | Value::DateTime(_) | Value::Null) => {
            invalid_cast(label, value, DataType::Duration)
        }
    }
}

fn cast_value_to_float(label: &'static str, value: Value) -> QueryResult<Value> {
    match value {
        Value::String(value) => match value.parse() {
            Ok(value) => Ok(Value::Float(value)),
            Err(_) => invalid_cast(label, Value::String(value), DataType::Float),
        },
        Value::Int(value) => Ok(Value::Float(value as f64)),
        Value::Float(_) => Ok(value),
        Value::Bool(value) => Ok(Value::Float(if value { 1.0 } else { 0.0 })),
        Value::DateTime(value) => {
            let datetime = value.and_utc();
            let milliseconds = datetime.timestamp_millis() as f64
                + f64::from(datetime.timestamp_subsec_nanos() % 1_000_000) / 1_000_000.0;

            Ok(Value::Float(milliseconds))
        }
        Value::Duration(value) => Ok(Value::Float(value.as_seconds_f64() * 1_000.0)),
        Value::Null => invalid_cast(label, Value::Null, DataType::Float),
    }
}

fn cast_value_to_int(label: &'static str, value: Value) -> QueryResult<Value> {
    match value {
        Value::String(value) => match value.parse() {
            Ok(value) => Ok(Value::Int(value)),
            Err(_) => invalid_cast(label, Value::String(value), DataType::Int),
        },
        Value::Int(_) => Ok(value),
        Value::Float(value)
            if value.is_finite() && value >= i64::MIN as f64 && value < -(i64::MIN as f64) =>
        {
            Ok(Value::Int(value as i64))
        }
        Value::Float(value) => invalid_cast(label, Value::Float(value), DataType::Int),
        Value::Bool(value) => Ok(Value::Int(i64::from(value))),
        Value::DateTime(value) => Ok(Value::Int(value.and_utc().timestamp_millis())),
        Value::Duration(value) => Ok(Value::Int(value.num_milliseconds())),
        Value::Null => invalid_cast(label, Value::Null, DataType::Int),
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
        label: &'static str,
        value: Self::Value<'a>,
        _target: &BoolTarget,
    ) -> QueryResult<Self::Value<'a>> {
        cast_value_to_bool(label, value)
    }
}

impl ValueCast<DateTimeTarget> for Scalar {
    fn cast<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        _target: &DateTimeTarget,
    ) -> QueryResult<Self::Value<'a>> {
        cast_value_to_datetime(label, value)
    }
}

impl ValueCast<DurationTarget> for Scalar {
    fn cast<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        _target: &DurationTarget,
    ) -> QueryResult<Self::Value<'a>> {
        cast_value_to_duration(label, value)
    }
}

impl ValueCast<FloatTarget> for Scalar {
    fn cast<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        _target: &FloatTarget,
    ) -> QueryResult<Self::Value<'a>> {
        cast_value_to_float(label, value)
    }
}

impl ValueCast<IntTarget> for Scalar {
    fn cast<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        _target: &IntTarget,
    ) -> QueryResult<Self::Value<'a>> {
        cast_value_to_int(label, value)
    }
}

impl ValueCast<StringTarget> for Scalar {
    fn cast<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
        _target: &StringTarget,
    ) -> QueryResult<Self::Value<'a>> {
        Ok(cast_value_to_string(value))
    }
}

impl ValueCast<IntTarget> for AttributeName {
    fn cast<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        _target: &IntTarget,
    ) -> QueryResult<Self::Value<'a>> {
        match value.identifier() {
            Identifier::String(string) => match string.parse::<i64>() {
                Ok(integer) => Ok(Self::from(integer)),
                Err(_) => invalid_cast(label, value, DataType::Int),
            },
            Identifier::Int(_) => Ok(value),
        }
    }
}

impl ValueCast<StringTarget> for AttributeName {
    fn cast<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
        _target: &StringTarget,
    ) -> QueryResult<Self::Value<'a>> {
        Ok(match Identifier::from(value) {
            Identifier::String(string) => Self::from(string),
            Identifier::Int(integer) => Self::from(integer.to_string()),
        })
    }
}

impl ValueCast<BoolTarget> for IndexValue<Value> {
    fn cast<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        _target: &BoolTarget,
    ) -> QueryResult<Self::Value<'a>> {
        cast_value_to_bool(label, value)
    }
}

impl ValueCast<DateTimeTarget> for IndexValue<Value> {
    fn cast<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        _target: &DateTimeTarget,
    ) -> QueryResult<Self::Value<'a>> {
        cast_value_to_datetime(label, value)
    }
}

impl ValueCast<DurationTarget> for IndexValue<Value> {
    fn cast<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        _target: &DurationTarget,
    ) -> QueryResult<Self::Value<'a>> {
        cast_value_to_duration(label, value)
    }
}

impl ValueCast<FloatTarget> for IndexValue<Value> {
    fn cast<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        _target: &FloatTarget,
    ) -> QueryResult<Self::Value<'a>> {
        cast_value_to_float(label, value)
    }
}

impl ValueCast<IntTarget> for IndexValue<Value> {
    fn cast<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        _target: &IntTarget,
    ) -> QueryResult<Self::Value<'a>> {
        cast_value_to_int(label, value)
    }
}

impl ValueCast<StringTarget> for IndexValue<Value> {
    fn cast<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
        _target: &StringTarget,
    ) -> QueryResult<Self::Value<'a>> {
        Ok(cast_value_to_string(value))
    }
}

impl ValueCast<IntTarget> for IndexValue<NodeIndex> {
    fn cast<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        _target: &IntTarget,
    ) -> QueryResult<Self::Value<'a>> {
        match value.identifier() {
            Identifier::String(string) => match string.parse::<i64>() {
                Ok(integer) => Ok(NodeIndex::from(integer)),
                Err(_) => invalid_cast(label, value, DataType::Int),
            },
            Identifier::Int(_) => Ok(value),
        }
    }
}

impl ValueCast<StringTarget> for IndexValue<NodeIndex> {
    fn cast<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
        _target: &StringTarget,
    ) -> QueryResult<Self::Value<'a>> {
        Ok(match Identifier::from(value) {
            Identifier::String(string) => NodeIndex::from(string),
            Identifier::Int(integer) => NodeIndex::from(integer.to_string()),
        })
    }
}

impl ValueCast<IntTarget> for IndexValue<AttributeName> {
    fn cast<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        _target: &IntTarget,
    ) -> QueryResult<Self::Value<'a>> {
        match value.identifier() {
            Identifier::String(string) => match string.parse::<i64>() {
                Ok(integer) => Ok(AttributeName::from(integer)),
                Err(_) => invalid_cast(label, value, DataType::Int),
            },
            Identifier::Int(_) => Ok(value),
        }
    }
}

impl ValueCast<StringTarget> for IndexValue<AttributeName> {
    fn cast<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
        _target: &StringTarget,
    ) -> QueryResult<Self::Value<'a>> {
        Ok(match Identifier::from(value) {
            Identifier::String(string) => AttributeName::from(string),
            Identifier::Int(integer) => AttributeName::from(integer.to_string()),
        })
    }
}
