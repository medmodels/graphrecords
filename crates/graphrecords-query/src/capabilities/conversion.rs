use crate::{
    AttributeName, Failure, IndexValue, QueryResult, Scalar, ValueDomain,
    cast::{
        Bool as BoolTarget, CastTarget, DateTime as DateTimeTarget, Duration as DurationTarget,
        Float as FloatTarget, Int as IntTarget, String as StringTarget,
    },
    error::conversion::InvalidCast,
};
use chrono::{DateTime, NaiveDateTime, TimeDelta};
use graphrecords_core::graphrecord::{
    GraphRecordAttribute, GraphRecordValue, NodeIndex, datatypes::DataType,
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

fn cast_value_to_bool(
    label: &'static str,
    value: GraphRecordValue,
) -> QueryResult<GraphRecordValue> {
    match value {
        GraphRecordValue::String(value) => match value.parse() {
            Ok(value) => Ok(GraphRecordValue::Bool(value)),
            Err(_) => invalid_cast(label, GraphRecordValue::String(value), DataType::Bool),
        },
        GraphRecordValue::Int(value) => Ok(GraphRecordValue::Bool(value != 0)),
        GraphRecordValue::Float(value) => Ok(GraphRecordValue::Bool(value != 0.0)),
        GraphRecordValue::Bool(_) => Ok(value),
        value @ (GraphRecordValue::DateTime(_)
        | GraphRecordValue::Duration(_)
        | GraphRecordValue::Null) => invalid_cast(label, value, DataType::Bool),
    }
}

fn cast_value_to_datetime(
    label: &'static str,
    value: GraphRecordValue,
) -> QueryResult<GraphRecordValue> {
    match value {
        GraphRecordValue::String(value) => match value.parse().map(GraphRecordValue::DateTime) {
            Ok(value) => Ok(value),
            Err(_) => invalid_cast(label, GraphRecordValue::String(value), DataType::DateTime),
        },
        GraphRecordValue::Int(value) => duration_from_milliseconds(value)
            .and_then(datetime_from_duration)
            .map(GraphRecordValue::DateTime)
            .ok_or_else(|| {
                Failure::new(
                    label,
                    InvalidCast::new(GraphRecordValue::Int(value), DataType::DateTime),
                )
            }),
        GraphRecordValue::Float(value) => duration_from_fractional_milliseconds(value)
            .and_then(datetime_from_duration)
            .map(GraphRecordValue::DateTime)
            .ok_or_else(|| {
                Failure::new(
                    label,
                    InvalidCast::new(GraphRecordValue::Float(value), DataType::DateTime),
                )
            }),
        GraphRecordValue::DateTime(_) => Ok(value),
        value @ (GraphRecordValue::Bool(_)
        | GraphRecordValue::Duration(_)
        | GraphRecordValue::Null) => invalid_cast(label, value, DataType::DateTime),
    }
}

fn cast_value_to_duration(
    label: &'static str,
    value: GraphRecordValue,
) -> QueryResult<GraphRecordValue> {
    match value {
        GraphRecordValue::String(value) => parse_duration(&value)
            .map(GraphRecordValue::Duration)
            .ok_or_else(|| {
                Failure::new(
                    label,
                    InvalidCast::new(GraphRecordValue::String(value), DataType::Duration),
                )
            }),
        GraphRecordValue::Int(value) => duration_from_milliseconds(value)
            .map(GraphRecordValue::Duration)
            .ok_or_else(|| {
                Failure::new(
                    label,
                    InvalidCast::new(GraphRecordValue::Int(value), DataType::Duration),
                )
            }),
        GraphRecordValue::Float(value) => duration_from_fractional_milliseconds(value)
            .map(GraphRecordValue::Duration)
            .ok_or_else(|| {
                Failure::new(
                    label,
                    InvalidCast::new(GraphRecordValue::Float(value), DataType::Duration),
                )
            }),
        GraphRecordValue::Duration(_) => Ok(value),
        value @ (GraphRecordValue::Bool(_)
        | GraphRecordValue::DateTime(_)
        | GraphRecordValue::Null) => invalid_cast(label, value, DataType::Duration),
    }
}

fn cast_value_to_float(
    label: &'static str,
    value: GraphRecordValue,
) -> QueryResult<GraphRecordValue> {
    match value {
        GraphRecordValue::String(value) => match value.parse() {
            Ok(value) => Ok(GraphRecordValue::Float(value)),
            Err(_) => invalid_cast(label, GraphRecordValue::String(value), DataType::Float),
        },
        GraphRecordValue::Int(value) => Ok(GraphRecordValue::Float(value as f64)),
        GraphRecordValue::Float(_) => Ok(value),
        GraphRecordValue::Bool(value) => Ok(GraphRecordValue::Float(if value { 1.0 } else { 0.0 })),
        GraphRecordValue::DateTime(value) => {
            let datetime = value.and_utc();
            let milliseconds = datetime.timestamp_millis() as f64
                + f64::from(datetime.timestamp_subsec_nanos() % 1_000_000) / 1_000_000.0;

            Ok(GraphRecordValue::Float(milliseconds))
        }
        GraphRecordValue::Duration(value) => {
            Ok(GraphRecordValue::Float(value.as_seconds_f64() * 1_000.0))
        }
        GraphRecordValue::Null => invalid_cast(label, GraphRecordValue::Null, DataType::Float),
    }
}

fn cast_value_to_int(
    label: &'static str,
    value: GraphRecordValue,
) -> QueryResult<GraphRecordValue> {
    match value {
        GraphRecordValue::String(value) => match value.parse() {
            Ok(value) => Ok(GraphRecordValue::Int(value)),
            Err(_) => invalid_cast(label, GraphRecordValue::String(value), DataType::Int),
        },
        GraphRecordValue::Int(_) => Ok(value),
        GraphRecordValue::Float(value)
            if value.is_finite() && value >= i64::MIN as f64 && value < -(i64::MIN as f64) =>
        {
            Ok(GraphRecordValue::Int(value as i64))
        }
        GraphRecordValue::Float(value) => {
            invalid_cast(label, GraphRecordValue::Float(value), DataType::Int)
        }
        GraphRecordValue::Bool(value) => Ok(GraphRecordValue::Int(i64::from(value))),
        GraphRecordValue::DateTime(value) => {
            Ok(GraphRecordValue::Int(value.and_utc().timestamp_millis()))
        }
        GraphRecordValue::Duration(value) => Ok(GraphRecordValue::Int(value.num_milliseconds())),
        GraphRecordValue::Null => invalid_cast(label, GraphRecordValue::Null, DataType::Int),
    }
}

fn cast_value_to_string(value: GraphRecordValue) -> GraphRecordValue {
    GraphRecordValue::String(match value {
        GraphRecordValue::String(value) => value,
        GraphRecordValue::Int(value) => value.to_string(),
        GraphRecordValue::Float(value) => value.to_string(),
        GraphRecordValue::Bool(value) => value.to_string(),
        GraphRecordValue::DateTime(value) => value.format("%Y-%m-%dT%H:%M:%S%.f").to_string(),
        GraphRecordValue::Duration(value) => value.to_string(),
        GraphRecordValue::Null => "Null".to_string(),
    })
}

fn cast_attribute_to_int(
    label: &'static str,
    value: GraphRecordAttribute,
) -> QueryResult<GraphRecordAttribute> {
    match value {
        GraphRecordAttribute::String(value) => match value.parse() {
            Ok(value) => Ok(GraphRecordAttribute::Int(value)),
            Err(_) => invalid_cast(label, GraphRecordAttribute::String(value), DataType::Int),
        },
        GraphRecordAttribute::Int(_) => Ok(value),
    }
}

fn cast_attribute_to_string(value: GraphRecordAttribute) -> GraphRecordAttribute {
    GraphRecordAttribute::String(match value {
        GraphRecordAttribute::String(value) => value,
        GraphRecordAttribute::Int(value) => value.to_string(),
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
        cast_attribute_to_int(label, value)
    }
}

impl ValueCast<StringTarget> for AttributeName {
    fn cast<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
        _target: &StringTarget,
    ) -> QueryResult<Self::Value<'a>> {
        Ok(cast_attribute_to_string(value))
    }
}

impl ValueCast<BoolTarget> for IndexValue<GraphRecordValue> {
    fn cast<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        _target: &BoolTarget,
    ) -> QueryResult<Self::Value<'a>> {
        cast_value_to_bool(label, value)
    }
}

impl ValueCast<DateTimeTarget> for IndexValue<GraphRecordValue> {
    fn cast<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        _target: &DateTimeTarget,
    ) -> QueryResult<Self::Value<'a>> {
        cast_value_to_datetime(label, value)
    }
}

impl ValueCast<DurationTarget> for IndexValue<GraphRecordValue> {
    fn cast<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        _target: &DurationTarget,
    ) -> QueryResult<Self::Value<'a>> {
        cast_value_to_duration(label, value)
    }
}

impl ValueCast<FloatTarget> for IndexValue<GraphRecordValue> {
    fn cast<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        _target: &FloatTarget,
    ) -> QueryResult<Self::Value<'a>> {
        cast_value_to_float(label, value)
    }
}

impl ValueCast<IntTarget> for IndexValue<GraphRecordValue> {
    fn cast<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        _target: &IntTarget,
    ) -> QueryResult<Self::Value<'a>> {
        cast_value_to_int(label, value)
    }
}

impl ValueCast<StringTarget> for IndexValue<GraphRecordValue> {
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
        cast_attribute_to_int(label, value)
    }
}

impl ValueCast<StringTarget> for IndexValue<NodeIndex> {
    fn cast<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
        _target: &StringTarget,
    ) -> QueryResult<Self::Value<'a>> {
        Ok(cast_attribute_to_string(value))
    }
}

impl ValueCast<IntTarget> for IndexValue<AttributeName> {
    fn cast<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        _target: &IntTarget,
    ) -> QueryResult<Self::Value<'a>> {
        cast_attribute_to_int(label, value)
    }
}

impl ValueCast<StringTarget> for IndexValue<AttributeName> {
    fn cast<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
        _target: &StringTarget,
    ) -> QueryResult<Self::Value<'a>> {
        Ok(cast_attribute_to_string(value))
    }
}
