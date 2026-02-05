use super::{
    Abs, Ceil, Contains, EndsWith, Floor, Lowercase, Mod, Pow, Round, Slice, Sqrt, StartsWith,
    Trim, TrimEnd, TrimStart, Uppercase,
};
use crate::errors::GraphRecordError;
use chrono::{DateTime, NaiveDateTime, TimeDelta};
use graphrecords_utils::implement_from_for_wrapper;
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    fmt::Display,
    ops::{Add, Div, Mul, Range, Sub},
};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum GraphRecordValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    DateTime(NaiveDateTime),
    Duration(TimeDelta),
    #[default]
    Null,
}

impl From<&str> for GraphRecordValue {
    fn from(value: &str) -> Self {
        value.to_string().into()
    }
}

// TODO: Add tests for Duration
implement_from_for_wrapper!(GraphRecordValue, String, String);
implement_from_for_wrapper!(GraphRecordValue, i64, Int);
implement_from_for_wrapper!(GraphRecordValue, f64, Float);
implement_from_for_wrapper!(GraphRecordValue, bool, Bool);
implement_from_for_wrapper!(GraphRecordValue, NaiveDateTime, DateTime);
implement_from_for_wrapper!(GraphRecordValue, TimeDelta, Duration);

impl<T> From<Option<T>> for GraphRecordValue
where
    T: Into<GraphRecordValue>,
{
    fn from(value: Option<T>) -> Self {
        match value {
            Some(value) => value.into(),
            None => Self::Null,
        }
    }
}

// TODO: Add tests for Duration
impl PartialEq for GraphRecordValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (GraphRecordValue::String(value), GraphRecordValue::String(other)) => value == other,
            (GraphRecordValue::Int(value), GraphRecordValue::Int(other)) => value == other,
            (GraphRecordValue::Int(value), GraphRecordValue::Float(other)) => {
                &(*value as f64) == other
            }
            (GraphRecordValue::Float(value), GraphRecordValue::Float(other)) => value == other,
            (GraphRecordValue::Float(value), GraphRecordValue::Int(other)) => {
                value == &(*other as f64)
            }
            (GraphRecordValue::Bool(value), GraphRecordValue::Bool(other)) => value == other,
            (GraphRecordValue::DateTime(value), GraphRecordValue::DateTime(other)) => {
                value == other
            }
            (GraphRecordValue::Duration(value), GraphRecordValue::Duration(other)) => {
                value == other
            }
            (GraphRecordValue::Null, GraphRecordValue::Null) => true,
            _ => false,
        }
    }
}

// TODO: Add tests for Duration
impl PartialOrd for GraphRecordValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (GraphRecordValue::String(value), GraphRecordValue::String(other)) => {
                Some(value.cmp(other))
            }
            (GraphRecordValue::Int(value), GraphRecordValue::Int(other)) => Some(value.cmp(other)),
            (GraphRecordValue::Int(value), GraphRecordValue::Float(other)) => {
                (*value as f64).partial_cmp(other)
            }
            (GraphRecordValue::Float(value), GraphRecordValue::Int(other)) => {
                value.partial_cmp(&(*other as f64))
            }
            (GraphRecordValue::Float(value), GraphRecordValue::Float(other)) => {
                value.partial_cmp(other)
            }
            (GraphRecordValue::Bool(value), GraphRecordValue::Bool(other)) => {
                Some(value.cmp(other))
            }
            (GraphRecordValue::DateTime(value), GraphRecordValue::DateTime(other)) => {
                Some(value.cmp(other))
            }
            (GraphRecordValue::Duration(value), GraphRecordValue::Duration(other)) => {
                Some(value.cmp(other))
            }
            (GraphRecordValue::Null, GraphRecordValue::Null) => Some(Ordering::Equal),
            _ => None,
        }
    }
}

// TODO: Add tests for Duration
impl Display for GraphRecordValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::String(value) => write!(f, "{value}"),
            Self::Int(value) => write!(f, "{value}"),
            Self::Float(value) => write!(f, "{value}"),
            Self::Bool(value) => write!(f, "{value}"),
            Self::DateTime(value) => write!(f, "{value}"),
            Self::Duration(value) => write!(f, "{value}"),
            Self::Null => write!(f, "Null"),
        }
    }
}

// TODO: Add tests for Duration
impl Add for GraphRecordValue {
    type Output = Result<Self, GraphRecordError>;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (GraphRecordValue::String(value), GraphRecordValue::String(rhs)) => {
                Ok(GraphRecordValue::String(value + rhs.as_str()))
            }
            (GraphRecordValue::String(value), GraphRecordValue::Int(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot add {rhs} to {value}")),
            ),
            (GraphRecordValue::String(value), GraphRecordValue::Float(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot add {rhs} to {value}")),
            ),
            (GraphRecordValue::String(value), GraphRecordValue::Bool(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot add {rhs} to {value}")),
            ),
            (GraphRecordValue::String(value), GraphRecordValue::DateTime(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot add {rhs} to {value}")),
            ),
            (GraphRecordValue::String(value), GraphRecordValue::Duration(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot add {rhs} to {value}")),
            ),
            (GraphRecordValue::String(value), GraphRecordValue::Null) => Err(
                GraphRecordError::AssertionError(format!("Cannot add None to {value}")),
            ),
            (GraphRecordValue::Int(value), GraphRecordValue::String(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot add {rhs} to {value}")),
            ),
            (GraphRecordValue::Int(value), GraphRecordValue::Int(rhs)) => {
                Ok(GraphRecordValue::Int(value + rhs))
            }
            (GraphRecordValue::Int(value), GraphRecordValue::Float(rhs)) => {
                Ok(GraphRecordValue::Float(value as f64 + rhs))
            }
            (GraphRecordValue::Int(value), GraphRecordValue::Bool(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot add {rhs} to {value}")),
            ),
            (GraphRecordValue::Int(value), GraphRecordValue::DateTime(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot add {rhs} to {value}")),
            ),
            (GraphRecordValue::Int(value), GraphRecordValue::Duration(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot add {rhs} to {value}")),
            ),
            (GraphRecordValue::Int(value), GraphRecordValue::Null) => Err(
                GraphRecordError::AssertionError(format!("Cannot add None to {value}")),
            ),
            (GraphRecordValue::Float(value), GraphRecordValue::String(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot add {rhs} to {value}")),
            ),
            (GraphRecordValue::Float(value), GraphRecordValue::Int(rhs)) => {
                Ok(GraphRecordValue::Float(value + rhs as f64))
            }
            (GraphRecordValue::Float(value), GraphRecordValue::Float(rhs)) => {
                Ok(GraphRecordValue::Float(value + rhs))
            }
            (GraphRecordValue::Float(value), GraphRecordValue::Bool(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot add {rhs} to {value}")),
            ),
            (GraphRecordValue::Float(value), GraphRecordValue::DateTime(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot add {rhs} to {value}")),
            ),
            (GraphRecordValue::Float(value), GraphRecordValue::Duration(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot add {rhs} to {value}")),
            ),
            (GraphRecordValue::Float(value), GraphRecordValue::Null) => Err(
                GraphRecordError::AssertionError(format!("Cannot add None to {value}")),
            ),
            (GraphRecordValue::Bool(value), GraphRecordValue::String(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot add {rhs} to {value}")),
            ),
            (GraphRecordValue::Bool(value), GraphRecordValue::Int(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot add {rhs} to {value}")),
            ),
            (GraphRecordValue::Bool(value), GraphRecordValue::Float(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot add {rhs} to {value}")),
            ),
            (GraphRecordValue::Bool(value), GraphRecordValue::Bool(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot add {rhs} to {value}")),
            ),
            (GraphRecordValue::Bool(value), GraphRecordValue::DateTime(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot add {rhs} to {value}")),
            ),
            (GraphRecordValue::Bool(value), GraphRecordValue::Duration(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot add {rhs} to {value}")),
            ),
            (GraphRecordValue::Bool(value), GraphRecordValue::Null) => Err(
                GraphRecordError::AssertionError(format!("Cannot add None to {value}")),
            ),
            (GraphRecordValue::DateTime(value), GraphRecordValue::String(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot add {rhs} to {value}")),
            ),
            (GraphRecordValue::DateTime(value), GraphRecordValue::Int(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot add {rhs} to {value}")),
            ),
            (GraphRecordValue::DateTime(value), GraphRecordValue::Float(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot add {rhs} to {value}")),
            ),
            (GraphRecordValue::DateTime(value), GraphRecordValue::Bool(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot add {rhs} to {value}")),
            ),
            (GraphRecordValue::DateTime(value), GraphRecordValue::DateTime(rhs)) => {
                Ok(DateTime::from_timestamp(
                    value.and_utc().timestamp() + rhs.and_utc().timestamp(),
                    0,
                )
                .ok_or(GraphRecordError::AssertionError(
                    "Invalid timestamp".to_string(),
                ))?
                .naive_utc()
                .into())
            }
            (GraphRecordValue::DateTime(value), GraphRecordValue::Duration(rhs)) => {
                Ok(value.add(rhs).into())
            }
            (GraphRecordValue::DateTime(value), GraphRecordValue::Null) => Err(
                GraphRecordError::AssertionError(format!("Cannot add None to {value}")),
            ),
            (GraphRecordValue::Duration(value), GraphRecordValue::String(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot add {rhs} to {value}")),
            ),
            (GraphRecordValue::Duration(value), GraphRecordValue::Int(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot add {rhs} to {value}")),
            ),
            (GraphRecordValue::Duration(value), GraphRecordValue::Float(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot add {rhs} to {value}")),
            ),
            (GraphRecordValue::Duration(value), GraphRecordValue::Bool(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot add {rhs} to {value}")),
            ),
            (GraphRecordValue::Duration(value), GraphRecordValue::DateTime(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot add {rhs} to {value}")),
            ),
            (GraphRecordValue::Duration(value), GraphRecordValue::Duration(rhs)) => {
                Ok((value + rhs).into())
            }
            (GraphRecordValue::Duration(value), GraphRecordValue::Null) => Err(
                GraphRecordError::AssertionError(format!("Cannot add None to {value}")),
            ),
            (GraphRecordValue::Null, GraphRecordValue::String(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot add {rhs} to None")),
            ),
            (GraphRecordValue::Null, GraphRecordValue::Int(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot add {rhs} to None")),
            ),
            (GraphRecordValue::Null, GraphRecordValue::Float(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot add {rhs} to None")),
            ),
            (GraphRecordValue::Null, GraphRecordValue::Bool(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot add {rhs} to None")),
            ),
            (GraphRecordValue::Null, GraphRecordValue::DateTime(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot add {rhs} to None")),
            ),
            (GraphRecordValue::Null, GraphRecordValue::Duration(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot add {rhs} to None")),
            ),
            (GraphRecordValue::Null, GraphRecordValue::Null) => Err(
                GraphRecordError::AssertionError("Cannot add None to None".to_string()),
            ),
        }
    }
}

// TODO: Add tests for Duration
impl Sub for GraphRecordValue {
    type Output = Result<Self, GraphRecordError>;

    fn sub(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (GraphRecordValue::String(value), GraphRecordValue::String(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract {rhs} from {value}")),
            ),
            (GraphRecordValue::String(value), GraphRecordValue::Int(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract {rhs} from {value}")),
            ),
            (GraphRecordValue::String(value), GraphRecordValue::Float(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract {rhs} from {value}")),
            ),
            (GraphRecordValue::String(value), GraphRecordValue::Bool(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract {rhs} from {value}")),
            ),
            (GraphRecordValue::String(value), GraphRecordValue::DateTime(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract {rhs} from {value}")),
            ),
            (GraphRecordValue::String(value), GraphRecordValue::Duration(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract {rhs} from {value}")),
            ),
            (GraphRecordValue::String(value), GraphRecordValue::Null) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract None from {value}")),
            ),
            (GraphRecordValue::Int(value), GraphRecordValue::String(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract {rhs} from {value}")),
            ),
            (GraphRecordValue::Int(value), GraphRecordValue::Int(rhs)) => {
                Ok(GraphRecordValue::Int(value - rhs))
            }
            (GraphRecordValue::Int(value), GraphRecordValue::Float(rhs)) => {
                Ok(GraphRecordValue::Float(value as f64 - rhs))
            }
            (GraphRecordValue::Int(value), GraphRecordValue::Bool(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract {rhs} from {value}")),
            ),
            (GraphRecordValue::Int(value), GraphRecordValue::DateTime(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract {rhs} from {value}")),
            ),
            (GraphRecordValue::Int(value), GraphRecordValue::Duration(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract {rhs} from {value}")),
            ),
            (GraphRecordValue::Int(value), GraphRecordValue::Null) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract None from {value}")),
            ),
            (GraphRecordValue::Float(value), GraphRecordValue::String(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract {rhs} from {value}")),
            ),
            (GraphRecordValue::Float(value), GraphRecordValue::Int(rhs)) => {
                Ok(GraphRecordValue::Float(value - rhs as f64))
            }
            (GraphRecordValue::Float(value), GraphRecordValue::Float(rhs)) => {
                Ok(GraphRecordValue::Float(value - rhs))
            }
            (GraphRecordValue::Float(value), GraphRecordValue::Bool(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract {rhs} from {value}")),
            ),
            (GraphRecordValue::Float(value), GraphRecordValue::DateTime(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract {rhs} from {value}")),
            ),
            (GraphRecordValue::Float(value), GraphRecordValue::Duration(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract {rhs} from {value}")),
            ),
            (GraphRecordValue::Float(value), GraphRecordValue::Null) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract None from {value}")),
            ),
            (GraphRecordValue::Bool(value), GraphRecordValue::String(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract {rhs} from {value}")),
            ),
            (GraphRecordValue::Bool(value), GraphRecordValue::Int(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract {rhs} from {value}")),
            ),
            (GraphRecordValue::Bool(value), GraphRecordValue::Float(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract {rhs} from {value}")),
            ),
            (GraphRecordValue::Bool(value), GraphRecordValue::Bool(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract {rhs} from {value}")),
            ),
            (GraphRecordValue::Bool(value), GraphRecordValue::DateTime(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract {rhs} from {value}")),
            ),
            (GraphRecordValue::Bool(value), GraphRecordValue::Duration(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract {rhs} from {value}")),
            ),
            (GraphRecordValue::Bool(value), GraphRecordValue::Null) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract None from {value}")),
            ),
            (GraphRecordValue::DateTime(value), GraphRecordValue::String(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract {rhs} from {value}")),
            ),
            (GraphRecordValue::DateTime(value), GraphRecordValue::Int(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract {rhs} from {value}")),
            ),
            (GraphRecordValue::DateTime(value), GraphRecordValue::Float(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract {rhs} from {value}")),
            ),
            (GraphRecordValue::DateTime(value), GraphRecordValue::Bool(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract {rhs} from {value}")),
            ),
            (GraphRecordValue::DateTime(value), GraphRecordValue::DateTime(rhs)) => {
                let duration = value - rhs;

                Ok(duration.into())
            }
            (GraphRecordValue::DateTime(value), GraphRecordValue::Duration(rhs)) => {
                Ok((value - rhs).into())
            }
            (GraphRecordValue::DateTime(value), GraphRecordValue::Null) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract None from {value}")),
            ),
            (GraphRecordValue::Duration(value), GraphRecordValue::String(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract {rhs} from {value}")),
            ),
            (GraphRecordValue::Duration(value), GraphRecordValue::Int(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract {rhs} from {value}")),
            ),
            (GraphRecordValue::Duration(value), GraphRecordValue::Float(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract {rhs} from {value}")),
            ),
            (GraphRecordValue::Duration(value), GraphRecordValue::Bool(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract {rhs} from {value}")),
            ),
            (GraphRecordValue::Duration(value), GraphRecordValue::DateTime(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract {rhs} from {value}")),
            ),
            (GraphRecordValue::Duration(value), GraphRecordValue::Duration(rhs)) => {
                Ok((value + rhs).into())
            }
            (GraphRecordValue::Duration(value), GraphRecordValue::Null) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract None from {value}")),
            ),
            (GraphRecordValue::Null, GraphRecordValue::String(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract {rhs} from None")),
            ),
            (GraphRecordValue::Null, GraphRecordValue::Int(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract {rhs} from None")),
            ),
            (GraphRecordValue::Null, GraphRecordValue::Float(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract {rhs} from None")),
            ),
            (GraphRecordValue::Null, GraphRecordValue::Bool(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract {rhs} from None")),
            ),
            (GraphRecordValue::Null, GraphRecordValue::DateTime(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract {rhs} from None")),
            ),
            (GraphRecordValue::Null, GraphRecordValue::Duration(rhs)) => Err(
                GraphRecordError::AssertionError(format!("Cannot subtract {rhs} from None")),
            ),
            (GraphRecordValue::Null, GraphRecordValue::Null) => Err(
                GraphRecordError::AssertionError("Cannot subtract None from None".to_string()),
            ),
        }
    }
}

// TODO: Add tests for Duration
impl Mul for GraphRecordValue {
    type Output = Result<Self, GraphRecordError>;

    fn mul(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (GraphRecordValue::String(value), GraphRecordValue::String(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty {value} with {other}")),
            ),
            (GraphRecordValue::String(value), GraphRecordValue::Int(other)) => {
                let mut result = String::new();

                for _ in 0..other {
                    result.push_str(&value)
                }

                Ok(GraphRecordValue::String(result))
            }
            (GraphRecordValue::String(value), GraphRecordValue::Float(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty {value} with {other}")),
            ),
            (GraphRecordValue::String(value), GraphRecordValue::Bool(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty {value} with {other}")),
            ),
            (GraphRecordValue::String(value), GraphRecordValue::DateTime(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty {value} with {other}")),
            ),
            (GraphRecordValue::String(value), GraphRecordValue::Duration(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty {value} with {other}")),
            ),
            (GraphRecordValue::String(value), GraphRecordValue::Null) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty {value} with None")),
            ),
            (GraphRecordValue::Int(value), GraphRecordValue::String(other)) => {
                let mut result = String::new();

                for _ in 0..value {
                    result.push_str(&other)
                }

                Ok(GraphRecordValue::String(result))
            }
            (GraphRecordValue::Int(value), GraphRecordValue::Int(other)) => {
                Ok(GraphRecordValue::Int(value * other))
            }
            (GraphRecordValue::Int(value), GraphRecordValue::Float(other)) => {
                Ok(GraphRecordValue::Float(value as f64 * other))
            }
            (GraphRecordValue::Int(value), GraphRecordValue::Bool(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty {value} with {other}")),
            ),
            (GraphRecordValue::Int(value), GraphRecordValue::DateTime(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty {value} with {other}")),
            ),
            (GraphRecordValue::Int(value), GraphRecordValue::Duration(other)) => {
                Ok((other * (value as i32)).into())
            }
            (GraphRecordValue::Int(value), GraphRecordValue::Null) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty {value} with None")),
            ),
            (GraphRecordValue::Float(value), GraphRecordValue::String(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty {value} with {other}")),
            ),
            (GraphRecordValue::Float(value), GraphRecordValue::Int(other)) => {
                Ok(GraphRecordValue::Float(value * other as f64))
            }
            (GraphRecordValue::Float(value), GraphRecordValue::Float(other)) => {
                Ok(GraphRecordValue::Float(value * other))
            }
            (GraphRecordValue::Float(value), GraphRecordValue::Bool(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty {value} with {other}")),
            ),
            (GraphRecordValue::Float(value), GraphRecordValue::DateTime(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty {value} with {other}")),
            ),
            (GraphRecordValue::Float(value), GraphRecordValue::Duration(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty {value} with {other}")),
            ),
            (GraphRecordValue::Float(value), GraphRecordValue::Null) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty {value} with None")),
            ),
            (GraphRecordValue::Bool(value), GraphRecordValue::String(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty {value} with {other}")),
            ),
            (GraphRecordValue::Bool(value), GraphRecordValue::Int(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty {value} with {other}")),
            ),
            (GraphRecordValue::Bool(value), GraphRecordValue::Float(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty {value} with {other}")),
            ),
            (GraphRecordValue::Bool(value), GraphRecordValue::Bool(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty {value} with {other}")),
            ),
            (GraphRecordValue::Bool(value), GraphRecordValue::DateTime(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty {value} with {other}")),
            ),
            (GraphRecordValue::Bool(value), GraphRecordValue::Duration(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty {value} with {other}")),
            ),
            (GraphRecordValue::Bool(value), GraphRecordValue::Null) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty {value} with None")),
            ),
            (GraphRecordValue::DateTime(value), GraphRecordValue::String(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty {value} with {other}")),
            ),
            (GraphRecordValue::DateTime(value), GraphRecordValue::Int(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty {value} with {other}")),
            ),
            (GraphRecordValue::DateTime(value), GraphRecordValue::Float(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty {value} with {other}")),
            ),
            (GraphRecordValue::DateTime(value), GraphRecordValue::Bool(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty {value} with {other}")),
            ),
            (GraphRecordValue::DateTime(value), GraphRecordValue::DateTime(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty {value} with {other}")),
            ),
            (GraphRecordValue::DateTime(value), GraphRecordValue::Duration(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty {value} with {other}")),
            ),
            (GraphRecordValue::DateTime(value), GraphRecordValue::Null) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty {value} with None")),
            ),
            (GraphRecordValue::Duration(value), GraphRecordValue::String(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty {value} with {other}")),
            ),
            (GraphRecordValue::Duration(value), GraphRecordValue::Int(other)) => {
                Ok((value * (other as i32)).into())
            }
            (GraphRecordValue::Duration(value), GraphRecordValue::Float(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty {value} with {other}")),
            ),
            (GraphRecordValue::Duration(value), GraphRecordValue::Bool(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty {value} with {other}")),
            ),
            (GraphRecordValue::Duration(value), GraphRecordValue::DateTime(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty {value} with {other}")),
            ),
            (GraphRecordValue::Duration(value), GraphRecordValue::Duration(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty {value} with {other}")),
            ),
            (GraphRecordValue::Duration(value), GraphRecordValue::Null) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty {value} with None")),
            ),
            (GraphRecordValue::Null, GraphRecordValue::String(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty None with {other}")),
            ),
            (GraphRecordValue::Null, GraphRecordValue::Int(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty None with {other}")),
            ),
            (GraphRecordValue::Null, GraphRecordValue::Float(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty None with {other}")),
            ),
            (GraphRecordValue::Null, GraphRecordValue::Bool(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty None with {other}")),
            ),
            (GraphRecordValue::Null, GraphRecordValue::DateTime(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty None with {other}")),
            ),
            (GraphRecordValue::Null, GraphRecordValue::Duration(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot multiplty None with {other}")),
            ),
            (GraphRecordValue::Null, GraphRecordValue::Null) => Err(
                GraphRecordError::AssertionError("Cannot multiplty None with None".to_string()),
            ),
        }
    }
}

// TODO: Add tests for Duration
impl Div for GraphRecordValue {
    type Output = Result<Self, GraphRecordError>;

    fn div(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (GraphRecordValue::String(value), GraphRecordValue::String(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide {value} by {other}")),
            ),
            (GraphRecordValue::String(value), GraphRecordValue::Int(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide {value} by {other}")),
            ),
            (GraphRecordValue::String(value), GraphRecordValue::Float(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide {value} by {other}")),
            ),
            (GraphRecordValue::String(value), GraphRecordValue::Bool(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide {value} by {other}")),
            ),
            (GraphRecordValue::String(value), GraphRecordValue::DateTime(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide {value} by {other}")),
            ),
            (GraphRecordValue::String(value), GraphRecordValue::Duration(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide {value} by {other}")),
            ),
            (GraphRecordValue::String(value), GraphRecordValue::Null) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide {value} by None")),
            ),
            (GraphRecordValue::Int(value), GraphRecordValue::String(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide {value} by {other}")),
            ),
            (GraphRecordValue::Int(value), GraphRecordValue::Int(other)) => {
                Ok(GraphRecordValue::Float(value as f64 / other as f64))
            }
            (GraphRecordValue::Int(value), GraphRecordValue::Float(other)) => {
                Ok(GraphRecordValue::Float(value as f64 / other))
            }
            (GraphRecordValue::Int(value), GraphRecordValue::Bool(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide {value} by {other}")),
            ),
            (GraphRecordValue::Int(value), GraphRecordValue::DateTime(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide {value} by {other}")),
            ),
            (GraphRecordValue::Int(value), GraphRecordValue::Duration(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide {value} by {other}")),
            ),
            (GraphRecordValue::Int(value), GraphRecordValue::Null) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide {value} by None")),
            ),
            (GraphRecordValue::Float(value), GraphRecordValue::String(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide {value} by {other}")),
            ),
            (GraphRecordValue::Float(value), GraphRecordValue::Int(other)) => {
                Ok(GraphRecordValue::Float(value / other as f64))
            }
            (GraphRecordValue::Float(value), GraphRecordValue::Float(other)) => {
                Ok(GraphRecordValue::Float(value / other))
            }
            (GraphRecordValue::Float(value), GraphRecordValue::Bool(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide {value} by {other}")),
            ),
            (GraphRecordValue::Float(value), GraphRecordValue::DateTime(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide {value} by {other}")),
            ),
            (GraphRecordValue::Float(value), GraphRecordValue::Duration(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide {value} by {other}")),
            ),
            (GraphRecordValue::Float(value), GraphRecordValue::Null) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide {value} by None")),
            ),
            (GraphRecordValue::Bool(value), GraphRecordValue::String(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide {value} by {other}")),
            ),
            (GraphRecordValue::Bool(value), GraphRecordValue::Int(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide {value} by {other}")),
            ),
            (GraphRecordValue::Bool(value), GraphRecordValue::Float(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide {value} by {other}")),
            ),
            (GraphRecordValue::Bool(value), GraphRecordValue::Bool(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide {value} by {other}")),
            ),
            (GraphRecordValue::Bool(value), GraphRecordValue::DateTime(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide {value} by {other}")),
            ),
            (GraphRecordValue::Bool(value), GraphRecordValue::Duration(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide {value} by {other}")),
            ),
            (GraphRecordValue::Bool(value), GraphRecordValue::Null) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide {value} by None")),
            ),
            (GraphRecordValue::DateTime(value), GraphRecordValue::String(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide {value} by {other}")),
            ),
            (GraphRecordValue::DateTime(value), GraphRecordValue::Int(other)) => {
                Ok(DateTime::from_timestamp(
                    (value.and_utc().timestamp() as f64 / other as f64).floor() as i64,
                    0,
                )
                .ok_or(GraphRecordError::AssertionError(
                    "Invalid timestamp".to_string(),
                ))?
                .naive_utc()
                .into())
            }
            (GraphRecordValue::DateTime(value), GraphRecordValue::Float(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide {value} by {other}")),
            ),
            (GraphRecordValue::DateTime(value), GraphRecordValue::Bool(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide {value} by {other}")),
            ),
            (GraphRecordValue::DateTime(value), GraphRecordValue::DateTime(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide {value} by {other}")),
            ),
            (GraphRecordValue::DateTime(value), GraphRecordValue::Duration(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide {value} by {other}")),
            ),
            (GraphRecordValue::DateTime(value), GraphRecordValue::Null) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide {value} by None")),
            ),
            (GraphRecordValue::Duration(value), GraphRecordValue::String(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide {value} by {other}")),
            ),
            (GraphRecordValue::Duration(value), GraphRecordValue::Int(other)) => {
                Ok((value / (other as i32)).into())
            }
            (GraphRecordValue::Duration(value), GraphRecordValue::Float(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide {value} by {other}")),
            ),
            (GraphRecordValue::Duration(value), GraphRecordValue::Bool(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide {value} by {other}")),
            ),
            (GraphRecordValue::Duration(value), GraphRecordValue::DateTime(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide {value} by {other}")),
            ),
            (GraphRecordValue::Duration(value), GraphRecordValue::Duration(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide {value} by {other}")),
            ),
            (GraphRecordValue::Duration(value), GraphRecordValue::Null) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide {value} by None")),
            ),
            (GraphRecordValue::Null, GraphRecordValue::String(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide None by {other}")),
            ),
            (GraphRecordValue::Null, GraphRecordValue::Int(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide None by {other}")),
            ),
            (GraphRecordValue::Null, GraphRecordValue::Float(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide None by {other}")),
            ),
            (GraphRecordValue::Null, GraphRecordValue::Bool(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide None by {other}")),
            ),
            (GraphRecordValue::Null, GraphRecordValue::DateTime(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide None by {other}")),
            ),
            (GraphRecordValue::Null, GraphRecordValue::Duration(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot divide None by {other}")),
            ),
            (GraphRecordValue::Null, GraphRecordValue::Null) => Err(
                GraphRecordError::AssertionError("Cannot divide None by None".to_string()),
            ),
        }
    }
}

// TODO: Add tests for Duration
impl Pow for GraphRecordValue {
    fn pow(self, exp: Self) -> Result<Self, GraphRecordError> {
        match (self, exp) {
            (GraphRecordValue::String(value), GraphRecordValue::String(other)) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise {value} to the power of {other}"
                )))
            }
            (GraphRecordValue::String(value), GraphRecordValue::Int(other)) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise {value} to the power of {other}"
                )))
            }
            (GraphRecordValue::String(value), GraphRecordValue::Float(other)) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise {value} to the power of {other}"
                )))
            }
            (GraphRecordValue::String(value), GraphRecordValue::Bool(other)) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise {value} to the power of {other}"
                )))
            }
            (GraphRecordValue::String(value), GraphRecordValue::DateTime(other)) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise {value} to the power of {other}"
                )))
            }
            (GraphRecordValue::String(value), GraphRecordValue::Duration(other)) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise {value} to the power of {other}"
                )))
            }
            (GraphRecordValue::String(value), GraphRecordValue::Null) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise {value} to the power of None"
                )))
            }
            (GraphRecordValue::Int(value), GraphRecordValue::String(other)) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise {value} to the power of {other}"
                )))
            }
            (GraphRecordValue::Int(value), GraphRecordValue::Int(exp)) => {
                Ok(GraphRecordValue::Int(value.pow(exp as u32)))
            }
            (GraphRecordValue::Int(value), GraphRecordValue::Float(exp)) => {
                Ok(GraphRecordValue::Float((value as f64).powf(exp)))
            }
            (GraphRecordValue::Int(value), GraphRecordValue::Bool(other)) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise {value} to the power of {other}"
                )))
            }
            (GraphRecordValue::Int(value), GraphRecordValue::DateTime(other)) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise {value} to the power of {other}"
                )))
            }
            (GraphRecordValue::Int(value), GraphRecordValue::Duration(other)) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise {value} to the power of {other}"
                )))
            }
            (GraphRecordValue::Int(value), GraphRecordValue::Null) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise {value} to the power of None"
                )))
            }
            (GraphRecordValue::Float(value), GraphRecordValue::String(other)) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise {value} to the power of {other}"
                )))
            }
            (GraphRecordValue::Float(value), GraphRecordValue::Int(exp)) => {
                Ok(GraphRecordValue::Float(value.powi(exp as i32)))
            }
            (GraphRecordValue::Float(value), GraphRecordValue::Float(exp)) => {
                Ok(GraphRecordValue::Float(value.powf(exp)))
            }
            (GraphRecordValue::Float(value), GraphRecordValue::Bool(other)) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise {value} to the power of {other}"
                )))
            }
            (GraphRecordValue::Float(value), GraphRecordValue::DateTime(other)) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise {value} to the power of {other}"
                )))
            }
            (GraphRecordValue::Float(value), GraphRecordValue::Duration(other)) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise {value} to the power of {other}"
                )))
            }
            (GraphRecordValue::Float(value), GraphRecordValue::Null) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise {value} to the power of None"
                )))
            }
            (GraphRecordValue::Bool(value), GraphRecordValue::String(other)) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise {value} to the power of {other}"
                )))
            }
            (GraphRecordValue::Bool(value), GraphRecordValue::Int(other)) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise {value} to the power of {other}"
                )))
            }
            (GraphRecordValue::Bool(value), GraphRecordValue::Float(other)) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise {value} to the power of {other}"
                )))
            }
            (GraphRecordValue::Bool(value), GraphRecordValue::Bool(other)) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise {value} to the power of {other}"
                )))
            }
            (GraphRecordValue::Bool(value), GraphRecordValue::DateTime(other)) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise {value} to the power of {other}"
                )))
            }
            (GraphRecordValue::Bool(value), GraphRecordValue::Duration(other)) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise {value} to the power of {other}"
                )))
            }
            (GraphRecordValue::Bool(value), GraphRecordValue::Null) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise {value} to the power of None"
                )))
            }
            (GraphRecordValue::DateTime(value), GraphRecordValue::String(other)) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise {value} to the power of {other}"
                )))
            }
            (GraphRecordValue::DateTime(value), GraphRecordValue::Int(other)) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise {value} to the power of {other}"
                )))
            }
            (GraphRecordValue::DateTime(value), GraphRecordValue::Float(other)) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise {value} to the power of {other}"
                )))
            }
            (GraphRecordValue::DateTime(value), GraphRecordValue::Bool(other)) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise {value} to the power of {other}"
                )))
            }
            (GraphRecordValue::DateTime(value), GraphRecordValue::DateTime(other)) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise {value} to the power of {other}"
                )))
            }
            (GraphRecordValue::DateTime(value), GraphRecordValue::Duration(other)) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise {value} to the power of {other}"
                )))
            }
            (GraphRecordValue::DateTime(value), GraphRecordValue::Null) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise {value} to the power of Null"
                )))
            }
            (GraphRecordValue::Duration(value), GraphRecordValue::String(other)) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise {value} to the power of {other}"
                )))
            }
            (GraphRecordValue::Duration(value), GraphRecordValue::Int(other)) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise {value} to the power of {other}"
                )))
            }
            (GraphRecordValue::Duration(value), GraphRecordValue::Float(other)) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise {value} to the power of {other}"
                )))
            }
            (GraphRecordValue::Duration(value), GraphRecordValue::Bool(other)) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise {value} to the power of {other}"
                )))
            }
            (GraphRecordValue::Duration(value), GraphRecordValue::DateTime(other)) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise {value} to the power of {other}"
                )))
            }
            (GraphRecordValue::Duration(value), GraphRecordValue::Duration(other)) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise {value} to the power of {other}"
                )))
            }
            (GraphRecordValue::Duration(value), GraphRecordValue::Null) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise {value} to the power of Null"
                )))
            }
            (GraphRecordValue::Null, GraphRecordValue::String(other)) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise None to the power of {other}"
                )))
            }
            (GraphRecordValue::Null, GraphRecordValue::Int(other)) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise None to the power of {other}"
                )))
            }
            (GraphRecordValue::Null, GraphRecordValue::Float(other)) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise None to the power of {other}"
                )))
            }
            (GraphRecordValue::Null, GraphRecordValue::Bool(other)) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise None to the power of {other}"
                )))
            }
            (GraphRecordValue::Null, GraphRecordValue::DateTime(other)) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise None to the power of {other}"
                )))
            }
            (GraphRecordValue::Null, GraphRecordValue::Duration(other)) => {
                Err(GraphRecordError::AssertionError(format!(
                    "Cannot raise None to the power of {other}"
                )))
            }
            (GraphRecordValue::Null, GraphRecordValue::Null) => {
                Err(GraphRecordError::AssertionError(
                    "Cannot raise None to the power of None".to_string(),
                ))
            }
        }
    }
}

// TODO: Add tests for Duration
impl Mod for GraphRecordValue {
    fn r#mod(self, other: Self) -> Result<Self, GraphRecordError> {
        match (self, other) {
            (GraphRecordValue::String(value), GraphRecordValue::String(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod {value} with {other}")),
            ),
            (GraphRecordValue::String(value), GraphRecordValue::Int(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod {value} with {other}")),
            ),
            (GraphRecordValue::String(value), GraphRecordValue::Float(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod {value} with {other}")),
            ),
            (GraphRecordValue::String(value), GraphRecordValue::Bool(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod {value} with {other}")),
            ),
            (GraphRecordValue::String(value), GraphRecordValue::DateTime(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod {value} with {other}")),
            ),
            (GraphRecordValue::String(value), GraphRecordValue::Duration(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod {value} with {other}")),
            ),
            (GraphRecordValue::String(value), GraphRecordValue::Null) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod {value} with None")),
            ),
            (GraphRecordValue::Int(value), GraphRecordValue::String(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod {value} with {other}")),
            ),
            (GraphRecordValue::Int(value), GraphRecordValue::Int(other)) => {
                Ok(GraphRecordValue::Int(value % other))
            }
            (GraphRecordValue::Int(value), GraphRecordValue::Float(other)) => {
                Ok(GraphRecordValue::Float(value as f64 % other))
            }
            (GraphRecordValue::Int(value), GraphRecordValue::Bool(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod {value} with {other}")),
            ),
            (GraphRecordValue::Int(value), GraphRecordValue::DateTime(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod {value} with {other}")),
            ),
            (GraphRecordValue::Int(value), GraphRecordValue::Duration(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod {value} with {other}")),
            ),
            (GraphRecordValue::Int(value), GraphRecordValue::Null) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod {value} with None")),
            ),
            (GraphRecordValue::Float(value), GraphRecordValue::String(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod {value} with {other}")),
            ),
            (GraphRecordValue::Float(value), GraphRecordValue::Int(other)) => {
                Ok(GraphRecordValue::Float(value % other as f64))
            }
            (GraphRecordValue::Float(value), GraphRecordValue::Float(other)) => {
                Ok(GraphRecordValue::Float(value % other))
            }
            (GraphRecordValue::Float(value), GraphRecordValue::Bool(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod {value} with {other}")),
            ),
            (GraphRecordValue::Float(value), GraphRecordValue::DateTime(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod {value} with {other}")),
            ),
            (GraphRecordValue::Float(value), GraphRecordValue::Duration(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod {value} with {other}")),
            ),
            (GraphRecordValue::Float(value), GraphRecordValue::Null) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod {value} with None")),
            ),
            (GraphRecordValue::Bool(value), GraphRecordValue::String(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod {value} with {other}")),
            ),
            (GraphRecordValue::Bool(value), GraphRecordValue::Int(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod {value} with {other}")),
            ),
            (GraphRecordValue::Bool(value), GraphRecordValue::Float(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod {value} with {other}")),
            ),
            (GraphRecordValue::Bool(value), GraphRecordValue::Bool(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod {value} with {other}")),
            ),
            (GraphRecordValue::Bool(value), GraphRecordValue::DateTime(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod {value} with {other}")),
            ),
            (GraphRecordValue::Bool(value), GraphRecordValue::Duration(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod {value} with {other}")),
            ),
            (GraphRecordValue::Bool(value), GraphRecordValue::Null) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod {value} with None")),
            ),
            (GraphRecordValue::DateTime(value), GraphRecordValue::String(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod {value} with {other}")),
            ),
            (GraphRecordValue::DateTime(value), GraphRecordValue::Int(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod {value} with {other}")),
            ),
            (GraphRecordValue::DateTime(value), GraphRecordValue::Float(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod {value} with {other}")),
            ),
            (GraphRecordValue::DateTime(value), GraphRecordValue::Bool(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod {value} with {other}")),
            ),
            (GraphRecordValue::DateTime(value), GraphRecordValue::DateTime(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod {value} with {other}")),
            ),
            (GraphRecordValue::DateTime(value), GraphRecordValue::Duration(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod {value} with {other}")),
            ),
            (GraphRecordValue::DateTime(value), GraphRecordValue::Null) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod {value} with None")),
            ),
            (GraphRecordValue::Duration(value), GraphRecordValue::String(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod {value} with {other}")),
            ),
            (GraphRecordValue::Duration(value), GraphRecordValue::Int(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod {value} with {other}")),
            ),
            (GraphRecordValue::Duration(value), GraphRecordValue::Float(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod {value} with {other}")),
            ),
            (GraphRecordValue::Duration(value), GraphRecordValue::Bool(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod {value} with {other}")),
            ),
            (GraphRecordValue::Duration(value), GraphRecordValue::DateTime(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod {value} with {other}")),
            ),
            (GraphRecordValue::Duration(value), GraphRecordValue::Duration(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod {value} with {other}")),
            ),
            (GraphRecordValue::Duration(value), GraphRecordValue::Null) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod {value} with None")),
            ),
            (GraphRecordValue::Null, GraphRecordValue::String(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod None with {other}")),
            ),
            (GraphRecordValue::Null, GraphRecordValue::Int(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod None with {other}")),
            ),
            (GraphRecordValue::Null, GraphRecordValue::Float(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod None with {other}")),
            ),
            (GraphRecordValue::Null, GraphRecordValue::Bool(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod None with {other}")),
            ),
            (GraphRecordValue::Null, GraphRecordValue::DateTime(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod None with {other}")),
            ),
            (GraphRecordValue::Null, GraphRecordValue::Duration(other)) => Err(
                GraphRecordError::AssertionError(format!("Cannot mod None with {other}")),
            ),
            (GraphRecordValue::Null, GraphRecordValue::Null) => Err(
                GraphRecordError::AssertionError("Cannot mod None with None".to_string()),
            ),
        }
    }
}

impl Round for GraphRecordValue {
    fn round(self) -> Self {
        match self {
            GraphRecordValue::Float(value) => GraphRecordValue::Float(value.round()),
            _ => self,
        }
    }
}

impl Ceil for GraphRecordValue {
    fn ceil(self) -> Self {
        match self {
            GraphRecordValue::Float(value) => GraphRecordValue::Float(value.ceil()),
            _ => self,
        }
    }
}

impl Floor for GraphRecordValue {
    fn floor(self) -> Self {
        match self {
            GraphRecordValue::Float(value) => GraphRecordValue::Float(value.floor()),
            _ => self,
        }
    }
}

impl Abs for GraphRecordValue {
    fn abs(self) -> Self {
        match self {
            GraphRecordValue::Int(value) => GraphRecordValue::Int(value.abs()),
            GraphRecordValue::Float(value) => GraphRecordValue::Float(value.abs()),
            _ => self,
        }
    }
}

impl Sqrt for GraphRecordValue {
    fn sqrt(self) -> Self {
        match self {
            GraphRecordValue::Int(value) => GraphRecordValue::Float((value as f64).sqrt()),
            GraphRecordValue::Float(value) => GraphRecordValue::Float(value.sqrt()),
            _ => self,
        }
    }
}

impl StartsWith for GraphRecordValue {
    fn starts_with(&self, other: &Self) -> bool {
        match (self, other) {
            (GraphRecordValue::String(value), GraphRecordValue::String(other)) => {
                value.starts_with(other)
            }
            (GraphRecordValue::String(value), GraphRecordValue::Int(other)) => {
                value.starts_with(&other.to_string())
            }
            (GraphRecordValue::String(value), GraphRecordValue::Float(other)) => {
                value.starts_with(&other.to_string())
            }
            (GraphRecordValue::Int(value), GraphRecordValue::String(other)) => {
                value.to_string().starts_with(other)
            }
            (GraphRecordValue::Int(value), GraphRecordValue::Int(other)) => {
                value.to_string().starts_with(&other.to_string())
            }
            (GraphRecordValue::Int(value), GraphRecordValue::Float(other)) => {
                value.to_string().starts_with(&other.to_string())
            }
            (GraphRecordValue::Float(value), GraphRecordValue::String(other)) => {
                value.to_string().starts_with(other)
            }
            (GraphRecordValue::Float(value), GraphRecordValue::Int(other)) => {
                value.to_string().starts_with(&other.to_string())
            }
            (GraphRecordValue::Float(value), GraphRecordValue::Float(other)) => {
                value.to_string().starts_with(&other.to_string())
            }
            _ => false,
        }
    }
}

impl EndsWith for GraphRecordValue {
    fn ends_with(&self, other: &Self) -> bool {
        match (self, other) {
            (GraphRecordValue::String(value), GraphRecordValue::String(other)) => {
                value.ends_with(other)
            }
            (GraphRecordValue::String(value), GraphRecordValue::Int(other)) => {
                value.ends_with(&other.to_string())
            }
            (GraphRecordValue::String(value), GraphRecordValue::Float(other)) => {
                value.ends_with(&other.to_string())
            }
            (GraphRecordValue::Int(value), GraphRecordValue::String(other)) => {
                value.to_string().ends_with(other)
            }
            (GraphRecordValue::Int(value), GraphRecordValue::Int(other)) => {
                value.to_string().ends_with(&other.to_string())
            }
            (GraphRecordValue::Int(value), GraphRecordValue::Float(other)) => {
                value.to_string().ends_with(&other.to_string())
            }
            (GraphRecordValue::Float(value), GraphRecordValue::String(other)) => {
                value.to_string().ends_with(other)
            }
            (GraphRecordValue::Float(value), GraphRecordValue::Int(other)) => {
                value.to_string().ends_with(&other.to_string())
            }
            (GraphRecordValue::Float(value), GraphRecordValue::Float(other)) => {
                value.to_string().ends_with(&other.to_string())
            }
            _ => false,
        }
    }
}

impl Contains for GraphRecordValue {
    fn contains(&self, other: &Self) -> bool {
        match (self, other) {
            (GraphRecordValue::String(value), GraphRecordValue::String(other)) => {
                value.contains(other)
            }
            (GraphRecordValue::String(value), GraphRecordValue::Int(other)) => {
                value.contains(&other.to_string())
            }
            (GraphRecordValue::String(value), GraphRecordValue::Float(other)) => {
                value.contains(&other.to_string())
            }
            (GraphRecordValue::Int(value), GraphRecordValue::String(other)) => {
                value.to_string().contains(other)
            }
            (GraphRecordValue::Int(value), GraphRecordValue::Int(other)) => {
                value.to_string().contains(&other.to_string())
            }
            (GraphRecordValue::Int(value), GraphRecordValue::Float(other)) => {
                value.to_string().contains(&other.to_string())
            }
            (GraphRecordValue::Float(value), GraphRecordValue::String(other)) => {
                value.to_string().contains(other)
            }
            (GraphRecordValue::Float(value), GraphRecordValue::Int(other)) => {
                value.to_string().contains(&other.to_string())
            }
            (GraphRecordValue::Float(value), GraphRecordValue::Float(other)) => {
                value.to_string().contains(&other.to_string())
            }
            _ => false,
        }
    }
}

impl Slice for GraphRecordValue {
    fn slice(self, range: Range<usize>) -> Self {
        match self {
            GraphRecordValue::String(value) => value[range].into(),
            GraphRecordValue::Int(value) => value.to_string()[range].into(),
            GraphRecordValue::Float(value) => value.to_string()[range].into(),
            GraphRecordValue::Bool(value) => value.to_string()[range].into(),
            _ => self,
        }
    }
}

impl Trim for GraphRecordValue {
    fn trim(self) -> Self {
        match self {
            GraphRecordValue::String(value) => GraphRecordValue::String(value.trim().to_string()),
            _ => self,
        }
    }
}

impl TrimStart for GraphRecordValue {
    fn trim_start(self) -> Self {
        match self {
            GraphRecordValue::String(value) => {
                GraphRecordValue::String(value.trim_start().to_string())
            }
            _ => self,
        }
    }
}

impl TrimEnd for GraphRecordValue {
    fn trim_end(self) -> Self {
        match self {
            GraphRecordValue::String(value) => {
                GraphRecordValue::String(value.trim_end().to_string())
            }
            _ => self,
        }
    }
}

impl Lowercase for GraphRecordValue {
    fn lowercase(self) -> Self {
        match self {
            GraphRecordValue::String(value) => GraphRecordValue::String(value.to_lowercase()),
            _ => self,
        }
    }
}

impl Uppercase for GraphRecordValue {
    fn uppercase(self) -> Self {
        match self {
            GraphRecordValue::String(value) => GraphRecordValue::String(value.to_uppercase()),
            _ => self,
        }
    }
}

#[cfg(test)]
mod test {
    use super::{Contains, EndsWith, GraphRecordValue, StartsWith};
    use crate::{
        errors::GraphRecordError,
        graphrecord::datatypes::{
            Abs, Ceil, Floor, Lowercase, Mod, Pow, Round, Slice, Sqrt, Trim, TrimEnd, TrimStart,
            Uppercase,
        },
    };
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

    #[test]
    fn test_default() {
        let value = GraphRecordValue::default();

        assert_eq!(GraphRecordValue::Null, value);
    }

    #[test]
    fn test_from_str() {
        let value = GraphRecordValue::from("value");

        assert_eq!(GraphRecordValue::String("value".to_string()), value)
    }

    #[test]
    fn test_from_string() {
        let value = GraphRecordValue::from("value".to_string());

        assert_eq!(GraphRecordValue::String("value".to_string()), value);
    }

    #[test]
    fn test_from_int() {
        let value = GraphRecordValue::from(0);

        assert_eq!(GraphRecordValue::Int(0), value);
    }

    #[test]
    fn test_from_f64() {
        let value = GraphRecordValue::from(0_f64);

        assert_eq!(GraphRecordValue::Float(0.0), value);
    }

    #[test]
    fn test_from_bool() {
        let value = GraphRecordValue::from(false);

        assert_eq!(GraphRecordValue::Bool(false), value);
    }

    #[test]
    fn test_from_datetime() {
        let value = GraphRecordValue::from(NaiveDateTime::MIN);

        assert_eq!(GraphRecordValue::DateTime(NaiveDateTime::MIN), value);
    }

    #[test]
    fn test_from_option() {
        let value = GraphRecordValue::from(Some("value"));

        assert_eq!(GraphRecordValue::String("value".to_string()), value);

        let value = GraphRecordValue::from(None::<String>);

        assert_eq!(GraphRecordValue::Null, value);
    }

    #[test]
    fn test_partial_eq() {
        assert!(
            GraphRecordValue::String("value".to_string())
                == GraphRecordValue::String("value".to_string())
        );
        assert!(
            GraphRecordValue::String("value2".to_string())
                != GraphRecordValue::String("value".to_string())
        );

        assert!(GraphRecordValue::Int(0) == GraphRecordValue::Int(0));
        assert!(GraphRecordValue::Int(1) != GraphRecordValue::Int(0));

        assert!(GraphRecordValue::Int(0) == GraphRecordValue::Float(0_f64));
        assert!(GraphRecordValue::Int(1) != GraphRecordValue::Float(0_f64));

        assert!(GraphRecordValue::Float(0_f64) == GraphRecordValue::Float(0_f64));
        assert!(GraphRecordValue::Float(1_f64) != GraphRecordValue::Float(0_f64));

        assert!(GraphRecordValue::Float(0_f64) == GraphRecordValue::Int(0));
        assert!(GraphRecordValue::Float(1_f64) != GraphRecordValue::Int(0));

        assert!(GraphRecordValue::Bool(false) == GraphRecordValue::Bool(false));
        assert!(GraphRecordValue::Bool(true) != GraphRecordValue::Bool(false));

        assert!(
            GraphRecordValue::DateTime(NaiveDateTime::MIN)
                == GraphRecordValue::DateTime(NaiveDateTime::MIN)
        );
        assert!(
            GraphRecordValue::DateTime(NaiveDateTime::MAX)
                != GraphRecordValue::DateTime(NaiveDateTime::MIN)
        );

        assert!(GraphRecordValue::Null == GraphRecordValue::Null);

        assert!(GraphRecordValue::String("0".to_string()) != GraphRecordValue::Int(0));
        assert!(GraphRecordValue::String("0".to_string()) != GraphRecordValue::Float(0_f64));
        assert!(GraphRecordValue::String("false".to_string()) != GraphRecordValue::Bool(false));
        assert!(
            GraphRecordValue::String("false".to_string())
                != GraphRecordValue::DateTime(NaiveDateTime::MIN)
        );
        assert!(GraphRecordValue::String("false".to_string()) != GraphRecordValue::Null);

        assert!(GraphRecordValue::Int(0) != GraphRecordValue::String("0".to_string()));
        assert!(GraphRecordValue::Int(0) != GraphRecordValue::Bool(false));
        assert!(GraphRecordValue::Int(0) != GraphRecordValue::DateTime(NaiveDateTime::MIN));
        assert!(GraphRecordValue::Int(0) != GraphRecordValue::Null);

        assert!(GraphRecordValue::Float(0_f64) != GraphRecordValue::String("0.0".to_string()));
        assert!(GraphRecordValue::Float(0_f64) != GraphRecordValue::Bool(false));
        assert!(GraphRecordValue::Float(0_f64) != GraphRecordValue::DateTime(NaiveDateTime::MIN));
        assert!(GraphRecordValue::Float(0_f64) != GraphRecordValue::Null);

        assert!(GraphRecordValue::Bool(false) != GraphRecordValue::String("false".to_string()));
        assert!(GraphRecordValue::Bool(false) != GraphRecordValue::Int(0));
        assert!(GraphRecordValue::Bool(false) != GraphRecordValue::Float(0_f64));
        assert!(GraphRecordValue::Bool(false) != GraphRecordValue::DateTime(NaiveDateTime::MIN));
        assert!(GraphRecordValue::Bool(false) != GraphRecordValue::Null);

        assert!(GraphRecordValue::Null != GraphRecordValue::String("false".to_string()));
        assert!(GraphRecordValue::Null != GraphRecordValue::Int(0));
        assert!(GraphRecordValue::Null != GraphRecordValue::Float(0_f64));
        assert!(GraphRecordValue::Null != GraphRecordValue::Bool(false));
        assert!(GraphRecordValue::Null != GraphRecordValue::DateTime(NaiveDateTime::MIN));
    }

    #[test]
    #[allow(clippy::neg_cmp_op_on_partial_ord)]
    fn test_partial_ord() {
        assert!(
            GraphRecordValue::String("b".to_string()) > GraphRecordValue::String("a".to_string())
        );
        assert!(
            GraphRecordValue::String("b".to_string()) >= GraphRecordValue::String("a".to_string())
        );
        assert!(
            GraphRecordValue::String("a".to_string()) < GraphRecordValue::String("b".to_string())
        );
        assert!(
            GraphRecordValue::String("a".to_string()) <= GraphRecordValue::String("b".to_string())
        );
        assert!(
            GraphRecordValue::String("a".to_string()) >= GraphRecordValue::String("a".to_string())
        );
        assert!(
            GraphRecordValue::String("a".to_string()) <= GraphRecordValue::String("a".to_string())
        );

        assert!(GraphRecordValue::Int(1) > GraphRecordValue::Int(0));
        assert!(GraphRecordValue::Int(1) >= GraphRecordValue::Int(0));
        assert!(GraphRecordValue::Int(0) < GraphRecordValue::Int(1));
        assert!(GraphRecordValue::Int(0) <= GraphRecordValue::Int(1));
        assert!(GraphRecordValue::Int(0) >= GraphRecordValue::Int(0));
        assert!(GraphRecordValue::Int(0) <= GraphRecordValue::Int(0));

        assert!(GraphRecordValue::Int(1) > GraphRecordValue::Float(0_f64));
        assert!(GraphRecordValue::Int(1) >= GraphRecordValue::Float(0_f64));
        assert!(GraphRecordValue::Int(0) < GraphRecordValue::Float(1_f64));
        assert!(GraphRecordValue::Int(0) <= GraphRecordValue::Float(1_f64));
        assert!(GraphRecordValue::Int(0) >= GraphRecordValue::Float(0_f64));
        assert!(GraphRecordValue::Int(0) <= GraphRecordValue::Float(0_f64));

        assert!(GraphRecordValue::Float(1_f64) > GraphRecordValue::Int(0));
        assert!(GraphRecordValue::Float(1_f64) >= GraphRecordValue::Int(0));
        assert!(GraphRecordValue::Float(0_f64) < GraphRecordValue::Int(1));
        assert!(GraphRecordValue::Float(0_f64) <= GraphRecordValue::Int(1));
        assert!(GraphRecordValue::Float(0_f64) >= GraphRecordValue::Int(0));
        assert!(GraphRecordValue::Float(0_f64) <= GraphRecordValue::Int(0));

        assert!(GraphRecordValue::Bool(true) > GraphRecordValue::Bool(false));
        assert!(GraphRecordValue::Bool(true) >= GraphRecordValue::Bool(false));
        assert!(GraphRecordValue::Bool(false) < GraphRecordValue::Bool(true));
        assert!(GraphRecordValue::Bool(false) <= GraphRecordValue::Bool(true));
        assert!(GraphRecordValue::Bool(false) >= GraphRecordValue::Bool(false));
        assert!(GraphRecordValue::Bool(false) <= GraphRecordValue::Bool(false));

        assert!(
            GraphRecordValue::DateTime(NaiveDateTime::MAX)
                > GraphRecordValue::DateTime(NaiveDateTime::MIN)
        );
        assert!(
            GraphRecordValue::DateTime(NaiveDateTime::MAX)
                >= GraphRecordValue::DateTime(NaiveDateTime::MIN)
        );
        assert!(
            GraphRecordValue::DateTime(NaiveDateTime::MIN)
                < GraphRecordValue::DateTime(NaiveDateTime::MAX)
        );
        assert!(
            GraphRecordValue::DateTime(NaiveDateTime::MIN)
                <= GraphRecordValue::DateTime(NaiveDateTime::MAX)
        );
        assert!(
            GraphRecordValue::DateTime(NaiveDateTime::MIN)
                >= GraphRecordValue::DateTime(NaiveDateTime::MIN)
        );
        assert!(
            GraphRecordValue::DateTime(NaiveDateTime::MIN)
                <= GraphRecordValue::DateTime(NaiveDateTime::MIN)
        );

        assert!(GraphRecordValue::Null <= GraphRecordValue::Null);
        assert!(GraphRecordValue::Null >= GraphRecordValue::Null);

        assert!(!(GraphRecordValue::String("a".to_string()) > GraphRecordValue::Int(1)));
        assert!(!(GraphRecordValue::String("a".to_string()) >= GraphRecordValue::Int(1)));
        assert!(!(GraphRecordValue::String("a".to_string()) < GraphRecordValue::Int(1)));
        assert!(!(GraphRecordValue::String("a".to_string()) <= GraphRecordValue::Int(1)));

        assert!(!(GraphRecordValue::String("a".to_string()) > GraphRecordValue::Float(1_f64)));
        assert!(!(GraphRecordValue::String("a".to_string()) >= GraphRecordValue::Float(1_f64)));
        assert!(!(GraphRecordValue::String("a".to_string()) < GraphRecordValue::Float(1_f64)));
        assert!(!(GraphRecordValue::String("a".to_string()) <= GraphRecordValue::Float(1_f64)));

        assert!(!(GraphRecordValue::String("a".to_string()) > GraphRecordValue::Bool(true)));
        assert!(!(GraphRecordValue::String("a".to_string()) >= GraphRecordValue::Bool(true)));
        assert!(!(GraphRecordValue::String("a".to_string()) < GraphRecordValue::Bool(true)));
        assert!(!(GraphRecordValue::String("a".to_string()) <= GraphRecordValue::Bool(true)));

        assert!(
            !(GraphRecordValue::String("a".to_string())
                > GraphRecordValue::DateTime(NaiveDateTime::MAX))
        );
        assert!(
            !(GraphRecordValue::String("a".to_string())
                >= GraphRecordValue::DateTime(NaiveDateTime::MAX))
        );
        assert!(
            !(GraphRecordValue::String("a".to_string())
                < GraphRecordValue::DateTime(NaiveDateTime::MAX))
        );
        assert!(
            !(GraphRecordValue::String("a".to_string())
                <= GraphRecordValue::DateTime(NaiveDateTime::MAX))
        );

        assert!(!(GraphRecordValue::String("a".to_string()) > GraphRecordValue::Null));
        assert!(!(GraphRecordValue::String("a".to_string()) >= GraphRecordValue::Null));
        assert!(!(GraphRecordValue::String("a".to_string()) < GraphRecordValue::Null));
        assert!(!(GraphRecordValue::String("a".to_string()) <= GraphRecordValue::Null));

        assert!(!(GraphRecordValue::Int(1) > GraphRecordValue::String("a".to_string())));
        assert!(!(GraphRecordValue::Int(1) >= GraphRecordValue::String("a".to_string())));
        assert!(!(GraphRecordValue::Int(1) < GraphRecordValue::String("a".to_string())));
        assert!(!(GraphRecordValue::Int(1) <= GraphRecordValue::String("a".to_string())));

        assert!(!(GraphRecordValue::Int(1) > GraphRecordValue::Bool(true)));
        assert!(!(GraphRecordValue::Int(1) >= GraphRecordValue::Bool(true)));
        assert!(!(GraphRecordValue::Int(1) < GraphRecordValue::Bool(true)));
        assert!(!(GraphRecordValue::Int(1) <= GraphRecordValue::Bool(true)));

        assert!(!(GraphRecordValue::Int(1) > GraphRecordValue::DateTime(NaiveDateTime::MAX)));
        assert!(!(GraphRecordValue::Int(1) >= GraphRecordValue::DateTime(NaiveDateTime::MAX)));
        assert!(!(GraphRecordValue::Int(1) < GraphRecordValue::DateTime(NaiveDateTime::MAX)));
        assert!(!(GraphRecordValue::Int(1) <= GraphRecordValue::DateTime(NaiveDateTime::MAX)));

        assert!(!(GraphRecordValue::Int(1) > GraphRecordValue::Null));
        assert!(!(GraphRecordValue::Int(1) >= GraphRecordValue::Null));
        assert!(!(GraphRecordValue::Int(1) < GraphRecordValue::Null));
        assert!(!(GraphRecordValue::Int(1) <= GraphRecordValue::Null));

        assert!(!(GraphRecordValue::Float(1_f64) > GraphRecordValue::String("a".to_string())));
        assert!(!(GraphRecordValue::Float(1_f64) >= GraphRecordValue::String("a".to_string())));
        assert!(!(GraphRecordValue::Float(1_f64) < GraphRecordValue::String("a".to_string())));
        assert!(!(GraphRecordValue::Float(1_f64) <= GraphRecordValue::String("a".to_string())));

        assert!(!(GraphRecordValue::Float(1_f64) > GraphRecordValue::Bool(true)));
        assert!(!(GraphRecordValue::Float(1_f64) >= GraphRecordValue::Bool(true)));
        assert!(!(GraphRecordValue::Float(1_f64) < GraphRecordValue::Bool(true)));
        assert!(!(GraphRecordValue::Float(1_f64) <= GraphRecordValue::Bool(true)));

        assert!(!(GraphRecordValue::Float(1_f64) > GraphRecordValue::DateTime(NaiveDateTime::MAX)));
        assert!(
            !(GraphRecordValue::Float(1_f64) >= GraphRecordValue::DateTime(NaiveDateTime::MAX))
        );
        assert!(!(GraphRecordValue::Float(1_f64) < GraphRecordValue::DateTime(NaiveDateTime::MAX)));
        assert!(
            !(GraphRecordValue::Float(1_f64) <= GraphRecordValue::DateTime(NaiveDateTime::MAX))
        );

        assert!(!(GraphRecordValue::Float(1_f64) > GraphRecordValue::Null));
        assert!(!(GraphRecordValue::Float(1_f64) >= GraphRecordValue::Null));
        assert!(!(GraphRecordValue::Float(1_f64) < GraphRecordValue::Null));
        assert!(!(GraphRecordValue::Float(1_f64) <= GraphRecordValue::Null));

        assert!(!(GraphRecordValue::Bool(true) > GraphRecordValue::String("a".to_string())));
        assert!(!(GraphRecordValue::Bool(true) >= GraphRecordValue::String("a".to_string())));
        assert!(!(GraphRecordValue::Bool(true) < GraphRecordValue::String("a".to_string())));
        assert!(!(GraphRecordValue::Bool(true) <= GraphRecordValue::String("a".to_string())));

        assert!(!(GraphRecordValue::Bool(true) > GraphRecordValue::Int(1)));
        assert!(!(GraphRecordValue::Bool(true) >= GraphRecordValue::Int(1)));
        assert!(!(GraphRecordValue::Bool(true) < GraphRecordValue::Int(1)));
        assert!(!(GraphRecordValue::Bool(true) <= GraphRecordValue::Int(1)));

        assert!(!(GraphRecordValue::Bool(true) > GraphRecordValue::Float(1_f64)));
        assert!(!(GraphRecordValue::Bool(true) >= GraphRecordValue::Float(1_f64)));
        assert!(!(GraphRecordValue::Bool(true) < GraphRecordValue::Float(1_f64)));
        assert!(!(GraphRecordValue::Bool(true) <= GraphRecordValue::Float(1_f64)));

        assert!(!(GraphRecordValue::Bool(true) > GraphRecordValue::DateTime(NaiveDateTime::MAX)));
        assert!(!(GraphRecordValue::Bool(true) >= GraphRecordValue::DateTime(NaiveDateTime::MAX)));
        assert!(!(GraphRecordValue::Bool(true) < GraphRecordValue::DateTime(NaiveDateTime::MAX)));
        assert!(!(GraphRecordValue::Bool(true) <= GraphRecordValue::DateTime(NaiveDateTime::MAX)));

        assert!(!(GraphRecordValue::Bool(true) > GraphRecordValue::Null));
        assert!(!(GraphRecordValue::Bool(true) >= GraphRecordValue::Null));
        assert!(!(GraphRecordValue::Bool(true) < GraphRecordValue::Null));
        assert!(!(GraphRecordValue::Bool(true) <= GraphRecordValue::Null));

        assert!(
            !(GraphRecordValue::DateTime(NaiveDateTime::MAX)
                > GraphRecordValue::String("a".to_string()))
        );
        assert!(
            !(GraphRecordValue::DateTime(NaiveDateTime::MAX)
                >= GraphRecordValue::String("a".to_string()))
        );
        assert!(
            !(GraphRecordValue::DateTime(NaiveDateTime::MAX)
                < GraphRecordValue::String("a".to_string()))
        );
        assert!(
            !(GraphRecordValue::DateTime(NaiveDateTime::MAX)
                <= GraphRecordValue::String("a".to_string()))
        );

        assert!(!(GraphRecordValue::DateTime(NaiveDateTime::MAX) > GraphRecordValue::Int(1)));
        assert!(!(GraphRecordValue::DateTime(NaiveDateTime::MAX) >= GraphRecordValue::Int(1)));
        assert!(!(GraphRecordValue::DateTime(NaiveDateTime::MAX) < GraphRecordValue::Int(1)));
        assert!(!(GraphRecordValue::DateTime(NaiveDateTime::MAX) <= GraphRecordValue::Int(1)));

        assert!(!(GraphRecordValue::DateTime(NaiveDateTime::MAX) > GraphRecordValue::Float(1_f64)));
        assert!(
            !(GraphRecordValue::DateTime(NaiveDateTime::MAX) >= GraphRecordValue::Float(1_f64))
        );
        assert!(!(GraphRecordValue::DateTime(NaiveDateTime::MAX) < GraphRecordValue::Float(1_f64)));
        assert!(
            !(GraphRecordValue::DateTime(NaiveDateTime::MAX) <= GraphRecordValue::Float(1_f64))
        );

        assert!(!(GraphRecordValue::DateTime(NaiveDateTime::MAX) > GraphRecordValue::Bool(true)));
        assert!(!(GraphRecordValue::DateTime(NaiveDateTime::MAX) >= GraphRecordValue::Bool(true)));
        assert!(!(GraphRecordValue::DateTime(NaiveDateTime::MAX) < GraphRecordValue::Bool(true)));
        assert!(!(GraphRecordValue::DateTime(NaiveDateTime::MAX) <= GraphRecordValue::Bool(true)));

        assert!(!(GraphRecordValue::DateTime(NaiveDateTime::MAX) > GraphRecordValue::Null));
        assert!(!(GraphRecordValue::DateTime(NaiveDateTime::MAX) >= GraphRecordValue::Null));
        assert!(!(GraphRecordValue::DateTime(NaiveDateTime::MAX) < GraphRecordValue::Null));
        assert!(!(GraphRecordValue::DateTime(NaiveDateTime::MAX) <= GraphRecordValue::Null));

        assert!(!(GraphRecordValue::Null > GraphRecordValue::String("a".to_string())));
        assert!(!(GraphRecordValue::Null >= GraphRecordValue::String("a".to_string())));
        assert!(!(GraphRecordValue::Null < GraphRecordValue::String("a".to_string())));
        assert!(!(GraphRecordValue::Null <= GraphRecordValue::String("a".to_string())));

        assert!(!(GraphRecordValue::Null > GraphRecordValue::Int(0)));
        assert!(!(GraphRecordValue::Null >= GraphRecordValue::Int(0)));
        assert!(!(GraphRecordValue::Null < GraphRecordValue::Int(0)));
        assert!(!(GraphRecordValue::Null <= GraphRecordValue::Int(0)));

        assert!(!(GraphRecordValue::Null > GraphRecordValue::Float(0_f64)));
        assert!(!(GraphRecordValue::Null >= GraphRecordValue::Float(0_f64)));
        assert!(!(GraphRecordValue::Null < GraphRecordValue::Float(0_f64)));
        assert!(!(GraphRecordValue::Null <= GraphRecordValue::Float(0_f64)));

        assert!(!(GraphRecordValue::Null > GraphRecordValue::Bool(false)));
        assert!(!(GraphRecordValue::Null >= GraphRecordValue::Bool(false)));
        assert!(!(GraphRecordValue::Null < GraphRecordValue::Bool(false)));
        assert!(!(GraphRecordValue::Null <= GraphRecordValue::Bool(false)));

        assert!(!(GraphRecordValue::Null > GraphRecordValue::DateTime(NaiveDateTime::MAX)));
        assert!(!(GraphRecordValue::Null >= GraphRecordValue::DateTime(NaiveDateTime::MAX)));
        assert!(!(GraphRecordValue::Null < GraphRecordValue::DateTime(NaiveDateTime::MAX)));
        assert!(!(GraphRecordValue::Null <= GraphRecordValue::DateTime(NaiveDateTime::MAX)));

        assert!(!(GraphRecordValue::Null > GraphRecordValue::Null));
        assert!(!(GraphRecordValue::Null < GraphRecordValue::Null));
    }

    #[test]
    fn test_display() {
        assert_eq!(
            "value",
            GraphRecordValue::String("value".to_string()).to_string()
        );

        assert_eq!("0", GraphRecordValue::Int(0).to_string());

        assert_eq!("0.5", GraphRecordValue::Float(0.5).to_string());

        assert_eq!("false", GraphRecordValue::Bool(false).to_string());

        assert_eq!(
            "-262143-01-01 00:00:00",
            GraphRecordValue::DateTime(NaiveDateTime::MIN).to_string()
        );

        assert_eq!("Null", GraphRecordValue::Null.to_string());
    }

    #[test]
    fn test_add() {
        assert_eq!(
            GraphRecordValue::String("value".to_string()),
            (GraphRecordValue::String("val".to_string())
                + GraphRecordValue::String("ue".to_string()))
            .unwrap()
        );
        assert!(
            (GraphRecordValue::String("value".to_string()) + GraphRecordValue::Int(0))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!(
            (GraphRecordValue::String("value".to_string()) + GraphRecordValue::Float(0_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!(
            (GraphRecordValue::String("value".to_string()) + GraphRecordValue::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::String("value".to_string())
            + GraphRecordValue::DateTime(NaiveDateTime::MIN))
        .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!(
            (GraphRecordValue::String("value".to_string()) + GraphRecordValue::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );

        assert!(
            (GraphRecordValue::Int(0) + GraphRecordValue::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert_eq!(
            GraphRecordValue::Int(10),
            (GraphRecordValue::Int(5) + GraphRecordValue::Int(5)).unwrap()
        );
        assert_eq!(
            GraphRecordValue::Float(10_f64),
            (GraphRecordValue::Int(5) + GraphRecordValue::Float(5_f64)).unwrap()
        );
        assert!((GraphRecordValue::Int(0) + GraphRecordValue::Bool(false))
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!(
            (GraphRecordValue::Int(0) + GraphRecordValue::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::Int(0) + GraphRecordValue::Null)
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));

        assert!(
            (GraphRecordValue::Float(0_f64) + GraphRecordValue::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert_eq!(
            GraphRecordValue::Float(10_f64),
            (GraphRecordValue::Float(5_f64) + GraphRecordValue::Int(5)).unwrap()
        );
        assert_eq!(
            GraphRecordValue::Float(10_f64),
            (GraphRecordValue::Float(5_f64) + GraphRecordValue::Float(5_f64)).unwrap()
        );
        assert!(
            (GraphRecordValue::Float(0_f64) + GraphRecordValue::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!(
            (GraphRecordValue::Float(0_f64) + GraphRecordValue::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::Float(0_f64) + GraphRecordValue::Null)
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));

        assert!(
            (GraphRecordValue::Bool(false) + GraphRecordValue::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::Bool(false) + GraphRecordValue::Int(0))
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!(
            (GraphRecordValue::Bool(false) + GraphRecordValue::Float(0_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!(
            (GraphRecordValue::Bool(false) + GraphRecordValue::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!(
            (GraphRecordValue::Bool(false) + GraphRecordValue::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::Bool(false) + GraphRecordValue::Null)
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));

        assert!((GraphRecordValue::DateTime(NaiveDateTime::MIN)
            + GraphRecordValue::String("value".to_string()))
        .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN) + GraphRecordValue::Int(0))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN) + GraphRecordValue::Float(0_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN) + GraphRecordValue::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert_eq!(
            GraphRecordValue::DateTime(
                NaiveDate::from_ymd_opt(1970, 1, 4)
                    .unwrap()
                    .and_time(NaiveTime::MIN)
            ),
            (GraphRecordValue::DateTime(
                NaiveDate::from_ymd_opt(1970, 1, 2)
                    .unwrap()
                    .and_time(NaiveTime::MIN)
            ) + GraphRecordValue::DateTime(
                NaiveDate::from_ymd_opt(1970, 1, 3)
                    .unwrap()
                    .and_time(NaiveTime::MIN)
            ))
            .unwrap()
        );
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN) + GraphRecordValue::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );

        assert!(
            (GraphRecordValue::Null + GraphRecordValue::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::Null + GraphRecordValue::Int(0))
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!((GraphRecordValue::Null + GraphRecordValue::Float(0_f64))
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!((GraphRecordValue::Null + GraphRecordValue::Bool(false))
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!(
            (GraphRecordValue::Null + GraphRecordValue::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::Null + GraphRecordValue::Null)
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
    }

    #[test]
    fn test_sub() {
        assert!((GraphRecordValue::String("value".to_string())
            - GraphRecordValue::String("value".to_string()))
        .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!(
            (GraphRecordValue::String("value".to_string()) - GraphRecordValue::Int(0))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!(
            (GraphRecordValue::String("value".to_string()) - GraphRecordValue::Float(0_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!(
            (GraphRecordValue::String("value".to_string()) - GraphRecordValue::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::String("value".to_string())
            - GraphRecordValue::DateTime(NaiveDateTime::MIN))
        .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!(
            (GraphRecordValue::String("value".to_string()) - GraphRecordValue::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );

        assert!(
            (GraphRecordValue::Int(0) - GraphRecordValue::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert_eq!(
            GraphRecordValue::Int(0),
            (GraphRecordValue::Int(5) - GraphRecordValue::Int(5)).unwrap()
        );
        assert_eq!(
            GraphRecordValue::Float(0_f64),
            (GraphRecordValue::Int(5) - GraphRecordValue::Float(5_f64)).unwrap()
        );
        assert!((GraphRecordValue::Int(0) - GraphRecordValue::Bool(false))
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!(
            (GraphRecordValue::Int(0) - GraphRecordValue::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::Int(0) - GraphRecordValue::Null)
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));

        assert!(
            (GraphRecordValue::Float(0_f64) - GraphRecordValue::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert_eq!(
            GraphRecordValue::Float(0_f64),
            (GraphRecordValue::Float(5_f64) - GraphRecordValue::Int(5)).unwrap()
        );
        assert_eq!(
            GraphRecordValue::Float(0_f64),
            (GraphRecordValue::Float(5_f64) - GraphRecordValue::Float(5_f64)).unwrap()
        );
        assert!(
            (GraphRecordValue::Float(0_f64) - GraphRecordValue::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!(
            (GraphRecordValue::Float(0_f64) - GraphRecordValue::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::Float(0_f64) - GraphRecordValue::Null)
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));

        assert!(
            (GraphRecordValue::Bool(false) - GraphRecordValue::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::Bool(false) - GraphRecordValue::Int(0))
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!(
            (GraphRecordValue::Bool(false) - GraphRecordValue::Float(0_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!(
            (GraphRecordValue::Bool(false) - GraphRecordValue::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!(
            (GraphRecordValue::Bool(false) - GraphRecordValue::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::Bool(false) - GraphRecordValue::Null)
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));

        assert!((GraphRecordValue::DateTime(NaiveDateTime::MIN)
            - GraphRecordValue::String("value".to_string()))
        .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN) - GraphRecordValue::Int(0))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN) - GraphRecordValue::Float(0_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN) - GraphRecordValue::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN) - GraphRecordValue::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );

        assert!(
            (GraphRecordValue::Null - GraphRecordValue::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::Null - GraphRecordValue::Int(0))
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!((GraphRecordValue::Null - GraphRecordValue::Float(0_f64))
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!((GraphRecordValue::Null - GraphRecordValue::Bool(false))
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!(
            (GraphRecordValue::Null - GraphRecordValue::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::Null - GraphRecordValue::Null)
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
    }

    #[test]
    fn test_mul() {
        assert!((GraphRecordValue::String("value".to_string())
            * GraphRecordValue::String("value".to_string()))
        .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert_eq!(
            GraphRecordValue::String("valuevaluevalue".to_string()),
            (GraphRecordValue::String("value".to_string()) * GraphRecordValue::Int(3)).unwrap()
        );
        assert!(
            (GraphRecordValue::String("value".to_string()) * GraphRecordValue::Float(0_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!(
            (GraphRecordValue::String("value".to_string()) * GraphRecordValue::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::String("value".to_string())
            * GraphRecordValue::DateTime(NaiveDateTime::MIN))
        .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!(
            (GraphRecordValue::String("value".to_string()) * GraphRecordValue::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );

        assert_eq!(
            GraphRecordValue::String("valuevaluevalue".to_string()),
            (GraphRecordValue::Int(3) * GraphRecordValue::String("value".to_string())).unwrap()
        );
        assert_eq!(
            GraphRecordValue::Int(25),
            (GraphRecordValue::Int(5) * GraphRecordValue::Int(5)).unwrap()
        );
        assert_eq!(
            GraphRecordValue::Float(25_f64),
            (GraphRecordValue::Int(5) * GraphRecordValue::Float(5_f64)).unwrap()
        );
        assert!((GraphRecordValue::Int(0) * GraphRecordValue::Bool(false))
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!(
            (GraphRecordValue::Int(0) * GraphRecordValue::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::Int(0) * GraphRecordValue::Null)
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));

        assert!(
            (GraphRecordValue::Float(0_f64) * GraphRecordValue::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert_eq!(
            GraphRecordValue::Float(25_f64),
            (GraphRecordValue::Float(5_f64) * GraphRecordValue::Int(5)).unwrap()
        );
        assert_eq!(
            GraphRecordValue::Float(25_f64),
            (GraphRecordValue::Float(5_f64) * GraphRecordValue::Float(5_f64)).unwrap()
        );
        assert!(
            (GraphRecordValue::Float(0_f64) * GraphRecordValue::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!(
            (GraphRecordValue::Float(0_f64) * GraphRecordValue::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::Float(0_f64) * GraphRecordValue::Null)
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));

        assert!(
            (GraphRecordValue::Bool(false) * GraphRecordValue::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::Bool(false) * GraphRecordValue::Int(0))
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!(
            (GraphRecordValue::Bool(false) * GraphRecordValue::Float(0_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!(
            (GraphRecordValue::Bool(false) * GraphRecordValue::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!(
            (GraphRecordValue::Bool(false) * GraphRecordValue::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::Bool(false) * GraphRecordValue::Null)
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));

        assert!((GraphRecordValue::DateTime(NaiveDateTime::MIN)
            * GraphRecordValue::String("value".to_string()))
        .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN) * GraphRecordValue::Int(0))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN) * GraphRecordValue::Float(0_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN) * GraphRecordValue::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::DateTime(NaiveDateTime::MIN)
            * GraphRecordValue::DateTime(NaiveDateTime::MIN))
        .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN) * GraphRecordValue::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );

        assert!(
            (GraphRecordValue::Null * GraphRecordValue::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::Null * GraphRecordValue::Int(0))
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!((GraphRecordValue::Null * GraphRecordValue::Float(0_f64))
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!((GraphRecordValue::Null * GraphRecordValue::Bool(false))
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!(
            (GraphRecordValue::Null * GraphRecordValue::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::Null * GraphRecordValue::Null)
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
    }

    #[test]
    fn test_div() {
        assert!((GraphRecordValue::String("value".to_string())
            / GraphRecordValue::String("value".to_string()))
        .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!(
            (GraphRecordValue::String("value".to_string()) / GraphRecordValue::Int(1))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!(
            (GraphRecordValue::String("value".to_string()) / GraphRecordValue::Float(1_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!(
            (GraphRecordValue::String("value".to_string()) / GraphRecordValue::Bool(true))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::String("value".to_string())
            / GraphRecordValue::DateTime(NaiveDateTime::MIN))
        .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!(
            (GraphRecordValue::String("value".to_string()) / GraphRecordValue::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );

        assert!(
            (GraphRecordValue::Int(0) / GraphRecordValue::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert_eq!(
            GraphRecordValue::Float(1_f64),
            (GraphRecordValue::Int(5) / GraphRecordValue::Int(5)).unwrap()
        );
        assert_eq!(
            GraphRecordValue::Float(1_f64),
            (GraphRecordValue::Int(5) / GraphRecordValue::Float(5_f64)).unwrap()
        );
        assert!((GraphRecordValue::Int(0) / GraphRecordValue::Bool(true))
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!(
            (GraphRecordValue::Int(0) / GraphRecordValue::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::Int(0) / GraphRecordValue::Null)
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));

        assert!(
            (GraphRecordValue::Float(0_f64) / GraphRecordValue::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert_eq!(
            GraphRecordValue::Float(1_f64),
            (GraphRecordValue::Float(5_f64) / GraphRecordValue::Int(5)).unwrap()
        );
        assert_eq!(
            GraphRecordValue::Float(1_f64),
            (GraphRecordValue::Float(5_f64) / GraphRecordValue::Float(5_f64)).unwrap()
        );
        assert!(
            (GraphRecordValue::Float(0_f64) / GraphRecordValue::Bool(true))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!(
            (GraphRecordValue::Float(0_f64) / GraphRecordValue::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::Float(0_f64) / GraphRecordValue::Null)
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));

        assert!(
            (GraphRecordValue::Bool(false) / GraphRecordValue::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::Bool(false) / GraphRecordValue::Int(1))
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!(
            (GraphRecordValue::Bool(false) / GraphRecordValue::Float(1_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!(
            (GraphRecordValue::Bool(false) / GraphRecordValue::Bool(true))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!(
            (GraphRecordValue::Bool(false) / GraphRecordValue::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::Bool(false) / GraphRecordValue::Null)
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));

        assert!((GraphRecordValue::DateTime(NaiveDateTime::MIN)
            / GraphRecordValue::String("value".to_string()))
        .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert_eq!(
            GraphRecordValue::DateTime(NaiveDateTime::MIN),
            (GraphRecordValue::DateTime(NaiveDateTime::MIN) / GraphRecordValue::Int(1)).unwrap()
        );
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN) / GraphRecordValue::Float(1_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN) / GraphRecordValue::Bool(true))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::DateTime(NaiveDateTime::MIN)
            / GraphRecordValue::DateTime(NaiveDateTime::MIN))
        .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN) / GraphRecordValue::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );

        assert!(
            (GraphRecordValue::Null / GraphRecordValue::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::Null / GraphRecordValue::Int(1))
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!((GraphRecordValue::Null / GraphRecordValue::Float(1_f64))
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!((GraphRecordValue::Null / GraphRecordValue::Bool(true))
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!(
            (GraphRecordValue::Null / GraphRecordValue::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::Null / GraphRecordValue::Null)
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
    }

    #[test]
    fn test_pow() {
        assert!((GraphRecordValue::String("value".to_string())
            .pow(GraphRecordValue::String("value".to_string())))
        .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!(
            (GraphRecordValue::String("value".to_string()).pow(GraphRecordValue::Int(0)))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::String("value".to_string())
            .pow(GraphRecordValue::Float(0_f64)))
        .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!(
            (GraphRecordValue::String("value".to_string()).pow(GraphRecordValue::Bool(false)))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::String("value".to_string())
            .pow(GraphRecordValue::DateTime(NaiveDateTime::MIN)))
        .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!(
            (GraphRecordValue::String("value".to_string()).pow(GraphRecordValue::Null))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );

        assert!(
            (GraphRecordValue::Int(0).pow(GraphRecordValue::String("value".to_string())))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert_eq!(
            GraphRecordValue::Int(25),
            (GraphRecordValue::Int(5).pow(GraphRecordValue::Int(2))).unwrap()
        );
        assert_eq!(
            GraphRecordValue::Float(25_f64),
            (GraphRecordValue::Int(5).pow(GraphRecordValue::Float(2_f64))).unwrap()
        );
        assert!(
            (GraphRecordValue::Int(0).pow(GraphRecordValue::Bool(false)))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!(
            (GraphRecordValue::Int(0).pow(GraphRecordValue::DateTime(NaiveDateTime::MIN)))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::Int(0).pow(GraphRecordValue::Null))
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));

        assert!((GraphRecordValue::Float(0_f64)
            .pow(GraphRecordValue::String("value".to_string())))
        .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert_eq!(
            GraphRecordValue::Float(25_f64),
            (GraphRecordValue::Float(5_f64).pow(GraphRecordValue::Int(2))).unwrap()
        );
        assert_eq!(
            GraphRecordValue::Float(25_f64),
            (GraphRecordValue::Float(5_f64).pow(GraphRecordValue::Float(2_f64))).unwrap()
        );
        assert!(
            (GraphRecordValue::Float(0_f64).pow(GraphRecordValue::Bool(false)))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::Float(0_f64)
            .pow(GraphRecordValue::DateTime(NaiveDateTime::MIN)))
        .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!((GraphRecordValue::Float(0_f64).pow(GraphRecordValue::Null))
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));

        assert!(
            (GraphRecordValue::Bool(false).pow(GraphRecordValue::String("value".to_string())))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!(
            (GraphRecordValue::Bool(false).pow(GraphRecordValue::Int(0)))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!(
            (GraphRecordValue::Bool(false).pow(GraphRecordValue::Float(0_f64)))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!(
            (GraphRecordValue::Bool(false).pow(GraphRecordValue::Bool(false)))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::Bool(false)
            .pow(GraphRecordValue::DateTime(NaiveDateTime::MIN)))
        .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!((GraphRecordValue::Bool(false).pow(GraphRecordValue::Null))
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));

        assert!((GraphRecordValue::DateTime(NaiveDateTime::MIN)
            .pow(GraphRecordValue::String("value".to_string())))
        .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN).pow(GraphRecordValue::Int(0)))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::DateTime(NaiveDateTime::MIN)
            .pow(GraphRecordValue::Float(0_f64)))
        .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!((GraphRecordValue::DateTime(NaiveDateTime::MIN)
            .pow(GraphRecordValue::Bool(false)))
        .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!((GraphRecordValue::DateTime(NaiveDateTime::MIN)
            .pow(GraphRecordValue::DateTime(NaiveDateTime::MIN)))
        .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN).pow(GraphRecordValue::Null))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );

        assert!(
            (GraphRecordValue::Null.pow(GraphRecordValue::String("value".to_string())))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::Null.pow(GraphRecordValue::Int(0)))
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!((GraphRecordValue::Null.pow(GraphRecordValue::Float(0_f64)))
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!((GraphRecordValue::Null.pow(GraphRecordValue::Bool(false)))
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!(
            (GraphRecordValue::Null.pow(GraphRecordValue::DateTime(NaiveDateTime::MIN)))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::Null.pow(GraphRecordValue::Null))
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
    }

    #[test]
    fn test_mod() {
        assert!((GraphRecordValue::String("value".to_string())
            .r#mod(GraphRecordValue::String("value".to_string())))
        .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!(
            (GraphRecordValue::String("value".to_string()).r#mod(GraphRecordValue::Int(0)))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::String("value".to_string())
            .r#mod(GraphRecordValue::Float(0_f64)))
        .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!((GraphRecordValue::String("value".to_string())
            .r#mod(GraphRecordValue::Bool(false)))
        .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!((GraphRecordValue::String("value".to_string())
            .r#mod(GraphRecordValue::DateTime(NaiveDateTime::MIN)))
        .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!(
            (GraphRecordValue::String("value".to_string()).r#mod(GraphRecordValue::Null))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );

        assert!(
            (GraphRecordValue::Int(0).r#mod(GraphRecordValue::String("value".to_string())))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert_eq!(
            GraphRecordValue::Int(1),
            (GraphRecordValue::Int(5).r#mod(GraphRecordValue::Int(2))).unwrap()
        );
        assert_eq!(
            GraphRecordValue::Float(1_f64),
            (GraphRecordValue::Int(5).r#mod(GraphRecordValue::Float(2_f64))).unwrap()
        );
        assert!(
            (GraphRecordValue::Int(0).r#mod(GraphRecordValue::Bool(false)))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!(
            (GraphRecordValue::Int(0).r#mod(GraphRecordValue::DateTime(NaiveDateTime::MIN)))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::Int(0).r#mod(GraphRecordValue::Null))
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));

        assert!((GraphRecordValue::Float(0_f64)
            .r#mod(GraphRecordValue::String("value".to_string())))
        .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert_eq!(
            GraphRecordValue::Float(1_f64),
            (GraphRecordValue::Float(5_f64).r#mod(GraphRecordValue::Int(2))).unwrap()
        );
        assert_eq!(
            GraphRecordValue::Float(1_f64),
            (GraphRecordValue::Float(5_f64).r#mod(GraphRecordValue::Float(2_f64))).unwrap()
        );
        assert!(
            (GraphRecordValue::Float(0_f64).r#mod(GraphRecordValue::Bool(false)))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::Float(0_f64)
            .r#mod(GraphRecordValue::DateTime(NaiveDateTime::MIN)))
        .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!(
            (GraphRecordValue::Float(0_f64).r#mod(GraphRecordValue::Null))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );

        assert!((GraphRecordValue::Bool(false)
            .r#mod(GraphRecordValue::String("value".to_string())))
        .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!(
            (GraphRecordValue::Bool(false).r#mod(GraphRecordValue::Int(0)))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!(
            (GraphRecordValue::Bool(false).r#mod(GraphRecordValue::Float(0_f64)))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!(
            (GraphRecordValue::Bool(false).r#mod(GraphRecordValue::Bool(false)))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::Bool(false)
            .r#mod(GraphRecordValue::DateTime(NaiveDateTime::MIN)))
        .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!(
            (GraphRecordValue::Bool(false).r#mod(GraphRecordValue::Null))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );

        assert!((GraphRecordValue::DateTime(NaiveDateTime::MIN)
            .r#mod(GraphRecordValue::String("value".to_string())))
        .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN).r#mod(GraphRecordValue::Int(0)))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::DateTime(NaiveDateTime::MIN)
            .r#mod(GraphRecordValue::Float(0_f64)))
        .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!((GraphRecordValue::DateTime(NaiveDateTime::MIN)
            .r#mod(GraphRecordValue::Bool(false)))
        .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!((GraphRecordValue::DateTime(NaiveDateTime::MIN)
            .r#mod(GraphRecordValue::DateTime(NaiveDateTime::MIN)))
        .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN).r#mod(GraphRecordValue::Null))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );

        assert!(
            (GraphRecordValue::Null.r#mod(GraphRecordValue::String("value".to_string())))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::Null.r#mod(GraphRecordValue::Int(0)))
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
        assert!(
            (GraphRecordValue::Null.r#mod(GraphRecordValue::Float(0_f64)))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!(
            (GraphRecordValue::Null.r#mod(GraphRecordValue::Bool(false)))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!(
            (GraphRecordValue::Null.r#mod(GraphRecordValue::DateTime(NaiveDateTime::MIN)))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
        assert!((GraphRecordValue::Null.r#mod(GraphRecordValue::Null))
            .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_))));
    }

    #[test]
    fn test_starts_with() {
        assert!(GraphRecordValue::String("value".to_string())
            .starts_with(&GraphRecordValue::String("val".to_string())));
        assert!(!GraphRecordValue::String("value".to_string())
            .starts_with(&GraphRecordValue::String("not_val".to_string())));
        assert!(GraphRecordValue::String("10".to_string()).starts_with(&GraphRecordValue::Int(1)));
        assert!(!GraphRecordValue::String("10".to_string()).starts_with(&GraphRecordValue::Int(0)));
        assert!(
            GraphRecordValue::String("10".to_string()).starts_with(&GraphRecordValue::Float(1_f64))
        );
        assert!(!GraphRecordValue::String("10".to_string())
            .starts_with(&GraphRecordValue::Float(0_f64)));

        assert!(GraphRecordValue::Int(10).starts_with(&GraphRecordValue::String("1".to_string())));
        assert!(!GraphRecordValue::Int(10).starts_with(&GraphRecordValue::String("0".to_string())));
        assert!(GraphRecordValue::Int(10).starts_with(&GraphRecordValue::Int(1)));
        assert!(!GraphRecordValue::Int(10).starts_with(&GraphRecordValue::Int(0)));
        assert!(GraphRecordValue::Int(10).starts_with(&GraphRecordValue::Float(1_f64)));
        assert!(!GraphRecordValue::Int(10).starts_with(&GraphRecordValue::Float(0_f64)));

        assert!(
            GraphRecordValue::Float(10_f64).starts_with(&GraphRecordValue::String("1".to_string()))
        );
        assert!(!GraphRecordValue::Float(10_f64)
            .starts_with(&GraphRecordValue::String("0".to_string())));
        assert!(GraphRecordValue::Float(10_f64).starts_with(&GraphRecordValue::Int(1)));
        assert!(!GraphRecordValue::Float(10_f64).starts_with(&GraphRecordValue::Int(0)));
        assert!(GraphRecordValue::Float(10_f64).starts_with(&GraphRecordValue::Float(1_f64)));
        assert!(!GraphRecordValue::Float(10_f64).starts_with(&GraphRecordValue::Float(0_f64)));

        assert!(!GraphRecordValue::String("true".to_string())
            .starts_with(&GraphRecordValue::Bool(true)));
        assert!(
            !GraphRecordValue::String("-262143-01-01 00:00:00".to_string())
                .starts_with(&GraphRecordValue::DateTime(NaiveDateTime::MIN))
        );
        assert!(!GraphRecordValue::String("Null".to_string()).starts_with(&GraphRecordValue::Null));

        assert!(!GraphRecordValue::Int(1).starts_with(&GraphRecordValue::Bool(true)));
        assert!(
            !GraphRecordValue::Int(-2).starts_with(&GraphRecordValue::DateTime(NaiveDateTime::MIN))
        );
        assert!(!GraphRecordValue::Int(0).starts_with(&GraphRecordValue::Null));

        assert!(!GraphRecordValue::Float(1_f64).starts_with(&GraphRecordValue::Bool(true)));
        assert!(!GraphRecordValue::Float(-2_f64)
            .starts_with(&GraphRecordValue::DateTime(NaiveDateTime::MIN)));
        assert!(!GraphRecordValue::Float(0_f64).starts_with(&GraphRecordValue::Null));

        assert!(!GraphRecordValue::Bool(true)
            .starts_with(&GraphRecordValue::String("true".to_string())));
        assert!(!GraphRecordValue::Bool(true).starts_with(&GraphRecordValue::Int(1)));
        assert!(!GraphRecordValue::Bool(true).starts_with(&GraphRecordValue::Float(1_f64)));
        assert!(!GraphRecordValue::Bool(true).starts_with(&GraphRecordValue::Bool(true)));
        assert!(!GraphRecordValue::Bool(true)
            .starts_with(&GraphRecordValue::DateTime(NaiveDateTime::MAX)));
        assert!(!GraphRecordValue::Bool(false).starts_with(&GraphRecordValue::Null));

        assert!(!GraphRecordValue::DateTime(NaiveDateTime::MAX).starts_with(
            &GraphRecordValue::String("-262143-01-01 00:00:00".to_string())
        ));
        assert!(
            !GraphRecordValue::DateTime(NaiveDateTime::MAX).starts_with(&GraphRecordValue::Int(-2))
        );
        assert!(!GraphRecordValue::DateTime(NaiveDateTime::MAX)
            .starts_with(&GraphRecordValue::Float(1_f64)));
        assert!(!GraphRecordValue::DateTime(NaiveDateTime::MAX)
            .starts_with(&GraphRecordValue::Bool(false)));
        assert!(!GraphRecordValue::DateTime(NaiveDateTime::MAX)
            .starts_with(&GraphRecordValue::DateTime(NaiveDateTime::MAX)));
        assert!(
            !GraphRecordValue::DateTime(NaiveDateTime::MIN).starts_with(&GraphRecordValue::Null)
        );

        assert!(!GraphRecordValue::Null.starts_with(&GraphRecordValue::String("Null".to_string())));
        assert!(!GraphRecordValue::Null.starts_with(&GraphRecordValue::Int(0)));
        assert!(!GraphRecordValue::Null.starts_with(&GraphRecordValue::Float(0_f64)));
        assert!(!GraphRecordValue::Null.starts_with(&GraphRecordValue::Bool(false)));
        assert!(
            !GraphRecordValue::Null.starts_with(&GraphRecordValue::DateTime(NaiveDateTime::MIN))
        );
        assert!(!GraphRecordValue::Null.starts_with(&GraphRecordValue::Null));
    }

    #[test]
    fn test_ends_with() {
        assert!(GraphRecordValue::String("value".to_string())
            .ends_with(&GraphRecordValue::String("ue".to_string())));
        assert!(!GraphRecordValue::String("value".to_string())
            .ends_with(&GraphRecordValue::String("not_ue".to_string())));
        assert!(GraphRecordValue::String("10".to_string()).ends_with(&GraphRecordValue::Int(0)));
        assert!(!GraphRecordValue::String("10".to_string()).ends_with(&GraphRecordValue::Int(1)));
        assert!(
            GraphRecordValue::String("10".to_string()).ends_with(&GraphRecordValue::Float(0_f64))
        );
        assert!(
            !GraphRecordValue::String("10".to_string()).ends_with(&GraphRecordValue::Float(1_f64))
        );

        assert!(GraphRecordValue::Int(10).ends_with(&GraphRecordValue::String("0".to_string())));
        assert!(!GraphRecordValue::Int(10).ends_with(&GraphRecordValue::String("1".to_string())));
        assert!(GraphRecordValue::Int(10).ends_with(&GraphRecordValue::Int(0)));
        assert!(!GraphRecordValue::Int(10).ends_with(&GraphRecordValue::Int(1)));
        assert!(GraphRecordValue::Int(10).ends_with(&GraphRecordValue::Float(0_f64)));
        assert!(!GraphRecordValue::Int(10).ends_with(&GraphRecordValue::Float(1_f64)));

        assert!(
            GraphRecordValue::Float(10_f64).ends_with(&GraphRecordValue::String("0".to_string()))
        );
        assert!(
            !GraphRecordValue::Float(10_f64).ends_with(&GraphRecordValue::String("1".to_string()))
        );
        assert!(GraphRecordValue::Float(10_f64).ends_with(&GraphRecordValue::Int(0)));
        assert!(!GraphRecordValue::Float(10_f64).ends_with(&GraphRecordValue::Int(1)));
        assert!(GraphRecordValue::Float(10_f64).ends_with(&GraphRecordValue::Float(0_f64)));
        assert!(!GraphRecordValue::Float(10_f64).ends_with(&GraphRecordValue::Float(1_f64)));

        assert!(
            !GraphRecordValue::String("true".to_string()).ends_with(&GraphRecordValue::Bool(true))
        );
        assert!(
            !GraphRecordValue::String("-262143-01-01 00:00:00".to_string())
                .ends_with(&GraphRecordValue::DateTime(NaiveDateTime::MIN))
        );
        assert!(!GraphRecordValue::String("Null".to_string()).ends_with(&GraphRecordValue::Null));

        assert!(!GraphRecordValue::Int(1).ends_with(&GraphRecordValue::Bool(true)));
        assert!(
            !GraphRecordValue::Int(0).ends_with(&GraphRecordValue::DateTime(NaiveDateTime::MIN))
        );
        assert!(!GraphRecordValue::Int(0).ends_with(&GraphRecordValue::Null));

        assert!(!GraphRecordValue::Float(1_f64).ends_with(&GraphRecordValue::Bool(true)));
        assert!(!GraphRecordValue::Float(0_f64)
            .ends_with(&GraphRecordValue::DateTime(NaiveDateTime::MIN)));
        assert!(!GraphRecordValue::Float(0_f64).ends_with(&GraphRecordValue::Null));

        assert!(
            !GraphRecordValue::Bool(true).ends_with(&GraphRecordValue::String("true".to_string()))
        );
        assert!(!GraphRecordValue::Bool(true).ends_with(&GraphRecordValue::Int(1)));
        assert!(!GraphRecordValue::Bool(true).ends_with(&GraphRecordValue::Float(1_f64)));
        assert!(!GraphRecordValue::Bool(true).ends_with(&GraphRecordValue::Bool(true)));
        assert!(!GraphRecordValue::Bool(true)
            .ends_with(&GraphRecordValue::DateTime(NaiveDateTime::MAX)));
        assert!(!GraphRecordValue::Bool(false).ends_with(&GraphRecordValue::Null));

        assert!(!GraphRecordValue::DateTime(NaiveDateTime::MIN).ends_with(
            &GraphRecordValue::String("-262143-01-01 00:00:00".to_string())
        ));
        assert!(
            !GraphRecordValue::DateTime(NaiveDateTime::MAX).ends_with(&GraphRecordValue::Int(0))
        );
        assert!(!GraphRecordValue::DateTime(NaiveDateTime::MAX)
            .ends_with(&GraphRecordValue::Float(0_f64)));
        assert!(!GraphRecordValue::DateTime(NaiveDateTime::MAX)
            .ends_with(&GraphRecordValue::Bool(false)));
        assert!(!GraphRecordValue::DateTime(NaiveDateTime::MAX)
            .ends_with(&GraphRecordValue::DateTime(NaiveDateTime::MAX)));
        assert!(!GraphRecordValue::DateTime(NaiveDateTime::MAX).ends_with(&GraphRecordValue::Null));

        assert!(!GraphRecordValue::Null.ends_with(&GraphRecordValue::String("true".to_string())));
        assert!(!GraphRecordValue::Null.ends_with(&GraphRecordValue::Int(1)));
        assert!(!GraphRecordValue::Null.ends_with(&GraphRecordValue::Float(1_f64)));
        assert!(!GraphRecordValue::Null.ends_with(&GraphRecordValue::Bool(false)));
        assert!(!GraphRecordValue::Null.ends_with(&GraphRecordValue::DateTime(NaiveDateTime::MIN)));
        assert!(!GraphRecordValue::Null.ends_with(&GraphRecordValue::Null));
    }

    #[test]
    fn test_contains() {
        assert!(GraphRecordValue::String("value".to_string())
            .contains(&GraphRecordValue::String("al".to_string())));
        assert!(!GraphRecordValue::String("value".to_string())
            .contains(&GraphRecordValue::String("not_al".to_string())));
        assert!(GraphRecordValue::String("10".to_string()).contains(&GraphRecordValue::Int(0)));
        assert!(!GraphRecordValue::String("10".to_string()).contains(&GraphRecordValue::Int(2)));
        assert!(
            GraphRecordValue::String("10".to_string()).contains(&GraphRecordValue::Float(0_f64))
        );
        assert!(
            !GraphRecordValue::String("10".to_string()).contains(&GraphRecordValue::Float(2_f64))
        );

        assert!(GraphRecordValue::Int(10).contains(&GraphRecordValue::String("0".to_string())));
        assert!(!GraphRecordValue::Int(10).contains(&GraphRecordValue::String("2".to_string())));
        assert!(GraphRecordValue::Int(10).contains(&GraphRecordValue::Int(0)));
        assert!(!GraphRecordValue::Int(10).contains(&GraphRecordValue::Int(2)));
        assert!(GraphRecordValue::Int(10).contains(&GraphRecordValue::Float(0_f64)));
        assert!(!GraphRecordValue::Int(10).contains(&GraphRecordValue::Float(2_f64)));

        assert!(
            GraphRecordValue::Float(10_f64).contains(&GraphRecordValue::String("0".to_string()))
        );
        assert!(
            !GraphRecordValue::Float(10_f64).contains(&GraphRecordValue::String("2".to_string()))
        );
        assert!(GraphRecordValue::Float(10_f64).contains(&GraphRecordValue::Int(0)));
        assert!(!GraphRecordValue::Float(10_f64).contains(&GraphRecordValue::Int(2)));
        assert!(GraphRecordValue::Float(10_f64).contains(&GraphRecordValue::Float(0_f64)));
        assert!(!GraphRecordValue::Float(10_f64).contains(&GraphRecordValue::Float(2_f64)));

        assert!(
            !GraphRecordValue::String("true".to_string()).contains(&GraphRecordValue::Bool(true))
        );
        assert!(
            !GraphRecordValue::String("-262143-01-01 00:00:00".to_string())
                .contains(&GraphRecordValue::DateTime(NaiveDateTime::MIN))
        );
        assert!(!GraphRecordValue::String("Null".to_string()).contains(&GraphRecordValue::Null));

        assert!(!GraphRecordValue::Int(1).contains(&GraphRecordValue::Bool(true)));
        assert!(!GraphRecordValue::Int(0).contains(&GraphRecordValue::DateTime(NaiveDateTime::MIN)));
        assert!(!GraphRecordValue::Int(0).contains(&GraphRecordValue::Null));

        assert!(!GraphRecordValue::Float(1_f64).contains(&GraphRecordValue::Bool(true)));
        assert!(!GraphRecordValue::Float(0_f64)
            .contains(&GraphRecordValue::DateTime(NaiveDateTime::MIN)));
        assert!(!GraphRecordValue::Float(0_f64).contains(&GraphRecordValue::Null));

        assert!(
            !GraphRecordValue::Bool(true).contains(&GraphRecordValue::String("true".to_string()))
        );
        assert!(!GraphRecordValue::Bool(true).contains(&GraphRecordValue::Int(1)));
        assert!(!GraphRecordValue::Bool(true).contains(&GraphRecordValue::Float(1_f64)));
        assert!(!GraphRecordValue::Bool(true).contains(&GraphRecordValue::Bool(true)));
        assert!(
            !GraphRecordValue::Bool(true).contains(&GraphRecordValue::DateTime(NaiveDateTime::MAX))
        );
        assert!(!GraphRecordValue::Bool(false).contains(&GraphRecordValue::Null));

        assert!(!GraphRecordValue::DateTime(NaiveDateTime::MIN).contains(
            &GraphRecordValue::String("-262143-01-01 00:00:00".to_string())
        ));
        assert!(!GraphRecordValue::DateTime(NaiveDateTime::MIN).contains(&GraphRecordValue::Int(0)));
        assert!(!GraphRecordValue::DateTime(NaiveDateTime::MIN)
            .contains(&GraphRecordValue::Float(0_f64)));
        assert!(!GraphRecordValue::DateTime(NaiveDateTime::MIN)
            .contains(&GraphRecordValue::Bool(false)));
        assert!(!GraphRecordValue::DateTime(NaiveDateTime::MIN)
            .contains(&GraphRecordValue::DateTime(NaiveDateTime::MIN)));
        assert!(!GraphRecordValue::DateTime(NaiveDateTime::MIN).contains(&GraphRecordValue::Null));

        assert!(!GraphRecordValue::Null.contains(&GraphRecordValue::String("true".to_string())));
        assert!(!GraphRecordValue::Null.contains(&GraphRecordValue::Int(1)));
        assert!(!GraphRecordValue::Null.contains(&GraphRecordValue::Float(1_f64)));
        assert!(!GraphRecordValue::Null.contains(&GraphRecordValue::Bool(true)));
        assert!(!GraphRecordValue::Null.contains(&GraphRecordValue::DateTime(NaiveDateTime::MIN)));
        assert!(!GraphRecordValue::Null.contains(&GraphRecordValue::Null));
    }

    #[test]
    fn test_slice() {
        assert_eq!(
            GraphRecordValue::String("al".to_string()),
            GraphRecordValue::String("value".to_string()).slice(1..3)
        );

        assert_eq!(
            GraphRecordValue::String("23".to_string()),
            GraphRecordValue::Int(1234).slice(1..3)
        );

        assert_eq!(
            GraphRecordValue::String("23".to_string()),
            GraphRecordValue::Float(1234_f64).slice(1..3)
        );

        assert_eq!(
            GraphRecordValue::String("al".to_string()),
            GraphRecordValue::Bool(false).slice(1..3)
        );

        assert_eq!(
            GraphRecordValue::DateTime(NaiveDateTime::MIN),
            GraphRecordValue::DateTime(NaiveDateTime::MIN).slice(1..3)
        );

        assert_eq!(GraphRecordValue::Null, GraphRecordValue::Null.slice(1..3));
    }

    #[test]
    fn test_round() {
        assert_eq!(
            GraphRecordValue::String("value".to_string()),
            GraphRecordValue::String("value".to_string()).round()
        );

        assert_eq!(
            GraphRecordValue::Int(1234),
            GraphRecordValue::Int(1234).round()
        );

        assert_eq!(
            GraphRecordValue::Float(1234_f64),
            GraphRecordValue::Float(1234.3).round()
        );

        assert_eq!(
            GraphRecordValue::Bool(false),
            GraphRecordValue::Bool(false).round()
        );

        assert_eq!(
            GraphRecordValue::DateTime(NaiveDateTime::MIN),
            GraphRecordValue::DateTime(NaiveDateTime::MIN).round()
        );

        assert_eq!(GraphRecordValue::Null, GraphRecordValue::Null.round());
    }

    #[test]
    fn test_ceil() {
        assert_eq!(
            GraphRecordValue::String("value".to_string()),
            GraphRecordValue::String("value".to_string()).ceil()
        );

        assert_eq!(
            GraphRecordValue::Int(1234),
            GraphRecordValue::Int(1234).ceil()
        );

        assert_eq!(
            GraphRecordValue::Float(1235_f64),
            GraphRecordValue::Float(1234.3).ceil()
        );

        assert_eq!(
            GraphRecordValue::Bool(false),
            GraphRecordValue::Bool(false).ceil()
        );

        assert_eq!(
            GraphRecordValue::DateTime(NaiveDateTime::MIN),
            GraphRecordValue::DateTime(NaiveDateTime::MIN).ceil()
        );

        assert_eq!(GraphRecordValue::Null, GraphRecordValue::Null.ceil());
    }

    #[test]
    fn test_floor() {
        assert_eq!(
            GraphRecordValue::String("value".to_string()),
            GraphRecordValue::String("value".to_string()).floor()
        );

        assert_eq!(
            GraphRecordValue::Int(1234),
            GraphRecordValue::Int(1234).floor()
        );

        assert_eq!(
            GraphRecordValue::Float(1234_f64),
            GraphRecordValue::Float(1234.3).floor()
        );

        assert_eq!(
            GraphRecordValue::Bool(false),
            GraphRecordValue::Bool(false).floor()
        );

        assert_eq!(
            GraphRecordValue::DateTime(NaiveDateTime::MAX),
            GraphRecordValue::DateTime(NaiveDateTime::MAX).floor()
        );

        assert_eq!(GraphRecordValue::Null, GraphRecordValue::Null.floor());
    }

    #[test]
    fn test_abs() {
        assert_eq!(
            GraphRecordValue::String("value".to_string()),
            GraphRecordValue::String("value".to_string()).abs()
        );

        assert_eq!(
            GraphRecordValue::Int(1234),
            GraphRecordValue::Int(1234).abs()
        );
        assert_eq!(
            GraphRecordValue::Int(1234),
            GraphRecordValue::Int(-1234).abs()
        );

        assert_eq!(
            GraphRecordValue::Float(1234_f64),
            GraphRecordValue::Float(1234_f64).abs()
        );
        assert_eq!(
            GraphRecordValue::Float(1234_f64),
            GraphRecordValue::Float(-1234_f64).abs()
        );

        assert_eq!(
            GraphRecordValue::Bool(false),
            GraphRecordValue::Bool(false).abs()
        );

        assert_eq!(
            GraphRecordValue::DateTime(NaiveDateTime::MIN),
            GraphRecordValue::DateTime(NaiveDateTime::MIN).abs()
        );

        assert_eq!(GraphRecordValue::Null, GraphRecordValue::Null.abs());
    }

    #[test]
    fn test_sqrt() {
        assert_eq!(
            GraphRecordValue::String("value".to_string()),
            GraphRecordValue::String("value".to_string()).sqrt()
        );

        assert_eq!(
            GraphRecordValue::Float(2_f64),
            GraphRecordValue::Int(4).sqrt()
        );

        assert_eq!(
            GraphRecordValue::Float(2_f64),
            GraphRecordValue::Float(4_f64).sqrt()
        );

        assert_eq!(
            GraphRecordValue::Bool(false),
            GraphRecordValue::Bool(false).sqrt()
        );

        assert_eq!(
            GraphRecordValue::DateTime(NaiveDateTime::MIN),
            GraphRecordValue::DateTime(NaiveDateTime::MIN).sqrt()
        );

        assert_eq!(GraphRecordValue::Null, GraphRecordValue::Null.sqrt());
    }

    #[test]
    fn test_trim() {
        assert_eq!(
            GraphRecordValue::String("value".to_string()),
            GraphRecordValue::String("  value  ".to_string()).trim()
        );

        assert_eq!(
            GraphRecordValue::Int(1234),
            GraphRecordValue::Int(1234).trim()
        );

        assert_eq!(
            GraphRecordValue::Float(1234_f64),
            GraphRecordValue::Float(1234_f64).trim()
        );

        assert_eq!(
            GraphRecordValue::Bool(false),
            GraphRecordValue::Bool(false).trim()
        );

        assert_eq!(
            GraphRecordValue::DateTime(NaiveDateTime::MIN),
            GraphRecordValue::DateTime(NaiveDateTime::MIN).trim()
        );

        assert_eq!(GraphRecordValue::Null, GraphRecordValue::Null.trim());
    }

    #[test]
    fn test_trim_start() {
        assert_eq!(
            GraphRecordValue::String("value  ".to_string()),
            GraphRecordValue::String("  value  ".to_string()).trim_start()
        );

        assert_eq!(
            GraphRecordValue::Int(1234),
            GraphRecordValue::Int(1234).trim_start()
        );

        assert_eq!(
            GraphRecordValue::Float(1234_f64),
            GraphRecordValue::Float(1234_f64).trim_start()
        );

        assert_eq!(
            GraphRecordValue::Bool(false),
            GraphRecordValue::Bool(false).trim_start()
        );

        assert_eq!(
            GraphRecordValue::DateTime(NaiveDateTime::MIN),
            GraphRecordValue::DateTime(NaiveDateTime::MIN).trim_start()
        );

        assert_eq!(GraphRecordValue::Null, GraphRecordValue::Null.trim_start());
    }

    #[test]
    fn test_trim_end() {
        assert_eq!(
            GraphRecordValue::String("  value".to_string()),
            GraphRecordValue::String("  value  ".to_string()).trim_end()
        );

        assert_eq!(
            GraphRecordValue::Int(1234),
            GraphRecordValue::Int(1234).trim_end()
        );

        assert_eq!(
            GraphRecordValue::Float(1234_f64),
            GraphRecordValue::Float(1234_f64).trim_end()
        );

        assert_eq!(
            GraphRecordValue::Bool(false),
            GraphRecordValue::Bool(false).trim_end()
        );

        assert_eq!(
            GraphRecordValue::DateTime(NaiveDateTime::MIN),
            GraphRecordValue::DateTime(NaiveDateTime::MIN).trim_end()
        );

        assert_eq!(GraphRecordValue::Null, GraphRecordValue::Null.trim_end());
    }

    #[test]
    fn test_lowercase() {
        assert_eq!(
            GraphRecordValue::String("value".to_string()),
            GraphRecordValue::String("VaLuE".to_string()).lowercase()
        );

        assert_eq!(
            GraphRecordValue::Int(1234),
            GraphRecordValue::Int(1234).lowercase()
        );

        assert_eq!(
            GraphRecordValue::Float(1234_f64),
            GraphRecordValue::Float(1234_f64).lowercase()
        );

        assert_eq!(
            GraphRecordValue::Bool(false),
            GraphRecordValue::Bool(false).lowercase()
        );

        assert_eq!(
            GraphRecordValue::DateTime(NaiveDateTime::MIN),
            GraphRecordValue::DateTime(NaiveDateTime::MIN).lowercase()
        );

        assert_eq!(GraphRecordValue::Null, GraphRecordValue::Null.lowercase());
    }

    #[test]
    fn test_uppercase() {
        assert_eq!(
            GraphRecordValue::String("VALUE".to_string()),
            GraphRecordValue::String("VaLuE".to_string()).uppercase()
        );

        assert_eq!(
            GraphRecordValue::Int(1234),
            GraphRecordValue::Int(1234).uppercase()
        );

        assert_eq!(
            GraphRecordValue::Float(1234_f64),
            GraphRecordValue::Float(1234_f64).uppercase()
        );

        assert_eq!(
            GraphRecordValue::Bool(false),
            GraphRecordValue::Bool(false).uppercase()
        );

        assert_eq!(
            GraphRecordValue::DateTime(NaiveDateTime::MIN),
            GraphRecordValue::DateTime(NaiveDateTime::MIN).uppercase()
        );

        assert_eq!(GraphRecordValue::Null, GraphRecordValue::Null.uppercase());
    }
}
