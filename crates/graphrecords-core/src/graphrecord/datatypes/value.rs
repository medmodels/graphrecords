use super::{
    Abs, Ceil, Contains, EndsWith, Floor, Lowercase, Mod, Pow, Round, Slice, Sqrt, StartsWith,
    Trim, TrimEnd, TrimStart, Uppercase,
};
use crate::errors::{GraphRecordError, GraphRecordResult, ValueOperation};
use chrono::{DateTime, NaiveDateTime, TimeDelta};
use graphrecords_utils::implement_from_for_wrapper;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    fmt::Display,
    hash::{Hash, Hasher},
    ops::{Add, Div, Mul, Range, Sub},
};

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Value {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    DateTime(NaiveDateTime),
    Duration(TimeDelta),
    #[default]
    Null,
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        value.to_string().into()
    }
}

// TODO: Add tests for Duration
implement_from_for_wrapper!(Value, String, String);
implement_from_for_wrapper!(Value, i64, Int);
implement_from_for_wrapper!(Value, f64, Float);
implement_from_for_wrapper!(Value, bool, Bool);
implement_from_for_wrapper!(Value, NaiveDateTime, DateTime);
implement_from_for_wrapper!(Value, TimeDelta, Duration);

impl<T> From<Option<T>> for Value
where
    T: Into<Self>,
{
    fn from(value: Option<T>) -> Self {
        match value {
            Some(value) => value.into(),
            None => Self::Null,
        }
    }
}

fn canonicalize_float(value: f64) -> f64 {
    if value.is_nan() {
        f64::NAN
    } else if value == 0.0 {
        0.0_f64
    } else {
        value
    }
}

fn int_float_eq(int_value: i64, float_value: f64) -> bool {
    let converted = int_value as f64;

    converted == float_value && converted as i64 == int_value
}

fn int_float_cmp(int_value: i64, float_value: f64) -> Option<Ordering> {
    if float_value.is_nan() {
        return None;
    }

    match (int_value as f64).partial_cmp(&float_value) {
        Some(Ordering::Equal) => Some(i128::from(int_value).cmp(&(float_value as i128))),
        ordering => ordering,
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::String(value), Self::String(other)) => value == other,
            (Self::Int(value), Self::Int(other)) => value == other,
            (Self::Int(int_value), Self::Float(float_value))
            | (Self::Float(float_value), Self::Int(int_value)) => {
                int_float_eq(*int_value, *float_value)
            }
            (Self::Float(value), Self::Float(other)) => {
                if value.is_nan() {
                    other.is_nan()
                } else {
                    value == other
                }
            }
            (Self::Bool(value), Self::Bool(other)) => value == other,
            (Self::DateTime(value), Self::DateTime(other)) => value == other,
            (Self::Duration(value), Self::Duration(other)) => value == other,
            (Self::Null, Self::Null) => true,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl Value {
    const fn variant_rank(&self) -> u8 {
        match self {
            Self::Null => 0,
            Self::Bool(_) => 1,
            Self::Int(_) | Self::Float(_) => 2,
            Self::String(_) => 3,
            Self::DateTime(_) => 4,
            Self::Duration(_) => 5,
        }
    }

    #[must_use]
    pub fn total_cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::String(value), Self::String(other)) => value.cmp(other),
            (Self::Int(value), Self::Int(other)) => value.cmp(other),
            (Self::Int(value), Self::Float(other)) => (*value as f64).total_cmp(other),
            (Self::Float(value), Self::Int(other)) => value.total_cmp(&(*other as f64)),
            (Self::Float(value), Self::Float(other)) => value.total_cmp(other),
            (Self::Bool(value), Self::Bool(other)) => value.cmp(other),
            (Self::DateTime(value), Self::DateTime(other)) => value.cmp(other),
            (Self::Duration(value), Self::Duration(other)) => value.cmp(other),
            (Self::Null, Self::Null) => Ordering::Equal,
            _ => self.variant_rank().cmp(&other.variant_rank()),
        }
    }

    fn hash_discriminant<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Int(_) | Self::Float(_) => 0_u8.hash(state),
            Self::String(_) => 1_u8.hash(state),
            Self::Bool(_) => 2_u8.hash(state),
            Self::DateTime(_) => 3_u8.hash(state),
            Self::Duration(_) => 4_u8.hash(state),
            Self::Null => 5_u8.hash(state),
        }
    }
}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash_discriminant(state);
        match self {
            Self::Int(value) => {
                canonicalize_float(*value as f64).to_bits().hash(state);
            }
            Self::Float(value) => {
                canonicalize_float(*value).to_bits().hash(state);
            }
            Self::String(value) => value.hash(state),
            Self::Bool(value) => value.hash(state),
            Self::DateTime(value) => value.hash(state),
            Self::Duration(value) => value.hash(state),
            Self::Null => {}
        }
    }
}

// TODO: Add tests for Duration
impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::String(value), Self::String(other)) => Some(value.cmp(other)),
            (Self::Int(value), Self::Int(other)) => Some(value.cmp(other)),
            (Self::Int(value), Self::Float(other)) => int_float_cmp(*value, *other),
            (Self::Float(value), Self::Int(other)) => {
                int_float_cmp(*other, *value).map(Ordering::reverse)
            }
            (Self::Float(value), Self::Float(other)) => {
                if value.is_nan() && other.is_nan() {
                    Some(Ordering::Equal)
                } else {
                    value.partial_cmp(other)
                }
            }
            (Self::Bool(value), Self::Bool(other)) => Some(value.cmp(other)),
            (Self::DateTime(value), Self::DateTime(other)) => Some(value.cmp(other)),
            (Self::Duration(value), Self::Duration(other)) => Some(value.cmp(other)),
            (Self::Null, Self::Null) => Some(Ordering::Equal),
            _ => None,
        }
    }
}

// TODO: Add tests for Duration
impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::String(value) => write!(f, "\"{value}\""),
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
impl Add for Value {
    type Output = GraphRecordResult<Self>;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Self::String(value), Self::String(rhs)) => Ok(Self::String(value + rhs.as_str())),
            (Self::Int(value), Self::Int(rhs)) => Ok(Self::Int(value + rhs)),
            (Self::Int(value), Self::Float(rhs)) => Ok(Self::Float(value as f64 + rhs)),
            (Self::Float(value), Self::Int(rhs)) => Ok(Self::Float(value + rhs as f64)),
            (Self::Float(value), Self::Float(rhs)) => Ok(Self::Float(value + rhs)),
            (Self::DateTime(value), Self::DateTime(rhs)) => Ok(DateTime::from_timestamp(
                value.and_utc().timestamp() + rhs.and_utc().timestamp(),
                0,
            )
            .ok_or(GraphRecordError::InvalidTimestamp)?
            .naive_utc()
            .into()),
            (Self::DateTime(value), Self::Duration(rhs)) => Ok(value.add(rhs).into()),
            (Self::Duration(value), Self::Duration(rhs)) => Ok((value + rhs).into()),
            (left, right) => Err(GraphRecordError::IncompatibleValueOperands {
                operation: ValueOperation::Add,
                left,
                right,
            }),
        }
    }
}

// TODO: Add tests for Duration
impl Sub for Value {
    type Output = GraphRecordResult<Self>;

    fn sub(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Self::Int(value), Self::Int(rhs)) => Ok(Self::Int(value - rhs)),
            (Self::Int(value), Self::Float(rhs)) => Ok(Self::Float(value as f64 - rhs)),
            (Self::Float(value), Self::Int(rhs)) => Ok(Self::Float(value - rhs as f64)),
            (Self::Float(value), Self::Float(rhs)) => Ok(Self::Float(value - rhs)),
            (Self::DateTime(value), Self::DateTime(rhs)) => {
                let duration = value - rhs;

                Ok(duration.into())
            }
            (Self::DateTime(value), Self::Duration(rhs)) => Ok((value - rhs).into()),
            (Self::Duration(value), Self::Duration(rhs)) => Ok((value - rhs).into()),
            (left, right) => Err(GraphRecordError::IncompatibleValueOperands {
                operation: ValueOperation::Subtract,
                left,
                right,
            }),
        }
    }
}

impl Mul for Value {
    type Output = GraphRecordResult<Self>;

    fn mul(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Self::String(value), Self::Int(other)) => Ok(Self::String(
                value.repeat(usize::try_from(other).unwrap_or(0)),
            )),
            (Self::Int(value), Self::String(other)) => Ok(Self::String(
                other.repeat(usize::try_from(value).unwrap_or(0)),
            )),
            (Self::Int(value), Self::Int(other)) => Ok(Self::Int(value * other)),
            (Self::Int(value), Self::Float(other)) => Ok(Self::Float(value as f64 * other)),
            (Self::Int(value), Self::Duration(other)) => Ok((other * (value as i32)).into()),
            (Self::Float(value), Self::Int(other)) => Ok(Self::Float(value * other as f64)),
            (Self::Float(value), Self::Float(other)) => Ok(Self::Float(value * other)),
            (Self::Duration(value), Self::Int(other)) => Ok((value * (other as i32)).into()),
            (left, right) => Err(GraphRecordError::IncompatibleValueOperands {
                operation: ValueOperation::Multiply,
                left,
                right,
            }),
        }
    }
}

impl Div for Value {
    type Output = GraphRecordResult<Self>;

    fn div(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Self::Int(value), Self::Int(other)) => Ok(Self::Float(value as f64 / other as f64)),
            (Self::Int(value), Self::Float(other)) => Ok(Self::Float(value as f64 / other)),
            (Self::Float(value), Self::Int(other)) => Ok(Self::Float(value / other as f64)),
            (Self::Float(value), Self::Float(other)) => Ok(Self::Float(value / other)),
            (Self::Duration(value), Self::Int(other)) => Ok((value / (other as i32)).into()),
            (left, right) => Err(GraphRecordError::IncompatibleValueOperands {
                operation: ValueOperation::Divide,
                left,
                right,
            }),
        }
    }
}

impl Pow for Value {
    fn pow(self, exp: Self) -> GraphRecordResult<Self> {
        match (self, exp) {
            (Self::Int(value), Self::Int(exp)) => Ok(Self::Int(value.pow(exp as u32))),
            (Self::Int(value), Self::Float(exp)) => Ok(Self::Float((value as f64).powf(exp))),
            (Self::Float(value), Self::Int(exp)) => Ok(Self::Float(value.powi(exp as i32))),
            (Self::Float(value), Self::Float(exp)) => Ok(Self::Float(value.powf(exp))),
            (left, right) => Err(GraphRecordError::IncompatibleValueOperands {
                operation: ValueOperation::Power,
                left,
                right,
            }),
        }
    }
}

impl Mod for Value {
    fn r#mod(self, other: Self) -> GraphRecordResult<Self> {
        match (self, other) {
            (Self::Int(value), Self::Int(other)) => Ok(Self::Int(value % other)),
            (Self::Int(value), Self::Float(other)) => Ok(Self::Float(value as f64 % other)),
            (Self::Float(value), Self::Int(other)) => Ok(Self::Float(value % other as f64)),
            (Self::Float(value), Self::Float(other)) => Ok(Self::Float(value % other)),
            (left, right) => Err(GraphRecordError::IncompatibleValueOperands {
                operation: ValueOperation::Modulo,
                left,
                right,
            }),
        }
    }
}

impl Round for Value {
    fn round(self) -> Self {
        match self {
            Self::Float(value) => Self::Float(value.round()),
            _ => self,
        }
    }
}

impl Ceil for Value {
    fn ceil(self) -> Self {
        match self {
            Self::Float(value) => Self::Float(value.ceil()),
            _ => self,
        }
    }
}

impl Floor for Value {
    fn floor(self) -> Self {
        match self {
            Self::Float(value) => Self::Float(value.floor()),
            _ => self,
        }
    }
}

impl Abs for Value {
    fn abs(self) -> Self {
        match self {
            Self::Int(value) => Self::Int(value.abs()),
            Self::Float(value) => Self::Float(value.abs()),
            _ => self,
        }
    }
}

impl Sqrt for Value {
    fn sqrt(self) -> Self {
        match self {
            Self::Int(value) => Self::Float((value as f64).sqrt()),
            Self::Float(value) => Self::Float(value.sqrt()),
            _ => self,
        }
    }
}

impl StartsWith for Value {
    fn starts_with(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::String(value), Self::String(other)) => value.starts_with(other),
            (Self::String(value), Self::Int(other)) => value.starts_with(&other.to_string()),
            (Self::String(value), Self::Float(other)) => value.starts_with(&other.to_string()),
            (Self::Int(value), Self::String(other)) => value.to_string().starts_with(other),
            (Self::Int(value), Self::Int(other)) => {
                value.to_string().starts_with(&other.to_string())
            }
            (Self::Int(value), Self::Float(other)) => {
                value.to_string().starts_with(&other.to_string())
            }
            (Self::Float(value), Self::String(other)) => value.to_string().starts_with(other),
            (Self::Float(value), Self::Int(other)) => {
                value.to_string().starts_with(&other.to_string())
            }
            (Self::Float(value), Self::Float(other)) => {
                value.to_string().starts_with(&other.to_string())
            }
            _ => false,
        }
    }
}

impl EndsWith for Value {
    fn ends_with(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::String(value), Self::String(other)) => value.ends_with(other),
            (Self::String(value), Self::Int(other)) => value.ends_with(&other.to_string()),
            (Self::String(value), Self::Float(other)) => value.ends_with(&other.to_string()),
            (Self::Int(value), Self::String(other)) => value.to_string().ends_with(other),
            (Self::Int(value), Self::Int(other)) => value.to_string().ends_with(&other.to_string()),
            (Self::Int(value), Self::Float(other)) => {
                value.to_string().ends_with(&other.to_string())
            }
            (Self::Float(value), Self::String(other)) => value.to_string().ends_with(other),
            (Self::Float(value), Self::Int(other)) => {
                value.to_string().ends_with(&other.to_string())
            }
            (Self::Float(value), Self::Float(other)) => {
                value.to_string().ends_with(&other.to_string())
            }
            _ => false,
        }
    }
}

impl Contains for Value {
    fn contains(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::String(value), Self::String(other)) => value.contains(other),
            (Self::String(value), Self::Int(other)) => value.contains(&other.to_string()),
            (Self::String(value), Self::Float(other)) => value.contains(&other.to_string()),
            (Self::Int(value), Self::String(other)) => value.to_string().contains(other),
            (Self::Int(value), Self::Int(other)) => value.to_string().contains(&other.to_string()),
            (Self::Int(value), Self::Float(other)) => {
                value.to_string().contains(&other.to_string())
            }
            (Self::Float(value), Self::String(other)) => value.to_string().contains(other),
            (Self::Float(value), Self::Int(other)) => {
                value.to_string().contains(&other.to_string())
            }
            (Self::Float(value), Self::Float(other)) => {
                value.to_string().contains(&other.to_string())
            }
            _ => false,
        }
    }
}

impl Slice for Value {
    fn slice(self, range: Range<usize>) -> Self {
        match self {
            Self::String(value) => value[range].into(),
            Self::Int(value) => value.to_string()[range].into(),
            Self::Float(value) => value.to_string()[range].into(),
            Self::Bool(value) => value.to_string()[range].into(),
            _ => self,
        }
    }
}

impl Trim for Value {
    fn trim(self) -> Self {
        match self {
            Self::String(value) => Self::String(value.trim().to_string()),
            _ => self,
        }
    }
}

impl TrimStart for Value {
    fn trim_start(self) -> Self {
        match self {
            Self::String(value) => Self::String(value.trim_start().to_string()),
            _ => self,
        }
    }
}

impl TrimEnd for Value {
    fn trim_end(self) -> Self {
        match self {
            Self::String(value) => Self::String(value.trim_end().to_string()),
            _ => self,
        }
    }
}

impl Lowercase for Value {
    fn lowercase(self) -> Self {
        match self {
            Self::String(value) => Self::String(value.to_lowercase()),
            _ => self,
        }
    }
}

impl Uppercase for Value {
    fn uppercase(self) -> Self {
        match self {
            Self::String(value) => Self::String(value.to_uppercase()),
            _ => self,
        }
    }
}

#[cfg(test)]
mod test {
    use super::{Contains, EndsWith, StartsWith, Value};
    use crate::{
        errors::GraphRecordError,
        graphrecord::datatypes::{
            Abs, Ceil, Floor, Lowercase, Mod, Pow, Round, Slice, Sqrt, Trim, TrimEnd, TrimStart,
            Uppercase,
        },
    };
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeDelta};
    use std::hash::{Hash, Hasher};

    #[test]
    fn test_default() {
        let value = Value::default();

        assert_eq!(Value::Null, value);
    }

    #[test]
    fn test_from_str() {
        let value = Value::from("value");

        assert_eq!(Value::String("value".to_string()), value);
    }

    #[test]
    fn test_from_string() {
        let value = Value::from("value".to_string());

        assert_eq!(Value::String("value".to_string()), value);
    }

    #[test]
    fn test_from_int() {
        let value = Value::from(0);

        assert_eq!(Value::Int(0), value);
    }

    #[test]
    fn test_from_f64() {
        let value = Value::from(0_f64);

        assert_eq!(Value::Float(0.0), value);
    }

    #[test]
    fn test_from_bool() {
        let value = Value::from(false);

        assert_eq!(Value::Bool(false), value);
    }

    #[test]
    fn test_from_datetime() {
        let value = Value::from(NaiveDateTime::MIN);

        assert_eq!(Value::DateTime(NaiveDateTime::MIN), value);
    }

    #[test]
    fn test_from_option() {
        let value = Value::from(Some("value"));

        assert_eq!(Value::String("value".to_string()), value);

        let value = Value::from(None::<String>);

        assert_eq!(Value::Null, value);
    }

    #[test]
    fn test_partial_eq() {
        assert_eq!(
            Value::String("value".to_string()),
            Value::String("value".to_string())
        );
        assert_ne!(
            Value::String("value2".to_string()),
            Value::String("value".to_string())
        );

        assert_eq!(Value::Int(0), Value::Int(0));
        assert_ne!(Value::Int(1), Value::Int(0));

        assert_eq!(Value::Int(0), Value::Float(0_f64));
        assert_ne!(Value::Int(1), Value::Float(0_f64));
        assert_eq!(Value::Int(1), Value::Float(1_f64));

        assert_eq!(Value::Float(0_f64), Value::Float(0_f64));
        assert_ne!(Value::Float(1_f64), Value::Float(0_f64));

        assert_eq!(Value::Float(0_f64), Value::Int(0));
        assert_ne!(Value::Float(1_f64), Value::Int(0));

        assert_eq!(Value::Float(f64::NAN), Value::Float(f64::NAN));
        assert_eq!(Value::Float(-0.0), Value::Float(0.0));

        let large_int = (1_i64 << 53) + 1;
        assert_ne!(Value::Int(large_int), Value::Float(large_int as f64));
        assert_ne!(Value::Float(large_int as f64), Value::Int(large_int));

        assert_eq!(Value::Bool(false), Value::Bool(false));
        assert_ne!(Value::Bool(true), Value::Bool(false));

        assert_eq!(
            Value::DateTime(NaiveDateTime::MIN),
            Value::DateTime(NaiveDateTime::MIN)
        );
        assert_ne!(
            Value::DateTime(NaiveDateTime::MAX),
            Value::DateTime(NaiveDateTime::MIN)
        );

        assert_eq!(Value::Null, Value::Null);

        assert_ne!(Value::String("0".to_string()), Value::Int(0));
        assert_ne!(Value::String("0".to_string()), Value::Float(0_f64));
        assert_ne!(Value::String("false".to_string()), Value::Bool(false));
        assert_ne!(
            Value::String("false".to_string()),
            Value::DateTime(NaiveDateTime::MIN)
        );
        assert_ne!(Value::String("false".to_string()), Value::Null);

        assert_ne!(Value::Int(0), Value::String("0".to_string()));
        assert_ne!(Value::Int(0), Value::Bool(false));
        assert_ne!(Value::Int(0), Value::DateTime(NaiveDateTime::MIN));
        assert_ne!(Value::Int(0), Value::Null);

        assert_ne!(Value::Float(0_f64), Value::String("0.0".to_string()));
        assert_ne!(Value::Float(0_f64), Value::Bool(false));
        assert_ne!(Value::Float(0_f64), Value::DateTime(NaiveDateTime::MIN));
        assert_ne!(Value::Float(0_f64), Value::Null);

        assert_ne!(Value::Bool(false), Value::String("false".to_string()));
        assert_ne!(Value::Bool(false), Value::Int(0));
        assert_ne!(Value::Bool(false), Value::Float(0_f64));
        assert_ne!(Value::Bool(false), Value::DateTime(NaiveDateTime::MIN));
        assert_ne!(Value::Bool(false), Value::Null);

        assert_ne!(Value::Null, Value::String("false".to_string()));
        assert_ne!(Value::Null, Value::Int(0));
        assert_ne!(Value::Null, Value::Float(0_f64));
        assert_ne!(Value::Null, Value::Bool(false));
        assert_ne!(Value::Null, Value::DateTime(NaiveDateTime::MIN));
    }

    #[test]
    #[allow(clippy::neg_cmp_op_on_partial_ord)]
    fn test_partial_ord() {
        assert!(Value::String("b".to_string()) > Value::String("a".to_string()));
        assert!(Value::String("b".to_string()) >= Value::String("a".to_string()));
        assert!(Value::String("a".to_string()) < Value::String("b".to_string()));
        assert!(Value::String("a".to_string()) <= Value::String("b".to_string()));
        assert!(Value::String("a".to_string()) >= Value::String("a".to_string()));
        assert!(Value::String("a".to_string()) <= Value::String("a".to_string()));

        assert!(Value::Int(1) > Value::Int(0));
        assert!(Value::Int(1) >= Value::Int(0));
        assert!(Value::Int(0) < Value::Int(1));
        assert!(Value::Int(0) <= Value::Int(1));
        assert!(Value::Int(0) >= Value::Int(0));
        assert!(Value::Int(0) <= Value::Int(0));

        assert!(Value::Int(1) > Value::Float(0_f64));
        assert!(Value::Int(1) >= Value::Float(0_f64));
        assert!(Value::Int(0) < Value::Float(1_f64));
        assert!(Value::Int(0) <= Value::Float(1_f64));
        assert!(Value::Int(0) >= Value::Float(0_f64));
        assert!(Value::Int(0) <= Value::Float(0_f64));

        assert!(Value::Float(1_f64) > Value::Int(0));
        assert!(Value::Float(1_f64) >= Value::Int(0));
        assert!(Value::Float(0_f64) < Value::Int(1));
        assert!(Value::Float(0_f64) <= Value::Int(1));
        assert!(Value::Float(0_f64) >= Value::Int(0));
        assert!(Value::Float(0_f64) <= Value::Int(0));

        assert!(Value::Bool(true) > Value::Bool(false));
        assert!(Value::Bool(true) >= Value::Bool(false));
        assert!(Value::Bool(false) < Value::Bool(true));
        assert!(Value::Bool(false) <= Value::Bool(true));
        assert!(Value::Bool(false) >= Value::Bool(false));
        assert!(Value::Bool(false) <= Value::Bool(false));

        assert!(Value::DateTime(NaiveDateTime::MAX) > Value::DateTime(NaiveDateTime::MIN));
        assert!(Value::DateTime(NaiveDateTime::MAX) >= Value::DateTime(NaiveDateTime::MIN));
        assert!(Value::DateTime(NaiveDateTime::MIN) < Value::DateTime(NaiveDateTime::MAX));
        assert!(Value::DateTime(NaiveDateTime::MIN) <= Value::DateTime(NaiveDateTime::MAX));
        assert!(Value::DateTime(NaiveDateTime::MIN) >= Value::DateTime(NaiveDateTime::MIN));
        assert!(Value::DateTime(NaiveDateTime::MIN) <= Value::DateTime(NaiveDateTime::MIN));

        assert!(Value::Null <= Value::Null);
        assert!(Value::Null >= Value::Null);

        assert!(!(Value::String("a".to_string()) > Value::Int(1)));
        assert!(!(Value::String("a".to_string()) >= Value::Int(1)));
        assert!(!(Value::String("a".to_string()) < Value::Int(1)));
        assert!(!(Value::String("a".to_string()) <= Value::Int(1)));

        assert!(!(Value::String("a".to_string()) > Value::Float(1_f64)));
        assert!(!(Value::String("a".to_string()) >= Value::Float(1_f64)));
        assert!(!(Value::String("a".to_string()) < Value::Float(1_f64)));
        assert!(!(Value::String("a".to_string()) <= Value::Float(1_f64)));

        assert!(!(Value::String("a".to_string()) > Value::Bool(true)));
        assert!(!(Value::String("a".to_string()) >= Value::Bool(true)));
        assert!(!(Value::String("a".to_string()) < Value::Bool(true)));
        assert!(!(Value::String("a".to_string()) <= Value::Bool(true)));

        assert!(!(Value::String("a".to_string()) > Value::DateTime(NaiveDateTime::MAX)));
        assert!(!(Value::String("a".to_string()) >= Value::DateTime(NaiveDateTime::MAX)));
        assert!(!(Value::String("a".to_string()) < Value::DateTime(NaiveDateTime::MAX)));
        assert!(!(Value::String("a".to_string()) <= Value::DateTime(NaiveDateTime::MAX)));

        assert!(!(Value::String("a".to_string()) > Value::Null));
        assert!(!(Value::String("a".to_string()) >= Value::Null));
        assert!(!(Value::String("a".to_string()) < Value::Null));
        assert!(!(Value::String("a".to_string()) <= Value::Null));

        assert!(!(Value::Int(1) > Value::String("a".to_string())));
        assert!(!(Value::Int(1) >= Value::String("a".to_string())));
        assert!(!(Value::Int(1) < Value::String("a".to_string())));
        assert!(!(Value::Int(1) <= Value::String("a".to_string())));

        assert!(!(Value::Int(1) > Value::Bool(true)));
        assert!(!(Value::Int(1) >= Value::Bool(true)));
        assert!(!(Value::Int(1) < Value::Bool(true)));
        assert!(!(Value::Int(1) <= Value::Bool(true)));

        assert!(!(Value::Int(1) > Value::DateTime(NaiveDateTime::MAX)));
        assert!(!(Value::Int(1) >= Value::DateTime(NaiveDateTime::MAX)));
        assert!(!(Value::Int(1) < Value::DateTime(NaiveDateTime::MAX)));
        assert!(!(Value::Int(1) <= Value::DateTime(NaiveDateTime::MAX)));

        assert!(!(Value::Int(1) > Value::Null));
        assert!(!(Value::Int(1) >= Value::Null));
        assert!(!(Value::Int(1) < Value::Null));
        assert!(!(Value::Int(1) <= Value::Null));

        assert!(!(Value::Float(1_f64) > Value::String("a".to_string())));
        assert!(!(Value::Float(1_f64) >= Value::String("a".to_string())));
        assert!(!(Value::Float(1_f64) < Value::String("a".to_string())));
        assert!(!(Value::Float(1_f64) <= Value::String("a".to_string())));

        assert!(!(Value::Float(1_f64) > Value::Bool(true)));
        assert!(!(Value::Float(1_f64) >= Value::Bool(true)));
        assert!(!(Value::Float(1_f64) < Value::Bool(true)));
        assert!(!(Value::Float(1_f64) <= Value::Bool(true)));

        assert!(!(Value::Float(1_f64) > Value::DateTime(NaiveDateTime::MAX)));
        assert!(!(Value::Float(1_f64) >= Value::DateTime(NaiveDateTime::MAX)));
        assert!(!(Value::Float(1_f64) < Value::DateTime(NaiveDateTime::MAX)));
        assert!(!(Value::Float(1_f64) <= Value::DateTime(NaiveDateTime::MAX)));

        assert!(!(Value::Float(1_f64) > Value::Null));
        assert!(!(Value::Float(1_f64) >= Value::Null));
        assert!(!(Value::Float(1_f64) < Value::Null));
        assert!(!(Value::Float(1_f64) <= Value::Null));

        assert!(!(Value::Bool(true) > Value::String("a".to_string())));
        assert!(!(Value::Bool(true) >= Value::String("a".to_string())));
        assert!(!(Value::Bool(true) < Value::String("a".to_string())));
        assert!(!(Value::Bool(true) <= Value::String("a".to_string())));

        assert!(!(Value::Bool(true) > Value::Int(1)));
        assert!(!(Value::Bool(true) >= Value::Int(1)));
        assert!(!(Value::Bool(true) < Value::Int(1)));
        assert!(!(Value::Bool(true) <= Value::Int(1)));

        assert!(!(Value::Bool(true) > Value::Float(1_f64)));
        assert!(!(Value::Bool(true) >= Value::Float(1_f64)));
        assert!(!(Value::Bool(true) < Value::Float(1_f64)));
        assert!(!(Value::Bool(true) <= Value::Float(1_f64)));

        assert!(!(Value::Bool(true) > Value::DateTime(NaiveDateTime::MAX)));
        assert!(!(Value::Bool(true) >= Value::DateTime(NaiveDateTime::MAX)));
        assert!(!(Value::Bool(true) < Value::DateTime(NaiveDateTime::MAX)));
        assert!(!(Value::Bool(true) <= Value::DateTime(NaiveDateTime::MAX)));

        assert!(!(Value::Bool(true) > Value::Null));
        assert!(!(Value::Bool(true) >= Value::Null));
        assert!(!(Value::Bool(true) < Value::Null));
        assert!(!(Value::Bool(true) <= Value::Null));

        assert!(!(Value::DateTime(NaiveDateTime::MAX) > Value::String("a".to_string())));
        assert!(!(Value::DateTime(NaiveDateTime::MAX) >= Value::String("a".to_string())));
        assert!(!(Value::DateTime(NaiveDateTime::MAX) < Value::String("a".to_string())));
        assert!(!(Value::DateTime(NaiveDateTime::MAX) <= Value::String("a".to_string())));

        assert!(!(Value::DateTime(NaiveDateTime::MAX) > Value::Int(1)));
        assert!(!(Value::DateTime(NaiveDateTime::MAX) >= Value::Int(1)));
        assert!(!(Value::DateTime(NaiveDateTime::MAX) < Value::Int(1)));
        assert!(!(Value::DateTime(NaiveDateTime::MAX) <= Value::Int(1)));

        assert!(!(Value::DateTime(NaiveDateTime::MAX) > Value::Float(1_f64)));
        assert!(!(Value::DateTime(NaiveDateTime::MAX) >= Value::Float(1_f64)));
        assert!(!(Value::DateTime(NaiveDateTime::MAX) < Value::Float(1_f64)));
        assert!(!(Value::DateTime(NaiveDateTime::MAX) <= Value::Float(1_f64)));

        assert!(!(Value::DateTime(NaiveDateTime::MAX) > Value::Bool(true)));
        assert!(!(Value::DateTime(NaiveDateTime::MAX) >= Value::Bool(true)));
        assert!(!(Value::DateTime(NaiveDateTime::MAX) < Value::Bool(true)));
        assert!(!(Value::DateTime(NaiveDateTime::MAX) <= Value::Bool(true)));

        assert!(!(Value::DateTime(NaiveDateTime::MAX) > Value::Null));
        assert!(!(Value::DateTime(NaiveDateTime::MAX) >= Value::Null));
        assert!(!(Value::DateTime(NaiveDateTime::MAX) < Value::Null));
        assert!(!(Value::DateTime(NaiveDateTime::MAX) <= Value::Null));

        assert!(!(Value::Null > Value::String("a".to_string())));
        assert!(!(Value::Null >= Value::String("a".to_string())));
        assert!(!(Value::Null < Value::String("a".to_string())));
        assert!(!(Value::Null <= Value::String("a".to_string())));

        assert!(!(Value::Null > Value::Int(0)));
        assert!(!(Value::Null >= Value::Int(0)));
        assert!(!(Value::Null < Value::Int(0)));
        assert!(!(Value::Null <= Value::Int(0)));

        assert!(!(Value::Null > Value::Float(0_f64)));
        assert!(!(Value::Null >= Value::Float(0_f64)));
        assert!(!(Value::Null < Value::Float(0_f64)));
        assert!(!(Value::Null <= Value::Float(0_f64)));

        assert!(!(Value::Null > Value::Bool(false)));
        assert!(!(Value::Null >= Value::Bool(false)));
        assert!(!(Value::Null < Value::Bool(false)));
        assert!(!(Value::Null <= Value::Bool(false)));

        assert!(!(Value::Null > Value::DateTime(NaiveDateTime::MAX)));
        assert!(!(Value::Null >= Value::DateTime(NaiveDateTime::MAX)));
        assert!(!(Value::Null < Value::DateTime(NaiveDateTime::MAX)));
        assert!(!(Value::Null <= Value::DateTime(NaiveDateTime::MAX)));

        assert!(!(Value::Null > Value::Null));
        assert!(!(Value::Null < Value::Null));
    }

    #[test]
    fn test_display() {
        assert_eq!("\"value\"", Value::String("value".to_string()).to_string());

        assert_eq!("0", Value::Int(0).to_string());

        assert_eq!("0.5", Value::Float(0.5).to_string());

        assert_eq!("false", Value::Bool(false).to_string());

        assert_eq!(
            "-262143-01-01 00:00:00",
            Value::DateTime(NaiveDateTime::MIN).to_string()
        );

        assert_eq!("Null", Value::Null.to_string());
    }

    #[test]
    fn test_add() {
        assert_eq!(
            Value::String("value".to_string()),
            (Value::String("val".to_string()) + Value::String("ue".to_string())).unwrap()
        );
        assert!(
            (Value::String("value".to_string()) + Value::Int(0))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::String("value".to_string()) + Value::Float(0_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::String("value".to_string()) + Value::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::String("value".to_string()) + Value::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::String("value".to_string()) + Value::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (Value::Int(0) + Value::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert_eq!(Value::Int(10), (Value::Int(5) + Value::Int(5)).unwrap());
        assert_eq!(
            Value::Float(10_f64),
            (Value::Int(5) + Value::Float(5_f64)).unwrap()
        );
        assert!(
            (Value::Int(0) + Value::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Int(0) + Value::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Int(0) + Value::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (Value::Float(0_f64) + Value::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert_eq!(
            Value::Float(10_f64),
            (Value::Float(5_f64) + Value::Int(5)).unwrap()
        );
        assert_eq!(
            Value::Float(10_f64),
            (Value::Float(5_f64) + Value::Float(5_f64)).unwrap()
        );
        assert!(
            (Value::Float(0_f64) + Value::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Float(0_f64) + Value::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Float(0_f64) + Value::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (Value::Bool(false) + Value::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Bool(false) + Value::Int(0))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Bool(false) + Value::Float(0_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Bool(false) + Value::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Bool(false) + Value::Bool(true))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Bool(true) + Value::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Bool(true) + Value::Bool(true))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Bool(false) + Value::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Bool(false) + Value::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (Value::DateTime(NaiveDateTime::MIN) + Value::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::DateTime(NaiveDateTime::MIN) + Value::Int(0))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::DateTime(NaiveDateTime::MIN) + Value::Float(0_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::DateTime(NaiveDateTime::MIN) + Value::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert_eq!(
            Value::DateTime(
                NaiveDate::from_ymd_opt(1970, 1, 4)
                    .unwrap()
                    .and_time(NaiveTime::MIN)
            ),
            (Value::DateTime(
                NaiveDate::from_ymd_opt(1970, 1, 2)
                    .unwrap()
                    .and_time(NaiveTime::MIN)
            ) + Value::DateTime(
                NaiveDate::from_ymd_opt(1970, 1, 3)
                    .unwrap()
                    .and_time(NaiveTime::MIN)
            ))
            .unwrap()
        );
        assert!(
            (Value::DateTime(NaiveDateTime::MIN) + Value::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (Value::Null + Value::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Null + Value::Int(0))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Null + Value::Float(0_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Null + Value::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Null + Value::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Null + Value::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
    }

    #[test]
    fn test_sub() {
        assert!(
            (Value::String("value".to_string()) - Value::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::String("value".to_string()) - Value::Int(0))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::String("value".to_string()) - Value::Float(0_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::String("value".to_string()) - Value::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::String("value".to_string()) - Value::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::String("value".to_string()) - Value::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (Value::Int(0) - Value::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert_eq!(Value::Int(0), (Value::Int(5) - Value::Int(5)).unwrap());
        assert_eq!(
            Value::Float(0_f64),
            (Value::Int(5) - Value::Float(5_f64)).unwrap()
        );
        assert!(
            (Value::Int(0) - Value::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Int(0) - Value::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Int(0) - Value::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (Value::Float(0_f64) - Value::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert_eq!(
            Value::Float(0_f64),
            (Value::Float(5_f64) - Value::Int(5)).unwrap()
        );
        assert_eq!(
            Value::Float(0_f64),
            (Value::Float(5_f64) - Value::Float(5_f64)).unwrap()
        );
        assert!(
            (Value::Float(0_f64) - Value::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Float(0_f64) - Value::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Float(0_f64) - Value::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (Value::Bool(false) - Value::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Bool(false) - Value::Int(0))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Bool(false) - Value::Float(0_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Bool(false) - Value::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Bool(false) - Value::Bool(true))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Bool(true) - Value::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Bool(true) - Value::Bool(true))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Bool(false) - Value::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Bool(false) - Value::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (Value::DateTime(NaiveDateTime::MIN) - Value::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::DateTime(NaiveDateTime::MIN) - Value::Int(0))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::DateTime(NaiveDateTime::MIN) - Value::Float(0_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::DateTime(NaiveDateTime::MIN) - Value::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::DateTime(NaiveDateTime::MIN) - Value::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (Value::Duration(TimeDelta::seconds(5)) - Value::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Duration(TimeDelta::seconds(5)) - Value::Int(0))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Duration(TimeDelta::seconds(5)) - Value::Float(0_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Duration(TimeDelta::seconds(5)) - Value::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Duration(TimeDelta::seconds(5)) - Value::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert_eq!(
            Value::Duration(TimeDelta::seconds(2)),
            (Value::Duration(TimeDelta::seconds(5)) - Value::Duration(TimeDelta::seconds(3)))
                .unwrap()
        );
        assert!(
            (Value::Duration(TimeDelta::seconds(5)) - Value::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (Value::Null - Value::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Null - Value::Int(0))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Null - Value::Float(0_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Null - Value::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Null - Value::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Null - Value::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
    }

    #[test]
    fn test_mul() {
        assert!(
            (Value::String("value".to_string()) * Value::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert_eq!(
            Value::String("valuevaluevalue".to_string()),
            (Value::String("value".to_string()) * Value::Int(3)).unwrap()
        );
        assert!(
            (Value::String("value".to_string()) * Value::Float(0_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::String("value".to_string()) * Value::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::String("value".to_string()) * Value::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::String("value".to_string()) * Value::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert_eq!(
            Value::String("valuevaluevalue".to_string()),
            (Value::Int(3) * Value::String("value".to_string())).unwrap()
        );
        assert_eq!(Value::Int(25), (Value::Int(5) * Value::Int(5)).unwrap());
        assert_eq!(
            Value::Float(25_f64),
            (Value::Int(5) * Value::Float(5_f64)).unwrap()
        );
        assert!(
            (Value::Int(0) * Value::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Int(0) * Value::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Int(0) * Value::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (Value::Float(0_f64) * Value::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert_eq!(
            Value::Float(25_f64),
            (Value::Float(5_f64) * Value::Int(5)).unwrap()
        );
        assert_eq!(
            Value::Float(25_f64),
            (Value::Float(5_f64) * Value::Float(5_f64)).unwrap()
        );
        assert!(
            (Value::Float(0_f64) * Value::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Float(0_f64) * Value::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Float(0_f64) * Value::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (Value::Bool(false) * Value::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Bool(false) * Value::Int(0))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Bool(false) * Value::Float(0_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Bool(false) * Value::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Bool(false) * Value::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Bool(false) * Value::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (Value::DateTime(NaiveDateTime::MIN) * Value::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::DateTime(NaiveDateTime::MIN) * Value::Int(0))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::DateTime(NaiveDateTime::MIN) * Value::Float(0_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::DateTime(NaiveDateTime::MIN) * Value::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::DateTime(NaiveDateTime::MIN) * Value::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::DateTime(NaiveDateTime::MIN) * Value::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (Value::Null * Value::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Null * Value::Int(0))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Null * Value::Float(0_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Null * Value::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Null * Value::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Null * Value::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
    }

    #[test]
    fn test_div() {
        assert!(
            (Value::String("value".to_string()) / Value::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::String("value".to_string()) / Value::Int(1))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::String("value".to_string()) / Value::Float(1_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::String("value".to_string()) / Value::Bool(true))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::String("value".to_string()) / Value::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::String("value".to_string()) / Value::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (Value::Int(0) / Value::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert_eq!(
            Value::Float(1_f64),
            (Value::Int(5) / Value::Int(5)).unwrap()
        );
        assert_eq!(
            Value::Float(1_f64),
            (Value::Int(5) / Value::Float(5_f64)).unwrap()
        );
        assert!(
            (Value::Int(0) / Value::Bool(true))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Int(0) / Value::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Int(0) / Value::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (Value::Float(0_f64) / Value::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert_eq!(
            Value::Float(1_f64),
            (Value::Float(5_f64) / Value::Int(5)).unwrap()
        );
        assert_eq!(
            Value::Float(1_f64),
            (Value::Float(5_f64) / Value::Float(5_f64)).unwrap()
        );
        assert!(
            (Value::Float(0_f64) / Value::Bool(true))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Float(0_f64) / Value::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Float(0_f64) / Value::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (Value::Bool(false) / Value::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Bool(false) / Value::Int(1))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Bool(false) / Value::Float(1_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Bool(false) / Value::Bool(true))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Bool(false) / Value::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Bool(false) / Value::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (Value::DateTime(NaiveDateTime::MIN) / Value::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::DateTime(NaiveDateTime::MIN) / Value::Int(1))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::DateTime(NaiveDateTime::MIN) / Value::Float(1_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::DateTime(NaiveDateTime::MIN) / Value::Bool(true))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::DateTime(NaiveDateTime::MIN) / Value::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::DateTime(NaiveDateTime::MIN) / Value::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (Value::Null / Value::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Null / Value::Int(1))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Null / Value::Float(1_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Null / Value::Bool(true))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Null / Value::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Null / Value::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
    }

    #[test]
    fn test_pow() {
        assert!(
            (Value::String("value".to_string()).pow(Value::String("value".to_string())))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::String("value".to_string()).pow(Value::Int(0)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::String("value".to_string()).pow(Value::Float(0_f64)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::String("value".to_string()).pow(Value::Bool(false)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::String("value".to_string()).pow(Value::DateTime(NaiveDateTime::MIN)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::String("value".to_string()).pow(Value::Null))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (Value::Int(0).pow(Value::String("value".to_string())))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert_eq!(Value::Int(25), (Value::Int(5).pow(Value::Int(2))).unwrap());
        assert_eq!(
            Value::Float(25_f64),
            (Value::Int(5).pow(Value::Float(2_f64))).unwrap()
        );
        assert!(
            (Value::Int(0).pow(Value::Bool(false)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Int(0).pow(Value::DateTime(NaiveDateTime::MIN)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Int(0).pow(Value::Null))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (Value::Float(0_f64).pow(Value::String("value".to_string())))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert_eq!(
            Value::Float(25_f64),
            (Value::Float(5_f64).pow(Value::Int(2))).unwrap()
        );
        assert_eq!(
            Value::Float(25_f64),
            (Value::Float(5_f64).pow(Value::Float(2_f64))).unwrap()
        );
        assert!(
            (Value::Float(0_f64).pow(Value::Bool(false)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Float(0_f64).pow(Value::DateTime(NaiveDateTime::MIN)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Float(0_f64).pow(Value::Null))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (Value::Bool(false).pow(Value::String("value".to_string())))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Bool(false).pow(Value::Int(0)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Bool(false).pow(Value::Float(0_f64)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Bool(false).pow(Value::Bool(false)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Bool(false).pow(Value::DateTime(NaiveDateTime::MIN)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Bool(false).pow(Value::Null))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (Value::DateTime(NaiveDateTime::MIN).pow(Value::String("value".to_string())))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::DateTime(NaiveDateTime::MIN).pow(Value::Int(0)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::DateTime(NaiveDateTime::MIN).pow(Value::Float(0_f64)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::DateTime(NaiveDateTime::MIN).pow(Value::Bool(false)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::DateTime(NaiveDateTime::MIN).pow(Value::DateTime(NaiveDateTime::MIN)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::DateTime(NaiveDateTime::MIN).pow(Value::Null))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (Value::Null.pow(Value::String("value".to_string())))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Null.pow(Value::Int(0)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Null.pow(Value::Float(0_f64)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Null.pow(Value::Bool(false)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Null.pow(Value::DateTime(NaiveDateTime::MIN)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Null.pow(Value::Null))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
    }

    #[test]
    fn test_mod() {
        assert!(
            (Value::String("value".to_string()).r#mod(Value::String("value".to_string())))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::String("value".to_string()).r#mod(Value::Int(0)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::String("value".to_string()).r#mod(Value::Float(0_f64)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::String("value".to_string()).r#mod(Value::Bool(false)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::String("value".to_string()).r#mod(Value::DateTime(NaiveDateTime::MIN)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::String("value".to_string()).r#mod(Value::Null))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (Value::Int(0).r#mod(Value::String("value".to_string())))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert_eq!(Value::Int(1), (Value::Int(5).r#mod(Value::Int(2))).unwrap());
        assert_eq!(
            Value::Float(1_f64),
            (Value::Int(5).r#mod(Value::Float(2_f64))).unwrap()
        );
        assert!(
            (Value::Int(0).r#mod(Value::Bool(false)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Int(0).r#mod(Value::DateTime(NaiveDateTime::MIN)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Int(0).r#mod(Value::Null))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (Value::Float(0_f64).r#mod(Value::String("value".to_string())))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert_eq!(
            Value::Float(1_f64),
            (Value::Float(5_f64).r#mod(Value::Int(2))).unwrap()
        );
        assert_eq!(
            Value::Float(1_f64),
            (Value::Float(5_f64).r#mod(Value::Float(2_f64))).unwrap()
        );
        assert!(
            (Value::Float(0_f64).r#mod(Value::Bool(false)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Float(0_f64).r#mod(Value::DateTime(NaiveDateTime::MIN)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Float(0_f64).r#mod(Value::Null))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (Value::Bool(false).r#mod(Value::String("value".to_string())))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Bool(false).r#mod(Value::Int(0)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Bool(false).r#mod(Value::Float(0_f64)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Bool(false).r#mod(Value::Bool(false)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Bool(false).r#mod(Value::DateTime(NaiveDateTime::MIN)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Bool(false).r#mod(Value::Null))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (Value::DateTime(NaiveDateTime::MIN).r#mod(Value::String("value".to_string())))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::DateTime(NaiveDateTime::MIN).r#mod(Value::Int(0)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::DateTime(NaiveDateTime::MIN).r#mod(Value::Float(0_f64)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::DateTime(NaiveDateTime::MIN).r#mod(Value::Bool(false)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::DateTime(NaiveDateTime::MIN).r#mod(Value::DateTime(NaiveDateTime::MIN)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::DateTime(NaiveDateTime::MIN).r#mod(Value::Null))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (Value::Null.r#mod(Value::String("value".to_string())))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Null.r#mod(Value::Int(0)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Null.r#mod(Value::Float(0_f64)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Null.r#mod(Value::Bool(false)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Null.r#mod(Value::DateTime(NaiveDateTime::MIN)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (Value::Null.r#mod(Value::Null))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
    }

    #[test]
    fn test_starts_with() {
        assert!(Value::String("value".to_string()).starts_with(&Value::String("val".to_string())));
        assert!(
            !Value::String("value".to_string()).starts_with(&Value::String("not_val".to_string()))
        );
        assert!(Value::String("10".to_string()).starts_with(&Value::Int(1)));
        assert!(!Value::String("10".to_string()).starts_with(&Value::Int(0)));
        assert!(Value::String("10".to_string()).starts_with(&Value::Float(1_f64)));
        assert!(!Value::String("10".to_string()).starts_with(&Value::Float(0_f64)));

        assert!(Value::Int(10).starts_with(&Value::String("1".to_string())));
        assert!(!Value::Int(10).starts_with(&Value::String("0".to_string())));
        assert!(Value::Int(10).starts_with(&Value::Int(1)));
        assert!(!Value::Int(10).starts_with(&Value::Int(0)));
        assert!(Value::Int(10).starts_with(&Value::Float(1_f64)));
        assert!(!Value::Int(10).starts_with(&Value::Float(0_f64)));

        assert!(Value::Float(10_f64).starts_with(&Value::String("1".to_string())));
        assert!(!Value::Float(10_f64).starts_with(&Value::String("0".to_string())));
        assert!(Value::Float(10_f64).starts_with(&Value::Int(1)));
        assert!(!Value::Float(10_f64).starts_with(&Value::Int(0)));
        assert!(Value::Float(10_f64).starts_with(&Value::Float(1_f64)));
        assert!(!Value::Float(10_f64).starts_with(&Value::Float(0_f64)));

        assert!(!Value::String("true".to_string()).starts_with(&Value::Bool(true)));
        assert!(
            !Value::String("-262143-01-01 00:00:00".to_string())
                .starts_with(&Value::DateTime(NaiveDateTime::MIN))
        );
        assert!(!Value::String("Null".to_string()).starts_with(&Value::Null));

        assert!(!Value::Int(1).starts_with(&Value::Bool(true)));
        assert!(!Value::Int(-2).starts_with(&Value::DateTime(NaiveDateTime::MIN)));
        assert!(!Value::Int(0).starts_with(&Value::Null));

        assert!(!Value::Float(1_f64).starts_with(&Value::Bool(true)));
        assert!(!Value::Float(-2_f64).starts_with(&Value::DateTime(NaiveDateTime::MIN)));
        assert!(!Value::Float(0_f64).starts_with(&Value::Null));

        assert!(!Value::Bool(true).starts_with(&Value::String("true".to_string())));
        assert!(!Value::Bool(true).starts_with(&Value::Int(1)));
        assert!(!Value::Bool(true).starts_with(&Value::Float(1_f64)));
        assert!(!Value::Bool(true).starts_with(&Value::Bool(true)));
        assert!(!Value::Bool(true).starts_with(&Value::DateTime(NaiveDateTime::MAX)));
        assert!(!Value::Bool(false).starts_with(&Value::Null));

        assert!(
            !Value::DateTime(NaiveDateTime::MAX)
                .starts_with(&Value::String("-262143-01-01 00:00:00".to_string()))
        );
        assert!(!Value::DateTime(NaiveDateTime::MAX).starts_with(&Value::Int(-2)));
        assert!(!Value::DateTime(NaiveDateTime::MAX).starts_with(&Value::Float(1_f64)));
        assert!(!Value::DateTime(NaiveDateTime::MAX).starts_with(&Value::Bool(false)));
        assert!(
            !Value::DateTime(NaiveDateTime::MAX).starts_with(&Value::DateTime(NaiveDateTime::MAX))
        );
        assert!(!Value::DateTime(NaiveDateTime::MIN).starts_with(&Value::Null));

        assert!(!Value::Null.starts_with(&Value::String("Null".to_string())));
        assert!(!Value::Null.starts_with(&Value::Int(0)));
        assert!(!Value::Null.starts_with(&Value::Float(0_f64)));
        assert!(!Value::Null.starts_with(&Value::Bool(false)));
        assert!(!Value::Null.starts_with(&Value::DateTime(NaiveDateTime::MIN)));
        assert!(!Value::Null.starts_with(&Value::Null));
    }

    #[test]
    fn test_ends_with() {
        assert!(Value::String("value".to_string()).ends_with(&Value::String("ue".to_string())));
        assert!(
            !Value::String("value".to_string()).ends_with(&Value::String("not_ue".to_string()))
        );
        assert!(Value::String("10".to_string()).ends_with(&Value::Int(0)));
        assert!(!Value::String("10".to_string()).ends_with(&Value::Int(1)));
        assert!(Value::String("10".to_string()).ends_with(&Value::Float(0_f64)));
        assert!(!Value::String("10".to_string()).ends_with(&Value::Float(1_f64)));

        assert!(Value::Int(10).ends_with(&Value::String("0".to_string())));
        assert!(!Value::Int(10).ends_with(&Value::String("1".to_string())));
        assert!(Value::Int(10).ends_with(&Value::Int(0)));
        assert!(!Value::Int(10).ends_with(&Value::Int(1)));
        assert!(Value::Int(10).ends_with(&Value::Float(0_f64)));
        assert!(!Value::Int(10).ends_with(&Value::Float(1_f64)));

        assert!(Value::Float(10_f64).ends_with(&Value::String("0".to_string())));
        assert!(!Value::Float(10_f64).ends_with(&Value::String("1".to_string())));
        assert!(Value::Float(10_f64).ends_with(&Value::Int(0)));
        assert!(!Value::Float(10_f64).ends_with(&Value::Int(1)));
        assert!(Value::Float(10_f64).ends_with(&Value::Float(0_f64)));
        assert!(!Value::Float(10_f64).ends_with(&Value::Float(1_f64)));

        assert!(!Value::String("true".to_string()).ends_with(&Value::Bool(true)));
        assert!(
            !Value::String("-262143-01-01 00:00:00".to_string())
                .ends_with(&Value::DateTime(NaiveDateTime::MIN))
        );
        assert!(!Value::String("Null".to_string()).ends_with(&Value::Null));

        assert!(!Value::Int(1).ends_with(&Value::Bool(true)));
        assert!(!Value::Int(0).ends_with(&Value::DateTime(NaiveDateTime::MIN)));
        assert!(!Value::Int(0).ends_with(&Value::Null));

        assert!(!Value::Float(1_f64).ends_with(&Value::Bool(true)));
        assert!(!Value::Float(0_f64).ends_with(&Value::DateTime(NaiveDateTime::MIN)));
        assert!(!Value::Float(0_f64).ends_with(&Value::Null));

        assert!(!Value::Bool(true).ends_with(&Value::String("true".to_string())));
        assert!(!Value::Bool(true).ends_with(&Value::Int(1)));
        assert!(!Value::Bool(true).ends_with(&Value::Float(1_f64)));
        assert!(!Value::Bool(true).ends_with(&Value::Bool(true)));
        assert!(!Value::Bool(true).ends_with(&Value::DateTime(NaiveDateTime::MAX)));
        assert!(!Value::Bool(false).ends_with(&Value::Null));

        assert!(
            !Value::DateTime(NaiveDateTime::MIN)
                .ends_with(&Value::String("-262143-01-01 00:00:00".to_string()))
        );
        assert!(!Value::DateTime(NaiveDateTime::MAX).ends_with(&Value::Int(0)));
        assert!(!Value::DateTime(NaiveDateTime::MAX).ends_with(&Value::Float(0_f64)));
        assert!(!Value::DateTime(NaiveDateTime::MAX).ends_with(&Value::Bool(false)));
        assert!(
            !Value::DateTime(NaiveDateTime::MAX).ends_with(&Value::DateTime(NaiveDateTime::MAX))
        );
        assert!(!Value::DateTime(NaiveDateTime::MAX).ends_with(&Value::Null));

        assert!(!Value::Null.ends_with(&Value::String("true".to_string())));
        assert!(!Value::Null.ends_with(&Value::Int(1)));
        assert!(!Value::Null.ends_with(&Value::Float(1_f64)));
        assert!(!Value::Null.ends_with(&Value::Bool(false)));
        assert!(!Value::Null.ends_with(&Value::DateTime(NaiveDateTime::MIN)));
        assert!(!Value::Null.ends_with(&Value::Null));
    }

    #[test]
    fn test_contains() {
        assert!(Value::String("value".to_string()).contains(&Value::String("al".to_string())));
        assert!(!Value::String("value".to_string()).contains(&Value::String("not_al".to_string())));
        assert!(Value::String("10".to_string()).contains(&Value::Int(0)));
        assert!(!Value::String("10".to_string()).contains(&Value::Int(2)));
        assert!(Value::String("10".to_string()).contains(&Value::Float(0_f64)));
        assert!(!Value::String("10".to_string()).contains(&Value::Float(2_f64)));

        assert!(Value::Int(10).contains(&Value::String("0".to_string())));
        assert!(!Value::Int(10).contains(&Value::String("2".to_string())));
        assert!(Value::Int(10).contains(&Value::Int(0)));
        assert!(!Value::Int(10).contains(&Value::Int(2)));
        assert!(Value::Int(10).contains(&Value::Float(0_f64)));
        assert!(!Value::Int(10).contains(&Value::Float(2_f64)));

        assert!(Value::Float(10_f64).contains(&Value::String("0".to_string())));
        assert!(!Value::Float(10_f64).contains(&Value::String("2".to_string())));
        assert!(Value::Float(10_f64).contains(&Value::Int(0)));
        assert!(!Value::Float(10_f64).contains(&Value::Int(2)));
        assert!(Value::Float(10_f64).contains(&Value::Float(0_f64)));
        assert!(!Value::Float(10_f64).contains(&Value::Float(2_f64)));

        assert!(!Value::String("true".to_string()).contains(&Value::Bool(true)));
        assert!(
            !Value::String("-262143-01-01 00:00:00".to_string())
                .contains(&Value::DateTime(NaiveDateTime::MIN))
        );
        assert!(!Value::String("Null".to_string()).contains(&Value::Null));

        assert!(!Value::Int(1).contains(&Value::Bool(true)));
        assert!(!Value::Int(0).contains(&Value::DateTime(NaiveDateTime::MIN)));
        assert!(!Value::Int(0).contains(&Value::Null));

        assert!(!Value::Float(1_f64).contains(&Value::Bool(true)));
        assert!(!Value::Float(0_f64).contains(&Value::DateTime(NaiveDateTime::MIN)));
        assert!(!Value::Float(0_f64).contains(&Value::Null));

        assert!(!Value::Bool(true).contains(&Value::String("true".to_string())));
        assert!(!Value::Bool(true).contains(&Value::Int(1)));
        assert!(!Value::Bool(true).contains(&Value::Float(1_f64)));
        assert!(!Value::Bool(true).contains(&Value::Bool(true)));
        assert!(!Value::Bool(true).contains(&Value::DateTime(NaiveDateTime::MAX)));
        assert!(!Value::Bool(false).contains(&Value::Null));

        assert!(
            !Value::DateTime(NaiveDateTime::MIN)
                .contains(&Value::String("-262143-01-01 00:00:00".to_string()))
        );
        assert!(!Value::DateTime(NaiveDateTime::MIN).contains(&Value::Int(0)));
        assert!(!Value::DateTime(NaiveDateTime::MIN).contains(&Value::Float(0_f64)));
        assert!(!Value::DateTime(NaiveDateTime::MIN).contains(&Value::Bool(false)));
        assert!(
            !Value::DateTime(NaiveDateTime::MIN).contains(&Value::DateTime(NaiveDateTime::MIN))
        );
        assert!(!Value::DateTime(NaiveDateTime::MIN).contains(&Value::Null));

        assert!(!Value::Null.contains(&Value::String("true".to_string())));
        assert!(!Value::Null.contains(&Value::Int(1)));
        assert!(!Value::Null.contains(&Value::Float(1_f64)));
        assert!(!Value::Null.contains(&Value::Bool(true)));
        assert!(!Value::Null.contains(&Value::DateTime(NaiveDateTime::MIN)));
        assert!(!Value::Null.contains(&Value::Null));
    }

    #[test]
    fn test_slice() {
        assert_eq!(
            Value::String("al".to_string()),
            Value::String("value".to_string()).slice(1..3)
        );

        assert_eq!(
            Value::String("23".to_string()),
            Value::Int(1234).slice(1..3)
        );

        assert_eq!(
            Value::String("23".to_string()),
            Value::Float(1234_f64).slice(1..3)
        );

        assert_eq!(
            Value::String("al".to_string()),
            Value::Bool(false).slice(1..3)
        );

        assert_eq!(
            Value::DateTime(NaiveDateTime::MIN),
            Value::DateTime(NaiveDateTime::MIN).slice(1..3)
        );

        assert_eq!(Value::Null, Value::Null.slice(1..3));
    }

    #[test]
    fn test_round() {
        assert_eq!(
            Value::String("value".to_string()),
            Value::String("value".to_string()).round()
        );

        assert_eq!(Value::Int(1234), Value::Int(1234).round());

        assert_eq!(Value::Float(1234_f64), Value::Float(1234.3).round());

        assert_eq!(Value::Bool(false), Value::Bool(false).round());

        assert_eq!(
            Value::DateTime(NaiveDateTime::MIN),
            Value::DateTime(NaiveDateTime::MIN).round()
        );

        assert_eq!(Value::Null, Value::Null.round());
    }

    #[test]
    fn test_ceil() {
        assert_eq!(
            Value::String("value".to_string()),
            Value::String("value".to_string()).ceil()
        );

        assert_eq!(Value::Int(1234), Value::Int(1234).ceil());

        assert_eq!(Value::Float(1235_f64), Value::Float(1234.3).ceil());

        assert_eq!(Value::Bool(false), Value::Bool(false).ceil());

        assert_eq!(
            Value::DateTime(NaiveDateTime::MIN),
            Value::DateTime(NaiveDateTime::MIN).ceil()
        );

        assert_eq!(Value::Null, Value::Null.ceil());
    }

    #[test]
    fn test_floor() {
        assert_eq!(
            Value::String("value".to_string()),
            Value::String("value".to_string()).floor()
        );

        assert_eq!(Value::Int(1234), Value::Int(1234).floor());

        assert_eq!(Value::Float(1234_f64), Value::Float(1234.3).floor());

        assert_eq!(Value::Bool(false), Value::Bool(false).floor());

        assert_eq!(
            Value::DateTime(NaiveDateTime::MAX),
            Value::DateTime(NaiveDateTime::MAX).floor()
        );

        assert_eq!(Value::Null, Value::Null.floor());
    }

    #[test]
    fn test_abs() {
        assert_eq!(
            Value::String("value".to_string()),
            Value::String("value".to_string()).abs()
        );

        assert_eq!(Value::Int(1234), Value::Int(1234).abs());
        assert_eq!(Value::Int(1234), Value::Int(-1234).abs());

        assert_eq!(Value::Float(1234_f64), Value::Float(1234_f64).abs());
        assert_eq!(Value::Float(1234_f64), Value::Float(-1234_f64).abs());

        assert_eq!(Value::Bool(false), Value::Bool(false).abs());

        assert_eq!(
            Value::DateTime(NaiveDateTime::MIN),
            Value::DateTime(NaiveDateTime::MIN).abs()
        );

        assert_eq!(Value::Null, Value::Null.abs());
    }

    #[test]
    fn test_sqrt() {
        assert_eq!(
            Value::String("value".to_string()),
            Value::String("value".to_string()).sqrt()
        );

        assert_eq!(Value::Float(2_f64), Value::Int(4).sqrt());

        assert_eq!(Value::Float(2_f64), Value::Float(4_f64).sqrt());

        assert_eq!(Value::Bool(false), Value::Bool(false).sqrt());

        assert_eq!(
            Value::DateTime(NaiveDateTime::MIN),
            Value::DateTime(NaiveDateTime::MIN).sqrt()
        );

        assert_eq!(Value::Null, Value::Null.sqrt());
    }

    #[test]
    fn test_trim() {
        assert_eq!(
            Value::String("value".to_string()),
            Value::String("  value  ".to_string()).trim()
        );

        assert_eq!(Value::Int(1234), Value::Int(1234).trim());

        assert_eq!(Value::Float(1234_f64), Value::Float(1234_f64).trim());

        assert_eq!(Value::Bool(false), Value::Bool(false).trim());

        assert_eq!(
            Value::DateTime(NaiveDateTime::MIN),
            Value::DateTime(NaiveDateTime::MIN).trim()
        );

        assert_eq!(Value::Null, Value::Null.trim());
    }

    #[test]
    fn test_trim_start() {
        assert_eq!(
            Value::String("value  ".to_string()),
            Value::String("  value  ".to_string()).trim_start()
        );

        assert_eq!(Value::Int(1234), Value::Int(1234).trim_start());

        assert_eq!(Value::Float(1234_f64), Value::Float(1234_f64).trim_start());

        assert_eq!(Value::Bool(false), Value::Bool(false).trim_start());

        assert_eq!(
            Value::DateTime(NaiveDateTime::MIN),
            Value::DateTime(NaiveDateTime::MIN).trim_start()
        );

        assert_eq!(Value::Null, Value::Null.trim_start());
    }

    #[test]
    fn test_trim_end() {
        assert_eq!(
            Value::String("  value".to_string()),
            Value::String("  value  ".to_string()).trim_end()
        );

        assert_eq!(Value::Int(1234), Value::Int(1234).trim_end());

        assert_eq!(Value::Float(1234_f64), Value::Float(1234_f64).trim_end());

        assert_eq!(Value::Bool(false), Value::Bool(false).trim_end());

        assert_eq!(
            Value::DateTime(NaiveDateTime::MIN),
            Value::DateTime(NaiveDateTime::MIN).trim_end()
        );

        assert_eq!(Value::Null, Value::Null.trim_end());
    }

    #[test]
    fn test_lowercase() {
        assert_eq!(
            Value::String("value".to_string()),
            Value::String("VaLuE".to_string()).lowercase()
        );

        assert_eq!(Value::Int(1234), Value::Int(1234).lowercase());

        assert_eq!(Value::Float(1234_f64), Value::Float(1234_f64).lowercase());

        assert_eq!(Value::Bool(false), Value::Bool(false).lowercase());

        assert_eq!(
            Value::DateTime(NaiveDateTime::MIN),
            Value::DateTime(NaiveDateTime::MIN).lowercase()
        );

        assert_eq!(Value::Null, Value::Null.lowercase());
    }

    #[test]
    fn test_uppercase() {
        assert_eq!(
            Value::String("VALUE".to_string()),
            Value::String("VaLuE".to_string()).uppercase()
        );

        assert_eq!(Value::Int(1234), Value::Int(1234).uppercase());

        assert_eq!(Value::Float(1234_f64), Value::Float(1234_f64).uppercase());

        assert_eq!(Value::Bool(false), Value::Bool(false).uppercase());

        assert_eq!(
            Value::DateTime(NaiveDateTime::MIN),
            Value::DateTime(NaiveDateTime::MIN).uppercase()
        );

        assert_eq!(Value::Null, Value::Null.uppercase());
    }

    #[test]
    fn test_hash() {
        use std::collections::hash_map::DefaultHasher;

        let hash = |value: Value| -> u64 {
            let mut hasher = DefaultHasher::new();
            value.hash(&mut hasher);
            hasher.finish()
        };

        assert_eq!(hash(Value::Int(1)), hash(Value::Float(1.0)));
        assert_eq!(hash(Value::Int(0)), hash(Value::Float(0.0)));
        assert_eq!(hash(Value::Float(-0.0)), hash(Value::Float(0.0)));
        assert_eq!(hash(Value::Float(f64::NAN)), hash(Value::Float(f64::NAN)));
        assert_eq!(hash(Value::Null), hash(Value::Null));

        assert_ne!(hash(Value::Int(1)), hash(Value::String("1".to_string())));
        assert_ne!(hash(Value::Int(0)), hash(Value::Bool(false)));
    }

    #[test]
    fn test_eq_transitivity() {
        let large_int = (1_i64 << 53) + 1;
        let large_float = large_int as f64;
        let rounded_int = large_float as i64;

        assert_ne!(large_int, rounded_int);

        let a = Value::Int(large_int);
        let b = Value::Float(large_float);
        let c = Value::Int(rounded_int);

        assert_ne!(a, b);
        assert_eq!(b, c);
        assert_ne!(a, c);
    }
}
