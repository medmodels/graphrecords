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

impl PartialEq for GraphRecordValue {
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

impl Eq for GraphRecordValue {}

impl GraphRecordValue {
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

impl Hash for GraphRecordValue {
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
impl PartialOrd for GraphRecordValue {
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
impl Display for GraphRecordValue {
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
impl Add for GraphRecordValue {
    type Output = GraphRecordResult<Self>;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Self::String(value), Self::String(rhs)) => Ok(Self::String(value + rhs.as_str())),
            (Self::Int(value), Self::Int(rhs)) => Ok(Self::Int(value + rhs)),
            (Self::Int(value), Self::Float(rhs)) => Ok(Self::Float(value as f64 + rhs)),
            (Self::Float(value), Self::Int(rhs)) => Ok(Self::Float(value + rhs as f64)),
            (Self::Float(value), Self::Float(rhs)) => Ok(Self::Float(value + rhs)),
            (Self::Bool(value), Self::Bool(rhs)) => Ok(Self::Bool(value || rhs)),
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
impl Sub for GraphRecordValue {
    type Output = GraphRecordResult<Self>;

    fn sub(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Self::Int(value), Self::Int(rhs)) => Ok(Self::Int(value - rhs)),
            (Self::Int(value), Self::Float(rhs)) => Ok(Self::Float(value as f64 - rhs)),
            (Self::Float(value), Self::Int(rhs)) => Ok(Self::Float(value - rhs as f64)),
            (Self::Float(value), Self::Float(rhs)) => Ok(Self::Float(value - rhs)),
            (Self::Bool(value), Self::Bool(rhs)) => Ok(Self::Bool(value && !rhs)),
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

impl Mul for GraphRecordValue {
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

impl Div for GraphRecordValue {
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

impl Pow for GraphRecordValue {
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

impl Mod for GraphRecordValue {
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

impl Round for GraphRecordValue {
    fn round(self) -> Self {
        match self {
            Self::Float(value) => Self::Float(value.round()),
            _ => self,
        }
    }
}

impl Ceil for GraphRecordValue {
    fn ceil(self) -> Self {
        match self {
            Self::Float(value) => Self::Float(value.ceil()),
            _ => self,
        }
    }
}

impl Floor for GraphRecordValue {
    fn floor(self) -> Self {
        match self {
            Self::Float(value) => Self::Float(value.floor()),
            _ => self,
        }
    }
}

impl Abs for GraphRecordValue {
    fn abs(self) -> Self {
        match self {
            Self::Int(value) => Self::Int(value.abs()),
            Self::Float(value) => Self::Float(value.abs()),
            _ => self,
        }
    }
}

impl Sqrt for GraphRecordValue {
    fn sqrt(self) -> Self {
        match self {
            Self::Int(value) => Self::Float((value as f64).sqrt()),
            Self::Float(value) => Self::Float(value.sqrt()),
            _ => self,
        }
    }
}

impl StartsWith for GraphRecordValue {
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

impl EndsWith for GraphRecordValue {
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

impl Contains for GraphRecordValue {
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

impl Slice for GraphRecordValue {
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

impl Trim for GraphRecordValue {
    fn trim(self) -> Self {
        match self {
            Self::String(value) => Self::String(value.trim().to_string()),
            _ => self,
        }
    }
}

impl TrimStart for GraphRecordValue {
    fn trim_start(self) -> Self {
        match self {
            Self::String(value) => Self::String(value.trim_start().to_string()),
            _ => self,
        }
    }
}

impl TrimEnd for GraphRecordValue {
    fn trim_end(self) -> Self {
        match self {
            Self::String(value) => Self::String(value.trim_end().to_string()),
            _ => self,
        }
    }
}

impl Lowercase for GraphRecordValue {
    fn lowercase(self) -> Self {
        match self {
            Self::String(value) => Self::String(value.to_lowercase()),
            _ => self,
        }
    }
}

impl Uppercase for GraphRecordValue {
    fn uppercase(self) -> Self {
        match self {
            Self::String(value) => Self::String(value.to_uppercase()),
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
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeDelta};
    use std::hash::{Hash, Hasher};

    #[test]
    fn test_default() {
        let value = GraphRecordValue::default();

        assert_eq!(GraphRecordValue::Null, value);
    }

    #[test]
    fn test_from_str() {
        let value = GraphRecordValue::from("value");

        assert_eq!(GraphRecordValue::String("value".to_string()), value);
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
        assert!(GraphRecordValue::Int(1) == GraphRecordValue::Float(1_f64));

        assert!(GraphRecordValue::Float(0_f64) == GraphRecordValue::Float(0_f64));
        assert!(GraphRecordValue::Float(1_f64) != GraphRecordValue::Float(0_f64));

        assert!(GraphRecordValue::Float(0_f64) == GraphRecordValue::Int(0));
        assert!(GraphRecordValue::Float(1_f64) != GraphRecordValue::Int(0));

        assert!(GraphRecordValue::Float(f64::NAN) == GraphRecordValue::Float(f64::NAN));
        assert!(GraphRecordValue::Float(-0.0) == GraphRecordValue::Float(0.0));

        let large_int = (1_i64 << 53) + 1;
        assert!(GraphRecordValue::Int(large_int) != GraphRecordValue::Float(large_int as f64));
        assert!(GraphRecordValue::Float(large_int as f64) != GraphRecordValue::Int(large_int));

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
            "\"value\"",
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
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::String("value".to_string()) + GraphRecordValue::Float(0_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::String("value".to_string()) + GraphRecordValue::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::String("value".to_string())
                + GraphRecordValue::DateTime(NaiveDateTime::MIN))
            .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::String("value".to_string()) + GraphRecordValue::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (GraphRecordValue::Int(0) + GraphRecordValue::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert_eq!(
            GraphRecordValue::Int(10),
            (GraphRecordValue::Int(5) + GraphRecordValue::Int(5)).unwrap()
        );
        assert_eq!(
            GraphRecordValue::Float(10_f64),
            (GraphRecordValue::Int(5) + GraphRecordValue::Float(5_f64)).unwrap()
        );
        assert!(
            (GraphRecordValue::Int(0) + GraphRecordValue::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Int(0) + GraphRecordValue::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Int(0) + GraphRecordValue::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (GraphRecordValue::Float(0_f64) + GraphRecordValue::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
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
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Float(0_f64) + GraphRecordValue::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Float(0_f64) + GraphRecordValue::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (GraphRecordValue::Bool(false) + GraphRecordValue::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Bool(false) + GraphRecordValue::Int(0))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Bool(false) + GraphRecordValue::Float(0_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert_eq!(
            GraphRecordValue::Bool(false),
            (GraphRecordValue::Bool(false) + GraphRecordValue::Bool(false)).unwrap()
        );
        assert_eq!(
            GraphRecordValue::Bool(true),
            (GraphRecordValue::Bool(false) + GraphRecordValue::Bool(true)).unwrap()
        );
        assert_eq!(
            GraphRecordValue::Bool(true),
            (GraphRecordValue::Bool(true) + GraphRecordValue::Bool(false)).unwrap()
        );
        assert_eq!(
            GraphRecordValue::Bool(true),
            (GraphRecordValue::Bool(true) + GraphRecordValue::Bool(true)).unwrap()
        );
        assert!(
            (GraphRecordValue::Bool(false) + GraphRecordValue::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Bool(false) + GraphRecordValue::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN)
                + GraphRecordValue::String("value".to_string()))
            .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN) + GraphRecordValue::Int(0))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN) + GraphRecordValue::Float(0_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN) + GraphRecordValue::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
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
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (GraphRecordValue::Null + GraphRecordValue::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Null + GraphRecordValue::Int(0))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Null + GraphRecordValue::Float(0_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Null + GraphRecordValue::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Null + GraphRecordValue::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Null + GraphRecordValue::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
    }

    #[test]
    fn test_sub() {
        assert!(
            (GraphRecordValue::String("value".to_string())
                - GraphRecordValue::String("value".to_string()))
            .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::String("value".to_string()) - GraphRecordValue::Int(0))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::String("value".to_string()) - GraphRecordValue::Float(0_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::String("value".to_string()) - GraphRecordValue::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::String("value".to_string())
                - GraphRecordValue::DateTime(NaiveDateTime::MIN))
            .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::String("value".to_string()) - GraphRecordValue::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (GraphRecordValue::Int(0) - GraphRecordValue::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert_eq!(
            GraphRecordValue::Int(0),
            (GraphRecordValue::Int(5) - GraphRecordValue::Int(5)).unwrap()
        );
        assert_eq!(
            GraphRecordValue::Float(0_f64),
            (GraphRecordValue::Int(5) - GraphRecordValue::Float(5_f64)).unwrap()
        );
        assert!(
            (GraphRecordValue::Int(0) - GraphRecordValue::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Int(0) - GraphRecordValue::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Int(0) - GraphRecordValue::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (GraphRecordValue::Float(0_f64) - GraphRecordValue::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
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
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Float(0_f64) - GraphRecordValue::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Float(0_f64) - GraphRecordValue::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (GraphRecordValue::Bool(false) - GraphRecordValue::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Bool(false) - GraphRecordValue::Int(0))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Bool(false) - GraphRecordValue::Float(0_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert_eq!(
            GraphRecordValue::Bool(false),
            (GraphRecordValue::Bool(false) - GraphRecordValue::Bool(false)).unwrap()
        );
        assert_eq!(
            GraphRecordValue::Bool(false),
            (GraphRecordValue::Bool(false) - GraphRecordValue::Bool(true)).unwrap()
        );
        assert_eq!(
            GraphRecordValue::Bool(true),
            (GraphRecordValue::Bool(true) - GraphRecordValue::Bool(false)).unwrap()
        );
        assert_eq!(
            GraphRecordValue::Bool(false),
            (GraphRecordValue::Bool(true) - GraphRecordValue::Bool(true)).unwrap()
        );
        assert!(
            (GraphRecordValue::Bool(false) - GraphRecordValue::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Bool(false) - GraphRecordValue::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN)
                - GraphRecordValue::String("value".to_string()))
            .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN) - GraphRecordValue::Int(0))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN) - GraphRecordValue::Float(0_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN) - GraphRecordValue::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN) - GraphRecordValue::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (GraphRecordValue::Duration(TimeDelta::seconds(5))
                - GraphRecordValue::String("value".to_string()))
            .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Duration(TimeDelta::seconds(5)) - GraphRecordValue::Int(0))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Duration(TimeDelta::seconds(5)) - GraphRecordValue::Float(0_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Duration(TimeDelta::seconds(5)) - GraphRecordValue::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Duration(TimeDelta::seconds(5))
                - GraphRecordValue::DateTime(NaiveDateTime::MIN))
            .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert_eq!(
            GraphRecordValue::Duration(TimeDelta::seconds(2)),
            (GraphRecordValue::Duration(TimeDelta::seconds(5))
                - GraphRecordValue::Duration(TimeDelta::seconds(3)))
            .unwrap()
        );
        assert!(
            (GraphRecordValue::Duration(TimeDelta::seconds(5)) - GraphRecordValue::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (GraphRecordValue::Null - GraphRecordValue::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Null - GraphRecordValue::Int(0))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Null - GraphRecordValue::Float(0_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Null - GraphRecordValue::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Null - GraphRecordValue::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Null - GraphRecordValue::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
    }

    #[test]
    fn test_mul() {
        assert!(
            (GraphRecordValue::String("value".to_string())
                * GraphRecordValue::String("value".to_string()))
            .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert_eq!(
            GraphRecordValue::String("valuevaluevalue".to_string()),
            (GraphRecordValue::String("value".to_string()) * GraphRecordValue::Int(3)).unwrap()
        );
        assert!(
            (GraphRecordValue::String("value".to_string()) * GraphRecordValue::Float(0_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::String("value".to_string()) * GraphRecordValue::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::String("value".to_string())
                * GraphRecordValue::DateTime(NaiveDateTime::MIN))
            .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::String("value".to_string()) * GraphRecordValue::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
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
        assert!(
            (GraphRecordValue::Int(0) * GraphRecordValue::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Int(0) * GraphRecordValue::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Int(0) * GraphRecordValue::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (GraphRecordValue::Float(0_f64) * GraphRecordValue::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
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
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Float(0_f64) * GraphRecordValue::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Float(0_f64) * GraphRecordValue::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (GraphRecordValue::Bool(false) * GraphRecordValue::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Bool(false) * GraphRecordValue::Int(0))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Bool(false) * GraphRecordValue::Float(0_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Bool(false) * GraphRecordValue::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Bool(false) * GraphRecordValue::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Bool(false) * GraphRecordValue::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN)
                * GraphRecordValue::String("value".to_string()))
            .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN) * GraphRecordValue::Int(0))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN) * GraphRecordValue::Float(0_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN) * GraphRecordValue::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN)
                * GraphRecordValue::DateTime(NaiveDateTime::MIN))
            .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN) * GraphRecordValue::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (GraphRecordValue::Null * GraphRecordValue::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Null * GraphRecordValue::Int(0))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Null * GraphRecordValue::Float(0_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Null * GraphRecordValue::Bool(false))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Null * GraphRecordValue::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Null * GraphRecordValue::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
    }

    #[test]
    fn test_div() {
        assert!(
            (GraphRecordValue::String("value".to_string())
                / GraphRecordValue::String("value".to_string()))
            .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::String("value".to_string()) / GraphRecordValue::Int(1))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::String("value".to_string()) / GraphRecordValue::Float(1_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::String("value".to_string()) / GraphRecordValue::Bool(true))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::String("value".to_string())
                / GraphRecordValue::DateTime(NaiveDateTime::MIN))
            .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::String("value".to_string()) / GraphRecordValue::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (GraphRecordValue::Int(0) / GraphRecordValue::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert_eq!(
            GraphRecordValue::Float(1_f64),
            (GraphRecordValue::Int(5) / GraphRecordValue::Int(5)).unwrap()
        );
        assert_eq!(
            GraphRecordValue::Float(1_f64),
            (GraphRecordValue::Int(5) / GraphRecordValue::Float(5_f64)).unwrap()
        );
        assert!(
            (GraphRecordValue::Int(0) / GraphRecordValue::Bool(true))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Int(0) / GraphRecordValue::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Int(0) / GraphRecordValue::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (GraphRecordValue::Float(0_f64) / GraphRecordValue::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
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
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Float(0_f64) / GraphRecordValue::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Float(0_f64) / GraphRecordValue::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (GraphRecordValue::Bool(false) / GraphRecordValue::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Bool(false) / GraphRecordValue::Int(1))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Bool(false) / GraphRecordValue::Float(1_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Bool(false) / GraphRecordValue::Bool(true))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Bool(false) / GraphRecordValue::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Bool(false) / GraphRecordValue::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN)
                / GraphRecordValue::String("value".to_string()))
            .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN) / GraphRecordValue::Int(1))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN) / GraphRecordValue::Float(1_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN) / GraphRecordValue::Bool(true))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN)
                / GraphRecordValue::DateTime(NaiveDateTime::MIN))
            .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN) / GraphRecordValue::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (GraphRecordValue::Null / GraphRecordValue::String("value".to_string()))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Null / GraphRecordValue::Int(1))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Null / GraphRecordValue::Float(1_f64))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Null / GraphRecordValue::Bool(true))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Null / GraphRecordValue::DateTime(NaiveDateTime::MIN))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Null / GraphRecordValue::Null)
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
    }

    #[test]
    fn test_pow() {
        assert!(
            (GraphRecordValue::String("value".to_string())
                .pow(GraphRecordValue::String("value".to_string())))
            .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::String("value".to_string()).pow(GraphRecordValue::Int(0)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::String("value".to_string()).pow(GraphRecordValue::Float(0_f64)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::String("value".to_string()).pow(GraphRecordValue::Bool(false)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::String("value".to_string())
                .pow(GraphRecordValue::DateTime(NaiveDateTime::MIN)))
            .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::String("value".to_string()).pow(GraphRecordValue::Null))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (GraphRecordValue::Int(0).pow(GraphRecordValue::String("value".to_string())))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
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
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Int(0).pow(GraphRecordValue::DateTime(NaiveDateTime::MIN)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Int(0).pow(GraphRecordValue::Null))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (GraphRecordValue::Float(0_f64).pow(GraphRecordValue::String("value".to_string())))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
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
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Float(0_f64).pow(GraphRecordValue::DateTime(NaiveDateTime::MIN)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Float(0_f64).pow(GraphRecordValue::Null))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (GraphRecordValue::Bool(false).pow(GraphRecordValue::String("value".to_string())))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Bool(false).pow(GraphRecordValue::Int(0)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Bool(false).pow(GraphRecordValue::Float(0_f64)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Bool(false).pow(GraphRecordValue::Bool(false)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Bool(false).pow(GraphRecordValue::DateTime(NaiveDateTime::MIN)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Bool(false).pow(GraphRecordValue::Null))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN)
                .pow(GraphRecordValue::String("value".to_string())))
            .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN).pow(GraphRecordValue::Int(0)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN).pow(GraphRecordValue::Float(0_f64)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN).pow(GraphRecordValue::Bool(false)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN)
                .pow(GraphRecordValue::DateTime(NaiveDateTime::MIN)))
            .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN).pow(GraphRecordValue::Null))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (GraphRecordValue::Null.pow(GraphRecordValue::String("value".to_string())))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Null.pow(GraphRecordValue::Int(0)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Null.pow(GraphRecordValue::Float(0_f64)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Null.pow(GraphRecordValue::Bool(false)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Null.pow(GraphRecordValue::DateTime(NaiveDateTime::MIN)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Null.pow(GraphRecordValue::Null))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
    }

    #[test]
    fn test_mod() {
        assert!(
            (GraphRecordValue::String("value".to_string())
                .r#mod(GraphRecordValue::String("value".to_string())))
            .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::String("value".to_string()).r#mod(GraphRecordValue::Int(0)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::String("value".to_string()).r#mod(GraphRecordValue::Float(0_f64)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::String("value".to_string()).r#mod(GraphRecordValue::Bool(false)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::String("value".to_string())
                .r#mod(GraphRecordValue::DateTime(NaiveDateTime::MIN)))
            .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::String("value".to_string()).r#mod(GraphRecordValue::Null))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (GraphRecordValue::Int(0).r#mod(GraphRecordValue::String("value".to_string())))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
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
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Int(0).r#mod(GraphRecordValue::DateTime(NaiveDateTime::MIN)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Int(0).r#mod(GraphRecordValue::Null))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (GraphRecordValue::Float(0_f64).r#mod(GraphRecordValue::String("value".to_string())))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
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
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Float(0_f64).r#mod(GraphRecordValue::DateTime(NaiveDateTime::MIN)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Float(0_f64).r#mod(GraphRecordValue::Null))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (GraphRecordValue::Bool(false).r#mod(GraphRecordValue::String("value".to_string())))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Bool(false).r#mod(GraphRecordValue::Int(0)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Bool(false).r#mod(GraphRecordValue::Float(0_f64)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Bool(false).r#mod(GraphRecordValue::Bool(false)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Bool(false).r#mod(GraphRecordValue::DateTime(NaiveDateTime::MIN)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Bool(false).r#mod(GraphRecordValue::Null))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN)
                .r#mod(GraphRecordValue::String("value".to_string())))
            .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN).r#mod(GraphRecordValue::Int(0)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN).r#mod(GraphRecordValue::Float(0_f64)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN).r#mod(GraphRecordValue::Bool(false)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN)
                .r#mod(GraphRecordValue::DateTime(NaiveDateTime::MIN)))
            .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::DateTime(NaiveDateTime::MIN).r#mod(GraphRecordValue::Null))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );

        assert!(
            (GraphRecordValue::Null.r#mod(GraphRecordValue::String("value".to_string())))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Null.r#mod(GraphRecordValue::Int(0)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Null.r#mod(GraphRecordValue::Float(0_f64)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Null.r#mod(GraphRecordValue::Bool(false)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Null.r#mod(GraphRecordValue::DateTime(NaiveDateTime::MIN)))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
        assert!(
            (GraphRecordValue::Null.r#mod(GraphRecordValue::Null))
                .is_err_and(|e| matches!(e, GraphRecordError::IncompatibleValueOperands { .. }))
        );
    }

    #[test]
    fn test_starts_with() {
        assert!(
            GraphRecordValue::String("value".to_string())
                .starts_with(&GraphRecordValue::String("val".to_string()))
        );
        assert!(
            !GraphRecordValue::String("value".to_string())
                .starts_with(&GraphRecordValue::String("not_val".to_string()))
        );
        assert!(GraphRecordValue::String("10".to_string()).starts_with(&GraphRecordValue::Int(1)));
        assert!(!GraphRecordValue::String("10".to_string()).starts_with(&GraphRecordValue::Int(0)));
        assert!(
            GraphRecordValue::String("10".to_string()).starts_with(&GraphRecordValue::Float(1_f64))
        );
        assert!(
            !GraphRecordValue::String("10".to_string())
                .starts_with(&GraphRecordValue::Float(0_f64))
        );

        assert!(GraphRecordValue::Int(10).starts_with(&GraphRecordValue::String("1".to_string())));
        assert!(!GraphRecordValue::Int(10).starts_with(&GraphRecordValue::String("0".to_string())));
        assert!(GraphRecordValue::Int(10).starts_with(&GraphRecordValue::Int(1)));
        assert!(!GraphRecordValue::Int(10).starts_with(&GraphRecordValue::Int(0)));
        assert!(GraphRecordValue::Int(10).starts_with(&GraphRecordValue::Float(1_f64)));
        assert!(!GraphRecordValue::Int(10).starts_with(&GraphRecordValue::Float(0_f64)));

        assert!(
            GraphRecordValue::Float(10_f64).starts_with(&GraphRecordValue::String("1".to_string()))
        );
        assert!(
            !GraphRecordValue::Float(10_f64)
                .starts_with(&GraphRecordValue::String("0".to_string()))
        );
        assert!(GraphRecordValue::Float(10_f64).starts_with(&GraphRecordValue::Int(1)));
        assert!(!GraphRecordValue::Float(10_f64).starts_with(&GraphRecordValue::Int(0)));
        assert!(GraphRecordValue::Float(10_f64).starts_with(&GraphRecordValue::Float(1_f64)));
        assert!(!GraphRecordValue::Float(10_f64).starts_with(&GraphRecordValue::Float(0_f64)));

        assert!(
            !GraphRecordValue::String("true".to_string())
                .starts_with(&GraphRecordValue::Bool(true))
        );
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
        assert!(
            !GraphRecordValue::Float(-2_f64)
                .starts_with(&GraphRecordValue::DateTime(NaiveDateTime::MIN))
        );
        assert!(!GraphRecordValue::Float(0_f64).starts_with(&GraphRecordValue::Null));

        assert!(
            !GraphRecordValue::Bool(true)
                .starts_with(&GraphRecordValue::String("true".to_string()))
        );
        assert!(!GraphRecordValue::Bool(true).starts_with(&GraphRecordValue::Int(1)));
        assert!(!GraphRecordValue::Bool(true).starts_with(&GraphRecordValue::Float(1_f64)));
        assert!(!GraphRecordValue::Bool(true).starts_with(&GraphRecordValue::Bool(true)));
        assert!(
            !GraphRecordValue::Bool(true)
                .starts_with(&GraphRecordValue::DateTime(NaiveDateTime::MAX))
        );
        assert!(!GraphRecordValue::Bool(false).starts_with(&GraphRecordValue::Null));

        assert!(!GraphRecordValue::DateTime(NaiveDateTime::MAX).starts_with(
            &GraphRecordValue::String("-262143-01-01 00:00:00".to_string())
        ));
        assert!(
            !GraphRecordValue::DateTime(NaiveDateTime::MAX).starts_with(&GraphRecordValue::Int(-2))
        );
        assert!(
            !GraphRecordValue::DateTime(NaiveDateTime::MAX)
                .starts_with(&GraphRecordValue::Float(1_f64))
        );
        assert!(
            !GraphRecordValue::DateTime(NaiveDateTime::MAX)
                .starts_with(&GraphRecordValue::Bool(false))
        );
        assert!(
            !GraphRecordValue::DateTime(NaiveDateTime::MAX)
                .starts_with(&GraphRecordValue::DateTime(NaiveDateTime::MAX))
        );
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
        assert!(
            GraphRecordValue::String("value".to_string())
                .ends_with(&GraphRecordValue::String("ue".to_string()))
        );
        assert!(
            !GraphRecordValue::String("value".to_string())
                .ends_with(&GraphRecordValue::String("not_ue".to_string()))
        );
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
        assert!(
            !GraphRecordValue::Float(0_f64)
                .ends_with(&GraphRecordValue::DateTime(NaiveDateTime::MIN))
        );
        assert!(!GraphRecordValue::Float(0_f64).ends_with(&GraphRecordValue::Null));

        assert!(
            !GraphRecordValue::Bool(true).ends_with(&GraphRecordValue::String("true".to_string()))
        );
        assert!(!GraphRecordValue::Bool(true).ends_with(&GraphRecordValue::Int(1)));
        assert!(!GraphRecordValue::Bool(true).ends_with(&GraphRecordValue::Float(1_f64)));
        assert!(!GraphRecordValue::Bool(true).ends_with(&GraphRecordValue::Bool(true)));
        assert!(
            !GraphRecordValue::Bool(true)
                .ends_with(&GraphRecordValue::DateTime(NaiveDateTime::MAX))
        );
        assert!(!GraphRecordValue::Bool(false).ends_with(&GraphRecordValue::Null));

        assert!(!GraphRecordValue::DateTime(NaiveDateTime::MIN).ends_with(
            &GraphRecordValue::String("-262143-01-01 00:00:00".to_string())
        ));
        assert!(
            !GraphRecordValue::DateTime(NaiveDateTime::MAX).ends_with(&GraphRecordValue::Int(0))
        );
        assert!(
            !GraphRecordValue::DateTime(NaiveDateTime::MAX)
                .ends_with(&GraphRecordValue::Float(0_f64))
        );
        assert!(
            !GraphRecordValue::DateTime(NaiveDateTime::MAX)
                .ends_with(&GraphRecordValue::Bool(false))
        );
        assert!(
            !GraphRecordValue::DateTime(NaiveDateTime::MAX)
                .ends_with(&GraphRecordValue::DateTime(NaiveDateTime::MAX))
        );
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
        assert!(
            GraphRecordValue::String("value".to_string())
                .contains(&GraphRecordValue::String("al".to_string()))
        );
        assert!(
            !GraphRecordValue::String("value".to_string())
                .contains(&GraphRecordValue::String("not_al".to_string()))
        );
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
        assert!(
            !GraphRecordValue::Int(0).contains(&GraphRecordValue::DateTime(NaiveDateTime::MIN))
        );
        assert!(!GraphRecordValue::Int(0).contains(&GraphRecordValue::Null));

        assert!(!GraphRecordValue::Float(1_f64).contains(&GraphRecordValue::Bool(true)));
        assert!(
            !GraphRecordValue::Float(0_f64)
                .contains(&GraphRecordValue::DateTime(NaiveDateTime::MIN))
        );
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
        assert!(
            !GraphRecordValue::DateTime(NaiveDateTime::MIN).contains(&GraphRecordValue::Int(0))
        );
        assert!(
            !GraphRecordValue::DateTime(NaiveDateTime::MIN)
                .contains(&GraphRecordValue::Float(0_f64))
        );
        assert!(
            !GraphRecordValue::DateTime(NaiveDateTime::MIN)
                .contains(&GraphRecordValue::Bool(false))
        );
        assert!(
            !GraphRecordValue::DateTime(NaiveDateTime::MIN)
                .contains(&GraphRecordValue::DateTime(NaiveDateTime::MIN))
        );
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

    #[test]
    fn test_hash() {
        use std::collections::hash_map::DefaultHasher;

        let hash = |value: GraphRecordValue| -> u64 {
            let mut hasher = DefaultHasher::new();
            value.hash(&mut hasher);
            hasher.finish()
        };

        assert_eq!(
            hash(GraphRecordValue::Int(1)),
            hash(GraphRecordValue::Float(1.0))
        );
        assert_eq!(
            hash(GraphRecordValue::Int(0)),
            hash(GraphRecordValue::Float(0.0))
        );
        assert_eq!(
            hash(GraphRecordValue::Float(-0.0)),
            hash(GraphRecordValue::Float(0.0))
        );
        assert_eq!(
            hash(GraphRecordValue::Float(f64::NAN)),
            hash(GraphRecordValue::Float(f64::NAN))
        );
        assert_eq!(hash(GraphRecordValue::Null), hash(GraphRecordValue::Null));

        assert_ne!(
            hash(GraphRecordValue::Int(1)),
            hash(GraphRecordValue::String("1".to_string()))
        );
        assert_ne!(
            hash(GraphRecordValue::Int(0)),
            hash(GraphRecordValue::Bool(false))
        );
    }

    #[test]
    fn test_eq_transitivity() {
        let large_int = (1_i64 << 53) + 1;
        let large_float = large_int as f64;
        let rounded_int = large_float as i64;

        assert_ne!(large_int, rounded_int);

        let a = GraphRecordValue::Int(large_int);
        let b = GraphRecordValue::Float(large_float);
        let c = GraphRecordValue::Int(rounded_int);

        assert_ne!(a, b);
        assert_eq!(b, c);
        assert_ne!(a, c);
    }
}
