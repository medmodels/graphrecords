use super::{
    Abs, Contains, EndsWith, GraphRecordValue, Lowercase, Mod, Pow, Slice, StartsWith, Trim,
    TrimEnd, TrimStart, Uppercase,
};
use crate::errors::{GraphRecordError, GraphRecordResult};
use graphrecords_utils::implement_from_for_wrapper;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    fmt::Display,
    hash::Hash,
    ops::{Add, Mul, Sub},
};

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum GraphRecordAttribute {
    Int(i64),
    String(String),
}

impl Hash for GraphRecordAttribute {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Self::String(value) => value.hash(state),
            Self::Int(value) => value.hash(state),
        }
    }
}

impl From<&str> for GraphRecordAttribute {
    fn from(value: &str) -> Self {
        value.to_string().into()
    }
}

implement_from_for_wrapper!(GraphRecordAttribute, String, String);
implement_from_for_wrapper!(GraphRecordAttribute, i64, Int);

impl TryFrom<GraphRecordValue> for GraphRecordAttribute {
    type Error = GraphRecordError;

    fn try_from(value: GraphRecordValue) -> Result<Self, Self::Error> {
        match value {
            GraphRecordValue::String(value) => Ok(Self::String(value)),
            GraphRecordValue::Int(value) => Ok(Self::Int(value)),
            _ => Err(GraphRecordError::ConversionError(format!(
                "Cannot convert {value} into GraphRecordAttribute"
            ))),
        }
    }
}

impl PartialEq for GraphRecordAttribute {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::String(value), Self::String(other)) => value == other,
            (Self::Int(value), Self::Int(other)) => value == other,
            _ => false,
        }
    }
}

impl Eq for GraphRecordAttribute {}

impl PartialOrd for GraphRecordAttribute {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::String(value), Self::String(other)) => Some(value.cmp(other)),
            (Self::Int(value), Self::Int(other)) => Some(value.cmp(other)),
            _ => None,
        }
    }
}

impl Display for GraphRecordAttribute {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::String(value) => write!(f, "{value}"),
            Self::Int(value) => write!(f, "{value}"),
        }
    }
}

// TODO: Add tests
impl Add for GraphRecordAttribute {
    type Output = GraphRecordResult<Self>;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Self::String(value), Self::String(rhs)) => Ok(Self::String(value + rhs.as_str())),
            (Self::String(value), Self::Int(rhs)) => Err(GraphRecordError::AssertionError(
                format!("Cannot add {rhs} to {value}"),
            )),
            (Self::Int(value), Self::String(rhs)) => Err(GraphRecordError::AssertionError(
                format!("Cannot add {rhs} to {value}"),
            )),
            (Self::Int(value), Self::Int(rhs)) => Ok(Self::Int(value + rhs)),
        }
    }
}

// TODO: Add tests
impl Sub for GraphRecordAttribute {
    type Output = GraphRecordResult<Self>;

    fn sub(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Self::String(value), Self::String(rhs)) => Err(GraphRecordError::AssertionError(
                format!("Cannot subtract {rhs} from {value}"),
            )),
            (Self::String(value), Self::Int(rhs)) => Err(GraphRecordError::AssertionError(
                format!("Cannot subtract {rhs} from {value}"),
            )),
            (Self::Int(value), Self::String(rhs)) => Err(GraphRecordError::AssertionError(
                format!("Cannot subtract {rhs} from {value}"),
            )),
            (Self::Int(value), Self::Int(rhs)) => Ok(Self::Int(value - rhs)),
        }
    }
}

// TODO: Add tests
impl Mul for GraphRecordAttribute {
    type Output = GraphRecordResult<Self>;

    fn mul(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Self::String(value), Self::String(rhs)) => Err(GraphRecordError::AssertionError(
                format!("Cannot multiply {value} by {rhs}"),
            )),
            (Self::String(value), Self::Int(rhs)) => Err(GraphRecordError::AssertionError(
                format!("Cannot multiply {value} by {rhs}"),
            )),
            (Self::Int(value), Self::String(rhs)) => Err(GraphRecordError::AssertionError(
                format!("Cannot multiply {value} by {rhs}"),
            )),
            (Self::Int(value), Self::Int(rhs)) => Ok(Self::Int(value * rhs)),
        }
    }
}

// TODO: Add tests
impl Pow for GraphRecordAttribute {
    fn pow(self, rhs: Self) -> GraphRecordResult<Self> {
        match (self, rhs) {
            (Self::String(value), Self::String(rhs)) => Err(GraphRecordError::AssertionError(
                format!("Cannot raise {value} to the power of {rhs}"),
            )),
            (Self::String(value), Self::Int(rhs)) => Err(GraphRecordError::AssertionError(
                format!("Cannot raise {value} to the power of {rhs}"),
            )),
            (Self::Int(value), Self::String(rhs)) => Err(GraphRecordError::AssertionError(
                format!("Cannot raise {value} to the power of {rhs}"),
            )),
            (Self::Int(value), Self::Int(rhs)) => Ok(Self::Int(value.pow(rhs as u32))),
        }
    }
}

// TODO: Add tests
impl Mod for GraphRecordAttribute {
    fn r#mod(self, rhs: Self) -> GraphRecordResult<Self> {
        match (self, rhs) {
            (Self::String(value), Self::String(rhs)) => Err(GraphRecordError::AssertionError(
                format!("Cannot mod {value} by {rhs}"),
            )),
            (Self::String(value), Self::Int(rhs)) => Err(GraphRecordError::AssertionError(
                format!("Cannot mod {value} by {rhs}"),
            )),
            (Self::Int(value), Self::String(rhs)) => Err(GraphRecordError::AssertionError(
                format!("Cannot mod {value} by {rhs}"),
            )),
            (Self::Int(value), Self::Int(rhs)) => Ok(Self::Int(value % rhs)),
        }
    }
}

// TODO: Add tests
impl Abs for GraphRecordAttribute {
    fn abs(self) -> Self {
        match self {
            Self::Int(value) => Self::Int(value.abs()),
            Self::String(_) => self,
        }
    }
}

impl StartsWith for GraphRecordAttribute {
    fn starts_with(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::String(value), Self::String(other)) => value.starts_with(other),
            (Self::String(value), Self::Int(other)) => value.starts_with(&other.to_string()),
            (Self::Int(value), Self::String(other)) => value.to_string().starts_with(other),
            (Self::Int(value), Self::Int(other)) => {
                value.to_string().starts_with(&other.to_string())
            }
        }
    }
}

impl EndsWith for GraphRecordAttribute {
    fn ends_with(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::String(value), Self::String(other)) => value.ends_with(other),
            (Self::String(value), Self::Int(other)) => value.ends_with(&other.to_string()),
            (Self::Int(value), Self::String(other)) => value.to_string().ends_with(other),
            (Self::Int(value), Self::Int(other)) => value.to_string().ends_with(&other.to_string()),
        }
    }
}

impl Contains for GraphRecordAttribute {
    fn contains(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::String(value), Self::String(other)) => value.contains(other),
            (Self::String(value), Self::Int(other)) => value.contains(&other.to_string()),
            (Self::Int(value), Self::String(other)) => value.to_string().contains(other),
            (Self::Int(value), Self::Int(other)) => value.to_string().contains(&other.to_string()),
        }
    }
}

// TODO: Add tests
impl Slice for GraphRecordAttribute {
    fn slice(self, range: std::ops::Range<usize>) -> Self {
        match self {
            Self::String(value) => value[range].into(),
            Self::Int(value) => value.to_string()[range].into(),
        }
    }
}

// TODO: Add tests
impl Trim for GraphRecordAttribute {
    fn trim(self) -> Self {
        match self {
            Self::String(value) => Self::String(value.trim().to_string()),
            Self::Int(_) => self,
        }
    }
}

// TODO: Add tests
impl TrimStart for GraphRecordAttribute {
    fn trim_start(self) -> Self {
        match self {
            Self::String(value) => Self::String(value.trim_start().to_string()),
            Self::Int(_) => self,
        }
    }
}

// TODO: Add tests
impl TrimEnd for GraphRecordAttribute {
    fn trim_end(self) -> Self {
        match self {
            Self::String(value) => Self::String(value.trim_end().to_string()),
            Self::Int(_) => self,
        }
    }
}

// TODO: Add tests
impl Lowercase for GraphRecordAttribute {
    fn lowercase(self) -> Self {
        match self {
            Self::String(value) => Self::String(value.to_lowercase()),
            Self::Int(_) => self,
        }
    }
}

// TODO: Add tests
impl Uppercase for GraphRecordAttribute {
    fn uppercase(self) -> Self {
        match self {
            Self::String(value) => Self::String(value.to_uppercase()),
            Self::Int(_) => self,
        }
    }
}

#[cfg(test)]
mod test {
    use super::GraphRecordAttribute;
    use crate::{
        errors::GraphRecordError,
        graphrecord::{
            GraphRecordValue,
            datatypes::{Contains, EndsWith, StartsWith},
        },
    };

    #[test]
    fn test_from_str() {
        let attribute = GraphRecordAttribute::from("value");

        assert_eq!(GraphRecordAttribute::String("value".to_string()), attribute);
    }

    #[test]
    fn test_from_string() {
        let attribute = GraphRecordAttribute::from("value".to_string());

        assert_eq!(GraphRecordAttribute::String("value".to_string()), attribute);
    }

    #[test]
    fn test_from_int() {
        let attribute = GraphRecordAttribute::from(0);

        assert_eq!(GraphRecordAttribute::Int(0), attribute);
    }

    #[test]
    fn test_try_from_graphrecord_value() {
        let attribute = GraphRecordAttribute::try_from(GraphRecordValue::from("value")).unwrap();

        assert_eq!(GraphRecordAttribute::String("value".to_string()), attribute);

        let attribute = GraphRecordAttribute::try_from(GraphRecordValue::from(0)).unwrap();

        assert_eq!(GraphRecordAttribute::Int(0), attribute);

        assert!(
            GraphRecordAttribute::try_from(GraphRecordValue::from(true))
                .is_err_and(|e| matches!(e, GraphRecordError::ConversionError(_)))
        );

        assert!(
            GraphRecordAttribute::try_from(GraphRecordValue::from(0.0))
                .is_err_and(|e| matches!(e, GraphRecordError::ConversionError(_)))
        );
    }

    #[test]
    fn test_display() {
        assert_eq!(
            "value",
            GraphRecordAttribute::from("value".to_string()).to_string()
        );

        assert_eq!("0", GraphRecordAttribute::from(0).to_string());
    }

    #[test]
    fn test_partial_eq() {
        assert!(
            GraphRecordAttribute::String("attribute".to_string())
                == GraphRecordAttribute::String("attribute".to_string())
        );
        assert!(
            GraphRecordAttribute::String("attribute2".to_string())
                != GraphRecordAttribute::String("attribute".to_string())
        );

        assert!(GraphRecordAttribute::Int(0) == GraphRecordAttribute::Int(0));
        assert!(GraphRecordAttribute::Int(1) != GraphRecordAttribute::Int(0));
    }

    #[test]
    #[allow(clippy::neg_cmp_op_on_partial_ord)]
    fn test_partial_ord() {
        assert!(
            GraphRecordAttribute::String("b".to_string())
                > GraphRecordAttribute::String("a".to_string())
        );
        assert!(
            GraphRecordAttribute::String("b".to_string())
                >= GraphRecordAttribute::String("a".to_string())
        );
        assert!(
            GraphRecordAttribute::String("a".to_string())
                < GraphRecordAttribute::String("b".to_string())
        );
        assert!(
            GraphRecordAttribute::String("a".to_string())
                <= GraphRecordAttribute::String("b".to_string())
        );
        assert!(
            GraphRecordAttribute::String("a".to_string())
                >= GraphRecordAttribute::String("a".to_string())
        );
        assert!(
            GraphRecordAttribute::String("a".to_string())
                <= GraphRecordAttribute::String("a".to_string())
        );

        assert!(GraphRecordAttribute::Int(1) > GraphRecordAttribute::Int(0));
        assert!(GraphRecordAttribute::Int(1) >= GraphRecordAttribute::Int(0));
        assert!(GraphRecordAttribute::Int(0) < GraphRecordAttribute::Int(1));
        assert!(GraphRecordAttribute::Int(0) <= GraphRecordAttribute::Int(1));
        assert!(GraphRecordAttribute::Int(0) >= GraphRecordAttribute::Int(0));
        assert!(GraphRecordAttribute::Int(0) <= GraphRecordAttribute::Int(0));

        assert!(!(GraphRecordAttribute::String("a".to_string()) > GraphRecordAttribute::Int(1)));
        assert!(!(GraphRecordAttribute::String("a".to_string()) >= GraphRecordAttribute::Int(1)));
        assert!(!(GraphRecordAttribute::String("a".to_string()) < GraphRecordAttribute::Int(1)));
        assert!(!(GraphRecordAttribute::String("a".to_string()) <= GraphRecordAttribute::Int(1)));
        assert!(!(GraphRecordAttribute::String("a".to_string()) >= GraphRecordAttribute::Int(1)));
        assert!(!(GraphRecordAttribute::String("a".to_string()) <= GraphRecordAttribute::Int(1)));

        assert!(!(GraphRecordAttribute::Int(1) > GraphRecordAttribute::String("a".to_string())));
        assert!(!(GraphRecordAttribute::Int(1) >= GraphRecordAttribute::String("a".to_string())));
        assert!(!(GraphRecordAttribute::Int(1) < GraphRecordAttribute::String("a".to_string())));
        assert!(!(GraphRecordAttribute::Int(1) <= GraphRecordAttribute::String("a".to_string())));
        assert!(!(GraphRecordAttribute::Int(1) >= GraphRecordAttribute::String("a".to_string())));
        assert!(!(GraphRecordAttribute::Int(1) <= GraphRecordAttribute::String("a".to_string())));
    }

    #[test]
    fn test_starts_with() {
        assert!(
            GraphRecordAttribute::String("value".to_string())
                .starts_with(&GraphRecordAttribute::String("val".to_string()))
        );
        assert!(
            !GraphRecordAttribute::String("value".to_string())
                .starts_with(&GraphRecordAttribute::String("not_val".to_string()))
        );
        assert!(
            GraphRecordAttribute::String("10".to_string())
                .starts_with(&GraphRecordAttribute::Int(1))
        );
        assert!(
            !GraphRecordAttribute::String("10".to_string())
                .starts_with(&GraphRecordAttribute::Int(0))
        );

        assert!(
            GraphRecordAttribute::Int(10)
                .starts_with(&GraphRecordAttribute::String("1".to_string()))
        );
        assert!(
            !GraphRecordAttribute::Int(10)
                .starts_with(&GraphRecordAttribute::String("0".to_string()))
        );
        assert!(GraphRecordAttribute::Int(10).starts_with(&GraphRecordAttribute::Int(1)));
        assert!(!GraphRecordAttribute::Int(10).starts_with(&GraphRecordAttribute::Int(0)));
    }

    #[test]
    fn test_ends_with() {
        assert!(
            GraphRecordAttribute::String("value".to_string())
                .ends_with(&GraphRecordAttribute::String("ue".to_string()))
        );
        assert!(
            !GraphRecordAttribute::String("value".to_string())
                .ends_with(&GraphRecordAttribute::String("not_ue".to_string()))
        );
        assert!(
            GraphRecordAttribute::String("10".to_string()).ends_with(&GraphRecordAttribute::Int(0))
        );
        assert!(
            !GraphRecordAttribute::String("10".to_string())
                .ends_with(&GraphRecordAttribute::Int(1))
        );

        assert!(
            GraphRecordAttribute::Int(10).ends_with(&GraphRecordAttribute::String("0".to_string()))
        );
        assert!(
            !GraphRecordAttribute::Int(10)
                .ends_with(&GraphRecordAttribute::String("1".to_string()))
        );
        assert!(GraphRecordAttribute::Int(10).ends_with(&GraphRecordAttribute::Int(0)));
        assert!(!GraphRecordAttribute::Int(10).ends_with(&GraphRecordAttribute::Int(1)));
    }

    #[test]
    fn test_contains() {
        assert!(
            GraphRecordAttribute::String("value".to_string())
                .contains(&GraphRecordAttribute::String("al".to_string()))
        );
        assert!(
            !GraphRecordAttribute::String("value".to_string())
                .contains(&GraphRecordAttribute::String("not_al".to_string()))
        );
        assert!(
            GraphRecordAttribute::String("101".to_string()).contains(&GraphRecordAttribute::Int(0))
        );
        assert!(
            !GraphRecordAttribute::String("101".to_string())
                .contains(&GraphRecordAttribute::Int(2))
        );

        assert!(
            GraphRecordAttribute::Int(101).contains(&GraphRecordAttribute::String("0".to_string()))
        );
        assert!(
            !GraphRecordAttribute::Int(101)
                .contains(&GraphRecordAttribute::String("2".to_string()))
        );
        assert!(GraphRecordAttribute::Int(101).contains(&GraphRecordAttribute::Int(0)));
        assert!(!GraphRecordAttribute::Int(101).contains(&GraphRecordAttribute::Int(2)));
    }
}
