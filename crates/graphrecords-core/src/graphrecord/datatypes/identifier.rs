use super::{
    Abs, Contains, EndsWith, Lowercase, Mod, Pow, Slice, StartsWith, Trim, TrimEnd, TrimStart,
    Uppercase, Value,
};
use crate::errors::{ConversionError, GraphRecordError, GraphRecordResult, ValueOperation};
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
pub enum Identifier {
    Int(i64),
    String(String),
}

impl Hash for Identifier {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Self::String(value) => value.hash(state),
            Self::Int(value) => value.hash(state),
        }
    }
}

impl From<&str> for Identifier {
    fn from(value: &str) -> Self {
        value.to_string().into()
    }
}

implement_from_for_wrapper!(Identifier, String, String);
implement_from_for_wrapper!(Identifier, i64, Int);

impl TryFrom<Value> for Identifier {
    type Error = GraphRecordError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::String(value) => Ok(Self::String(value)),
            Value::Int(value) => Ok(Self::Int(value)),
            _ => Err(GraphRecordError::Conversion(
                ConversionError::ValueToIdentifier { value },
            )),
        }
    }
}

impl PartialEq for Identifier {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::String(value), Self::String(other)) => value == other,
            (Self::Int(value), Self::Int(other)) => value == other,
            _ => false,
        }
    }
}

impl Eq for Identifier {}

impl PartialOrd for Identifier {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::String(value), Self::String(other)) => Some(value.cmp(other)),
            (Self::Int(value), Self::Int(other)) => Some(value.cmp(other)),
            _ => None,
        }
    }
}

impl Display for Identifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::String(value) => write!(f, "\"{value}\""),
            Self::Int(value) => write!(f, "{value}"),
        }
    }
}

// TODO: Add tests
impl Add for Identifier {
    type Output = GraphRecordResult<Self>;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Self::String(value), Self::String(rhs)) => Ok(Self::String(value + rhs.as_str())),
            (Self::Int(value), Self::Int(rhs)) => Ok(Self::Int(value + rhs)),
            (left, right) => Err(GraphRecordError::IncompatibleIdentifierOperands {
                operation: ValueOperation::Add,
                left,
                right,
            }),
        }
    }
}

// TODO: Add tests
impl Sub for Identifier {
    type Output = GraphRecordResult<Self>;

    fn sub(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Self::Int(value), Self::Int(rhs)) => Ok(Self::Int(value - rhs)),
            (left, right) => Err(GraphRecordError::IncompatibleIdentifierOperands {
                operation: ValueOperation::Subtract,
                left,
                right,
            }),
        }
    }
}

// TODO: Add tests
impl Mul for Identifier {
    type Output = GraphRecordResult<Self>;

    fn mul(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Self::Int(value), Self::Int(rhs)) => Ok(Self::Int(value * rhs)),
            (left, right) => Err(GraphRecordError::IncompatibleIdentifierOperands {
                operation: ValueOperation::Multiply,
                left,
                right,
            }),
        }
    }
}

// TODO: Add tests
impl Pow for Identifier {
    fn pow(self, rhs: Self) -> GraphRecordResult<Self> {
        match (self, rhs) {
            (Self::Int(value), Self::Int(rhs)) => Ok(Self::Int(value.pow(rhs as u32))),
            (left, right) => Err(GraphRecordError::IncompatibleIdentifierOperands {
                operation: ValueOperation::Power,
                left,
                right,
            }),
        }
    }
}

// TODO: Add tests
impl Mod for Identifier {
    fn r#mod(self, rhs: Self) -> GraphRecordResult<Self> {
        match (self, rhs) {
            (Self::Int(value), Self::Int(rhs)) => Ok(Self::Int(value % rhs)),
            (left, right) => Err(GraphRecordError::IncompatibleIdentifierOperands {
                operation: ValueOperation::Modulo,
                left,
                right,
            }),
        }
    }
}

// TODO: Add tests
impl Abs for Identifier {
    fn abs(self) -> Self {
        match self {
            Self::Int(value) => Self::Int(value.abs()),
            Self::String(_) => self,
        }
    }
}

impl StartsWith for Identifier {
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

impl EndsWith for Identifier {
    fn ends_with(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::String(value), Self::String(other)) => value.ends_with(other),
            (Self::String(value), Self::Int(other)) => value.ends_with(&other.to_string()),
            (Self::Int(value), Self::String(other)) => value.to_string().ends_with(other),
            (Self::Int(value), Self::Int(other)) => value.to_string().ends_with(&other.to_string()),
        }
    }
}

impl Contains for Identifier {
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
impl Slice for Identifier {
    fn slice(self, range: std::ops::Range<usize>) -> Self {
        match self {
            Self::String(value) => value[range].into(),
            Self::Int(value) => value.to_string()[range].into(),
        }
    }
}

// TODO: Add tests
impl Trim for Identifier {
    fn trim(self) -> Self {
        match self {
            Self::String(value) => Self::String(value.trim().to_string()),
            Self::Int(_) => self,
        }
    }
}

// TODO: Add tests
impl TrimStart for Identifier {
    fn trim_start(self) -> Self {
        match self {
            Self::String(value) => Self::String(value.trim_start().to_string()),
            Self::Int(_) => self,
        }
    }
}

// TODO: Add tests
impl TrimEnd for Identifier {
    fn trim_end(self) -> Self {
        match self {
            Self::String(value) => Self::String(value.trim_end().to_string()),
            Self::Int(_) => self,
        }
    }
}

// TODO: Add tests
impl Lowercase for Identifier {
    fn lowercase(self) -> Self {
        match self {
            Self::String(value) => Self::String(value.to_lowercase()),
            Self::Int(_) => self,
        }
    }
}

// TODO: Add tests
impl Uppercase for Identifier {
    fn uppercase(self) -> Self {
        match self {
            Self::String(value) => Self::String(value.to_uppercase()),
            Self::Int(_) => self,
        }
    }
}

#[cfg(test)]
mod test {
    use super::Identifier;
    use crate::{
        errors::{ConversionError, GraphRecordError},
        graphrecord::{
            Value,
            datatypes::{Contains, EndsWith, StartsWith},
        },
    };

    #[test]
    fn test_from_str() {
        let attribute = Identifier::from("value");

        assert_eq!(Identifier::String("value".to_string()), attribute);
    }

    #[test]
    fn test_from_string() {
        let attribute = Identifier::from("value".to_string());

        assert_eq!(Identifier::String("value".to_string()), attribute);
    }

    #[test]
    fn test_from_int() {
        let attribute = Identifier::from(0);

        assert_eq!(Identifier::Int(0), attribute);
    }

    #[test]
    fn test_try_from_value() {
        let attribute = Identifier::try_from(Value::from("value")).unwrap();

        assert_eq!(Identifier::String("value".to_string()), attribute);

        let attribute = Identifier::try_from(Value::from(0)).unwrap();

        assert_eq!(Identifier::Int(0), attribute);

        assert!(
            Identifier::try_from(Value::from(true)).is_err_and(|e| matches!(
                e,
                GraphRecordError::Conversion(ConversionError::ValueToIdentifier { .. })
            ))
        );

        assert!(
            Identifier::try_from(Value::from(0.0)).is_err_and(|e| matches!(
                e,
                GraphRecordError::Conversion(ConversionError::ValueToIdentifier { .. })
            ))
        );
    }

    #[test]
    fn test_display() {
        assert_eq!(
            "\"value\"",
            Identifier::from("value".to_string()).to_string()
        );

        assert_eq!("0", Identifier::from(0).to_string());
    }

    #[test]
    fn test_partial_eq() {
        assert_eq!(
            Identifier::String("attribute".to_string()),
            Identifier::String("attribute".to_string())
        );
        assert_ne!(
            Identifier::String("attribute2".to_string()),
            Identifier::String("attribute".to_string())
        );

        assert_eq!(Identifier::Int(0), Identifier::Int(0));
        assert_ne!(Identifier::Int(1), Identifier::Int(0));
    }

    #[test]
    #[allow(clippy::neg_cmp_op_on_partial_ord)]
    fn test_partial_ord() {
        assert!(Identifier::String("b".to_string()) > Identifier::String("a".to_string()));
        assert!(Identifier::String("b".to_string()) >= Identifier::String("a".to_string()));
        assert!(Identifier::String("a".to_string()) < Identifier::String("b".to_string()));
        assert!(Identifier::String("a".to_string()) <= Identifier::String("b".to_string()));
        assert!(Identifier::String("a".to_string()) >= Identifier::String("a".to_string()));
        assert!(Identifier::String("a".to_string()) <= Identifier::String("a".to_string()));

        assert!(Identifier::Int(1) > Identifier::Int(0));
        assert!(Identifier::Int(1) >= Identifier::Int(0));
        assert!(Identifier::Int(0) < Identifier::Int(1));
        assert!(Identifier::Int(0) <= Identifier::Int(1));
        assert!(Identifier::Int(0) >= Identifier::Int(0));
        assert!(Identifier::Int(0) <= Identifier::Int(0));

        assert!(!(Identifier::String("a".to_string()) > Identifier::Int(1)));
        assert!(!(Identifier::String("a".to_string()) >= Identifier::Int(1)));
        assert!(!(Identifier::String("a".to_string()) < Identifier::Int(1)));
        assert!(!(Identifier::String("a".to_string()) <= Identifier::Int(1)));
        assert!(!(Identifier::String("a".to_string()) >= Identifier::Int(1)));
        assert!(!(Identifier::String("a".to_string()) <= Identifier::Int(1)));

        assert!(!(Identifier::Int(1) > Identifier::String("a".to_string())));
        assert!(!(Identifier::Int(1) >= Identifier::String("a".to_string())));
        assert!(!(Identifier::Int(1) < Identifier::String("a".to_string())));
        assert!(!(Identifier::Int(1) <= Identifier::String("a".to_string())));
        assert!(!(Identifier::Int(1) >= Identifier::String("a".to_string())));
        assert!(!(Identifier::Int(1) <= Identifier::String("a".to_string())));
    }

    #[test]
    fn test_starts_with() {
        assert!(
            Identifier::String("value".to_string())
                .starts_with(&Identifier::String("val".to_string()))
        );
        assert!(
            !Identifier::String("value".to_string())
                .starts_with(&Identifier::String("not_val".to_string()))
        );
        assert!(Identifier::String("10".to_string()).starts_with(&Identifier::Int(1)));
        assert!(!Identifier::String("10".to_string()).starts_with(&Identifier::Int(0)));

        assert!(Identifier::Int(10).starts_with(&Identifier::String("1".to_string())));
        assert!(!Identifier::Int(10).starts_with(&Identifier::String("0".to_string())));
        assert!(Identifier::Int(10).starts_with(&Identifier::Int(1)));
        assert!(!Identifier::Int(10).starts_with(&Identifier::Int(0)));
    }

    #[test]
    fn test_ends_with() {
        assert!(
            Identifier::String("value".to_string())
                .ends_with(&Identifier::String("ue".to_string()))
        );
        assert!(
            !Identifier::String("value".to_string())
                .ends_with(&Identifier::String("not_ue".to_string()))
        );
        assert!(Identifier::String("10".to_string()).ends_with(&Identifier::Int(0)));
        assert!(!Identifier::String("10".to_string()).ends_with(&Identifier::Int(1)));

        assert!(Identifier::Int(10).ends_with(&Identifier::String("0".to_string())));
        assert!(!Identifier::Int(10).ends_with(&Identifier::String("1".to_string())));
        assert!(Identifier::Int(10).ends_with(&Identifier::Int(0)));
        assert!(!Identifier::Int(10).ends_with(&Identifier::Int(1)));
    }

    #[test]
    fn test_contains() {
        assert!(
            Identifier::String("value".to_string()).contains(&Identifier::String("al".to_string()))
        );
        assert!(
            !Identifier::String("value".to_string())
                .contains(&Identifier::String("not_al".to_string()))
        );
        assert!(Identifier::String("101".to_string()).contains(&Identifier::Int(0)));
        assert!(!Identifier::String("101".to_string()).contains(&Identifier::Int(2)));

        assert!(Identifier::Int(101).contains(&Identifier::String("0".to_string())));
        assert!(!Identifier::Int(101).contains(&Identifier::String("2".to_string())));
        assert!(Identifier::Int(101).contains(&Identifier::Int(0)));
        assert!(!Identifier::Int(101).contains(&Identifier::Int(2)));
    }
}
