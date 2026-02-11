mod attribute;
mod value;

pub use self::{attribute::GraphRecordAttribute, value::GraphRecordValue};
use super::EdgeIndex;
use crate::errors::GraphRecordError;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::{fmt::Display, ops::Range};

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum DataType {
    String,
    Int,
    Float,
    Bool,
    DateTime,
    Duration,
    Null,
    #[default]
    Any,
    Union((Box<Self>, Box<Self>)),
    Option(Box<Self>),
}

// TODO: Add tests for Duration
impl From<GraphRecordValue> for DataType {
    fn from(value: GraphRecordValue) -> Self {
        match value {
            GraphRecordValue::String(_) => Self::String,
            GraphRecordValue::Int(_) => Self::Int,
            GraphRecordValue::Float(_) => Self::Float,
            GraphRecordValue::Bool(_) => Self::Bool,
            GraphRecordValue::DateTime(_) => Self::DateTime,
            GraphRecordValue::Duration(_) => Self::Duration,
            GraphRecordValue::Null => Self::Null,
        }
    }
}

// TODO: Add tests for Duration
impl From<&GraphRecordValue> for DataType {
    fn from(value: &GraphRecordValue) -> Self {
        match value {
            GraphRecordValue::String(_) => Self::String,
            GraphRecordValue::Int(_) => Self::Int,
            GraphRecordValue::Float(_) => Self::Float,
            GraphRecordValue::Bool(_) => Self::Bool,
            GraphRecordValue::DateTime(_) => Self::DateTime,
            GraphRecordValue::Duration(_) => Self::Duration,
            GraphRecordValue::Null => Self::Null,
        }
    }
}

impl From<GraphRecordAttribute> for DataType {
    fn from(value: GraphRecordAttribute) -> Self {
        match value {
            GraphRecordAttribute::String(_) => Self::String,
            GraphRecordAttribute::Int(_) => Self::Int,
        }
    }
}

impl From<&GraphRecordAttribute> for DataType {
    fn from(value: &GraphRecordAttribute) -> Self {
        match value {
            GraphRecordAttribute::String(_) => Self::String,
            GraphRecordAttribute::Int(_) => Self::Int,
        }
    }
}

impl PartialEq for DataType {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Union(first_union), Self::Union(second_union)) => {
                (first_union.0 == second_union.0 && first_union.1 == second_union.1)
                    || (first_union.1 == second_union.0 && first_union.0 == second_union.1)
            }
            (Self::Option(first_datatype), Self::Option(second_datatype)) => {
                first_datatype == second_datatype
            }
            _ => matches!(
                (self, other),
                (Self::String, Self::String)
                    | (Self::Int, Self::Int)
                    | (Self::Float, Self::Float)
                    | (Self::Bool, Self::Bool)
                    | (Self::DateTime, Self::DateTime)
                    | (Self::Null, Self::Null)
                    | (Self::Any, Self::Any)
            ),
        }
    }
}

// TODO: Add tests for Duration
impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::String => write!(f, "String"),
            Self::Int => write!(f, "Int"),
            Self::Float => write!(f, "Float"),
            Self::Bool => write!(f, "Bool"),
            Self::DateTime => write!(f, "DateTime"),
            Self::Duration => write!(f, "Duration"),
            Self::Null => write!(f, "Null"),
            Self::Any => write!(f, "Any"),
            Self::Union((first_datatype, second_datatype)) => {
                write!(f, "Union[")?;
                first_datatype.fmt(f)?;
                write!(f, ", ")?;
                second_datatype.fmt(f)?;
                write!(f, "]")
            }
            Self::Option(data_type) => {
                write!(f, "Option[")?;
                data_type.fmt(f)?;
                write!(f, "]")
            }
        }
    }
}

// TODO: Add tests for Duration
impl DataType {
    pub(crate) fn evaluate(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Union(_), Self::Union(_)) | (Self::Option(_), Self::Option(_)) => self == other,
            (Self::Union((first_datatype, second_datatype)), _) => {
                first_datatype.evaluate(other) || second_datatype.evaluate(other)
            }
            (Self::Option(_), Self::Null) | (Self::Any, _) => true,
            (Self::Option(datatype), _) => datatype.evaluate(other),
            _ => matches!(
                (self, other),
                (Self::String, Self::String)
                    | (Self::Int, Self::Int)
                    | (Self::Float, Self::Float)
                    | (Self::Bool, Self::Bool)
                    | (Self::DateTime, Self::DateTime)
                    | (Self::Duration, Self::Duration)
                    | (Self::Null, Self::Null)
                    | (Self::Any, Self::Any)
            ),
        }
    }
}

pub trait StartsWith {
    fn starts_with(&self, other: &Self) -> bool;
}

// TODO: Add tests
impl StartsWith for EdgeIndex {
    fn starts_with(&self, other: &Self) -> bool {
        self.to_string().starts_with(&other.to_string())
    }
}

pub trait EndsWith {
    fn ends_with(&self, other: &Self) -> bool;
}

// TODO: Add tests
impl EndsWith for EdgeIndex {
    fn ends_with(&self, other: &Self) -> bool {
        self.to_string().ends_with(&other.to_string())
    }
}

pub trait Contains {
    fn contains(&self, other: &Self) -> bool;
}

// TODO: Add tests
impl Contains for EdgeIndex {
    fn contains(&self, other: &Self) -> bool {
        self.to_string().contains(&other.to_string())
    }
}

pub trait Pow: Sized {
    fn pow(self, exp: Self) -> Result<Self, GraphRecordError>;
}

pub trait Mod: Sized {
    fn r#mod(self, other: Self) -> Result<Self, GraphRecordError>;
}

// TODO: Add tests
impl Mod for EdgeIndex {
    fn r#mod(self, other: Self) -> Result<Self, GraphRecordError> {
        Ok(self % other)
    }
}

pub trait Round {
    #[must_use]
    fn round(self) -> Self;
}

pub trait Ceil {
    #[must_use]
    fn ceil(self) -> Self;
}

pub trait Floor {
    #[must_use]
    fn floor(self) -> Self;
}

pub trait Abs {
    #[must_use]
    fn abs(self) -> Self;
}

pub trait Sqrt {
    #[must_use]
    fn sqrt(self) -> Self;
}

pub trait Trim {
    #[must_use]
    fn trim(self) -> Self;
}

pub trait TrimStart {
    #[must_use]
    fn trim_start(self) -> Self;
}

pub trait TrimEnd {
    #[must_use]
    fn trim_end(self) -> Self;
}

pub trait Lowercase {
    #[must_use]
    fn lowercase(self) -> Self;
}

pub trait Uppercase {
    #[must_use]
    fn uppercase(self) -> Self;
}

pub trait Slice {
    #[must_use]
    fn slice(self, range: Range<usize>) -> Self;
}

#[cfg(test)]
mod test {
    use super::{DataType, GraphRecordValue};
    use chrono::NaiveDateTime;

    #[test]
    fn test_default() {
        assert_eq!(DataType::Any, DataType::default());
    }

    #[test]
    fn test_from_graphrecordvalue() {
        assert_eq!(
            DataType::String,
            DataType::from(GraphRecordValue::String(String::new()))
        );
        assert_eq!(DataType::Int, DataType::from(GraphRecordValue::Int(0)));
        assert_eq!(
            DataType::Float,
            DataType::from(GraphRecordValue::Float(0.0))
        );
        assert_eq!(
            DataType::Bool,
            DataType::from(GraphRecordValue::Bool(false))
        );
        assert_eq!(
            DataType::DateTime,
            DataType::from(GraphRecordValue::DateTime(NaiveDateTime::MIN))
        );
        assert_eq!(DataType::Null, DataType::from(GraphRecordValue::Null));
    }

    #[test]
    fn test_from_graphrecordvalue_reference() {
        assert_eq!(
            DataType::String,
            DataType::from(&GraphRecordValue::String(String::new()))
        );
        assert_eq!(DataType::Int, DataType::from(&GraphRecordValue::Int(0)));
        assert_eq!(
            DataType::Float,
            DataType::from(&GraphRecordValue::Float(0.0))
        );
        assert_eq!(
            DataType::Bool,
            DataType::from(&GraphRecordValue::Bool(false))
        );
        assert_eq!(
            DataType::DateTime,
            DataType::from(&GraphRecordValue::DateTime(NaiveDateTime::MIN))
        );
        assert_eq!(DataType::Null, DataType::from(&GraphRecordValue::Null));
    }

    #[test]
    fn test_partial_eq() {
        assert!(DataType::String == DataType::String);
        assert!(DataType::Int == DataType::Int);
        assert!(DataType::Float == DataType::Float);
        assert!(DataType::Bool == DataType::Bool);
        assert!(DataType::DateTime == DataType::DateTime);
        assert!(DataType::Null == DataType::Null);
        assert!(DataType::Any == DataType::Any);

        assert!(
            DataType::Union((Box::new(DataType::String), Box::new(DataType::Int)))
                == DataType::Union((Box::new(DataType::String), Box::new(DataType::Int)))
        );
        assert!(
            DataType::Union((Box::new(DataType::String), Box::new(DataType::Int)))
                == DataType::Union((Box::new(DataType::Int), Box::new(DataType::String)))
        );

        assert!(
            DataType::Option(Box::new(DataType::String))
                == DataType::Option(Box::new(DataType::String))
        );

        assert!(DataType::String != DataType::Int);
        assert!(DataType::String != DataType::Float);
        assert!(DataType::String != DataType::Bool);
        assert!(DataType::String != DataType::DateTime);
        assert!(DataType::String != DataType::Null);
        assert!(DataType::String != DataType::Any);

        assert!(DataType::Int != DataType::String);
        assert!(DataType::Int != DataType::Float);
        assert!(DataType::Int != DataType::Bool);
        assert!(DataType::Int != DataType::DateTime);
        assert!(DataType::Int != DataType::Null);
        assert!(DataType::Int != DataType::Any);

        assert!(DataType::Float != DataType::String);
        assert!(DataType::Float != DataType::Int);
        assert!(DataType::Float != DataType::Bool);
        assert!(DataType::Float != DataType::DateTime);
        assert!(DataType::Float != DataType::Null);
        assert!(DataType::Float != DataType::Any);

        assert!(DataType::Bool != DataType::String);
        assert!(DataType::Bool != DataType::Int);
        assert!(DataType::Bool != DataType::Float);
        assert!(DataType::Bool != DataType::DateTime);
        assert!(DataType::Bool != DataType::Null);
        assert!(DataType::Bool != DataType::Any);

        assert!(DataType::DateTime != DataType::String);
        assert!(DataType::DateTime != DataType::Int);
        assert!(DataType::DateTime != DataType::Float);
        assert!(DataType::DateTime != DataType::Bool);
        assert!(DataType::DateTime != DataType::Null);
        assert!(DataType::DateTime != DataType::Any);

        assert!(DataType::Null != DataType::String);
        assert!(DataType::Null != DataType::Int);
        assert!(DataType::Null != DataType::Float);
        assert!(DataType::Null != DataType::Bool);
        assert!(DataType::Null != DataType::DateTime);
        assert!(DataType::Null != DataType::Any);

        assert!(DataType::Any != DataType::String);
        assert!(DataType::Any != DataType::Int);
        assert!(DataType::Any != DataType::Float);
        assert!(DataType::Any != DataType::Bool);
        assert!(DataType::Any != DataType::DateTime);
        assert!(DataType::Any != DataType::Null);

        // If all the basic datatypes have been tested, it should be safe to assume that the
        // Union and Option variants will work as expected.
        assert!(
            DataType::Union((Box::new(DataType::String), Box::new(DataType::Int)))
                != DataType::Union((Box::new(DataType::Int), Box::new(DataType::Float)))
        );
        assert!(
            DataType::Option(Box::new(DataType::String))
                != DataType::Option(Box::new(DataType::Int))
        );
    }

    #[test]
    fn test_display() {
        assert_eq!("String", format!("{}", DataType::String));
        assert_eq!("Int", format!("{}", DataType::Int));
        assert_eq!("Float", format!("{}", DataType::Float));
        assert_eq!("Bool", format!("{}", DataType::Bool));
        assert_eq!("DateTime", format!("{}", DataType::DateTime));
        assert_eq!("Null", format!("{}", DataType::Null));
        assert_eq!("Any", format!("{}", DataType::Any));
        assert_eq!(
            "Union[String, Int]",
            format!(
                "{}",
                DataType::Union((Box::new(DataType::String), Box::new(DataType::Int)))
            )
        );
        assert_eq!(
            "Option[String]",
            format!("{}", DataType::Option(Box::new(DataType::String)))
        );
    }

    #[test]
    fn test_evaluate() {
        assert!(DataType::String.evaluate(&DataType::String));
        assert!(DataType::Int.evaluate(&DataType::Int));
        assert!(DataType::Float.evaluate(&DataType::Float));
        assert!(DataType::Bool.evaluate(&DataType::Bool));
        assert!(DataType::DateTime.evaluate(&DataType::DateTime));
        assert!(DataType::Null.evaluate(&DataType::Null));
        assert!(DataType::Any.evaluate(&DataType::Any));

        assert!(
            DataType::Union((Box::new(DataType::String), Box::new(DataType::Int))).evaluate(
                &DataType::Union((Box::new(DataType::String), Box::new(DataType::Int)))
            )
        );
        assert!(
            DataType::Union((Box::new(DataType::String), Box::new(DataType::Int))).evaluate(
                &DataType::Union((Box::new(DataType::Int), Box::new(DataType::String)))
            )
        );

        assert!(
            DataType::Union((Box::new(DataType::String), Box::new(DataType::Int)))
                .evaluate(&DataType::String)
        );
        assert!(
            DataType::Union((Box::new(DataType::String), Box::new(DataType::Int)))
                .evaluate(&DataType::Int)
        );

        assert!(
            DataType::Option(Box::new(DataType::String))
                .evaluate(&DataType::Option(Box::new(DataType::String)))
        );
        assert!(DataType::Option(Box::new(DataType::String)).evaluate(&DataType::Null));
        assert!(DataType::Option(Box::new(DataType::String)).evaluate(&DataType::String));

        assert!(DataType::Any.evaluate(&DataType::String));

        assert!(!DataType::String.evaluate(&DataType::Int));
        assert!(!DataType::String.evaluate(&DataType::Float));
        assert!(!DataType::String.evaluate(&DataType::Bool));
        assert!(!DataType::String.evaluate(&DataType::DateTime));
        assert!(!DataType::String.evaluate(&DataType::Null));
        assert!(!DataType::String.evaluate(&DataType::Any));

        assert!(!DataType::Int.evaluate(&DataType::String));
        assert!(!DataType::Int.evaluate(&DataType::Float));
        assert!(!DataType::Int.evaluate(&DataType::Bool));
        assert!(!DataType::Int.evaluate(&DataType::DateTime));
        assert!(!DataType::Int.evaluate(&DataType::Null));
        assert!(!DataType::Int.evaluate(&DataType::Any));

        assert!(!DataType::Float.evaluate(&DataType::String));
        assert!(!DataType::Float.evaluate(&DataType::Int));
        assert!(!DataType::Float.evaluate(&DataType::Bool));
        assert!(!DataType::Float.evaluate(&DataType::DateTime));
        assert!(!DataType::Float.evaluate(&DataType::Null));
        assert!(!DataType::Float.evaluate(&DataType::Any));

        assert!(!DataType::Bool.evaluate(&DataType::String));
        assert!(!DataType::Bool.evaluate(&DataType::Int));
        assert!(!DataType::Bool.evaluate(&DataType::Float));
        assert!(!DataType::Bool.evaluate(&DataType::DateTime));
        assert!(!DataType::Bool.evaluate(&DataType::Null));
        assert!(!DataType::Bool.evaluate(&DataType::Any));

        assert!(!DataType::DateTime.evaluate(&DataType::String));
        assert!(!DataType::DateTime.evaluate(&DataType::Int));
        assert!(!DataType::DateTime.evaluate(&DataType::Float));
        assert!(!DataType::DateTime.evaluate(&DataType::Bool));
        assert!(!DataType::DateTime.evaluate(&DataType::Null));
        assert!(!DataType::DateTime.evaluate(&DataType::Any));

        assert!(!DataType::Null.evaluate(&DataType::String));
        assert!(!DataType::Null.evaluate(&DataType::Int));
        assert!(!DataType::Null.evaluate(&DataType::Float));
        assert!(!DataType::Null.evaluate(&DataType::Bool));
        assert!(!DataType::Null.evaluate(&DataType::DateTime));
        assert!(!DataType::Null.evaluate(&DataType::Any));

        assert!(
            !DataType::Union((Box::new(DataType::String), Box::new(DataType::Int))).evaluate(
                &DataType::Union((Box::new(DataType::Int), Box::new(DataType::Float)))
            )
        );

        assert!(
            !DataType::Option(Box::new(DataType::String))
                .evaluate(&DataType::Option(Box::new(DataType::Int)))
        );
    }
}
