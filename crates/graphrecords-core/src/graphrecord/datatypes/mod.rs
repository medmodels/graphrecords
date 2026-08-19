mod edge_index;
mod identifier;
mod identity;
mod value;
mod view;

pub use self::{
    edge_index::EdgeIndex,
    identifier::Identifier,
    identity::{AttributeName, Group, NodeIndex, PluginName},
    value::Value,
    view::{
        AttributeNameView, GroupView, IdentifierView, NodeIndexView, PluginNameView, ValueView,
    },
};
use crate::errors::GraphRecordResult;
#[cfg(any(feature = "serde", feature = "io"))]
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Display, ops::Range};

pub type AttributeMap = HashMap<AttributeName, Value>;

#[derive(Debug, Clone, Default)]
#[cfg_attr(any(feature = "serde", feature = "io"), derive(Serialize, Deserialize))]
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

impl From<Value> for DataType {
    fn from(value: Value) -> Self {
        match value {
            Value::String(_) => Self::String,
            Value::Int(_) => Self::Int,
            Value::Float(_) => Self::Float,
            Value::Bool(_) => Self::Bool,
            Value::DateTime(_) => Self::DateTime,
            Value::Duration(_) => Self::Duration,
            Value::Null => Self::Null,
        }
    }
}

impl From<&Value> for DataType {
    fn from(value: &Value) -> Self {
        match value {
            Value::String(_) => Self::String,
            Value::Int(_) => Self::Int,
            Value::Float(_) => Self::Float,
            Value::Bool(_) => Self::Bool,
            Value::DateTime(_) => Self::DateTime,
            Value::Duration(_) => Self::Duration,
            Value::Null => Self::Null,
        }
    }
}

impl From<Identifier> for DataType {
    fn from(value: Identifier) -> Self {
        match value {
            Identifier::String(_) => Self::String,
            Identifier::Int(_) => Self::Int,
        }
    }
}

impl From<&Identifier> for DataType {
    fn from(value: &Identifier) -> Self {
        match value {
            Identifier::String(_) => Self::String,
            Identifier::Int(_) => Self::Int,
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
            (Self::Option(first_data_type), Self::Option(second_data_type)) => {
                first_data_type == second_data_type
            }
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

impl Eq for DataType {}

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
            Self::Union((first_data_type, second_data_type)) => {
                write!(f, "Union[")?;
                first_data_type.fmt(f)?;
                write!(f, ", ")?;
                second_data_type.fmt(f)?;
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

impl DataType {
    pub(crate) fn accepts(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Union(_), Self::Union(_)) | (Self::Option(_), Self::Option(_)) => self == other,
            (Self::Union((first_data_type, second_data_type)), _) => {
                first_data_type.accepts(other) || second_data_type.accepts(other)
            }
            (Self::Option(_), Self::Null) | (Self::Any, _) => true,
            (Self::Option(data_type), _) => data_type.accepts(other),
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

pub trait EndsWith {
    fn ends_with(&self, other: &Self) -> bool;
}

pub trait Contains {
    fn contains(&self, other: &Self) -> bool;
}

pub trait Pow: Sized {
    fn pow(self, exponent: Self) -> GraphRecordResult<Self>;
}

pub trait Mod: Sized {
    fn r#mod(self, other: Self) -> GraphRecordResult<Self>;
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
    use super::{DataType, Identifier, Value};
    use chrono::{NaiveDateTime, TimeDelta};

    #[test]
    fn test_default() {
        assert_eq!(DataType::Any, DataType::default());
    }

    #[test]
    fn test_from_value() {
        assert_eq!(
            DataType::String,
            DataType::from(Value::String(String::new()))
        );
        assert_eq!(DataType::Int, DataType::from(Value::Int(0)));
        assert_eq!(DataType::Float, DataType::from(Value::Float(0.0)));
        assert_eq!(DataType::Bool, DataType::from(Value::Bool(false)));
        assert_eq!(
            DataType::DateTime,
            DataType::from(Value::DateTime(NaiveDateTime::MIN))
        );
        assert_eq!(
            DataType::Duration,
            DataType::from(Value::Duration(TimeDelta::seconds(5)))
        );
        assert_eq!(DataType::Null, DataType::from(Value::Null));
    }

    #[test]
    fn test_from_value_reference() {
        assert_eq!(
            DataType::String,
            DataType::from(&Value::String(String::new()))
        );
        assert_eq!(DataType::Int, DataType::from(&Value::Int(0)));
        assert_eq!(DataType::Float, DataType::from(&Value::Float(0.0)));
        assert_eq!(DataType::Bool, DataType::from(&Value::Bool(false)));
        assert_eq!(
            DataType::DateTime,
            DataType::from(&Value::DateTime(NaiveDateTime::MIN))
        );
        assert_eq!(
            DataType::Duration,
            DataType::from(&Value::Duration(TimeDelta::seconds(5)))
        );
        assert_eq!(DataType::Null, DataType::from(&Value::Null));
    }

    #[test]
    fn test_from_identifier() {
        assert_eq!(
            DataType::String,
            DataType::from(Identifier::String(String::new()))
        );
        assert_eq!(DataType::Int, DataType::from(Identifier::Int(0)));
    }

    #[test]
    fn test_from_identifier_reference() {
        assert_eq!(
            DataType::String,
            DataType::from(&Identifier::String(String::new()))
        );
        assert_eq!(DataType::Int, DataType::from(&Identifier::Int(0)));
    }

    #[test]
    fn test_partial_eq() {
        assert_eq!(DataType::String, DataType::String);
        assert_eq!(DataType::Int, DataType::Int);
        assert_eq!(DataType::Float, DataType::Float);
        assert_eq!(DataType::Bool, DataType::Bool);
        assert_eq!(DataType::DateTime, DataType::DateTime);
        assert_eq!(DataType::Duration, DataType::Duration);
        assert_eq!(DataType::Null, DataType::Null);
        assert_eq!(DataType::Any, DataType::Any);

        assert_eq!(
            DataType::Union((Box::new(DataType::String), Box::new(DataType::Int))),
            DataType::Union((Box::new(DataType::String), Box::new(DataType::Int)))
        );
        assert_eq!(
            DataType::Union((Box::new(DataType::String), Box::new(DataType::Int))),
            DataType::Union((Box::new(DataType::Int), Box::new(DataType::String)))
        );

        assert_eq!(
            DataType::Option(Box::new(DataType::String)),
            DataType::Option(Box::new(DataType::String))
        );

        assert_ne!(DataType::String, DataType::Int);
        assert_ne!(DataType::String, DataType::Float);
        assert_ne!(DataType::String, DataType::Bool);
        assert_ne!(DataType::String, DataType::DateTime);
        assert_ne!(DataType::String, DataType::Duration);
        assert_ne!(DataType::String, DataType::Null);
        assert_ne!(DataType::String, DataType::Any);

        assert_ne!(DataType::Int, DataType::String);
        assert_ne!(DataType::Int, DataType::Float);
        assert_ne!(DataType::Int, DataType::Bool);
        assert_ne!(DataType::Int, DataType::DateTime);
        assert_ne!(DataType::Int, DataType::Duration);
        assert_ne!(DataType::Int, DataType::Null);
        assert_ne!(DataType::Int, DataType::Any);

        assert_ne!(DataType::Float, DataType::String);
        assert_ne!(DataType::Float, DataType::Int);
        assert_ne!(DataType::Float, DataType::Bool);
        assert_ne!(DataType::Float, DataType::DateTime);
        assert_ne!(DataType::Float, DataType::Duration);
        assert_ne!(DataType::Float, DataType::Null);
        assert_ne!(DataType::Float, DataType::Any);

        assert_ne!(DataType::Bool, DataType::String);
        assert_ne!(DataType::Bool, DataType::Int);
        assert_ne!(DataType::Bool, DataType::Float);
        assert_ne!(DataType::Bool, DataType::DateTime);
        assert_ne!(DataType::Bool, DataType::Duration);
        assert_ne!(DataType::Bool, DataType::Null);
        assert_ne!(DataType::Bool, DataType::Any);

        assert_ne!(DataType::DateTime, DataType::String);
        assert_ne!(DataType::DateTime, DataType::Int);
        assert_ne!(DataType::DateTime, DataType::Float);
        assert_ne!(DataType::DateTime, DataType::Bool);
        assert_ne!(DataType::DateTime, DataType::Duration);
        assert_ne!(DataType::DateTime, DataType::Null);
        assert_ne!(DataType::DateTime, DataType::Any);

        assert_ne!(DataType::Duration, DataType::String);
        assert_ne!(DataType::Duration, DataType::Int);
        assert_ne!(DataType::Duration, DataType::Float);
        assert_ne!(DataType::Duration, DataType::Bool);
        assert_ne!(DataType::Duration, DataType::DateTime);
        assert_ne!(DataType::Duration, DataType::Null);
        assert_ne!(DataType::Duration, DataType::Any);

        assert_ne!(DataType::Null, DataType::String);
        assert_ne!(DataType::Null, DataType::Int);
        assert_ne!(DataType::Null, DataType::Float);
        assert_ne!(DataType::Null, DataType::Bool);
        assert_ne!(DataType::Null, DataType::DateTime);
        assert_ne!(DataType::Null, DataType::Duration);
        assert_ne!(DataType::Null, DataType::Any);

        assert_ne!(DataType::Any, DataType::String);
        assert_ne!(DataType::Any, DataType::Int);
        assert_ne!(DataType::Any, DataType::Float);
        assert_ne!(DataType::Any, DataType::Bool);
        assert_ne!(DataType::Any, DataType::DateTime);
        assert_ne!(DataType::Any, DataType::Duration);
        assert_ne!(DataType::Any, DataType::Null);

        // If all the basic datatypes have been tested, it should be safe to assume that the
        // Union and Option variants will work as expected.
        assert_ne!(
            DataType::Union((Box::new(DataType::String), Box::new(DataType::Int))),
            DataType::Union((Box::new(DataType::Int), Box::new(DataType::Float)))
        );
        assert_ne!(
            DataType::Option(Box::new(DataType::String)),
            DataType::Option(Box::new(DataType::Int))
        );
    }

    #[test]
    fn test_display() {
        assert_eq!("String", format!("{}", DataType::String));
        assert_eq!("Int", format!("{}", DataType::Int));
        assert_eq!("Float", format!("{}", DataType::Float));
        assert_eq!("Bool", format!("{}", DataType::Bool));
        assert_eq!("DateTime", format!("{}", DataType::DateTime));
        assert_eq!("Duration", format!("{}", DataType::Duration));
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
    fn test_accepts() {
        assert!(DataType::String.accepts(&DataType::String));
        assert!(DataType::Int.accepts(&DataType::Int));
        assert!(DataType::Float.accepts(&DataType::Float));
        assert!(DataType::Bool.accepts(&DataType::Bool));
        assert!(DataType::DateTime.accepts(&DataType::DateTime));
        assert!(DataType::Duration.accepts(&DataType::Duration));
        assert!(DataType::Null.accepts(&DataType::Null));
        assert!(DataType::Any.accepts(&DataType::Any));

        assert!(
            DataType::Union((Box::new(DataType::String), Box::new(DataType::Int))).accepts(
                &DataType::Union((Box::new(DataType::String), Box::new(DataType::Int)))
            )
        );
        assert!(
            DataType::Union((Box::new(DataType::String), Box::new(DataType::Int))).accepts(
                &DataType::Union((Box::new(DataType::Int), Box::new(DataType::String)))
            )
        );

        assert!(
            DataType::Union((Box::new(DataType::String), Box::new(DataType::Int)))
                .accepts(&DataType::String)
        );
        assert!(
            DataType::Union((Box::new(DataType::String), Box::new(DataType::Int)))
                .accepts(&DataType::Int)
        );

        assert!(
            DataType::Option(Box::new(DataType::String))
                .accepts(&DataType::Option(Box::new(DataType::String)))
        );
        assert!(DataType::Option(Box::new(DataType::String)).accepts(&DataType::Null));
        assert!(DataType::Option(Box::new(DataType::String)).accepts(&DataType::String));

        assert!(DataType::Any.accepts(&DataType::String));

        assert!(!DataType::String.accepts(&DataType::Int));
        assert!(!DataType::String.accepts(&DataType::Float));
        assert!(!DataType::String.accepts(&DataType::Bool));
        assert!(!DataType::String.accepts(&DataType::DateTime));
        assert!(!DataType::String.accepts(&DataType::Duration));
        assert!(!DataType::String.accepts(&DataType::Null));
        assert!(!DataType::String.accepts(&DataType::Any));

        assert!(!DataType::Int.accepts(&DataType::String));
        assert!(!DataType::Int.accepts(&DataType::Float));
        assert!(!DataType::Int.accepts(&DataType::Bool));
        assert!(!DataType::Int.accepts(&DataType::DateTime));
        assert!(!DataType::Int.accepts(&DataType::Duration));
        assert!(!DataType::Int.accepts(&DataType::Null));
        assert!(!DataType::Int.accepts(&DataType::Any));

        assert!(!DataType::Float.accepts(&DataType::String));
        assert!(!DataType::Float.accepts(&DataType::Int));
        assert!(!DataType::Float.accepts(&DataType::Bool));
        assert!(!DataType::Float.accepts(&DataType::DateTime));
        assert!(!DataType::Float.accepts(&DataType::Duration));
        assert!(!DataType::Float.accepts(&DataType::Null));
        assert!(!DataType::Float.accepts(&DataType::Any));

        assert!(!DataType::Bool.accepts(&DataType::String));
        assert!(!DataType::Bool.accepts(&DataType::Int));
        assert!(!DataType::Bool.accepts(&DataType::Float));
        assert!(!DataType::Bool.accepts(&DataType::DateTime));
        assert!(!DataType::Bool.accepts(&DataType::Duration));
        assert!(!DataType::Bool.accepts(&DataType::Null));
        assert!(!DataType::Bool.accepts(&DataType::Any));

        assert!(!DataType::DateTime.accepts(&DataType::String));
        assert!(!DataType::DateTime.accepts(&DataType::Int));
        assert!(!DataType::DateTime.accepts(&DataType::Float));
        assert!(!DataType::DateTime.accepts(&DataType::Bool));
        assert!(!DataType::DateTime.accepts(&DataType::Duration));
        assert!(!DataType::DateTime.accepts(&DataType::Null));
        assert!(!DataType::DateTime.accepts(&DataType::Any));

        assert!(!DataType::Duration.accepts(&DataType::String));
        assert!(!DataType::Duration.accepts(&DataType::Int));
        assert!(!DataType::Duration.accepts(&DataType::Float));
        assert!(!DataType::Duration.accepts(&DataType::Bool));
        assert!(!DataType::Duration.accepts(&DataType::DateTime));
        assert!(!DataType::Duration.accepts(&DataType::Null));
        assert!(!DataType::Duration.accepts(&DataType::Any));

        assert!(!DataType::Null.accepts(&DataType::String));
        assert!(!DataType::Null.accepts(&DataType::Int));
        assert!(!DataType::Null.accepts(&DataType::Float));
        assert!(!DataType::Null.accepts(&DataType::Bool));
        assert!(!DataType::Null.accepts(&DataType::DateTime));
        assert!(!DataType::Null.accepts(&DataType::Duration));
        assert!(!DataType::Null.accepts(&DataType::Any));

        assert!(
            !DataType::Union((Box::new(DataType::String), Box::new(DataType::Int))).accepts(
                &DataType::Union((Box::new(DataType::Int), Box::new(DataType::Float)))
            )
        );

        assert!(
            !DataType::Option(Box::new(DataType::String))
                .accepts(&DataType::Option(Box::new(DataType::Int)))
        );
    }
}
