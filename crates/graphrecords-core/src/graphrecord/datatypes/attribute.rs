use super::{
    Contains, EndsWith, GraphRecordValue, Lowercase, Slice, StartsWith, Trim, TrimEnd, TrimStart,
    Uppercase,
};
use crate::errors::GraphRecordError;
#[cfg(feature = "serde")]
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{
    cmp::Ordering,
    fmt::Display,
    hash::Hash,
    ops::{Add, Deref},
    sync::Arc,
};

#[derive(Debug, Clone)]
pub struct GraphRecordAttribute(Arc<str>);

impl Hash for GraphRecordAttribute {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl Deref for GraphRecordAttribute {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

impl From<&str> for GraphRecordAttribute {
    fn from(value: &str) -> Self {
        Self(Arc::from(value))
    }
}

impl From<String> for GraphRecordAttribute {
    fn from(value: String) -> Self {
        Self(Arc::from(value.as_str()))
    }
}

impl TryFrom<GraphRecordValue> for GraphRecordAttribute {
    type Error = GraphRecordError;

    fn try_from(value: GraphRecordValue) -> Result<Self, Self::Error> {
        match value {
            GraphRecordValue::String(value) => Ok(Self::from(value)),
            _ => Err(GraphRecordError::ConversionError(format!(
                "Cannot convert {value} into GraphRecordAttribute"
            ))),
        }
    }
}

impl PartialEq for GraphRecordAttribute {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for GraphRecordAttribute {}

impl PartialOrd for GraphRecordAttribute {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for GraphRecordAttribute {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl Display for GraphRecordAttribute {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(feature = "serde")]
impl Serialize for GraphRecordAttribute {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de> Deserialize<'de> for GraphRecordAttribute {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum OldOrNew {
            New(String),
            Old(OldFormat),
        }

        #[derive(Deserialize)]
        enum OldFormat {
            String(String),
        }

        match OldOrNew::deserialize(deserializer)? {
            OldOrNew::New(s) | OldOrNew::Old(OldFormat::String(s)) => Ok(Self::from(s)),
        }
    }
}

impl Add for GraphRecordAttribute {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        let mut result = String::from(&*self.0);
        result.push_str(&rhs.0);
        Self::from(result)
    }
}

impl StartsWith for GraphRecordAttribute {
    fn starts_with(&self, other: &Self) -> bool {
        self.0.starts_with(&*other.0)
    }
}

impl EndsWith for GraphRecordAttribute {
    fn ends_with(&self, other: &Self) -> bool {
        self.0.ends_with(&*other.0)
    }
}

impl Contains for GraphRecordAttribute {
    fn contains(&self, other: &Self) -> bool {
        self.0.contains(&*other.0)
    }
}

impl Slice for GraphRecordAttribute {
    fn slice(self, range: std::ops::Range<usize>) -> Self {
        Self::from(&self.0[range])
    }
}

impl Trim for GraphRecordAttribute {
    fn trim(self) -> Self {
        Self::from(self.0.trim())
    }
}

impl TrimStart for GraphRecordAttribute {
    fn trim_start(self) -> Self {
        Self::from(self.0.trim_start())
    }
}

impl TrimEnd for GraphRecordAttribute {
    fn trim_end(self) -> Self {
        Self::from(self.0.trim_end())
    }
}

impl Lowercase for GraphRecordAttribute {
    fn lowercase(self) -> Self {
        Self::from(self.0.to_lowercase())
    }
}

impl Uppercase for GraphRecordAttribute {
    fn uppercase(self) -> Self {
        Self::from(self.0.to_uppercase())
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

        assert_eq!(GraphRecordAttribute::from("value"), attribute);
    }

    #[test]
    fn test_from_string() {
        let attribute = GraphRecordAttribute::from("value".to_string());

        assert_eq!(GraphRecordAttribute::from("value"), attribute);
    }

    #[test]
    fn test_try_from_graphrecord_value() {
        let attribute = GraphRecordAttribute::try_from(GraphRecordValue::from("value")).unwrap();

        assert_eq!(GraphRecordAttribute::from("value"), attribute);

        assert!(
            GraphRecordAttribute::try_from(GraphRecordValue::from(0))
                .is_err_and(|e| matches!(e, GraphRecordError::ConversionError(_)))
        );

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
    fn test_deref() {
        let attribute = GraphRecordAttribute::from("hello");
        let s: &str = &attribute;
        assert_eq!("hello", s);
    }

    #[test]
    fn test_display() {
        assert_eq!("value", GraphRecordAttribute::from("value").to_string());

        assert_eq!("42", GraphRecordAttribute::from("42").to_string());
    }

    #[test]
    fn test_partial_eq() {
        assert!(GraphRecordAttribute::from("attribute") == GraphRecordAttribute::from("attribute"));
        assert!(
            GraphRecordAttribute::from("attribute2") != GraphRecordAttribute::from("attribute")
        );
    }

    #[test]
    fn test_partial_ord() {
        assert!(GraphRecordAttribute::from("b") > GraphRecordAttribute::from("a"));
        assert!(GraphRecordAttribute::from("b") >= GraphRecordAttribute::from("a"));
        assert!(GraphRecordAttribute::from("a") < GraphRecordAttribute::from("b"));
        assert!(GraphRecordAttribute::from("a") <= GraphRecordAttribute::from("b"));
        assert!(GraphRecordAttribute::from("a") >= GraphRecordAttribute::from("a"));
        assert!(GraphRecordAttribute::from("a") <= GraphRecordAttribute::from("a"));
    }

    #[test]
    fn test_starts_with() {
        assert!(
            GraphRecordAttribute::from("value").starts_with(&GraphRecordAttribute::from("val"))
        );
        assert!(
            !GraphRecordAttribute::from("value")
                .starts_with(&GraphRecordAttribute::from("not_val"))
        );
    }

    #[test]
    fn test_ends_with() {
        assert!(GraphRecordAttribute::from("value").ends_with(&GraphRecordAttribute::from("ue")));
        assert!(
            !GraphRecordAttribute::from("value").ends_with(&GraphRecordAttribute::from("not_ue"))
        );
    }

    #[test]
    fn test_contains() {
        assert!(GraphRecordAttribute::from("value").contains(&GraphRecordAttribute::from("al")));
        assert!(
            !GraphRecordAttribute::from("value").contains(&GraphRecordAttribute::from("not_al"))
        );
    }
}
