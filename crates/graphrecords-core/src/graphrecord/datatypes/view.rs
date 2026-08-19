use super::{
    AttributeName, Group, Identifier, NodeIndex, PluginName, Value,
    value::{collapse_nan_and_negative_zero, int_float_cmp, int_float_eq},
};
use chrono::{NaiveDateTime, TimeDelta};
use std::{
    borrow::Cow,
    cmp::Ordering,
    fmt::Display,
    hash::{Hash, Hasher},
};

#[derive(Debug, Clone)]
pub enum IdentifierView<'a> {
    Int(i64),
    String(Cow<'a, str>),
}

impl<'a> From<&'a Identifier> for IdentifierView<'a> {
    fn from(identifier: &'a Identifier) -> Self {
        match identifier {
            Identifier::String(value) => Self::String(Cow::Borrowed(value)),
            Identifier::Int(value) => Self::Int(*value),
        }
    }
}

impl From<IdentifierView<'_>> for Identifier {
    fn from(view: IdentifierView<'_>) -> Self {
        match view {
            IdentifierView::String(value) => Self::String(value.into_owned()),
            IdentifierView::Int(value) => Self::Int(value),
        }
    }
}

impl PartialEq for IdentifierView<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::String(value), Self::String(other)) => value.as_ref() == other.as_ref(),
            (Self::Int(value), Self::Int(other)) => value == other,
            _ => false,
        }
    }
}

impl Eq for IdentifierView<'_> {}

impl Hash for IdentifierView<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::String(value) => value.as_ref().hash(state),
            Self::Int(value) => value.hash(state),
        }
    }
}

impl PartialOrd for IdentifierView<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::String(value), Self::String(other)) => Some(value.as_ref().cmp(other.as_ref())),
            (Self::Int(value), Self::Int(other)) => Some(value.cmp(other)),
            _ => None,
        }
    }
}

impl Display for IdentifierView<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::String(value) => write!(f, "\"{value}\""),
            Self::Int(value) => write!(f, "{value}"),
        }
    }
}

macro_rules! implement_identifier_view_wrapper {
    ($name:ident, $owned:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
        pub struct $name<'a>(IdentifierView<'a>);

        impl<'a> $name<'a> {
            pub const fn identifier_view(&self) -> &IdentifierView<'a> {
                &self.0
            }
        }

        impl<'a> From<IdentifierView<'a>> for $name<'a> {
            fn from(view: IdentifierView<'a>) -> Self {
                Self(view)
            }
        }

        impl<'a> From<&'a Identifier> for $name<'a> {
            fn from(identifier: &'a Identifier) -> Self {
                Self(IdentifierView::from(identifier))
            }
        }

        impl From<$name<'_>> for $owned {
            fn from(view: $name<'_>) -> Self {
                Self::from(Identifier::from(view.0))
            }
        }

        impl Display for $name<'_> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

implement_identifier_view_wrapper!(NodeIndexView, NodeIndex);
implement_identifier_view_wrapper!(GroupView, Group);
implement_identifier_view_wrapper!(AttributeNameView, AttributeName);
implement_identifier_view_wrapper!(PluginNameView, PluginName);

#[derive(Debug, Clone)]
pub enum ValueView<'a> {
    String(Cow<'a, str>),
    Int(i64),
    Float(f64),
    Bool(bool),
    DateTime(NaiveDateTime),
    Duration(TimeDelta),
    Null,
}

impl<'a> From<&'a Value> for ValueView<'a> {
    fn from(value: &'a Value) -> Self {
        match value {
            Value::String(value) => Self::String(Cow::Borrowed(value)),
            Value::Int(value) => Self::Int(*value),
            Value::Float(value) => Self::Float(*value),
            Value::Bool(value) => Self::Bool(*value),
            Value::DateTime(value) => Self::DateTime(*value),
            Value::Duration(value) => Self::Duration(*value),
            Value::Null => Self::Null,
        }
    }
}

impl From<ValueView<'_>> for Value {
    fn from(view: ValueView<'_>) -> Self {
        match view {
            ValueView::String(value) => Self::String(value.into_owned()),
            ValueView::Int(value) => Self::Int(value),
            ValueView::Float(value) => Self::Float(value),
            ValueView::Bool(value) => Self::Bool(value),
            ValueView::DateTime(value) => Self::DateTime(value),
            ValueView::Duration(value) => Self::Duration(value),
            ValueView::Null => Self::Null,
        }
    }
}

impl PartialEq for ValueView<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::String(value), Self::String(other)) => value.as_ref() == other.as_ref(),
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

impl Eq for ValueView<'_> {}

impl Hash for ValueView<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash_discriminant(state);

        match self {
            Self::Int(value) => {
                collapse_nan_and_negative_zero(*value as f64)
                    .to_bits()
                    .hash(state);
            }
            Self::Float(value) => {
                collapse_nan_and_negative_zero(*value).to_bits().hash(state);
            }
            Self::String(value) => value.as_ref().hash(state),
            Self::Bool(value) => value.hash(state),
            Self::DateTime(value) => value.hash(state),
            Self::Duration(value) => value.hash(state),
            Self::Null => {}
        }
    }
}

impl PartialOrd for ValueView<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::String(value), Self::String(other)) => Some(value.as_ref().cmp(other.as_ref())),
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

impl Display for ValueView<'_> {
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

impl ValueView<'_> {
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

#[cfg(test)]
mod test {
    use super::{IdentifierView, NodeIndexView, ValueView};
    use crate::graphrecord::datatypes::{Identifier, NodeIndex, Value};
    use chrono::{NaiveDateTime, TimeDelta};
    use std::{
        borrow::Cow,
        cmp::Ordering,
        hash::{DefaultHasher, Hash, Hasher},
    };

    fn hash_of<T: Hash>(value: &T) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);

        hasher.finish()
    }

    fn create_identifier_pairs() -> [(Identifier, Identifier); 4] {
        [
            (
                Identifier::String("lorem".to_string()),
                Identifier::String("lorem".to_string()),
            ),
            (
                Identifier::String("lorem".to_string()),
                Identifier::String("ipsum".to_string()),
            ),
            (Identifier::Int(0), Identifier::Int(0)),
            (Identifier::Int(0), Identifier::String("0".to_string())),
        ]
    }

    fn create_value_pairs() -> [(Value, Value); 8] {
        [
            (Value::Int(2), Value::Float(2.0)),
            (Value::Float(f64::NAN), Value::Float(f64::NAN)),
            (Value::Float(-0.0), Value::Float(0.0)),
            (
                Value::String("lorem".to_string()),
                Value::String("lorem".to_string()),
            ),
            (Value::Int(0), Value::String("0".to_string())),
            (Value::Null, Value::Null),
            (
                Value::DateTime(NaiveDateTime::MIN),
                Value::DateTime(NaiveDateTime::MIN),
            ),
            (
                Value::Duration(TimeDelta::seconds(5)),
                Value::Duration(TimeDelta::seconds(5)),
            ),
        ]
    }

    #[test]
    fn test_identifier_view_from() {
        let string_identifier = Identifier::String("lorem".to_string());

        let string_view = IdentifierView::from(&string_identifier);

        assert_eq!(IdentifierView::String(Cow::Borrowed("lorem")), string_view);
        assert!(matches!(
            string_view,
            IdentifierView::String(Cow::Borrowed(_))
        ));
        assert_eq!(
            Identifier::String("lorem".to_string()),
            Identifier::from(string_view)
        );

        let int_view = IdentifierView::from(&Identifier::Int(5));

        assert_eq!(Identifier::Int(5), Identifier::from(int_view));
    }

    #[test]
    fn test_identifier_view_eq() {
        assert_eq!(
            IdentifierView::String(Cow::Borrowed("lorem")),
            IdentifierView::String(Cow::Owned("lorem".to_string()))
        );
        assert_ne!(
            IdentifierView::String(Cow::Borrowed("lorem")),
            IdentifierView::String(Cow::Borrowed("ipsum"))
        );

        assert_eq!(IdentifierView::Int(0), IdentifierView::Int(0));
        assert_ne!(IdentifierView::Int(1), IdentifierView::Int(0));

        assert_ne!(
            IdentifierView::Int(0),
            IdentifierView::String(Cow::Borrowed("0"))
        );
        assert_ne!(
            IdentifierView::String(Cow::Borrowed("0")),
            IdentifierView::Int(0)
        );

        for (left, right) in create_identifier_pairs() {
            assert_eq!(
                left == right,
                IdentifierView::from(&left) == IdentifierView::from(&right)
            );
        }
    }

    #[test]
    fn test_identifier_view_hash() {
        let borrowed = IdentifierView::String(Cow::Borrowed("lorem"));
        let owned = IdentifierView::String(Cow::Owned("lorem".to_string()));

        assert_eq!(IdentifierView::String(Cow::Borrowed("lorem")), owned);
        assert_eq!(hash_of(&borrowed), hash_of(&owned));

        for (left, right) in create_identifier_pairs() {
            let left_view = IdentifierView::from(&left);
            let right_view = IdentifierView::from(&right);

            if left_view == right_view {
                assert_eq!(hash_of(&left_view), hash_of(&right_view));
            }
        }
    }

    #[test]
    fn test_identifier_view_partial_cmp() {
        assert!(
            IdentifierView::String(Cow::Borrowed("b")) > IdentifierView::String(Cow::Borrowed("a"))
        );
        assert!(IdentifierView::Int(1) > IdentifierView::Int(0));

        assert_eq!(
            None,
            IdentifierView::String(Cow::Borrowed("a")).partial_cmp(&IdentifierView::Int(1))
        );
        assert_eq!(
            None,
            IdentifierView::Int(1).partial_cmp(&IdentifierView::String(Cow::Borrowed("a")))
        );

        for (left, right) in create_identifier_pairs() {
            assert_eq!(
                left.partial_cmp(&right),
                IdentifierView::from(&left).partial_cmp(&IdentifierView::from(&right))
            );
        }
    }

    #[test]
    fn test_identifier_view_display() {
        assert_eq!(
            "\"lorem\"",
            IdentifierView::String(Cow::Borrowed("lorem")).to_string()
        );
        assert_eq!("5", IdentifierView::Int(5).to_string());
    }

    #[test]
    fn test_node_index_view_identifier_view() {
        let identifier = Identifier::String("lorem".to_string());

        let view = NodeIndexView::from(&identifier);

        assert_eq!(&IdentifierView::from(&identifier), view.identifier_view());
    }

    #[test]
    fn test_node_index_from() {
        let identifier = Identifier::String("lorem".to_string());
        let view = NodeIndexView::from(&identifier);

        assert_eq!(NodeIndex::from("lorem"), NodeIndex::from(view));
    }

    #[test]
    fn test_value_view_from() {
        let string_value = Value::String("lorem".to_string());

        let string_view = ValueView::from(&string_value);

        assert_eq!(ValueView::String(Cow::Borrowed("lorem")), string_view);
        assert!(matches!(string_view, ValueView::String(Cow::Borrowed(_))));
        assert_eq!(Value::String("lorem".to_string()), Value::from(string_view));

        assert_eq!(Value::Int(5), Value::from(ValueView::from(&Value::Int(5))));
        assert_eq!(
            Value::Float(5.5),
            Value::from(ValueView::from(&Value::Float(5.5)))
        );
        assert_eq!(
            Value::Bool(true),
            Value::from(ValueView::from(&Value::Bool(true)))
        );
        assert_eq!(
            Value::DateTime(NaiveDateTime::MIN),
            Value::from(ValueView::from(&Value::DateTime(NaiveDateTime::MIN)))
        );
        assert_eq!(
            Value::Duration(TimeDelta::seconds(5)),
            Value::from(ValueView::from(&Value::Duration(TimeDelta::seconds(5))))
        );
        assert_eq!(Value::Null, Value::from(ValueView::from(&Value::Null)));
    }

    #[test]
    fn test_value_view_eq() {
        assert_eq!(
            ValueView::String(Cow::Borrowed("lorem")),
            ValueView::String(Cow::Owned("lorem".to_string()))
        );
        assert_ne!(
            ValueView::String(Cow::Borrowed("lorem")),
            ValueView::String(Cow::Borrowed("ipsum"))
        );

        assert_eq!(ValueView::Int(0), ValueView::Int(0));
        assert_eq!(ValueView::Int(0), ValueView::Float(0.0));
        assert_eq!(ValueView::Float(0.0), ValueView::Int(0));
        assert_ne!(ValueView::Int(1), ValueView::Float(0.0));

        assert_eq!(ValueView::Float(f64::NAN), ValueView::Float(f64::NAN));
        assert_eq!(ValueView::Float(-0.0), ValueView::Float(0.0));

        assert_eq!(ValueView::Bool(true), ValueView::Bool(true));
        assert_eq!(
            ValueView::DateTime(NaiveDateTime::MIN),
            ValueView::DateTime(NaiveDateTime::MIN)
        );
        assert_eq!(
            ValueView::Duration(TimeDelta::seconds(5)),
            ValueView::Duration(TimeDelta::seconds(5))
        );
        assert_eq!(ValueView::Null, ValueView::Null);

        assert_ne!(ValueView::Int(0), ValueView::String(Cow::Borrowed("0")));
        assert_ne!(ValueView::Null, ValueView::Int(0));

        for (left, right) in create_value_pairs() {
            assert_eq!(
                left == right,
                ValueView::from(&left) == ValueView::from(&right)
            );
        }
    }

    #[test]
    fn test_value_view_hash() {
        let borrowed = ValueView::String(Cow::Borrowed("lorem"));
        let owned = ValueView::String(Cow::Owned("lorem".to_string()));

        assert_eq!(ValueView::String(Cow::Borrowed("lorem")), owned);
        assert_eq!(hash_of(&borrowed), hash_of(&owned));

        let int_view = ValueView::Int(2);
        let float_view = ValueView::Float(2.0);

        assert_eq!(ValueView::Int(2), float_view);
        assert_eq!(hash_of(&int_view), hash_of(&float_view));

        let negative_zero = ValueView::Float(-0.0);
        let positive_zero = ValueView::Float(0.0);

        assert_eq!(ValueView::Float(-0.0), positive_zero);
        assert_eq!(hash_of(&negative_zero), hash_of(&positive_zero));

        for (left, right) in create_value_pairs() {
            let left_view = ValueView::from(&left);
            let right_view = ValueView::from(&right);

            if left_view == right_view {
                assert_eq!(hash_of(&left_view), hash_of(&right_view));
            }
        }
    }

    #[test]
    fn test_value_view_partial_cmp() {
        assert!(ValueView::String(Cow::Borrowed("b")) > ValueView::String(Cow::Borrowed("a")));
        assert!(ValueView::Int(1) > ValueView::Int(0));
        assert!(ValueView::Int(1) > ValueView::Float(0.0));
        assert!(ValueView::Float(1.0) > ValueView::Int(0));
        assert!(ValueView::Bool(true) > ValueView::Bool(false));
        assert!(ValueView::DateTime(NaiveDateTime::MAX) > ValueView::DateTime(NaiveDateTime::MIN));
        assert!(
            ValueView::Duration(TimeDelta::seconds(5)) > ValueView::Duration(TimeDelta::seconds(0))
        );

        assert_eq!(
            Some(Ordering::Equal),
            ValueView::Null.partial_cmp(&ValueView::Null)
        );
        assert_eq!(
            None,
            ValueView::String(Cow::Borrowed("a")).partial_cmp(&ValueView::Int(1))
        );
        assert_eq!(None, ValueView::Int(1).partial_cmp(&ValueView::Null));
        assert_eq!(None, ValueView::Null.partial_cmp(&ValueView::Int(1)));

        for (left, right) in create_value_pairs() {
            assert_eq!(
                left.partial_cmp(&right),
                ValueView::from(&left).partial_cmp(&ValueView::from(&right))
            );
        }
    }

    #[test]
    fn test_value_view_display() {
        assert_eq!(
            "\"lorem\"",
            ValueView::String(Cow::Borrowed("lorem")).to_string()
        );
        assert_eq!("5", ValueView::Int(5).to_string());
        assert_eq!("5.5", ValueView::Float(5.5).to_string());
        assert_eq!("true", ValueView::Bool(true).to_string());
        assert_eq!(
            "-262143-01-01 00:00:00",
            ValueView::DateTime(NaiveDateTime::MIN).to_string()
        );
        assert_eq!(
            "PT2S",
            ValueView::Duration(TimeDelta::seconds(2)).to_string()
        );
        assert_eq!("Null", ValueView::Null.to_string());
    }
}
