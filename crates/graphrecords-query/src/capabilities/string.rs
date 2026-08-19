use crate::{Failure, IndexValue, QueryResult, Scalar, ValueDomain, error::string::NonStringValue};
use graphrecords_core::graphrecord::{AttributeName, Identifier, NodeIndex, Value};

pub trait StringValue: ValueDomain {
    fn into_string(label: &'static str, value: Self::Value<'_>) -> QueryResult<String>;

    fn from_string<'a>(role: &Self::Value<'_>, value: String) -> Self::Value<'a>;
}

fn string_from_value(label: &'static str, value: Value) -> QueryResult<String> {
    match value {
        Value::String(value) => Ok(value),
        value => Err(Failure::new(label, NonStringValue::new(value))),
    }
}

impl StringValue for Scalar {
    fn into_string(label: &'static str, value: Self::Value<'_>) -> QueryResult<String> {
        string_from_value(label, value)
    }

    fn from_string<'a>(_role: &Self::Value<'_>, value: String) -> Self::Value<'a> {
        Value::String(value)
    }
}

impl StringValue for IndexValue<Value> {
    fn into_string(label: &'static str, value: Self::Value<'_>) -> QueryResult<String> {
        string_from_value(label, value)
    }

    fn from_string<'a>(_role: &Self::Value<'_>, value: String) -> Self::Value<'a> {
        Value::String(value)
    }
}

impl StringValue for AttributeName {
    fn into_string(label: &'static str, value: Self::Value<'_>) -> QueryResult<String> {
        match Identifier::from(value) {
            Identifier::String(string) => Ok(string),
            value @ Identifier::Int(_) => {
                Err(Failure::new(label, NonStringValue::new(Self::from(value))))
            }
        }
    }

    fn from_string<'a>(_role: &Self::Value<'_>, value: String) -> Self::Value<'a> {
        Self::from(value)
    }
}

impl StringValue for IndexValue<NodeIndex> {
    fn into_string(label: &'static str, value: Self::Value<'_>) -> QueryResult<String> {
        match Identifier::from(value) {
            Identifier::String(string) => Ok(string),
            value @ Identifier::Int(_) => Err(Failure::new(
                label,
                NonStringValue::new(NodeIndex::from(value)),
            )),
        }
    }

    fn from_string<'a>(_role: &Self::Value<'_>, value: String) -> Self::Value<'a> {
        NodeIndex::from(value)
    }
}

impl StringValue for IndexValue<AttributeName> {
    fn into_string(label: &'static str, value: Self::Value<'_>) -> QueryResult<String> {
        match Identifier::from(value) {
            Identifier::String(string) => Ok(string),
            value @ Identifier::Int(_) => Err(Failure::new(
                label,
                NonStringValue::new(AttributeName::from(value)),
            )),
        }
    }

    fn from_string<'a>(_role: &Self::Value<'_>, value: String) -> Self::Value<'a> {
        AttributeName::from(value)
    }
}
