use crate::{
    AttributeName, Failure, IndexValue, QueryResult, Scalar, ValueDomain,
    error::string::NonStringValue,
};
use graphrecords_core::graphrecord::{GraphRecordAttribute, GraphRecordValue, NodeIndex};

pub trait StringValue: ValueDomain {
    fn into_string(label: &'static str, value: Self::Value<'_>) -> QueryResult<String>;

    fn from_string<'a>(role: &Self::Value<'_>, value: String) -> Self::Value<'a>;
}

fn string_from_value(label: &'static str, value: GraphRecordValue) -> QueryResult<String> {
    match value {
        GraphRecordValue::String(value) => Ok(value),
        value => Err(Failure::new(label, NonStringValue::new(value))),
    }
}

fn string_from_attribute(label: &'static str, value: GraphRecordAttribute) -> QueryResult<String> {
    match value {
        GraphRecordAttribute::String(value) => Ok(value),
        value @ GraphRecordAttribute::Int(_) => {
            Err(Failure::new(label, NonStringValue::new(value)))
        }
    }
}

impl StringValue for Scalar {
    fn into_string(label: &'static str, value: Self::Value<'_>) -> QueryResult<String> {
        string_from_value(label, value)
    }

    fn from_string<'a>(_role: &Self::Value<'_>, value: String) -> Self::Value<'a> {
        GraphRecordValue::String(value)
    }
}

impl StringValue for IndexValue<GraphRecordValue> {
    fn into_string(label: &'static str, value: Self::Value<'_>) -> QueryResult<String> {
        string_from_value(label, value)
    }

    fn from_string<'a>(_role: &Self::Value<'_>, value: String) -> Self::Value<'a> {
        GraphRecordValue::String(value)
    }
}

impl StringValue for AttributeName {
    fn into_string(label: &'static str, value: Self::Value<'_>) -> QueryResult<String> {
        string_from_attribute(label, value)
    }

    fn from_string<'a>(_role: &Self::Value<'_>, value: String) -> Self::Value<'a> {
        GraphRecordAttribute::String(value)
    }
}

impl StringValue for IndexValue<NodeIndex> {
    fn into_string(label: &'static str, value: Self::Value<'_>) -> QueryResult<String> {
        string_from_attribute(label, value)
    }

    fn from_string<'a>(_role: &Self::Value<'_>, value: String) -> Self::Value<'a> {
        GraphRecordAttribute::String(value)
    }
}

impl StringValue for IndexValue<AttributeName> {
    fn into_string(label: &'static str, value: Self::Value<'_>) -> QueryResult<String> {
        string_from_attribute(label, value)
    }

    fn from_string<'a>(_role: &Self::Value<'_>, value: String) -> Self::Value<'a> {
        GraphRecordAttribute::String(value)
    }
}
