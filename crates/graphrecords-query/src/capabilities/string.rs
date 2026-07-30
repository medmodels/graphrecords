use crate::{
    AttributeName, Failure, IndexValue, QueryResult, Scalar, ValueType,
    error::string::NonStringValue,
};
use graphrecords_core::graphrecord::{GraphRecordAttribute, GraphRecordValue, NodeIndex};

pub trait StringValue: ValueType {
    fn into_string<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<String>
    where
        Self: 'a;

    fn from_string<'a>(value: String) -> Self::Value<'a>
    where
        Self: 'a;
}

impl StringValue for Scalar {
    fn into_string<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<String>
    where
        Self: 'a,
    {
        match value {
            GraphRecordValue::String(value) => Ok(value),
            value => Err(Failure::new(label, NonStringValue::new(value))),
        }
    }

    fn from_string<'a>(value: String) -> Self::Value<'a>
    where
        Self: 'a,
    {
        GraphRecordValue::String(value)
    }
}

impl StringValue for AttributeName {
    fn into_string<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<String>
    where
        Self: 'a,
    {
        match value {
            GraphRecordAttribute::String(value) => Ok(value),
            value @ GraphRecordAttribute::Int(_) => {
                Err(Failure::new(label, NonStringValue::new(value)))
            }
        }
    }

    fn from_string<'a>(value: String) -> Self::Value<'a>
    where
        Self: 'a,
    {
        GraphRecordAttribute::String(value)
    }
}

impl StringValue for IndexValue<NodeIndex> {
    fn into_string<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<String>
    where
        Self: 'a,
    {
        match value {
            GraphRecordAttribute::String(value) => Ok(value),
            value @ GraphRecordAttribute::Int(_) => {
                Err(Failure::new(label, NonStringValue::new(value)))
            }
        }
    }

    fn from_string<'a>(value: String) -> Self::Value<'a>
    where
        Self: 'a,
    {
        GraphRecordAttribute::String(value)
    }
}

impl StringValue for IndexValue<AttributeName> {
    fn into_string<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<String>
    where
        Self: 'a,
    {
        match value {
            GraphRecordAttribute::String(value) => Ok(value),
            value @ GraphRecordAttribute::Int(_) => {
                Err(Failure::new(label, NonStringValue::new(value)))
            }
        }
    }

    fn from_string<'a>(value: String) -> Self::Value<'a>
    where
        Self: 'a,
    {
        GraphRecordAttribute::String(value)
    }
}

impl StringValue for IndexValue<GraphRecordValue> {
    fn into_string<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<String>
    where
        Self: 'a,
    {
        match value {
            GraphRecordValue::String(value) => Ok(value),
            value => Err(Failure::new(label, NonStringValue::new(value))),
        }
    }

    fn from_string<'a>(value: String) -> Self::Value<'a>
    where
        Self: 'a,
    {
        GraphRecordValue::String(value)
    }
}
