use crate::{
    AttributeName, Failure, IndexValue, QueryResult, Scalar, ValueType,
    error::numeric::NonNumericValue,
};
use graphrecords_core::graphrecord::{
    GraphRecordAttribute, GraphRecordValue, NodeIndex, datatypes::Abs,
};

pub trait ValueAbsolute: ValueType {
    fn absolute<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>>;
}

impl ValueAbsolute for Scalar {
    fn absolute<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            GraphRecordValue::Int(_) | GraphRecordValue::Float(_) => Ok(value.abs()),
            GraphRecordValue::Duration(duration) => Ok(GraphRecordValue::Duration(duration.abs())),
            value => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}

impl ValueAbsolute for AttributeName {
    fn absolute<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            GraphRecordAttribute::Int(_) => Ok(value.abs()),
            value @ GraphRecordAttribute::String(_) => {
                Err(Failure::new(label, NonNumericValue::new(value)))
            }
        }
    }
}

impl ValueAbsolute for IndexValue<NodeIndex> {
    fn absolute<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            GraphRecordAttribute::Int(_) => Ok(value.abs()),
            value @ GraphRecordAttribute::String(_) => {
                Err(Failure::new(label, NonNumericValue::new(value)))
            }
        }
    }
}

impl ValueAbsolute for IndexValue<AttributeName> {
    fn absolute<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            GraphRecordAttribute::Int(_) => Ok(value.abs()),
            value @ GraphRecordAttribute::String(_) => {
                Err(Failure::new(label, NonNumericValue::new(value)))
            }
        }
    }
}

impl ValueAbsolute for IndexValue<GraphRecordValue> {
    fn absolute<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            GraphRecordValue::Int(_) | GraphRecordValue::Float(_) => Ok(value.abs()),
            GraphRecordValue::Duration(duration) => Ok(GraphRecordValue::Duration(duration.abs())),
            value => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}
