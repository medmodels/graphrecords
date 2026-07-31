use crate::{
    AttributeName, Failure, IndexValue, QueryResult, Scalar, ValueDomain,
    error::numeric::NonNumericValue,
};
use graphrecords_core::graphrecord::{GraphRecordAttribute, GraphRecordValue, NodeIndex};

pub trait ValueNegate: ValueDomain {
    fn negate<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>>;
}

impl ValueNegate for Scalar {
    fn negate<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            GraphRecordValue::Int(integer) => Ok(GraphRecordValue::Int(-integer)),
            GraphRecordValue::Float(float) => Ok(GraphRecordValue::Float(-float)),
            GraphRecordValue::Duration(duration) => Ok(GraphRecordValue::Duration(-duration)),
            value => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}

impl ValueNegate for AttributeName {
    fn negate<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            GraphRecordAttribute::Int(integer) => Ok(GraphRecordAttribute::Int(-integer)),
            value @ GraphRecordAttribute::String(_) => {
                Err(Failure::new(label, NonNumericValue::new(value)))
            }
        }
    }
}

impl ValueNegate for IndexValue<NodeIndex> {
    fn negate<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            GraphRecordAttribute::Int(integer) => Ok(GraphRecordAttribute::Int(-integer)),
            value @ GraphRecordAttribute::String(_) => {
                Err(Failure::new(label, NonNumericValue::new(value)))
            }
        }
    }
}

impl ValueNegate for IndexValue<AttributeName> {
    fn negate<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            GraphRecordAttribute::Int(integer) => Ok(GraphRecordAttribute::Int(-integer)),
            value @ GraphRecordAttribute::String(_) => {
                Err(Failure::new(label, NonNumericValue::new(value)))
            }
        }
    }
}

impl ValueNegate for IndexValue<GraphRecordValue> {
    fn negate<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            GraphRecordValue::Int(integer) => Ok(GraphRecordValue::Int(-integer)),
            GraphRecordValue::Float(float) => Ok(GraphRecordValue::Float(-float)),
            GraphRecordValue::Duration(duration) => Ok(GraphRecordValue::Duration(-duration)),
            value => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}
