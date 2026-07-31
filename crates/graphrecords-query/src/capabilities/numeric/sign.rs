use crate::{
    AttributeName, Failure, IndexValue, QueryResult, Scalar, ValueDomain,
    error::numeric::NonNumericValue,
};
use graphrecords_core::graphrecord::{GraphRecordAttribute, GraphRecordValue, NodeIndex};

pub trait ValueSign: ValueDomain {
    fn sign<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>>;
}

impl ValueSign for Scalar {
    fn sign<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            GraphRecordValue::Int(integer) => Ok(GraphRecordValue::Int(integer.signum())),
            GraphRecordValue::Float(float) => Ok(GraphRecordValue::Float(if float == 0.0 {
                0.0
            } else {
                float.signum()
            })),
            value => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}

impl ValueSign for AttributeName {
    fn sign<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            GraphRecordAttribute::Int(integer) => Ok(GraphRecordAttribute::Int(integer.signum())),
            value @ GraphRecordAttribute::String(_) => {
                Err(Failure::new(label, NonNumericValue::new(value)))
            }
        }
    }
}

impl ValueSign for IndexValue<NodeIndex> {
    fn sign<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            GraphRecordAttribute::Int(integer) => Ok(GraphRecordAttribute::Int(integer.signum())),
            value @ GraphRecordAttribute::String(_) => {
                Err(Failure::new(label, NonNumericValue::new(value)))
            }
        }
    }
}

impl ValueSign for IndexValue<AttributeName> {
    fn sign<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            GraphRecordAttribute::Int(integer) => Ok(GraphRecordAttribute::Int(integer.signum())),
            value @ GraphRecordAttribute::String(_) => {
                Err(Failure::new(label, NonNumericValue::new(value)))
            }
        }
    }
}

impl ValueSign for IndexValue<GraphRecordValue> {
    fn sign<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            GraphRecordValue::Int(integer) => Ok(GraphRecordValue::Int(integer.signum())),
            GraphRecordValue::Float(float) => Ok(GraphRecordValue::Float(if float == 0.0 {
                0.0
            } else {
                float.signum()
            })),
            value => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}
