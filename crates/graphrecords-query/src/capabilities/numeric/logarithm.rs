use crate::{
    Failure, IndexValue, QueryResult, Scalar, ValueType,
    error::numeric::{NonNumericValue, NonPositiveLogarithm},
};
use graphrecords_core::graphrecord::GraphRecordValue;

pub trait ValueLogarithm: ValueType {
    fn logarithm<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>>;
}

impl ValueLogarithm for Scalar {
    fn logarithm<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            GraphRecordValue::Int(integer) if integer <= 0 => {
                Err(Failure::new(label, NonPositiveLogarithm::new(value)))
            }
            GraphRecordValue::Float(float) if float <= 0.0 => {
                Err(Failure::new(label, NonPositiveLogarithm::new(value)))
            }
            GraphRecordValue::Int(integer) => Ok(GraphRecordValue::Float((integer as f64).ln())),
            GraphRecordValue::Float(float) => Ok(GraphRecordValue::Float(float.ln())),
            value => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}

impl ValueLogarithm for IndexValue<GraphRecordValue> {
    fn logarithm<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            GraphRecordValue::Int(integer) if integer <= 0 => {
                Err(Failure::new(label, NonPositiveLogarithm::new(value)))
            }
            GraphRecordValue::Float(float) if float <= 0.0 => {
                Err(Failure::new(label, NonPositiveLogarithm::new(value)))
            }
            GraphRecordValue::Int(integer) => Ok(GraphRecordValue::Float((integer as f64).ln())),
            GraphRecordValue::Float(float) => Ok(GraphRecordValue::Float(float.ln())),
            value => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}
