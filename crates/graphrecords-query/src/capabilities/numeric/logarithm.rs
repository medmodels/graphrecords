use crate::{
    Failure, IndexValue, QueryResult, Scalar, ValueDomain,
    error::numeric::{NonNumericValue, NonPositiveLogarithm},
};
use graphrecords_core::graphrecord::Value;

pub trait ValueLogarithm: ValueDomain {
    fn logarithm<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>>;
}

impl ValueLogarithm for Scalar {
    fn logarithm<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            Value::Int(integer) if integer <= 0 => {
                Err(Failure::new(label, NonPositiveLogarithm::new(value)))
            }
            Value::Float(float) if float <= 0.0 => {
                Err(Failure::new(label, NonPositiveLogarithm::new(value)))
            }
            Value::Int(integer) => Ok(Value::Float((integer as f64).ln())),
            Value::Float(float) => Ok(Value::Float(float.ln())),
            value => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}

impl ValueLogarithm for IndexValue<Value> {
    fn logarithm<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            Value::Int(integer) if integer <= 0 => {
                Err(Failure::new(label, NonPositiveLogarithm::new(value)))
            }
            Value::Float(float) if float <= 0.0 => {
                Err(Failure::new(label, NonPositiveLogarithm::new(value)))
            }
            Value::Int(integer) => Ok(Value::Float((integer as f64).ln())),
            Value::Float(float) => Ok(Value::Float(float.ln())),
            value => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}
