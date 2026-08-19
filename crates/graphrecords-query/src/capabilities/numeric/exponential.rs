use crate::{
    Failure, IndexValue, QueryResult, Scalar, ValueDomain, error::numeric::NonNumericValue,
};
use graphrecords_core::graphrecord::Value;

pub trait ValueExponential: ValueDomain {
    fn exponential<'a>(label: &'static str, value: Self::Value<'a>)
    -> QueryResult<Self::Value<'a>>;
}

impl ValueExponential for Scalar {
    fn exponential<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        match value {
            Value::Int(integer) => Ok(Value::Float((integer as f64).exp())),
            Value::Float(float) => Ok(Value::Float(float.exp())),
            value => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}

impl ValueExponential for IndexValue<Value> {
    fn exponential<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        match value {
            Value::Int(integer) => Ok(Value::Float((integer as f64).exp())),
            Value::Float(float) => Ok(Value::Float(float.exp())),
            value => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}
