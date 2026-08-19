use crate::{
    Failure, IndexValue, QueryResult, Scalar, ValueDomain, error::numeric::NonNumericValue,
};
use graphrecords_core::graphrecord::{Value, datatypes::Ceil};

pub trait ValueCeil: ValueDomain {
    fn ceil<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>>;
}

impl ValueCeil for Scalar {
    fn ceil<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            Value::Int(_) | Value::Float(_) => Ok(value.ceil()),
            value => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}

impl ValueCeil for IndexValue<Value> {
    fn ceil<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            Value::Int(_) | Value::Float(_) => Ok(value.ceil()),
            value => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}
