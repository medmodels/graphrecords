use crate::{
    Failure, IndexValue, QueryResult, Scalar, ValueDomain, error::numeric::NonNumericValue,
};
use graphrecords_core::graphrecord::{Value, datatypes::Round};

pub trait ValueRound: ValueDomain {
    fn round<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>>;
}

impl ValueRound for Scalar {
    fn round<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            Value::Int(_) | Value::Float(_) => Ok(value.round()),
            value => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}

impl ValueRound for IndexValue<Value> {
    fn round<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            Value::Int(_) | Value::Float(_) => Ok(value.round()),
            value => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}
