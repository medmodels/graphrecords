use crate::{Failure, IndexValue, QueryResult, Scalar, ValueType, error::numeric::NonNumericValue};
use graphrecords_core::graphrecord::{GraphRecordValue, datatypes::Floor};

pub trait ValueFloor: ValueType {
    fn floor<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>>;
}

impl ValueFloor for Scalar {
    fn floor<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            GraphRecordValue::Int(_) | GraphRecordValue::Float(_) => Ok(value.floor()),
            value => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}

impl ValueFloor for IndexValue<GraphRecordValue> {
    fn floor<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            GraphRecordValue::Int(_) | GraphRecordValue::Float(_) => Ok(value.floor()),
            value => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}
