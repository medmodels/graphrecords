use crate::{Failure, IndexValue, QueryResult, Scalar, ValueType, error::numeric::NonNumericValue};
use graphrecords_core::graphrecord::{GraphRecordValue, datatypes::Ceil};

pub trait ValueCeil: ValueType {
    fn ceil<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>>;
}

impl ValueCeil for Scalar {
    fn ceil<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            GraphRecordValue::Int(_) | GraphRecordValue::Float(_) => Ok(value.ceil()),
            value => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}

impl ValueCeil for IndexValue<GraphRecordValue> {
    fn ceil<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            GraphRecordValue::Int(_) | GraphRecordValue::Float(_) => Ok(value.ceil()),
            value => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}
