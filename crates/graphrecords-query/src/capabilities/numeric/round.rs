use crate::{
    Failure, IndexValue, QueryResult, Scalar, ValueDomain, error::numeric::NonNumericValue,
};
use graphrecords_core::graphrecord::{GraphRecordValue, datatypes::Round};

pub trait ValueRound: ValueDomain {
    fn round<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>>;
}

impl ValueRound for Scalar {
    fn round<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            GraphRecordValue::Int(_) | GraphRecordValue::Float(_) => Ok(value.round()),
            value => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}

impl ValueRound for IndexValue<GraphRecordValue> {
    fn round<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            GraphRecordValue::Int(_) | GraphRecordValue::Float(_) => Ok(value.round()),
            value => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}
