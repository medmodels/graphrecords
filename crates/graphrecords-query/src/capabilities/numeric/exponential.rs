use crate::{
    Failure, IndexValue, QueryResult, Scalar, ValueDomain, error::numeric::NonNumericValue,
};
use graphrecords_core::graphrecord::GraphRecordValue;

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
            GraphRecordValue::Int(integer) => Ok(GraphRecordValue::Float((integer as f64).exp())),
            GraphRecordValue::Float(float) => Ok(GraphRecordValue::Float(float.exp())),
            value => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}

impl ValueExponential for IndexValue<GraphRecordValue> {
    fn exponential<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        match value {
            GraphRecordValue::Int(integer) => Ok(GraphRecordValue::Float((integer as f64).exp())),
            GraphRecordValue::Float(float) => Ok(GraphRecordValue::Float(float.exp())),
            value => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}
