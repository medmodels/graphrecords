use crate::{
    Failure, IndexValue, QueryResult, Scalar, ValueDomain, error::numeric::NonNumericValue,
};
use graphrecords_core::graphrecord::GraphRecordValue;

pub trait ValueCubeRoot: ValueDomain {
    fn cube_root<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>>;
}

impl ValueCubeRoot for Scalar {
    fn cube_root<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            GraphRecordValue::Int(integer) => Ok(GraphRecordValue::Float((integer as f64).cbrt())),
            GraphRecordValue::Float(float) => Ok(GraphRecordValue::Float(float.cbrt())),
            value => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}

impl ValueCubeRoot for IndexValue<GraphRecordValue> {
    fn cube_root<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            GraphRecordValue::Int(integer) => Ok(GraphRecordValue::Float((integer as f64).cbrt())),
            GraphRecordValue::Float(float) => Ok(GraphRecordValue::Float(float.cbrt())),
            value => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}
