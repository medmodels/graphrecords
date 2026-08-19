use crate::{
    Failure, IndexValue, QueryResult, Scalar, ValueDomain, error::numeric::NonNumericValue,
};
use graphrecords_core::graphrecord::Value;

pub trait ValueCubeRoot: ValueDomain {
    fn cube_root<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>>;
}

impl ValueCubeRoot for Scalar {
    fn cube_root<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            Value::Int(integer) => Ok(Value::Float((integer as f64).cbrt())),
            Value::Float(float) => Ok(Value::Float(float.cbrt())),
            value => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}

impl ValueCubeRoot for IndexValue<Value> {
    fn cube_root<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            Value::Int(integer) => Ok(Value::Float((integer as f64).cbrt())),
            Value::Float(float) => Ok(Value::Float(float.cbrt())),
            value => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}
