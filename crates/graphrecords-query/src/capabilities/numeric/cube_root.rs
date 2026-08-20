use crate::{
    Failure, IndexValue, QueryResult, Scalar, ValueDomain, error::numeric::NonNumericValue,
};
use graphrecords_core::graphrecord::{Value, ValueView};

pub trait ValueCubeRoot: ValueDomain {
    fn cube_root<'a>(value: Self::Value<'a>, label: &'static str) -> QueryResult<Self::Value<'a>>;
}

impl ValueCubeRoot for Scalar {
    fn cube_root<'a>(value: Self::Value<'a>, label: &'static str) -> QueryResult<Self::Value<'a>> {
        match value {
            ValueView::Int(integer) => Ok(ValueView::Float((integer as f64).cbrt())),
            ValueView::Float(float) => Ok(ValueView::Float(float.cbrt())),
            value => Err(Failure::new(
                NonNumericValue::new(Value::from(value)),
                label,
            )),
        }
    }
}

impl ValueCubeRoot for IndexValue<Value> {
    fn cube_root<'a>(value: Self::Value<'a>, label: &'static str) -> QueryResult<Self::Value<'a>> {
        match value {
            Value::Int(integer) => Ok(Value::Float((integer as f64).cbrt())),
            Value::Float(float) => Ok(Value::Float(float.cbrt())),
            value => Err(Failure::new(NonNumericValue::new(value), label)),
        }
    }
}
