use crate::{
    Failure, IndexValue, QueryResult, Scalar, ValueDomain, error::numeric::NonNumericValue,
};
use graphrecords_core::graphrecord::{Value, ValueView, datatypes::Floor};

pub trait ValueFloor: ValueDomain {
    fn floor<'a>(value: Self::Value<'a>, label: &'static str) -> QueryResult<Self::Value<'a>>;
}

impl ValueFloor for Scalar {
    fn floor<'a>(value: Self::Value<'a>, label: &'static str) -> QueryResult<Self::Value<'a>> {
        match value {
            ValueView::Int(integer) => Ok(ValueView::Int(integer)),
            ValueView::Float(float) => Ok(ValueView::Float(float.floor())),
            value => Err(Failure::new(
                NonNumericValue::new(Value::from(value)),
                label,
            )),
        }
    }
}

impl ValueFloor for IndexValue<Value> {
    fn floor<'a>(value: Self::Value<'a>, label: &'static str) -> QueryResult<Self::Value<'a>> {
        match value {
            Value::Int(_) | Value::Float(_) => Ok(value.floor()),
            value => Err(Failure::new(NonNumericValue::new(value), label)),
        }
    }
}
