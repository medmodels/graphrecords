use crate::{
    Failure, IndexValue, QueryResult, Scalar, ValueDomain, error::numeric::NonNumericValue,
};
use graphrecords_core::graphrecord::{Value, ValueView, datatypes::Ceil};

pub trait ValueCeil: ValueDomain {
    fn ceil<'a>(value: Self::Value<'a>, label: &'static str) -> QueryResult<Self::Value<'a>>;
}

impl ValueCeil for Scalar {
    fn ceil<'a>(value: Self::Value<'a>, label: &'static str) -> QueryResult<Self::Value<'a>> {
        match value {
            ValueView::Int(integer) => Ok(ValueView::Int(integer)),
            ValueView::Float(float) => Ok(ValueView::Float(float.ceil())),
            value => Err(Failure::new(
                NonNumericValue::new(Value::from(value)),
                label,
            )),
        }
    }
}

impl ValueCeil for IndexValue<Value> {
    fn ceil<'a>(value: Self::Value<'a>, label: &'static str) -> QueryResult<Self::Value<'a>> {
        match value {
            Value::Int(_) | Value::Float(_) => Ok(value.ceil()),
            value => Err(Failure::new(NonNumericValue::new(value), label)),
        }
    }
}
