use crate::{
    Failure, IndexValue, QueryResult, Scalar, ValueDomain, error::numeric::NonNumericValue,
};
use graphrecords_core::graphrecord::{Value, ValueView, datatypes::Round};

pub trait ValueRound: ValueDomain {
    fn round<'a>(value: Self::Value<'a>, label: &'static str) -> QueryResult<Self::Value<'a>>;
}

impl ValueRound for Scalar {
    fn round<'a>(value: Self::Value<'a>, label: &'static str) -> QueryResult<Self::Value<'a>> {
        match value {
            ValueView::Int(integer) => Ok(ValueView::Int(integer)),
            ValueView::Float(float) => Ok(ValueView::Float(float.round())),
            value => Err(Failure::new(
                NonNumericValue::new(Value::from(value)),
                label,
            )),
        }
    }
}

impl ValueRound for IndexValue<Value> {
    fn round<'a>(value: Self::Value<'a>, label: &'static str) -> QueryResult<Self::Value<'a>> {
        match value {
            Value::Int(_) | Value::Float(_) => Ok(value.round()),
            value => Err(Failure::new(NonNumericValue::new(value), label)),
        }
    }
}
