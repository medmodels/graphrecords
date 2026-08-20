use crate::{
    Failure, IndexValue, QueryResult, Scalar, ValueDomain,
    error::numeric::{NonNumericValue, NonPositiveLogarithm},
};
use graphrecords_core::graphrecord::{Value, ValueView};

pub trait ValueLogarithm: ValueDomain {
    fn logarithm<'a>(value: Self::Value<'a>, label: &'static str) -> QueryResult<Self::Value<'a>>;
}

impl ValueLogarithm for Scalar {
    fn logarithm<'a>(value: Self::Value<'a>, label: &'static str) -> QueryResult<Self::Value<'a>> {
        match value {
            ValueView::Int(integer) if integer <= 0 => Err(Failure::new(
                NonPositiveLogarithm::new(Value::from(value)),
                label,
            )),
            ValueView::Float(float) if float <= 0.0 => Err(Failure::new(
                NonPositiveLogarithm::new(Value::from(value)),
                label,
            )),
            ValueView::Int(integer) => Ok(ValueView::Float((integer as f64).ln())),
            ValueView::Float(float) => Ok(ValueView::Float(float.ln())),
            value => Err(Failure::new(
                NonNumericValue::new(Value::from(value)),
                label,
            )),
        }
    }
}

impl ValueLogarithm for IndexValue<Value> {
    fn logarithm<'a>(value: Self::Value<'a>, label: &'static str) -> QueryResult<Self::Value<'a>> {
        match value {
            Value::Int(integer) if integer <= 0 => {
                Err(Failure::new(NonPositiveLogarithm::new(value), label))
            }
            Value::Float(float) if float <= 0.0 => {
                Err(Failure::new(NonPositiveLogarithm::new(value), label))
            }
            Value::Int(integer) => Ok(Value::Float((integer as f64).ln())),
            Value::Float(float) => Ok(Value::Float(float.ln())),
            value => Err(Failure::new(NonNumericValue::new(value), label)),
        }
    }
}
