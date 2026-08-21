use crate::{
    Failure, IndexValue, QueryResult, Scalar, ValueDomain, error::numeric::NonNumericValue,
};
use graphrecords_core::graphrecord::{Value, ValueView};

pub trait ValueExponential: ValueDomain {
    fn exponential<'a>(value: Self::Value<'a>, label: &'static str)
    -> QueryResult<Self::Value<'a>>;
}

impl ValueExponential for Scalar {
    fn exponential<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        match value {
            ValueView::Int(integer) => Ok(ValueView::Float((integer as f64).exp())),
            ValueView::Float(float) => Ok(ValueView::Float(float.exp())),
            value => Err(Failure::new(
                NonNumericValue::new(Value::from(value)),
                label,
            )),
        }
    }
}

impl ValueExponential for IndexValue<Value> {
    fn exponential<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        match value {
            Value::Int(integer) => Ok(Value::Float((integer as f64).exp())),
            Value::Float(float) => Ok(Value::Float(float.exp())),
            value => Err(Failure::new(NonNumericValue::new(value), label)),
        }
    }
}
