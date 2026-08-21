use crate::{
    Failure, IndexValue, QueryResult, Scalar, ValueDomain,
    error::numeric::{NegativeSquareRoot, NonNumericValue},
};
use graphrecords_core::graphrecord::{Value, ValueView, datatypes::Sqrt};

pub trait ValueSquareRoot: ValueDomain {
    fn square_root<'a>(value: Self::Value<'a>, label: &'static str)
    -> QueryResult<Self::Value<'a>>;
}

impl ValueSquareRoot for Scalar {
    fn square_root<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        match value {
            ValueView::Int(integer) if integer < 0 => Err(Failure::new(
                NegativeSquareRoot::new(Value::from(value)),
                label,
            )),
            ValueView::Float(float) if float < 0.0 => Err(Failure::new(
                NegativeSquareRoot::new(Value::from(value)),
                label,
            )),
            ValueView::Int(integer) => Ok(ValueView::Float((integer as f64).sqrt())),
            ValueView::Float(float) => Ok(ValueView::Float(float.sqrt())),
            value => Err(Failure::new(
                NonNumericValue::new(Value::from(value)),
                label,
            )),
        }
    }
}

impl ValueSquareRoot for IndexValue<Value> {
    fn square_root<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        match value {
            Value::Int(integer) if integer < 0 => {
                Err(Failure::new(NegativeSquareRoot::new(value), label))
            }
            Value::Float(float) if float < 0.0 => {
                Err(Failure::new(NegativeSquareRoot::new(value), label))
            }
            Value::Int(_) | Value::Float(_) => Ok(value.sqrt()),
            value => Err(Failure::new(NonNumericValue::new(value), label)),
        }
    }
}
