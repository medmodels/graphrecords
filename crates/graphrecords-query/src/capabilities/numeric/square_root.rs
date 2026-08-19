use crate::{
    Failure, IndexValue, QueryResult, Scalar, ValueDomain,
    error::numeric::{NegativeSquareRoot, NonNumericValue},
};
use graphrecords_core::graphrecord::{Value, datatypes::Sqrt};

pub trait ValueSquareRoot: ValueDomain {
    fn square_root<'a>(label: &'static str, value: Self::Value<'a>)
    -> QueryResult<Self::Value<'a>>;
}

impl ValueSquareRoot for Scalar {
    fn square_root<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        match value {
            Value::Int(integer) if integer < 0 => {
                Err(Failure::new(label, NegativeSquareRoot::new(value)))
            }
            Value::Float(float) if float < 0.0 => {
                Err(Failure::new(label, NegativeSquareRoot::new(value)))
            }
            Value::Int(_) | Value::Float(_) => Ok(value.sqrt()),
            value => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}

impl ValueSquareRoot for IndexValue<Value> {
    fn square_root<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        match value {
            Value::Int(integer) if integer < 0 => {
                Err(Failure::new(label, NegativeSquareRoot::new(value)))
            }
            Value::Float(float) if float < 0.0 => {
                Err(Failure::new(label, NegativeSquareRoot::new(value)))
            }
            Value::Int(_) | Value::Float(_) => Ok(value.sqrt()),
            value => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}
