use crate::{
    Failure, IndexValue, QueryResult, Scalar, ValueType,
    error::numeric::{NegativeSquareRoot, NonNumericValue},
};
use graphrecords_core::graphrecord::{GraphRecordValue, datatypes::Sqrt};

pub trait ValueSquareRoot: ValueType {
    fn square_root<'a>(label: &'static str, value: Self::Value<'a>)
    -> QueryResult<Self::Value<'a>>;
}

impl ValueSquareRoot for Scalar {
    fn square_root<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        match value {
            GraphRecordValue::Int(integer) if integer < 0 => {
                Err(Failure::new(label, NegativeSquareRoot::new(value)))
            }
            GraphRecordValue::Float(float) if float < 0.0 => {
                Err(Failure::new(label, NegativeSquareRoot::new(value)))
            }
            GraphRecordValue::Int(_) | GraphRecordValue::Float(_) => Ok(value.sqrt()),
            value => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}

impl ValueSquareRoot for IndexValue<GraphRecordValue> {
    fn square_root<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        match value {
            GraphRecordValue::Int(integer) if integer < 0 => {
                Err(Failure::new(label, NegativeSquareRoot::new(value)))
            }
            GraphRecordValue::Float(float) if float < 0.0 => {
                Err(Failure::new(label, NegativeSquareRoot::new(value)))
            }
            GraphRecordValue::Int(_) | GraphRecordValue::Float(_) => Ok(value.sqrt()),
            value => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}
