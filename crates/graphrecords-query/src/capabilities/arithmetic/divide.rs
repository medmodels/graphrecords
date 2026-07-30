use crate::{
    Failure, IndexValue, QueryResult, Scalar, ValueType, error::arithmetic::DivisionByZero,
};
use graphrecords_core::graphrecord::GraphRecordValue;

pub trait ValueDivide: ValueType {
    fn divide<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>>;
}

fn is_division_by_zero(dividend: &GraphRecordValue, divisor: &GraphRecordValue) -> bool {
    match (dividend, divisor) {
        (
            GraphRecordValue::Int(_) | GraphRecordValue::Float(_) | GraphRecordValue::Duration(_),
            GraphRecordValue::Int(0),
        ) => true,
        (
            GraphRecordValue::Int(_) | GraphRecordValue::Float(_),
            GraphRecordValue::Float(divisor),
        ) => *divisor == 0.0,
        _ => false,
    }
}

impl ValueDivide for Scalar {
    fn divide<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        if is_division_by_zero(&value, &argument) {
            return Err(Failure::new(label, DivisionByZero::new(value)));
        }

        (value / argument).map_err(|error| Failure::new(label, error))
    }
}

impl ValueDivide for IndexValue<GraphRecordValue> {
    fn divide<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        if is_division_by_zero(&value, &argument) {
            return Err(Failure::new(label, DivisionByZero::new(value)));
        }

        (value / argument).map_err(|error| Failure::new(label, error))
    }
}
