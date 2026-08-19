use crate::{
    Failure, IndexValue, QueryResult, Scalar, ValueDomain, error::arithmetic::DivisionByZero,
};
use graphrecords_core::graphrecord::Value;

pub trait ValueDivide: ValueDomain {
    fn divide<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>>;
}

fn is_division_by_zero(dividend: &Value, divisor: &Value) -> bool {
    match (dividend, divisor) {
        (Value::Int(_) | Value::Float(_) | Value::Duration(_), Value::Int(0)) => true,
        (Value::Int(_) | Value::Float(_), Value::Float(divisor)) => *divisor == 0.0,
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

impl ValueDivide for IndexValue<Value> {
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
