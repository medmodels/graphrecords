use crate::{
    Failure, IndexValue, QueryResult, Scalar, ValueDomain, capabilities::value_into_view,
    error::arithmetic::DivisionByZero,
};
use graphrecords_core::graphrecord::Value;

pub trait ValueDivide: ValueDomain {
    fn divide<'a>(
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
        label: &'static str,
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
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        let value = Value::from(value);
        let argument = Value::from(argument);

        if is_division_by_zero(&value, &argument) {
            return Err(Failure::new(DivisionByZero::new(value), label));
        }

        (value / argument)
            .map(value_into_view)
            .map_err(|error| Failure::new(error, label))
    }
}

impl ValueDivide for IndexValue<Value> {
    fn divide<'a>(
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        if is_division_by_zero(&value, &argument) {
            return Err(Failure::new(DivisionByZero::new(value), label));
        }

        (value / argument).map_err(|error| Failure::new(error, label))
    }
}
