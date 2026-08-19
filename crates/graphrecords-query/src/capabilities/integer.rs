use crate::{
    Failure, IndexValue, Positional, QueryResult, Scalar, ValueDomain,
    error::numeric::{IntegerOverflow, NonIntegerValue},
};
use graphrecords_core::graphrecord::{AttributeName, EdgeIndex, Identifier, NodeIndex, Value};

pub trait IntValue: ValueDomain {
    fn into_int(label: &'static str, value: Self::Value<'_>) -> QueryResult<i64>;
}

fn int_from_value(label: &'static str, value: Value) -> QueryResult<i64> {
    match value {
        Value::Int(value) => Ok(value),
        value => Err(Failure::new(label, NonIntegerValue::new(value))),
    }
}

impl IntValue for Scalar {
    fn into_int(label: &'static str, value: Self::Value<'_>) -> QueryResult<i64> {
        int_from_value(label, value)
    }
}

impl IntValue for IndexValue<Value> {
    fn into_int(label: &'static str, value: Self::Value<'_>) -> QueryResult<i64> {
        int_from_value(label, value)
    }
}

impl IntValue for AttributeName {
    fn into_int(label: &'static str, value: Self::Value<'_>) -> QueryResult<i64> {
        match value.identifier() {
            Identifier::Int(integer) => Ok(*integer),
            Identifier::String(_) => Err(Failure::new(label, NonIntegerValue::new(value))),
        }
    }
}

impl IntValue for IndexValue<NodeIndex> {
    fn into_int(label: &'static str, value: Self::Value<'_>) -> QueryResult<i64> {
        match value.identifier() {
            Identifier::Int(integer) => Ok(*integer),
            Identifier::String(_) => Err(Failure::new(label, NonIntegerValue::new(value))),
        }
    }
}

impl IntValue for IndexValue<AttributeName> {
    fn into_int(label: &'static str, value: Self::Value<'_>) -> QueryResult<i64> {
        match value.identifier() {
            Identifier::Int(integer) => Ok(*integer),
            Identifier::String(_) => Err(Failure::new(label, NonIntegerValue::new(value))),
        }
    }
}

impl IntValue for IndexValue<EdgeIndex> {
    fn into_int(_label: &'static str, value: Self::Value<'_>) -> QueryResult<i64> {
        Ok(i64::from(value))
    }
}

impl IntValue for IndexValue<Positional> {
    fn into_int(label: &'static str, value: Self::Value<'_>) -> QueryResult<i64> {
        i64::try_from(value).map_err(|_| Failure::new(label, IntegerOverflow::new(value)))
    }
}
