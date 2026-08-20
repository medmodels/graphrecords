use crate::{
    Failure, IndexValue, Positional, QueryResult, Scalar, ValueDomain,
    error::numeric::{IntegerOverflow, NonIntegerValue},
};
use graphrecords_core::graphrecord::{AttributeName, IdentifierView, NodeIndex, Value, ValueView};

pub trait ValueInt: ValueDomain {
    fn into_int(value: Self::Value<'_>, label: &'static str) -> QueryResult<i64>;
}

fn int_from_value(value: ValueView<'_>, label: &'static str) -> QueryResult<i64> {
    match value {
        ValueView::Int(value) => Ok(value),
        value => Err(Failure::new(
            NonIntegerValue::new(Value::from(value)),
            label,
        )),
    }
}

fn int_from_value_owned(value: Value, label: &'static str) -> QueryResult<i64> {
    match value {
        Value::Int(value) => Ok(value),
        value => Err(Failure::new(NonIntegerValue::new(value), label)),
    }
}

impl ValueInt for Scalar {
    fn into_int(value: Self::Value<'_>, label: &'static str) -> QueryResult<i64> {
        int_from_value(value, label)
    }
}

impl ValueInt for IndexValue<Value> {
    fn into_int(value: Self::Value<'_>, label: &'static str) -> QueryResult<i64> {
        int_from_value_owned(value, label)
    }
}

impl ValueInt for AttributeName {
    fn into_int(value: Self::Value<'_>, label: &'static str) -> QueryResult<i64> {
        match value.identifier_view() {
            IdentifierView::Int(integer) => Ok(*integer),
            IdentifierView::String(_) => {
                Err(Failure::new(NonIntegerValue::new(Self::from(value)), label))
            }
        }
    }
}

impl ValueInt for IndexValue<NodeIndex> {
    fn into_int(value: Self::Value<'_>, label: &'static str) -> QueryResult<i64> {
        match value.identifier_view() {
            IdentifierView::Int(integer) => Ok(*integer),
            IdentifierView::String(_) => Err(Failure::new(
                NonIntegerValue::new(NodeIndex::from(value)),
                label,
            )),
        }
    }
}

impl ValueInt for IndexValue<AttributeName> {
    fn into_int(value: Self::Value<'_>, label: &'static str) -> QueryResult<i64> {
        match value.identifier_view() {
            IdentifierView::Int(integer) => Ok(*integer),
            IdentifierView::String(_) => Err(Failure::new(
                NonIntegerValue::new(AttributeName::from(value)),
                label,
            )),
        }
    }
}

impl ValueInt for IndexValue<Positional> {
    fn into_int(value: Self::Value<'_>, label: &'static str) -> QueryResult<i64> {
        i64::try_from(value).map_err(|_| Failure::new(IntegerOverflow::new(value), label))
    }
}
