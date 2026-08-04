use crate::{
    AttributeName, Failure, IndexValue, Positional, QueryResult, Scalar, ValueDomain,
    error::numeric::{IntegerOverflow, NonIntegerValue},
};
use graphrecords_core::graphrecord::{
    EdgeIndex, GraphRecordAttribute, GraphRecordValue, NodeIndex,
};

pub trait IntValue: ValueDomain {
    fn into_int(label: &'static str, value: Self::Value<'_>) -> QueryResult<i64>;
}

fn int_from_value(label: &'static str, value: GraphRecordValue) -> QueryResult<i64> {
    match value {
        GraphRecordValue::Int(value) => Ok(value),
        value => Err(Failure::new(label, NonIntegerValue::new(value))),
    }
}

fn int_from_attribute(label: &'static str, value: GraphRecordAttribute) -> QueryResult<i64> {
    match value {
        GraphRecordAttribute::Int(value) => Ok(value),
        value @ GraphRecordAttribute::String(_) => {
            Err(Failure::new(label, NonIntegerValue::new(value)))
        }
    }
}

impl IntValue for Scalar {
    fn into_int(label: &'static str, value: Self::Value<'_>) -> QueryResult<i64> {
        int_from_value(label, value)
    }
}

impl IntValue for IndexValue<GraphRecordValue> {
    fn into_int(label: &'static str, value: Self::Value<'_>) -> QueryResult<i64> {
        int_from_value(label, value)
    }
}

impl IntValue for AttributeName {
    fn into_int(label: &'static str, value: Self::Value<'_>) -> QueryResult<i64> {
        int_from_attribute(label, value)
    }
}

impl IntValue for IndexValue<NodeIndex> {
    fn into_int(label: &'static str, value: Self::Value<'_>) -> QueryResult<i64> {
        int_from_attribute(label, value)
    }
}

impl IntValue for IndexValue<AttributeName> {
    fn into_int(label: &'static str, value: Self::Value<'_>) -> QueryResult<i64> {
        int_from_attribute(label, value)
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
