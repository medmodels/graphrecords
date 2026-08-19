use crate::{
    Failure, IndexValue, QueryResult, Scalar, ValueDomain, error::numeric::NonNumericValue,
};
use graphrecords_core::graphrecord::{AttributeName, Identifier, NodeIndex, Value, datatypes::Abs};

pub trait ValueAbsolute: ValueDomain {
    fn absolute<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>>;
}

impl ValueAbsolute for Scalar {
    fn absolute<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            Value::Int(_) | Value::Float(_) => Ok(value.abs()),
            Value::Duration(duration) => Ok(Value::Duration(duration.abs())),
            value => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}

impl ValueAbsolute for AttributeName {
    fn absolute<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value.identifier() {
            Identifier::Int(_) => Ok(value.abs()),
            Identifier::String(_) => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}

impl ValueAbsolute for IndexValue<NodeIndex> {
    fn absolute<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value.identifier() {
            Identifier::Int(_) => Ok(value.abs()),
            Identifier::String(_) => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}

impl ValueAbsolute for IndexValue<AttributeName> {
    fn absolute<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value.identifier() {
            Identifier::Int(_) => Ok(value.abs()),
            Identifier::String(_) => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}

impl ValueAbsolute for IndexValue<Value> {
    fn absolute<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            Value::Int(_) | Value::Float(_) => Ok(value.abs()),
            Value::Duration(duration) => Ok(Value::Duration(duration.abs())),
            value => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}
