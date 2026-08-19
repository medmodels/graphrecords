use crate::{
    Failure, IndexValue, QueryResult, Scalar, ValueDomain, error::numeric::NonNumericValue,
};
use graphrecords_core::graphrecord::{AttributeName, Identifier, NodeIndex, Value};

pub trait ValueNegate: ValueDomain {
    fn negate<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>>;
}

impl ValueNegate for Scalar {
    fn negate<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            Value::Int(integer) => Ok(Value::Int(-integer)),
            Value::Float(float) => Ok(Value::Float(-float)),
            Value::Duration(duration) => Ok(Value::Duration(-duration)),
            value => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}

impl ValueNegate for AttributeName {
    fn negate<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value.identifier() {
            Identifier::Int(integer) => Ok(Self::from(-integer)),
            Identifier::String(_) => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}

impl ValueNegate for IndexValue<NodeIndex> {
    fn negate<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value.identifier() {
            Identifier::Int(integer) => Ok(NodeIndex::from(-integer)),
            Identifier::String(_) => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}

impl ValueNegate for IndexValue<AttributeName> {
    fn negate<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value.identifier() {
            Identifier::Int(integer) => Ok(AttributeName::from(-integer)),
            Identifier::String(_) => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}

impl ValueNegate for IndexValue<Value> {
    fn negate<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            Value::Int(integer) => Ok(Value::Int(-integer)),
            Value::Float(float) => Ok(Value::Float(-float)),
            Value::Duration(duration) => Ok(Value::Duration(-duration)),
            value => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}
