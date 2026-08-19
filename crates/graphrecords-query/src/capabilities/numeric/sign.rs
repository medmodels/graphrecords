use crate::{
    Failure, IndexValue, QueryResult, Scalar, ValueDomain, error::numeric::NonNumericValue,
};
use graphrecords_core::graphrecord::{AttributeName, Identifier, NodeIndex, Value};

pub trait ValueSign: ValueDomain {
    fn sign<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>>;
}

impl ValueSign for Scalar {
    fn sign<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            Value::Int(integer) => Ok(Value::Int(integer.signum())),
            Value::Float(float) => Ok(Value::Float(if float == 0.0 {
                0.0
            } else {
                float.signum()
            })),
            value => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}

impl ValueSign for AttributeName {
    fn sign<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value.identifier() {
            Identifier::Int(integer) => Ok(Self::from(integer.signum())),
            Identifier::String(_) => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}

impl ValueSign for IndexValue<NodeIndex> {
    fn sign<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value.identifier() {
            Identifier::Int(integer) => Ok(NodeIndex::from(integer.signum())),
            Identifier::String(_) => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}

impl ValueSign for IndexValue<AttributeName> {
    fn sign<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value.identifier() {
            Identifier::Int(integer) => Ok(AttributeName::from(integer.signum())),
            Identifier::String(_) => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}

impl ValueSign for IndexValue<Value> {
    fn sign<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        match value {
            Value::Int(integer) => Ok(Value::Int(integer.signum())),
            Value::Float(float) => Ok(Value::Float(if float == 0.0 {
                0.0
            } else {
                float.signum()
            })),
            value => Err(Failure::new(label, NonNumericValue::new(value))),
        }
    }
}
