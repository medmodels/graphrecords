use crate::{IndexValue, Scalar, ValueDomain};
use graphrecords_core::graphrecord::{AttributeName, Identifier, NodeIndex, Value};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PayloadKind {
    Bool,
    Int,
    Float,
    String,
    DateTime,
    Duration,
    Null,
}

pub trait ValueKindTest: ValueDomain {
    fn kind(value: &Self::Value<'_>) -> PayloadKind;
}

pub trait ValueScalarKindTest: ValueKindTest {}

const fn value_kind(value: &Value) -> PayloadKind {
    match value {
        Value::String(_) => PayloadKind::String,
        Value::Int(_) => PayloadKind::Int,
        Value::Float(_) => PayloadKind::Float,
        Value::Bool(_) => PayloadKind::Bool,
        Value::DateTime(_) => PayloadKind::DateTime,
        Value::Duration(_) => PayloadKind::Duration,
        Value::Null => PayloadKind::Null,
    }
}

const fn identifier_kind(attribute: &Identifier) -> PayloadKind {
    match attribute {
        Identifier::Int(_) => PayloadKind::Int,
        Identifier::String(_) => PayloadKind::String,
    }
}

impl ValueKindTest for Scalar {
    fn kind(value: &Self::Value<'_>) -> PayloadKind {
        value_kind(value)
    }
}

impl ValueScalarKindTest for Scalar {}

impl ValueKindTest for AttributeName {
    fn kind(value: &Self::Value<'_>) -> PayloadKind {
        identifier_kind(value.identifier())
    }
}

impl ValueKindTest for IndexValue<Value> {
    fn kind(value: &Self::Value<'_>) -> PayloadKind {
        value_kind(value)
    }
}

impl ValueScalarKindTest for IndexValue<Value> {}

impl ValueKindTest for IndexValue<NodeIndex> {
    fn kind(value: &Self::Value<'_>) -> PayloadKind {
        identifier_kind(value.identifier())
    }
}

impl ValueKindTest for IndexValue<AttributeName> {
    fn kind(value: &Self::Value<'_>) -> PayloadKind {
        identifier_kind(value.identifier())
    }
}
