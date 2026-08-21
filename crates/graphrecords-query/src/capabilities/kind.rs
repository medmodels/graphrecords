use crate::{IndexValue, Scalar, ValueDomain};
use graphrecords_core::graphrecord::{AttributeName, IdentifierView, NodeIndex, Value, ValueView};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PayloadKind {
    String,
    Int,
    Float,
    Bool,
    DateTime,
    Duration,
    Null,
}

pub trait ValueKindTest: ValueDomain {
    fn kind(value: &Self::Value<'_>) -> PayloadKind;
}

pub trait ValueScalarKindTest: ValueKindTest {}

const fn value_kind(value: &ValueView<'_>) -> PayloadKind {
    match value {
        ValueView::String(_) => PayloadKind::String,
        ValueView::Int(_) => PayloadKind::Int,
        ValueView::Float(_) => PayloadKind::Float,
        ValueView::Bool(_) => PayloadKind::Bool,
        ValueView::DateTime(_) => PayloadKind::DateTime,
        ValueView::Duration(_) => PayloadKind::Duration,
        ValueView::Null => PayloadKind::Null,
    }
}

const fn value_kind_owned(value: &Value) -> PayloadKind {
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

const fn identifier_kind(identifier: &IdentifierView<'_>) -> PayloadKind {
    match identifier {
        IdentifierView::Int(_) => PayloadKind::Int,
        IdentifierView::String(_) => PayloadKind::String,
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
        identifier_kind(value.identifier_view())
    }
}

impl ValueKindTest for IndexValue<NodeIndex> {
    fn kind(value: &Self::Value<'_>) -> PayloadKind {
        identifier_kind(value.identifier_view())
    }
}

impl ValueKindTest for IndexValue<AttributeName> {
    fn kind(value: &Self::Value<'_>) -> PayloadKind {
        identifier_kind(value.identifier_view())
    }
}

impl ValueKindTest for IndexValue<Value> {
    fn kind(value: &Self::Value<'_>) -> PayloadKind {
        value_kind_owned(value)
    }
}

impl ValueScalarKindTest for IndexValue<Value> {}
