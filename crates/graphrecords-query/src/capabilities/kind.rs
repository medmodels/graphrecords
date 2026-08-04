use crate::{AttributeName, IndexValue, Scalar, ValueDomain};
use graphrecords_core::graphrecord::{GraphRecordAttribute, GraphRecordValue, NodeIndex};

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

const fn graphrecord_value_kind(value: &GraphRecordValue) -> PayloadKind {
    match value {
        GraphRecordValue::String(_) => PayloadKind::String,
        GraphRecordValue::Int(_) => PayloadKind::Int,
        GraphRecordValue::Float(_) => PayloadKind::Float,
        GraphRecordValue::Bool(_) => PayloadKind::Bool,
        GraphRecordValue::DateTime(_) => PayloadKind::DateTime,
        GraphRecordValue::Duration(_) => PayloadKind::Duration,
        GraphRecordValue::Null => PayloadKind::Null,
    }
}

const fn graphrecord_attribute_kind(attribute: &GraphRecordAttribute) -> PayloadKind {
    match attribute {
        GraphRecordAttribute::Int(_) => PayloadKind::Int,
        GraphRecordAttribute::String(_) => PayloadKind::String,
    }
}

impl ValueKindTest for Scalar {
    fn kind(value: &Self::Value<'_>) -> PayloadKind {
        graphrecord_value_kind(value)
    }
}

impl ValueScalarKindTest for Scalar {}

impl ValueKindTest for AttributeName {
    fn kind(value: &Self::Value<'_>) -> PayloadKind {
        graphrecord_attribute_kind(value)
    }
}

impl ValueKindTest for IndexValue<GraphRecordValue> {
    fn kind(value: &Self::Value<'_>) -> PayloadKind {
        graphrecord_value_kind(value)
    }
}

impl ValueScalarKindTest for IndexValue<GraphRecordValue> {}

impl ValueKindTest for IndexValue<NodeIndex> {
    fn kind(value: &Self::Value<'_>) -> PayloadKind {
        graphrecord_attribute_kind(value)
    }
}

impl ValueKindTest for IndexValue<AttributeName> {
    fn kind(value: &Self::Value<'_>) -> PayloadKind {
        graphrecord_attribute_kind(value)
    }
}
