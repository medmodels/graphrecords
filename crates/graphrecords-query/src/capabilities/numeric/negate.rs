use crate::{
    Failure, IndexValue, QueryResult, Scalar, ValueDomain, error::numeric::NonNumericValue,
};
use graphrecords_core::graphrecord::{
    AttributeName, IdentifierView, NodeIndex, NodeIndexView, Value, ValueView,
    datatypes::AttributeNameView,
};

pub trait ValueNegate: ValueDomain {
    fn negate<'a>(value: Self::Value<'a>, label: &'static str) -> QueryResult<Self::Value<'a>>;
}

impl ValueNegate for Scalar {
    fn negate<'a>(value: Self::Value<'a>, label: &'static str) -> QueryResult<Self::Value<'a>> {
        match value {
            ValueView::Int(integer) => Ok(ValueView::Int(-integer)),
            ValueView::Float(float) => Ok(ValueView::Float(-float)),
            ValueView::Duration(duration) => Ok(ValueView::Duration(-duration)),
            value => Err(Failure::new(
                NonNumericValue::new(Value::from(value)),
                label,
            )),
        }
    }
}

impl ValueNegate for AttributeName {
    fn negate<'a>(value: Self::Value<'a>, label: &'static str) -> QueryResult<Self::Value<'a>> {
        match value.identifier_view() {
            IdentifierView::Int(integer) => {
                Ok(AttributeNameView::from(IdentifierView::Int(-integer)))
            }
            IdentifierView::String(_) => {
                Err(Failure::new(NonNumericValue::new(Self::from(value)), label))
            }
        }
    }
}

impl ValueNegate for IndexValue<NodeIndex> {
    fn negate<'a>(value: Self::Value<'a>, label: &'static str) -> QueryResult<Self::Value<'a>> {
        match value.identifier_view() {
            IdentifierView::Int(integer) => Ok(NodeIndexView::from(IdentifierView::Int(-integer))),
            IdentifierView::String(_) => Err(Failure::new(
                NonNumericValue::new(NodeIndex::from(value)),
                label,
            )),
        }
    }
}

impl ValueNegate for IndexValue<AttributeName> {
    fn negate<'a>(value: Self::Value<'a>, label: &'static str) -> QueryResult<Self::Value<'a>> {
        match value.identifier_view() {
            IdentifierView::Int(integer) => {
                Ok(AttributeNameView::from(IdentifierView::Int(-integer)))
            }
            IdentifierView::String(_) => Err(Failure::new(
                NonNumericValue::new(AttributeName::from(value)),
                label,
            )),
        }
    }
}

impl ValueNegate for IndexValue<Value> {
    fn negate<'a>(value: Self::Value<'a>, label: &'static str) -> QueryResult<Self::Value<'a>> {
        match value {
            Value::Int(integer) => Ok(Value::Int(-integer)),
            Value::Float(float) => Ok(Value::Float(-float)),
            Value::Duration(duration) => Ok(Value::Duration(-duration)),
            value => Err(Failure::new(NonNumericValue::new(value), label)),
        }
    }
}
