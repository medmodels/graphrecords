use crate::{
    Failure, IndexValue, QueryResult, Scalar, ValueDomain, error::numeric::NonNumericValue,
};
use graphrecords_core::graphrecord::{
    AttributeName, IdentifierView, NodeIndex, NodeIndexView, Value, ValueView,
    datatypes::{Abs, AttributeNameView},
};

pub trait ValueAbsolute: ValueDomain {
    fn absolute<'a>(value: Self::Value<'a>, label: &'static str) -> QueryResult<Self::Value<'a>>;
}

impl ValueAbsolute for Scalar {
    fn absolute<'a>(value: Self::Value<'a>, label: &'static str) -> QueryResult<Self::Value<'a>> {
        match value {
            ValueView::Int(integer) => Ok(ValueView::Int(integer.abs())),
            ValueView::Float(float) => Ok(ValueView::Float(float.abs())),
            ValueView::Duration(duration) => Ok(ValueView::Duration(duration.abs())),
            value => Err(Failure::new(
                NonNumericValue::new(Value::from(value)),
                label,
            )),
        }
    }
}

impl ValueAbsolute for AttributeName {
    fn absolute<'a>(value: Self::Value<'a>, label: &'static str) -> QueryResult<Self::Value<'a>> {
        match value.identifier_view() {
            IdentifierView::Int(integer) => {
                Ok(AttributeNameView::from(IdentifierView::Int(integer.abs())))
            }
            IdentifierView::String(_) => {
                Err(Failure::new(NonNumericValue::new(Self::from(value)), label))
            }
        }
    }
}

impl ValueAbsolute for IndexValue<NodeIndex> {
    fn absolute<'a>(value: Self::Value<'a>, label: &'static str) -> QueryResult<Self::Value<'a>> {
        match value.identifier_view() {
            IdentifierView::Int(integer) => {
                Ok(NodeIndexView::from(IdentifierView::Int(integer.abs())))
            }
            IdentifierView::String(_) => Err(Failure::new(
                NonNumericValue::new(NodeIndex::from(value)),
                label,
            )),
        }
    }
}

impl ValueAbsolute for IndexValue<AttributeName> {
    fn absolute<'a>(value: Self::Value<'a>, label: &'static str) -> QueryResult<Self::Value<'a>> {
        match value.identifier_view() {
            IdentifierView::Int(integer) => {
                Ok(AttributeNameView::from(IdentifierView::Int(integer.abs())))
            }
            IdentifierView::String(_) => Err(Failure::new(
                NonNumericValue::new(AttributeName::from(value)),
                label,
            )),
        }
    }
}

impl ValueAbsolute for IndexValue<Value> {
    fn absolute<'a>(value: Self::Value<'a>, label: &'static str) -> QueryResult<Self::Value<'a>> {
        match value {
            Value::Int(_) | Value::Float(_) => Ok(value.abs()),
            Value::Duration(duration) => Ok(Value::Duration(duration.abs())),
            value => Err(Failure::new(NonNumericValue::new(value), label)),
        }
    }
}
