use crate::{
    Failure, IndexValue, QueryResult, Scalar, ValueDomain, error::numeric::NonNumericValue,
};
use chrono::TimeDelta;
use graphrecords_core::graphrecord::{
    AttributeName, IdentifierView, NodeIndex, NodeIndexView, Value, ValueView,
    datatypes::AttributeNameView,
};
use std::cmp::Ordering;

pub trait ValueSign: ValueDomain {
    fn sign<'a>(value: Self::Value<'a>, label: &'static str) -> QueryResult<Self::Value<'a>>;
}

impl ValueSign for Scalar {
    fn sign<'a>(value: Self::Value<'a>, label: &'static str) -> QueryResult<Self::Value<'a>> {
        match value {
            ValueView::Int(integer) => Ok(ValueView::Int(integer.signum())),
            ValueView::Float(float) => Ok(ValueView::Float(if float == 0.0 {
                0.0
            } else {
                float.signum()
            })),
            ValueView::Duration(duration) => {
                Ok(ValueView::Int(match duration.cmp(&TimeDelta::zero()) {
                    Ordering::Less => -1,
                    Ordering::Equal => 0,
                    Ordering::Greater => 1,
                }))
            }
            value => Err(Failure::new(
                NonNumericValue::new(Value::from(value)),
                label,
            )),
        }
    }
}

impl ValueSign for AttributeName {
    fn sign<'a>(value: Self::Value<'a>, label: &'static str) -> QueryResult<Self::Value<'a>> {
        match value.identifier_view() {
            IdentifierView::Int(integer) => Ok(AttributeNameView::from(IdentifierView::Int(
                integer.signum(),
            ))),
            IdentifierView::String(_) => {
                Err(Failure::new(NonNumericValue::new(Self::from(value)), label))
            }
        }
    }
}

impl ValueSign for IndexValue<NodeIndex> {
    fn sign<'a>(value: Self::Value<'a>, label: &'static str) -> QueryResult<Self::Value<'a>> {
        match value.identifier_view() {
            IdentifierView::Int(integer) => {
                Ok(NodeIndexView::from(IdentifierView::Int(integer.signum())))
            }
            IdentifierView::String(_) => Err(Failure::new(
                NonNumericValue::new(NodeIndex::from(value)),
                label,
            )),
        }
    }
}

impl ValueSign for IndexValue<AttributeName> {
    fn sign<'a>(value: Self::Value<'a>, label: &'static str) -> QueryResult<Self::Value<'a>> {
        match value.identifier_view() {
            IdentifierView::Int(integer) => Ok(AttributeNameView::from(IdentifierView::Int(
                integer.signum(),
            ))),
            IdentifierView::String(_) => Err(Failure::new(
                NonNumericValue::new(AttributeName::from(value)),
                label,
            )),
        }
    }
}

impl ValueSign for IndexValue<Value> {
    fn sign<'a>(value: Self::Value<'a>, label: &'static str) -> QueryResult<Self::Value<'a>> {
        match value {
            Value::Int(integer) => Ok(Value::Int(integer.signum())),
            Value::Float(float) => Ok(Value::Float(if float == 0.0 {
                0.0
            } else {
                float.signum()
            })),
            Value::Duration(duration) => Ok(Value::Int(match duration.cmp(&TimeDelta::zero()) {
                Ordering::Less => -1,
                Ordering::Equal => 0,
                Ordering::Greater => 1,
            })),
            value => Err(Failure::new(NonNumericValue::new(value), label)),
        }
    }
}
