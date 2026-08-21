use crate::{Failure, IndexValue, QueryResult, Scalar, ValueDomain, error::string::NonStringValue};
use graphrecords_core::graphrecord::{
    AttributeName, GroupIndex, GroupIndexView, IdentifierView, NodeIndex, NodeIndexView, Value,
    ValueView, datatypes::AttributeNameView,
};

pub trait ValueString: ValueDomain {
    fn as_str<'a>(value: &'a Self::Value<'_>, label: &'static str) -> QueryResult<&'a str>;

    fn with_string<'a>(original: &Self::Value<'_>, string: String) -> Self::Value<'a>;
}

fn string_from_value<'a>(value: &'a ValueView<'_>, label: &'static str) -> QueryResult<&'a str> {
    match value {
        ValueView::String(value) => Ok(value.as_ref()),
        value => Err(Failure::new(
            NonStringValue::new(Value::from(value.clone())),
            label,
        )),
    }
}

fn string_from_value_owned<'a>(value: &'a Value, label: &'static str) -> QueryResult<&'a str> {
    match value {
        Value::String(value) => Ok(value.as_str()),
        value => Err(Failure::new(NonStringValue::new(value.clone()), label)),
    }
}

impl ValueString for Scalar {
    fn as_str<'a>(value: &'a Self::Value<'_>, label: &'static str) -> QueryResult<&'a str> {
        string_from_value(value, label)
    }

    fn with_string<'a>(_original: &Self::Value<'_>, string: String) -> Self::Value<'a> {
        ValueView::String(string.into())
    }
}

impl ValueString for IndexValue<Value> {
    fn as_str<'a>(value: &'a Self::Value<'_>, label: &'static str) -> QueryResult<&'a str> {
        string_from_value_owned(value, label)
    }

    fn with_string<'a>(_original: &Self::Value<'_>, string: String) -> Self::Value<'a> {
        Value::String(string)
    }
}

impl ValueString for AttributeName {
    fn as_str<'a>(value: &'a Self::Value<'_>, label: &'static str) -> QueryResult<&'a str> {
        match value.identifier_view() {
            IdentifierView::String(cow) => Ok(cow.as_ref()),
            IdentifierView::Int(_) => Err(Failure::new(
                NonStringValue::new(Self::from(value.clone())),
                label,
            )),
        }
    }

    fn with_string<'a>(_original: &Self::Value<'_>, string: String) -> Self::Value<'a> {
        AttributeNameView::from(IdentifierView::String(string.into()))
    }
}

impl ValueString for IndexValue<NodeIndex> {
    fn as_str<'a>(value: &'a Self::Value<'_>, label: &'static str) -> QueryResult<&'a str> {
        match value.identifier_view() {
            IdentifierView::String(cow) => Ok(cow.as_ref()),
            IdentifierView::Int(_) => Err(Failure::new(
                NonStringValue::new(NodeIndex::from(value.clone())),
                label,
            )),
        }
    }

    fn with_string<'a>(_original: &Self::Value<'_>, string: String) -> Self::Value<'a> {
        NodeIndexView::from(IdentifierView::String(string.into()))
    }
}

impl ValueString for IndexValue<GroupIndex> {
    fn as_str<'a>(value: &'a Self::Value<'_>, label: &'static str) -> QueryResult<&'a str> {
        match value.identifier_view() {
            IdentifierView::String(cow) => Ok(cow.as_ref()),
            IdentifierView::Int(_) => Err(Failure::new(
                NonStringValue::new(GroupIndex::from(value.clone())),
                label,
            )),
        }
    }

    fn with_string<'a>(_original: &Self::Value<'_>, string: String) -> Self::Value<'a> {
        GroupIndexView::from(IdentifierView::String(string.into()))
    }
}

impl ValueString for IndexValue<AttributeName> {
    fn as_str<'a>(value: &'a Self::Value<'_>, label: &'static str) -> QueryResult<&'a str> {
        match value.identifier_view() {
            IdentifierView::String(cow) => Ok(cow.as_ref()),
            IdentifierView::Int(_) => Err(Failure::new(
                NonStringValue::new(AttributeName::from(value.clone())),
                label,
            )),
        }
    }

    fn with_string<'a>(_original: &Self::Value<'_>, string: String) -> Self::Value<'a> {
        AttributeNameView::from(IdentifierView::String(string.into()))
    }
}
