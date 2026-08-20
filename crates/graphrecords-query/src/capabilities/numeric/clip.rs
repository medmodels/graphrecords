use crate::{
    Failure, IndexValue, Positional, QueryResult, Scalar, ValueDomain,
    capabilities::{identifier_into_view, value_into_view},
    error::{
        comparison::IncomparableValues,
        numeric::{InvalidClipBounds, NonNumericValue},
    },
};
use graphrecords_core::graphrecord::{
    AttributeName, Identifier, IdentifierView, NodeIndex, NodeIndexView, Value,
    datatypes::AttributeNameView,
};
use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
};

pub trait ValueClip: ValueDomain {
    fn clip<'a>(
        value: Self::Value<'a>,
        lower: Self::Value<'a>,
        upper: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>>;
}

fn clip_ordered<T>(value: T, lower: T, upper: T, label: &'static str) -> QueryResult<T>
where
    T: Debug + Display + PartialOrd + Send + Sync + 'static,
{
    match lower.partial_cmp(&upper) {
        Some(Ordering::Greater) => {
            return Err(Failure::new(InvalidClipBounds::new(lower, upper), label));
        }
        None => {
            return Err(Failure::new(IncomparableValues::new(lower, upper), label));
        }
        Some(Ordering::Less | Ordering::Equal) => {}
    }

    match value.partial_cmp(&lower) {
        Some(Ordering::Less) => return Ok(lower),
        None => {
            return Err(Failure::new(IncomparableValues::new(value, lower), label));
        }
        Some(Ordering::Equal | Ordering::Greater) => {}
    }

    match value.partial_cmp(&upper) {
        Some(Ordering::Greater) => Ok(upper),
        Some(Ordering::Less | Ordering::Equal) => Ok(value),
        None => Err(Failure::new(IncomparableValues::new(value, upper), label)),
    }
}

fn clip_value(value: Value, lower: Value, upper: Value, label: &'static str) -> QueryResult<Value> {
    let value = match value {
        Value::Int(_) | Value::Float(_) | Value::DateTime(_) | Value::Duration(_) => value,
        value => return Err(Failure::new(NonNumericValue::new(value), label)),
    };
    let lower = match lower {
        Value::Int(_) | Value::Float(_) | Value::DateTime(_) | Value::Duration(_) => lower,
        lower => return Err(Failure::new(NonNumericValue::new(lower), label)),
    };
    let upper = match upper {
        Value::Int(_) | Value::Float(_) | Value::DateTime(_) | Value::Duration(_) => upper,
        upper => return Err(Failure::new(NonNumericValue::new(upper), label)),
    };

    clip_ordered(value, lower, upper, label)
}

impl ValueClip for Scalar {
    fn clip<'a>(
        value: Self::Value<'a>,
        lower: Self::Value<'a>,
        upper: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        clip_value(
            Value::from(value),
            Value::from(lower),
            Value::from(upper),
            label,
        )
        .map(value_into_view)
    }
}

impl ValueClip for AttributeName {
    fn clip<'a>(
        value: Self::Value<'a>,
        lower: Self::Value<'a>,
        upper: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        if let IdentifierView::String(_) = value.identifier_view() {
            return Err(Failure::new(NonNumericValue::new(Self::from(value)), label));
        }
        if let IdentifierView::String(_) = lower.identifier_view() {
            return Err(Failure::new(NonNumericValue::new(Self::from(lower)), label));
        }
        if let IdentifierView::String(_) = upper.identifier_view() {
            return Err(Failure::new(NonNumericValue::new(Self::from(upper)), label));
        }

        clip_ordered(
            Self::from(value),
            Self::from(lower),
            Self::from(upper),
            label,
        )
        .map(|result| AttributeNameView::from(identifier_into_view(Identifier::from(result))))
    }
}

impl ValueClip for IndexValue<Positional> {
    fn clip<'a>(
        value: Self::Value<'a>,
        lower: Self::Value<'a>,
        upper: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        clip_ordered(value, lower, upper, label)
    }
}

impl ValueClip for IndexValue<NodeIndex> {
    fn clip<'a>(
        value: Self::Value<'a>,
        lower: Self::Value<'a>,
        upper: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        if let IdentifierView::String(_) = value.identifier_view() {
            return Err(Failure::new(
                NonNumericValue::new(NodeIndex::from(value)),
                label,
            ));
        }
        if let IdentifierView::String(_) = lower.identifier_view() {
            return Err(Failure::new(
                NonNumericValue::new(NodeIndex::from(lower)),
                label,
            ));
        }
        if let IdentifierView::String(_) = upper.identifier_view() {
            return Err(Failure::new(
                NonNumericValue::new(NodeIndex::from(upper)),
                label,
            ));
        }

        clip_ordered(
            NodeIndex::from(value),
            NodeIndex::from(lower),
            NodeIndex::from(upper),
            label,
        )
        .map(|result| NodeIndexView::from(identifier_into_view(Identifier::from(result))))
    }
}

impl ValueClip for IndexValue<AttributeName> {
    fn clip<'a>(
        value: Self::Value<'a>,
        lower: Self::Value<'a>,
        upper: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        if let IdentifierView::String(_) = value.identifier_view() {
            return Err(Failure::new(
                NonNumericValue::new(AttributeName::from(value)),
                label,
            ));
        }
        if let IdentifierView::String(_) = lower.identifier_view() {
            return Err(Failure::new(
                NonNumericValue::new(AttributeName::from(lower)),
                label,
            ));
        }
        if let IdentifierView::String(_) = upper.identifier_view() {
            return Err(Failure::new(
                NonNumericValue::new(AttributeName::from(upper)),
                label,
            ));
        }

        clip_ordered(
            AttributeName::from(value),
            AttributeName::from(lower),
            AttributeName::from(upper),
            label,
        )
        .map(|result| AttributeNameView::from(identifier_into_view(Identifier::from(result))))
    }
}

impl ValueClip for IndexValue<Value> {
    fn clip<'a>(
        value: Self::Value<'a>,
        lower: Self::Value<'a>,
        upper: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        clip_value(value, lower, upper, label)
    }
}
