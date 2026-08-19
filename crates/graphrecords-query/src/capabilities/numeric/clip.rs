use crate::{
    Failure, IndexValue, Positional, QueryResult, Scalar, ValueDomain,
    error::{
        comparison::IncomparableValues,
        numeric::{InvalidClipBounds, NonNumericValue},
    },
};
use graphrecords_core::graphrecord::{AttributeName, EdgeIndex, Identifier, NodeIndex, Value};
use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
};

pub trait ValueClip: ValueDomain {
    fn clip<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        lower: Self::Value<'a>,
        upper: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>>;
}

fn clip_ordered<T>(label: &'static str, value: T, lower: T, upper: T) -> QueryResult<T>
where
    T: Debug + Display + PartialOrd + Send + Sync + 'static,
{
    match lower.partial_cmp(&upper) {
        Some(Ordering::Greater) => {
            return Err(Failure::new(label, InvalidClipBounds::new(lower, upper)));
        }
        None => {
            return Err(Failure::new(label, IncomparableValues::new(lower, upper)));
        }
        Some(Ordering::Less | Ordering::Equal) => {}
    }

    match value.partial_cmp(&lower) {
        Some(Ordering::Less) => return Ok(lower),
        None => {
            return Err(Failure::new(label, IncomparableValues::new(value, lower)));
        }
        Some(Ordering::Equal | Ordering::Greater) => {}
    }

    match value.partial_cmp(&upper) {
        Some(Ordering::Greater) => Ok(upper),
        Some(Ordering::Less | Ordering::Equal) => Ok(value),
        None => Err(Failure::new(label, IncomparableValues::new(value, upper))),
    }
}

fn clip_value(label: &'static str, value: Value, lower: Value, upper: Value) -> QueryResult<Value> {
    let value = match value {
        Value::Int(_) | Value::Float(_) | Value::DateTime(_) | Value::Duration(_) => value,
        value => return Err(Failure::new(label, NonNumericValue::new(value))),
    };
    let lower = match lower {
        Value::Int(_) | Value::Float(_) | Value::DateTime(_) | Value::Duration(_) => lower,
        lower => return Err(Failure::new(label, NonNumericValue::new(lower))),
    };
    let upper = match upper {
        Value::Int(_) | Value::Float(_) | Value::DateTime(_) | Value::Duration(_) => upper,
        upper => return Err(Failure::new(label, NonNumericValue::new(upper))),
    };

    clip_ordered(label, value, lower, upper)
}

impl ValueClip for Scalar {
    fn clip<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        lower: Self::Value<'a>,
        upper: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        clip_value(label, value, lower, upper)
    }
}

impl ValueClip for AttributeName {
    fn clip<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        lower: Self::Value<'a>,
        upper: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        if let Identifier::String(_) = value.identifier() {
            return Err(Failure::new(label, NonNumericValue::new(value)));
        }
        if let Identifier::String(_) = lower.identifier() {
            return Err(Failure::new(label, NonNumericValue::new(lower)));
        }
        if let Identifier::String(_) = upper.identifier() {
            return Err(Failure::new(label, NonNumericValue::new(upper)));
        }

        clip_ordered(label, value, lower, upper)
    }
}

impl ValueClip for IndexValue<Positional> {
    fn clip<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        lower: Self::Value<'a>,
        upper: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        clip_ordered(label, value, lower, upper)
    }
}

impl ValueClip for IndexValue<NodeIndex> {
    fn clip<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        lower: Self::Value<'a>,
        upper: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        if let Identifier::String(_) = value.identifier() {
            return Err(Failure::new(label, NonNumericValue::new(value)));
        }
        if let Identifier::String(_) = lower.identifier() {
            return Err(Failure::new(label, NonNumericValue::new(lower)));
        }
        if let Identifier::String(_) = upper.identifier() {
            return Err(Failure::new(label, NonNumericValue::new(upper)));
        }

        clip_ordered(label, value, lower, upper)
    }
}

impl ValueClip for IndexValue<AttributeName> {
    fn clip<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        lower: Self::Value<'a>,
        upper: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        if let Identifier::String(_) = value.identifier() {
            return Err(Failure::new(label, NonNumericValue::new(value)));
        }
        if let Identifier::String(_) = lower.identifier() {
            return Err(Failure::new(label, NonNumericValue::new(lower)));
        }
        if let Identifier::String(_) = upper.identifier() {
            return Err(Failure::new(label, NonNumericValue::new(upper)));
        }

        clip_ordered(label, value, lower, upper)
    }
}

impl ValueClip for IndexValue<EdgeIndex> {
    fn clip<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        lower: Self::Value<'a>,
        upper: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        clip_ordered(label, value, lower, upper)
    }
}

impl ValueClip for IndexValue<Value> {
    fn clip<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        lower: Self::Value<'a>,
        upper: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        clip_value(label, value, lower, upper)
    }
}
