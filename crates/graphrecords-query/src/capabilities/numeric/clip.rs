use crate::{
    AttributeName, Failure, IndexValue, Positional, QueryResult, Scalar, ValueDomain,
    error::{
        comparison::IncomparableValues,
        numeric::{InvalidClipBounds, NonNumericValue},
    },
};
use graphrecords_core::graphrecord::{
    EdgeIndex, GraphRecordAttribute, GraphRecordValue, NodeIndex,
};
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

fn clip_graphrecord_attribute(
    label: &'static str,
    value: GraphRecordAttribute,
    lower: GraphRecordAttribute,
    upper: GraphRecordAttribute,
) -> QueryResult<GraphRecordAttribute> {
    let value = match value {
        GraphRecordAttribute::Int(_) => value,
        value @ GraphRecordAttribute::String(_) => {
            return Err(Failure::new(label, NonNumericValue::new(value)));
        }
    };
    let lower = match lower {
        GraphRecordAttribute::Int(_) => lower,
        lower @ GraphRecordAttribute::String(_) => {
            return Err(Failure::new(label, NonNumericValue::new(lower)));
        }
    };
    let upper = match upper {
        GraphRecordAttribute::Int(_) => upper,
        upper @ GraphRecordAttribute::String(_) => {
            return Err(Failure::new(label, NonNumericValue::new(upper)));
        }
    };

    clip_ordered(label, value, lower, upper)
}

fn clip_graphrecord_value(
    label: &'static str,
    value: GraphRecordValue,
    lower: GraphRecordValue,
    upper: GraphRecordValue,
) -> QueryResult<GraphRecordValue> {
    let value = match value {
        GraphRecordValue::Int(_)
        | GraphRecordValue::Float(_)
        | GraphRecordValue::DateTime(_)
        | GraphRecordValue::Duration(_) => value,
        value => return Err(Failure::new(label, NonNumericValue::new(value))),
    };
    let lower = match lower {
        GraphRecordValue::Int(_)
        | GraphRecordValue::Float(_)
        | GraphRecordValue::DateTime(_)
        | GraphRecordValue::Duration(_) => lower,
        lower => return Err(Failure::new(label, NonNumericValue::new(lower))),
    };
    let upper = match upper {
        GraphRecordValue::Int(_)
        | GraphRecordValue::Float(_)
        | GraphRecordValue::DateTime(_)
        | GraphRecordValue::Duration(_) => upper,
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
        clip_graphrecord_value(label, value, lower, upper)
    }
}

impl ValueClip for AttributeName {
    fn clip<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        lower: Self::Value<'a>,
        upper: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        clip_graphrecord_attribute(label, value, lower, upper)
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
        clip_graphrecord_attribute(label, value, lower, upper)
    }
}

impl ValueClip for IndexValue<AttributeName> {
    fn clip<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        lower: Self::Value<'a>,
        upper: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        clip_graphrecord_attribute(label, value, lower, upper)
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

impl ValueClip for IndexValue<GraphRecordValue> {
    fn clip<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        lower: Self::Value<'a>,
        upper: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        clip_graphrecord_value(label, value, lower, upper)
    }
}
