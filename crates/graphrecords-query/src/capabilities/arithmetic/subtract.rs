use crate::{AttributeName, Failure, IndexValue, Positional, QueryResult, Scalar, ValueType};
use graphrecords_core::graphrecord::{EdgeIndex, GraphRecordValue, NodeIndex};

pub trait ValueSubtract: ValueType {
    fn subtract<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>>;
}

impl ValueSubtract for Scalar {
    fn subtract<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        (value - argument).map_err(|error| Failure::new(label, error))
    }
}

impl ValueSubtract for AttributeName {
    fn subtract<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        (value - argument).map_err(|error| Failure::new(label, error))
    }
}

impl ValueSubtract for IndexValue<Positional> {
    fn subtract<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        Ok(value - argument)
    }
}

impl ValueSubtract for IndexValue<NodeIndex> {
    fn subtract<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        (value - argument).map_err(|error| Failure::new(label, error))
    }
}

impl ValueSubtract for IndexValue<AttributeName> {
    fn subtract<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        (value - argument).map_err(|error| Failure::new(label, error))
    }
}

impl ValueSubtract for IndexValue<EdgeIndex> {
    fn subtract<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        Ok(value - argument)
    }
}

impl ValueSubtract for IndexValue<GraphRecordValue> {
    fn subtract<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        (value - argument).map_err(|error| Failure::new(label, error))
    }
}
