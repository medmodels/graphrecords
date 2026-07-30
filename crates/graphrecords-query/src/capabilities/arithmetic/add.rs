use crate::{AttributeName, Failure, IndexValue, Positional, QueryResult, Scalar, ValueType};
use graphrecords_core::graphrecord::{EdgeIndex, GraphRecordValue, NodeIndex};

pub trait ValueAdd: ValueType {
    fn add<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>>;
}

impl ValueAdd for Scalar {
    fn add<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        (value + argument).map_err(|error| Failure::new(label, error))
    }
}

impl ValueAdd for AttributeName {
    fn add<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        (value + argument).map_err(|error| Failure::new(label, error))
    }
}

impl ValueAdd for IndexValue<Positional> {
    fn add<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        Ok(value + argument)
    }
}

impl ValueAdd for IndexValue<NodeIndex> {
    fn add<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        (value + argument).map_err(|error| Failure::new(label, error))
    }
}

impl ValueAdd for IndexValue<AttributeName> {
    fn add<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        (value + argument).map_err(|error| Failure::new(label, error))
    }
}

impl ValueAdd for IndexValue<EdgeIndex> {
    fn add<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        Ok(value + argument)
    }
}

impl ValueAdd for IndexValue<GraphRecordValue> {
    fn add<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        (value + argument).map_err(|error| Failure::new(label, error))
    }
}
