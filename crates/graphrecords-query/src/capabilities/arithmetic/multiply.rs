use crate::{AttributeName, Failure, IndexValue, Positional, QueryResult, Scalar, ValueDomain};
use graphrecords_core::graphrecord::{EdgeIndex, GraphRecordValue, NodeIndex};

pub trait ValueMultiply: ValueDomain {
    fn multiply<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>>;
}

impl ValueMultiply for Scalar {
    fn multiply<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        (value * argument).map_err(|error| Failure::new(label, error))
    }
}

impl ValueMultiply for AttributeName {
    fn multiply<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        (value * argument).map_err(|error| Failure::new(label, error))
    }
}

impl ValueMultiply for IndexValue<Positional> {
    fn multiply<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        Ok(value * argument)
    }
}

impl ValueMultiply for IndexValue<NodeIndex> {
    fn multiply<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        (value * argument).map_err(|error| Failure::new(label, error))
    }
}

impl ValueMultiply for IndexValue<AttributeName> {
    fn multiply<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        (value * argument).map_err(|error| Failure::new(label, error))
    }
}

impl ValueMultiply for IndexValue<EdgeIndex> {
    fn multiply<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        Ok(value * argument)
    }
}

impl ValueMultiply for IndexValue<GraphRecordValue> {
    fn multiply<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        (value * argument).map_err(|error| Failure::new(label, error))
    }
}
