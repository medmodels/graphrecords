use crate::{AttributeName, Failure, IndexValue, Positional, QueryResult, Scalar, ValueDomain};
use graphrecords_core::graphrecord::{EdgeIndex, GraphRecordValue, NodeIndex, datatypes::Pow};

pub trait ValuePower: ValueDomain {
    fn power<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>>;
}

impl ValuePower for Scalar {
    fn power<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        value
            .pow(argument)
            .map_err(|error| Failure::new(label, error))
    }
}

impl ValuePower for AttributeName {
    fn power<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        value
            .pow(argument)
            .map_err(|error| Failure::new(label, error))
    }
}

impl ValuePower for IndexValue<Positional> {
    fn power<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        Ok(value.pow(argument as u32))
    }
}

impl ValuePower for IndexValue<NodeIndex> {
    fn power<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        value
            .pow(argument)
            .map_err(|error| Failure::new(label, error))
    }
}

impl ValuePower for IndexValue<AttributeName> {
    fn power<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        value
            .pow(argument)
            .map_err(|error| Failure::new(label, error))
    }
}

impl ValuePower for IndexValue<EdgeIndex> {
    fn power<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        Ok(value.pow(argument))
    }
}

impl ValuePower for IndexValue<GraphRecordValue> {
    fn power<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        value
            .pow(argument)
            .map_err(|error| Failure::new(label, error))
    }
}
