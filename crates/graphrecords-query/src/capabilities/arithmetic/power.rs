use crate::{
    Failure, IndexValue, Positional, QueryResult, Scalar, ValueDomain,
    capabilities::{identifier_into_view, value_into_view},
};
use graphrecords_core::graphrecord::{
    AttributeName, Identifier, NodeIndex, NodeIndexView, Value,
    datatypes::{AttributeNameView, Pow},
};

pub trait ValuePower: ValueDomain {
    fn power<'a>(
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>>;
}

impl ValuePower for Scalar {
    fn power<'a>(
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        Value::from(value)
            .pow(Value::from(argument))
            .map(value_into_view)
            .map_err(|error| Failure::new(error, label))
    }
}

impl ValuePower for AttributeName {
    fn power<'a>(
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        Self::from(value)
            .pow(Self::from(argument))
            .map(|result| AttributeNameView::from(identifier_into_view(Identifier::from(result))))
            .map_err(|error| Failure::new(error, label))
    }
}

impl ValuePower for IndexValue<Positional> {
    fn power<'a>(
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        Ok(value.pow(argument as u32))
    }
}

impl ValuePower for IndexValue<NodeIndex> {
    fn power<'a>(
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        NodeIndex::from(value)
            .pow(NodeIndex::from(argument))
            .map(|result| NodeIndexView::from(identifier_into_view(Identifier::from(result))))
            .map_err(|error| Failure::new(error, label))
    }
}

impl ValuePower for IndexValue<AttributeName> {
    fn power<'a>(
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        AttributeName::from(value)
            .pow(AttributeName::from(argument))
            .map(|result| AttributeNameView::from(identifier_into_view(Identifier::from(result))))
            .map_err(|error| Failure::new(error, label))
    }
}

impl ValuePower for IndexValue<Value> {
    fn power<'a>(
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        value
            .pow(argument)
            .map_err(|error| Failure::new(error, label))
    }
}
