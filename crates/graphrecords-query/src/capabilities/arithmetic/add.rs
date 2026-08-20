use crate::{
    Failure, IndexValue, Positional, QueryResult, Scalar, ValueDomain,
    capabilities::{identifier_into_view, value_into_view},
};
use graphrecords_core::graphrecord::{
    AttributeName, Identifier, NodeIndex, NodeIndexView, Value, datatypes::AttributeNameView,
};

pub trait ValueAdd: ValueDomain {
    fn add<'a>(
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>>;
}

impl ValueAdd for Scalar {
    fn add<'a>(
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        (Value::from(value) + Value::from(argument))
            .map(value_into_view)
            .map_err(|error| Failure::new(error, label))
    }
}

impl ValueAdd for AttributeName {
    fn add<'a>(
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        (Self::from(value) + Self::from(argument))
            .map(|result| AttributeNameView::from(identifier_into_view(Identifier::from(result))))
            .map_err(|error| Failure::new(error, label))
    }
}

impl ValueAdd for IndexValue<Positional> {
    fn add<'a>(
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        Ok(value + argument)
    }
}

impl ValueAdd for IndexValue<NodeIndex> {
    fn add<'a>(
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        (NodeIndex::from(value) + NodeIndex::from(argument))
            .map(|result| NodeIndexView::from(identifier_into_view(Identifier::from(result))))
            .map_err(|error| Failure::new(error, label))
    }
}

impl ValueAdd for IndexValue<AttributeName> {
    fn add<'a>(
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        (AttributeName::from(value) + AttributeName::from(argument))
            .map(|result| AttributeNameView::from(identifier_into_view(Identifier::from(result))))
            .map_err(|error| Failure::new(error, label))
    }
}

impl ValueAdd for IndexValue<Value> {
    fn add<'a>(
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        (value + argument).map_err(|error| Failure::new(error, label))
    }
}
