use crate::{
    Failure, IndexValue, Positional, QueryResult, Scalar, ValueDomain,
    capabilities::{identifier_into_view, value_into_view},
    error::arithmetic::ModuloByZero,
};
use graphrecords_core::graphrecord::{
    AttributeName, Identifier, IdentifierView, NodeIndex, NodeIndexView, Value,
    datatypes::{AttributeNameView, Mod},
};

pub trait ValueModulo: ValueDomain {
    fn modulo<'a>(
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>>;
}

const fn is_identifier_modulo_by_zero(
    value: &IdentifierView<'_>,
    modulus: &IdentifierView<'_>,
) -> bool {
    matches!(
        (value, modulus),
        (IdentifierView::Int(_), IdentifierView::Int(0))
    )
}

fn is_value_modulo_by_zero(value: &Value, modulus: &Value) -> bool {
    match (value, modulus) {
        (Value::Int(_) | Value::Float(_), Value::Int(0)) => true,
        (Value::Int(_) | Value::Float(_), Value::Float(modulus)) => *modulus == 0.0,
        _ => false,
    }
}

impl ValueModulo for Scalar {
    fn modulo<'a>(
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        let value = Value::from(value);
        let argument = Value::from(argument);

        if is_value_modulo_by_zero(&value, &argument) {
            return Err(Failure::new(ModuloByZero, label));
        }

        value
            .r#mod(argument)
            .map(value_into_view)
            .map_err(|error| Failure::new(error, label))
    }
}

impl ValueModulo for AttributeName {
    fn modulo<'a>(
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        if is_identifier_modulo_by_zero(value.identifier_view(), argument.identifier_view()) {
            return Err(Failure::new(ModuloByZero, label));
        }

        Self::from(value)
            .r#mod(Self::from(argument))
            .map(|result| AttributeNameView::from(identifier_into_view(Identifier::from(result))))
            .map_err(|error| Failure::new(error, label))
    }
}

impl ValueModulo for IndexValue<Positional> {
    fn modulo<'a>(
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        if argument == 0 {
            return Err(Failure::new(ModuloByZero, label));
        }

        Ok(value % argument)
    }
}

impl ValueModulo for IndexValue<NodeIndex> {
    fn modulo<'a>(
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        if is_identifier_modulo_by_zero(value.identifier_view(), argument.identifier_view()) {
            return Err(Failure::new(ModuloByZero, label));
        }

        NodeIndex::from(value)
            .r#mod(NodeIndex::from(argument))
            .map(|result| NodeIndexView::from(identifier_into_view(Identifier::from(result))))
            .map_err(|error| Failure::new(error, label))
    }
}

impl ValueModulo for IndexValue<AttributeName> {
    fn modulo<'a>(
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        if is_identifier_modulo_by_zero(value.identifier_view(), argument.identifier_view()) {
            return Err(Failure::new(ModuloByZero, label));
        }

        AttributeName::from(value)
            .r#mod(AttributeName::from(argument))
            .map(|result| AttributeNameView::from(identifier_into_view(Identifier::from(result))))
            .map_err(|error| Failure::new(error, label))
    }
}

impl ValueModulo for IndexValue<Value> {
    fn modulo<'a>(
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        if is_value_modulo_by_zero(&value, &argument) {
            return Err(Failure::new(ModuloByZero, label));
        }

        value
            .r#mod(argument)
            .map_err(|error| Failure::new(error, label))
    }
}
