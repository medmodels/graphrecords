use crate::{
    AttributeName, Failure, IndexValue, Positional, QueryResult, Scalar, ValueDomain,
    error::arithmetic::ModuloByZero,
};
use graphrecords_core::graphrecord::{
    EdgeIndex, GraphRecordAttribute, GraphRecordValue, NodeIndex, datatypes::Mod,
};

pub trait ValueModulo: ValueDomain {
    fn modulo<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>>;
}

const fn is_attribute_modulo_by_zero(
    value: &GraphRecordAttribute,
    modulus: &GraphRecordAttribute,
) -> bool {
    matches!(
        (value, modulus),
        (GraphRecordAttribute::Int(_), GraphRecordAttribute::Int(0))
    )
}

fn is_graphrecord_value_modulo_by_zero(
    value: &GraphRecordValue,
    modulus: &GraphRecordValue,
) -> bool {
    match (value, modulus) {
        (GraphRecordValue::Int(_) | GraphRecordValue::Float(_), GraphRecordValue::Int(0)) => true,
        (
            GraphRecordValue::Int(_) | GraphRecordValue::Float(_),
            GraphRecordValue::Float(modulus),
        ) => *modulus == 0.0,
        _ => false,
    }
}

impl ValueModulo for Scalar {
    fn modulo<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        if is_graphrecord_value_modulo_by_zero(&value, &argument) {
            return Err(Failure::new(label, ModuloByZero));
        }

        value
            .r#mod(argument)
            .map_err(|error| Failure::new(label, error))
    }
}

impl ValueModulo for AttributeName {
    fn modulo<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        if is_attribute_modulo_by_zero(&value, &argument) {
            return Err(Failure::new(label, ModuloByZero));
        }

        value
            .r#mod(argument)
            .map_err(|error| Failure::new(label, error))
    }
}

impl ValueModulo for IndexValue<Positional> {
    fn modulo<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        if argument == 0 {
            return Err(Failure::new(label, ModuloByZero));
        }

        Ok(value % argument)
    }
}

impl ValueModulo for IndexValue<NodeIndex> {
    fn modulo<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        if is_attribute_modulo_by_zero(&value, &argument) {
            return Err(Failure::new(label, ModuloByZero));
        }

        value
            .r#mod(argument)
            .map_err(|error| Failure::new(label, error))
    }
}

impl ValueModulo for IndexValue<AttributeName> {
    fn modulo<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        if is_attribute_modulo_by_zero(&value, &argument) {
            return Err(Failure::new(label, ModuloByZero));
        }

        value
            .r#mod(argument)
            .map_err(|error| Failure::new(label, error))
    }
}

impl ValueModulo for IndexValue<EdgeIndex> {
    fn modulo<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        if argument == 0 {
            return Err(Failure::new(label, ModuloByZero));
        }

        Ok(value % argument)
    }
}

impl ValueModulo for IndexValue<GraphRecordValue> {
    fn modulo<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        if is_graphrecord_value_modulo_by_zero(&value, &argument) {
            return Err(Failure::new(label, ModuloByZero));
        }

        value
            .r#mod(argument)
            .map_err(|error| Failure::new(label, error))
    }
}
