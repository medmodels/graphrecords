use crate::{
    Failure, IndexValue, Positional, QueryResult, Scalar, ValueDomain,
    error::arithmetic::ModuloByZero,
};
use graphrecords_core::graphrecord::{
    AttributeName, EdgeIndex, Identifier, NodeIndex, Value, datatypes::Mod,
};

pub trait ValueModulo: ValueDomain {
    fn modulo<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>>;
}

const fn is_identifier_modulo_by_zero(value: &Identifier, modulus: &Identifier) -> bool {
    matches!((value, modulus), (Identifier::Int(_), Identifier::Int(0)))
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
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        if is_value_modulo_by_zero(&value, &argument) {
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
        if is_identifier_modulo_by_zero(value.identifier(), argument.identifier()) {
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
        if is_identifier_modulo_by_zero(value.identifier(), argument.identifier()) {
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
        if is_identifier_modulo_by_zero(value.identifier(), argument.identifier()) {
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

impl ValueModulo for IndexValue<Value> {
    fn modulo<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        if is_value_modulo_by_zero(&value, &argument) {
            return Err(Failure::new(label, ModuloByZero));
        }

        value
            .r#mod(argument)
            .map_err(|error| Failure::new(label, error))
    }
}
