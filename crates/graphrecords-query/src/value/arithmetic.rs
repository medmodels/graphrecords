use crate::{
    AttributeName, Diagnostic, Failure, IndexValue, Positional, QueryResult, Scalar, ValueType,
};
use graphrecords_core::graphrecord::{
    EdgeIndex, GraphRecordAttribute, GraphRecordValue, NodeIndex,
    datatypes::{Mod, Pow},
};
use std::{
    error::Error,
    fmt::{self, Display, Formatter},
};

pub trait ValueAdd: ValueType {
    fn add<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>>;
}

pub trait ValueDivide: ValueType {
    fn divide<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>>;
}

pub trait ValueModulo: ValueType {
    fn modulo<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>>;
}

pub trait ValueMultiply: ValueType {
    fn multiply<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>>;
}

pub trait ValuePower: ValueType {
    fn power<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>>;
}

pub trait ValueSubtract: ValueType {
    fn subtract<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>>;
}

#[derive(Debug)]
pub struct DivisionByZero {
    pub dividend: GraphRecordValue,
}

impl Display for DivisionByZero {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "cannot divide `{}` by zero", self.dividend)
    }
}

impl Error for DivisionByZero {}

impl Diagnostic for DivisionByZero {
    fn name() -> &'static str {
        "DivisionByZero"
    }
}

#[derive(Debug)]
pub struct ModuloByZero;

impl Display for ModuloByZero {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("cannot calculate a remainder with a zero modulus")
    }
}

impl Error for ModuloByZero {}

impl Diagnostic for ModuloByZero {
    fn name() -> &'static str {
        "ModuloByZero"
    }
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

fn is_division_by_zero(dividend: &GraphRecordValue, divisor: &GraphRecordValue) -> bool {
    match (dividend, divisor) {
        (
            GraphRecordValue::Int(_) | GraphRecordValue::Float(_) | GraphRecordValue::Duration(_),
            GraphRecordValue::Int(0),
        ) => true,
        (
            GraphRecordValue::Int(_) | GraphRecordValue::Float(_),
            GraphRecordValue::Float(divisor),
        ) => *divisor == 0.0,
        _ => false,
    }
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

impl ValueDivide for Scalar {
    fn divide<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        if is_division_by_zero(&value, &argument) {
            return Err(Failure::new(label, DivisionByZero { dividend: value }));
        }

        (value / argument).map_err(|error| Failure::new(label, error))
    }
}

impl ValueDivide for IndexValue<GraphRecordValue> {
    fn divide<'a>(
        label: &'static str,
        value: Self::Value<'a>,
        argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        if is_division_by_zero(&value, &argument) {
            return Err(Failure::new(label, DivisionByZero { dividend: value }));
        }

        (value / argument).map_err(|error| Failure::new(label, error))
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
