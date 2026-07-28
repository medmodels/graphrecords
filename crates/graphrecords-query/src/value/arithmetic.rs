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
    fn add(
        label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned>;
}

pub trait ValueDivide: ValueType {
    fn divide(
        label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned>;
}

pub trait ValueModulo: ValueType {
    fn modulo(
        label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned>;
}

pub trait ValueMultiply: ValueType {
    fn multiply(
        label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned>;
}

pub trait ValuePower: ValueType {
    fn power(
        label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned>;
}

pub trait ValueSubtract: ValueType {
    fn subtract(
        label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned>;
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
    fn add(
        label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        (value + argument).map_err(|error| Failure::new(label, error))
    }
}

impl ValueAdd for AttributeName {
    fn add(
        label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        (value + argument).map_err(|error| Failure::new(label, error))
    }
}

impl ValueAdd for IndexValue<Positional> {
    fn add(
        _label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        Ok(value + argument)
    }
}

impl ValueAdd for IndexValue<NodeIndex> {
    fn add(
        label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        (value + argument).map_err(|error| Failure::new(label, error))
    }
}

impl ValueAdd for IndexValue<AttributeName> {
    fn add(
        label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        (value + argument).map_err(|error| Failure::new(label, error))
    }
}

impl ValueAdd for IndexValue<EdgeIndex> {
    fn add(
        _label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        Ok(value + argument)
    }
}

impl ValueAdd for IndexValue<GraphRecordValue> {
    fn add(
        label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        (value + argument).map_err(|error| Failure::new(label, error))
    }
}

impl ValueDivide for Scalar {
    fn divide(
        label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        if is_division_by_zero(&value, &argument) {
            return Err(Failure::new(label, DivisionByZero { dividend: value }));
        }

        (value / argument).map_err(|error| Failure::new(label, error))
    }
}

impl ValueDivide for IndexValue<GraphRecordValue> {
    fn divide(
        label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        if is_division_by_zero(&value, &argument) {
            return Err(Failure::new(label, DivisionByZero { dividend: value }));
        }

        (value / argument).map_err(|error| Failure::new(label, error))
    }
}

impl ValueModulo for Scalar {
    fn modulo(
        label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        if is_graphrecord_value_modulo_by_zero(&value, &argument) {
            return Err(Failure::new(label, ModuloByZero));
        }

        value
            .r#mod(argument)
            .map_err(|error| Failure::new(label, error))
    }
}

impl ValueModulo for AttributeName {
    fn modulo(
        label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        if is_attribute_modulo_by_zero(&value, &argument) {
            return Err(Failure::new(label, ModuloByZero));
        }

        value
            .r#mod(argument)
            .map_err(|error| Failure::new(label, error))
    }
}

impl ValueModulo for IndexValue<Positional> {
    fn modulo(
        label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        if argument == 0 {
            return Err(Failure::new(label, ModuloByZero));
        }

        Ok(value % argument)
    }
}

impl ValueModulo for IndexValue<NodeIndex> {
    fn modulo(
        label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        if is_attribute_modulo_by_zero(&value, &argument) {
            return Err(Failure::new(label, ModuloByZero));
        }

        value
            .r#mod(argument)
            .map_err(|error| Failure::new(label, error))
    }
}

impl ValueModulo for IndexValue<AttributeName> {
    fn modulo(
        label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        if is_attribute_modulo_by_zero(&value, &argument) {
            return Err(Failure::new(label, ModuloByZero));
        }

        value
            .r#mod(argument)
            .map_err(|error| Failure::new(label, error))
    }
}

impl ValueModulo for IndexValue<EdgeIndex> {
    fn modulo(
        label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        if argument == 0 {
            return Err(Failure::new(label, ModuloByZero));
        }

        Ok(value % argument)
    }
}

impl ValueModulo for IndexValue<GraphRecordValue> {
    fn modulo(
        label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        if is_graphrecord_value_modulo_by_zero(&value, &argument) {
            return Err(Failure::new(label, ModuloByZero));
        }

        value
            .r#mod(argument)
            .map_err(|error| Failure::new(label, error))
    }
}

impl ValueMultiply for Scalar {
    fn multiply(
        label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        (value * argument).map_err(|error| Failure::new(label, error))
    }
}

impl ValueMultiply for AttributeName {
    fn multiply(
        label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        (value * argument).map_err(|error| Failure::new(label, error))
    }
}

impl ValueMultiply for IndexValue<Positional> {
    fn multiply(
        _label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        Ok(value * argument)
    }
}

impl ValueMultiply for IndexValue<NodeIndex> {
    fn multiply(
        label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        (value * argument).map_err(|error| Failure::new(label, error))
    }
}

impl ValueMultiply for IndexValue<AttributeName> {
    fn multiply(
        label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        (value * argument).map_err(|error| Failure::new(label, error))
    }
}

impl ValueMultiply for IndexValue<EdgeIndex> {
    fn multiply(
        _label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        Ok(value * argument)
    }
}

impl ValueMultiply for IndexValue<GraphRecordValue> {
    fn multiply(
        label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        (value * argument).map_err(|error| Failure::new(label, error))
    }
}

impl ValuePower for Scalar {
    fn power(
        label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        value
            .pow(argument)
            .map_err(|error| Failure::new(label, error))
    }
}

impl ValuePower for AttributeName {
    fn power(
        label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        value
            .pow(argument)
            .map_err(|error| Failure::new(label, error))
    }
}

impl ValuePower for IndexValue<Positional> {
    fn power(
        _label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        Ok(value.pow(argument as u32))
    }
}

impl ValuePower for IndexValue<NodeIndex> {
    fn power(
        label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        value
            .pow(argument)
            .map_err(|error| Failure::new(label, error))
    }
}

impl ValuePower for IndexValue<AttributeName> {
    fn power(
        label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        value
            .pow(argument)
            .map_err(|error| Failure::new(label, error))
    }
}

impl ValuePower for IndexValue<EdgeIndex> {
    fn power(
        _label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        Ok(value.pow(argument))
    }
}

impl ValuePower for IndexValue<GraphRecordValue> {
    fn power(
        label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        value
            .pow(argument)
            .map_err(|error| Failure::new(label, error))
    }
}

impl ValueSubtract for Scalar {
    fn subtract(
        label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        (value - argument).map_err(|error| Failure::new(label, error))
    }
}

impl ValueSubtract for AttributeName {
    fn subtract(
        label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        (value - argument).map_err(|error| Failure::new(label, error))
    }
}

impl ValueSubtract for IndexValue<Positional> {
    fn subtract(
        _label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        Ok(value - argument)
    }
}

impl ValueSubtract for IndexValue<NodeIndex> {
    fn subtract(
        label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        (value - argument).map_err(|error| Failure::new(label, error))
    }
}

impl ValueSubtract for IndexValue<AttributeName> {
    fn subtract(
        label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        (value - argument).map_err(|error| Failure::new(label, error))
    }
}

impl ValueSubtract for IndexValue<EdgeIndex> {
    fn subtract(
        _label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        Ok(value - argument)
    }
}

impl ValueSubtract for IndexValue<GraphRecordValue> {
    fn subtract(
        label: &'static str,
        value: Self::Owned,
        argument: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        (value - argument).map_err(|error| Failure::new(label, error))
    }
}
