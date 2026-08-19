use crate::{
    Failure, FailureKind, FailureKindValue, IndexValue, Mask, Position, Positional, QueryResult,
    Scalar, ValueDomain, error::conversion::InvalidTransition,
};
use graphrecords_core::graphrecord::{AttributeName, EdgeIndex, Identifier, NodeIndex, Value};

pub trait ValueTransition<T: ValueDomain>: ValueDomain {
    fn transition<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<T::Value<'a>>;
}

const ATTRIBUTE_NAME_TARGET: &str = "AttributeName";
const ATTRIBUTE_NAME_INDEX_TARGET: &str = "IndexValue<AttributeName>";
const BOOL_INDEX_TARGET: &str = "IndexValue<bool>";
const EDGE_INDEX_TARGET: &str = "IndexValue<EdgeIndex>";
const VALUE_INDEX_TARGET: &str = "IndexValue<Value>";
const MASK_TARGET: &str = "Mask";
const NODE_INDEX_TARGET: &str = "IndexValue<NodeIndex>";
const POSITIONAL_INDEX_TARGET: &str = "IndexValue<Positional>";
const SCALAR_TARGET: &str = "Scalar";

fn value_to_identifier(
    label: &'static str,
    value: Value,
    target: &'static str,
) -> QueryResult<Identifier> {
    match value {
        Value::String(value) => Ok(Identifier::String(value)),
        Value::Int(value) => Ok(Identifier::Int(value)),
        value => Err(Failure::new(label, InvalidTransition::new(value, target))),
    }
}

fn value_to_bool(label: &'static str, value: Value, target: &'static str) -> QueryResult<bool> {
    match value {
        Value::Bool(value) => Ok(value),
        value => Err(Failure::new(label, InvalidTransition::new(value, target))),
    }
}

fn value_to_edge_index(label: &'static str, value: Value) -> QueryResult<EdgeIndex> {
    match value {
        Value::Int(value) => EdgeIndex::try_from(value).map_err(|_| {
            Failure::new(
                label,
                InvalidTransition::new(Value::Int(value), EDGE_INDEX_TARGET),
            )
        }),
        value => Err(Failure::new(
            label,
            InvalidTransition::new(value, EDGE_INDEX_TARGET),
        )),
    }
}

fn value_to_position(label: &'static str, value: Value) -> QueryResult<Position> {
    match value {
        Value::Int(value) => Position::try_from(value).map_err(|_| {
            Failure::new(
                label,
                InvalidTransition::new(Value::Int(value), POSITIONAL_INDEX_TARGET),
            )
        }),
        value => Err(Failure::new(
            label,
            InvalidTransition::new(value, POSITIONAL_INDEX_TARGET),
        )),
    }
}

fn identifier_to_value(value: Identifier) -> Value {
    match value {
        Identifier::String(value) => Value::String(value),
        Identifier::Int(value) => Value::Int(value),
    }
}

fn position_to_integer(
    label: &'static str,
    value: Position,
    target: &'static str,
) -> QueryResult<i64> {
    i64::try_from(value).map_err(|_| Failure::new(label, InvalidTransition::new(value, target)))
}

impl ValueTransition<IndexValue<Value>> for Scalar {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<Value> as ValueDomain>::Value<'a>> {
        Ok(value)
    }
}

impl ValueTransition<AttributeName> for Scalar {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<AttributeName as ValueDomain>::Value<'a>> {
        value_to_identifier(label, value, ATTRIBUTE_NAME_TARGET).map(AttributeName::from)
    }
}

impl ValueTransition<IndexValue<NodeIndex>> for Scalar {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<NodeIndex> as ValueDomain>::Value<'a>> {
        value_to_identifier(label, value, NODE_INDEX_TARGET).map(NodeIndex::from)
    }
}

impl ValueTransition<IndexValue<AttributeName>> for Scalar {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<AttributeName> as ValueDomain>::Value<'a>> {
        value_to_identifier(label, value, ATTRIBUTE_NAME_INDEX_TARGET).map(AttributeName::from)
    }
}

impl ValueTransition<Mask> for Scalar {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<Mask as ValueDomain>::Value<'a>> {
        value_to_bool(label, value, MASK_TARGET)
    }
}

impl ValueTransition<IndexValue<bool>> for Scalar {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<bool> as ValueDomain>::Value<'a>> {
        value_to_bool(label, value, BOOL_INDEX_TARGET)
    }
}

impl ValueTransition<IndexValue<EdgeIndex>> for Scalar {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<EdgeIndex> as ValueDomain>::Value<'a>> {
        value_to_edge_index(label, value)
    }
}

impl ValueTransition<IndexValue<Positional>> for Scalar {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<Positional> as ValueDomain>::Value<'a>> {
        value_to_position(label, value)
    }
}

impl ValueTransition<Scalar> for IndexValue<Value> {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<Scalar as ValueDomain>::Value<'a>> {
        Ok(value)
    }
}

impl ValueTransition<AttributeName> for IndexValue<Value> {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<AttributeName as ValueDomain>::Value<'a>> {
        value_to_identifier(label, value, ATTRIBUTE_NAME_TARGET).map(AttributeName::from)
    }
}

impl ValueTransition<IndexValue<NodeIndex>> for IndexValue<Value> {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<NodeIndex> as ValueDomain>::Value<'a>> {
        value_to_identifier(label, value, NODE_INDEX_TARGET).map(NodeIndex::from)
    }
}

impl ValueTransition<IndexValue<AttributeName>> for IndexValue<Value> {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<AttributeName> as ValueDomain>::Value<'a>> {
        value_to_identifier(label, value, ATTRIBUTE_NAME_INDEX_TARGET).map(AttributeName::from)
    }
}

impl ValueTransition<Mask> for IndexValue<Value> {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<Mask as ValueDomain>::Value<'a>> {
        value_to_bool(label, value, MASK_TARGET)
    }
}

impl ValueTransition<IndexValue<bool>> for IndexValue<Value> {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<bool> as ValueDomain>::Value<'a>> {
        value_to_bool(label, value, BOOL_INDEX_TARGET)
    }
}

impl ValueTransition<IndexValue<EdgeIndex>> for IndexValue<Value> {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<EdgeIndex> as ValueDomain>::Value<'a>> {
        value_to_edge_index(label, value)
    }
}

impl ValueTransition<IndexValue<Positional>> for IndexValue<Value> {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<Positional> as ValueDomain>::Value<'a>> {
        value_to_position(label, value)
    }
}

impl ValueTransition<Scalar> for AttributeName {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<Scalar as ValueDomain>::Value<'a>> {
        Ok(identifier_to_value(Identifier::from(value)))
    }
}

impl ValueTransition<IndexValue<Value>> for AttributeName {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<Value> as ValueDomain>::Value<'a>> {
        Ok(identifier_to_value(Identifier::from(value)))
    }
}

impl ValueTransition<IndexValue<NodeIndex>> for AttributeName {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<NodeIndex> as ValueDomain>::Value<'a>> {
        Ok(NodeIndex::from(Identifier::from(value)))
    }
}

impl ValueTransition<IndexValue<Self>> for AttributeName {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<Self> as ValueDomain>::Value<'a>> {
        Ok(value)
    }
}

impl ValueTransition<IndexValue<EdgeIndex>> for AttributeName {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<EdgeIndex> as ValueDomain>::Value<'a>> {
        match value.identifier() {
            Identifier::Int(integer) => EdgeIndex::try_from(*integer)
                .map_err(|_| Failure::new(label, InvalidTransition::new(value, EDGE_INDEX_TARGET))),
            Identifier::String(_) => Err(Failure::new(
                label,
                InvalidTransition::new(value, EDGE_INDEX_TARGET),
            )),
        }
    }
}

impl ValueTransition<IndexValue<Positional>> for AttributeName {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<Positional> as ValueDomain>::Value<'a>> {
        match value.identifier() {
            Identifier::Int(integer) => Position::try_from(*integer).map_err(|_| {
                Failure::new(
                    label,
                    InvalidTransition::new(value, POSITIONAL_INDEX_TARGET),
                )
            }),
            Identifier::String(_) => Err(Failure::new(
                label,
                InvalidTransition::new(value, POSITIONAL_INDEX_TARGET),
            )),
        }
    }
}

impl ValueTransition<Scalar> for IndexValue<NodeIndex> {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<Scalar as ValueDomain>::Value<'a>> {
        Ok(identifier_to_value(Identifier::from(value)))
    }
}

impl ValueTransition<IndexValue<Value>> for IndexValue<NodeIndex> {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<Value> as ValueDomain>::Value<'a>> {
        Ok(identifier_to_value(Identifier::from(value)))
    }
}

impl ValueTransition<AttributeName> for IndexValue<NodeIndex> {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<AttributeName as ValueDomain>::Value<'a>> {
        Ok(AttributeName::from(Identifier::from(value)))
    }
}

impl ValueTransition<IndexValue<AttributeName>> for IndexValue<NodeIndex> {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<AttributeName> as ValueDomain>::Value<'a>> {
        Ok(AttributeName::from(Identifier::from(value)))
    }
}

impl ValueTransition<IndexValue<EdgeIndex>> for IndexValue<NodeIndex> {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<EdgeIndex> as ValueDomain>::Value<'a>> {
        match value.identifier() {
            Identifier::Int(integer) => EdgeIndex::try_from(*integer)
                .map_err(|_| Failure::new(label, InvalidTransition::new(value, EDGE_INDEX_TARGET))),
            Identifier::String(_) => Err(Failure::new(
                label,
                InvalidTransition::new(value, EDGE_INDEX_TARGET),
            )),
        }
    }
}

impl ValueTransition<IndexValue<Positional>> for IndexValue<NodeIndex> {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<Positional> as ValueDomain>::Value<'a>> {
        match value.identifier() {
            Identifier::Int(integer) => Position::try_from(*integer).map_err(|_| {
                Failure::new(
                    label,
                    InvalidTransition::new(value, POSITIONAL_INDEX_TARGET),
                )
            }),
            Identifier::String(_) => Err(Failure::new(
                label,
                InvalidTransition::new(value, POSITIONAL_INDEX_TARGET),
            )),
        }
    }
}

impl ValueTransition<Scalar> for IndexValue<AttributeName> {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<Scalar as ValueDomain>::Value<'a>> {
        Ok(identifier_to_value(Identifier::from(value)))
    }
}

impl ValueTransition<IndexValue<Value>> for IndexValue<AttributeName> {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<Value> as ValueDomain>::Value<'a>> {
        Ok(identifier_to_value(Identifier::from(value)))
    }
}

impl ValueTransition<AttributeName> for IndexValue<AttributeName> {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<AttributeName as ValueDomain>::Value<'a>> {
        Ok(value)
    }
}

impl ValueTransition<IndexValue<NodeIndex>> for IndexValue<AttributeName> {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<NodeIndex> as ValueDomain>::Value<'a>> {
        Ok(NodeIndex::from(Identifier::from(value)))
    }
}

impl ValueTransition<IndexValue<EdgeIndex>> for IndexValue<AttributeName> {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<EdgeIndex> as ValueDomain>::Value<'a>> {
        match value.identifier() {
            Identifier::Int(integer) => EdgeIndex::try_from(*integer)
                .map_err(|_| Failure::new(label, InvalidTransition::new(value, EDGE_INDEX_TARGET))),
            Identifier::String(_) => Err(Failure::new(
                label,
                InvalidTransition::new(value, EDGE_INDEX_TARGET),
            )),
        }
    }
}

impl ValueTransition<IndexValue<Positional>> for IndexValue<AttributeName> {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<Positional> as ValueDomain>::Value<'a>> {
        match value.identifier() {
            Identifier::Int(integer) => Position::try_from(*integer).map_err(|_| {
                Failure::new(
                    label,
                    InvalidTransition::new(value, POSITIONAL_INDEX_TARGET),
                )
            }),
            Identifier::String(_) => Err(Failure::new(
                label,
                InvalidTransition::new(value, POSITIONAL_INDEX_TARGET),
            )),
        }
    }
}

impl ValueTransition<Scalar> for Mask {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<Scalar as ValueDomain>::Value<'a>> {
        Ok(Value::Bool(value))
    }
}

impl ValueTransition<IndexValue<Value>> for Mask {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<Value> as ValueDomain>::Value<'a>> {
        Ok(Value::Bool(value))
    }
}

impl ValueTransition<IndexValue<bool>> for Mask {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<bool> as ValueDomain>::Value<'a>> {
        Ok(value)
    }
}

impl ValueTransition<Scalar> for IndexValue<bool> {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<Scalar as ValueDomain>::Value<'a>> {
        Ok(Value::Bool(value))
    }
}

impl ValueTransition<IndexValue<Value>> for IndexValue<bool> {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<Value> as ValueDomain>::Value<'a>> {
        Ok(Value::Bool(value))
    }
}

impl ValueTransition<Mask> for IndexValue<bool> {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<Mask as ValueDomain>::Value<'a>> {
        Ok(value)
    }
}

impl ValueTransition<Scalar> for IndexValue<EdgeIndex> {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<Scalar as ValueDomain>::Value<'a>> {
        Ok(Value::Int(i64::from(value)))
    }
}

impl ValueTransition<IndexValue<Value>> for IndexValue<EdgeIndex> {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<Value> as ValueDomain>::Value<'a>> {
        Ok(Value::Int(i64::from(value)))
    }
}

impl ValueTransition<AttributeName> for IndexValue<EdgeIndex> {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<AttributeName as ValueDomain>::Value<'a>> {
        Ok(AttributeName::from(i64::from(value)))
    }
}

impl ValueTransition<IndexValue<NodeIndex>> for IndexValue<EdgeIndex> {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<NodeIndex> as ValueDomain>::Value<'a>> {
        Ok(NodeIndex::from(i64::from(value)))
    }
}

impl ValueTransition<IndexValue<AttributeName>> for IndexValue<EdgeIndex> {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<AttributeName> as ValueDomain>::Value<'a>> {
        Ok(AttributeName::from(i64::from(value)))
    }
}

impl ValueTransition<IndexValue<Positional>> for IndexValue<EdgeIndex> {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<Positional> as ValueDomain>::Value<'a>> {
        usize::try_from(value).map_err(|_| {
            Failure::new(
                label,
                InvalidTransition::new(value, POSITIONAL_INDEX_TARGET),
            )
        })
    }
}

impl ValueTransition<Scalar> for IndexValue<Positional> {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<Scalar as ValueDomain>::Value<'a>> {
        position_to_integer(label, value, SCALAR_TARGET).map(Value::Int)
    }
}

impl ValueTransition<IndexValue<Value>> for IndexValue<Positional> {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<Value> as ValueDomain>::Value<'a>> {
        position_to_integer(label, value, VALUE_INDEX_TARGET).map(Value::Int)
    }
}

impl ValueTransition<AttributeName> for IndexValue<Positional> {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<AttributeName as ValueDomain>::Value<'a>> {
        position_to_integer(label, value, ATTRIBUTE_NAME_TARGET).map(AttributeName::from)
    }
}

impl ValueTransition<IndexValue<NodeIndex>> for IndexValue<Positional> {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<NodeIndex> as ValueDomain>::Value<'a>> {
        position_to_integer(label, value, NODE_INDEX_TARGET).map(NodeIndex::from)
    }
}

impl ValueTransition<IndexValue<AttributeName>> for IndexValue<Positional> {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<AttributeName> as ValueDomain>::Value<'a>> {
        position_to_integer(label, value, ATTRIBUTE_NAME_INDEX_TARGET).map(AttributeName::from)
    }
}

impl ValueTransition<IndexValue<EdgeIndex>> for IndexValue<Positional> {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<EdgeIndex> as ValueDomain>::Value<'a>> {
        EdgeIndex::try_from(value)
            .map_err(|_| Failure::new(label, InvalidTransition::new(value, EDGE_INDEX_TARGET)))
    }
}

impl ValueTransition<IndexValue<FailureKind>> for FailureKindValue {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<FailureKind> as ValueDomain>::Value<'a>> {
        Ok(value)
    }
}

impl ValueTransition<FailureKindValue> for IndexValue<FailureKind> {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<FailureKindValue as ValueDomain>::Value<'a>> {
        Ok(value)
    }
}
