use super::{identifier_into_view, value_into_view};
use crate::{
    Failure, FailureKind, FailureKindValue, IndexValue, Mask, Position, Positional, QueryResult,
    Scalar, ValueDomain, error::conversion::InvalidTransition,
};
use graphrecords_core::graphrecord::{
    AttributeName, GroupIndex, GroupIndexView, Identifier, IdentifierView, NodeIndex,
    NodeIndexView, Value, ValueView, datatypes::AttributeNameView,
};

pub trait ValueTransition<T: ValueDomain>: ValueDomain {
    fn transition<'a>(value: Self::Value<'a>, label: &'static str) -> QueryResult<T::Value<'a>>;
}

const ATTRIBUTE_NAME_INDEX_TARGET: &str = "IndexValue<AttributeName>";
const ATTRIBUTE_NAME_TARGET: &str = "AttributeName";
const BOOL_INDEX_TARGET: &str = "IndexValue<bool>";
const GROUP_TARGET: &str = "IndexValue<GroupIndex>";
const MASK_TARGET: &str = "Mask";
const NODE_INDEX_TARGET: &str = "IndexValue<NodeIndex>";
const POSITIONAL_INDEX_TARGET: &str = "IndexValue<Positional>";
const SCALAR_TARGET: &str = "Scalar";
const VALUE_INDEX_TARGET: &str = "IndexValue<Value>";

fn identifier_view_into_value_view(identifier: IdentifierView<'_>) -> ValueView<'_> {
    match identifier {
        IdentifierView::String(value) => ValueView::String(value),
        IdentifierView::Int(value) => ValueView::Int(value),
    }
}

fn value_view_to_identifier_view<'a>(
    value: ValueView<'a>,
    target: &'static str,
    label: &'static str,
) -> QueryResult<IdentifierView<'a>> {
    match value {
        ValueView::String(value) => Ok(IdentifierView::String(value)),
        ValueView::Int(value) => Ok(IdentifierView::Int(value)),
        value => Err(Failure::new(
            InvalidTransition::new(Value::from(value), target),
            label,
        )),
    }
}

fn value_to_identifier(
    value: Value,
    target: &'static str,
    label: &'static str,
) -> QueryResult<Identifier> {
    match value {
        Value::String(value) => Ok(Identifier::String(value)),
        Value::Int(value) => Ok(Identifier::Int(value)),
        value => Err(Failure::new(InvalidTransition::new(value, target), label)),
    }
}

fn value_to_bool(value: Value, target: &'static str, label: &'static str) -> QueryResult<bool> {
    match value {
        Value::Bool(value) => Ok(value),
        value => Err(Failure::new(InvalidTransition::new(value, target), label)),
    }
}

fn value_to_position(value: Value, label: &'static str) -> QueryResult<Position> {
    match value {
        Value::Int(value) => Position::try_from(value).map_err(|_| {
            Failure::new(
                InvalidTransition::new(Value::Int(value), POSITIONAL_INDEX_TARGET),
                label,
            )
        }),
        value => Err(Failure::new(
            InvalidTransition::new(value, POSITIONAL_INDEX_TARGET),
            label,
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
    value: Position,
    target: &'static str,
    label: &'static str,
) -> QueryResult<i64> {
    i64::try_from(value).map_err(|_| Failure::new(InvalidTransition::new(value, target), label))
}

impl ValueTransition<IndexValue<Value>> for Scalar {
    fn transition<'a>(
        value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<IndexValue<Value> as ValueDomain>::Value<'a>> {
        Ok(Value::from(value))
    }
}

impl ValueTransition<AttributeName> for Scalar {
    fn transition<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<<AttributeName as ValueDomain>::Value<'a>> {
        value_view_to_identifier_view(value, ATTRIBUTE_NAME_TARGET, label)
            .map(AttributeNameView::from)
    }
}

impl ValueTransition<IndexValue<NodeIndex>> for Scalar {
    fn transition<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<<IndexValue<NodeIndex> as ValueDomain>::Value<'a>> {
        value_view_to_identifier_view(value, NODE_INDEX_TARGET, label).map(NodeIndexView::from)
    }
}

impl ValueTransition<IndexValue<GroupIndex>> for Scalar {
    fn transition<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<<IndexValue<GroupIndex> as ValueDomain>::Value<'a>> {
        value_view_to_identifier_view(value, GROUP_TARGET, label).map(GroupIndexView::from)
    }
}

impl ValueTransition<IndexValue<AttributeName>> for Scalar {
    fn transition<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<<IndexValue<AttributeName> as ValueDomain>::Value<'a>> {
        value_view_to_identifier_view(value, ATTRIBUTE_NAME_INDEX_TARGET, label)
            .map(AttributeNameView::from)
    }
}

impl ValueTransition<Mask> for Scalar {
    fn transition<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<<Mask as ValueDomain>::Value<'a>> {
        value_to_bool(Value::from(value), MASK_TARGET, label)
    }
}

impl ValueTransition<IndexValue<bool>> for Scalar {
    fn transition<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<<IndexValue<bool> as ValueDomain>::Value<'a>> {
        value_to_bool(Value::from(value), BOOL_INDEX_TARGET, label)
    }
}

impl ValueTransition<IndexValue<Positional>> for Scalar {
    fn transition<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<<IndexValue<Positional> as ValueDomain>::Value<'a>> {
        value_to_position(Value::from(value), label)
    }
}

impl ValueTransition<Scalar> for IndexValue<Value> {
    fn transition<'a>(
        value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<Scalar as ValueDomain>::Value<'a>> {
        Ok(value_into_view(value))
    }
}

impl ValueTransition<AttributeName> for IndexValue<Value> {
    fn transition<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<<AttributeName as ValueDomain>::Value<'a>> {
        value_to_identifier(value, ATTRIBUTE_NAME_TARGET, label)
            .map(|identifier| AttributeNameView::from(identifier_into_view(identifier)))
    }
}

impl ValueTransition<IndexValue<NodeIndex>> for IndexValue<Value> {
    fn transition<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<<IndexValue<NodeIndex> as ValueDomain>::Value<'a>> {
        value_to_identifier(value, NODE_INDEX_TARGET, label)
            .map(|identifier| NodeIndexView::from(identifier_into_view(identifier)))
    }
}

impl ValueTransition<IndexValue<GroupIndex>> for IndexValue<Value> {
    fn transition<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<<IndexValue<GroupIndex> as ValueDomain>::Value<'a>> {
        value_to_identifier(value, GROUP_TARGET, label)
            .map(|identifier| GroupIndexView::from(identifier_into_view(identifier)))
    }
}

impl ValueTransition<IndexValue<AttributeName>> for IndexValue<Value> {
    fn transition<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<<IndexValue<AttributeName> as ValueDomain>::Value<'a>> {
        value_to_identifier(value, ATTRIBUTE_NAME_INDEX_TARGET, label)
            .map(|identifier| AttributeNameView::from(identifier_into_view(identifier)))
    }
}

impl ValueTransition<Mask> for IndexValue<Value> {
    fn transition<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<<Mask as ValueDomain>::Value<'a>> {
        value_to_bool(value, MASK_TARGET, label)
    }
}

impl ValueTransition<IndexValue<bool>> for IndexValue<Value> {
    fn transition<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<<IndexValue<bool> as ValueDomain>::Value<'a>> {
        value_to_bool(value, BOOL_INDEX_TARGET, label)
    }
}

impl ValueTransition<IndexValue<Positional>> for IndexValue<Value> {
    fn transition<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<<IndexValue<Positional> as ValueDomain>::Value<'a>> {
        value_to_position(value, label)
    }
}

impl ValueTransition<Scalar> for AttributeName {
    fn transition<'a>(
        value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<Scalar as ValueDomain>::Value<'a>> {
        Ok(identifier_view_into_value_view(
            value.identifier_view().clone(),
        ))
    }
}

impl ValueTransition<IndexValue<Value>> for AttributeName {
    fn transition<'a>(
        value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<IndexValue<Value> as ValueDomain>::Value<'a>> {
        Ok(identifier_to_value(Identifier::from(Self::from(value))))
    }
}

impl ValueTransition<IndexValue<NodeIndex>> for AttributeName {
    fn transition<'a>(
        value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<IndexValue<NodeIndex> as ValueDomain>::Value<'a>> {
        Ok(NodeIndexView::from(value.identifier_view().clone()))
    }
}

impl ValueTransition<IndexValue<GroupIndex>> for AttributeName {
    fn transition<'a>(
        value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<IndexValue<GroupIndex> as ValueDomain>::Value<'a>> {
        Ok(GroupIndexView::from(value.identifier_view().clone()))
    }
}

impl ValueTransition<IndexValue<Self>> for AttributeName {
    fn transition<'a>(
        value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<IndexValue<Self> as ValueDomain>::Value<'a>> {
        Ok(value)
    }
}

impl ValueTransition<IndexValue<Positional>> for AttributeName {
    fn transition<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<<IndexValue<Positional> as ValueDomain>::Value<'a>> {
        match value.identifier_view() {
            IdentifierView::Int(integer) => Position::try_from(*integer).map_err(|_| {
                Failure::new(
                    InvalidTransition::new(Self::from(value), POSITIONAL_INDEX_TARGET),
                    label,
                )
            }),
            IdentifierView::String(_) => Err(Failure::new(
                InvalidTransition::new(Self::from(value), POSITIONAL_INDEX_TARGET),
                label,
            )),
        }
    }
}

impl ValueTransition<Scalar> for IndexValue<NodeIndex> {
    fn transition<'a>(
        value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<Scalar as ValueDomain>::Value<'a>> {
        Ok(identifier_view_into_value_view(
            value.identifier_view().clone(),
        ))
    }
}

impl ValueTransition<IndexValue<Value>> for IndexValue<NodeIndex> {
    fn transition<'a>(
        value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<IndexValue<Value> as ValueDomain>::Value<'a>> {
        Ok(identifier_to_value(Identifier::from(NodeIndex::from(
            value,
        ))))
    }
}

impl ValueTransition<AttributeName> for IndexValue<NodeIndex> {
    fn transition<'a>(
        value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<AttributeName as ValueDomain>::Value<'a>> {
        Ok(AttributeNameView::from(value.identifier_view().clone()))
    }
}

impl ValueTransition<IndexValue<AttributeName>> for IndexValue<NodeIndex> {
    fn transition<'a>(
        value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<IndexValue<AttributeName> as ValueDomain>::Value<'a>> {
        Ok(AttributeNameView::from(value.identifier_view().clone()))
    }
}

impl ValueTransition<IndexValue<Positional>> for IndexValue<NodeIndex> {
    fn transition<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<<IndexValue<Positional> as ValueDomain>::Value<'a>> {
        match value.identifier_view() {
            IdentifierView::Int(integer) => Position::try_from(*integer).map_err(|_| {
                Failure::new(
                    InvalidTransition::new(NodeIndex::from(value), POSITIONAL_INDEX_TARGET),
                    label,
                )
            }),
            IdentifierView::String(_) => Err(Failure::new(
                InvalidTransition::new(NodeIndex::from(value), POSITIONAL_INDEX_TARGET),
                label,
            )),
        }
    }
}

impl ValueTransition<Scalar> for IndexValue<GroupIndex> {
    fn transition<'a>(
        value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<Scalar as ValueDomain>::Value<'a>> {
        Ok(identifier_view_into_value_view(
            value.identifier_view().clone(),
        ))
    }
}

impl ValueTransition<IndexValue<Value>> for IndexValue<GroupIndex> {
    fn transition<'a>(
        value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<IndexValue<Value> as ValueDomain>::Value<'a>> {
        Ok(identifier_to_value(Identifier::from(GroupIndex::from(
            value,
        ))))
    }
}

impl ValueTransition<AttributeName> for IndexValue<GroupIndex> {
    fn transition<'a>(
        value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<AttributeName as ValueDomain>::Value<'a>> {
        Ok(AttributeNameView::from(value.identifier_view().clone()))
    }
}

impl ValueTransition<IndexValue<AttributeName>> for IndexValue<GroupIndex> {
    fn transition<'a>(
        value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<IndexValue<AttributeName> as ValueDomain>::Value<'a>> {
        Ok(AttributeNameView::from(value.identifier_view().clone()))
    }
}

impl ValueTransition<IndexValue<Positional>> for IndexValue<GroupIndex> {
    fn transition<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<<IndexValue<Positional> as ValueDomain>::Value<'a>> {
        match value.identifier_view() {
            IdentifierView::Int(integer) => Position::try_from(*integer).map_err(|_| {
                Failure::new(
                    InvalidTransition::new(GroupIndex::from(value), POSITIONAL_INDEX_TARGET),
                    label,
                )
            }),
            IdentifierView::String(_) => Err(Failure::new(
                InvalidTransition::new(GroupIndex::from(value), POSITIONAL_INDEX_TARGET),
                label,
            )),
        }
    }
}

impl ValueTransition<Scalar> for IndexValue<AttributeName> {
    fn transition<'a>(
        value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<Scalar as ValueDomain>::Value<'a>> {
        Ok(identifier_view_into_value_view(
            value.identifier_view().clone(),
        ))
    }
}

impl ValueTransition<IndexValue<Value>> for IndexValue<AttributeName> {
    fn transition<'a>(
        value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<IndexValue<Value> as ValueDomain>::Value<'a>> {
        Ok(identifier_to_value(Identifier::from(AttributeName::from(
            value,
        ))))
    }
}

impl ValueTransition<AttributeName> for IndexValue<AttributeName> {
    fn transition<'a>(
        value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<AttributeName as ValueDomain>::Value<'a>> {
        Ok(value)
    }
}

impl ValueTransition<IndexValue<NodeIndex>> for IndexValue<AttributeName> {
    fn transition<'a>(
        value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<IndexValue<NodeIndex> as ValueDomain>::Value<'a>> {
        Ok(NodeIndexView::from(value.identifier_view().clone()))
    }
}

impl ValueTransition<IndexValue<GroupIndex>> for IndexValue<AttributeName> {
    fn transition<'a>(
        value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<IndexValue<GroupIndex> as ValueDomain>::Value<'a>> {
        Ok(GroupIndexView::from(value.identifier_view().clone()))
    }
}

impl ValueTransition<IndexValue<Positional>> for IndexValue<AttributeName> {
    fn transition<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<<IndexValue<Positional> as ValueDomain>::Value<'a>> {
        match value.identifier_view() {
            IdentifierView::Int(integer) => Position::try_from(*integer).map_err(|_| {
                Failure::new(
                    InvalidTransition::new(AttributeName::from(value), POSITIONAL_INDEX_TARGET),
                    label,
                )
            }),
            IdentifierView::String(_) => Err(Failure::new(
                InvalidTransition::new(AttributeName::from(value), POSITIONAL_INDEX_TARGET),
                label,
            )),
        }
    }
}

impl ValueTransition<Scalar> for Mask {
    fn transition<'a>(
        value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<Scalar as ValueDomain>::Value<'a>> {
        Ok(ValueView::Bool(value))
    }
}

impl ValueTransition<IndexValue<Value>> for Mask {
    fn transition<'a>(
        value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<IndexValue<Value> as ValueDomain>::Value<'a>> {
        Ok(Value::Bool(value))
    }
}

impl ValueTransition<IndexValue<bool>> for Mask {
    fn transition<'a>(
        value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<IndexValue<bool> as ValueDomain>::Value<'a>> {
        Ok(value)
    }
}

impl ValueTransition<Scalar> for IndexValue<bool> {
    fn transition<'a>(
        value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<Scalar as ValueDomain>::Value<'a>> {
        Ok(ValueView::Bool(value))
    }
}

impl ValueTransition<IndexValue<Value>> for IndexValue<bool> {
    fn transition<'a>(
        value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<IndexValue<Value> as ValueDomain>::Value<'a>> {
        Ok(Value::Bool(value))
    }
}

impl ValueTransition<Mask> for IndexValue<bool> {
    fn transition<'a>(
        value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<Mask as ValueDomain>::Value<'a>> {
        Ok(value)
    }
}

impl ValueTransition<Scalar> for IndexValue<Positional> {
    fn transition<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<<Scalar as ValueDomain>::Value<'a>> {
        position_to_integer(value, SCALAR_TARGET, label).map(ValueView::Int)
    }
}

impl ValueTransition<IndexValue<Value>> for IndexValue<Positional> {
    fn transition<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<<IndexValue<Value> as ValueDomain>::Value<'a>> {
        position_to_integer(value, VALUE_INDEX_TARGET, label).map(Value::Int)
    }
}

impl ValueTransition<AttributeName> for IndexValue<Positional> {
    fn transition<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<<AttributeName as ValueDomain>::Value<'a>> {
        position_to_integer(value, ATTRIBUTE_NAME_TARGET, label)
            .map(|integer| AttributeNameView::from(IdentifierView::Int(integer)))
    }
}

impl ValueTransition<IndexValue<NodeIndex>> for IndexValue<Positional> {
    fn transition<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<<IndexValue<NodeIndex> as ValueDomain>::Value<'a>> {
        position_to_integer(value, NODE_INDEX_TARGET, label)
            .map(|integer| NodeIndexView::from(IdentifierView::Int(integer)))
    }
}

impl ValueTransition<IndexValue<GroupIndex>> for IndexValue<Positional> {
    fn transition<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<<IndexValue<GroupIndex> as ValueDomain>::Value<'a>> {
        position_to_integer(value, GROUP_TARGET, label)
            .map(|integer| GroupIndexView::from(IdentifierView::Int(integer)))
    }
}

impl ValueTransition<IndexValue<AttributeName>> for IndexValue<Positional> {
    fn transition<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<<IndexValue<AttributeName> as ValueDomain>::Value<'a>> {
        position_to_integer(value, ATTRIBUTE_NAME_INDEX_TARGET, label)
            .map(|integer| AttributeNameView::from(IdentifierView::Int(integer)))
    }
}

impl ValueTransition<IndexValue<FailureKind>> for FailureKindValue {
    fn transition<'a>(
        value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<IndexValue<FailureKind> as ValueDomain>::Value<'a>> {
        Ok(value)
    }
}

impl ValueTransition<FailureKindValue> for IndexValue<FailureKind> {
    fn transition<'a>(
        value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<FailureKindValue as ValueDomain>::Value<'a>> {
        Ok(value)
    }
}
