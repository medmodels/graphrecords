use crate::{
    AttributeName, Failure, FailureKind, FailureKindValue, IndexValue, Mask, Position, Positional,
    QueryResult, Scalar, ValueDomain, error::conversion::InvalidTransition,
};
use graphrecords_core::graphrecord::{
    EdgeIndex, GraphRecordAttribute, GraphRecordValue, NodeIndex,
};

pub trait ValueTransition<T: ValueDomain>: ValueDomain {
    fn transition<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<T::Value<'a>>;
}

const ATTRIBUTE_NAME_TARGET: &str = "AttributeName";
const ATTRIBUTE_NAME_INDEX_TARGET: &str = "IndexValue<AttributeName>";
const BOOL_INDEX_TARGET: &str = "IndexValue<bool>";
const EDGE_INDEX_TARGET: &str = "IndexValue<EdgeIndex>";
const GRAPHRECORD_VALUE_INDEX_TARGET: &str = "IndexValue<GraphRecordValue>";
const MASK_TARGET: &str = "Mask";
const NODE_INDEX_TARGET: &str = "IndexValue<NodeIndex>";
const POSITIONAL_INDEX_TARGET: &str = "IndexValue<Positional>";
const SCALAR_TARGET: &str = "Scalar";

fn graphrecord_value_to_attribute(
    label: &'static str,
    value: GraphRecordValue,
    target: &'static str,
) -> QueryResult<GraphRecordAttribute> {
    match value {
        GraphRecordValue::String(value) => Ok(GraphRecordAttribute::String(value)),
        GraphRecordValue::Int(value) => Ok(GraphRecordAttribute::Int(value)),
        value => Err(Failure::new(label, InvalidTransition::new(value, target))),
    }
}

fn graphrecord_value_to_bool(
    label: &'static str,
    value: GraphRecordValue,
    target: &'static str,
) -> QueryResult<bool> {
    match value {
        GraphRecordValue::Bool(value) => Ok(value),
        value => Err(Failure::new(label, InvalidTransition::new(value, target))),
    }
}

fn graphrecord_value_to_edge_index(
    label: &'static str,
    value: GraphRecordValue,
) -> QueryResult<EdgeIndex> {
    match value {
        GraphRecordValue::Int(value) => EdgeIndex::try_from(value).map_err(|_| {
            Failure::new(
                label,
                InvalidTransition::new(GraphRecordValue::Int(value), EDGE_INDEX_TARGET),
            )
        }),
        value => Err(Failure::new(
            label,
            InvalidTransition::new(value, EDGE_INDEX_TARGET),
        )),
    }
}

fn graphrecord_value_to_position(
    label: &'static str,
    value: GraphRecordValue,
) -> QueryResult<Position> {
    match value {
        GraphRecordValue::Int(value) => Position::try_from(value).map_err(|_| {
            Failure::new(
                label,
                InvalidTransition::new(GraphRecordValue::Int(value), POSITIONAL_INDEX_TARGET),
            )
        }),
        value => Err(Failure::new(
            label,
            InvalidTransition::new(value, POSITIONAL_INDEX_TARGET),
        )),
    }
}

fn attribute_to_graphrecord_value(value: GraphRecordAttribute) -> GraphRecordValue {
    match value {
        GraphRecordAttribute::String(value) => GraphRecordValue::String(value),
        GraphRecordAttribute::Int(value) => GraphRecordValue::Int(value),
    }
}

fn attribute_to_edge_index(
    label: &'static str,
    value: GraphRecordAttribute,
) -> QueryResult<EdgeIndex> {
    match value {
        GraphRecordAttribute::Int(value) => EdgeIndex::try_from(value).map_err(|_| {
            Failure::new(
                label,
                InvalidTransition::new(GraphRecordAttribute::Int(value), EDGE_INDEX_TARGET),
            )
        }),
        value @ GraphRecordAttribute::String(_) => Err(Failure::new(
            label,
            InvalidTransition::new(value, EDGE_INDEX_TARGET),
        )),
    }
}

fn attribute_to_position(
    label: &'static str,
    value: GraphRecordAttribute,
) -> QueryResult<Position> {
    match value {
        GraphRecordAttribute::Int(value) => Position::try_from(value).map_err(|_| {
            Failure::new(
                label,
                InvalidTransition::new(GraphRecordAttribute::Int(value), POSITIONAL_INDEX_TARGET),
            )
        }),
        value @ GraphRecordAttribute::String(_) => Err(Failure::new(
            label,
            InvalidTransition::new(value, POSITIONAL_INDEX_TARGET),
        )),
    }
}

fn position_to_integer(
    label: &'static str,
    value: Position,
    target: &'static str,
) -> QueryResult<i64> {
    i64::try_from(value).map_err(|_| Failure::new(label, InvalidTransition::new(value, target)))
}

impl ValueTransition<IndexValue<GraphRecordValue>> for Scalar {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<GraphRecordValue> as ValueDomain>::Value<'a>> {
        Ok(value)
    }
}

impl ValueTransition<AttributeName> for Scalar {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<AttributeName as ValueDomain>::Value<'a>> {
        graphrecord_value_to_attribute(label, value, ATTRIBUTE_NAME_TARGET)
    }
}

impl ValueTransition<IndexValue<NodeIndex>> for Scalar {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<NodeIndex> as ValueDomain>::Value<'a>> {
        graphrecord_value_to_attribute(label, value, NODE_INDEX_TARGET)
    }
}

impl ValueTransition<IndexValue<AttributeName>> for Scalar {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<AttributeName> as ValueDomain>::Value<'a>> {
        graphrecord_value_to_attribute(label, value, ATTRIBUTE_NAME_INDEX_TARGET)
    }
}

impl ValueTransition<Mask> for Scalar {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<Mask as ValueDomain>::Value<'a>> {
        graphrecord_value_to_bool(label, value, MASK_TARGET)
    }
}

impl ValueTransition<IndexValue<bool>> for Scalar {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<bool> as ValueDomain>::Value<'a>> {
        graphrecord_value_to_bool(label, value, BOOL_INDEX_TARGET)
    }
}

impl ValueTransition<IndexValue<EdgeIndex>> for Scalar {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<EdgeIndex> as ValueDomain>::Value<'a>> {
        graphrecord_value_to_edge_index(label, value)
    }
}

impl ValueTransition<IndexValue<Positional>> for Scalar {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<Positional> as ValueDomain>::Value<'a>> {
        graphrecord_value_to_position(label, value)
    }
}

impl ValueTransition<Scalar> for IndexValue<GraphRecordValue> {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<Scalar as ValueDomain>::Value<'a>> {
        Ok(value)
    }
}

impl ValueTransition<AttributeName> for IndexValue<GraphRecordValue> {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<AttributeName as ValueDomain>::Value<'a>> {
        graphrecord_value_to_attribute(label, value, ATTRIBUTE_NAME_TARGET)
    }
}

impl ValueTransition<IndexValue<NodeIndex>> for IndexValue<GraphRecordValue> {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<NodeIndex> as ValueDomain>::Value<'a>> {
        graphrecord_value_to_attribute(label, value, NODE_INDEX_TARGET)
    }
}

impl ValueTransition<IndexValue<AttributeName>> for IndexValue<GraphRecordValue> {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<AttributeName> as ValueDomain>::Value<'a>> {
        graphrecord_value_to_attribute(label, value, ATTRIBUTE_NAME_INDEX_TARGET)
    }
}

impl ValueTransition<Mask> for IndexValue<GraphRecordValue> {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<Mask as ValueDomain>::Value<'a>> {
        graphrecord_value_to_bool(label, value, MASK_TARGET)
    }
}

impl ValueTransition<IndexValue<bool>> for IndexValue<GraphRecordValue> {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<bool> as ValueDomain>::Value<'a>> {
        graphrecord_value_to_bool(label, value, BOOL_INDEX_TARGET)
    }
}

impl ValueTransition<IndexValue<EdgeIndex>> for IndexValue<GraphRecordValue> {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<EdgeIndex> as ValueDomain>::Value<'a>> {
        graphrecord_value_to_edge_index(label, value)
    }
}

impl ValueTransition<IndexValue<Positional>> for IndexValue<GraphRecordValue> {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<Positional> as ValueDomain>::Value<'a>> {
        graphrecord_value_to_position(label, value)
    }
}

impl ValueTransition<Scalar> for AttributeName {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<Scalar as ValueDomain>::Value<'a>> {
        Ok(attribute_to_graphrecord_value(value))
    }
}

impl ValueTransition<IndexValue<GraphRecordValue>> for AttributeName {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<GraphRecordValue> as ValueDomain>::Value<'a>> {
        Ok(attribute_to_graphrecord_value(value))
    }
}

impl ValueTransition<IndexValue<NodeIndex>> for AttributeName {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<NodeIndex> as ValueDomain>::Value<'a>> {
        Ok(value)
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
        attribute_to_edge_index(label, value)
    }
}

impl ValueTransition<IndexValue<Positional>> for AttributeName {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<Positional> as ValueDomain>::Value<'a>> {
        attribute_to_position(label, value)
    }
}

impl ValueTransition<Scalar> for IndexValue<NodeIndex> {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<Scalar as ValueDomain>::Value<'a>> {
        Ok(attribute_to_graphrecord_value(value))
    }
}

impl ValueTransition<IndexValue<GraphRecordValue>> for IndexValue<NodeIndex> {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<GraphRecordValue> as ValueDomain>::Value<'a>> {
        Ok(attribute_to_graphrecord_value(value))
    }
}

impl ValueTransition<AttributeName> for IndexValue<NodeIndex> {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<AttributeName as ValueDomain>::Value<'a>> {
        Ok(value)
    }
}

impl ValueTransition<IndexValue<AttributeName>> for IndexValue<NodeIndex> {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<AttributeName> as ValueDomain>::Value<'a>> {
        Ok(value)
    }
}

impl ValueTransition<IndexValue<EdgeIndex>> for IndexValue<NodeIndex> {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<EdgeIndex> as ValueDomain>::Value<'a>> {
        attribute_to_edge_index(label, value)
    }
}

impl ValueTransition<IndexValue<Positional>> for IndexValue<NodeIndex> {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<Positional> as ValueDomain>::Value<'a>> {
        attribute_to_position(label, value)
    }
}

impl ValueTransition<Scalar> for IndexValue<AttributeName> {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<Scalar as ValueDomain>::Value<'a>> {
        Ok(attribute_to_graphrecord_value(value))
    }
}

impl ValueTransition<IndexValue<GraphRecordValue>> for IndexValue<AttributeName> {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<GraphRecordValue> as ValueDomain>::Value<'a>> {
        Ok(attribute_to_graphrecord_value(value))
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
        Ok(value)
    }
}

impl ValueTransition<IndexValue<EdgeIndex>> for IndexValue<AttributeName> {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<EdgeIndex> as ValueDomain>::Value<'a>> {
        attribute_to_edge_index(label, value)
    }
}

impl ValueTransition<IndexValue<Positional>> for IndexValue<AttributeName> {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<Positional> as ValueDomain>::Value<'a>> {
        attribute_to_position(label, value)
    }
}

impl ValueTransition<Scalar> for Mask {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<Scalar as ValueDomain>::Value<'a>> {
        Ok(GraphRecordValue::Bool(value))
    }
}

impl ValueTransition<IndexValue<GraphRecordValue>> for Mask {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<GraphRecordValue> as ValueDomain>::Value<'a>> {
        Ok(GraphRecordValue::Bool(value))
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
        Ok(GraphRecordValue::Bool(value))
    }
}

impl ValueTransition<IndexValue<GraphRecordValue>> for IndexValue<bool> {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<GraphRecordValue> as ValueDomain>::Value<'a>> {
        Ok(GraphRecordValue::Bool(value))
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
        Ok(GraphRecordValue::Int(i64::from(value)))
    }
}

impl ValueTransition<IndexValue<GraphRecordValue>> for IndexValue<EdgeIndex> {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<GraphRecordValue> as ValueDomain>::Value<'a>> {
        Ok(GraphRecordValue::Int(i64::from(value)))
    }
}

impl ValueTransition<AttributeName> for IndexValue<EdgeIndex> {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<AttributeName as ValueDomain>::Value<'a>> {
        Ok(GraphRecordAttribute::Int(i64::from(value)))
    }
}

impl ValueTransition<IndexValue<NodeIndex>> for IndexValue<EdgeIndex> {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<NodeIndex> as ValueDomain>::Value<'a>> {
        Ok(GraphRecordAttribute::Int(i64::from(value)))
    }
}

impl ValueTransition<IndexValue<AttributeName>> for IndexValue<EdgeIndex> {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<AttributeName> as ValueDomain>::Value<'a>> {
        Ok(GraphRecordAttribute::Int(i64::from(value)))
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
        position_to_integer(label, value, SCALAR_TARGET).map(GraphRecordValue::Int)
    }
}

impl ValueTransition<IndexValue<GraphRecordValue>> for IndexValue<Positional> {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<GraphRecordValue> as ValueDomain>::Value<'a>> {
        position_to_integer(label, value, GRAPHRECORD_VALUE_INDEX_TARGET).map(GraphRecordValue::Int)
    }
}

impl ValueTransition<AttributeName> for IndexValue<Positional> {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<AttributeName as ValueDomain>::Value<'a>> {
        position_to_integer(label, value, ATTRIBUTE_NAME_TARGET).map(GraphRecordAttribute::Int)
    }
}

impl ValueTransition<IndexValue<NodeIndex>> for IndexValue<Positional> {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<NodeIndex> as ValueDomain>::Value<'a>> {
        position_to_integer(label, value, NODE_INDEX_TARGET).map(GraphRecordAttribute::Int)
    }
}

impl ValueTransition<IndexValue<AttributeName>> for IndexValue<Positional> {
    fn transition<'a>(
        label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<IndexValue<AttributeName> as ValueDomain>::Value<'a>> {
        position_to_integer(label, value, ATTRIBUTE_NAME_INDEX_TARGET)
            .map(GraphRecordAttribute::Int)
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
