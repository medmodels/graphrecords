mod group_by;
mod operand;
mod operation;

use super::{
    attributes::{MultipleAttributesWithIndexOperand, MultipleAttributesWithIndexOperation},
    edges::EdgeOperand,
    group_by::GroupOperand,
    nodes::NodeOperand,
    BoxedIterator, EvaluateBackward, Index, RootOperand,
};
use crate::{
    errors::GraphRecordResult,
    graphrecord::{
        querying::DeepClone, EdgeIndex, GraphRecordAttribute, GraphRecordValue, NodeIndex,
    },
    GraphRecord,
};
pub use operand::{
    EdgeMultipleValuesWithIndexOperand, EdgeMultipleValuesWithoutIndexOperand,
    EdgeSingleValueWithIndexOperand, EdgeSingleValueWithoutIndexOperand,
    MultipleValuesComparisonOperand, MultipleValuesWithIndexOperand,
    MultipleValuesWithoutIndexOperand, NodeMultipleValuesWithIndexOperand,
    NodeMultipleValuesWithoutIndexOperand, NodeSingleValueWithIndexOperand,
    NodeSingleValueWithoutIndexOperand, SingleValueComparisonOperand, SingleValueWithIndexOperand,
    SingleValueWithoutIndexOperand,
};
use std::fmt::Display;

#[derive(Debug, Clone)]
pub enum MultipleValuesWithIndexContext<O: RootOperand> {
    Operand((O, GraphRecordAttribute)),
    MultipleAttributesOperand(MultipleAttributesWithIndexOperand<O>),
    SingleValueWithIndexGroupByOperand(GroupOperand<SingleValueWithIndexOperand<O>>),
    MultipleValuesWithIndexGroupByOperand(GroupOperand<MultipleValuesWithIndexOperand<O>>),
}

impl<O: RootOperand> MultipleValuesWithIndexContext<O> {
    pub(crate) fn get_values<'a>(
        &self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<impl Iterator<Item = (&'a O::Index, GraphRecordValue)> + 'a>
    where
        O: 'a,
    {
        let values: BoxedIterator<_> = match self {
            Self::Operand((operand, attribute)) => {
                Box::new(operand.get_values(graphrecord, attribute.clone())?)
            }
            Self::MultipleAttributesOperand(operand) => {
                let attributes = operand.evaluate_backward(graphrecord)?;

                Box::new(MultipleAttributesWithIndexOperation::<O>::get_values(
                    graphrecord,
                    attributes,
                )?)
            }
            Self::SingleValueWithIndexGroupByOperand(operand) => Box::new(
                operand
                    .evaluate_backward(graphrecord)?
                    .filter_map(|(_, attribute)| attribute),
            ),
            Self::MultipleValuesWithIndexGroupByOperand(operand) => Box::new(
                operand
                    .evaluate_backward(graphrecord)?
                    .flat_map(|(_, values)| values),
            ),
        };

        Ok(values)
    }
}

#[derive(Debug, Clone)]
pub enum MultipleValuesWithoutIndexContext<O: RootOperand> {
    GroupByOperand(GroupOperand<SingleValueWithoutIndexOperand<O>>),
}

impl<O: RootOperand> MultipleValuesWithoutIndexContext<O> {
    pub(crate) fn get_values<'a>(
        &self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<impl Iterator<Item = GraphRecordValue> + 'a>
    where
        O: 'a,
    {
        let values: BoxedIterator<_> = match self {
            Self::GroupByOperand(operand) => Box::new(
                operand
                    .evaluate_backward(graphrecord)?
                    .filter_map(|(_, value)| value),
            ),
        };

        Ok(values)
    }
}

#[derive(Debug, Clone)]
pub enum SingleValueWithoutIndexContext<O: RootOperand> {
    MultipleValuesWithIndexOperand(MultipleValuesWithIndexOperand<O>),
    MultipleValuesWithoutIndexOperand(MultipleValuesWithoutIndexOperand<O>),
}

impl<O: RootOperand> DeepClone for SingleValueWithoutIndexContext<O> {
    fn deep_clone(&self) -> Self {
        match self {
            Self::MultipleValuesWithIndexOperand(operand) => {
                Self::MultipleValuesWithIndexOperand(operand.deep_clone())
            }
            Self::MultipleValuesWithoutIndexOperand(operand) => {
                Self::MultipleValuesWithoutIndexOperand(operand.deep_clone())
            }
        }
    }
}

impl<O: RootOperand> SingleValueWithoutIndexContext<O> {
    pub(crate) fn get_values<'a>(
        &self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, GraphRecordValue>>
    where
        O: 'a,
    {
        Ok(match self {
            Self::MultipleValuesWithIndexOperand(operand) => Box::new(
                operand
                    .evaluate_backward(graphrecord)?
                    .map(|(_, value)| value),
            ),
            Self::MultipleValuesWithoutIndexOperand(operand) => {
                Box::new(operand.evaluate_backward(graphrecord)?)
            }
        })
    }
}

#[derive(Debug, Clone)]
pub enum SingleKindWithIndex {
    Max,
    Min,
    Random,
}

#[derive(Debug, Clone)]
pub enum SingleKindWithoutIndex {
    Max,
    Min,
    Mean,
    Median,
    Mode,
    Std,
    Var,
    Count,
    Sum,
    Random,
}

#[derive(Debug, Clone)]
pub enum SingleComparisonKind {
    GreaterThan,
    GreaterThanOrEqualTo,
    LessThan,
    LessThanOrEqualTo,
    EqualTo,
    NotEqualTo,
    StartsWith,
    EndsWith,
    Contains,
}

#[derive(Debug, Clone)]
pub enum MultipleComparisonKind {
    IsIn,
    IsNotIn,
}

#[derive(Debug, Clone)]
pub enum BinaryArithmeticKind {
    Add,
    Sub,
    Mul,
    Div,
    Pow,
    Mod,
}

impl Display for BinaryArithmeticKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BinaryArithmeticKind::Add => write!(f, "add"),
            BinaryArithmeticKind::Sub => write!(f, "sub"),
            BinaryArithmeticKind::Mul => write!(f, "mul"),
            BinaryArithmeticKind::Div => write!(f, "div"),
            BinaryArithmeticKind::Pow => write!(f, "pow"),
            BinaryArithmeticKind::Mod => write!(f, "mod"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum UnaryArithmeticKind {
    Round,
    Ceil,
    Floor,
    Abs,
    Sqrt,
    Trim,
    TrimStart,
    TrimEnd,
    Lowercase,
    Uppercase,
}

pub trait GetValues<I: Index> {
    fn get_values<'a>(
        &self,
        graphrecord: &'a GraphRecord,
        attribute: GraphRecordAttribute,
    ) -> GraphRecordResult<impl Iterator<Item = (&'a I, GraphRecordValue)> + 'a>
    where
        I: 'a;

    fn get_values_from_indices<'a>(
        graphrecord: &'a GraphRecord,
        attribute: GraphRecordAttribute,
        indices: impl Iterator<Item = &'a I> + 'a,
    ) -> impl Iterator<Item = (&'a I, GraphRecordValue)> + 'a
    where
        I: 'a;
}

impl GetValues<NodeIndex> for NodeOperand {
    fn get_values<'a>(
        &self,
        graphrecord: &'a GraphRecord,
        attribute: GraphRecordAttribute,
    ) -> GraphRecordResult<impl Iterator<Item = (&'a NodeIndex, GraphRecordValue)> + 'a>
    where
        NodeIndex: 'a,
    {
        let node_indices = self.evaluate_backward(graphrecord)?;

        Ok(Self::get_values_from_indices(
            graphrecord,
            attribute,
            node_indices,
        ))
    }

    fn get_values_from_indices<'a>(
        graphrecord: &'a GraphRecord,
        attribute: GraphRecordAttribute,
        node_indices: impl Iterator<Item = &'a NodeIndex> + 'a,
    ) -> impl Iterator<Item = (&'a NodeIndex, GraphRecordValue)> + 'a
    where
        NodeIndex: 'a,
    {
        node_indices.flat_map(move |node_index| {
            let attribute = graphrecord
                .node_attributes(node_index)
                .expect("Node must exist")
                .get(&attribute)?
                .clone();

            Some((node_index, attribute))
        })
    }
}

impl GetValues<EdgeIndex> for EdgeOperand {
    fn get_values<'a>(
        &self,
        graphrecord: &'a GraphRecord,
        attribute: GraphRecordAttribute,
    ) -> GraphRecordResult<impl Iterator<Item = (&'a EdgeIndex, GraphRecordValue)> + 'a>
    where
        EdgeIndex: 'a,
    {
        let edge_indices = self.evaluate_backward(graphrecord)?;

        Ok(Self::get_values_from_indices(
            graphrecord,
            attribute,
            edge_indices,
        ))
    }

    fn get_values_from_indices<'a>(
        graphrecord: &'a GraphRecord,
        attribute: GraphRecordAttribute,
        edge_indices: impl Iterator<Item = &'a EdgeIndex> + 'a,
    ) -> impl Iterator<Item = (&'a EdgeIndex, GraphRecordValue)> + 'a
    where
        EdgeIndex: 'a,
    {
        edge_indices.flat_map(move |edge_index| {
            Some((
                edge_index,
                graphrecord
                    .edge_attributes(edge_index)
                    .expect("Edge must exist")
                    .get(&attribute)?
                    .clone(),
            ))
        })
    }
}
