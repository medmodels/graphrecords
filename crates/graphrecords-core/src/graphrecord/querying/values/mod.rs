mod group_by;
mod operand;
mod operation;

use super::{
    BoxedIterator, EvaluateBackward, Index, RootOperand,
    attributes::{MultipleAttributesWithIndexOperand, MultipleAttributesWithIndexOperation},
    edges::EdgeOperand,
    group_by::GroupOperand,
    nodes::NodeOperand,
};
use crate::{
    GraphRecord,
    errors::GraphRecordResult,
    graphrecord::{
        EdgeIndex, GraphRecordAttribute, GraphRecordValue, NodeIndex,
        querying::{
            CountableOperand, DeepClone,
            attributes::{
                AttributesTreeOperand, AttributesTreeOperation,
                MultipleAttributesWithoutIndexOperand,
            },
            values::operation::MultipleValuesWithoutIndexOperation,
        },
    },
};
pub use group_by::SingleValueWithoutIndexOperandContext;
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
    AttributesTreeOperand(AttributesTreeOperand<O>),
    MultipleAttributesOperand(MultipleAttributesWithIndexOperand<O>),
    SingleValueWithIndexGroupByOperand(GroupOperand<SingleValueWithIndexOperand<O>>),
    MultipleValuesWithIndexGroupByOperand(GroupOperand<MultipleValuesWithIndexOperand<O>>),
}

impl<O: RootOperand> MultipleValuesWithIndexContext<O> {
    pub(crate) fn get_values<'a>(
        &self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<impl Iterator<Item = (&'a O::Index, GraphRecordValue)> + 'a + use<'a, O>>
    where
        O: 'a,
    {
        let values: BoxedIterator<_> = match self {
            Self::Operand((operand, attribute)) => {
                Box::new(operand.get_values(graphrecord, attribute.clone())?)
            }
            Self::AttributesTreeOperand(operand) => {
                let attributes = operand.evaluate_backward(graphrecord)?;

                Box::new(AttributesTreeOperation::<O>::get_count(attributes))
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
    ) -> GraphRecordResult<impl Iterator<Item = GraphRecordValue> + 'a + use<'a, O>>
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
    MultipleValuesWithIndexOperand {
        operand: MultipleValuesWithIndexOperand<O>,
        kind: SingleKindWithoutIndex,
    },
    MultipleValuesWithoutIndexOperand {
        operand: MultipleValuesWithoutIndexOperand<O>,
        kind: SingleKindWithoutIndex,
    },
    MultipleAttributesWithIndexOperand(MultipleAttributesWithIndexOperand<O>),
    MultipleAttributesWithoutIndexOperand(MultipleAttributesWithoutIndexOperand<O>),
    IndicesOperand(<O as RootOperand>::IndicesOperand),
}

impl<O: RootOperand> DeepClone for SingleValueWithoutIndexContext<O> {
    fn deep_clone(&self) -> Self {
        match self {
            Self::MultipleValuesWithIndexOperand { operand, kind } => {
                Self::MultipleValuesWithIndexOperand {
                    operand: operand.deep_clone(),
                    kind: kind.clone(),
                }
            }
            Self::MultipleValuesWithoutIndexOperand { operand, kind } => {
                Self::MultipleValuesWithoutIndexOperand {
                    operand: operand.deep_clone(),
                    kind: kind.clone(),
                }
            }
            Self::MultipleAttributesWithIndexOperand(operand) => {
                Self::MultipleAttributesWithIndexOperand(operand.deep_clone())
            }
            Self::MultipleAttributesWithoutIndexOperand(operand) => {
                Self::MultipleAttributesWithoutIndexOperand(operand.deep_clone())
            }
            Self::IndicesOperand(operand) => Self::IndicesOperand(operand.deep_clone()),
        }
    }
}

impl<O: RootOperand> SingleValueWithoutIndexContext<O> {
    pub(crate) fn get_value(
        &self,
        graphrecord: &GraphRecord,
    ) -> GraphRecordResult<Option<GraphRecordValue>> {
        Ok(match self {
            Self::MultipleValuesWithIndexOperand { operand, kind } => {
                let values = operand
                    .evaluate_backward(graphrecord)?
                    .map(|(_, value)| value);

                match kind {
                    SingleKindWithoutIndex::Max => {
                        MultipleValuesWithoutIndexOperation::<O>::get_max(values)?
                    }
                    SingleKindWithoutIndex::Min => {
                        MultipleValuesWithoutIndexOperation::<O>::get_min(values)?
                    }
                    SingleKindWithoutIndex::Mean => {
                        MultipleValuesWithoutIndexOperation::<O>::get_mean(values)?
                    }
                    SingleKindWithoutIndex::Median => {
                        MultipleValuesWithoutIndexOperation::<O>::get_median(values)?
                    }
                    SingleKindWithoutIndex::Mode => {
                        MultipleValuesWithoutIndexOperation::<O>::get_mode(values)
                    }
                    SingleKindWithoutIndex::Std => {
                        MultipleValuesWithoutIndexOperation::<O>::get_std(values)?
                    }
                    SingleKindWithoutIndex::Var => {
                        MultipleValuesWithoutIndexOperation::<O>::get_var(values)?
                    }
                    SingleKindWithoutIndex::Count => {
                        Some(MultipleValuesWithoutIndexOperation::<O>::get_count(values))
                    }
                    SingleKindWithoutIndex::Sum => {
                        MultipleValuesWithoutIndexOperation::<O>::get_sum(values)?
                    }
                    SingleKindWithoutIndex::Random => {
                        MultipleValuesWithoutIndexOperation::<O>::get_random(values)
                    }
                }
            }
            Self::MultipleValuesWithoutIndexOperand { operand, kind } => {
                let values = operand.evaluate_backward(graphrecord)?;

                match kind {
                    SingleKindWithoutIndex::Max => {
                        MultipleValuesWithoutIndexOperation::<O>::get_max(values)?
                    }
                    SingleKindWithoutIndex::Min => {
                        MultipleValuesWithoutIndexOperation::<O>::get_min(values)?
                    }
                    SingleKindWithoutIndex::Mean => {
                        MultipleValuesWithoutIndexOperation::<O>::get_mean(values)?
                    }
                    SingleKindWithoutIndex::Median => {
                        MultipleValuesWithoutIndexOperation::<O>::get_median(values)?
                    }
                    SingleKindWithoutIndex::Mode => {
                        MultipleValuesWithoutIndexOperation::<O>::get_mode(values)
                    }
                    SingleKindWithoutIndex::Std => {
                        MultipleValuesWithoutIndexOperation::<O>::get_std(values)?
                    }
                    SingleKindWithoutIndex::Var => {
                        MultipleValuesWithoutIndexOperation::<O>::get_var(values)?
                    }
                    SingleKindWithoutIndex::Count => {
                        Some(MultipleValuesWithoutIndexOperation::<O>::get_count(values))
                    }
                    SingleKindWithoutIndex::Sum => {
                        MultipleValuesWithoutIndexOperation::<O>::get_sum(values)?
                    }
                    SingleKindWithoutIndex::Random => {
                        MultipleValuesWithoutIndexOperation::<O>::get_random(values)
                    }
                }
            }
            Self::MultipleAttributesWithIndexOperand(operand) => Some(GraphRecordValue::from(
                operand.evaluate_backward(graphrecord)?.count() as i64,
            )),
            Self::MultipleAttributesWithoutIndexOperand(operand) => Some(GraphRecordValue::from(
                operand.evaluate_backward(graphrecord)?.count() as i64,
            )),
            Self::IndicesOperand(operand) => {
                Some(GraphRecordValue::from(operand.count(graphrecord)?))
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
            Self::Add => write!(f, "add"),
            Self::Sub => write!(f, "sub"),
            Self::Mul => write!(f, "mul"),
            Self::Div => write!(f, "div"),
            Self::Pow => write!(f, "pow"),
            Self::Mod => write!(f, "mod"),
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
        node_indices.filter_map(move |node_index| {
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
        edge_indices.filter_map(move |edge_index| {
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
