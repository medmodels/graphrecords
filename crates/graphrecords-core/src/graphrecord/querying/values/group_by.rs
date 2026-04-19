use super::{MultipleValuesWithIndexOperand, SingleValueWithIndexOperand};
use crate::{
    GraphRecord,
    errors::GraphRecordResult,
    graphrecord::{
        GraphRecordValue,
        querying::{
            BoxedIterator, CountableOperand, DeepClone, EvaluateBackward, EvaluateForwardGrouped,
            GroupedIterator, RootOperand,
            attributes::{
                AttributesTreeOperand, AttributesTreeOperation, MultipleAttributesWithIndexOperand,
                MultipleAttributesWithIndexOperation,
            },
            edges::{EdgeIndicesOperand, EdgeOperand},
            group_by::{GroupOperand, GroupedOperand, Ungroup},
            nodes::{NodeIndicesOperand, NodeOperand},
            values::{
                MultipleValuesWithIndexContext, MultipleValuesWithoutIndexContext,
                SingleKindWithoutIndex, SingleValueWithoutIndexContext,
                SingleValueWithoutIndexOperand, operand::MultipleValuesWithoutIndexOperand,
                operation::MultipleValuesWithoutIndexOperation,
            },
            wrapper::Wrapper,
        },
    },
};
use std::fmt::Debug;

#[derive(Debug, Clone)]
#[allow(clippy::enum_variant_names)]
pub enum MultipleValuesWithIndexOperandContext<O: RootOperand> {
    RootOperand(GroupOperand<O>),
    AttributesTreeOperand(GroupOperand<AttributesTreeOperand<O>>),
    MultipleAttributesOperand(GroupOperand<MultipleAttributesWithIndexOperand<O>>),
}

impl<O: RootOperand> DeepClone for MultipleValuesWithIndexOperandContext<O> {
    fn deep_clone(&self) -> Self {
        match self {
            Self::RootOperand(operand) => Self::RootOperand(operand.deep_clone()),
            Self::AttributesTreeOperand(operand) => {
                Self::AttributesTreeOperand(operand.deep_clone())
            }
            Self::MultipleAttributesOperand(operand) => {
                Self::MultipleAttributesOperand(operand.deep_clone())
            }
        }
    }
}

impl<O: RootOperand> From<GroupOperand<O>> for MultipleValuesWithIndexOperandContext<O> {
    fn from(operand: GroupOperand<O>) -> Self {
        Self::RootOperand(operand)
    }
}

impl<O: RootOperand> From<GroupOperand<AttributesTreeOperand<O>>>
    for MultipleValuesWithIndexOperandContext<O>
{
    fn from(operand: GroupOperand<AttributesTreeOperand<O>>) -> Self {
        Self::AttributesTreeOperand(operand)
    }
}

impl<O: RootOperand> From<GroupOperand<MultipleAttributesWithIndexOperand<O>>>
    for MultipleValuesWithIndexOperandContext<O>
{
    fn from(operand: GroupOperand<MultipleAttributesWithIndexOperand<O>>) -> Self {
        Self::MultipleAttributesOperand(operand)
    }
}

impl<O: RootOperand> GroupedOperand for MultipleValuesWithIndexOperand<O> {
    type Context = MultipleValuesWithIndexOperandContext<O>;
}

impl<'a, O> EvaluateBackward<'a> for GroupOperand<MultipleValuesWithIndexOperand<O>>
where
    O: RootOperand + 'a,
{
    type ReturnValue = GroupedIterator<
        'a,
        <MultipleValuesWithIndexOperand<O> as EvaluateBackward<'a>>::ReturnValue,
    >;

    fn evaluate_backward(
        &self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<Self::ReturnValue> {
        match &self.context {
            MultipleValuesWithIndexOperandContext::RootOperand(context) => {
                let partitions = context.evaluate_backward(graphrecord)?;

                let values: Vec<_> = partitions
                    .map(|(key, partition)| {
                        let MultipleValuesWithIndexContext::Operand((_, attribute)) =
                            &self.operand.0.read().context
                        else {
                            unreachable!()
                        };

                        let reduced_partition: BoxedIterator<_> = Box::new(
                            O::get_values_from_indices(graphrecord, attribute.clone(), partition),
                        );

                        (key, reduced_partition)
                    })
                    .collect();

                self.operand
                    .evaluate_forward_grouped(graphrecord, Box::new(values.into_iter()))
            }
            MultipleValuesWithIndexOperandContext::AttributesTreeOperand(context) => {
                let partitions = context.evaluate_backward(graphrecord)?;

                let values = partitions.map(|(key, partition)| {
                    let reduced_partition: BoxedIterator<_> =
                        Box::new(AttributesTreeOperation::<O>::get_count(partition));

                    (key, reduced_partition)
                });

                self.operand
                    .evaluate_forward_grouped(graphrecord, Box::new(values))
            }
            MultipleValuesWithIndexOperandContext::MultipleAttributesOperand(context) => {
                let partitions = context.evaluate_backward(graphrecord)?;

                let values: Vec<_> = partitions
                    .map(|(key, partition)| {
                        let reduced_partition: BoxedIterator<_> =
                            Box::new(MultipleAttributesWithIndexOperation::<O>::get_values(
                                graphrecord,
                                partition,
                            )?);

                        Ok((key, reduced_partition))
                    })
                    .collect::<GraphRecordResult<_>>()?;

                self.operand
                    .evaluate_forward_grouped(graphrecord, Box::new(values.into_iter()))
            }
        }
    }
}

impl<O: RootOperand> Ungroup for GroupOperand<MultipleValuesWithIndexOperand<O>> {
    type OutputOperand = MultipleValuesWithIndexOperand<O>;

    fn ungroup(&self) -> Wrapper<Self::OutputOperand> {
        let operand = Wrapper::<Self::OutputOperand>::new(
            MultipleValuesWithIndexContext::MultipleValuesWithIndexGroupByOperand(
                self.deep_clone(),
            ),
        );

        self.operand.push_merge_operation(operand.clone());

        operand
    }
}

impl<O: RootOperand> GroupedOperand for SingleValueWithIndexOperand<O> {
    type Context = GroupOperand<MultipleValuesWithIndexOperand<O>>;
}

impl<'a, O: 'a + RootOperand> EvaluateBackward<'a>
    for GroupOperand<SingleValueWithIndexOperand<O>>
{
    type ReturnValue =
        GroupedIterator<'a, <SingleValueWithIndexOperand<O> as EvaluateBackward<'a>>::ReturnValue>;

    fn evaluate_backward(
        &self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<Self::ReturnValue> {
        let partitions = self.context.evaluate_backward(graphrecord)?;

        let values: Vec<_> = partitions
            .map(|(key, partition)| {
                let reduced_partition = self.operand.reduce_input(partition)?;

                Ok((key, reduced_partition))
            })
            .collect::<GraphRecordResult<_>>()?;

        self.operand
            .evaluate_forward_grouped(graphrecord, Box::new(values.into_iter()))
    }
}

impl<O: RootOperand> Ungroup for GroupOperand<SingleValueWithIndexOperand<O>> {
    type OutputOperand = MultipleValuesWithIndexOperand<O>;

    fn ungroup(&self) -> Wrapper<Self::OutputOperand> {
        let operand = Wrapper::<Self::OutputOperand>::new(
            MultipleValuesWithIndexContext::SingleValueWithIndexGroupByOperand(self.deep_clone()),
        );

        self.operand.push_merge_operation(operand.clone());

        operand
    }
}

#[derive(Debug, Clone)]
pub enum SingleValueWithoutIndexOperandContext<O: RootOperand> {
    MultipleValuesWithIndexOperand(GroupOperand<MultipleValuesWithIndexOperand<O>>),
    MultipleAttributesWithIndexOperand(GroupOperand<MultipleAttributesWithIndexOperand<O>>),
    IndicesOperand(GroupOperand<<O as RootOperand>::IndicesOperand>),
}

impl<O: RootOperand> DeepClone for SingleValueWithoutIndexOperandContext<O>
where
    GroupOperand<<O as RootOperand>::IndicesOperand>: DeepClone,
{
    fn deep_clone(&self) -> Self {
        match self {
            Self::MultipleValuesWithIndexOperand(operand) => {
                Self::MultipleValuesWithIndexOperand(operand.deep_clone())
            }
            Self::MultipleAttributesWithIndexOperand(operand) => {
                Self::MultipleAttributesWithIndexOperand(operand.deep_clone())
            }
            Self::IndicesOperand(operand) => Self::IndicesOperand(operand.deep_clone()),
        }
    }
}

impl<O: RootOperand> From<GroupOperand<MultipleValuesWithIndexOperand<O>>>
    for SingleValueWithoutIndexOperandContext<O>
{
    fn from(operand: GroupOperand<MultipleValuesWithIndexOperand<O>>) -> Self {
        Self::MultipleValuesWithIndexOperand(operand)
    }
}

impl<O: RootOperand> From<GroupOperand<MultipleAttributesWithIndexOperand<O>>>
    for SingleValueWithoutIndexOperandContext<O>
{
    fn from(operand: GroupOperand<MultipleAttributesWithIndexOperand<O>>) -> Self {
        Self::MultipleAttributesWithIndexOperand(operand)
    }
}

impl From<GroupOperand<NodeIndicesOperand>> for SingleValueWithoutIndexOperandContext<NodeOperand> {
    fn from(operand: GroupOperand<NodeIndicesOperand>) -> Self {
        Self::IndicesOperand(operand)
    }
}

impl From<GroupOperand<EdgeIndicesOperand>> for SingleValueWithoutIndexOperandContext<EdgeOperand> {
    fn from(operand: GroupOperand<EdgeIndicesOperand>) -> Self {
        Self::IndicesOperand(operand)
    }
}

impl<O: RootOperand> GroupedOperand for SingleValueWithoutIndexOperand<O> {
    type Context = SingleValueWithoutIndexOperandContext<O>;
}

impl<'a, O: 'a + RootOperand> EvaluateBackward<'a>
    for GroupOperand<SingleValueWithoutIndexOperand<O>>
{
    type ReturnValue = GroupedIterator<
        'a,
        <SingleValueWithoutIndexOperand<O> as EvaluateBackward<'a>>::ReturnValue,
    >;

    fn evaluate_backward(
        &self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<Self::ReturnValue> {
        match &self.context {
            SingleValueWithoutIndexOperandContext::MultipleValuesWithIndexOperand(context) => {
                let partitions = context.evaluate_backward(graphrecord)?;

                let values: Vec<_> = {
                    let operand_guard = self.operand.0.read();

                    partitions
                        .map(|(key, partition)| {
                            let partition = partition.map(|(_, value)| value);

                            let reduced_partition = match &operand_guard.context {
                        SingleValueWithoutIndexContext::MultipleValuesWithIndexOperand {
                            kind,
                            ..
                        }
                        | SingleValueWithoutIndexContext::MultipleValuesWithoutIndexOperand {
                            kind,
                            ..
                        } => match kind {
                            SingleKindWithoutIndex::Max => {
                                MultipleValuesWithoutIndexOperation::<O>::get_max(partition)?
                            }
                            SingleKindWithoutIndex::Min => {
                                MultipleValuesWithoutIndexOperation::<O>::get_min(partition)?
                            }
                            SingleKindWithoutIndex::Mean => {
                                MultipleValuesWithoutIndexOperation::<O>::get_mean(partition)?
                            }
                            SingleKindWithoutIndex::Median => {
                                MultipleValuesWithoutIndexOperation::<O>::get_median(partition)?
                            }
                            SingleKindWithoutIndex::Mode => {
                                MultipleValuesWithoutIndexOperation::<O>::get_mode(partition)
                            }
                            SingleKindWithoutIndex::Std => {
                                MultipleValuesWithoutIndexOperation::<O>::get_std(partition)?
                            }
                            SingleKindWithoutIndex::Var => {
                                MultipleValuesWithoutIndexOperation::<O>::get_var(partition)?
                            }
                            SingleKindWithoutIndex::Count => Some(
                                MultipleValuesWithoutIndexOperation::<O>::get_count(partition),
                            ),
                            SingleKindWithoutIndex::Sum => {
                                MultipleValuesWithoutIndexOperation::<O>::get_sum(partition)?
                            }
                            SingleKindWithoutIndex::Random => {
                                MultipleValuesWithoutIndexOperation::<O>::get_random(partition)
                            }
                        },
                        SingleValueWithoutIndexContext::MultipleAttributesWithIndexOperand(_)
                        | SingleValueWithoutIndexContext::MultipleAttributesWithoutIndexOperand(
                            _,
                        )
                        | SingleValueWithoutIndexContext::IndicesOperand(_) => Some(
                            GraphRecordValue::from(partition.count() as i64),
                        ),
                    };

                            Ok((key, reduced_partition))
                        })
                        .collect::<GraphRecordResult<_>>()?
                };

                self.operand
                    .evaluate_forward_grouped(graphrecord, Box::new(values.into_iter()))
            }
            SingleValueWithoutIndexOperandContext::MultipleAttributesWithIndexOperand(context) => {
                let partitions = context.evaluate_backward(graphrecord)?;

                let values: Vec<_> = partitions
                    .map(|(key, partition)| {
                        (key, Some(GraphRecordValue::from(partition.count() as i64)))
                    })
                    .collect();

                self.operand
                    .evaluate_forward_grouped(graphrecord, Box::new(values.into_iter()))
            }
            SingleValueWithoutIndexOperandContext::IndicesOperand(context) => {
                let counts = <O::IndicesOperand as CountableOperand>::count_per_partition(
                    context,
                    graphrecord,
                )?;

                let values = counts
                    .into_iter()
                    .map(|(key, count)| (key, Some(GraphRecordValue::from(count))));

                self.operand
                    .evaluate_forward_grouped(graphrecord, Box::new(values))
            }
        }
    }
}

impl<O: RootOperand> Ungroup for GroupOperand<SingleValueWithoutIndexOperand<O>> {
    type OutputOperand = MultipleValuesWithoutIndexOperand<O>;

    fn ungroup(&self) -> Wrapper<Self::OutputOperand> {
        let operand = Wrapper::<Self::OutputOperand>::new(
            MultipleValuesWithoutIndexContext::GroupByOperand(self.deep_clone()),
        );

        self.operand.push_merge_operation(operand.clone());

        operand
    }
}
