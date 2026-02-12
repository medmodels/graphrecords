use super::{MultipleValuesWithIndexOperand, SingleValueWithIndexOperand};
use crate::{
    GraphRecord,
    errors::GraphRecordResult,
    graphrecord::querying::{
        BoxedIterator, DeepClone, EvaluateBackward, EvaluateForwardGrouped, GroupedIterator,
        RootOperand,
        attributes::{MultipleAttributesWithIndexOperand, MultipleAttributesWithIndexOperation},
        group_by::{GroupOperand, GroupedOperand, Ungroup},
        values::{
            MultipleValuesWithIndexContext, MultipleValuesWithoutIndexContext,
            SingleKindWithoutIndex, SingleValueWithoutIndexOperand,
            operand::MultipleValuesWithoutIndexOperand,
            operation::MultipleValuesWithoutIndexOperation,
        },
        wrapper::Wrapper,
    },
};
use std::fmt::Debug;

#[derive(Debug, Clone)]
pub enum MultipleValuesWithIndexOperandContext<O: RootOperand> {
    RootOperand(GroupOperand<O>),
    MultipleAttributesOperand(GroupOperand<MultipleAttributesWithIndexOperand<O>>),
}

impl<O: RootOperand> DeepClone for MultipleValuesWithIndexOperandContext<O> {
    fn deep_clone(&self) -> Self {
        match self {
            Self::RootOperand(operand) => Self::RootOperand(operand.deep_clone()),
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

impl<O: RootOperand> GroupedOperand for SingleValueWithoutIndexOperand<O> {
    type Context = GroupOperand<MultipleValuesWithIndexOperand<O>>;
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
        let partitions = self.context.evaluate_backward(graphrecord)?;

        let values: Vec<_> = partitions
            .map(|(key, partition)| {
                let partition = partition.map(|(_, value)| value);

                let reduced_partition = match self.operand.0.read().kind {
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
                };

                Ok((key, reduced_partition))
            })
            .collect::<GraphRecordResult<_>>()?;

        self.operand
            .evaluate_forward_grouped(graphrecord, Box::new(values.into_iter()))
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
