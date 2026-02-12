use crate::{
    GraphRecord,
    errors::GraphRecordResult,
    graphrecord::querying::{
        BoxedIterator, DeepClone, EvaluateBackward, EvaluateForwardGrouped, GroupedIterator,
        RootOperand,
        attributes::{
            AttributesTreeContext, AttributesTreeOperand, MultipleAttributesWithIndexContext,
            MultipleAttributesWithIndexOperand, MultipleAttributesWithIndexOperation,
            MultipleAttributesWithoutIndexContext, MultipleAttributesWithoutIndexOperand,
            MultipleAttributesWithoutIndexOperation, MultipleKind, SingleAttributeWithIndexOperand,
            SingleAttributeWithoutIndexOperand, SingleKindWithIndex, SingleKindWithoutIndex,
            operation::AttributesTreeOperation,
        },
        group_by::{GroupOperand, GroupedOperand, Ungroup},
        wrapper::Wrapper,
    },
};

impl<O: RootOperand> GroupedOperand for AttributesTreeOperand<O> {
    type Context = GroupOperand<O>;
}

impl<'a, O: 'a + RootOperand> EvaluateBackward<'a> for GroupOperand<AttributesTreeOperand<O>> {
    type ReturnValue =
        GroupedIterator<'a, <AttributesTreeOperand<O> as EvaluateBackward<'a>>::ReturnValue>;

    fn evaluate_backward(
        &self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<Self::ReturnValue> {
        let partitions = self.context.evaluate_backward(graphrecord)?;

        let attributes: Vec<_> = partitions
            .map(|(key, partition)| {
                let reduced_partition: BoxedIterator<_> =
                    Box::new(O::get_attributes_from_indices(graphrecord, partition));

                Ok((key, reduced_partition))
            })
            .collect::<GraphRecordResult<_>>()?;

        self.operand
            .evaluate_forward_grouped(graphrecord, Box::new(attributes.into_iter()))
    }
}

impl<O: RootOperand> Ungroup for GroupOperand<AttributesTreeOperand<O>> {
    type OutputOperand = AttributesTreeOperand<O>;

    fn ungroup(&self) -> Wrapper<Self::OutputOperand> {
        let operand = Wrapper::<Self::OutputOperand>::new(AttributesTreeContext::GroupByOperand(
            self.deep_clone(),
        ));

        self.operand.push_merge_operation(operand.clone());

        operand
    }
}

impl<O: RootOperand> GroupedOperand for MultipleAttributesWithIndexOperand<O> {
    type Context = GroupOperand<AttributesTreeOperand<O>>;
}

impl<'a, O: 'a + RootOperand> EvaluateBackward<'a>
    for GroupOperand<MultipleAttributesWithIndexOperand<O>>
{
    type ReturnValue = GroupedIterator<
        'a,
        <MultipleAttributesWithIndexOperand<O> as EvaluateBackward<'a>>::ReturnValue,
    >;

    fn evaluate_backward(
        &self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<Self::ReturnValue> {
        let partitions = self.context.evaluate_backward(graphrecord)?;

        let MultipleAttributesWithIndexContext::AttributesTree {
            operand: _,
            ref kind,
        } = self.operand.0.read().context
        else {
            unreachable!()
        };

        let attributes: Vec<_> = partitions
            .map(|(key, partition)| {
                let reduced_partition: BoxedIterator<_> = match kind {
                    MultipleKind::Max => {
                        Box::new(AttributesTreeOperation::<O>::get_max(partition)?)
                    }
                    MultipleKind::Min => {
                        Box::new(AttributesTreeOperation::<O>::get_min(partition)?)
                    }
                    MultipleKind::Count => {
                        Box::new(AttributesTreeOperation::<O>::get_count(partition))
                    }
                    MultipleKind::Sum => {
                        Box::new(AttributesTreeOperation::<O>::get_sum(partition)?)
                    }
                    MultipleKind::Random => {
                        Box::new(AttributesTreeOperation::<O>::get_random(partition)?)
                    }
                };

                Ok((key, reduced_partition))
            })
            .collect::<GraphRecordResult<_>>()?;

        self.operand
            .evaluate_forward_grouped(graphrecord, Box::new(attributes.into_iter()))
    }
}

impl<O: RootOperand> Ungroup for GroupOperand<MultipleAttributesWithIndexOperand<O>> {
    type OutputOperand = MultipleAttributesWithIndexOperand<O>;

    fn ungroup(&self) -> Wrapper<Self::OutputOperand> {
        let operand = Wrapper::<Self::OutputOperand>::new(
            MultipleAttributesWithIndexContext::MultipleAttributesWithIndexGroupByOperand(
                self.deep_clone(),
            ),
        );

        self.operand.push_merge_operation(operand.clone());

        operand
    }
}

impl<O: RootOperand> GroupedOperand for SingleAttributeWithIndexOperand<O> {
    type Context = GroupOperand<MultipleAttributesWithIndexOperand<O>>;
}

impl<'a, O: 'a + RootOperand> EvaluateBackward<'a>
    for GroupOperand<SingleAttributeWithIndexOperand<O>>
{
    type ReturnValue = GroupedIterator<
        'a,
        <SingleAttributeWithIndexOperand<O> as EvaluateBackward<'a>>::ReturnValue,
    >;

    fn evaluate_backward(
        &self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<Self::ReturnValue> {
        let partitiions = self.context.evaluate_backward(graphrecord)?;

        let attributes: Vec<_> = partitiions
            .map(|(key, partition)| {
                let reduced_partition = match self.operand.0.read().kind {
                    SingleKindWithIndex::Max => {
                        MultipleAttributesWithIndexOperation::<O>::get_max(partition)?
                    }
                    SingleKindWithIndex::Min => {
                        MultipleAttributesWithIndexOperation::<O>::get_min(partition)?
                    }
                    SingleKindWithIndex::Random => {
                        MultipleAttributesWithIndexOperation::<O>::get_random(partition)
                    }
                };

                Ok((key, reduced_partition))
            })
            .collect::<GraphRecordResult<_>>()?;

        self.operand
            .evaluate_forward_grouped(graphrecord, Box::new(attributes.into_iter()))
    }
}

impl<O: RootOperand> Ungroup for GroupOperand<SingleAttributeWithIndexOperand<O>> {
    type OutputOperand = MultipleAttributesWithIndexOperand<O>;

    fn ungroup(&self) -> Wrapper<Self::OutputOperand> {
        let operand = Wrapper::<Self::OutputOperand>::new(
            MultipleAttributesWithIndexContext::SingleAttributeWithIndexGroupByOperand(
                self.deep_clone(),
            ),
        );

        self.operand.push_merge_operation(operand.clone());

        operand
    }
}

impl<O: RootOperand> GroupedOperand for SingleAttributeWithoutIndexOperand<O> {
    type Context = GroupOperand<MultipleAttributesWithIndexOperand<O>>;
}

impl<'a, O: 'a + RootOperand> EvaluateBackward<'a>
    for GroupOperand<SingleAttributeWithoutIndexOperand<O>>
{
    type ReturnValue = GroupedIterator<
        'a,
        <SingleAttributeWithoutIndexOperand<O> as EvaluateBackward<'a>>::ReturnValue,
    >;

    fn evaluate_backward(
        &self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<Self::ReturnValue> {
        let partitiions = self.context.evaluate_backward(graphrecord)?;

        let attributes: Vec<_> = partitiions
            .map(|(key, partition)| {
                let partition = partition.map(|(_, attribute)| attribute);

                let reduced_partition = match self.operand.0.read().kind {
                    SingleKindWithoutIndex::Max => {
                        MultipleAttributesWithoutIndexOperation::<O>::get_max(partition)?
                    }
                    SingleKindWithoutIndex::Min => {
                        MultipleAttributesWithoutIndexOperation::<O>::get_min(partition)?
                    }
                    SingleKindWithoutIndex::Count => Some(
                        MultipleAttributesWithoutIndexOperation::<O>::get_count(partition),
                    ),
                    SingleKindWithoutIndex::Sum => {
                        MultipleAttributesWithoutIndexOperation::<O>::get_sum(partition)?
                    }
                    SingleKindWithoutIndex::Random => {
                        MultipleAttributesWithoutIndexOperation::<O>::get_random(partition)
                    }
                };

                Ok((key, reduced_partition))
            })
            .collect::<GraphRecordResult<_>>()?;

        self.operand
            .evaluate_forward_grouped(graphrecord, Box::new(attributes.into_iter()))
    }
}

impl<O: RootOperand> Ungroup for GroupOperand<SingleAttributeWithoutIndexOperand<O>> {
    type OutputOperand = MultipleAttributesWithoutIndexOperand<O>;

    fn ungroup(&self) -> Wrapper<Self::OutputOperand> {
        let operand = Wrapper::<Self::OutputOperand>::new(
            MultipleAttributesWithoutIndexContext::GroupByOperand(self.deep_clone()),
        );

        self.operand.push_merge_operation(operand.clone());

        operand
    }
}
