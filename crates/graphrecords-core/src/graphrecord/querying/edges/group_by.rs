use super::EdgeOperand;
use crate::{
    GraphRecord,
    errors::GraphRecordResult,
    graphrecord::querying::{
        BoxedIterator, DeepClone, EvaluateBackward, EvaluateForward, EvaluateForwardGrouped,
        GroupedIterator,
        edges::{EdgeIndexOperand, EdgeIndicesOperand, EdgeIndicesOperandContext},
        group_by::{GroupBy, GroupOperand, GroupedOperand, PartitionGroups, Ungroup},
        nodes::NodeOperand,
        wrapper::Wrapper,
    },
    prelude::GraphRecordAttribute,
};

#[derive(Debug, Clone)]
pub enum EdgeOperandContext {
    Discriminator(<EdgeOperand as GroupBy>::Discriminator),
    Edges(GroupOperand<NodeOperand>),
}

impl DeepClone for EdgeOperandContext {
    fn deep_clone(&self) -> Self {
        match self {
            Self::Discriminator(discriminator) => Self::Discriminator(discriminator.clone()),
            Self::Edges(operand) => Self::Edges(operand.deep_clone()),
        }
    }
}

impl From<<EdgeOperand as GroupBy>::Discriminator> for EdgeOperandContext {
    fn from(discriminator: <EdgeOperand as GroupBy>::Discriminator) -> Self {
        Self::Discriminator(discriminator)
    }
}

impl From<GroupOperand<NodeOperand>> for EdgeOperandContext {
    fn from(operand: GroupOperand<NodeOperand>) -> Self {
        Self::Edges(operand)
    }
}

impl GroupedOperand for EdgeOperand {
    type Context = EdgeOperandContext;
}

#[derive(Debug, Clone)]
pub enum EdgeOperandGroupDiscriminator {
    SourceNode,
    TargetNode,
    Parallel,
    Attribute(GraphRecordAttribute),
}

impl DeepClone for EdgeOperandGroupDiscriminator {
    fn deep_clone(&self) -> Self {
        match self {
            Self::SourceNode => Self::SourceNode,
            Self::TargetNode => Self::TargetNode,
            Self::Parallel => Self::Parallel,
            Self::Attribute(attr) => Self::Attribute(attr.clone()),
        }
    }
}

impl<'a> EvaluateForward<'a> for GroupOperand<EdgeOperand> {
    type InputValue = <EdgeOperand as EvaluateForward<'a>>::InputValue;
    type ReturnValue = GroupedIterator<'a, <EdgeOperand as EvaluateForward<'a>>::ReturnValue>;

    fn evaluate_forward(
        &self,
        graphrecord: &'a GraphRecord,
        indices: Self::InputValue,
    ) -> GraphRecordResult<Self::ReturnValue> {
        match &self.context {
            EdgeOperandContext::Discriminator(discriminator) => {
                let partitions = EdgeOperand::partition(graphrecord, indices, discriminator);

                self.operand
                    .evaluate_forward_grouped(graphrecord, Box::new(partitions))
            }
            EdgeOperandContext::Edges(_) => unreachable!(),
        }
    }
}

impl GroupedOperand for EdgeIndicesOperand {
    type Context = GroupOperand<EdgeOperand>;
}

impl<'a> EvaluateBackward<'a> for GroupOperand<EdgeIndicesOperand> {
    type ReturnValue =
        GroupedIterator<'a, <EdgeIndicesOperand as EvaluateBackward<'a>>::ReturnValue>;

    fn evaluate_backward(
        &self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<Self::ReturnValue> {
        let partitions = self.context.evaluate_backward(graphrecord)?;

        let indices: Vec<_> = partitions
            .map(|(key, partition)| {
                let reduced_partition: BoxedIterator<_> = Box::new(partition.copied());

                Ok((key, reduced_partition))
            })
            .collect::<GraphRecordResult<_>>()?;

        self.operand
            .evaluate_forward_grouped(graphrecord, Box::new(indices.into_iter()))
    }
}

impl Ungroup for GroupOperand<EdgeIndicesOperand> {
    type OutputOperand = EdgeIndicesOperand;

    fn ungroup(&self) -> Wrapper<Self::OutputOperand> {
        let operand = Wrapper::<Self::OutputOperand>::new(
            EdgeIndicesOperandContext::EdgeIndicesGroupByOperand(self.deep_clone()),
        );

        self.operand.push_merge_operation(operand.clone());

        operand
    }
}

impl GroupedOperand for EdgeIndexOperand {
    type Context = GroupOperand<EdgeIndicesOperand>;
}

impl<'a> EvaluateBackward<'a> for GroupOperand<EdgeIndexOperand> {
    type ReturnValue = GroupedIterator<'a, <EdgeIndexOperand as EvaluateBackward<'a>>::ReturnValue>;

    fn evaluate_backward(
        &self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<Self::ReturnValue> {
        let partitions = self.context.evaluate_backward(graphrecord)?;

        let indices: Vec<_> = partitions
            .map(|(key, partition)| {
                let reduced_partition = self.operand.reduce_input(partition)?;

                Ok((key, reduced_partition))
            })
            .collect::<GraphRecordResult<_>>()?;

        self.operand
            .evaluate_forward_grouped(graphrecord, Box::new(indices.into_iter()))
    }
}

impl Ungroup for GroupOperand<EdgeIndexOperand> {
    type OutputOperand = EdgeIndicesOperand;

    fn ungroup(&self) -> Wrapper<Self::OutputOperand> {
        let operand = Wrapper::<Self::OutputOperand>::new(
            EdgeIndicesOperandContext::EdgeIndexGroupByOperand(self.deep_clone()),
        );

        self.operand.push_merge_operation(operand.clone());

        operand
    }
}
