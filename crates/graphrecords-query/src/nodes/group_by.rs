use crate::{
    EvaluateContext, EvaluateOperand, Explain,
    execution::ExecutionContext,
    group::{
        AttributeDiscriminator, Discriminator, GroupBy, GroupOperand, GroupedOperandContext,
        map_partitions, partition_by,
    },
    nodes::NodeOperand,
    optimizer::{
        Cardinality, HasInputs, NodeAttributeCardinality, OptimizeInputs, OptimizerHints, PlanNode,
        Stats,
    },
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult};

#[derive(PlanNode, HasInputs, OptimizeInputs, OptimizerHints, Explain)]
#[plan(operand = GroupOperand<NodeOperand, AttributeDiscriminator>)]
#[explain(label = "GroupBy")]
struct NodeGroupByAttributeContext {
    #[input]
    input: NodeOperand,
    #[explain(label)]
    discriminator: AttributeDiscriminator,
}

impl Cardinality for NodeGroupByAttributeContext {
    fn cardinality(&self, stats: &Stats) -> usize {
        stats.get::<NodeAttributeCardinality>(&self.discriminator.attribute)
    }
}

impl GroupedOperandContext<NodeOperand, AttributeDiscriminator> for NodeGroupByAttributeContext {}

impl EvaluateContext for NodeGroupByAttributeContext {
    type Operand = GroupOperand<NodeOperand, AttributeDiscriminator>;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<<Self::Operand as EvaluateOperand>::ReturnValue<'a>> {
        let node_indices = self.input.evaluate(graphrecord, context)?;
        let attribute = &self.discriminator.attribute;

        Ok(partition_by(node_indices, |&node_index| {
            graphrecord
                .node_attributes(node_index)
                .expect("Node must exist")
                .get(attribute)
        }))
    }
}

impl GroupBy<AttributeDiscriminator> for NodeOperand {
    type Output = GroupOperand<Self, AttributeDiscriminator>;

    fn group_by(&self, discriminator: AttributeDiscriminator) -> Self::Output {
        GroupOperand::new(NodeGroupByAttributeContext {
            input: self.clone(),
            discriminator,
        })
    }
}

#[derive(PlanNode, HasInputs, OptimizeInputs, OptimizerHints, Explain)]
#[plan(operand = GroupOperand<GroupOperand<NodeOperand, AttributeDiscriminator>, D>)]
#[explain(label = "GroupBy")]
struct GroupedNodeGroupByAttributeContext<D: Discriminator> {
    #[input]
    input: GroupOperand<NodeOperand, D>,
    #[explain(label)]
    discriminator: AttributeDiscriminator,
}

impl<D: Discriminator> Cardinality for GroupedNodeGroupByAttributeContext<D> {
    fn cardinality(&self, stats: &Stats) -> usize {
        stats.get::<NodeAttributeCardinality>(&self.discriminator.attribute)
    }
}

impl<D: Discriminator> GroupedOperandContext<GroupOperand<NodeOperand, AttributeDiscriminator>, D>
    for GroupedNodeGroupByAttributeContext<D>
{
}

impl<D: Discriminator> EvaluateContext for GroupedNodeGroupByAttributeContext<D> {
    type Operand = GroupOperand<GroupOperand<NodeOperand, AttributeDiscriminator>, D>;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<<Self::Operand as EvaluateOperand>::ReturnValue<'a>> {
        let partitions = self.input.evaluate(graphrecord, context)?;
        let attribute = &self.discriminator.attribute;

        Ok(map_partitions(partitions, |partition| {
            partition_by(partition, |&node_index| {
                graphrecord
                    .node_attributes(node_index)
                    .expect("Node must exist")
                    .get(attribute)
            })
        }))
    }
}

impl<D: Discriminator> GroupBy<AttributeDiscriminator> for GroupOperand<NodeOperand, D> {
    type Output = GroupOperand<GroupOperand<NodeOperand, AttributeDiscriminator>, D>;

    fn group_by(&self, discriminator: AttributeDiscriminator) -> Self::Output {
        GroupOperand::new(GroupedNodeGroupByAttributeContext {
            input: self.clone(),
            discriminator,
        })
    }
}
