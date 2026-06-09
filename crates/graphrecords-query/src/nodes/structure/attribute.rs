use crate::{
    BoxedIterator, Operand, RootOperand,
    execution::ExecutionContext,
    group::{
        Discriminator, GroupOperand, GroupableOperand, GroupedIterator, GroupedOperandContext,
    },
    nodes::NodeOperand,
    optimizer::{Cardinality, Explain, OptimizerHints, PlanNode, Stats},
    traits::Attribute,
    values::{ValuesOperand, ValuesOperandContext},
};
use graphrecords_core::{
    GraphRecord,
    errors::GraphRecordResult,
    graphrecord::{GraphRecordAttribute, GraphRecordValue, NodeIndex},
};

fn attribute_values<'a>(
    graphrecord: &'a GraphRecord,
    node_indices: BoxedIterator<'a, &'a NodeIndex>,
    attribute: &'a GraphRecordAttribute,
) -> BoxedIterator<'a, (&'a NodeIndex, GraphRecordValue)> {
    Box::new(node_indices.filter_map(move |node_index| {
        let value = graphrecord
            .node_attributes(node_index)
            .expect("Node must exist")
            .get(attribute)?
            .clone();

        Some((node_index, value))
    }))
}

#[derive(PlanNode, OptimizerHints, Explain)]
#[plan_node(operand = ValuesOperand<NodeOperand>)]
#[optimizer_hints(distinct, empty = if_any)]
#[explain(label = "Attribute")]
pub struct AttributeContext {
    #[plan_node(input)]
    input: NodeOperand,
    #[explain]
    attribute: GraphRecordAttribute,
}

impl Cardinality for AttributeContext {
    fn cardinality(&self, stats: &Stats) -> usize {
        self.input.context().cardinality(stats)
    }
}

impl ValuesOperandContext for AttributeContext {
    type Operand = NodeOperand;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<
        BoxedIterator<'a, (<Self::Operand as RootOperand>::Index<'a>, GraphRecordValue)>,
    > {
        let node_indices = self.input.evaluate(graphrecord, context)?;

        Ok(attribute_values(graphrecord, node_indices, &self.attribute))
    }
}

#[derive(PlanNode, OptimizerHints, Explain)]
#[plan_node(operand = GroupOperand<ValuesOperand<NodeOperand>, D>)]
#[explain(label = "Attribute")]
pub struct GroupedAttributeContext<D: Discriminator> {
    #[plan_node(input)]
    input: GroupOperand<NodeOperand, D>,
    #[explain]
    attribute: GraphRecordAttribute,
}

impl<D: Discriminator> GroupedOperandContext<ValuesOperand<NodeOperand>, D>
    for GroupedAttributeContext<D>
{
    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<
        GroupedIterator<
            'a,
            D::Key<'a>,
            <ValuesOperand<NodeOperand> as GroupableOperand>::Grouped<'a>,
        >,
    > {
        let partitions = self.input.evaluate(graphrecord, context)?;

        Ok(Box::new(partitions.map(move |(key, partition)| {
            (
                key,
                attribute_values(graphrecord, partition, &self.attribute),
            )
        })))
    }
}

impl Attribute for NodeOperand {
    type ReturnOperand = ValuesOperand<Self>;

    fn attribute(&self, attribute: GraphRecordAttribute) -> Self::ReturnOperand {
        ValuesOperand::new(AttributeContext {
            input: self.clone(),
            attribute,
        })
    }
}

impl<D: Discriminator> Attribute for GroupOperand<NodeOperand, D> {
    type ReturnOperand = GroupOperand<ValuesOperand<NodeOperand>, D>;

    fn attribute(&self, attribute: GraphRecordAttribute) -> Self::ReturnOperand {
        GroupOperand::new(GroupedAttributeContext {
            input: self.clone(),
            attribute,
        })
    }
}
