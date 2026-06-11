use crate::{
    BoxedIterator, EvaluateContext, EvaluateOperand, Explain, Operand,
    execution::ExecutionContext,
    group::{Discriminator, GroupOperand, GroupedOperandContext, map_partitions},
    nodes::NodeOperand,
    optimizer::{Cardinality, HasInputs, OptimizeInputs, OptimizerHints, PlanNode, Stats},
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

#[derive(PlanNode, HasInputs, OptimizeInputs, OptimizerHints, Explain)]
#[plan(operand = ValuesOperand<NodeOperand>, optimizer_hints(distinct, empty = if_any))]
#[explain(label = "Attribute")]
pub struct AttributeContext {
    #[input]
    input: NodeOperand,
    #[explain(label)]
    attribute: GraphRecordAttribute,
}

impl Cardinality for AttributeContext {
    fn cardinality(&self, stats: &Stats) -> usize {
        self.input.context().cardinality(stats)
    }
}

impl EvaluateContext for AttributeContext {
    type Operand = ValuesOperand<NodeOperand>;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<<Self::Operand as EvaluateOperand>::ReturnValue<'a>> {
        let node_indices = self.input.evaluate(graphrecord, context)?;

        Ok(attribute_values(graphrecord, node_indices, &self.attribute))
    }
}

impl ValuesOperandContext for AttributeContext {
    type RootOperand = NodeOperand;
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

#[derive(PlanNode, HasInputs, OptimizeInputs, OptimizerHints, Explain)]
#[plan(operand = GroupOperand<ValuesOperand<NodeOperand>, D>)]
#[explain(label = "Attribute")]
pub struct GroupedAttributeContext<D: Discriminator> {
    #[input]
    input: GroupOperand<NodeOperand, D>,
    #[explain(label)]
    attribute: GraphRecordAttribute,
}

impl<D: Discriminator> Cardinality for GroupedAttributeContext<D> {
    fn cardinality(&self, stats: &Stats) -> usize {
        self.input.context().cardinality(stats)
    }
}

impl<D: Discriminator> EvaluateContext for GroupedAttributeContext<D> {
    type Operand = GroupOperand<ValuesOperand<NodeOperand>, D>;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<<Self::Operand as EvaluateOperand>::ReturnValue<'a>> {
        let partitions = self.input.evaluate(graphrecord, context)?;

        Ok(map_partitions(partitions, |partition| {
            attribute_values(graphrecord, partition, &self.attribute)
        }))
    }
}

impl<D: Discriminator> GroupedOperandContext<ValuesOperand<NodeOperand>, D>
    for GroupedAttributeContext<D>
{
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
