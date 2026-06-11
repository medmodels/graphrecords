use crate::{
    BoxedIterator, EvaluateContext, EvaluateOperand, Explain, Operand,
    edges::EdgeOperand,
    execution::ExecutionContext,
    group::{Discriminator, GroupOperand, GroupedOperandContext, map_partitions},
    optimizer::{Cardinality, HasInputs, OptimizeInputs, OptimizerHints, PlanNode, Stats},
    traits::Attribute,
    values::{ValuesOperand, ValuesOperandContext},
};
use graphrecords_core::{
    GraphRecord,
    errors::GraphRecordResult,
    graphrecord::{EdgeIndex, GraphRecordAttribute, GraphRecordValue},
};

fn attribute_values<'a>(
    graphrecord: &'a GraphRecord,
    edge_indices: BoxedIterator<'a, &'a EdgeIndex>,
    attribute: &'a GraphRecordAttribute,
) -> BoxedIterator<'a, (&'a EdgeIndex, GraphRecordValue)> {
    Box::new(edge_indices.filter_map(move |edge_index| {
        let value = graphrecord
            .edge_attributes(edge_index)
            .expect("Edge must exist")
            .get(attribute)?
            .clone();

        Some((edge_index, value))
    }))
}

#[derive(PlanNode, HasInputs, OptimizeInputs, OptimizerHints, Explain)]
#[plan(operand = ValuesOperand<EdgeOperand>, optimizer_hints(distinct, empty = if_any))]
#[explain(label = "Attribute")]
pub struct AttributeContext {
    #[input]
    input: EdgeOperand,
    #[explain(label)]
    attribute: GraphRecordAttribute,
}

impl Cardinality for AttributeContext {
    fn cardinality(&self, stats: &Stats) -> usize {
        self.input.context().cardinality(stats)
    }
}

impl EvaluateContext for AttributeContext {
    type Operand = ValuesOperand<EdgeOperand>;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<<Self::Operand as EvaluateOperand>::ReturnValue<'a>> {
        let edge_indices = self.input.evaluate(graphrecord, context)?;

        Ok(attribute_values(graphrecord, edge_indices, &self.attribute))
    }
}

impl ValuesOperandContext for AttributeContext {
    type RootOperand = EdgeOperand;
}

#[derive(PlanNode, HasInputs, OptimizeInputs, OptimizerHints, Explain)]
#[plan(operand = GroupOperand<ValuesOperand<EdgeOperand>, D>)]
#[explain(label = "Attribute")]
pub struct GroupedAttributeContext<D: Discriminator> {
    #[input]
    input: GroupOperand<EdgeOperand, D>,
    #[explain(label)]
    attribute: GraphRecordAttribute,
}

impl<D: Discriminator> Cardinality for GroupedAttributeContext<D> {
    fn cardinality(&self, stats: &Stats) -> usize {
        self.input.context().cardinality(stats)
    }
}

impl<D: Discriminator> EvaluateContext for GroupedAttributeContext<D> {
    type Operand = GroupOperand<ValuesOperand<EdgeOperand>, D>;

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

impl<D: Discriminator> GroupedOperandContext<ValuesOperand<EdgeOperand>, D>
    for GroupedAttributeContext<D>
{
}

impl Attribute for EdgeOperand {
    type ReturnOperand = ValuesOperand<Self>;

    fn attribute(&self, attribute: GraphRecordAttribute) -> Self::ReturnOperand {
        ValuesOperand::new(AttributeContext {
            input: self.clone(),
            attribute,
        })
    }
}

impl<D: Discriminator> Attribute for GroupOperand<EdgeOperand, D> {
    type ReturnOperand = GroupOperand<ValuesOperand<EdgeOperand>, D>;

    fn attribute(&self, attribute: GraphRecordAttribute) -> Self::ReturnOperand {
        GroupOperand::new(GroupedAttributeContext {
            input: self.clone(),
            attribute,
        })
    }
}
