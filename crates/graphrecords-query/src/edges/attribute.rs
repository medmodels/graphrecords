use crate::{
    BoxedIterator, Operand, RootOperand,
    edges::EdgeOperand,
    execution::ExecutionContext,
    group::{
        Discriminator, GroupOperand, GroupableOperand, GroupedIterator, GroupedOperandContext,
    },
    optimizer::{Cardinality, PlanNode, Stats},
    traits::Attribute,
    values::{MultipleValuesOperand, MultipleValuesOperandContext},
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

#[derive(PlanNode)]
#[plan_node(
    crate = "crate",
    label = "Attribute",
    operand = MultipleValuesOperand<EdgeOperand>,
    distinct,
    empty = "if_any"
)]
pub struct AttributeContext {
    #[plan_node(input)]
    input: EdgeOperand,
    #[plan_node(describe)]
    attribute: GraphRecordAttribute,
}

impl Cardinality for AttributeContext {
    fn cardinality(&self, stats: &Stats) -> usize {
        self.input.context().cardinality(stats)
    }
}

impl MultipleValuesOperandContext for AttributeContext {
    type Operand = EdgeOperand;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<
        BoxedIterator<'a, (<Self::Operand as RootOperand>::Index<'a>, GraphRecordValue)>,
    > {
        let edge_indices = self.input.evaluate(graphrecord, context)?;

        Ok(attribute_values(graphrecord, edge_indices, &self.attribute))
    }
}

#[derive(PlanNode)]
#[plan_node(
    crate = "crate",
    label = "Attribute",
    operand = GroupOperand<MultipleValuesOperand<EdgeOperand>, D>
)]
pub struct GroupedAttributeContext<D: Discriminator> {
    #[plan_node(input)]
    input: GroupOperand<EdgeOperand, D>,
    #[plan_node(describe)]
    attribute: GraphRecordAttribute,
}

impl<D: Discriminator> GroupedOperandContext<MultipleValuesOperand<EdgeOperand>, D>
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
            <MultipleValuesOperand<EdgeOperand> as GroupableOperand>::Grouped<'a>,
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

impl Attribute for EdgeOperand {
    type ReturnOperand = MultipleValuesOperand<Self>;

    fn attribute(&self, attribute: GraphRecordAttribute) -> Self::ReturnOperand {
        MultipleValuesOperand::new(AttributeContext {
            input: self.clone(),
            attribute,
        })
    }
}

impl<D: Discriminator> Attribute for GroupOperand<EdgeOperand, D> {
    type ReturnOperand = GroupOperand<MultipleValuesOperand<EdgeOperand>, D>;

    fn attribute(&self, attribute: GraphRecordAttribute) -> Self::ReturnOperand {
        GroupOperand::new(GroupedAttributeContext {
            input: self.clone(),
            attribute,
        })
    }
}
