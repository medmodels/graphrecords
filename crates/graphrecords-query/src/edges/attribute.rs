use crate::{
    BoxedIterator, Operand, RootOperand,
    edges::EdgeOperand,
    execution::ExecutionContext,
    optimizer::{Cardinality, PlanNode, Stats},
    traits::Attribute,
    values::{MultipleValuesOperand, MultipleValuesOperandContext},
};
use graphrecords_core::{
    GraphRecord,
    errors::GraphRecordResult,
    graphrecord::{GraphRecordAttribute, GraphRecordValue},
};

#[derive(PlanNode)]
#[plan_node(
    crate = "crate",
    label = "Attribute",
    operand = "MultipleValuesOperand<EdgeOperand>",
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

        Ok(Box::new(edge_indices.filter_map(|edge_index| {
            let value = graphrecord
                .edge_attributes(edge_index)
                .expect("Edge must exist")
                .get(&self.attribute)?
                .clone();

            Some((edge_index, value))
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
