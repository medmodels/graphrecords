use crate::{
    BoxedIterator, Operand, RootOperand,
    execution::ExecutionContext,
    nodes::NodeOperand,
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
    operand = "MultipleValuesOperand<NodeOperand>",
    distinct,
    empty = "if_any"
)]
pub struct AttributeContext {
    #[plan_node(input)]
    input: NodeOperand,
    #[plan_node(describe)]
    attribute: GraphRecordAttribute,
}

impl Cardinality for AttributeContext {
    fn cardinality(&self, stats: &Stats) -> usize {
        self.input.context().cardinality(stats)
    }
}

impl MultipleValuesOperandContext for AttributeContext {
    type Operand = NodeOperand;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<
        BoxedIterator<'a, (<Self::Operand as RootOperand>::Index<'a>, GraphRecordValue)>,
    > {
        let node_indices = self.input.evaluate(graphrecord, context)?;

        Ok(Box::new(node_indices.filter_map(|node_index| {
            let value = graphrecord
                .node_attributes(node_index)
                .expect("Node must exist")
                .get(&self.attribute)?
                .clone();

            Some((node_index, value))
        })))
    }
}

impl Attribute for NodeOperand {
    type ReturnOperand = MultipleValuesOperand<Self>;

    fn attribute(&self, attribute: GraphRecordAttribute) -> Self::ReturnOperand {
        MultipleValuesOperand::new(AttributeContext {
            input: self.clone(),
            attribute,
        })
    }
}
