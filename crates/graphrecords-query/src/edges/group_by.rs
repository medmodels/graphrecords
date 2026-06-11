use crate::{
    EvaluateContext, EvaluateOperand, Explain,
    edges::EdgeOperand,
    execution::ExecutionContext,
    group::{AttributeDiscriminator, GroupBy, GroupOperand, GroupedOperandContext, partition_by},
    optimizer::{
        Cardinality, EdgeAttributeCardinality, HasInputs, OptimizeInputs, OptimizerHints, PlanNode,
        Stats,
    },
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult};

#[derive(PlanNode, HasInputs, OptimizeInputs, OptimizerHints, Explain)]
#[plan(operand = GroupOperand<EdgeOperand, AttributeDiscriminator>)]
#[explain(label = "GroupBy")]
struct EdgeGroupByAttributeContext {
    #[input]
    input: EdgeOperand,
    #[explain(label)]
    discriminator: AttributeDiscriminator,
}

impl Cardinality for EdgeGroupByAttributeContext {
    fn cardinality(&self, stats: &Stats) -> usize {
        stats.get::<EdgeAttributeCardinality>(&self.discriminator.attribute)
    }
}

impl GroupedOperandContext<EdgeOperand, AttributeDiscriminator> for EdgeGroupByAttributeContext {}

impl EvaluateContext for EdgeGroupByAttributeContext {
    type Operand = GroupOperand<EdgeOperand, AttributeDiscriminator>;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<<Self::Operand as EvaluateOperand>::ReturnValue<'a>> {
        let edge_indices = self.input.evaluate(graphrecord, context)?;
        let attribute = &self.discriminator.attribute;

        Ok(partition_by(edge_indices, |&edge_index| {
            graphrecord
                .edge_attributes(edge_index)
                .expect("Edge must exist")
                .get(attribute)
        }))
    }
}

impl GroupBy<AttributeDiscriminator> for EdgeOperand {
    type Output = GroupOperand<Self, AttributeDiscriminator>;

    fn group_by(&self, discriminator: AttributeDiscriminator) -> Self::Output {
        GroupOperand::new(EdgeGroupByAttributeContext {
            input: self.clone(),
            discriminator,
        })
    }
}
