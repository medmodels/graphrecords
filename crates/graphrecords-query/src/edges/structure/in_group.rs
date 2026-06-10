use crate::{
    EdgeOperand, EvaluateContext, EvaluateOperand, Explain, Operand,
    bool::{BoolMaskOperand, BoolMaskOperandContext},
    execution::ExecutionContext,
    optimizer::{
        EdgeGroupSize, HasInputs, OptimizeInputs, OptimizerHints, PlanNode, Selectivity, Stats,
    },
    traits::InGroup,
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult, graphrecord::Group};
use graphrecords_utils::aliases::GrHashSet;

#[derive(PlanNode, HasInputs, OptimizeInputs, OptimizerHints, Explain)]
#[plan(operand = BoolMaskOperand<EdgeOperand>, optimizer_hints(distinct, empty = if_any))]
#[explain(label = "InGroup")]
pub struct InGroupContext {
    #[input]
    input: EdgeOperand,
    #[explain(label)]
    group: Group,
}

impl Selectivity for InGroupContext {
    fn selectivity(&self, stats: &Stats) -> f64 {
        let group = stats.get::<EdgeGroupSize>(&self.group) as f64;
        let input_cardinality = self.input.context().cardinality(stats).max(1) as f64;

        (group / input_cardinality).min(1.0)
    }
}

impl EvaluateContext for InGroupContext {
    type Operand = BoolMaskOperand<EdgeOperand>;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<<Self::Operand as EvaluateOperand>::ReturnValue<'a>> {
        let edge_indices = self.input.evaluate(graphrecord, context)?;

        let edge_indices_in_group: GrHashSet<_> =
            graphrecord.edges_in_group(&self.group)?.collect();

        Ok(Box::new(edge_indices.map(move |edge_index| {
            let in_group = edge_indices_in_group.contains(&edge_index);

            (edge_index, in_group)
        })))
    }
}

impl BoolMaskOperandContext for InGroupContext {
    type RootOperand = EdgeOperand;
}

impl InGroup for EdgeOperand {
    type ReturnOperand = BoolMaskOperand<Self>;

    fn in_group(&self, group: Group) -> Self::ReturnOperand {
        BoolMaskOperand::new(InGroupContext {
            input: self.clone(),
            group,
        })
    }
}
