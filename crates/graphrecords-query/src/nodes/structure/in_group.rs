use crate::{
    BoxedIterator, Evaluate, Explain, Operand, RootOperand,
    bool::{BoolMaskOperand, BoolMaskOperandContext},
    execution::ExecutionContext,
    nodes::NodeOperand,
    optimizer::{
        HasInputs, NodeGroupSize, OptimizeInputs, OptimizerHints, PlanNode, Selectivity, Stats,
    },
    traits::InGroup,
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult, graphrecord::Group};
use graphrecords_utils::aliases::GrHashSet;

#[derive(PlanNode, HasInputs, OptimizeInputs, OptimizerHints, Explain)]
#[plan(operand = BoolMaskOperand<NodeOperand>, optimizer_hints(distinct, empty = if_any))]
#[explain(label = "InGroup")]
pub struct InGroupContext {
    #[input]
    input: NodeOperand,
    #[explain(label)]
    group: Group,
}

impl Selectivity for InGroupContext {
    fn selectivity(&self, stats: &Stats) -> f64 {
        let group = stats.get::<NodeGroupSize>(&self.group) as f64;
        let input_cardinality = self.input.context().cardinality(stats).max(1) as f64;

        (group / input_cardinality).min(1.0)
    }
}

impl BoolMaskOperandContext for InGroupContext {
    type Operand = NodeOperand;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<BoxedIterator<'a, (<Self::Operand as RootOperand>::Index<'a>, bool)>>
    {
        let node_indices = self.input.evaluate(graphrecord, context)?;

        let node_indices_in_group: GrHashSet<_> =
            graphrecord.nodes_in_group(&self.group)?.collect();

        Ok(Box::new(node_indices.map(move |node_index| {
            let in_group = node_indices_in_group.contains(&node_index);

            (node_index, in_group)
        })))
    }
}

impl InGroup for NodeOperand {
    type ReturnOperand = BoolMaskOperand<Self>;

    fn in_group(&self, group: Group) -> Self::ReturnOperand {
        BoolMaskOperand::new(InGroupContext {
            input: self.clone(),
            group,
        })
    }
}
