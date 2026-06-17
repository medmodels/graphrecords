use crate::{
    BoxedIterator, EdgeDirection, EvaluateOperand, Explain, Indexed, Multiple, Operand,
    QueryResult, Unit,
    execution::EvaluationCache,
    operands::{EdgeOperand, NodeOperand, OperandHandle},
    operations::{Kernel, KeyedStream, Operation, OperationContext, Prepare},
    optimizer::{EstimateCost, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Edges,
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{EdgeIndex, NodeIndex},
};
use graphrecords_utils::aliases::GrHashSet;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "Edges")]
#[plan(optimizer_hints(distinct))]
pub struct EdgesOperation {
    direction: EdgeDirection,
}

impl Prepare for EdgesOperation {
    type Prepared<'a> = EdgeDirection;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self.direction)
    }
}

impl Kernel<Indexed<NodeIndex, Unit>, Multiple> for EdgesOperation {
    type Output = EdgeOperand;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, NodeIndex, Unit, Multiple>,
        direction: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let edges: GrHashSet<_> = values
            .map(|(node, membership)| {
                membership.map(|()| -> BoxedIterator<'a, &'a EdgeIndex> {
                    match direction {
                        EdgeDirection::Outgoing => {
                            Box::new(graphrecord.outgoing_edges(node).expect("Node must exist"))
                        }
                        EdgeDirection::Incoming => {
                            Box::new(graphrecord.incoming_edges(node).expect("Node must exist"))
                        }
                        EdgeDirection::Both => Box::new(
                            graphrecord
                                .outgoing_edges(node)
                                .expect("Node must exist")
                                .chain(graphrecord.incoming_edges(node).expect("Node must exist")),
                        ),
                    }
                })
            })
            .collect::<QueryResult<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect();

        Ok(Box::new(edges.into_iter().map(|edge| (edge, Ok(())))))
    }
}

impl EstimateCost<EdgesOperation> for OperandHandle<Indexed<NodeIndex, Unit>, Multiple> {
    type OutputCost = <EdgeOperand as Operand>::Cost;

    fn estimate(
        _operation: &EdgesOperation,
        input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        input_cost
    }
}

impl Edges for NodeOperand {
    type ReturnOperand = EdgeOperand;

    fn edges(&self, direction: EdgeDirection) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            EdgesOperation { direction },
        ))
    }
}
