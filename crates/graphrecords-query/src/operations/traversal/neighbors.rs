use crate::{
    BoxedIterator, EdgeDirection, EvaluateOperand, Explain, Indexed, Multiple, Operand, OrderState,
    QueryResult, Unit,
    execution::EvaluationCache,
    operands::{NodeOperand, OperandHandle},
    operations::{Kernel, KeyedStream, Operation, OperationContext, Prepare},
    optimizer::{EstimateCost, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Neighbors,
};
use graphrecords_core::{GraphRecord, graphrecord::NodeIndex};
use graphrecords_utils::aliases::GrHashSet;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "Neighbors")]
#[plan(optimizer_hints(distinct))]
pub struct NeighborsOperation {
    direction: EdgeDirection,
}

impl Prepare for NeighborsOperation {
    type Prepared<'a> = EdgeDirection;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self.direction)
    }
}

impl<O: OrderState> Kernel<Indexed<NodeIndex, Unit>, Multiple<O>> for NeighborsOperation {
    type Output = NodeOperand;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, NodeIndex, Unit, Multiple<O>>,
        direction: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let neighbors: GrHashSet<_> = values
            .map(|(node, membership)| {
                membership.map(|()| -> BoxedIterator<'a, &'a NodeIndex> {
                    match direction {
                        EdgeDirection::Outgoing => Box::new(
                            graphrecord
                                .outgoing_edges(node)
                                .expect("Node must exist")
                                .map(|edge| {
                                    graphrecord.edge_endpoints(edge).expect("Edge must exist").1
                                }),
                        ),
                        EdgeDirection::Incoming => Box::new(
                            graphrecord
                                .incoming_edges(node)
                                .expect("Node must exist")
                                .map(|edge| {
                                    graphrecord.edge_endpoints(edge).expect("Edge must exist").0
                                }),
                        ),
                        EdgeDirection::Both => {
                            Box::new(graphrecord.neighbors(node).expect("Node must exist"))
                        }
                    }
                })
            })
            .collect::<QueryResult<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect();

        Ok(Box::new(neighbors.into_iter().map(|node| (node, Ok(())))))
    }
}

impl<O: OrderState> EstimateCost<NeighborsOperation>
    for OperandHandle<Indexed<NodeIndex, Unit>, Multiple<O>>
{
    type OutputCost = <Self as Operand>::Cost;

    fn estimate(
        _operation: &NeighborsOperation,
        input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        input_cost
    }
}

impl Neighbors for NodeOperand {
    type ReturnOperand = Self;

    fn neighbors(&self, direction: EdgeDirection) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            NeighborsOperation { direction },
        ))
    }
}
