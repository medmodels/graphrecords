use crate::{
    BoxedIterator, EdgeDirection, EvaluateOperand, Explain, Failure, Indexed, Labeled, Multiple,
    Operand, OrderState, QueryResult, Unit, Unordered,
    execution::EvaluationCache,
    operands::{EdgeOperand, NodeOperand},
    operations::{Kernel, KeyedStream, Operation, OperationContext, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
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

impl<O: OrderState> Kernel<Indexed<NodeIndex, Unit>, Multiple<O>> for EdgesOperation {
    type Output = EdgeOperand<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, NodeIndex, Unit, Multiple<O>>,
        direction: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let edges: GrHashSet<_> = values
            .map(|(node, membership)| {
                membership.and_then(|()| -> QueryResult<BoxedIterator<'a, &'a EdgeIndex>> {
                    let raise = |error| Failure::new_at(Self::LABEL, error, &node);

                    Ok(match direction {
                        EdgeDirection::Outgoing => {
                            Box::new(graphrecord.outgoing_edges(node).map_err(raise)?)
                        }
                        EdgeDirection::Incoming => {
                            Box::new(graphrecord.incoming_edges(node).map_err(raise)?)
                        }
                        EdgeDirection::Both => Box::new(
                            graphrecord
                                .outgoing_edges(node)
                                .map_err(raise)?
                                .chain(graphrecord.incoming_edges(node).map_err(raise)?),
                        ),
                    })
                })
            })
            .collect::<QueryResult<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect();

        Ok(Box::new(edges.into_iter().map(|edge| (edge, Ok(())))))
    }
}

impl<O: OrderState> Edges for NodeOperand<O> {
    type ReturnOperand = EdgeOperand<Unordered>;

    fn edges(&self, direction: EdgeDirection) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            EdgesOperation { direction },
        ))
    }
}
