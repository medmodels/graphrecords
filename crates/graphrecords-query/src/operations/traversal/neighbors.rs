use crate::{
    BoxedIterator, Definite, EdgeDirection, EvaluateOperand, Explain, Failure, Indexed, Labeled,
    Multiple, Operand, OrderState, QueryResult, Single, Unit, Unordered,
    execution::EvaluationCache,
    operands::NodesOperand,
    operations::{Apply, KeyedStream, LaneKernel, Operation, OperationContext, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    traits::Neighbors,
};
use graphrecords_core::{GraphRecord, graphrecord::NodeIndex};
use graphrecords_utils::aliases::GrHashSet;

fn neighbors_for_node<'a>(
    graphrecord: &'a GraphRecord,
    node: &'a NodeIndex,
    direction: EdgeDirection,
) -> QueryResult<BoxedIterator<'a, &'a NodeIndex>> {
    let raise = |error| Failure::new_at::<NodeIndex, _>(NeighborsOperation::LABEL, error, &node);

    Ok(match direction {
        EdgeDirection::Outgoing => Box::new(graphrecord.outgoing_neighbors(node).map_err(raise)?),
        EdgeDirection::Incoming => Box::new(graphrecord.incoming_neighbors(node).map_err(raise)?),
        EdgeDirection::Both => Box::new(graphrecord.neighbors(node).map_err(raise)?),
    })
}

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "Neighbors")]
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

impl<O: OrderState> LaneKernel<Indexed<NodeIndex, Unit>, Multiple<O>> for NeighborsOperation {
    type Output = NodesOperand<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, NodeIndex, Unit, Multiple<O>>,
        direction: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let neighbors: GrHashSet<_> = values
            .map(|(node, membership)| {
                membership.and_then(|()| neighbors_for_node(graphrecord, node, direction))
            })
            .collect::<QueryResult<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect();

        Ok(Box::new(neighbors.into_iter().map(|node| (node, Ok(())))))
    }
}

impl LaneKernel<Indexed<NodeIndex, Unit>, Single> for NeighborsOperation {
    type Output = NodesOperand<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, NodeIndex, Unit, Single>,
        direction: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let Some((node, membership)) = value else {
            return Ok(Box::new(std::iter::empty()));
        };
        membership?;

        let neighbors: GrHashSet<_> = neighbors_for_node(graphrecord, node, direction)?.collect();

        Ok(Box::new(
            neighbors.into_iter().map(|neighbor| (neighbor, Ok(()))),
        ))
    }
}

impl LaneKernel<Indexed<NodeIndex, Unit>, Definite> for NeighborsOperation {
    type Output = NodesOperand<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, NodeIndex, Unit, Definite>,
        direction: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let (node, membership) = value;
        membership?;

        let neighbors: GrHashSet<_> = neighbors_for_node(graphrecord, node, direction)?.collect();

        Ok(Box::new(
            neighbors.into_iter().map(|neighbor| (neighbor, Ok(()))),
        ))
    }
}

impl<O: Apply<NeighborsOperation>> Neighbors for O {
    type ReturnOperand = O::Output;

    fn neighbors(&self, direction: EdgeDirection) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            NeighborsOperation { direction },
        ))
    }
}
