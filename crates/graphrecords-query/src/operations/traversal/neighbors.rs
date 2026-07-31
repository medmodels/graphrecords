use crate::{
    Definite, EdgeDirection, EntityReference, EvaluateOperand, Explain, IndexDomain, Indexed,
    Multiple, Operand, OrderState, QueryResult, Single, Unit, Unordered,
    execution::EvaluationCache,
    operands::NodesOperand,
    operations::{Apply, KeyedStream, LaneKernel, Operation, OperationContext, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    registry::operation_manifest,
    traits::Neighbors,
};
use graphrecords_core::{GraphRecord, graphrecord::NodeIndex};
use graphrecords_utils::aliases::GrHashSet;
use std::iter::empty;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "Neighbors")]
#[plan(optimizer_hints(empty = if_any))]
pub struct NeighborsOperation {
    #[explain(label)]
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
        let mut neighbors = GrHashSet::default();

        for (node, membership) in values {
            membership?;
            neighbors.extend(direction.neighbors_for_node(graphrecord, node));
        }

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
            return Ok(Box::new(empty()));
        };
        membership?;

        let neighbors: GrHashSet<_> = direction.neighbors_for_node(graphrecord, node).collect();

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

        let neighbors: GrHashSet<_> = direction.neighbors_for_node(graphrecord, node).collect();

        Ok(Box::new(
            neighbors.into_iter().map(|neighbor| (neighbor, Ok(()))),
        ))
    }
}

impl<I: IndexDomain, O: OrderState> LaneKernel<Indexed<I, EntityReference<NodeIndex>>, Multiple<O>>
    for NeighborsOperation
{
    type Output = NodesOperand<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, EntityReference<NodeIndex>, Multiple<O>>,
        direction: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let mut neighbors = GrHashSet::default();

        for value in values {
            let node = value.1?;
            neighbors.extend(direction.neighbors_for_node(graphrecord, node));
        }

        Ok(Box::new(
            neighbors.into_iter().map(|neighbor| (neighbor, Ok(()))),
        ))
    }
}

impl<I: IndexDomain> LaneKernel<Indexed<I, EntityReference<NodeIndex>>, Single>
    for NeighborsOperation
{
    type Output = NodesOperand<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, EntityReference<NodeIndex>, Single>,
        direction: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let Some(value) = value else {
            return Ok(Box::new(empty()));
        };
        let node = value.1?;
        let neighbors: GrHashSet<_> = direction.neighbors_for_node(graphrecord, node).collect();

        Ok(Box::new(
            neighbors.into_iter().map(|neighbor| (neighbor, Ok(()))),
        ))
    }
}

impl<I: IndexDomain> LaneKernel<Indexed<I, EntityReference<NodeIndex>>, Definite>
    for NeighborsOperation
{
    type Output = NodesOperand<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, EntityReference<NodeIndex>, Definite>,
        direction: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let node = value.1?;
        let neighbors: GrHashSet<_> = direction.neighbors_for_node(graphrecord, node).collect();

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

operation_manifest! {
    NeighborsOperation {
        method: Neighbors::neighbors;
        scope: lane;

        kernel {
            parameters: <O: OrderState>;
            field: direction: EdgeDirection;
            input: (Indexed<NodeIndex, Unit>, Multiple<O>);
            output: NodesOperand<Unordered>;
        }
        kernel {
            parameters: <>;
            field: direction: EdgeDirection;
            input: (Indexed<NodeIndex, Unit>, Single);
            output: NodesOperand<Unordered>;
        }
        kernel {
            parameters: <>;
            field: direction: EdgeDirection;
            input: (Indexed<NodeIndex, Unit>, Definite);
            output: NodesOperand<Unordered>;
        }
        kernel {
            parameters: <I: IndexDomain, O: OrderState>;
            field: direction: EdgeDirection;
            input: (Indexed<I, EntityReference<NodeIndex>>, Multiple<O>);
            output: NodesOperand<Unordered>;
        }
        kernel {
            parameters: <I: IndexDomain>;
            field: direction: EdgeDirection;
            input: (Indexed<I, EntityReference<NodeIndex>>, Single);
            output: NodesOperand<Unordered>;
        }
        kernel {
            parameters: <I: IndexDomain>;
            field: direction: EdgeDirection;
            input: (Indexed<I, EntityReference<NodeIndex>>, Definite);
            output: NodesOperand<Unordered>;
        }
    }
}
