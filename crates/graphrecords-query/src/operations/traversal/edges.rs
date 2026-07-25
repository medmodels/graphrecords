use crate::{
    BoxedIterator, Definite, EdgeDirection, EvaluateOperand, Explain, Failure, Indexed, Labeled,
    Multiple, Operand, OrderState, QueryResult, Single, Unit, Unordered,
    execution::EvaluationCache,
    operands::EdgesOperand,
    operations::{Apply, Kernel, KeyedStream, Operation, OperationContext, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    traits::Edges,
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{EdgeIndex, NodeIndex},
};
use graphrecords_utils::aliases::GrHashSet;

fn edges_for_node<'a>(
    graphrecord: &'a GraphRecord,
    node: &'a NodeIndex,
    direction: EdgeDirection,
) -> QueryResult<BoxedIterator<'a, &'a EdgeIndex>> {
    let raise = |error| Failure::new_at(EdgesOperation::LABEL, error, &node);

    Ok(match direction {
        EdgeDirection::Outgoing => Box::new(graphrecord.outgoing_edges(node).map_err(raise)?),
        EdgeDirection::Incoming => Box::new(graphrecord.incoming_edges(node).map_err(raise)?),
        EdgeDirection::Both => Box::new(
            graphrecord
                .outgoing_edges(node)
                .map_err(raise)?
                .chain(graphrecord.incoming_edges(node).map_err(raise)?),
        ),
    })
}

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
    type Output = EdgesOperand<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, NodeIndex, Unit, Multiple<O>>,
        direction: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let edges: GrHashSet<_> = values
            .map(|(node, membership)| {
                membership.and_then(|()| edges_for_node(graphrecord, node, direction))
            })
            .collect::<QueryResult<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect();

        Ok(Box::new(edges.into_iter().map(|edge| (edge, Ok(())))))
    }
}

impl Kernel<Indexed<NodeIndex, Unit>, Single> for EdgesOperation {
    type Output = EdgesOperand<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, NodeIndex, Unit, Single>,
        direction: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let Some((node, membership)) = value else {
            return Ok(Box::new(std::iter::empty()));
        };
        membership?;

        let edges: GrHashSet<_> = edges_for_node(graphrecord, node, direction)?.collect();

        Ok(Box::new(edges.into_iter().map(|edge| (edge, Ok(())))))
    }
}

impl Kernel<Indexed<NodeIndex, Unit>, Definite> for EdgesOperation {
    type Output = EdgesOperand<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, NodeIndex, Unit, Definite>,
        direction: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let (node, membership) = value;
        membership?;

        let edges: GrHashSet<_> = edges_for_node(graphrecord, node, direction)?.collect();

        Ok(Box::new(edges.into_iter().map(|edge| (edge, Ok(())))))
    }
}

impl<O> Edges for O
where
    O: Apply<EdgesOperation>,
{
    type ReturnOperand = <O as Apply<EdgesOperation>>::Output;

    fn edges(&self, direction: EdgeDirection) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            EdgesOperation { direction },
        ))
    }
}
