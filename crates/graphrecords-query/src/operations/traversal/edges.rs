use crate::{
    BoxedIterator, Definite, EdgeDirection, EntityReference, EvaluateOperand, Explain, IndexDomain,
    Indexed, Multiple, Operand, OrderState, QueryResult, Single, Unit, Unordered,
    execution::EvaluationCache,
    operands::EdgesOperand,
    operations::{Apply, KeyedStream, LaneKernel, Operation, OperationContext, Prepare},
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
) -> BoxedIterator<'a, &'a EdgeIndex> {
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
}

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "Edges")]
#[plan(optimizer_hints(empty = if_any))]
pub struct EdgesOperation {
    #[explain(label)]
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

impl<O: OrderState> LaneKernel<Indexed<NodeIndex, Unit>, Multiple<O>> for EdgesOperation {
    type Output = EdgesOperand<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, NodeIndex, Unit, Multiple<O>>,
        direction: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let mut edges = GrHashSet::default();

        for (node, membership) in values {
            membership?;
            edges.extend(edges_for_node(graphrecord, node, direction));
        }

        Ok(Box::new(edges.into_iter().map(|edge| (edge, Ok(())))))
    }
}

impl LaneKernel<Indexed<NodeIndex, Unit>, Single> for EdgesOperation {
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

        let edges: GrHashSet<_> = edges_for_node(graphrecord, node, direction).collect();

        Ok(Box::new(edges.into_iter().map(|edge| (edge, Ok(())))))
    }
}

impl LaneKernel<Indexed<NodeIndex, Unit>, Definite> for EdgesOperation {
    type Output = EdgesOperand<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, NodeIndex, Unit, Definite>,
        direction: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let (node, membership) = value;
        membership?;

        let edges: GrHashSet<_> = edges_for_node(graphrecord, node, direction).collect();

        Ok(Box::new(edges.into_iter().map(|edge| (edge, Ok(())))))
    }
}

impl<I: IndexDomain, O: OrderState> LaneKernel<Indexed<I, EntityReference<NodeIndex>>, Multiple<O>>
    for EdgesOperation
{
    type Output = EdgesOperand<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, EntityReference<NodeIndex>, Multiple<O>>,
        direction: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let mut edges = GrHashSet::default();

        for value in values {
            let node = value.1?;
            edges.extend(edges_for_node(graphrecord, node, direction));
        }

        Ok(Box::new(edges.into_iter().map(|edge| (edge, Ok(())))))
    }
}

impl<I: IndexDomain> LaneKernel<Indexed<I, EntityReference<NodeIndex>>, Single> for EdgesOperation {
    type Output = EdgesOperand<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, EntityReference<NodeIndex>, Single>,
        direction: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let Some(value) = value else {
            return Ok(Box::new(std::iter::empty()));
        };
        let node = value.1?;
        let edges: GrHashSet<_> = edges_for_node(graphrecord, node, direction).collect();

        Ok(Box::new(edges.into_iter().map(|edge| (edge, Ok(())))))
    }
}

impl<I: IndexDomain> LaneKernel<Indexed<I, EntityReference<NodeIndex>>, Definite>
    for EdgesOperation
{
    type Output = EdgesOperand<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, EntityReference<NodeIndex>, Definite>,
        direction: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let node = value.1?;
        let edges: GrHashSet<_> = edges_for_node(graphrecord, node, direction).collect();

        Ok(Box::new(edges.into_iter().map(|edge| (edge, Ok(())))))
    }
}

impl<O: Apply<EdgesOperation>> Edges for O {
    type ReturnOperand = O::Output;

    fn edges(&self, direction: EdgeDirection) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            EdgesOperation { direction },
        ))
    }
}
