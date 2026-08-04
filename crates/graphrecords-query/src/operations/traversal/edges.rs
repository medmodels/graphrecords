use crate::{
    Definite, EdgeDirection, EntityReference, EvaluateOperand, Explain, IndexDomain, Indexed,
    Multiple, Operand, OrderState, QueryResult, Single, Unit, Unordered,
    execution::EvaluationCache,
    operands::EdgesOperand,
    operations::{Apply, KeyedStream, LaneKernel, Operation, OperationContext, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    registry::operation_manifest,
    traits::Edges,
};
use graphrecords_core::{GraphRecord, graphrecord::NodeIndex};
use graphrecords_utils::aliases::GrHashSet;
use std::iter::empty;

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
            edges.extend(direction.edges_for_node(graphrecord, node));
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
            return Ok(Box::new(empty()));
        };
        membership?;

        let edges: GrHashSet<_> = direction.edges_for_node(graphrecord, node).collect();

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

        let edges: GrHashSet<_> = direction.edges_for_node(graphrecord, node).collect();

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
            edges.extend(direction.edges_for_node(graphrecord, node));
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
            return Ok(Box::new(empty()));
        };
        let node = value.1?;
        let edges: GrHashSet<_> = direction.edges_for_node(graphrecord, node).collect();

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
        let edges: GrHashSet<_> = direction.edges_for_node(graphrecord, node).collect();

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

operation_manifest! {
    EdgesOperation {
        method: Edges::edges;
        scope: lane;

        kernel {
            parameters: <O: OrderState>;
            field: direction: EdgeDirection;
            input: (Indexed<NodeIndex, Unit>, Multiple<O>);
            output: EdgesOperand<Unordered>;
        }
        kernel {
            parameters: <>;
            field: direction: EdgeDirection;
            input: (Indexed<NodeIndex, Unit>, Single);
            output: EdgesOperand<Unordered>;
        }
        kernel {
            parameters: <>;
            field: direction: EdgeDirection;
            input: (Indexed<NodeIndex, Unit>, Definite);
            output: EdgesOperand<Unordered>;
        }
        kernel {
            parameters: <I: IndexDomain, O: OrderState>;
            field: direction: EdgeDirection;
            input: (Indexed<I, EntityReference<NodeIndex>>, Multiple<O>);
            output: EdgesOperand<Unordered>;
        }
        kernel {
            parameters: <I: IndexDomain>;
            field: direction: EdgeDirection;
            input: (Indexed<I, EntityReference<NodeIndex>>, Single);
            output: EdgesOperand<Unordered>;
        }
        kernel {
            parameters: <I: IndexDomain>;
            field: direction: EdgeDirection;
            input: (Indexed<I, EntityReference<NodeIndex>>, Definite);
            output: EdgesOperand<Unordered>;
        }
    }
}
