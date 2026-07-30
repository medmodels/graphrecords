use crate::{
    BoxedIterator, EdgeDirection, EntityReference, ExpandedChild, ExpandedIndex, Explain,
    IndexDomain, Indexed, Operand, QueryResult, Unit, Unordered,
    element::{Expanding, Pipeline},
    execution::EvaluationCache,
    operations::{Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    traits::ViaEdges,
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
#[operation(scope = Element)]
#[explain(label = "ViaEdges")]
#[plan(optimizer_hints(empty = if_any))]
pub struct ViaEdgesOperation {
    #[explain(label)]
    direction: EdgeDirection,
}

impl Prepare for ViaEdgesOperation {
    type Prepared<'a> = EdgeDirection;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self.direction)
    }
}

impl ElementKernel<Indexed<NodeIndex, Unit>> for ViaEdgesOperation {
    type Emission = Expanding<Unordered>;
    type OutShape = Indexed<ExpandedIndex<NodeIndex, EdgeIndex>, EntityReference<EdgeIndex>>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<NodeIndex, Unit>, Self>> {
        Ok(Pipeline::keyed(move |parent_index, ()| {
            let edges: GrHashSet<_> = edges_for_node(graphrecord, parent_index, prepared).collect();

            Ok(edges
                .into_iter()
                .map(|edge| ExpandedChild::success(edge, edge))
                .collect())
        }))
    }
}

impl<I: IndexDomain> ElementKernel<Indexed<I, EntityReference<NodeIndex>>> for ViaEdgesOperation {
    type Emission = Expanding<Unordered>;
    type OutShape = Indexed<ExpandedIndex<I, EdgeIndex>, EntityReference<EdgeIndex>>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, EntityReference<NodeIndex>>, Self>> {
        Ok(Pipeline::unkeyed(move |node| {
            let edges: GrHashSet<_> = edges_for_node(graphrecord, node, prepared).collect();

            Ok(edges
                .into_iter()
                .map(|edge| ExpandedChild::success(edge, edge))
                .collect())
        }))
    }
}

impl<O: Apply<ViaEdgesOperation>> ViaEdges for O {
    type ReturnOperand = O::Output;

    fn via_edges(&self, direction: EdgeDirection) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            ViaEdgesOperation { direction },
        ))
    }
}
