use crate::{
    EdgeDirection, EntityReference, ExpandedChild, ExpandedIndex, Explain, IndexDomain, Indexed,
    Operand, QueryResult, Unit, Unordered,
    element::{Expanding, Pipeline},
    execution::EvaluationCache,
    operations::{Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    registry::operation_manifest,
    traits::ViaEdges,
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{EdgeIndex, NodeIndex},
};
use graphrecords_utils::aliases::GrHashSet;

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
            let edges: GrHashSet<_> = prepared.edges_for_node(graphrecord, parent_index).collect();

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
            let edges: GrHashSet<_> = prepared.edges_for_node(graphrecord, node).collect();

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

operation_manifest! {
    ViaEdgesOperation {
        method: ViaEdges::via_edges;
        scope: element;

        kernel {
            parameters: <>;
            field: direction: EdgeDirection;
            input: Indexed<NodeIndex, Unit>;
            output: Indexed<ExpandedIndex<NodeIndex, EdgeIndex>, EntityReference<EdgeIndex>>;
            emission: Expanding<Unordered>;
        }
        kernel {
            parameters: <I: IndexDomain>;
            field: direction: EdgeDirection;
            input: Indexed<I, EntityReference<NodeIndex>>;
            output: Indexed<ExpandedIndex<I, EdgeIndex>, EntityReference<EdgeIndex>>;
            emission: Expanding<Unordered>;
        }
    }
}
