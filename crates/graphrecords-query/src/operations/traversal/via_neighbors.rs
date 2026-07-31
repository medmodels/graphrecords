use crate::{
    EdgeDirection, EntityReference, ExpandedChild, ExpandedIndex, Explain, IndexDomain, Indexed,
    Operand, QueryResult, Unit, Unordered,
    element::{Expanding, Pipeline},
    execution::EvaluationCache,
    operations::{Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    registry::operation_manifest,
    traits::ViaNeighbors,
};
use graphrecords_core::{GraphRecord, graphrecord::NodeIndex};
use graphrecords_utils::aliases::GrHashSet;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "ViaNeighbors")]
#[plan(optimizer_hints(empty = if_any))]
pub struct ViaNeighborsOperation {
    #[explain(label)]
    direction: EdgeDirection,
}

impl Prepare for ViaNeighborsOperation {
    type Prepared<'a> = EdgeDirection;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self.direction)
    }
}

impl ElementKernel<Indexed<NodeIndex, Unit>> for ViaNeighborsOperation {
    type Emission = Expanding<Unordered>;
    type OutShape = Indexed<ExpandedIndex<NodeIndex, NodeIndex>, EntityReference<NodeIndex>>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<NodeIndex, Unit>, Self>> {
        Ok(Pipeline::keyed(move |parent_index, ()| {
            let neighbors: GrHashSet<_> = prepared
                .neighbors_for_node(graphrecord, parent_index)
                .collect();

            Ok(neighbors
                .into_iter()
                .map(|neighbor| ExpandedChild::success(neighbor, neighbor))
                .collect())
        }))
    }
}

impl<I: IndexDomain> ElementKernel<Indexed<I, EntityReference<NodeIndex>>>
    for ViaNeighborsOperation
{
    type Emission = Expanding<Unordered>;
    type OutShape = Indexed<ExpandedIndex<I, NodeIndex>, EntityReference<NodeIndex>>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, EntityReference<NodeIndex>>, Self>> {
        Ok(Pipeline::unkeyed(move |node| {
            let neighbors: GrHashSet<_> = prepared.neighbors_for_node(graphrecord, node).collect();

            Ok(neighbors
                .into_iter()
                .map(|neighbor| ExpandedChild::success(neighbor, neighbor))
                .collect())
        }))
    }
}

impl<O: Apply<ViaNeighborsOperation>> ViaNeighbors for O {
    type ReturnOperand = O::Output;

    fn via_neighbors(&self, direction: EdgeDirection) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            ViaNeighborsOperation { direction },
        ))
    }
}

operation_manifest! {
    ViaNeighborsOperation {
        method: ViaNeighbors::via_neighbors;
        scope: element;

        kernel {
            parameters: <>;
            field: direction: EdgeDirection;
            input: Indexed<NodeIndex, Unit>;
            output: Indexed<ExpandedIndex<NodeIndex, NodeIndex>, EntityReference<NodeIndex>>;
            emission: Expanding<Unordered>;
        }
        kernel {
            parameters: <I: IndexDomain>;
            field: direction: EdgeDirection;
            input: Indexed<I, EntityReference<NodeIndex>>;
            output: Indexed<ExpandedIndex<I, NodeIndex>, EntityReference<NodeIndex>>;
            emission: Expanding<Unordered>;
        }
    }
}
