use crate::{
    EdgeDirection, EntityRef, EntityReference, ExpandedChild, ExpandedIndex, Explain, IndexDomain,
    Indexed, QueryResult, Unit, Unordered,
    element::{Expanding, Pipeline},
    execution::EvaluationCache,
    operations::{Build, ElementKernel, ElementPipeline, Operation, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    registry::operation_manifest,
    traits::ViaNeighbors,
};
use graphrecords_core::{GraphRecord, graphrecord::NodeIndex};
use graphrecords_utils::distinct::Distinct;

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
        _cache: &'a EvaluationCache,
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
        Ok(Pipeline::keyed(move |parent_address, ()| {
            let neighbors: Vec<_> = prepared
                .neighbors_for_node(graphrecord, parent_address)
                .collect::<Distinct<_>>()
                .into();

            Ok(neighbors
                .into_iter()
                .map(|neighbor| {
                    ExpandedChild::success(neighbor, EntityRef::new(graphrecord, neighbor))
                })
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
        Ok(Pipeline::unkeyed(move |node: EntityRef<'a, NodeIndex>| {
            let neighbors: Vec<_> = prepared
                .neighbors_for_node(graphrecord, *node.address())
                .collect::<Distinct<_>>()
                .into();

            Ok(neighbors
                .into_iter()
                .map(|neighbor| {
                    ExpandedChild::success(neighbor, EntityRef::new(graphrecord, neighbor))
                })
                .collect())
        }))
    }
}

impl<E: Build<ViaNeighborsOperation>> ViaNeighbors for E {
    type Output = E::Output;

    fn via_neighbors(&self, direction: EdgeDirection) -> Self::Output {
        self.build(ViaNeighborsOperation { direction })
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
