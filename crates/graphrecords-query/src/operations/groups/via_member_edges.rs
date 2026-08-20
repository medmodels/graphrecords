use crate::{
    EntityRef, EntityReference, ExpandedChild, ExpandedIndex, Explain, IndexDomain, Indexed,
    QueryResult, Unit, Unordered,
    element::{Expanding, Pipeline},
    index::GroupMembership,
    operations::{Build, ElementKernel, ElementPipeline, Operation, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    registry::operation_manifest,
    traits::ViaMemberEdges,
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{EdgeIndex, Group},
};

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Element)]
#[explain(label = "ViaMemberEdges")]
#[plan(optimizer_hints(empty = if_any))]
pub struct ViaMemberEdgesOperation;

impl ElementKernel<Indexed<Group, Unit>> for ViaMemberEdgesOperation {
    type Emission = Expanding<Unordered>;
    type OutShape = Indexed<ExpandedIndex<Group, EdgeIndex>, EntityReference<EdgeIndex>>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<Group, Unit>, Self>> {
        Ok(Pipeline::keyed(move |parent_address, ()| {
            Ok(EdgeIndex::addresses_in_group(graphrecord, parent_address)
                .map(|edge| ExpandedChild::success(edge, EntityRef::new(graphrecord, edge)))
                .collect())
        }))
    }
}

impl<I: IndexDomain> ElementKernel<Indexed<I, EntityReference<Group>>> for ViaMemberEdgesOperation {
    type Emission = Expanding<Unordered>;
    type OutShape = Indexed<ExpandedIndex<I, EdgeIndex>, EntityReference<EdgeIndex>>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, EntityReference<Group>>, Self>> {
        Ok(Pipeline::unkeyed(move |group: EntityRef<'a, Group>| {
            Ok(EdgeIndex::addresses_in_group(graphrecord, *group.address())
                .map(|edge| ExpandedChild::success(edge, EntityRef::new(graphrecord, edge)))
                .collect())
        }))
    }
}

impl<E: Build<ViaMemberEdgesOperation>> ViaMemberEdges for E {
    type Output = E::Output;

    fn via_edges(&self) -> Self::Output {
        self.build(ViaMemberEdgesOperation)
    }
}

operation_manifest! {
    ViaMemberEdgesOperation {
        method: ViaMemberEdges::via_edges;
        scope: element;

        kernel {
            parameters: <>;
            input: Indexed<Group, Unit>;
            output: Indexed<ExpandedIndex<Group, EdgeIndex>, EntityReference<EdgeIndex>>;
            emission: Expanding<Unordered>;
        }

        kernel {
            parameters: <I: IndexDomain>;
            input: Indexed<I, EntityReference<Group>>;
            output: Indexed<ExpandedIndex<I, EdgeIndex>, EntityReference<EdgeIndex>>;
            emission: Expanding<Unordered>;
        }
    }
}
