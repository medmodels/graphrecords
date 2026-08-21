use crate::{
    EdgeEndpointRole, EntityRef, EntityReference, ExpandedChild, ExpandedIndex, Explain,
    IndexDomain, Indexed, Ordered, QueryResult, Unit, Unordered,
    element::{Expanding, Pipeline},
    index::GroupMembership,
    operations::{Build, ElementKernel, ElementPipeline, Operation, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    registry::operation_manifest,
    traits::ViaNodes,
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{EdgeIndex, GroupIndex, NodeIndex, StateView},
};

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Element)]
#[explain(label = "ViaNodes")]
#[plan(optimizer_hints(empty = if_any))]
pub struct ViaNodesOperation;

impl ElementKernel<Indexed<EdgeIndex, Unit>> for ViaNodesOperation {
    type Emission = Expanding<Ordered>;
    type OutShape = Indexed<ExpandedIndex<EdgeIndex, EdgeEndpointRole>, EntityReference<NodeIndex>>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<EdgeIndex, Unit>, Self>> {
        Ok(Pipeline::keyed(move |parent_address, ()| {
            let (source, target) = StateView::of(graphrecord).edge_endpoints(parent_address);

            Ok(vec![
                ExpandedChild::success(
                    EdgeEndpointRole::Source,
                    EntityRef::new(graphrecord, source),
                ),
                ExpandedChild::success(
                    EdgeEndpointRole::Target,
                    EntityRef::new(graphrecord, target),
                ),
            ])
        }))
    }
}

impl<I: IndexDomain> ElementKernel<Indexed<I, EntityReference<EdgeIndex>>> for ViaNodesOperation {
    type Emission = Expanding<Ordered>;
    type OutShape = Indexed<ExpandedIndex<I, EdgeEndpointRole>, EntityReference<NodeIndex>>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, EntityReference<EdgeIndex>>, Self>> {
        Ok(Pipeline::unkeyed(move |edge: EntityRef<'a, EdgeIndex>| {
            let (source, target) = StateView::of(graphrecord).edge_endpoints(*edge.address());

            Ok(vec![
                ExpandedChild::success(
                    EdgeEndpointRole::Source,
                    EntityRef::new(graphrecord, source),
                ),
                ExpandedChild::success(
                    EdgeEndpointRole::Target,
                    EntityRef::new(graphrecord, target),
                ),
            ])
        }))
    }
}

impl ElementKernel<Indexed<GroupIndex, Unit>> for ViaNodesOperation {
    type Emission = Expanding<Unordered>;
    type OutShape = Indexed<ExpandedIndex<GroupIndex, NodeIndex>, EntityReference<NodeIndex>>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<GroupIndex, Unit>, Self>> {
        Ok(Pipeline::keyed(move |parent_address, ()| {
            Ok(NodeIndex::addresses_in_group(graphrecord, parent_address)
                .map(|node| ExpandedChild::success(node, EntityRef::new(graphrecord, node)))
                .collect())
        }))
    }
}

impl<I: IndexDomain> ElementKernel<Indexed<I, EntityReference<GroupIndex>>> for ViaNodesOperation {
    type Emission = Expanding<Unordered>;
    type OutShape = Indexed<ExpandedIndex<I, NodeIndex>, EntityReference<NodeIndex>>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, EntityReference<GroupIndex>>, Self>> {
        Ok(Pipeline::unkeyed(
            move |group_index: EntityRef<'a, GroupIndex>| {
                Ok(
                    NodeIndex::addresses_in_group(graphrecord, *group_index.address())
                        .map(|node| ExpandedChild::success(node, EntityRef::new(graphrecord, node)))
                        .collect(),
                )
            },
        ))
    }
}

impl<E: Build<ViaNodesOperation>> ViaNodes for E {
    type Output = E::Output;

    fn via_nodes(&self) -> Self::Output {
        self.build(ViaNodesOperation)
    }
}

operation_manifest! {
    ViaNodesOperation {
        method: ViaNodes::via_nodes;
        scope: element;

        kernel {
            parameters: <>;
            input: Indexed<EdgeIndex, Unit>;
            output: Indexed<ExpandedIndex<EdgeIndex, EdgeEndpointRole>, EntityReference<NodeIndex>>;
            emission: Expanding<Ordered>;
        }

        kernel {
            parameters: <I: IndexDomain>;
            input: Indexed<I, EntityReference<EdgeIndex>>;
            output: Indexed<ExpandedIndex<I, EdgeEndpointRole>, EntityReference<NodeIndex>>;
            emission: Expanding<Ordered>;
        }

        kernel {
            parameters: <>;
            input: Indexed<GroupIndex, Unit>;
            output: Indexed<ExpandedIndex<GroupIndex, NodeIndex>, EntityReference<NodeIndex>>;
            emission: Expanding<Unordered>;
        }

        kernel {
            parameters: <I: IndexDomain>;
            input: Indexed<I, EntityReference<GroupIndex>>;
            output: Indexed<ExpandedIndex<I, NodeIndex>, EntityReference<NodeIndex>>;
            emission: Expanding<Unordered>;
        }
    }
}
