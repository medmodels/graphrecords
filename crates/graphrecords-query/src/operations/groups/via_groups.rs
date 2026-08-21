use crate::{
    EntityRef, EntityReference, ExpandedChild, ExpandedIndex, Explain, IndexDomain, Indexed,
    QueryResult, Unit, Unordered,
    element::{Expanding, Pipeline},
    index::GroupMembership,
    operations::{Build, ElementKernel, ElementPipeline, Operation, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    registry::operation_manifest,
    traits::ViaGroups,
};
use graphrecords_core::{GraphRecord, graphrecord::GroupIndex};

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Element)]
#[explain(label = "ViaGroups")]
#[plan(optimizer_hints(empty = if_any))]
pub struct ViaGroupsOperation;

impl<E: GroupMembership> ElementKernel<Indexed<E, Unit>> for ViaGroupsOperation {
    type Emission = Expanding<Unordered>;
    type OutShape = Indexed<ExpandedIndex<E, GroupIndex>, EntityReference<GroupIndex>>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<E, Unit>, Self>> {
        Ok(Pipeline::keyed(move |parent_address, ()| {
            Ok(E::group_addresses(graphrecord, &parent_address)
                .map(|group| ExpandedChild::success(group, EntityRef::new(graphrecord, group)))
                .collect())
        }))
    }
}

impl<E: GroupMembership, I: IndexDomain> ElementKernel<Indexed<I, EntityReference<E>>>
    for ViaGroupsOperation
{
    type Emission = Expanding<Unordered>;
    type OutShape = Indexed<ExpandedIndex<I, GroupIndex>, EntityReference<GroupIndex>>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, EntityReference<E>>, Self>> {
        Ok(Pipeline::unkeyed(move |entity: EntityRef<'a, E>| {
            Ok(E::group_addresses(graphrecord, entity.address())
                .map(|group| ExpandedChild::success(group, EntityRef::new(graphrecord, group)))
                .collect())
        }))
    }
}

impl<E: Build<ViaGroupsOperation>> ViaGroups for E {
    type Output = E::Output;

    fn via_groups(&self) -> Self::Output {
        self.build(ViaGroupsOperation)
    }
}

operation_manifest! {
    ViaGroupsOperation {
        method: ViaGroups::via_groups;
        scope: element;

        kernel {
            parameters: <E: GroupMembership>;
            input: Indexed<E, Unit>;
            output: Indexed<ExpandedIndex<E, GroupIndex>, EntityReference<GroupIndex>>;
            emission: Expanding<Unordered>;
        }

        kernel {
            parameters: <E: GroupMembership, I: IndexDomain>;
            input: Indexed<I, EntityReference<E>>;
            output: Indexed<ExpandedIndex<I, GroupIndex>, EntityReference<GroupIndex>>;
            emission: Expanding<Unordered>;
        }
    }
}
