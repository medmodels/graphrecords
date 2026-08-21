use crate::{
    EntityRef, EntityReference, Explain, Failure, IndexDomain, Indexed, Labeled, Mask, QueryResult,
    Unit,
    element::{Pipeline, Preserving},
    execution::EvaluationCache,
    index::GroupMembership,
    operations::{Build, ElementKernel, ElementPipeline, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::InGroup,
};
use graphrecords_core::{
    GraphRecord,
    errors::GraphRecordError,
    graphrecord::{GroupAddress, GroupIndex, StateView},
};
use graphrecords_utils::aliases::GrHashSet;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "InGroup")]
#[plan(optimizer_hints(commutes_with_filter, allows_limit_pushdown, empty = if_any))]
pub struct InGroupOperation {
    #[explain(label)]
    group_index: GroupIndex,
}

impl Prepare for InGroupOperation {
    type Prepared<'a> = GroupAddress;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        StateView::of(graphrecord)
            .resolve_group_address(&self.group_index)
            .ok_or_else(|| {
                Failure::new(
                    GraphRecordError::GroupNotFound {
                        group_index: self.group_index.clone(),
                    },
                    Self::LABEL,
                )
            })
    }
}

impl<I: GroupMembership> ElementKernel<Indexed<I, Unit>> for InGroupOperation {
    type Emission = Preserving;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, Unit>, Self>> {
        let members: GrHashSet<I::Address> = I::addresses_in_group(graphrecord, prepared).collect();

        Ok(Pipeline::keyed(
            move |address, membership: QueryResult<_>| {
                membership.map(|()| members.contains(&address))
            },
        ))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        let size = I::group_size(stats, &self.group_index);
        let selectivity = input
            .elements
            .map(|elements| size.min(elements) as f64 / elements.max(1) as f64);

        Estimate {
            selectivity,
            ..input
        }
    }
}

impl<E: GroupMembership, I: IndexDomain> ElementKernel<Indexed<I, EntityReference<E>>>
    for InGroupOperation
{
    type Emission = Preserving;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, EntityReference<E>>, Self>> {
        let members: GrHashSet<E::Address> = E::addresses_in_group(graphrecord, prepared).collect();

        Ok(Pipeline::unkeyed(move |reference: QueryResult<_>| {
            reference.map(|entity: EntityRef<'a, E>| members.contains(entity.address()))
        }))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            selectivity: None,
            ..input
        }
    }
}

impl<E: Build<InGroupOperation>> InGroup for E {
    type Output = E::Output;

    fn in_group(&self, group_index: impl Into<GroupIndex>) -> Self::Output {
        self.build(InGroupOperation {
            group_index: group_index.into(),
        })
    }
}

operation_manifest! {
    InGroupOperation {
        method: InGroup::in_group;
        scope: element;

        kernel {
            parameters: <I: GroupMembership>;
            field: group_index: GroupIndex;
            input: Indexed<I, Unit>;
            output: Indexed<I, Mask>;
            emission: Preserving;
        }

        kernel {
            parameters: <E: GroupMembership, I: IndexDomain>;
            field: group_index: GroupIndex;
            input: Indexed<I, EntityReference<E>>;
            output: Indexed<I, Mask>;
            emission: Preserving;
        }
    }
}
