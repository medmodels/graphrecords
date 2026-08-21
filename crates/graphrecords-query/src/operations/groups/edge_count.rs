use crate::{
    EntityRef, EntityReference, Explain, IndexDomain, Indexed, QueryResult, Scalar, Unit,
    element::{Pipeline, Preserving},
    operations::{Build, ElementKernel, ElementPipeline, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::EdgeCount,
};
use graphrecords_core::{
    GraphRecord, StateView,
    graphrecord::{GroupIndex, ValueView},
};

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Element)]
#[explain(label = "EdgeCount")]
#[plan(optimizer_hints(commutes_with_filter, allows_limit_pushdown, empty = if_any))]
pub struct EdgeCountOperation;

impl ElementKernel<Indexed<GroupIndex, Unit>> for EdgeCountOperation {
    type Emission = Preserving;
    type OutShape = Indexed<GroupIndex, Scalar>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<GroupIndex, Unit>, Self>> {
        Ok(Pipeline::keyed(
            move |address, membership: QueryResult<_>| {
                membership.map(|()| {
                    ValueView::Int(StateView::of(graphrecord).group_edge_count(address) as i64)
                })
            },
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<I: IndexDomain> ElementKernel<Indexed<I, EntityReference<GroupIndex>>> for EdgeCountOperation {
    type Emission = Preserving;
    type OutShape = Indexed<I, Scalar>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, EntityReference<GroupIndex>>, Self>> {
        Ok(Pipeline::unkeyed(move |reference: QueryResult<_>| {
            reference.map(|group_index: EntityRef<'a, GroupIndex>| {
                ValueView::Int(
                    StateView::of(graphrecord).group_edge_count(*group_index.address()) as i64,
                )
            })
        }))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<E: Build<EdgeCountOperation>> EdgeCount for E {
    type Output = E::Output;

    fn edge_count(&self) -> Self::Output {
        self.build(EdgeCountOperation)
    }
}

operation_manifest! {
    EdgeCountOperation {
        method: EdgeCount::edge_count;
        scope: element;

        kernel {
            parameters: <>;
            input: Indexed<GroupIndex, Unit>;
            output: Indexed<GroupIndex, Scalar>;
            emission: Preserving;
        }

        kernel {
            parameters: <I: IndexDomain>;
            input: Indexed<I, EntityReference<GroupIndex>>;
            output: Indexed<I, Scalar>;
            emission: Preserving;
        }
    }
}
