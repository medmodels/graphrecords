use crate::{
    EntityIndexDomain, EntityRef, EntityReference, Explain, IndexDomain, IndexValue, Indexed,
    QueryResult, Unit,
    element::{Pipeline, Preserving},
    operations::{Build, ElementKernel, ElementPipeline, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Index,
};
use graphrecords_core::GraphRecord;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Element)]
#[explain(label = "Index")]
#[plan(optimizer_hints(commutes_with_filter, allows_limit_pushdown, empty = if_any))]
pub struct IndexOperation;

impl<K: IndexDomain> ElementKernel<Indexed<K, Unit>> for IndexOperation {
    type Emission = Preserving;
    type OutShape = Indexed<K, IndexValue<K>>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<K, Unit>, Self>> {
        Ok(Pipeline::keyed(move |address, value: QueryResult<_>| {
            value.map(|()| K::index(graphrecord, &address))
        }))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: input.elements,
            ..input
        }
    }
}

impl<E: EntityIndexDomain, I: IndexDomain> ElementKernel<Indexed<I, EntityReference<E>>>
    for IndexOperation
{
    type Emission = Preserving;
    type OutShape = Indexed<I, IndexValue<E>>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, EntityReference<E>>, Self>> {
        Ok(Pipeline::unkeyed(
            |reference: QueryResult<EntityRef<'a, E>>| reference.map(|entity| entity.index()),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<E: Build<IndexOperation>> Index for E {
    type Output = E::Output;

    fn index(&self) -> Self::Output {
        self.build(IndexOperation)
    }
}

operation_manifest! {
    IndexOperation {
        method: Index::index;
        scope: element;

        kernel {
            parameters: <K: IndexDomain>;
            input: Indexed<K, Unit>;
            output: Indexed<K, IndexValue<K>>;
            emission: Preserving;
        }

        kernel {
            parameters: <E: EntityIndexDomain, I: IndexDomain>;
            input: Indexed<I, EntityReference<E>>;
            output: Indexed<I, IndexValue<E>>;
            emission: Preserving;
        }
    }
}
