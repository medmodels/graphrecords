use crate::{
    EntityDomain, EntityReference, Explain, IndexDomain, IndexValue, Indexed, Operand, QueryResult,
    Unit,
    element::{Pipeline, Preserving},
    execution::EvaluationCache,
    operations::{Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Index,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "Index")]
#[plan(optimizer_hints(commutes_with_filter, allows_limit_pushdown, empty = if_any))]
pub struct IndexOperation;

impl Prepare for IndexOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<K: IndexDomain> ElementKernel<Indexed<K, Unit>> for IndexOperation {
    type Emission = Preserving;
    type OutShape = Indexed<K, IndexValue<K>>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<K, Unit>, Self>> {
        Ok(Pipeline::keyed(|index, value: QueryResult<_>| {
            value.map(|()| K::to_owned(&index))
        }))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: input.elements,
            ..input
        }
    }
}

impl<E: EntityDomain, I: IndexDomain> ElementKernel<Indexed<I, EntityReference<E>>>
    for IndexOperation
{
    type Emission = Preserving;
    type OutShape = Indexed<I, IndexValue<E>>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, EntityReference<E>>, Self>> {
        Ok(Pipeline::unkeyed(|reference: QueryResult<_>| {
            reference.map(|entity| E::to_owned(&entity))
        }))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<O: Apply<IndexOperation>> Index for O {
    type ReturnOperand = O::Output;

    fn index(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), IndexOperation))
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
            parameters: <E: EntityDomain, I: IndexDomain>;
            input: Indexed<I, EntityReference<E>>;
            output: Indexed<I, IndexValue<E>>;
            emission: Preserving;
        }
    }
}
