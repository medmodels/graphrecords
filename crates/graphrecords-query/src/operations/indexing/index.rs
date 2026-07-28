use crate::{
    AttributeName, Bare, EntityDomain, EntityReference, Explain, FailureKindValue, FailureValue,
    IndexDomain, IndexValue, Indexed, Mask, Operand, QueryResult, Scalar, Unit,
    execution::EvaluationCache,
    operations::{
        Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Pipeline, Prepare,
        Preserving,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Index,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "Index")]
#[plan(optimizer_hints(empty = if_any))]
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

impl<K: IndexDomain> ElementKernel<Indexed<K, Scalar>> for IndexOperation {
    type Emission = Preserving;
    type OutShape = Indexed<K, IndexValue<K>>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<K, Scalar>, Self>> {
        Ok(Pipeline::keyed(|index, value: QueryResult<_>| {
            value.map(|_| K::to_owned(&index))
        }))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: input.elements,
            ..input
        }
    }
}

impl<K: IndexDomain> ElementKernel<Indexed<K, Mask>> for IndexOperation {
    type Emission = Preserving;
    type OutShape = Indexed<K, IndexValue<K>>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<K, Mask>, Self>> {
        Ok(Pipeline::keyed(|index, value: QueryResult<_>| {
            value.map(|_| K::to_owned(&index))
        }))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: input.elements,
            ..input
        }
    }
}

impl<K: IndexDomain> ElementKernel<Indexed<K, AttributeName>> for IndexOperation {
    type Emission = Preserving;
    type OutShape = Indexed<K, IndexValue<K>>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<K, AttributeName>, Self>> {
        Ok(Pipeline::keyed(|index, value: QueryResult<_>| {
            value.map(|_| K::to_owned(&index))
        }))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: input.elements,
            ..input
        }
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

impl<K: IndexDomain> ElementKernel<Indexed<K, FailureValue>> for IndexOperation {
    type Emission = Preserving;
    type OutShape = Indexed<K, IndexValue<K>>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<K, FailureValue>, Self>> {
        Ok(Pipeline::keyed(|index, value: QueryResult<_>| {
            value.map(|_| K::to_owned(&index))
        }))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: input.elements,
            ..input
        }
    }
}

impl<K: IndexDomain> ElementKernel<Indexed<K, FailureKindValue>> for IndexOperation {
    type Emission = Preserving;
    type OutShape = Indexed<K, IndexValue<K>>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<K, FailureKindValue>, Self>> {
        Ok(Pipeline::keyed(|index, value: QueryResult<_>| {
            value.map(|_| K::to_owned(&index))
        }))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: input.elements,
            ..input
        }
    }
}

impl<K: IndexDomain, I: IndexDomain> ElementKernel<Indexed<K, IndexValue<I>>> for IndexOperation {
    type Emission = Preserving;
    type OutShape = Indexed<K, IndexValue<K>>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<K, IndexValue<I>>, Self>> {
        Ok(Pipeline::keyed(|index, value: QueryResult<_>| {
            value.map(|_| K::to_owned(&index))
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

impl<E: EntityDomain> ElementKernel<Bare<EntityReference<E>>> for IndexOperation {
    type Emission = Preserving;
    type OutShape = Bare<IndexValue<E>>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<EntityReference<E>>, Self>> {
        Ok(Pipeline::new(|reference: QueryResult<_>| {
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
