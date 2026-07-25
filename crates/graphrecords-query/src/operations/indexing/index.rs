use crate::{
    AttributeName, AttributeSet, EntityDomain, EntityReference, Explain, FailureKindValue,
    FailureValue, IndexDomain, IndexValue, Indexed, Mask, MaskMap, Operand, QueryResult, Scalar,
    ToOwnedValue, Unit, ValueType,
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
#[explain(label = "Index")]
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
    type OutShape = Indexed<K, IndexValue<K>>;
    type Retention = Preserving;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<K, Scalar>, Self>> {
        Ok(Pipeline::default().map(
            |(index, value): (K::Index<'a>, QueryResult<<Scalar as ValueType>::Value<'a>>)| {
                let promoted = value.map(|_| index.to_owned_value());

                (index, promoted)
            },
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: input.elements,
            ..input
        }
    }
}

impl<K: IndexDomain> ElementKernel<Indexed<K, Mask>> for IndexOperation {
    type OutShape = Indexed<K, IndexValue<K>>;
    type Retention = Preserving;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<K, Mask>, Self>> {
        Ok(Pipeline::default().map(
            |(index, value): (K::Index<'a>, QueryResult<<Mask as ValueType>::Value<'a>>)| {
                let promoted = value.map(|_| index.to_owned_value());

                (index, promoted)
            },
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: input.elements,
            ..input
        }
    }
}

impl<K: IndexDomain> ElementKernel<Indexed<K, AttributeName>> for IndexOperation {
    type OutShape = Indexed<K, IndexValue<K>>;
    type Retention = Preserving;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<K, AttributeName>, Self>> {
        Ok(Pipeline::default().map(
            |(index, value): (
                K::Index<'a>,
                QueryResult<<AttributeName as ValueType>::Value<'a>>,
            )| {
                let promoted = value.map(|_| index.to_owned_value());

                (index, promoted)
            },
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: input.elements,
            ..input
        }
    }
}

impl<K: IndexDomain> ElementKernel<Indexed<K, Unit>> for IndexOperation {
    type OutShape = Indexed<K, IndexValue<K>>;
    type Retention = Preserving;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<K, Unit>, Self>> {
        Ok(Pipeline::default().map(
            |(index, value): (K::Index<'a>, QueryResult<<Unit as ValueType>::Value<'a>>)| {
                let promoted = value.map(|()| index.to_owned_value());

                (index, promoted)
            },
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: input.elements,
            ..input
        }
    }
}

impl<K: IndexDomain> ElementKernel<Indexed<K, AttributeSet>> for IndexOperation {
    type OutShape = Indexed<K, IndexValue<K>>;
    type Retention = Preserving;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<K, AttributeSet>, Self>> {
        Ok(Pipeline::default().map(
            |(index, value): (
                K::Index<'a>,
                QueryResult<<AttributeSet as ValueType>::Value<'a>>,
            )| {
                let promoted = value.map(|_| index.to_owned_value());

                (index, promoted)
            },
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: input.elements,
            ..input
        }
    }
}

impl<K: IndexDomain> ElementKernel<Indexed<K, FailureValue>> for IndexOperation {
    type OutShape = Indexed<K, IndexValue<K>>;
    type Retention = Preserving;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<K, FailureValue>, Self>> {
        Ok(Pipeline::default().map(
            |(index, value): (
                K::Index<'a>,
                QueryResult<<FailureValue as ValueType>::Value<'a>>,
            )| {
                let promoted = value.map(|_| index.to_owned_value());

                (index, promoted)
            },
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: input.elements,
            ..input
        }
    }
}

impl<K: IndexDomain> ElementKernel<Indexed<K, FailureKindValue>> for IndexOperation {
    type OutShape = Indexed<K, IndexValue<K>>;
    type Retention = Preserving;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<K, FailureKindValue>, Self>> {
        Ok(Pipeline::default().map(
            |(index, value): (
                K::Index<'a>,
                QueryResult<<FailureKindValue as ValueType>::Value<'a>>,
            )| {
                let promoted = value.map(|_| index.to_owned_value());

                (index, promoted)
            },
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: input.elements,
            ..input
        }
    }
}

impl<K: IndexDomain, T: 'static + Clone> ElementKernel<Indexed<K, MaskMap<T>>> for IndexOperation {
    type OutShape = Indexed<K, IndexValue<K>>;
    type Retention = Preserving;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<K, MaskMap<T>>, Self>> {
        Ok(Pipeline::default().map(
            |(index, value): (
                K::Index<'a>,
                QueryResult<<MaskMap<T> as ValueType>::Value<'a>>,
            )| {
                let promoted = value.map(|_| index.to_owned_value());

                (index, promoted)
            },
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: input.elements,
            ..input
        }
    }
}

impl<K: IndexDomain, I: IndexDomain> ElementKernel<Indexed<K, IndexValue<I>>> for IndexOperation {
    type OutShape = Indexed<K, IndexValue<K>>;
    type Retention = Preserving;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<K, IndexValue<I>>, Self>> {
        Ok(Pipeline::default().map(
            |(index, value): (
                K::Index<'a>,
                QueryResult<<IndexValue<I> as ValueType>::Value<'a>>,
            )| {
                let promoted = value.map(|_| index.to_owned_value());

                (index, promoted)
            },
        ))
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
    type OutShape = Indexed<I, IndexValue<E>>;
    type Retention = Preserving;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, EntityReference<E>>, Self>> {
        Ok(Pipeline::default().map(
            |(index, reference): (I::Index<'a>, QueryResult<E::Index<'a>>)| {
                (index, reference.map(|entity| entity.to_owned_value()))
            },
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<O> Index for O
where
    O: Apply<IndexOperation>,
{
    type ReturnOperand = <O as Apply<IndexOperation>>::Output;

    fn index(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), IndexOperation))
    }
}
