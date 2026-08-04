use crate::{
    Bare, EntityDomain, EntityReference, Explain, Failure, IndexDomain, IndexValue, Indexed,
    Labeled, Operand, QueryResult,
    element::{Pipeline, Preserving},
    execution::EvaluationCache,
    operations::{Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    registry::operation_manifest,
    traits::Resolve,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "Resolve")]
#[plan(optimizer_hints(allows_limit_pushdown, empty = if_any))]
pub struct ResolveOperation;

impl Prepare for ResolveOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<E: EntityDomain, I: IndexDomain> ElementKernel<Indexed<I, IndexValue<E>>>
    for ResolveOperation
{
    type Emission = Preserving;
    type OutShape = Indexed<I, EntityReference<E>>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, IndexValue<E>>, Self>> {
        Ok(Pipeline::keyed(move |index, value: QueryResult<_>| {
            value.and_then(|identifier| {
                E::resolve_index(graphrecord, &identifier)
                    .map_err(|error| Failure::new_at::<I, _>(Self::LABEL, error, &index))
            })
        }))
    }
}

impl<E: EntityDomain> ElementKernel<Bare<IndexValue<E>>> for ResolveOperation {
    type Emission = Preserving;
    type OutShape = Bare<EntityReference<E>>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<IndexValue<E>>, Self>> {
        Ok(Pipeline::new(move |value: QueryResult<_>| {
            value.and_then(|identifier| {
                E::resolve_index(graphrecord, &identifier)
                    .map_err(|error| Failure::new(Self::LABEL, error))
            })
        }))
    }
}

impl<O: Apply<ResolveOperation>> Resolve for O {
    type ReturnOperand = O::Output;

    fn resolve(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), ResolveOperation))
    }
}

operation_manifest! {
    ResolveOperation {
        method: Resolve::resolve;
        scope: element;

        kernel {
            parameters: <E: EntityDomain, I: IndexDomain>;
            input: Indexed<I, IndexValue<E>>;
            output: Indexed<I, EntityReference<E>>;
            emission: Preserving;
        }

        kernel {
            parameters: <E: EntityDomain>;
            input: Bare<IndexValue<E>>;
            output: Bare<EntityReference<E>>;
            emission: Preserving;
        }
    }
}
