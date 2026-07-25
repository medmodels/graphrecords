use crate::{
    Bare, EntityDomain, EntityReference, Explain, Failure, IndexDomain, IndexValue, Indexed,
    Labeled, Operand, QueryResult,
    execution::EvaluationCache,
    operations::{
        Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Pipeline, Prepare,
        Preserving,
    },
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    traits::Resolve,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "Resolve")]
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
    type OutShape = Indexed<I, EntityReference<E>>;
    type Retention = Preserving;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, IndexValue<E>>, Self>> {
        Ok(Pipeline::default().map(
            move |(index, value): (I::Index<'a>, QueryResult<E::Owned>)| {
                let reference = value.and_then(|identifier| {
                    E::resolve_index(graphrecord, &identifier)
                        .map_err(|error| Failure::new_at(Self::LABEL, error, &index))
                });

                (index, reference)
            },
        ))
    }
}

impl<E: EntityDomain> ElementKernel<Bare<IndexValue<E>>> for ResolveOperation {
    type OutShape = Bare<EntityReference<E>>;
    type Retention = Preserving;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<IndexValue<E>>, Self>> {
        Ok(
            Pipeline::default().map(move |value: QueryResult<E::Owned>| {
                value.and_then(|identifier| {
                    E::resolve_index(graphrecord, &identifier)
                        .map_err(|error| Failure::new(Self::LABEL, error))
                })
            }),
        )
    }
}

impl<O> Resolve for O
where
    O: Apply<ResolveOperation>,
{
    type ReturnOperand = <O as Apply<ResolveOperation>>::Output;

    fn resolve(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), ResolveOperation))
    }
}
