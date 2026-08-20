use crate::{
    Bare, EntityDomain, EntityRef, EntityReference, Explain, IndexDomain, IndexValue, Indexed,
    Labeled, QueryResult,
    element::{Pipeline, Preserving},
    operations::{Build, ElementKernel, ElementPipeline, Operation, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    registry::operation_manifest,
    traits::Resolve,
};
use graphrecords_core::GraphRecord;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Element)]
#[explain(label = "Resolve")]
#[plan(optimizer_hints(allows_limit_pushdown, empty = if_any))]
pub struct ResolveOperation;

impl<E: EntityDomain, I: IndexDomain> ElementKernel<Indexed<I, IndexValue<E>>>
    for ResolveOperation
{
    type Emission = Preserving;
    type OutShape = Indexed<I, EntityReference<E>>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, IndexValue<E>>, Self>> {
        Ok(Pipeline::keyed(move |address, value: QueryResult<_>| {
            value.and_then(|identifier| {
                let target = E::resolve(graphrecord, &E::own_index(&identifier), Self::LABEL)
                    .map_err(|failure| failure.at_address::<I>(graphrecord, &address))?;

                Ok(EntityRef::new(graphrecord, target))
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
                let target = E::resolve(graphrecord, &E::own_index(&identifier), Self::LABEL)?;

                Ok(EntityRef::new(graphrecord, target))
            })
        }))
    }
}

impl<E: Build<ResolveOperation>> Resolve for E {
    type Output = E::Output;

    fn resolve(&self) -> Self::Output {
        self.build(ResolveOperation)
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
