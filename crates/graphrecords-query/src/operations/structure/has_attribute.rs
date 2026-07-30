use crate::{
    EntityReference, Explain, IndexDomain, Indexed, Mask, Operand, QueryResult, Unit,
    element::{Pipeline, Preserving},
    execution::EvaluationCache,
    index::EntityAttributes,
    operations::{Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    traits::HasAttribute,
};
use graphrecords_core::{GraphRecord, graphrecord::GraphRecordAttribute};

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "HasAttribute")]
#[plan(optimizer_hints(empty = if_any))]
pub struct HasAttributeOperation {
    #[explain(label)]
    attribute: GraphRecordAttribute,
}

impl Prepare for HasAttributeOperation {
    type Prepared<'a> = &'a GraphRecordAttribute;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(&self.attribute)
    }
}

impl<E: EntityAttributes> ElementKernel<Indexed<E, Unit>> for HasAttributeOperation {
    type Emission = Preserving;
    type OutShape = Indexed<E, Mask>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<E, Unit>, Self>> {
        Ok(Pipeline::keyed(move |index, membership: QueryResult<_>| {
            membership.map(|()| {
                E::attributes(graphrecord, &index)
                    .expect("Entity must exist")
                    .contains_key(prepared)
            })
        }))
    }
}

impl<E: EntityAttributes, I: IndexDomain> ElementKernel<Indexed<I, EntityReference<E>>>
    for HasAttributeOperation
{
    type Emission = Preserving;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, EntityReference<E>>, Self>> {
        Ok(Pipeline::unkeyed(move |reference: QueryResult<_>| {
            reference.map(|entity| {
                E::attributes(graphrecord, &entity)
                    .expect("Entity must exist")
                    .contains_key(prepared)
            })
        }))
    }
}

impl<O: Apply<HasAttributeOperation>> HasAttribute for O {
    type ReturnOperand = O::Output;

    fn has_attribute(&self, attribute: GraphRecordAttribute) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            HasAttributeOperation { attribute },
        ))
    }
}
