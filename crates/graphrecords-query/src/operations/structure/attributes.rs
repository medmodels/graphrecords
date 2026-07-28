use crate::index::EntityAttributes;
use crate::{
    AttributeName, Bare, EntityReference, ExpandedChild, ExpandedIndex, Explain, Failure,
    IndexDomain, Indexed, Labeled, Operand, QueryResult, Unit, Unordered,
    element::{Expanding, Pipeline},
    execution::EvaluationCache,
    operations::{Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    traits::Attributes,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "Attributes")]
#[plan(optimizer_hints(empty = if_any))]
pub struct AttributesOperation;

impl Prepare for AttributesOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I: EntityAttributes> ElementKernel<Indexed<I, Unit>> for AttributesOperation {
    type Emission = Expanding<Unordered>;
    type OutShape = Indexed<ExpandedIndex<I, AttributeName>, AttributeName>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, Unit>, Self>> {
        Ok(Pipeline::keyed(move |parent_index, ()| {
            let attributes = I::attributes(graphrecord, &parent_index)
                .map_err(|error| Failure::new_at::<I, _>(Self::LABEL, error, &parent_index))?;

            Ok(attributes
                .keys()
                .cloned()
                .map(|attribute| ExpandedChild::success(attribute.clone(), attribute))
                .collect())
        }))
    }
}

impl<E: EntityAttributes, I: IndexDomain> ElementKernel<Indexed<I, EntityReference<E>>>
    for AttributesOperation
{
    type Emission = Expanding<Unordered>;
    type OutShape = Indexed<ExpandedIndex<I, AttributeName>, AttributeName>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, EntityReference<E>>, Self>> {
        Ok(Pipeline::keyed(move |parent_index, entity| {
            let attributes = E::attributes(graphrecord, &entity)
                .map_err(|error| Failure::new_at::<I, _>(Self::LABEL, error, &parent_index))?;

            Ok(attributes
                .keys()
                .cloned()
                .map(|attribute| ExpandedChild::success(attribute.clone(), attribute))
                .collect())
        }))
    }
}

impl<E: EntityAttributes> ElementKernel<Bare<EntityReference<E>>> for AttributesOperation {
    type Emission = Expanding<Unordered>;
    type OutShape = Bare<AttributeName>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<EntityReference<E>>, Self>> {
        Ok(Pipeline::new(move |outcome| match outcome {
            Err(failure) => vec![Err(failure)],
            Ok(entity) => match E::attributes(graphrecord, &entity) {
                Err(error) => vec![Err(Failure::new(Self::LABEL, error))],
                Ok(attributes) => attributes.keys().cloned().map(Ok).collect(),
            },
        }))
    }
}

impl<O: Apply<AttributesOperation>> Attributes for O {
    type ReturnOperand = O::Output;

    fn attributes(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), AttributesOperation))
    }
}
