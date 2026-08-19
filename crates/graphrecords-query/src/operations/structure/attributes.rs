use crate::{
    EntityReference, ExpandedChild, ExpandedIndex, Explain, IndexDomain, Indexed, Operand,
    QueryResult, Unit, Unordered,
    element::{Expanding, Pipeline},
    execution::EvaluationCache,
    index::EntityAttributes,
    operations::{Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    registry::operation_manifest,
    traits::Attributes,
};
use graphrecords_core::{GraphRecord, graphrecord::AttributeName};

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
            let attributes = I::attributes(graphrecord, &parent_index).expect("Entity must exist");

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
        Ok(Pipeline::unkeyed(move |entity| {
            let attributes = E::attributes(graphrecord, &entity).expect("Entity must exist");

            Ok(attributes
                .keys()
                .cloned()
                .map(|attribute| ExpandedChild::success(attribute.clone(), attribute))
                .collect())
        }))
    }
}

impl<O: Apply<AttributesOperation>> Attributes for O {
    type ReturnOperand = O::Output;

    fn attributes(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), AttributesOperation))
    }
}

operation_manifest! {
    AttributesOperation {
        method: Attributes::attributes;
        scope: element;

        kernel {
            parameters: <I: EntityAttributes>;
            input: Indexed<I, Unit>;
            output: Indexed<ExpandedIndex<I, AttributeName>, AttributeName>;
            emission: Expanding<Unordered>;
        }

        kernel {
            parameters: <E: EntityAttributes, I: IndexDomain>;
            input: Indexed<I, EntityReference<E>>;
            output: Indexed<ExpandedIndex<I, AttributeName>, AttributeName>;
            emission: Expanding<Unordered>;
        }
    }
}
