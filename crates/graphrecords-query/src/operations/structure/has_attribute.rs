use crate::{
    EntityRef, EntityReference, Explain, IndexDomain, Indexed, Mask, QueryResult, Unit,
    element::{Pipeline, Preserving},
    execution::EvaluationCache,
    index::EntityAttributes,
    operations::{Build, ElementKernel, ElementPipeline, Operation, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    registry::operation_manifest,
    traits::HasAttribute,
};
use graphrecords_core::{GraphRecord, graphrecord::AttributeName};

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "HasAttribute")]
#[plan(optimizer_hints(commutes_with_filter, allows_limit_pushdown, empty = if_any))]
pub struct HasAttributeOperation {
    #[explain(label)]
    attribute: AttributeName,
}

impl Prepare for HasAttributeOperation {
    type Prepared<'a> = &'a AttributeName;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
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
        let attribute_address = E::resolve_attribute_address(graphrecord, prepared);

        Ok(Pipeline::keyed(
            move |address, membership: QueryResult<_>| {
                membership.map(|()| {
                    attribute_address.is_some_and(|attribute_address| {
                        E::attribute(graphrecord, &address, attribute_address).is_some()
                    })
                })
            },
        ))
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
        let attribute_address = E::resolve_attribute_address(graphrecord, prepared);

        Ok(Pipeline::unkeyed(move |reference: QueryResult<_>| {
            reference.map(|entity: EntityRef<'a, E>| {
                attribute_address.is_some_and(|attribute_address| {
                    E::attribute(graphrecord, entity.address(), attribute_address).is_some()
                })
            })
        }))
    }
}

impl<E: Build<HasAttributeOperation>> HasAttribute for E {
    type Output = E::Output;

    fn has_attribute(&self, attribute: AttributeName) -> Self::Output {
        self.build(HasAttributeOperation { attribute })
    }
}

operation_manifest! {
    HasAttributeOperation {
        method: HasAttribute::has_attribute;
        scope: element;

        kernel {
            parameters: <E: EntityAttributes>;
            field: attribute: AttributeName;
            input: Indexed<E, Unit>;
            output: Indexed<E, Mask>;
            emission: Preserving;
        }

        kernel {
            parameters: <E: EntityAttributes, I: IndexDomain>;
            field: attribute: AttributeName;
            input: Indexed<I, EntityReference<E>>;
            output: Indexed<I, Mask>;
            emission: Preserving;
        }
    }
}
