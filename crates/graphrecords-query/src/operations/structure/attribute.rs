use crate::{
    EntityRef, EntityReference, Explain, Failure, IndexDomain, Indexed, Labeled, QueryResult,
    Scalar, Unit,
    element::{Pipeline, Preserving},
    error::structure::{MissingAttribute, MissingTraversedAttribute},
    execution::EvaluationCache,
    index::EntityAttributes,
    operations::{Build, ElementKernel, ElementPipeline, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Attribute,
};
use graphrecords_core::{GraphRecord, graphrecord::AttributeName};

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "Attribute")]
#[plan(optimizer_hints(allows_limit_pushdown, empty = if_any))]
pub struct AttributeOperation {
    #[explain(label)]
    attribute: AttributeName,
}

impl Prepare for AttributeOperation {
    type Prepared<'a> = &'a AttributeName;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(&self.attribute)
    }
}

impl<I: EntityAttributes> ElementKernel<Indexed<I, Unit>> for AttributeOperation {
    type Emission = Preserving;
    type OutShape = Indexed<I, Scalar>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, Unit>, Self>> {
        let attribute_address = I::resolve_attribute_address(graphrecord, prepared);

        Ok(Pipeline::keyed(
            move |address, membership: QueryResult<_>| {
                membership?;

                attribute_address
                    .and_then(|attribute_address| {
                        I::attribute(graphrecord, &address, attribute_address)
                    })
                    .ok_or_else(|| {
                        Failure::new_at_address::<I, _>(
                            MissingAttribute::new(prepared.clone()),
                            graphrecord,
                            &address,
                            Self::LABEL,
                        )
                    })
            },
        ))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        let mut distinct = I::attribute_cardinality(stats, &self.attribute);
        if let Some(elements) = input.elements {
            distinct = distinct.min(elements);
        }

        Estimate {
            distinct: Some(distinct),
            selectivity: None,
            ..input
        }
    }
}

impl<E: EntityAttributes, I: IndexDomain> ElementKernel<Indexed<I, EntityReference<E>>>
    for AttributeOperation
{
    type Emission = Preserving;
    type OutShape = Indexed<I, Scalar>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, EntityReference<E>>, Self>> {
        let attribute_address = E::resolve_attribute_address(graphrecord, prepared);

        Ok(Pipeline::keyed(
            move |address, reference: QueryResult<EntityRef<'a, E>>| {
                let entity = reference?;

                match attribute_address.and_then(|attribute_address| {
                    E::attribute(graphrecord, entity.address(), attribute_address)
                }) {
                    Some(value) => Ok(value),
                    None => Err(Failure::new_at_address::<I, _>(
                        MissingTraversedAttribute::new(prepared.clone(), entity.into_owned()),
                        graphrecord,
                        &address,
                        Self::LABEL,
                    )),
                }
            },
        ))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        let mut distinct = E::attribute_cardinality(stats, &self.attribute);
        if let Some(input_distinct) = input.distinct {
            distinct = distinct.min(input_distinct);
        }
        if let Some(elements) = input.elements {
            distinct = distinct.min(elements);
        }

        Estimate {
            distinct: Some(distinct),
            selectivity: None,
            ..input
        }
    }
}

impl<E: Build<AttributeOperation>> Attribute for E {
    type Output = E::Output;

    fn attribute(&self, attribute: AttributeName) -> Self::Output {
        self.build(AttributeOperation { attribute })
    }
}

operation_manifest! {
    AttributeOperation {
        method: Attribute::attribute;
        scope: element;

        kernel {
            parameters: <I: EntityAttributes>;
            field: attribute: AttributeName;
            input: Indexed<I, Unit>;
            output: Indexed<I, Scalar>;
            emission: Preserving;
        }

        kernel {
            parameters: <E: EntityAttributes, I: IndexDomain>;
            field: attribute: AttributeName;
            input: Indexed<I, EntityReference<E>>;
            output: Indexed<I, Scalar>;
            emission: Preserving;
        }
    }
}
