use crate::{
    Diagnostic, EntityReference, Explain, Failure, IndexDomain, Indexed, Labeled, Operand,
    OwnedIndex, QueryResult, Scalar, Unit,
    element::{Pipeline, Preserving},
    execution::EvaluationCache,
    index::EntityAttributes,
    operations::{Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Attribute,
};
use graphrecords_core::{GraphRecord, graphrecord::GraphRecordAttribute};
use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
};

#[derive(Debug)]
pub struct MissingAttribute {
    pub attribute: GraphRecordAttribute,
}

impl Display for MissingAttribute {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "no attribute `{}`", self.attribute)
    }
}

impl Error for MissingAttribute {}

impl Diagnostic for MissingAttribute {
    fn name() -> &'static str {
        "MissingAttribute"
    }

    fn help(&self) -> Option<String> {
        Some(
            "filter the elements using `has_attribute(...)` first or handle missing attributes with `on_error(...)`"
                .to_string(),
        )
    }
}

#[derive(Debug)]
pub struct MissingTraversedAttribute<T: OwnedIndex> {
    pub attribute: GraphRecordAttribute,
    pub entity: T,
}

impl<T: OwnedIndex> Display for MissingTraversedAttribute<T> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "no attribute `{}` on the traversed element `{}`",
            self.attribute, self.entity
        )
    }
}

impl<T: OwnedIndex> Error for MissingTraversedAttribute<T> {}

impl<T: OwnedIndex> Diagnostic for MissingTraversedAttribute<T> {
    fn name() -> &'static str {
        "MissingTraversedAttribute"
    }

    fn help(&self) -> Option<String> {
        Some(
            "filter the elements using `has_attribute(...)` first or handle missing attributes with `on_error(...)`"
                .to_string(),
        )
    }
}

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "Attribute")]
#[plan(optimizer_hints(empty = if_any))]
pub struct AttributeOperation {
    #[explain(label)]
    attribute: GraphRecordAttribute,
}

impl Prepare for AttributeOperation {
    type Prepared<'a> = &'a GraphRecordAttribute;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
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
        Ok(Pipeline::keyed(move |index, membership| {
            if let Err(failure) = membership {
                return Err(failure);
            }

            let attributes = match I::attributes(graphrecord, &index) {
                Ok(attributes) => attributes,
                Err(error) => {
                    let failure = Failure::new_at::<I, _>(Self::LABEL, error, &index);

                    return Err(failure);
                }
            };

            if let Some(value) = attributes.get(prepared) {
                return Ok(value.clone());
            }

            let failure = Failure::new_at::<I, _>(
                Self::LABEL,
                MissingAttribute {
                    attribute: prepared.clone(),
                },
                &index,
            );

            Err(failure)
        }))
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
        Ok(Pipeline::keyed(move |key, reference: QueryResult<_>| {
            reference.and_then(|entity| {
                let attributes = E::attributes(graphrecord, &entity)
                    .map_err(|error| Failure::new_at::<I, _>(Self::LABEL, error, &key))?;

                if let Some(value) = attributes.get(prepared) {
                    return Ok(value.clone());
                }

                Err(Failure::new_at::<I, _>(
                    Self::LABEL,
                    MissingTraversedAttribute {
                        attribute: prepared.clone(),
                        entity: E::to_owned(&entity),
                    },
                    &key,
                ))
            })
        }))
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

impl<O: Apply<AttributeOperation>> Attribute for O {
    type ReturnOperand = O::Output;

    fn attribute(&self, attribute: GraphRecordAttribute) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            AttributeOperation { attribute },
        ))
    }
}
