use crate::{
    Diagnostic, EntityDomain, EntityReference, Explain, Failure, IndexDomain, Indexed, Labeled,
    Operand, OwnedIndex, QueryResult, Scalar, ToOwnedValue, Unit,
    execution::EvaluationCache,
    operations::{
        Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Pipeline, Prepare,
        Preserving,
    },
    optimizer::{
        EdgeAttributeCardinality, Estimate, NodeAttributeCardinality, OperationInputs,
        OptimizerHints, PlanIdentity, PlanInputs, Stats,
    },
    traits::Attribute,
};
use graphrecords_core::{
    GraphRecord,
    errors::GraphRecordError,
    graphrecord::{AttributeMap, EdgeIndex, GraphRecordAttribute, NodeIndex},
};
use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
};

pub trait EntityAttributes: EntityDomain {
    fn attributes<'a>(
        graphrecord: &'a GraphRecord,
        index: &Self::Index<'a>,
    ) -> Result<&'a AttributeMap, GraphRecordError>;

    fn attribute_cardinality(stats: &Stats, attribute: &GraphRecordAttribute) -> usize;
}

impl EntityAttributes for NodeIndex {
    fn attributes<'a>(
        graphrecord: &'a GraphRecord,
        index: &Self::Index<'a>,
    ) -> Result<&'a AttributeMap, GraphRecordError> {
        graphrecord.node_attributes(index)
    }

    fn attribute_cardinality(stats: &Stats, attribute: &GraphRecordAttribute) -> usize {
        stats.get::<NodeAttributeCardinality>(attribute)
    }
}

impl EntityAttributes for EdgeIndex {
    fn attributes<'a>(
        graphrecord: &'a GraphRecord,
        index: &Self::Index<'a>,
    ) -> Result<&'a AttributeMap, GraphRecordError> {
        graphrecord.edge_attributes(index)
    }

    fn attribute_cardinality(stats: &Stats, attribute: &GraphRecordAttribute) -> usize {
        stats.get::<EdgeAttributeCardinality>(attribute)
    }
}

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "Attribute")]
#[plan(optimizer_hints(empty = if_any))]
pub struct AttributeOperation {
    #[explain(label)]
    pub attribute: GraphRecordAttribute,
}

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
    type OutShape = Indexed<I, Scalar>;
    type Retention = Preserving;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        attribute: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, Unit>, Self>> {
        Ok(Pipeline::default().map(
            move |(index, membership): (I::Index<'a>, QueryResult<()>)| {
                if let Err(failure) = membership {
                    return (index, Err(failure));
                }

                let attributes = match I::attributes(graphrecord, &index) {
                    Ok(attributes) => attributes,
                    Err(error) => {
                        let failure = Failure::new_at(Self::LABEL, error, &index);

                        return (index, Err(failure));
                    }
                };

                if let Some(value) = attributes.get(attribute) {
                    return (index, Ok(value.clone()));
                }

                let failure = Failure::new_at(
                    Self::LABEL,
                    MissingAttribute {
                        attribute: attribute.clone(),
                    },
                    &index,
                );

                (index, Err(failure))
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
    type OutShape = Indexed<I, Scalar>;
    type Retention = Preserving;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        attribute: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, EntityReference<E>>, Self>> {
        Ok(Pipeline::default().map(
            move |(key, reference): (I::Index<'a>, QueryResult<<E as IndexDomain>::Index<'a>>)| {
                let value = reference.and_then(|entity| {
                    let attributes = E::attributes(graphrecord, &entity)
                        .map_err(|error| Failure::new_at(Self::LABEL, error, &key))?;

                    if let Some(value) = attributes.get(attribute) {
                        return Ok(value.clone());
                    }

                    Err(Failure::new_at(
                        Self::LABEL,
                        MissingTraversedAttribute {
                            attribute: attribute.clone(),
                            entity: entity.to_owned_value(),
                        },
                        &key,
                    ))
                });

                (key, value)
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

impl<O> Attribute for O
where
    O: Apply<AttributeOperation>,
{
    type ReturnOperand = <O as Apply<AttributeOperation>>::Output;

    fn attribute(&self, attribute: GraphRecordAttribute) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            AttributeOperation { attribute },
        ))
    }
}
