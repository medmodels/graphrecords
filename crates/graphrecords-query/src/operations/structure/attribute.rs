use crate::{
    Explain, Failure, IndexDomain, IndexValue, Indexed, Labeled, Multiple, Operand, OrderState,
    QueryResult, Scalar, Unit,
    execution::EvaluationCache,
    operands::{OperandHandle, ValuesOperand},
    operations::{Apply, ElementKernel, Operation, OperationContext, Pipeline, Prepare},
    optimizer::{
        Cardinality, EdgeAttributeCardinality, EstimateCost, NodeAttributeCardinality,
        OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats, ValueCost,
    },
    traits::Attribute,
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{AttributeMap, EdgeIndex, GraphRecordAttribute, GraphRecordValue, NodeIndex},
};
use std::{
    error::Error,
    fmt::{self, Display, Formatter},
};

pub trait EntityAttributes: IndexDomain {
    fn attributes<'a>(graphrecord: &'a GraphRecord, index: &Self::Index<'a>) -> &'a AttributeMap;

    fn attribute_cardinality(stats: &Stats, attribute: &GraphRecordAttribute) -> Cardinality;
}

impl EntityAttributes for NodeIndex {
    fn attributes<'a>(graphrecord: &'a GraphRecord, index: &Self::Index<'a>) -> &'a AttributeMap {
        graphrecord.node_attributes(index).expect("Node must exist")
    }

    fn attribute_cardinality(stats: &Stats, attribute: &GraphRecordAttribute) -> Cardinality {
        Cardinality(stats.get::<NodeAttributeCardinality>(attribute))
    }
}

impl EntityAttributes for EdgeIndex {
    fn attributes<'a>(graphrecord: &'a GraphRecord, index: &Self::Index<'a>) -> &'a AttributeMap {
        graphrecord.edge_attributes(index).expect("Edge must exist")
    }

    fn attribute_cardinality(stats: &Stats, attribute: &GraphRecordAttribute) -> Cardinality {
        Cardinality(stats.get::<EdgeAttributeCardinality>(attribute))
    }
}

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "Attribute")]
#[plan(optimizer_hints(distinct, empty = if_any))]
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

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        attribute: Self::Prepared<'a>,
    ) -> QueryResult<
        Pipeline<
            'a,
            (I::Index<'a>, QueryResult<()>),
            (I::Index<'a>, QueryResult<GraphRecordValue>),
        >,
    > {
        Ok(Pipeline::default().map(
            move |(index, membership): (I::Index<'a>, QueryResult<()>)| {
                if let Err(failure) = membership {
                    return (index, Err(failure));
                }

                let attributes = I::attributes(graphrecord, &index);

                if let Some(value) = attributes.get(attribute) {
                    return (index, Ok(value.clone()));
                }

                let mut available: Vec<_> = attributes.keys().map(ToString::to_string).collect();
                available.sort();

                let help = if available.is_empty() {
                    "no attributes are present here".to_string()
                } else {
                    format!("available attributes: {}", available.join(", "))
                };

                let failure = Failure::new(
                    Self::LABEL,
                    MissingAttribute {
                        attribute: attribute.clone(),
                    },
                )
                .at(&index)
                .help(help);

                (index, Err(failure))
            },
        ))
    }
}

impl<I: EntityAttributes, O: OrderState> EstimateCost<AttributeOperation>
    for OperandHandle<Indexed<I, Unit>, Multiple<O>>
{
    type OutputCost = <ValuesOperand<I, O> as Operand>::Cost;

    fn estimate(
        operation: &AttributeOperation,
        input_cost: <Self as Operand>::Cost,
        stats: &Stats,
    ) -> Self::OutputCost {
        ValueCost::new(
            input_cost,
            I::attribute_cardinality(stats, &operation.attribute),
        )
    }
}

impl<K: IndexDomain, E: EntityAttributes> ElementKernel<Indexed<K, IndexValue<E>>>
    for AttributeOperation
{
    type OutShape = Indexed<K, Scalar>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        attribute: Self::Prepared<'a>,
    ) -> QueryResult<
        Pipeline<
            'a,
            (K::Index<'a>, QueryResult<<E as IndexDomain>::Index<'a>>),
            (K::Index<'a>, QueryResult<GraphRecordValue>),
        >,
    > {
        Ok(Pipeline::default().map(
            move |(key, reference): (K::Index<'a>, QueryResult<<E as IndexDomain>::Index<'a>>)| {
                let value = reference.and_then(|entity| {
                    let attributes = E::attributes(graphrecord, &entity);

                    if let Some(value) = attributes.get(attribute) {
                        return Ok(value.clone());
                    }

                    let mut available: Vec<_> =
                        attributes.keys().map(ToString::to_string).collect();
                    available.sort();

                    let help = if available.is_empty() {
                        "no attributes are present here".to_string()
                    } else {
                        format!("available attributes: {}", available.join(", "))
                    };

                    Err(Failure::new(
                        Self::LABEL,
                        MissingAttribute {
                            attribute: attribute.clone(),
                        },
                    )
                    .at(&key)
                    .help(help))
                });

                (key, value)
            },
        ))
    }
}

impl<K: IndexDomain, E: EntityAttributes, O: OrderState> EstimateCost<AttributeOperation>
    for OperandHandle<Indexed<K, IndexValue<E>>, Multiple<O>>
{
    type OutputCost = <ValuesOperand<K, O> as Operand>::Cost;

    fn estimate(
        operation: &AttributeOperation,
        input_cost: <Self as Operand>::Cost,
        stats: &Stats,
    ) -> Self::OutputCost {
        let cardinality = E::attribute_cardinality(stats, &operation.attribute);

        ValueCost::new(input_cost.rows(), cardinality.min(input_cost.distinct()))
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
