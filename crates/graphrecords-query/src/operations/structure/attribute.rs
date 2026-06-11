use crate::{
    EvaluateOperand, Explain, Failure, IndexDomain, Indexed, Labeled, Multiple, Operand,
    QueryResult, Unit,
    execution::EvaluationCache,
    operands::{OperandHandle, ValuesOperand},
    operations::{Apply, Kernel, KeyedStream, Operation, OperationContext, Prepare},
    optimizer::{EstimateCost, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Attribute,
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{AttributeMap, EdgeIndex, GraphRecordAttribute, NodeIndex},
};
use std::{
    error::Error,
    fmt::{self, Display, Formatter},
};

pub trait EntityAttributes: IndexDomain {
    fn attributes<'a>(graphrecord: &'a GraphRecord, index: &Self::Index<'a>) -> &'a AttributeMap;
}

impl EntityAttributes for NodeIndex {
    fn attributes<'a>(graphrecord: &'a GraphRecord, index: &Self::Index<'a>) -> &'a AttributeMap {
        graphrecord.node_attributes(index).expect("Node must exist")
    }
}

impl EntityAttributes for EdgeIndex {
    fn attributes<'a>(graphrecord: &'a GraphRecord, index: &Self::Index<'a>) -> &'a AttributeMap {
        graphrecord.edge_attributes(index).expect("Edge must exist")
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

fn read_attributes<'a, I>(
    graphrecord: &'a GraphRecord,
    indices: KeyedStream<'a, I, Unit, Multiple>,
    attribute: &'a GraphRecordAttribute,
) -> <ValuesOperand<I> as EvaluateOperand>::ReturnValue<'a>
where
    I: EntityAttributes,
{
    Box::new(indices.map(move |(index, membership)| {
        if let Err(failure) = membership {
            return (index, Err(failure));
        }

        let attributes = I::attributes(graphrecord, &index);

        if let Some(value) = attributes.get(attribute) {
            return (index, Ok(value.clone()));
        }

        let mut available = attributes
            .keys()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        available.sort();

        let help = if available.is_empty() {
            "no attributes are present here".to_string()
        } else {
            format!("available attributes: {}", available.join(", "))
        };

        let failure = Failure::new(
            AttributeOperation::LABEL,
            MissingAttribute {
                attribute: attribute.clone(),
            },
        )
        .at(&index)
        .help(help);

        (index, Err(failure))
    }))
}

impl<I: EntityAttributes> Kernel<Indexed<I, Unit>, Multiple> for AttributeOperation {
    type Output = ValuesOperand<I>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, Unit, Multiple>,
        attribute: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(read_attributes::<I>(graphrecord, values, attribute))
    }
}

impl<I: EntityAttributes> EstimateCost<AttributeOperation>
    for OperandHandle<Indexed<I, Unit>, Multiple>
{
    type OutputCost = <ValuesOperand<I> as Operand>::Cost;

    fn estimate(
        _operation: &AttributeOperation,
        input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        input_cost
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
