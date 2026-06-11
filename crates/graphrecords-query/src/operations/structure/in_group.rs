use crate::{
    EvaluateOperand, Explain, Failure, IndexDomain, Indexed, Labeled, Multiple, Operand,
    QueryResult, Unit,
    execution::EvaluationCache,
    operands::{BoolMaskOperand, OperandHandle},
    operations::{Apply, Kernel, KeyedStream, Operation, OperationContext, Prepare},
    optimizer::{
        Cardinality, EdgeGroupSize, EstimateCost, NodeGroupSize, OperationInputs, OptimizerHints,
        PlanIdentity, PlanInputs, Selectivity, Stats,
    },
    traits::InGroup,
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{EdgeIndex, Group, NodeIndex},
};
use graphrecords_utils::aliases::GrHashSet;

pub trait IndicesInGroup: IndexDomain {
    fn indices_in_group<'a>(
        graphrecord: &'a GraphRecord,
        group: &Group,
    ) -> QueryResult<GrHashSet<Self::Index<'a>>>;

    fn group_size(stats: &Stats, group: &Group) -> Cardinality;
}

impl IndicesInGroup for NodeIndex {
    fn indices_in_group<'a>(
        graphrecord: &'a GraphRecord,
        group: &Group,
    ) -> QueryResult<GrHashSet<Self::Index<'a>>> {
        Ok(graphrecord
            .nodes_in_group(group)
            .map_err(|error| Failure::new(<InGroupOperation as Labeled>::LABEL, error))?
            .collect())
    }

    fn group_size(stats: &Stats, group: &Group) -> Cardinality {
        Cardinality(stats.get::<NodeGroupSize>(group))
    }
}

impl IndicesInGroup for EdgeIndex {
    fn indices_in_group<'a>(
        graphrecord: &'a GraphRecord,
        group: &Group,
    ) -> QueryResult<GrHashSet<Self::Index<'a>>> {
        Ok(graphrecord
            .edges_in_group(group)
            .map_err(|error| Failure::new(<InGroupOperation as Labeled>::LABEL, error))?
            .collect())
    }

    fn group_size(stats: &Stats, group: &Group) -> Cardinality {
        Cardinality(stats.get::<EdgeGroupSize>(group))
    }
}

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "InGroup")]
#[plan(optimizer_hints(distinct, empty = if_any))]
pub struct InGroupOperation {
    #[explain(label)]
    group: Group,
}

impl Prepare for InGroupOperation {
    type Prepared<'a> = &'a Group;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(&self.group)
    }
}

impl<I: IndicesInGroup> Kernel<Indexed<I, Unit>, Multiple> for InGroupOperation {
    type Output = BoolMaskOperand<I>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, Unit, Multiple>,
        group: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let members = I::indices_in_group(graphrecord, group)?;

        Ok(Box::new(values.map(move |(index, membership)| {
            let in_group = membership.map(|()| members.contains(&index));

            (index, in_group)
        })))
    }
}

impl<I: IndicesInGroup> EstimateCost<InGroupOperation>
    for OperandHandle<Indexed<I, Unit>, Multiple>
{
    type OutputCost = <BoolMaskOperand<I> as Operand>::Cost;

    fn estimate(
        operation: &InGroupOperation,
        input_cost: <Self as Operand>::Cost,
        stats: &Stats,
    ) -> Self::OutputCost {
        Selectivity::ratio(I::group_size(stats, &operation.group), input_cost)
    }
}

impl<O> InGroup for O
where
    O: Apply<InGroupOperation>,
{
    type ReturnOperand = <O as Apply<InGroupOperation>>::Output;

    fn in_group(&self, group: Group) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            InGroupOperation { group },
        ))
    }
}
