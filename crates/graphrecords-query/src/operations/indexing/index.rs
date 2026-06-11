use crate::{
    EvaluateOperand, Explain, IndexDomain, Indexed, Multiple, Operand, QueryResult, Unit,
    execution::EvaluationCache,
    operands::{IndicesOperand, OperandHandle},
    operations::{Apply, Kernel, KeyedStream, Operation, OperationContext, Prepare},
    optimizer::{EstimateCost, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Index,
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{EdgeIndex, NodeIndex},
};

pub trait OwnedIndex: IndexDomain {
    fn owned(index: Self::Index<'_>) -> Self;
}

impl OwnedIndex for NodeIndex {
    fn owned(index: Self::Index<'_>) -> Self {
        index.clone()
    }
}

impl OwnedIndex for EdgeIndex {
    fn owned(index: Self::Index<'_>) -> Self {
        *index
    }
}

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "Index")]
#[plan(optimizer_hints(distinct))]
pub struct IndexOperation;

impl Prepare for IndexOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I: OwnedIndex> Kernel<Indexed<I, Unit>, Multiple> for IndexOperation {
    type Output = IndicesOperand<I>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, Unit, Multiple>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(Box::new(values.map(|(index, membership)| {
            let promoted = membership.map(|()| I::owned(index.clone()));

            (index, promoted)
        })))
    }
}

impl<I: OwnedIndex> EstimateCost<IndexOperation> for OperandHandle<Indexed<I, Unit>, Multiple> {
    type OutputCost = <IndicesOperand<I> as Operand>::Cost;

    fn estimate(
        _operation: &IndexOperation,
        input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        input_cost
    }
}

impl<O> Index for O
where
    O: Apply<IndexOperation>,
{
    type ReturnOperand = <O as Apply<IndexOperation>>::Output;

    fn index(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), IndexOperation))
    }
}
