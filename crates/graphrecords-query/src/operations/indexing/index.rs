use crate::{
    Explain, IndexDomain, IndexValue, Indexed, Multiple, Operand, OrderState, QueryResult, Unit,
    execution::EvaluationCache,
    operands::{IndicesOperand, OperandHandle},
    operations::{Apply, ElementKernel, Operation, OperationContext, Pipeline, Prepare},
    optimizer::{
        EstimateCost, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats, ValueCost,
    },
    traits::Index,
};
use graphrecords_core::GraphRecord;

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

impl<I: IndexDomain> ElementKernel<Indexed<I, Unit>> for IndexOperation {
    type OutShape = Indexed<I, IndexValue<I>>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<
        Pipeline<'a, (I::Index<'a>, QueryResult<()>), (I::Index<'a>, QueryResult<I::Index<'a>>)>,
    > {
        Ok(
            Pipeline::default().map(|(index, membership): (I::Index<'a>, QueryResult<()>)| {
                let promoted = membership.map(|()| index.clone());

                (index, promoted)
            }),
        )
    }
}

impl<I: IndexDomain, O: OrderState> EstimateCost<IndexOperation>
    for OperandHandle<Indexed<I, Unit>, Multiple<O>>
{
    type OutputCost = <IndicesOperand<I, O> as Operand>::Cost;

    fn estimate(
        _operation: &IndexOperation,
        input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        ValueCost::new(input_cost, input_cost)
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
