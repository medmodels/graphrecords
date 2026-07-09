use crate::{
    EvaluateOperand, Explain, IndexDomain, IndexValue, Indexed, Multiple, Operand, OrderState,
    QueryResult, Unit,
    execution::EvaluationCache,
    operands::OperandHandle,
    operations::{Apply, Kernel, KeyedStream, Operation, OperationContext, Prepare},
    optimizer::{EstimateCost, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Select,
};
use graphrecords_core::GraphRecord;
use graphrecords_utils::aliases::GrHashSet;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "Select")]
#[plan(optimizer_hints(distinct))]
pub struct SelectOperation;

impl Prepare for SelectOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<K: IndexDomain, E: IndexDomain, O: OrderState> Kernel<Indexed<K, IndexValue<E>>, Multiple<O>>
    for SelectOperation
{
    type Output = OperandHandle<Indexed<E, Unit>, Multiple>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, K, IndexValue<E>, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let targets: GrHashSet<_> = values
            .map(|(_key, reference)| reference)
            .collect::<QueryResult<_>>()?;

        Ok(Box::new(targets.into_iter().map(|target| (target, Ok(())))))
    }
}

impl<K: IndexDomain, E: IndexDomain, O: OrderState> EstimateCost<SelectOperation>
    for OperandHandle<Indexed<K, IndexValue<E>>, Multiple<O>>
{
    type OutputCost = <OperandHandle<Indexed<E, Unit>, Multiple> as Operand>::Cost;

    fn estimate(
        _operation: &SelectOperation,
        input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        input_cost
    }
}

impl<O> Select for O
where
    O: Apply<SelectOperation>,
{
    type ReturnOperand = <O as Apply<SelectOperation>>::Output;

    fn select(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), SelectOperation))
    }
}
