use crate::{
    Bare, EvaluateOperand, Explain, IndexDomain, Indexed, Multiple, Operand, OrderState,
    QueryResult, Unordered, ValueType,
    execution::EvaluationCache,
    operands::OperandHandle,
    operations::{Apply, BareStream, Kernel, KeyedStream, Operation, OperationContext, Prepare},
    optimizer::{EstimateCost, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Unorder,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "Unorder")]
pub struct UnorderOperation;

impl Prepare for UnorderOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I: IndexDomain, V: ValueType, O: OrderState> Kernel<Indexed<I, V>, Multiple<O>>
    for UnorderOperation
{
    type Output = OperandHandle<Indexed<I, V>, Multiple<Unordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(values)
    }
}

impl<I: IndexDomain, V: ValueType, O: OrderState> EstimateCost<UnorderOperation>
    for OperandHandle<Indexed<I, V>, Multiple<O>>
{
    type OutputCost = <Self as Operand>::Cost;

    fn estimate(
        _operation: &UnorderOperation,
        input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        input_cost
    }
}

impl<V: ValueType, O: OrderState> Kernel<Bare<V>, Multiple<O>> for UnorderOperation {
    type Output = OperandHandle<Bare<V>, Multiple<Unordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(values)
    }
}

impl<V: ValueType, O: OrderState> EstimateCost<UnorderOperation>
    for OperandHandle<Bare<V>, Multiple<O>>
{
    type OutputCost = <Self as Operand>::Cost;

    fn estimate(
        _operation: &UnorderOperation,
        input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        input_cost
    }
}

impl<O> Unorder for O
where
    O: Apply<UnorderOperation>,
{
    type ReturnOperand = <O as Apply<UnorderOperation>>::Output;

    fn unorder(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), UnorderOperation))
    }
}
