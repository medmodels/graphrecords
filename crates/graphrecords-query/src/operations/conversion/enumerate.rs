use crate::{
    Bare, EvaluateOperand, Explain, IndexDomain, Indexed, Multiple, Operand, Positional,
    QueryResult, Sorted, ValueType,
    execution::EvaluationCache,
    operands::OperandHandle,
    operations::{Apply, BareStream, Kernel, KeyedStream, Operation, OperationContext, Prepare},
    optimizer::{EstimateCost, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Enumerate,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "Enumerate")]
#[plan(optimizer_hints(distinct))]
pub struct EnumerateOperation;

impl Prepare for EnumerateOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<V: ValueType> Kernel<Bare<V>, Multiple<Sorted>> for EnumerateOperation {
    type Output = OperandHandle<Indexed<Positional, V>, Multiple<Sorted>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<Sorted>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(Box::new(values.enumerate()))
    }
}

impl<I: IndexDomain, V: ValueType> Kernel<Indexed<I, V>, Multiple<Sorted>> for EnumerateOperation {
    type Output = OperandHandle<Indexed<Positional, V>, Multiple<Sorted>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<Sorted>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(Box::new(
            values
                .enumerate()
                .map(|(position, (_index, value))| (position, value)),
        ))
    }
}

impl<V: ValueType> EstimateCost<EnumerateOperation> for OperandHandle<Bare<V>, Multiple<Sorted>> {
    type OutputCost = <OperandHandle<Indexed<Positional, V>, Multiple<Sorted>> as Operand>::Cost;

    fn estimate(
        _operation: &EnumerateOperation,
        input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        input_cost
    }
}

impl<I: IndexDomain, V: ValueType> EstimateCost<EnumerateOperation>
    for OperandHandle<Indexed<I, V>, Multiple<Sorted>>
{
    type OutputCost = <OperandHandle<Indexed<Positional, V>, Multiple<Sorted>> as Operand>::Cost;

    fn estimate(
        _operation: &EnumerateOperation,
        input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        input_cost
    }
}

impl<O> Enumerate for O
where
    O: Apply<EnumerateOperation>,
{
    type ReturnOperand = <O as Apply<EnumerateOperation>>::Output;

    fn enumerate(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), EnumerateOperation))
    }
}
