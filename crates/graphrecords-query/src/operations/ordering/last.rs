use crate::{
    Bare, EvaluateOperand, Explain, IndexDomain, Indexed, Multiple, Operand, QueryResult, Single,
    Sorted, ValueType,
    execution::EvaluationCache,
    operands::OperandHandle,
    operations::{Apply, BareStream, Kernel, KeyedStream, Operation, OperationContext, Prepare},
    optimizer::{
        Cardinality, EstimateCost, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats,
    },
    traits::Last,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "Last")]
pub struct LastOperation;

impl Prepare for LastOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I, V> Kernel<Indexed<I, V>, Multiple<Sorted>> for LastOperation
where
    I: IndexDomain,
    V: ValueType<Cost = Cardinality>,
{
    type Output = OperandHandle<Indexed<I, V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<Sorted>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(values.last())
    }
}

impl<I, V> EstimateCost<LastOperation> for OperandHandle<Indexed<I, V>, Multiple<Sorted>>
where
    I: IndexDomain,
    V: ValueType<Cost = Cardinality>,
{
    type OutputCost = <OperandHandle<Indexed<I, V>, Single> as Operand>::Cost;

    fn estimate(
        _operation: &LastOperation,
        _input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        Cardinality(1)
    }
}

impl<V> Kernel<Bare<V>, Multiple<Sorted>> for LastOperation
where
    V: ValueType<Cost = Cardinality>,
{
    type Output = OperandHandle<Bare<V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<Sorted>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(values.last())
    }
}

impl<V> EstimateCost<LastOperation> for OperandHandle<Bare<V>, Multiple<Sorted>>
where
    V: ValueType<Cost = Cardinality>,
{
    type OutputCost = <OperandHandle<Bare<V>, Single> as Operand>::Cost;

    fn estimate(
        _operation: &LastOperation,
        _input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        Cardinality(1)
    }
}

impl<S> Last for S
where
    S: Apply<LastOperation>,
{
    type ReturnOperand = <S as Apply<LastOperation>>::Output;

    fn last(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), LastOperation))
    }
}
