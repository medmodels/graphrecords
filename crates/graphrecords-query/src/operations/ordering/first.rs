use crate::{
    Bare, EvaluateOperand, Explain, IndexDomain, Indexed, Multiple, Operand, QueryResult, Single,
    Sorted, ValueType,
    execution::EvaluationCache,
    operands::OperandHandle,
    operations::{Apply, BareStream, Kernel, KeyedStream, Operation, OperationContext, Prepare},
    optimizer::{
        Cardinality, EstimateCost, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats,
    },
    traits::First,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "First")]
pub struct FirstOperation;

impl Prepare for FirstOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I, V> Kernel<Indexed<I, V>, Multiple<Sorted>> for FirstOperation
where
    I: IndexDomain,
    V: ValueType<Cost = Cardinality>,
{
    type Output = OperandHandle<Indexed<I, V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: KeyedStream<'a, I, V, Multiple<Sorted>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(values.next())
    }
}

impl<I, V> EstimateCost<FirstOperation> for OperandHandle<Indexed<I, V>, Multiple<Sorted>>
where
    I: IndexDomain,
    V: ValueType<Cost = Cardinality>,
{
    type OutputCost = <OperandHandle<Indexed<I, V>, Single> as Operand>::Cost;

    fn estimate(
        _operation: &FirstOperation,
        _input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        Cardinality(1)
    }
}

impl<V> Kernel<Bare<V>, Multiple<Sorted>> for FirstOperation
where
    V: ValueType<Cost = Cardinality>,
{
    type Output = OperandHandle<Bare<V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: BareStream<'a, V, Multiple<Sorted>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(values.next())
    }
}

impl<V> EstimateCost<FirstOperation> for OperandHandle<Bare<V>, Multiple<Sorted>>
where
    V: ValueType<Cost = Cardinality>,
{
    type OutputCost = <OperandHandle<Bare<V>, Single> as Operand>::Cost;

    fn estimate(
        _operation: &FirstOperation,
        _input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        Cardinality(1)
    }
}

impl<S> First for S
where
    S: Apply<FirstOperation>,
{
    type ReturnOperand = <S as Apply<FirstOperation>>::Output;

    fn first(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), FirstOperation))
    }
}
