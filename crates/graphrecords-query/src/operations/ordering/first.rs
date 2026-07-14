use crate::{
    Bare, EvaluateOperand, Explain, IndexDomain, Indexed, Multiple, Operand, Ordered, QueryResult,
    Single, ValueType,
    execution::EvaluationCache,
    operands::OperandHandle,
    operations::{Apply, BareStream, Kernel, KeyedStream, Operation, OperationContext, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
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

impl<I, V> Kernel<Indexed<I, V>, Multiple<Ordered>> for FirstOperation
where
    I: IndexDomain,
    V: ValueType,
{
    type Output = OperandHandle<Indexed<I, V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: KeyedStream<'a, I, V, Multiple<Ordered>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(values.next())
    }

    fn estimate(&self, _input: Estimate, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl<V> Kernel<Bare<V>, Multiple<Ordered>> for FirstOperation
where
    V: ValueType,
{
    type Output = OperandHandle<Bare<V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: BareStream<'a, V, Multiple<Ordered>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(values.next())
    }

    fn estimate(&self, _input: Estimate, _stats: &Stats) -> Estimate {
        Estimate::singleton()
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
