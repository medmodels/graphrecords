use crate::{
    Bare, EvaluateOperand, Explain, IndexDomain, Indexed, Multiple, Operand, Ordered, QueryResult,
    ValueType,
    execution::EvaluationCache,
    operands::OperandHandle,
    operations::{
        Apply, BareStream, KeyedStream, LaneKernel, Operation, OperationContext, Prepare,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Reverse,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "Reverse")]
pub struct ReverseOperation;

impl Prepare for ReverseOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I: IndexDomain, V: ValueType> LaneKernel<Indexed<I, V>, Multiple<Ordered>>
    for ReverseOperation
{
    type Output = OperandHandle<Indexed<I, V>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<Ordered>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let values: Vec<_> = values.collect();

        Ok(Box::new(values.into_iter().rev()))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<V: ValueType> LaneKernel<Bare<V>, Multiple<Ordered>> for ReverseOperation {
    type Output = OperandHandle<Bare<V>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<Ordered>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let values: Vec<_> = values.collect();

        Ok(Box::new(values.into_iter().rev()))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<O: Apply<ReverseOperation>> Reverse for O {
    type ReturnOperand = O::Output;

    fn reverse(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), ReverseOperation))
    }
}
