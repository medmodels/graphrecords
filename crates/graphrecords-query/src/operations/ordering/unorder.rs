use crate::{
    Bare, BareValueDomain, EvaluateOperand, Explain, IndexDomain, Indexed, Multiple, Operand,
    OrderState, QueryResult, Unordered, ValueDomain,
    execution::EvaluationCache,
    operands::OperandHandle,
    operations::{
        Apply, BareStream, KeyedStream, LaneKernel, Operation, OperationContext, Prepare,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Unorder,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "Unorder")]
#[plan(optimizer_hints(empty = if_any))]
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

impl<I: IndexDomain, V: ValueDomain, O: OrderState> LaneKernel<Indexed<I, V>, Multiple<O>>
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

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<V: BareValueDomain, O: OrderState> LaneKernel<Bare<V>, Multiple<O>> for UnorderOperation {
    type Output = OperandHandle<Bare<V>, Multiple<Unordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(values)
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<O: Apply<UnorderOperation>> Unorder for O {
    type ReturnOperand = O::Output;

    fn unorder(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), UnorderOperation))
    }
}

operation_manifest! {
    UnorderOperation {
        method: Unorder::unorder;
        scope: lane;

        kernel {
            parameters: <I: IndexDomain, V: ValueDomain, O: OrderState>;
            input: (Indexed<I, V>, Multiple<O>);
            output: OperandHandle<Indexed<I, V>, Multiple<Unordered>>;
        }
        kernel {
            parameters: <V: BareValueDomain, O: OrderState>;
            input: (Bare<V>, Multiple<O>);
            output: OperandHandle<Bare<V>, Multiple<Unordered>>;
        }
    }
}
