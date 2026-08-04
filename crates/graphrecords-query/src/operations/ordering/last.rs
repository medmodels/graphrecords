use crate::{
    Bare, BareValueDomain, EvaluateOperand, Explain, IndexDomain, Indexed, Multiple, Operand,
    Ordered, QueryResult, Single, ValueDomain,
    execution::EvaluationCache,
    operands::OperandHandle,
    operations::{
        Apply, BareStream, KeyedStream, LaneKernel, Operation, OperationContext, Prepare,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Last,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "Last")]
#[plan(optimizer_hints(empty = if_any))]
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

impl<I: IndexDomain, V: ValueDomain> LaneKernel<Indexed<I, V>, Multiple<Ordered>>
    for LastOperation
{
    type Output = OperandHandle<Indexed<I, V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<Ordered>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(values.last())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<V: BareValueDomain> LaneKernel<Bare<V>, Multiple<Ordered>> for LastOperation {
    type Output = OperandHandle<Bare<V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<Ordered>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(values.last())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<O: Apply<LastOperation>> Last for O {
    type ReturnOperand = O::Output;

    fn last(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), LastOperation))
    }
}

operation_manifest! {
    LastOperation {
        method: Last::last;
        scope: lane;

        kernel {
            parameters: <I: IndexDomain, V: ValueDomain>;
            input: (Indexed<I, V>, Multiple<Ordered>);
            output: OperandHandle<Indexed<I, V>, Single>;
        }
        kernel {
            parameters: <V: BareValueDomain>;
            input: (Bare<V>, Multiple<Ordered>);
            output: OperandHandle<Bare<V>, Single>;
        }
    }
}
