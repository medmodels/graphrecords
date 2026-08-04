use crate::{
    Bare, BareValueDomain, EvaluateOperand, Explain, IndexDomain, Indexed, Multiple, Operand,
    Ordered, Positional, QueryResult, ValueDomain,
    execution::EvaluationCache,
    operands::OperandHandle,
    operations::{
        Apply, BareStream, KeyedStream, LaneKernel, Operation, OperationContext, Prepare,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Enumerate,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "Enumerate")]
#[plan(optimizer_hints(empty = if_any))]
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

impl<V: BareValueDomain> LaneKernel<Bare<V>, Multiple<Ordered>> for EnumerateOperation {
    type Output = OperandHandle<Indexed<Positional, V>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<Ordered>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(Box::new(values.enumerate()))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<I: IndexDomain, V: ValueDomain> LaneKernel<Indexed<I, V>, Multiple<Ordered>>
    for EnumerateOperation
{
    type Output = OperandHandle<Indexed<Positional, V>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<Ordered>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(Box::new(
            values
                .enumerate()
                .map(|(position, (_, value))| (position, value)),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<O: Apply<EnumerateOperation>> Enumerate for O {
    type ReturnOperand = O::Output;

    fn enumerate(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), EnumerateOperation))
    }
}

operation_manifest! {
    EnumerateOperation {
        method: Enumerate::enumerate;
        scope: lane;

        kernel {
            parameters: <V: BareValueDomain>;
            input: (Bare<V>, Multiple<Ordered>);
            output: OperandHandle<Indexed<Positional, V>, Multiple<Ordered>>;
        }
        kernel {
            parameters: <
                I: IndexDomain,
                V: ValueDomain,
            >;
            input: (Indexed<I, V>, Multiple<Ordered>);
            output: OperandHandle<Indexed<Positional, V>, Multiple<Ordered>>;
        }
    }
}
