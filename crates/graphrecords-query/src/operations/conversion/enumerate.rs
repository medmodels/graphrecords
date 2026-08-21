use crate::{
    Bare, BareValueDomain, EvaluateExpression, Explain, IndexDomain, Indexed, Multiple, Ordered,
    Positional, QueryResult, ValueDomain,
    expressions::ExpressionHandle,
    operations::{BareStream, Build, KeyedStream, LaneKernel, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Enumerate,
};
use graphrecords_core::GraphRecord;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Lane)]
#[explain(label = "Enumerate")]
#[plan(optimizer_hints(empty = if_any))]
pub struct EnumerateOperation;

impl<I: IndexDomain, V: ValueDomain> LaneKernel<Indexed<I, V>, Multiple<Ordered>>
    for EnumerateOperation
{
    type Output = ExpressionHandle<Indexed<Positional, V>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<Ordered>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
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

impl<V: BareValueDomain> LaneKernel<Bare<V>, Multiple<Ordered>> for EnumerateOperation {
    type Output = ExpressionHandle<Indexed<Positional, V>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<Ordered>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        Ok(Box::new(values.enumerate()))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<E: Build<EnumerateOperation>> Enumerate for E {
    type Output = E::Output;

    fn enumerate(&self) -> Self::Output {
        self.build(EnumerateOperation)
    }
}

operation_manifest! {
    EnumerateOperation {
        method: Enumerate::enumerate;
        scope: lane;

        kernel {
            parameters: <
                I: IndexDomain,
                V: ValueDomain,
            >;
            input: (Indexed<I, V>, Multiple<Ordered>);
            output: ExpressionHandle<Indexed<Positional, V>, Multiple<Ordered>>;
        }

        kernel {
            parameters: <V: BareValueDomain>;
            input: (Bare<V>, Multiple<Ordered>);
            output: ExpressionHandle<Indexed<Positional, V>, Multiple<Ordered>>;
        }
    }
}
