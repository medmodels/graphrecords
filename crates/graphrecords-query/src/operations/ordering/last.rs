use crate::{
    Bare, BareValueDomain, EvaluateExpression, Explain, IndexDomain, Indexed, Multiple, Ordered,
    QueryResult, Single, ValueDomain,
    expressions::ExpressionHandle,
    operations::{BareStream, Build, KeyedStream, LaneKernel, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Last,
};
use graphrecords_core::GraphRecord;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Lane)]
#[explain(label = "Last")]
#[plan(optimizer_hints(empty = if_any))]
pub struct LastOperation;

impl<I: IndexDomain, V: ValueDomain> LaneKernel<Indexed<I, V>, Multiple<Ordered>>
    for LastOperation
{
    type Output = ExpressionHandle<Indexed<I, V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<Ordered>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        Ok(values.last())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<V: BareValueDomain> LaneKernel<Bare<V>, Multiple<Ordered>> for LastOperation {
    type Output = ExpressionHandle<Bare<V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<Ordered>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        Ok(values.last())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<E: Build<LastOperation>> Last for E {
    type Output = E::Output;

    fn last(&self) -> Self::Output {
        self.build(LastOperation)
    }
}

operation_manifest! {
    LastOperation {
        method: Last::last;
        scope: lane;

        kernel {
            parameters: <I: IndexDomain, V: ValueDomain>;
            input: (Indexed<I, V>, Multiple<Ordered>);
            output: ExpressionHandle<Indexed<I, V>, Single>;
        }

        kernel {
            parameters: <V: BareValueDomain>;
            input: (Bare<V>, Multiple<Ordered>);
            output: ExpressionHandle<Bare<V>, Single>;
        }
    }
}
