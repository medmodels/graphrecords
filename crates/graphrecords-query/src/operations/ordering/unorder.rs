use crate::{
    Bare, BareValueDomain, EvaluateExpression, Explain, IndexDomain, Indexed, Multiple, OrderState,
    QueryResult, Unordered, ValueDomain,
    expressions::ExpressionHandle,
    operations::{BareStream, Build, KeyedStream, LaneKernel, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Unorder,
};
use graphrecords_core::GraphRecord;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Lane)]
#[explain(label = "Unorder")]
#[plan(optimizer_hints(empty = if_any))]
pub struct UnorderOperation;

impl<I: IndexDomain, V: ValueDomain, O: OrderState> LaneKernel<Indexed<I, V>, Multiple<O>>
    for UnorderOperation
{
    type Output = ExpressionHandle<Indexed<I, V>, Multiple<Unordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        Ok(values)
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<V: BareValueDomain, O: OrderState> LaneKernel<Bare<V>, Multiple<O>> for UnorderOperation {
    type Output = ExpressionHandle<Bare<V>, Multiple<Unordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        Ok(values)
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<E: Build<UnorderOperation>> Unorder for E {
    type Output = E::Output;

    fn unorder(&self) -> Self::Output {
        self.build(UnorderOperation)
    }
}

operation_manifest! {
    UnorderOperation {
        method: Unorder::unorder;
        scope: lane;

        kernel {
            parameters: <I: IndexDomain, V: ValueDomain, O: OrderState>;
            input: (Indexed<I, V>, Multiple<O>);
            output: ExpressionHandle<Indexed<I, V>, Multiple<Unordered>>;
        }

        kernel {
            parameters: <V: BareValueDomain, O: OrderState>;
            input: (Bare<V>, Multiple<O>);
            output: ExpressionHandle<Bare<V>, Multiple<Unordered>>;
        }
    }
}
