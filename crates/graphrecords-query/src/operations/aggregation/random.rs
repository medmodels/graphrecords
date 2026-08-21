use crate::{
    Bare, BareValueDomain, EvaluateExpression, Explain, IndexDomain, Indexed, Multiple, OrderState,
    QueryResult, Single, ValueDomain,
    expressions::ExpressionHandle,
    operations::{BareStream, Build, KeyedStream, LaneKernel, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Random,
};
use graphrecords_core::GraphRecord;
use rand::seq::IteratorRandom;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Lane)]
#[explain(label = "Random")]
#[plan(optimizer_hints(volatile, empty = if_any))]
pub struct RandomOperation;

impl<I: IndexDomain, V: ValueDomain, O: OrderState> LaneKernel<Indexed<I, V>, Multiple<O>>
    for RandomOperation
{
    type Output = ExpressionHandle<Indexed<I, V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        Ok(values.choose(&mut rand::rng()))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<V: BareValueDomain, O: OrderState> LaneKernel<Bare<V>, Multiple<O>> for RandomOperation {
    type Output = ExpressionHandle<Bare<V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        Ok(values.choose(&mut rand::rng()))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<E: Build<RandomOperation>> Random for E {
    type Output = E::Output;

    fn random(&self) -> Self::Output {
        self.build(RandomOperation)
    }
}

operation_manifest! {
    RandomOperation {
        method: Random::random;
        scope: lane;

        kernel {
            parameters: <
                I: IndexDomain,
                V: ValueDomain,
                O: OrderState,
            >;
            input: (Indexed<I, V>, Multiple<O>);
            output: ExpressionHandle<Indexed<I, V>, Single>;
        }

        kernel {
            parameters: <
                V: BareValueDomain,
                O: OrderState,
            >;
            input: (Bare<V>, Multiple<O>);
            output: ExpressionHandle<Bare<V>, Single>;
        }
    }
}
