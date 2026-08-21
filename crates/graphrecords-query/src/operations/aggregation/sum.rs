use crate::{
    Bare, BareValueDomain, EvaluateExpression, Explain, IndexDomain, Indexed, Labeled, Multiple,
    OrderState, QueryResult, Single,
    capabilities::ValueAdd,
    expressions::ExpressionHandle,
    operations::{BareStream, Build, KeyedStream, LaneKernel, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Sum,
};
use graphrecords_core::GraphRecord;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Lane)]
#[explain(label = "Sum")]
#[plan(optimizer_hints(empty = if_any))]
pub struct SumOperation;

impl<I: IndexDomain, V: ValueAdd + BareValueDomain, O: OrderState>
    LaneKernel<Indexed<I, V>, Multiple<O>> for SumOperation
{
    type Output = ExpressionHandle<Bare<V>, Single>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        mut values: KeyedStream<'a, I, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let sum = values.try_fold(None, |sum, (address, value)| {
            let value = value?;

            match sum {
                Some(sum) => V::add(sum, value, Self::LABEL)
                    .map(Some)
                    .map_err(|failure| failure.at_address::<I>(graphrecord, &address)),
                None => Ok(Some(value)),
            }
        });

        Ok(sum.transpose())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<V: ValueAdd + BareValueDomain, O: OrderState> LaneKernel<Bare<V>, Multiple<O>>
    for SumOperation
{
    type Output = ExpressionHandle<Bare<V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let sum = values.try_fold(None, |sum, value| {
            let value = value?;

            match sum {
                Some(sum) => V::add(sum, value, Self::LABEL).map(Some),
                None => Ok(Some(value)),
            }
        });

        Ok(sum.transpose())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<E: Build<SumOperation>> Sum for E {
    type Output = E::Output;

    fn sum(&self) -> Self::Output {
        self.build(SumOperation)
    }
}

operation_manifest! {
    SumOperation {
        method: Sum::sum;
        scope: lane;

        kernel {
            parameters: <
                I: IndexDomain,
                V: ValueAdd + BareValueDomain,
                O: OrderState,
            >;
            input: (Indexed<I, V>, Multiple<O>);
            output: ExpressionHandle<Bare<V>, Single>;
        }

        kernel {
            parameters: <
                V: ValueAdd + BareValueDomain,
                O: OrderState,
            >;
            input: (Bare<V>, Multiple<O>);
            output: ExpressionHandle<Bare<V>, Single>;
        }
    }
}
