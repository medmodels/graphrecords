use crate::{
    Bare, BareValueDomain, EvaluateExpression, Explain, Failure, IndexDomain, Indexed, Labeled,
    Multiple, OrderState, QueryResult, Single,
    capabilities::ValueScalar,
    expressions::ExpressionHandle,
    operations::{BareStream, Build, KeyedStream, LaneKernel, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Mean,
};
use graphrecords_core::{GraphRecord, graphrecord::Value};
use std::ops::{Add, Div};

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Lane)]
#[explain(label = "Mean")]
#[plan(optimizer_hints(empty = if_any))]
pub struct MeanOperation;

impl<I: IndexDomain, V: ValueScalar + BareValueDomain, O: OrderState>
    LaneKernel<Indexed<I, V>, Multiple<O>> for MeanOperation
{
    type Output = ExpressionHandle<Bare<V>, Single>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        mut values: KeyedStream<'a, I, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let mean = values
            .try_fold(
                (None, 0_i64, None),
                |(sum, count, original): (Option<Value>, _, _), (address, value)| {
                    let value = value?;
                    let original = original.or_else(|| Some(value.clone()));
                    let value = V::into_scalar(value, Self::LABEL)
                        .map_err(|failure| failure.at_address::<I>(graphrecord, &address))?;
                    let sum = match sum {
                        Some(sum) => sum.add(value).map_err(|error| {
                            Failure::new_at_address::<I, _>(
                                error,
                                graphrecord,
                                &address,
                                Self::LABEL,
                            )
                        })?,
                        None => value,
                    };

                    Ok((Some(sum), count + 1, original))
                },
            )
            .and_then(|(sum, count, original)| {
                sum.zip(original)
                    .map(|(sum, original)| {
                        sum.div(Value::Int(count))
                            .map(|value| V::from_scalar(&original, value))
                            .map_err(|error| Failure::new(error, Self::LABEL))
                    })
                    .transpose()
            });

        Ok(mean.transpose())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<V: ValueScalar + BareValueDomain, O: OrderState> LaneKernel<Bare<V>, Multiple<O>>
    for MeanOperation
{
    type Output = ExpressionHandle<Bare<V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let mean = values
            .try_fold(
                (None, 0_i64, None),
                |(sum, count, original): (Option<Value>, _, _), value| {
                    let value = value?;
                    let original = original.or_else(|| Some(value.clone()));
                    let value = V::into_scalar(value, Self::LABEL)?;
                    let sum = match sum {
                        Some(sum) => sum
                            .add(value)
                            .map_err(|error| Failure::new(error, Self::LABEL))?,
                        None => value,
                    };

                    Ok((Some(sum), count + 1, original))
                },
            )
            .and_then(|(sum, count, original)| {
                sum.zip(original)
                    .map(|(sum, original)| {
                        sum.div(Value::Int(count))
                            .map(|value| V::from_scalar(&original, value))
                            .map_err(|error| Failure::new(error, Self::LABEL))
                    })
                    .transpose()
            });

        Ok(mean.transpose())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<E: Build<MeanOperation>> Mean for E {
    type Output = E::Output;

    fn mean(&self) -> Self::Output {
        self.build(MeanOperation)
    }
}

operation_manifest! {
    MeanOperation {
        method: Mean::mean;
        scope: lane;

        kernel {
            parameters: <
                I: IndexDomain,
                V: ValueScalar + BareValueDomain,
                O: OrderState,
            >;
            input: (Indexed<I, V>, Multiple<O>);
            output: ExpressionHandle<Bare<V>, Single>;
        }

        kernel {
            parameters: <
                V: ValueScalar + BareValueDomain,
                O: OrderState,
            >;
            input: (Bare<V>, Multiple<O>);
            output: ExpressionHandle<Bare<V>, Single>;
        }
    }
}
