use crate::{
    Bare, BareValueDomain, EvaluateExpression, Explain, Failure, IndexDomain, Indexed, Labeled,
    Multiple, OrderState, QueryResult,
    capabilities::ValueScalar,
    error::aggregation::InvalidVarianceValue,
    expressions::BareValueExpression,
    operations::{BareStream, Build, KeyedStream, LaneKernel, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Variance,
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{Value, ValueView},
};

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Lane)]
#[explain(label = "Variance")]
#[plan(optimizer_hints(empty = if_any))]
pub struct VarianceOperation;

fn update_state(
    (count, mean, squared_deviation): (usize, f64, f64),
    value: f64,
) -> (usize, f64, f64) {
    let count = count + 1;
    let difference = value - mean;
    let mean = mean + difference / count as f64;
    let updated_difference = value - mean;
    let squared_deviation = difference.mul_add(updated_difference, squared_deviation);

    (count, mean, squared_deviation)
}

impl<I: IndexDomain, V: ValueScalar + BareValueDomain, O: OrderState>
    LaneKernel<Indexed<I, V>, Multiple<O>> for VarianceOperation
{
    type Output = BareValueExpression;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        mut values: KeyedStream<'a, I, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let variance = values
            .try_fold((0, 0.0, 0.0), |state, (address, value)| {
                let value = V::into_scalar(value?, Self::LABEL)
                    .map_err(|failure| failure.at_address::<I>(graphrecord, &address))?;
                let value = match value {
                    Value::Int(value) => value as f64,
                    Value::Float(value) => value,
                    value => {
                        return Err(Failure::new_at_address::<I, _>(
                            InvalidVarianceValue::new(value),
                            graphrecord,
                            &address,
                            Self::LABEL,
                        ));
                    }
                };

                Ok(update_state(state, value))
            })
            .map(|(count, _, squared_deviation)| {
                (count > 1).then(|| ValueView::Float(squared_deviation / (count - 1) as f64))
            });

        Ok(variance.transpose())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<V: ValueScalar + BareValueDomain, O: OrderState> LaneKernel<Bare<V>, Multiple<O>>
    for VarianceOperation
{
    type Output = BareValueExpression;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let variance = values
            .try_fold((0, 0.0, 0.0), |state, value| {
                let value = V::into_scalar(value?, Self::LABEL)?;
                let value = match value {
                    Value::Int(value) => value as f64,
                    Value::Float(value) => value,
                    value => {
                        return Err(Failure::new(InvalidVarianceValue::new(value), Self::LABEL));
                    }
                };

                Ok(update_state(state, value))
            })
            .map(|(count, _, squared_deviation)| {
                (count > 1).then(|| ValueView::Float(squared_deviation / (count - 1) as f64))
            });

        Ok(variance.transpose())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<E: Build<VarianceOperation>> Variance for E {
    type Output = E::Output;

    fn var(&self) -> Self::Output {
        self.build(VarianceOperation)
    }
}

operation_manifest! {
    VarianceOperation {
        method: Variance::var;
        scope: lane;

        kernel {
            parameters: <
                I: IndexDomain,
                V: ValueScalar + BareValueDomain,
                O: OrderState,
            >;
            input: (Indexed<I, V>, Multiple<O>);
            output: BareValueExpression;
        }

        kernel {
            parameters: <
                V: ValueScalar + BareValueDomain,
                O: OrderState,
            >;
            input: (Bare<V>, Multiple<O>);
            output: BareValueExpression;
        }
    }
}
