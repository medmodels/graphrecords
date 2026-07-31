use crate::{
    Bare, BareValueDomain, EvaluateOperand, Explain, Failure, IndexDomain, Indexed, Labeled,
    Multiple, Operand, OrderState, QueryResult, Single,
    capabilities::ValueScalar,
    execution::EvaluationCache,
    operands::OperandHandle,
    operations::{
        Apply, BareStream, KeyedStream, LaneKernel, Operation, OperationContext, Prepare,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Mean,
};
use graphrecords_core::{GraphRecord, graphrecord::GraphRecordValue};
use std::ops::{Add, Div};

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "Mean")]
#[plan(optimizer_hints(empty = if_any))]
pub struct MeanOperation;

impl Prepare for MeanOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I, V, O> LaneKernel<Indexed<I, V>, Multiple<O>> for MeanOperation
where
    I: IndexDomain,
    V: ValueScalar + BareValueDomain,
    O: OrderState,
{
    type Output = OperandHandle<Bare<V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: KeyedStream<'a, I, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let mean = values
            .try_fold(
                (None, 0_i64, None),
                |(sum, count, role): (Option<GraphRecordValue>, _, _), (index, value)| {
                    let value = value?;
                    let role = role.or_else(|| Some(value.clone()));
                    let value = V::into_scalar(Self::LABEL, value)
                        .map_err(|failure| failure.at::<I>(&index))?;
                    let sum = match sum {
                        Some(sum) => sum
                            .add(value)
                            .map_err(|error| Failure::new_at::<I, _>(Self::LABEL, error, &index))?,
                        None => value,
                    };

                    Ok((Some(sum), count + 1, role))
                },
            )
            .and_then(|(sum, count, role)| {
                sum.zip(role)
                    .map(|(sum, role)| {
                        sum.div(GraphRecordValue::Int(count))
                            .map(|value| V::from_scalar(&role, value))
                            .map_err(|error| Failure::new(Self::LABEL, error))
                    })
                    .transpose()
            });

        Ok(mean.transpose())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<V, O> LaneKernel<Bare<V>, Multiple<O>> for MeanOperation
where
    V: ValueScalar + BareValueDomain,
    O: OrderState,
{
    type Output = OperandHandle<Bare<V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let mean = values
            .try_fold(
                (None, 0_i64, None),
                |(sum, count, role): (Option<GraphRecordValue>, _, _), value| {
                    let value = value?;
                    let role = role.or_else(|| Some(value.clone()));
                    let value = V::into_scalar(Self::LABEL, value)?;
                    let sum = match sum {
                        Some(sum) => sum
                            .add(value)
                            .map_err(|error| Failure::new(Self::LABEL, error))?,
                        None => value,
                    };

                    Ok((Some(sum), count + 1, role))
                },
            )
            .and_then(|(sum, count, role)| {
                sum.zip(role)
                    .map(|(sum, role)| {
                        sum.div(GraphRecordValue::Int(count))
                            .map(|value| V::from_scalar(&role, value))
                            .map_err(|error| Failure::new(Self::LABEL, error))
                    })
                    .transpose()
            });

        Ok(mean.transpose())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<O: Apply<MeanOperation>> Mean for O {
    type ReturnOperand = O::Output;

    fn mean(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), MeanOperation))
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
            output: OperandHandle<Bare<V>, Single>;
        }

        kernel {
            parameters: <
                V: ValueScalar + BareValueDomain,
                O: OrderState,
            >;
            input: (Bare<V>, Multiple<O>);
            output: OperandHandle<Bare<V>, Single>;
        }
    }
}
