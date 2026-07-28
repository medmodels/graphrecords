use crate::{
    Bare, EvaluateOperand, Explain, Failure, IndexDomain, Indexed, Labeled, Multiple, Operand,
    OrderState, QueryResult, Scalar,
    execution::EvaluationCache,
    operands::BareValueOperand,
    operations::{
        Apply, BareStream, KeyedStream, LaneKernel, Operation, OperationContext, Prepare,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Mean,
};
use graphrecords_core::{GraphRecord, graphrecord::GraphRecordValue};
use std::ops::{Add, Div};

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "Mean")]
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

impl<I: IndexDomain, O: OrderState> LaneKernel<Indexed<I, Scalar>, Multiple<O>> for MeanOperation {
    type Output = BareValueOperand;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: KeyedStream<'a, I, Scalar, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let mean = values
            .try_fold(
                (None, 0_i64),
                |(sum, count): (Option<GraphRecordValue>, _), (index, value)| {
                    let value = value?;
                    let sum = match sum {
                        Some(sum) => sum
                            .add(value)
                            .map_err(|error| Failure::new_at::<I, _>(Self::LABEL, error, &index))?,
                        None => value,
                    };

                    Ok((Some(sum), count + 1))
                },
            )
            .and_then(|(sum, count)| {
                sum.map(|sum| {
                    sum.div(GraphRecordValue::Int(count))
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

impl<O: OrderState> LaneKernel<Bare<Scalar>, Multiple<O>> for MeanOperation {
    type Output = BareValueOperand;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: BareStream<'a, Scalar, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let mean = values
            .try_fold(
                (None, 0_i64),
                |(sum, count): (Option<GraphRecordValue>, _), value| {
                    let value = value?;
                    let sum = match sum {
                        Some(sum) => sum
                            .add(value)
                            .map_err(|error| Failure::new(Self::LABEL, error))?,
                        None => value,
                    };

                    Ok((Some(sum), count + 1))
                },
            )
            .and_then(|(sum, count)| {
                sum.map(|sum| {
                    sum.div(GraphRecordValue::Int(count))
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
