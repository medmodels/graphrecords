use crate::{
    Bare, EvaluateOperand, Explain, Failure, IndexDomain, Indexed, Labeled, Multiple, Operand,
    OrderState, QueryResult, Scalar,
    execution::EvaluationCache,
    operands::BareValueOperand,
    operations::{Apply, BareStream, Kernel, KeyedStream, Operation, OperationContext, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Mean,
};
use graphrecords_core::{GraphRecord, graphrecord::GraphRecordValue};
use std::ops::{Add, Div};

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
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

impl<I: IndexDomain, O: OrderState> Kernel<Indexed<I, Scalar>, Multiple<O>> for MeanOperation {
    type Output = BareValueOperand;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: KeyedStream<'a, I, Scalar, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let mean = values
            .try_fold(
                (None, 0_i64),
                |(sum, count): (Option<GraphRecordValue>, i64), (index, value)| {
                    let value = value?;
                    let sum = match sum {
                        Some(sum) => sum
                            .add(value)
                            .map_err(|error| Failure::new_at(Self::LABEL, error, &index))?,
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

    fn estimate(&self, _input: Estimate, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl<O: OrderState> Kernel<Bare<Scalar>, Multiple<O>> for MeanOperation {
    type Output = BareValueOperand;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: BareStream<'a, Scalar, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let mean = values
            .try_fold(
                (None, 0_i64),
                |(sum, count): (Option<GraphRecordValue>, i64), value| {
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

    fn estimate(&self, _input: Estimate, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl<O> Mean for O
where
    O: Apply<MeanOperation>,
{
    type ReturnOperand = <O as Apply<MeanOperation>>::Output;

    fn mean(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), MeanOperation))
    }
}
