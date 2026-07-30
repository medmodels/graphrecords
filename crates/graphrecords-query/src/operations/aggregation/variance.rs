use crate::{
    Bare, EvaluateOperand, Explain, Failure, IndexDomain, Indexed, Labeled, Multiple, Operand,
    OrderState, QueryResult, Scalar,
    error::aggregation::InvalidVarianceValue,
    execution::EvaluationCache,
    operands::BareValueOperand,
    operations::{
        Apply, BareStream, KeyedStream, LaneKernel, Operation, OperationContext, Prepare,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Variance,
};
use graphrecords_core::{GraphRecord, graphrecord::GraphRecordValue};

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "Var")]
#[plan(optimizer_hints(empty = if_any))]
pub struct VarianceOperation;

impl Prepare for VarianceOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

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

impl<I: IndexDomain, O: OrderState> LaneKernel<Indexed<I, Scalar>, Multiple<O>>
    for VarianceOperation
{
    type Output = BareValueOperand;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: KeyedStream<'a, I, Scalar, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let variance = values
            .try_fold((0, 0.0, 0.0), |state, (index, value)| {
                let value = match value? {
                    GraphRecordValue::Int(value) => value as f64,
                    GraphRecordValue::Float(value) => value,
                    value => {
                        return Err(Failure::new_at::<I, _>(
                            Self::LABEL,
                            InvalidVarianceValue::new(value),
                            &index,
                        ));
                    }
                };

                Ok(update_state(state, value))
            })
            .map(|(count, _, squared_deviation)| {
                (count > 1).then(|| GraphRecordValue::Float(squared_deviation / (count - 1) as f64))
            });

        Ok(variance.transpose())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<O: OrderState> LaneKernel<Bare<Scalar>, Multiple<O>> for VarianceOperation {
    type Output = BareValueOperand;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: BareStream<'a, Scalar, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let variance = values
            .try_fold((0, 0.0, 0.0), |state, value| {
                let value = match value? {
                    GraphRecordValue::Int(value) => value as f64,
                    GraphRecordValue::Float(value) => value,
                    value => {
                        return Err(Failure::new(Self::LABEL, InvalidVarianceValue::new(value)));
                    }
                };

                Ok(update_state(state, value))
            })
            .map(|(count, _, squared_deviation)| {
                (count > 1).then(|| GraphRecordValue::Float(squared_deviation / (count - 1) as f64))
            });

        Ok(variance.transpose())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<O: Apply<VarianceOperation>> Variance for O {
    type ReturnOperand = O::Output;

    fn var(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), VarianceOperation))
    }
}
