use crate::{
    Bare, Diagnostic, EvaluateOperand, Explain, Failure, IndexDomain, Indexed, Labeled, Multiple,
    Operand, OrderState, QueryResult, Scalar,
    execution::EvaluationCache,
    operands::BareValueOperand,
    operations::{Apply, BareStream, Kernel, KeyedStream, Operation, OperationContext, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Std,
};
use graphrecords_core::{GraphRecord, graphrecord::GraphRecordValue};
use std::{
    error::Error,
    fmt::{self, Display, Formatter},
};

#[derive(Debug)]
pub struct InvalidStandardDeviationValue {
    pub value: GraphRecordValue,
}

impl Display for InvalidStandardDeviationValue {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "cannot calculate standard deviation of value `{}`",
            self.value
        )
    }
}

impl Error for InvalidStandardDeviationValue {}

impl Diagnostic for InvalidStandardDeviationValue {
    fn name() -> &'static str {
        "InvalidStandardDeviationValue"
    }

    fn help(&self) -> Option<String> {
        Some("narrow the values down first using is_int() or is_float()".to_string())
    }
}

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "Std")]
pub struct StdOperation;

impl Prepare for StdOperation {
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

impl<I: IndexDomain, O: OrderState> Kernel<Indexed<I, Scalar>, Multiple<O>> for StdOperation {
    type Output = BareValueOperand;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: KeyedStream<'a, I, Scalar, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let standard_deviation = values
            .try_fold((0_usize, 0.0, 0.0), |state, (index, value)| {
                let value = match value? {
                    GraphRecordValue::Int(value) => value as f64,
                    GraphRecordValue::Float(value) => value,
                    value => {
                        return Err(Failure::new_at(
                            Self::LABEL,
                            InvalidStandardDeviationValue { value },
                            &index,
                        ));
                    }
                };

                Ok(update_state(state, value))
            })
            .map(|(count, _mean, squared_deviation)| {
                (count != 0)
                    .then(|| GraphRecordValue::Float((squared_deviation / count as f64).sqrt()))
            });

        Ok(standard_deviation.transpose())
    }

    fn estimate(&self, _input: Estimate, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl<O: OrderState> Kernel<Bare<Scalar>, Multiple<O>> for StdOperation {
    type Output = BareValueOperand;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: BareStream<'a, Scalar, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let standard_deviation = values
            .try_fold((0_usize, 0.0, 0.0), |state, value| {
                let value = match value? {
                    GraphRecordValue::Int(value) => value as f64,
                    GraphRecordValue::Float(value) => value,
                    value => {
                        return Err(Failure::new(
                            Self::LABEL,
                            InvalidStandardDeviationValue { value },
                        ));
                    }
                };

                Ok(update_state(state, value))
            })
            .map(|(count, _mean, squared_deviation)| {
                (count != 0)
                    .then(|| GraphRecordValue::Float((squared_deviation / count as f64).sqrt()))
            });

        Ok(standard_deviation.transpose())
    }

    fn estimate(&self, _input: Estimate, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl<O> Std for O
where
    O: Apply<StdOperation>,
{
    type ReturnOperand = <O as Apply<StdOperation>>::Output;

    fn std(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), StdOperation))
    }
}
