use crate::{
    EvaluateOperand, Explain, Failure, IncomparableValues, IndexDomain, Indexed, Labeled, Multiple,
    Operand, OrderState, QueryResult, Scalar,
    execution::EvaluationCache,
    operands::ValueOperand,
    operations::{Apply, Kernel, KeyedStream, Operation, OperationContext, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Max,
};
use graphrecords_core::GraphRecord;
use std::cmp::Ordering;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "Max")]
pub struct MaxOperation;

impl Prepare for MaxOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I: IndexDomain, O: OrderState> Kernel<Indexed<I, Scalar>, Multiple<O>> for MaxOperation {
    type Output = ValueOperand<I>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: KeyedStream<'a, I, Scalar, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let best = values.try_fold(None, |best, (index, result)| {
            let value = result?;

            let Some((_, current)) = &best else {
                return Ok(Some((index, value)));
            };

            match value.partial_cmp(current) {
                Some(Ordering::Greater) => Ok(Some((index, value))),
                Some(_) => Ok(best),
                None => Err(Failure::new(
                    Self::LABEL,
                    IncomparableValues {
                        first: value,
                        second: current.clone(),
                    },
                )
                .at(index)
                .help(
                    "narrow the values down first using is_string(), is_int(), is_float(), is_bool(), is_datetime() or is_duration()",
                )),
            }
        })?;

        Ok(best.map(|(index, value)| (index, Ok(value))))
    }

    fn estimate(&self, _input: Estimate, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl<S> Max for S
where
    S: Apply<MaxOperation>,
{
    type ReturnOperand = <S as Apply<MaxOperation>>::Output;

    fn max(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), MaxOperation))
    }
}
