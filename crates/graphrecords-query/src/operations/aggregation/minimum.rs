use crate::{
    Bare, BareValueType, EvaluateOperand, Explain, Failure, IndexDomain, Indexed, Labeled,
    Multiple, Operand, OrderState, QueryResult, Single,
    capabilities::ValueOrdering,
    error::comparison::{IncomparableValues, IncomparableValuesAt},
    execution::EvaluationCache,
    operands::OperandHandle,
    operations::{
        Apply, BareStream, KeyedStream, LaneKernel, Operation, OperationContext, Prepare,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Minimum,
};
use graphrecords_core::GraphRecord;
use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
};

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "Min")]
#[plan(optimizer_hints(empty = if_any))]
pub struct MinimumOperation;

impl Prepare for MinimumOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I, V, O> LaneKernel<Indexed<I, V>, Multiple<O>> for MinimumOperation
where
    I: IndexDomain,
    V: ValueOrdering + BareValueType,
    O: OrderState,
    V::Owned: Debug + Display + Send + Sync,
{
    type Output = OperandHandle<Bare<V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: KeyedStream<'a, I, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let minimum = values.try_fold(None, |minimum, (index, value)| {
            let value = value?;

            let Some((minimum_index, minimum_value)) = minimum else {
                return Ok(Some((index, value)));
            };

            match V::ordering(&value, &minimum_value) {
                Some(Ordering::Less) => Ok(Some((index, value))),
                Some(Ordering::Equal | Ordering::Greater) => {
                    Ok(Some((minimum_index, minimum_value)))
                }
                None => Err(Failure::new_at::<I, _>(
                    Self::LABEL,
                    IncomparableValuesAt::new(
                        V::into_owned(value),
                        V::into_owned(minimum_value),
                        I::to_owned(&index),
                        I::to_owned(&minimum_index),
                    ),
                    &index,
                )),
            }
        });

        Ok(match minimum {
            Ok(minimum) => minimum.map(|(_, value)| Ok(value)),
            Err(failure) => Some(Err(failure)),
        })
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<V, O> LaneKernel<Bare<V>, Multiple<O>> for MinimumOperation
where
    V: ValueOrdering + BareValueType,
    O: OrderState,
    V::Owned: Debug + Display + Send + Sync,
{
    type Output = OperandHandle<Bare<V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let minimum = values.try_fold(None, |minimum, value| {
            let value = value?;

            let Some(minimum) = minimum else {
                return Ok(Some(value));
            };

            match V::ordering(&value, &minimum) {
                Some(Ordering::Less) => Ok(Some(value)),
                Some(Ordering::Equal | Ordering::Greater) => Ok(Some(minimum)),
                None => Err(Failure::new(
                    Self::LABEL,
                    IncomparableValues::new(V::into_owned(value), V::into_owned(minimum)),
                )),
            }
        });

        Ok(minimum.transpose())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<O: Apply<MinimumOperation>> Minimum for O {
    type ReturnOperand = O::Output;

    fn min(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), MinimumOperation))
    }
}
