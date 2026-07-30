use crate::{
    Bare, EvaluateOperand, Explain, Failure, IndexDomain, Indexed, Labeled, Multiple, Operand,
    OrderState, QueryResult, Single,
    capabilities::ValueMedian,
    error::comparison::{IncomparableValues, IncomparableValuesAt},
    execution::EvaluationCache,
    operands::OperandHandle,
    operations::{
        Apply, BareStream, KeyedStream, LaneKernel, Operation, OperationContext, Prepare,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Median,
};
use graphrecords_core::GraphRecord;
use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
};

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "Median")]
#[plan(optimizer_hints(empty = if_any))]
pub struct MedianOperation;

impl Prepare for MedianOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

fn middle_values<T, F>(mut values: Vec<T>, compare: F) -> Option<(T, Option<T>)>
where
    T: Clone,
    F: Fn(&T, &T) -> Ordering,
{
    let length = values.len();

    if length == 0 {
        return None;
    }

    let middle = length / 2;
    let (lower_values, middle_value, _) =
        values.select_nth_unstable_by(middle, |left, right| compare(left, right));

    if length.is_multiple_of(2) {
        let lower = lower_values
            .iter()
            .max_by(|left, right| compare(left, right))
            .expect("an even-length lane has a lower middle value");

        Some((lower.clone(), Some(middle_value.clone())))
    } else {
        Some((middle_value.clone(), None))
    }
}

impl<I, V, O> LaneKernel<Indexed<I, V>, Multiple<O>> for MedianOperation
where
    I: IndexDomain,
    V: ValueMedian,
    O: OrderState,
    V::Owned: Debug + Display + Send + Sync,
{
    type Output = OperandHandle<Bare<V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let collected = match values
            .map(|(index, value)| {
                let value = value?;
                V::validate_median(Self::LABEL, &value)
                    .map_err(|failure| failure.at::<I>(&index))?;

                Ok((index, value))
            })
            .collect::<QueryResult<Vec<_>>>()
        {
            Ok(collected) => collected,
            Err(failure) => return Ok(Some(Err(failure))),
        };

        if let Some((first_position, second_position)) =
            V::find_incomparable_median_values(collected.iter().map(|(_, value)| value))
        {
            let (first_index, first) = &collected[first_position];
            let (second_index, second) = &collected[second_position];
            let failure = Failure::new_at::<I, _>(
                Self::LABEL,
                IncomparableValuesAt::new(
                    V::into_owned(first.clone()),
                    V::into_owned(second.clone()),
                    I::to_owned(first_index),
                    I::to_owned(second_index),
                ),
                second_index,
            );

            return Ok(Some(Err(failure)));
        }

        let Some(((lower_index, lower), upper)) = middle_values(collected, |left, right| {
            V::ordering(&left.1, &right.1).expect("median values were checked for comparability")
        }) else {
            return Ok(None);
        };
        let (upper_index, upper) = match upper {
            Some((index, value)) => (Some(index), Some(value)),
            None => (None, None),
        };
        let failure_index = upper_index.as_ref().unwrap_or(&lower_index);

        Ok(Some(
            V::median(Self::LABEL, lower, upper).map_err(|failure| failure.at::<I>(failure_index)),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<V, O> LaneKernel<Bare<V>, Multiple<O>> for MedianOperation
where
    V: ValueMedian,
    O: OrderState,
    V::Owned: Debug + Display + Send + Sync,
{
    type Output = OperandHandle<Bare<V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let collected = match values
            .map(|value| {
                let value = value?;
                V::validate_median(Self::LABEL, &value)?;

                Ok(value)
            })
            .collect::<QueryResult<Vec<_>>>()
        {
            Ok(collected) => collected,
            Err(failure) => return Ok(Some(Err(failure))),
        };

        if let Some((first_position, second_position)) =
            V::find_incomparable_median_values(collected.iter())
        {
            let failure = Failure::new(
                Self::LABEL,
                IncomparableValues::new(
                    V::into_owned(collected[first_position].clone()),
                    V::into_owned(collected[second_position].clone()),
                ),
            );

            return Ok(Some(Err(failure)));
        }

        let Some((lower, upper)) = middle_values(collected, |left, right| {
            V::ordering(left, right).expect("median values were checked for comparability")
        }) else {
            return Ok(None);
        };

        Ok(Some(V::median(Self::LABEL, lower, upper)))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<O: Apply<MedianOperation>> Median for O {
    type ReturnOperand = O::Output;

    fn median(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), MedianOperation))
    }
}
