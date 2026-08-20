use crate::{
    Bare, BareValueDomain, EvaluateExpression, Explain, Failure, IndexDomain, Indexed, Labeled,
    Multiple, OrderState, QueryResult, Single,
    capabilities::ValueMedian,
    error::comparison::{IncomparableValues, IncomparableValuesAt},
    expressions::ExpressionHandle,
    operations::{BareStream, Build, KeyedStream, LaneKernel, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Median,
};
use graphrecords_core::GraphRecord;
use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
};

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Lane)]
#[explain(label = "Median")]
#[plan(optimizer_hints(empty = if_any))]
pub struct MedianOperation;

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
    V: ValueMedian + BareValueDomain,
    O: OrderState,
    V::Owned: Debug + Display + Send + Sync,
{
    type Output = ExpressionHandle<Bare<V>, Single>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let collected = match values
            .map(|(address, value)| {
                let value = value?;
                V::validate_median(&value, Self::LABEL)
                    .map_err(|failure| failure.at_address::<I>(graphrecord, &address))?;

                Ok((address, value))
            })
            .collect::<QueryResult<Vec<_>>>()
        {
            Ok(collected) => collected,
            Err(failure) => return Ok(Some(Err(failure))),
        };

        if let Some((first_position, second_position)) =
            V::find_incomparable_median_values(collected.iter().map(|(_, value)| value))
        {
            let (first_address, first) = &collected[first_position];
            let (second_address, second) = &collected[second_position];
            let failure = Failure::new_at_address::<I, _>(
                IncomparableValuesAt::new(
                    V::into_owned(first.clone()),
                    V::into_owned(second.clone()),
                    I::own_index(&I::index(graphrecord, first_address)),
                    I::own_index(&I::index(graphrecord, second_address)),
                ),
                graphrecord,
                second_address,
                Self::LABEL,
            );

            return Ok(Some(Err(failure)));
        }

        let Some(((lower_address, lower), upper)) = middle_values(collected, |left, right| {
            V::ordering(&left.1, &right.1).expect("median values were checked for comparability")
        }) else {
            return Ok(None);
        };
        let (upper_address, upper) = match upper {
            Some((address, value)) => (Some(address), Some(value)),
            None => (None, None),
        };
        let failure_address = upper_address.as_ref().unwrap_or(&lower_address);

        Ok(Some(V::median(lower, upper, Self::LABEL).map_err(
            |failure| failure.at_address::<I>(graphrecord, failure_address),
        )))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<V, O> LaneKernel<Bare<V>, Multiple<O>> for MedianOperation
where
    V: ValueMedian + BareValueDomain,
    O: OrderState,
    V::Owned: Debug + Display + Send + Sync,
{
    type Output = ExpressionHandle<Bare<V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let collected = match values
            .map(|value| {
                let value = value?;
                V::validate_median(&value, Self::LABEL)?;

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
                IncomparableValues::new(
                    V::into_owned(collected[first_position].clone()),
                    V::into_owned(collected[second_position].clone()),
                ),
                Self::LABEL,
            );

            return Ok(Some(Err(failure)));
        }

        let Some((lower, upper)) = middle_values(collected, |left, right| {
            V::ordering(left, right).expect("median values were checked for comparability")
        }) else {
            return Ok(None);
        };

        Ok(Some(V::median(lower, upper, Self::LABEL)))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<E: Build<MedianOperation>> Median for E {
    type Output = E::Output;

    fn median(&self) -> Self::Output {
        self.build(MedianOperation)
    }
}

operation_manifest! {
    MedianOperation {
        method: Median::median;
        scope: lane;

        kernel {
            parameters: <
                I: IndexDomain,
                V: ValueMedian + BareValueDomain,
                O: OrderState,
            >;
            input: (Indexed<I, V>, Multiple<O>);
            output: ExpressionHandle<Bare<V>, Single>;
            where V::Owned: Debug + Display + Send + Sync;
        }

        kernel {
            parameters: <
                V: ValueMedian + BareValueDomain,
                O: OrderState,
            >;
            input: (Bare<V>, Multiple<O>);
            output: ExpressionHandle<Bare<V>, Single>;
            where V::Owned: Debug + Display + Send + Sync;
        }
    }
}
