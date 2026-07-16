use super::{EnsureSortable, IncomparableIndices};
use crate::{
    Bare, EvaluateOperand, Explain, Failure, IncomparableValues, IncomparableValuesAt, IndexDomain,
    Indexed, Labeled, Multiple, Operand, OrderState, Ordered, QueryResult, ToOwnedValue, ValueType,
    execution::EvaluationCache,
    operands::OperandHandle,
    operations::{Apply, BareStream, Kernel, KeyedStream, Operation, OperationContext, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Sort,
};
use graphrecords_core::GraphRecord;
use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
};

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "Sort")]
pub struct SortOperation;

impl Prepare for SortOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I, V, O> Kernel<Indexed<I, V>, Multiple<O>> for SortOperation
where
    I: IndexDomain,
    V: ValueType,
    O: OrderState,
    for<'a> V::Value<'a>: EnsureSortable,
    for<'a> <V::Value<'a> as ToOwnedValue>::Owned: Debug + Display + Send + Sync,
{
    type Output = OperandHandle<Indexed<I, V>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let mut collected: Vec<_> = values
            .map(|(index, result)| result.map(|value| (index, value)))
            .collect::<QueryResult<_>>()?;

        if let Some((first_position, second_position)) =
            EnsureSortable::find_incomparable(collected.iter().map(|(_index, value)| value))
        {
            let (first_index, first) = &collected[first_position];
            let (second_index, second) = &collected[second_position];

            return Err(Failure::new(
                Self::LABEL,
                IncomparableValuesAt {
                    first: first.to_owned_value(),
                    second: second.to_owned_value(),
                    first_element: first_index.to_owned_value(),
                    second_element: second_index.to_owned_value(),
                },
            ));
        }

        collected.sort_by(|(_, left), (_, right)| {
            left.partial_cmp(right)
                .unwrap_or_else(|| panic!("EnsureSortable admitted an incomparable pair of values"))
        });

        for run in collected
            .chunk_by_mut(|(_, left), (_, right)| left.partial_cmp(right) == Some(Ordering::Equal))
        {
            if let Some((first_position, second_position)) =
                EnsureSortable::find_incomparable(run.iter().map(|(index, _value)| index))
            {
                let (first_index, value) = &run[first_position];
                let (second_index, _) = &run[second_position];

                return Err(Failure::new(
                    Self::LABEL,
                    IncomparableIndices {
                        value: value.to_owned_value(),
                        first: first_index.to_owned_value(),
                        second: second_index.to_owned_value(),
                    },
                ));
            }

            run.sort_by(|(left_index, _), (right_index, _)| {
                left_index.partial_cmp(right_index).unwrap_or_else(|| {
                    panic!("EnsureSortable admitted an incomparable pair of indices")
                })
            });
        }

        Ok(Box::new(
            collected
                .into_iter()
                .map(|(index, value)| (index, Ok(value))),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<V, O> Kernel<Bare<V>, Multiple<O>> for SortOperation
where
    V: ValueType,
    O: OrderState,
    for<'a> V::Value<'a>: EnsureSortable,
    for<'a> <V::Value<'a> as ToOwnedValue>::Owned: Debug + Display + Send + Sync,
{
    type Output = OperandHandle<Bare<V>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let mut collected: Vec<_> = values.collect::<QueryResult<_>>()?;

        if let Some((first_position, second_position)) =
            EnsureSortable::find_incomparable(collected.iter())
        {
            return Err(Failure::new(
                Self::LABEL,
                IncomparableValues {
                    first: collected[first_position].to_owned_value(),
                    second: collected[second_position].to_owned_value(),
                },
            ));
        }

        collected.sort_by(|left, right| {
            left.partial_cmp(right)
                .unwrap_or_else(|| panic!("EnsureSortable admitted an incomparable pair of values"))
        });

        Ok(Box::new(collected.into_iter().map(Ok)))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<S> Sort for S
where
    S: Apply<SortOperation>,
{
    type ReturnOperand = <S as Apply<SortOperation>>::Output;

    fn sort(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), SortOperation))
    }
}
