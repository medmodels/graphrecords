use crate::{
    Bare, EvaluateOperand, Explain, Failure, IncomparableValues, IncomparableValuesAt, IndexDomain,
    Indexed, Labeled, Multiple, Operand, OrderState, Ordered, QueryResult, ValueType,
    execution::EvaluationCache,
    operands::OperandHandle,
    operations::{
        Apply, BareStream, KeyedStream, LaneKernel, Operation, OperationContext, Prepare,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Sort,
    value::{EnsureSortable, IncomparableIndices, ValueOrdering},
};
use graphrecords_core::GraphRecord;
use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
};

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
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

fn sort_indexed<'a, I, V, O>(
    values: KeyedStream<'a, I, V, Multiple<O>>,
) -> QueryResult<KeyedStream<'a, I, V, Multiple<Ordered>>>
where
    I: IndexDomain,
    V: ValueType,
    O: OrderState,
    for<'b> I::Index<'b>: EnsureSortable,
    for<'b> V::Value<'b>: EnsureSortable,
    V::Owned: Debug + Display + Send + Sync,
{
    let mut collected: Vec<_> = values
        .map(|(index, result)| result.map(|value| (index, value)))
        .collect::<QueryResult<_>>()?;

    if let Some((first_position, second_position)) =
        EnsureSortable::find_incomparable(collected.iter().map(|(_, value)| value))
    {
        let (first_index, first) = &collected[first_position];
        let (second_index, second) = &collected[second_position];

        return Err(Failure::new(
            SortOperation::LABEL,
            IncomparableValuesAt {
                first: V::into_owned(first.clone()),
                second: V::into_owned(second.clone()),
                first_element: I::to_owned(first_index),
                second_element: I::to_owned(second_index),
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
            EnsureSortable::find_incomparable(run.iter().map(|(index, _)| index))
        {
            let (first_index, value) = &run[first_position];
            let (second_index, _) = &run[second_position];

            return Err(Failure::new(
                SortOperation::LABEL,
                IncomparableIndices {
                    value: V::into_owned(value.clone()),
                    first: I::to_owned(first_index),
                    second: I::to_owned(second_index),
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

fn sort_bare<'a, V, O>(
    values: BareStream<'a, V, Multiple<O>>,
) -> QueryResult<<OperandHandle<Bare<V>, Multiple<Ordered>> as EvaluateOperand>::ReturnValue<'a>>
where
    V: ValueType,
    O: OrderState,
    for<'b> V::Value<'b>: EnsureSortable,
    V::Owned: Debug + Display + Send + Sync,
{
    let mut collected: Vec<_> = values.collect::<QueryResult<_>>()?;

    if let Some((first_position, second_position)) =
        EnsureSortable::find_incomparable(collected.iter())
    {
        return Err(Failure::new(
            SortOperation::LABEL,
            IncomparableValues {
                first: V::into_owned(collected[first_position].clone()),
                second: V::into_owned(collected[second_position].clone()),
            },
        ));
    }

    collected.sort_by(|left, right| {
        left.partial_cmp(right)
            .unwrap_or_else(|| panic!("EnsureSortable admitted an incomparable pair of values"))
    });

    Ok(Box::new(collected.into_iter().map(Ok)))
}

impl<I, V, O> LaneKernel<Indexed<I, V>, Multiple<O>> for SortOperation
where
    I: IndexDomain,
    V: ValueOrdering,
    O: OrderState,
    for<'a> I::Index<'a>: EnsureSortable,
    for<'a> V::Value<'a>: EnsureSortable,
    V::Owned: Debug + Display + Send + Sync,
{
    type Output = OperandHandle<Indexed<I, V>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        sort_indexed::<I, V, O>(values)
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<V, O> LaneKernel<Bare<V>, Multiple<O>> for SortOperation
where
    V: ValueOrdering,
    O: OrderState,
    for<'a> V::Value<'a>: EnsureSortable,
    V::Owned: Debug + Display + Send + Sync,
{
    type Output = OperandHandle<Bare<V>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        sort_bare::<V, O>(values)
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<O: Apply<SortOperation>> Sort for O {
    type ReturnOperand = O::Output;

    fn sort(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), SortOperation))
    }
}
