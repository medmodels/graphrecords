use super::{EnsureSortable, IncomparableIndices};
use crate::{
    AttributeName, Bare, EvaluateOperand, Explain, Failure, FailureKindValue, IncomparableValues,
    IncomparableValuesAt, IndexDomain, IndexValue, Indexed, Labeled, Mask, Multiple, Operand,
    OrderState, Ordered, QueryResult, Scalar, ToOwnedValue, ValueType,
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

fn sort_indexed<'a, I, V, O>(
    values: KeyedStream<'a, I, V, Multiple<O>>,
) -> QueryResult<KeyedStream<'a, I, V, Multiple<Ordered>>>
where
    I: IndexDomain,
    V: ValueType,
    O: OrderState,
    for<'b> I::Index<'b>: EnsureSortable,
    for<'b> V::Value<'b>: EnsureSortable,
    for<'b> <V::Value<'b> as ToOwnedValue>::Owned: Debug + Display + Send + Sync,
{
    let mut collected: Vec<_> = values
        .map(|(index, result)| result.map(|value| (index, value)))
        .collect::<QueryResult<_>>()?;

    if let Some((first_position, second_position)) =
        EnsureSortable::find_incomparable(collected.iter().map(|(_index, value)| value))
    {
        let (first_index, first) = &collected[first_position];
        let (second_index, second) = &collected[second_position];

        return Err(Failure::new(
            SortOperation::LABEL,
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
                SortOperation::LABEL,
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

fn sort_bare<'a, V, O>(
    values: BareStream<'a, V, Multiple<O>>,
) -> QueryResult<<OperandHandle<Bare<V>, Multiple<Ordered>> as EvaluateOperand>::ReturnValue<'a>>
where
    V: ValueType,
    O: OrderState,
    for<'b> V::Value<'b>: EnsureSortable,
    for<'b> <V::Value<'b> as ToOwnedValue>::Owned: Debug + Display + Send + Sync,
{
    let mut collected: Vec<_> = values.collect::<QueryResult<_>>()?;

    if let Some((first_position, second_position)) =
        EnsureSortable::find_incomparable(collected.iter())
    {
        return Err(Failure::new(
            SortOperation::LABEL,
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

impl<I, O> Kernel<Indexed<I, Scalar>, Multiple<O>> for SortOperation
where
    I: IndexDomain,
    O: OrderState,
    for<'a> I::Index<'a>: EnsureSortable,
{
    type Output = OperandHandle<Indexed<I, Scalar>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, Scalar, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        sort_indexed::<I, Scalar, O>(values)
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<I, O> Kernel<Indexed<I, Mask>, Multiple<O>> for SortOperation
where
    I: IndexDomain,
    O: OrderState,
    for<'a> I::Index<'a>: EnsureSortable,
{
    type Output = OperandHandle<Indexed<I, Mask>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, Mask, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        sort_indexed::<I, Mask, O>(values)
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<I, O> Kernel<Indexed<I, AttributeName>, Multiple<O>> for SortOperation
where
    I: IndexDomain,
    O: OrderState,
    for<'a> I::Index<'a>: EnsureSortable,
{
    type Output = OperandHandle<Indexed<I, AttributeName>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, AttributeName, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        sort_indexed::<I, AttributeName, O>(values)
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<I, O> Kernel<Indexed<I, FailureKindValue>, Multiple<O>> for SortOperation
where
    I: IndexDomain,
    O: OrderState,
    for<'a> I::Index<'a>: EnsureSortable,
{
    type Output = OperandHandle<Indexed<I, FailureKindValue>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, FailureKindValue, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        sort_indexed::<I, FailureKindValue, O>(values)
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<K, I, O> Kernel<Indexed<K, IndexValue<I>>, Multiple<O>> for SortOperation
where
    K: IndexDomain,
    I: IndexDomain,
    O: OrderState,
    for<'a> K::Index<'a>: EnsureSortable,
    I::Owned: EnsureSortable,
{
    type Output = OperandHandle<Indexed<K, IndexValue<I>>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, K, IndexValue<I>, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        sort_indexed::<K, IndexValue<I>, O>(values)
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<O: OrderState> Kernel<Bare<Scalar>, Multiple<O>> for SortOperation {
    type Output = OperandHandle<Bare<Scalar>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, Scalar, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        sort_bare::<Scalar, O>(values)
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<O: OrderState> Kernel<Bare<Mask>, Multiple<O>> for SortOperation {
    type Output = OperandHandle<Bare<Mask>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, Mask, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        sort_bare::<Mask, O>(values)
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<O: OrderState> Kernel<Bare<AttributeName>, Multiple<O>> for SortOperation {
    type Output = OperandHandle<Bare<AttributeName>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, AttributeName, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        sort_bare::<AttributeName, O>(values)
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<O: OrderState> Kernel<Bare<FailureKindValue>, Multiple<O>> for SortOperation {
    type Output = OperandHandle<Bare<FailureKindValue>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, FailureKindValue, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        sort_bare::<FailureKindValue, O>(values)
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<I, O> Kernel<Bare<IndexValue<I>>, Multiple<O>> for SortOperation
where
    I: IndexDomain,
    O: OrderState,
    I::Owned: EnsureSortable,
{
    type Output = OperandHandle<Bare<IndexValue<I>>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, IndexValue<I>, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        sort_bare::<IndexValue<I>, O>(values)
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<O> Sort for O
where
    O: Apply<SortOperation>,
{
    type ReturnOperand = <O as Apply<SortOperation>>::Output;

    fn sort(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), SortOperation))
    }
}
