use super::IndexTiebreak;
use crate::{
    Bare, BareValueDomain, EvaluateExpression, Explain, Failure, Indexed, Labeled, Multiple,
    OrderState, Ordered, QueryResult, ValueDomain,
    capabilities::{EnsureSortable, ValueOrdering},
    error::comparison::{IncomparableValues, IncomparableValuesAt},
    expressions::ExpressionHandle,
    operations::{BareStream, Build, KeyedStream, LaneKernel, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Sort,
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
#[explain(label = "Sort")]
#[plan(optimizer_hints(empty = if_any))]
pub struct SortOperation;

fn sort_indexed<'a, I, V, O>(
    graphrecord: &'a GraphRecord,
    values: KeyedStream<'a, I, V, Multiple<O>>,
) -> QueryResult<KeyedStream<'a, I, V, Multiple<Ordered>>>
where
    I: IndexTiebreak,
    V: ValueDomain,
    O: OrderState,
    for<'b> V::Value<'b>: EnsureSortable,
    V::Owned: Debug + Display + Send + Sync,
{
    let mut collected: Vec<_> = values
        .map(|(address, result)| result.map(|value| (address, value)))
        .collect::<QueryResult<_>>()?;

    if let Some((first_position, second_position)) =
        EnsureSortable::find_incomparable(collected.iter().map(|(_, value)| value))
    {
        let (first_address, first) = &collected[first_position];
        let (second_address, second) = &collected[second_position];

        return Err(Failure::new(
            IncomparableValuesAt::new(
                V::into_owned(first.clone()),
                V::into_owned(second.clone()),
                I::own_index(&I::index(graphrecord, first_address)),
                I::own_index(&I::index(graphrecord, second_address)),
            ),
            SortOperation::LABEL,
        ));
    }

    collected.sort_by(|(_, left), (_, right)| {
        left.partial_cmp(right)
            .unwrap_or_else(|| panic!("EnsureSortable admitted an incomparable pair of values"))
    });

    for run in collected
        .chunk_by_mut(|(_, left), (_, right)| left.partial_cmp(right) == Some(Ordering::Equal))
    {
        I::tiebreak(graphrecord, run, |element| &element.0);
    }

    Ok(Box::new(
        collected
            .into_iter()
            .map(|(address, value)| (address, Ok(value))),
    ))
}

fn sort_bare<'a, V, O>(
    values: BareStream<'a, V, Multiple<O>>,
) -> QueryResult<
    <ExpressionHandle<Bare<V>, Multiple<Ordered>> as EvaluateExpression>::ReturnValue<'a>,
>
where
    V: BareValueDomain,
    O: OrderState,
    for<'b> V::Value<'b>: EnsureSortable,
    V::Owned: Debug + Display + Send + Sync,
{
    let mut collected: Vec<_> = values.collect::<QueryResult<_>>()?;

    if let Some((first_position, second_position)) =
        EnsureSortable::find_incomparable(collected.iter())
    {
        return Err(Failure::new(
            IncomparableValues::new(
                V::into_owned(collected[first_position].clone()),
                V::into_owned(collected[second_position].clone()),
            ),
            SortOperation::LABEL,
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
    I: IndexTiebreak,
    V: ValueOrdering,
    O: OrderState,
    for<'a> V::Value<'a>: EnsureSortable,
    V::Owned: Debug + Display + Send + Sync,
{
    type Output = ExpressionHandle<Indexed<I, V>, Multiple<Ordered>>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        sort_indexed::<I, V, O>(graphrecord, values)
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<V, O> LaneKernel<Bare<V>, Multiple<O>> for SortOperation
where
    V: ValueOrdering + BareValueDomain,
    O: OrderState,
    for<'a> V::Value<'a>: EnsureSortable,
    V::Owned: Debug + Display + Send + Sync,
{
    type Output = ExpressionHandle<Bare<V>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        sort_bare::<V, O>(values)
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<E: Build<SortOperation>> Sort for E {
    type Output = E::Output;

    fn sort(&self) -> Self::Output {
        self.build(SortOperation)
    }
}

operation_manifest! {
    SortOperation {
        method: Sort::sort;
        scope: lane;

        kernel {
            parameters: <I: IndexDomain, V: ValueOrdering + EnsureSortable, O: OrderState>;
            input: (Indexed<I, V>, Multiple<O>);
            output: ExpressionHandle<Indexed<I, V>, Multiple<Ordered>>;
            where V::Owned: Debug + Display + Send + Sync;
        }

        kernel {
            parameters: <V: ValueOrdering + EnsureSortable + BareValueDomain, O: OrderState>;
            input: (Bare<V>, Multiple<O>);
            output: ExpressionHandle<Bare<V>, Multiple<Ordered>>;
            where V::Owned: Debug + Display + Send + Sync;
        }
    }
}
