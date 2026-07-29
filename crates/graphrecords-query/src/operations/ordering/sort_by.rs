use crate::{
    EvaluateOperand, Explain, Failure, IncomparableValuesAt, IndexDomain, Indexed, Labeled,
    Multiple, Operand, OrderState, Ordered, QueryResult, ValueType,
    element::Retention,
    execution::EvaluationCache,
    operands::OperandHandle,
    operations::{
        Apply, ArgumentSource, Keyed, KeyedStream, LaneKernel, Operation, OperationContext, Prepare,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::SortBy,
    value::{EnsureSortable, IncomparableIndices},
};
use graphrecords_core::GraphRecord;
use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
};

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "SortBy")]
pub struct SortByOperation<A> {
    #[argument]
    key: A,
}

type SortedBy<I, V> = OperandHandle<Indexed<I, V>, Multiple<Ordered>>;

impl<A: Prepare> Prepare for SortByOperation<A> {
    type Prepared<'a>
        = A::Prepared<'a>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.key.prepare(graphrecord, cache)
    }
}

impl<I, V, A, O> LaneKernel<Indexed<I, V>, Multiple<O>> for SortByOperation<A>
where
    I: IndexDomain,
    V: ValueType,
    A: ArgumentSource<Keyed<I>>,
    O: OrderState,
    for<'a> I::Index<'a>: EnsureSortable,
    for<'a> A::Value<'a>: EnsureSortable,
    A::OwnedValue: Debug + Display + Send + Sync,
{
    type Output = SortedBy<I, V>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let label = Self::LABEL;

        let mut collected: Vec<_> = values
            .filter_map(|(index, subject)| {
                let step = A::resolve(&prepared, &index, label);

                <A::Retention as Retention>::collapse(step)
                    .map(|key| key.map(|key| (index, subject, key)))
            })
            .collect::<QueryResult<_>>()?;

        if let Some((first_position, second_position)) =
            EnsureSortable::find_incomparable(collected.iter().map(|(_, _, key)| key))
        {
            let (first_index, _, first) = &collected[first_position];
            let (second_index, _, second) = &collected[second_position];

            return Err(Failure::new(
                label,
                IncomparableValuesAt {
                    first: A::to_owned_value(first),
                    second: A::to_owned_value(second),
                    first_element: I::to_owned(first_index),
                    second_element: I::to_owned(second_index),
                },
            ));
        }

        collected.sort_by(|(_, _, left), (_, _, right)| {
            left.partial_cmp(right)
                .unwrap_or_else(|| panic!("EnsureSortable admitted an incomparable pair of keys"))
        });

        for run in collected.chunk_by_mut(|(_, _, left), (_, _, right)| {
            left.partial_cmp(right) == Some(Ordering::Equal)
        }) {
            if let Some((first_position, second_position)) =
                EnsureSortable::find_incomparable(run.iter().map(|(index, _, _)| index))
            {
                let (first_index, _, key) = &run[first_position];
                let (second_index, _, _) = &run[second_position];

                return Err(Failure::new(
                    label,
                    IncomparableIndices {
                        value: A::to_owned_value(key),
                        first: I::to_owned(first_index),
                        second: I::to_owned(second_index),
                    },
                ));
            }

            run.sort_by(|(left_index, _, _), (right_index, _, _)| {
                left_index.partial_cmp(right_index).unwrap_or_else(|| {
                    panic!("EnsureSortable admitted an incomparable pair of indices")
                })
            });
        }

        Ok(Box::new(
            collected
                .into_iter()
                .map(|(index, subject, _)| (index, subject)),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<O, A> SortBy<A> for O
where
    SortByOperation<A>: Operation,
    O: Apply<SortByOperation<A>>,
{
    type ReturnOperand = O::Output;

    fn sort_by(&self, key: A) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), SortByOperation { key }))
    }
}
