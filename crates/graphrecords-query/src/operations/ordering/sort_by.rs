use super::{EnsureSortable, IncomparableIndices};
use crate::{
    EvaluateOperand, Explain, Failure, IncomparableValuesAt, IndexDomain, Indexed, Labeled,
    Multiple, Operand, OrderState, Ordered, QueryResult, ToOwnedValue, ValueType,
    execution::EvaluationCache,
    operands::OperandHandle,
    operations::{
        Apply, ArgumentSource, Kernel, Keyed, KeyedStream, Operation, OperationContext, Prepare,
        Retention,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::SortBy,
};
use graphrecords_core::GraphRecord;
use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
};

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
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

impl<I, V, A, O> Kernel<Indexed<I, V>, Multiple<O>> for SortByOperation<A>
where
    I: IndexDomain,
    V: ValueType,
    A: ArgumentSource<Keyed<I>>,
    O: OrderState,
    for<'a> I::Index<'a>: EnsureSortable,
    for<'a> A::Value<'a>: EnsureSortable,
    for<'a> <A::Value<'a> as ToOwnedValue>::Owned: Debug + Display + Send + Sync,
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
            EnsureSortable::find_incomparable(collected.iter().map(|(_index, _subject, key)| key))
        {
            let (first_index, _, first) = &collected[first_position];
            let (second_index, _, second) = &collected[second_position];

            return Err(Failure::new(
                label,
                IncomparableValuesAt {
                    first: first.to_owned_value(),
                    second: second.to_owned_value(),
                    first_element: first_index.to_owned_value(),
                    second_element: second_index.to_owned_value(),
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
                EnsureSortable::find_incomparable(run.iter().map(|(index, _subject, _key)| index))
            {
                let (first_index, _, key) = &run[first_position];
                let (second_index, _, _) = &run[second_position];

                return Err(Failure::new(
                    label,
                    IncomparableIndices {
                        value: key.to_owned_value(),
                        first: first_index.to_owned_value(),
                        second: second_index.to_owned_value(),
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
                .map(|(index, subject, _key)| (index, subject)),
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
    type ReturnOperand = <O as Apply<SortByOperation<A>>>::Output;

    fn sort_by(&self, key: A) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), SortByOperation { key }))
    }
}
