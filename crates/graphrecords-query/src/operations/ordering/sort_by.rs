use super::IndexTiebreak;
use crate::{
    EvaluateExpression, Explain, Failure, Indexed, Labeled, Multiple, OrderState, Ordered,
    QueryResult, ValueDomain,
    capabilities::EnsureSortable,
    element::Retention,
    error::comparison::IncomparableValuesAt,
    expressions::ExpressionHandle,
    operations::{ArgumentSource, Build, Keyed, KeyedStream, LaneKernel, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::SortBy,
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
#[explain(label = "SortBy")]
#[plan(optimizer_hints(empty = if_all))]
pub struct SortByOperation<K> {
    #[argument]
    key: K,
}

type SortedBy<I, V> = ExpressionHandle<Indexed<I, V>, Multiple<Ordered>>;

impl<I, V, K, O> LaneKernel<Indexed<I, V>, Multiple<O>> for SortByOperation<K>
where
    I: IndexTiebreak,
    V: ValueDomain,
    K: ArgumentSource<Keyed<I>>,
    O: OrderState,
    for<'a> <K::ValueDomain as ValueDomain>::Value<'a>: EnsureSortable,
    <K::ValueDomain as ValueDomain>::Owned: Debug + Display + Send + Sync,
{
    type Output = SortedBy<I, V>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let label = Self::LABEL;

        let mut collected: Vec<_> = values
            .filter_map(|(address, subject)| {
                let step = K::resolve(graphrecord, &prepared, &address, label);

                K::Retention::collapse(step).map(|key| key.map(|key| (address, subject, key)))
            })
            .collect::<QueryResult<_>>()?;

        if let Some((first_position, second_position)) =
            EnsureSortable::find_incomparable(collected.iter().map(|(_, _, key)| key))
        {
            let (first_address, _, first) = &collected[first_position];
            let (second_address, _, second) = &collected[second_position];

            return Err(Failure::new(
                IncomparableValuesAt::new(
                    K::ValueDomain::into_owned(first.clone()),
                    K::ValueDomain::into_owned(second.clone()),
                    I::own_index(&I::index(graphrecord, first_address)),
                    I::own_index(&I::index(graphrecord, second_address)),
                ),
                label,
            ));
        }

        collected.sort_by(|(_, _, left), (_, _, right)| {
            left.partial_cmp(right)
                .unwrap_or_else(|| panic!("EnsureSortable admitted an incomparable pair of keys"))
        });

        for run in collected.chunk_by_mut(|(_, _, left), (_, _, right)| {
            left.partial_cmp(right) == Some(Ordering::Equal)
        }) {
            I::tiebreak(graphrecord, run, |element| &element.0);
        }

        Ok(Box::new(
            collected
                .into_iter()
                .map(|(address, subject, _)| (address, subject)),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<E, K> SortBy<K> for E
where
    SortByOperation<K>: Operation,
    E: Build<SortByOperation<K>>,
{
    type Output = E::Output;

    fn sort_by(&self, key: K) -> Self::Output {
        self.build(SortByOperation { key })
    }
}

operation_manifest! {
    SortByOperation<K> {
        method: SortBy<K>::sort_by;
        scope: lane;

        kernel {
            parameters: <I: IndexDomain, V: ValueDomain, O: OrderState>;
            argument: K: ArgumentSource<Keyed<I>> where K::ValueDomain: EnsureSortable;
            input: (Indexed<I, V>, Multiple<O>);
            output: SortedBy<I, V>;
        }
    }
}
