use super::IncomparableIndices;
use crate::{
    EvaluateOperand, Explain, Failure, IncomparableValues, IndexDomain, Indexed, Labeled, Multiple,
    Operand, OrderState, QueryResult, Sorted, ValueType,
    execution::EvaluationCache,
    operands::OperandHandle,
    operations::{
        Apply, ArgumentSource, Kernel, Keyed, KeyedStream, OnMissing, Operation, OperationContext,
        Prepare,
    },
    optimizer::{EstimateCost, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
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

type SortedBy<I, V> = OperandHandle<Indexed<I, V>, Multiple<Sorted>>;

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

impl<I, V, A, O> EstimateCost<SortByOperation<A>> for OperandHandle<Indexed<I, V>, Multiple<O>>
where
    I: IndexDomain,
    V: ValueType,
    A: ArgumentSource<Keyed<I>>,
    O: OrderState,
{
    type OutputCost = <Self as Operand>::Cost;

    fn estimate(
        _operation: &SortByOperation<A>,
        input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        input_cost
    }
}

impl<I, V, A, O> Kernel<Indexed<I, V>, Multiple<O>> for SortByOperation<A>
where
    I: IndexDomain,
    V: ValueType,
    A: ArgumentSource<Keyed<I>>,
    O: OrderState,
    for<'a> A::Value<'a>: PartialOrd + Display + Debug + Send + Sync,
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
                match A::resolve(&prepared, &index, label, OnMissing::Raise) {
                    Ok(Some(key)) => Some(Ok((index, subject, key))),
                    Ok(None) => None,
                    Err(failure) => Some(Err(failure)),
                }
            })
            .collect::<QueryResult<_>>()?;

        let mut incomparable_keys = None;
        let mut incomparable_indices = None;

        collected.sort_by(|(left_index, _, left_key), (right_index, _, right_key)| {
            let Some(ordering) = left_key.partial_cmp(right_key) else {
                if incomparable_keys.is_none() {
                    incomparable_keys = Some((left_key.clone(), right_key.clone()));
                }

                return Ordering::Equal;
            };

            if ordering != Ordering::Equal {
                return ordering;
            }

            let Some(index_ordering) = left_index.partial_cmp(right_index) else {
                if incomparable_indices.is_none() {
                    incomparable_indices = Some((
                        left_key.clone(),
                        left_index.to_string(),
                        right_index.to_string(),
                    ));
                }

                return Ordering::Equal;
            };

            index_ordering
        });

        if let Some((first, second)) = incomparable_keys {
            return Err(Failure::new(label, IncomparableValues { first: first.to_string(), second: second.to_string() }).help(
                "narrow the values down first using is_string(), is_int(), is_float(), is_bool(), is_datetime() or is_duration()",
            ));
        }

        if let Some((value, first, second)) = incomparable_indices {
            return Err(Failure::new(
                label,
                IncomparableIndices {
                    value: value.to_string(),
                    first,
                    second,
                },
            )
            .help(
                "to order them deterministically, sort by a key that distinguishes these elements",
            ));
        }

        Ok(Box::new(
            collected
                .into_iter()
                .map(|(index, subject, _key)| (index, subject)),
        ))
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
