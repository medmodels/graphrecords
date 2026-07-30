use super::reject_key_failures;
use crate::{
    Arity, Bare, Definite, EvaluateOperand, Explain, Failure, IndexDomain, Indexed, Labeled,
    Operand, QueryResult, Single, ValueType,
    error::grouping::MissingGroupAggregate,
    execution::EvaluationCache,
    index::GroupKey,
    operands::{OperandHandle, Partition},
    operations::{
        Apply, ArgumentSource, GroupKernel, IndexedElementContainer, IndexedElementSource,
        KeyOperand, Keyed, Operation, OperationContext, Prepare,
    },
    optimizer::{
        Estimate, Estimated, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats,
    },
    traits::BroadcastVia,
};
use graphrecords_core::GraphRecord;
use graphrecords_utils::aliases::GrHashMap;
use std::marker::PhantomData;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Group)]
#[explain(label = "BroadcastVia")]
pub struct BroadcastViaOperation<I: IndexDomain, A> {
    #[argument]
    via: A,
    index: PhantomData<fn() -> I>,
}

impl<I: IndexDomain, A: Prepare> Prepare for BroadcastViaOperation<I, A> {
    type Prepared<'a>
        = A::Prepared<'a>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.via.prepare(graphrecord, cache)
    }
}

fn broadcast_via<'a, I, K, V, A>(
    prepared: A::Prepared<'a>,
    aggregates: GrHashMap<K::Owned, Option<QueryResult<V::Value<'a>>>>,
    label: &'static str,
) -> IndexedElementContainer<'a, I, V::Value<'a>, A::Arity>
where
    I: IndexDomain,
    K: GroupKey,
    V: ValueType,
    for<'source> A: KeyOperand<I, Key = K>
        + IndexedElementSource<I, Value<'source> = <A as ArgumentSource<Keyed<I>>>::Value<'source>>
        + 'a,
{
    let elements = A::elements(prepared);

    <A::Arity as Arity>::map_elements(elements, move |(index, via_outcome)| {
        let outcome = match via_outcome {
            Err(failure) => Err(failure),
            Ok(value) => match aggregates.get(&A::to_key(&value)) {
                Some(Some(aggregate)) => aggregate.clone(),
                Some(None) | None => Err(Failure::new_at::<I, _>(
                    label,
                    MissingGroupAggregate,
                    &index,
                )),
            },
        };

        (index, outcome)
    })
}

fn broadcast_via_estimate<I: IndexDomain, A: Estimated>(
    operation: &BroadcastViaOperation<I, A>,
    input: &Estimate,
    stats: &Stats,
) -> Estimate {
    let via = operation.via.estimate(stats);

    Estimate {
        elements: via.elements,
        distinct: input.elements,
        selectivity: via.selectivity,
        per_group: None,
    }
}

impl<M, K, I, J, V, A> GroupKernel<M, K, OperandHandle<Indexed<J, V>, Single>>
    for BroadcastViaOperation<I, A>
where
    M: IndexDomain,
    K: GroupKey,
    I: IndexDomain,
    J: IndexDomain,
    V: ValueType,
    for<'a> A: KeyOperand<I, Key = K>
        + IndexedElementSource<I, Value<'a> = <A as ArgumentSource<Keyed<I>>>::Value<'a>>,
{
    type Output = OperandHandle<Indexed<I, V>, A::Arity>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, OperandHandle<Indexed<J, V>, Single>>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let (buckets, key_failures) = partition.into_parts();
        reject_key_failures::<M>(key_failures, Self::LABEL)?;

        let aggregates = buckets
            .into_iter()
            .map(|(key, _, payload)| {
                let aggregate = match payload {
                    Ok(Some((_, outcome))) => Some(outcome),
                    Ok(None) => None,
                    Err(failure) => Some(Err(failure)),
                };

                (key, aggregate)
            })
            .collect();

        Ok(broadcast_via::<_, _, V, A>(
            prepared,
            aggregates,
            Self::LABEL,
        ))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        broadcast_via_estimate(self, &input, stats)
    }
}

impl<M, K, I, V, A> GroupKernel<M, K, OperandHandle<Bare<V>, Single>>
    for BroadcastViaOperation<I, A>
where
    M: IndexDomain,
    K: GroupKey,
    I: IndexDomain,
    V: ValueType,
    for<'a> A: KeyOperand<I, Key = K>
        + IndexedElementSource<I, Value<'a> = <A as ArgumentSource<Keyed<I>>>::Value<'a>>,
{
    type Output = OperandHandle<Indexed<I, V>, A::Arity>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, OperandHandle<Bare<V>, Single>>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let (buckets, key_failures) = partition.into_parts();
        reject_key_failures::<M>(key_failures, Self::LABEL)?;

        let aggregates = buckets
            .into_iter()
            .map(|(key, _, payload)| {
                let aggregate = match payload {
                    Ok(Some(outcome)) => Some(outcome),
                    Ok(None) => None,
                    Err(failure) => Some(Err(failure)),
                };

                (key, aggregate)
            })
            .collect();

        Ok(broadcast_via::<_, _, V, A>(
            prepared,
            aggregates,
            Self::LABEL,
        ))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        broadcast_via_estimate(self, &input, stats)
    }
}

impl<M, K, I, J, V, A> GroupKernel<M, K, OperandHandle<Indexed<J, V>, Definite>>
    for BroadcastViaOperation<I, A>
where
    M: IndexDomain,
    K: GroupKey,
    I: IndexDomain,
    J: IndexDomain,
    V: ValueType,
    for<'a> A: KeyOperand<I, Key = K>
        + IndexedElementSource<I, Value<'a> = <A as ArgumentSource<Keyed<I>>>::Value<'a>>,
{
    type Output = OperandHandle<Indexed<I, V>, A::Arity>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, OperandHandle<Indexed<J, V>, Definite>>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let (buckets, key_failures) = partition.into_parts();
        reject_key_failures::<M>(key_failures, Self::LABEL)?;

        let aggregates = buckets
            .into_iter()
            .map(|(key, _, payload)| {
                let aggregate = match payload {
                    Ok((_, outcome)) => outcome,
                    Err(failure) => Err(failure),
                };

                (key, Some(aggregate))
            })
            .collect();

        Ok(broadcast_via::<_, _, V, A>(
            prepared,
            aggregates,
            Self::LABEL,
        ))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        broadcast_via_estimate(self, &input, stats)
    }
}

impl<M, K, I, V, A> GroupKernel<M, K, OperandHandle<Bare<V>, Definite>>
    for BroadcastViaOperation<I, A>
where
    M: IndexDomain,
    K: GroupKey,
    I: IndexDomain,
    V: ValueType,
    for<'a> A: KeyOperand<I, Key = K>
        + IndexedElementSource<I, Value<'a> = <A as ArgumentSource<Keyed<I>>>::Value<'a>>,
{
    type Output = OperandHandle<Indexed<I, V>, A::Arity>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, OperandHandle<Bare<V>, Definite>>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let (buckets, key_failures) = partition.into_parts();
        reject_key_failures::<M>(key_failures, Self::LABEL)?;

        let aggregates = buckets
            .into_iter()
            .map(|(key, _, payload)| {
                let aggregate = match payload {
                    Ok(outcome) => outcome,
                    Err(failure) => Err(failure),
                };

                (key, Some(aggregate))
            })
            .collect();

        Ok(broadcast_via::<_, _, V, A>(
            prepared,
            aggregates,
            Self::LABEL,
        ))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        broadcast_via_estimate(self, &input, stats)
    }
}

impl<O, I, A> BroadcastVia<I, A> for O
where
    I: IndexDomain,
    BroadcastViaOperation<I, A>: Operation,
    O: Apply<BroadcastViaOperation<I, A>>,
{
    type ReturnOperand = O::Output;

    fn broadcast_via(&self, via: A) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            BroadcastViaOperation {
                via,
                index: PhantomData,
            },
        ))
    }
}
