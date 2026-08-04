use super::reject_key_failures;
use crate::{
    Arity, Bare, BareValueDomain, Definite, EvaluateOperand, Explain, Failure, IndexDomain,
    Indexed, Labeled, Operand, QueryResult, Single, ValueDomain,
    capabilities::GroupingValue,
    error::grouping::MissingGroupAggregate,
    execution::EvaluationCache,
    index::GroupKey,
    operands::{OperandHandle, Partition},
    operations::{
        Apply, GroupKernel, IndexedElementContainer, IndexedElementSource, Operation,
        OperationContext, Prepare,
    },
    optimizer::{
        Estimate, Estimated, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats,
    },
    registry::operation_manifest,
    traits::BroadcastVia,
};
use graphrecords_core::GraphRecord;
use graphrecords_utils::aliases::GrHashMap;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Group)]
#[explain(label = "BroadcastVia")]
#[plan(optimizer_hints(empty = if_all))]
pub struct BroadcastViaOperation<A> {
    #[argument]
    via: A,
}

impl<A: Prepare> Prepare for BroadcastViaOperation<A> {
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

fn broadcast_via<'a, K, V, A>(
    prepared: A::Prepared<'a>,
    aggregates: GrHashMap<K::Owned, Option<QueryResult<V::Value<'a>>>>,
    label: &'static str,
) -> IndexedElementContainer<'a, A::IndexDomain, V::Value<'a>, A::Arity>
where
    K: GroupKey,
    V: ValueDomain,
    A: IndexedElementSource + 'a,
    A::ValueDomain: GroupingValue<Key = K>,
{
    let elements = A::elements(prepared);

    A::Arity::map_elements(elements, move |(index, via_outcome)| {
        let outcome = match via_outcome {
            Err(failure) => Err(failure),
            Ok(value) => match aggregates.get(&A::ValueDomain::to_group_key(&value)) {
                Some(Some(aggregate)) => aggregate.clone(),
                Some(None) | None => Err(Failure::new_at::<A::IndexDomain, _>(
                    label,
                    MissingGroupAggregate,
                    &index,
                )),
            },
        };

        (index, outcome)
    })
}

fn broadcast_via_estimate<A: Estimated>(
    operation: &BroadcastViaOperation<A>,
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

impl<M, K, J, V, A> GroupKernel<M, K, OperandHandle<Indexed<J, V>, Single>>
    for BroadcastViaOperation<A>
where
    M: IndexDomain,
    K: GroupKey,
    J: IndexDomain,
    V: ValueDomain,
    A: IndexedElementSource,
    A::ValueDomain: GroupingValue<Key = K>,
{
    type Output = OperandHandle<Indexed<A::IndexDomain, V>, A::Arity>;

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

        Ok(broadcast_via::<K, V, A>(prepared, aggregates, Self::LABEL))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        broadcast_via_estimate(self, &input, stats)
    }
}

impl<M, K, V, A> GroupKernel<M, K, OperandHandle<Bare<V>, Single>> for BroadcastViaOperation<A>
where
    M: IndexDomain,
    K: GroupKey,
    V: BareValueDomain,
    A: IndexedElementSource,
    A::ValueDomain: GroupingValue<Key = K>,
{
    type Output = OperandHandle<Indexed<A::IndexDomain, V>, A::Arity>;

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

        Ok(broadcast_via::<K, V, A>(prepared, aggregates, Self::LABEL))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        broadcast_via_estimate(self, &input, stats)
    }
}

impl<M, K, J, V, A> GroupKernel<M, K, OperandHandle<Indexed<J, V>, Definite>>
    for BroadcastViaOperation<A>
where
    M: IndexDomain,
    K: GroupKey,
    J: IndexDomain,
    V: ValueDomain,
    A: IndexedElementSource,
    A::ValueDomain: GroupingValue<Key = K>,
{
    type Output = OperandHandle<Indexed<A::IndexDomain, V>, A::Arity>;

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

        Ok(broadcast_via::<K, V, A>(prepared, aggregates, Self::LABEL))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        broadcast_via_estimate(self, &input, stats)
    }
}

impl<M, K, V, A> GroupKernel<M, K, OperandHandle<Bare<V>, Definite>> for BroadcastViaOperation<A>
where
    M: IndexDomain,
    K: GroupKey,
    V: BareValueDomain,
    A: IndexedElementSource,
    A::ValueDomain: GroupingValue<Key = K>,
{
    type Output = OperandHandle<Indexed<A::IndexDomain, V>, A::Arity>;

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

        Ok(broadcast_via::<K, V, A>(prepared, aggregates, Self::LABEL))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        broadcast_via_estimate(self, &input, stats)
    }
}

impl<O, A> BroadcastVia<A::IndexDomain, A> for O
where
    A: IndexedElementSource,
    BroadcastViaOperation<A>: Operation,
    O: Apply<BroadcastViaOperation<A>>,
{
    type ReturnOperand = O::Output;

    fn broadcast_via(&self, via: A) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            BroadcastViaOperation { via },
        ))
    }
}

operation_manifest! {
    BroadcastViaOperation<A> {
        method: BroadcastVia<J, A>::broadcast_via;
        scope: group;

        kernel {
            group: <M: IndexDomain, K: GroupKey>;
            parameters: <
                P: IndexDomain,
                V: ValueDomain,
                J: IndexDomain,
                X: GroupingValue<K>,
                C: EnumerableArity,
            >;
            argument: A: IndexedElementSource<Indexed<J, X>, C>;
            input: OperandHandle<Indexed<P, V>, Single>;
            output: OperandHandle<Indexed<J, V>, C>;
        }

        kernel {
            group: <M: IndexDomain, K: GroupKey>;
            parameters: <
                P: IndexDomain,
                V: ValueDomain,
                J: IndexDomain,
                X: GroupingValue<K>,
                C: EnumerableArity,
            >;
            argument: A: IndexedElementSource<Indexed<J, X>, C>;
            input: OperandHandle<Indexed<P, V>, Definite>;
            output: OperandHandle<Indexed<J, V>, C>;
        }

        kernel {
            group: <M: IndexDomain, K: GroupKey>;
            parameters: <
                V: BareValueDomain,
                J: IndexDomain,
                X: GroupingValue<K>,
                C: EnumerableArity,
            >;
            argument: A: IndexedElementSource<Indexed<J, X>, C>;
            input: OperandHandle<Bare<V>, Single>;
            output: OperandHandle<Indexed<J, V>, C>;
        }

        kernel {
            group: <M: IndexDomain, K: GroupKey>;
            parameters: <
                V: BareValueDomain,
                J: IndexDomain,
                X: GroupingValue<K>,
                C: EnumerableArity,
            >;
            argument: A: IndexedElementSource<Indexed<J, X>, C>;
            input: OperandHandle<Bare<V>, Definite>;
            output: OperandHandle<Indexed<J, V>, C>;
        }
    }
}
