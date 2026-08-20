use super::reject_key_failures;
use crate::{
    Arity, Bare, BareValueDomain, Definite, EvaluateExpression, Explain, Expression, Failure,
    IndexDomain, Indexed, Labeled, QueryResult, Series, Single, ValueDomain,
    capabilities::ValueGrouping,
    error::grouping::{MissingGroupAggregate, MissingGroupBucket},
    expressions::{ExpressionHandle, Partition},
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

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Group)]
#[explain(label = "BroadcastVia")]
#[plan(optimizer_hints(empty = if_all))]
pub struct BroadcastViaOperation<A> {
    #[argument]
    via: A,
}

fn broadcast_via<'a, K, V, A>(
    graphrecord: &'a GraphRecord,
    prepared: A::Prepared<'a>,
    aggregates: GrHashMap<K::Owned, Option<QueryResult<V::Value<'a>>>>,
    label: &'static str,
) -> IndexedElementContainer<'a, A::IndexDomain, V::Value<'a>, A::Arity>
where
    K: IndexDomain,
    V: ValueDomain,
    A: IndexedElementSource + 'a,
    A::ValueDomain: ValueGrouping<KeyDomain = K>,
{
    let elements = A::elements(prepared);

    A::Arity::map_elements(elements, move |(address, via_outcome)| {
        let outcome = match via_outcome {
            Err(failure) => Err(failure),
            Ok(value) => match aggregates.get(&A::ValueDomain::to_group_key(&value)) {
                Some(Some(aggregate)) => aggregate.clone(),
                Some(None) => Err(Failure::new_at_address::<A::IndexDomain, _>(
                    MissingGroupAggregate,
                    graphrecord,
                    &address,
                    label,
                )),
                None => Err(Failure::new_at_address::<A::IndexDomain, _>(
                    MissingGroupBucket,
                    graphrecord,
                    &address,
                    label,
                )),
            },
        };

        (address, outcome)
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

impl<M, K, J, V, A> GroupKernel<M, K, ExpressionHandle<Indexed<J, V>, Single>>
    for BroadcastViaOperation<A>
where
    M: IndexDomain,
    K: IndexDomain,
    J: IndexDomain,
    V: ValueDomain,
    A: IndexedElementSource,
    A::ValueDomain: ValueGrouping<KeyDomain = K>,
{
    type Output = ExpressionHandle<Indexed<A::IndexDomain, V>, A::Arity>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, ExpressionHandle<Indexed<J, V>, Single>>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
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

        Ok(broadcast_via::<K, V, A>(
            graphrecord,
            prepared,
            aggregates,
            Self::LABEL,
        ))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        broadcast_via_estimate(self, &input, stats)
    }
}

impl<M, K, V, A> GroupKernel<M, K, ExpressionHandle<Bare<V>, Single>> for BroadcastViaOperation<A>
where
    M: IndexDomain,
    K: IndexDomain,
    V: BareValueDomain,
    A: IndexedElementSource,
    A::ValueDomain: ValueGrouping<KeyDomain = K>,
{
    type Output = ExpressionHandle<Indexed<A::IndexDomain, V>, A::Arity>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, ExpressionHandle<Bare<V>, Single>>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
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

        Ok(broadcast_via::<K, V, A>(
            graphrecord,
            prepared,
            aggregates,
            Self::LABEL,
        ))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        broadcast_via_estimate(self, &input, stats)
    }
}

impl<M, K, J, V, A> GroupKernel<M, K, ExpressionHandle<Indexed<J, V>, Definite>>
    for BroadcastViaOperation<A>
where
    M: IndexDomain,
    K: IndexDomain,
    J: IndexDomain,
    V: ValueDomain,
    A: IndexedElementSource,
    A::ValueDomain: ValueGrouping<KeyDomain = K>,
{
    type Output = ExpressionHandle<Indexed<A::IndexDomain, V>, A::Arity>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, ExpressionHandle<Indexed<J, V>, Definite>>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
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

        Ok(broadcast_via::<K, V, A>(
            graphrecord,
            prepared,
            aggregates,
            Self::LABEL,
        ))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        broadcast_via_estimate(self, &input, stats)
    }
}

impl<M, K, V, A> GroupKernel<M, K, ExpressionHandle<Bare<V>, Definite>> for BroadcastViaOperation<A>
where
    M: IndexDomain,
    K: IndexDomain,
    V: BareValueDomain,
    A: IndexedElementSource,
    A::ValueDomain: ValueGrouping<KeyDomain = K>,
{
    type Output = ExpressionHandle<Indexed<A::IndexDomain, V>, A::Arity>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, ExpressionHandle<Bare<V>, Definite>>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
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

        Ok(broadcast_via::<K, V, A>(
            graphrecord,
            prepared,
            aggregates,
            Self::LABEL,
        ))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        broadcast_via_estimate(self, &input, stats)
    }
}

impl<E, A> BroadcastVia<A::IndexDomain, A> for E
where
    A: IndexedElementSource,
    BroadcastViaOperation<A>: Operation,
    E: Apply<BroadcastViaOperation<A>>,
{
    type Output = E::Output;

    fn broadcast_via(&self, via: A) -> Self::Output {
        Self::Output::new(OperationContext::new(
            self.clone(),
            BroadcastViaOperation { via },
        ))
    }
}

impl<E, A> BroadcastVia<A::IndexDomain, A> for Series<E>
where
    A: IndexedElementSource,
    E: Expression + BroadcastVia<A::IndexDomain, A>,
{
    type Output = Series<E::Output>;

    fn broadcast_via(&self, via: A) -> Self::Output {
        self.bind(self.expression().broadcast_via(via))
    }
}

operation_manifest! {
    BroadcastViaOperation<A> {
        method: BroadcastVia<I, A>::broadcast_via;
        scope: group;

        kernel {
            group: <M: IndexDomain, K: IndexDomain>;
            parameters: <
                J: IndexDomain,
                V: ValueDomain,
                I: IndexDomain,
                X: ValueGrouping<K>,
                C: EnumerableArity,
            >;
            argument: A: IndexedElementSource<Indexed<I, X>, C>;
            input: ExpressionHandle<Indexed<J, V>, Single>;
            output: ExpressionHandle<Indexed<I, V>, C>;
        }

        kernel {
            group: <M: IndexDomain, K: IndexDomain>;
            parameters: <
                J: IndexDomain,
                V: ValueDomain,
                I: IndexDomain,
                X: ValueGrouping<K>,
                C: EnumerableArity,
            >;
            argument: A: IndexedElementSource<Indexed<I, X>, C>;
            input: ExpressionHandle<Indexed<J, V>, Definite>;
            output: ExpressionHandle<Indexed<I, V>, C>;
        }

        kernel {
            group: <M: IndexDomain, K: IndexDomain>;
            parameters: <
                V: BareValueDomain,
                I: IndexDomain,
                X: ValueGrouping<K>,
                C: EnumerableArity,
            >;
            argument: A: IndexedElementSource<Indexed<I, X>, C>;
            input: ExpressionHandle<Bare<V>, Single>;
            output: ExpressionHandle<Indexed<I, V>, C>;
        }

        kernel {
            group: <M: IndexDomain, K: IndexDomain>;
            parameters: <
                V: BareValueDomain,
                I: IndexDomain,
                X: ValueGrouping<K>,
                C: EnumerableArity,
            >;
            argument: A: IndexedElementSource<Indexed<I, X>, C>;
            input: ExpressionHandle<Bare<V>, Definite>;
            output: ExpressionHandle<Indexed<I, V>, C>;
        }
    }
}
