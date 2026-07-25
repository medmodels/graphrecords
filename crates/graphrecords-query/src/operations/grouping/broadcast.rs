use super::MissingGroupAggregate;
use crate::{
    Bare, BoxedIterator, Definite, EvaluateOperand, Explain, Failure, IndexDomain, Indexed,
    Labeled, Multiple, Operand, QueryResult, Single, Unordered, ValueType,
    execution::EvaluationCache,
    operands::{GroupOperand, OperandHandle},
    operations::{Apply, KeyOperand, Operation, OperationContext, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Broadcast,
};
use graphrecords_core::GraphRecord;
use graphrecords_utils::aliases::GrHashMap;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "Broadcast")]
pub struct BroadcastOperation<K: KeyOperand> {
    #[argument]
    pub key: K,
}

fn broadcast_estimate<K: KeyOperand>(
    operation: &BroadcastOperation<K>,
    input: &Estimate,
    stats: &Stats,
) -> Estimate {
    let elements = operation.key.estimate(stats).elements;
    let distinct = match (input.elements, elements) {
        (Some(distinct), Some(elements)) => Some(distinct.min(elements)),
        (distinct, _) => distinct,
    };

    Estimate {
        elements,
        distinct,
        selectivity: None,
        per_group: None,
    }
}

fn broadcast_aggregates<'a, I, V, K>(
    aggregates: GrHashMap<<K::Key as IndexDomain>::Index<'a>, QueryResult<V>>,
    prepared: &K::Prepared<'a>,
) -> BoxedIterator<'a, (I::Index<'a>, QueryResult<V>)>
where
    I: IndexDomain,
    V: Clone + 'a,
    K: KeyOperand<Subject = I>,
{
    let assignments: Vec<_> = K::assignments(prepared).collect();
    let label = <BroadcastOperation<K> as Labeled>::LABEL;

    Box::new(assignments.into_iter().map(move |(index, key)| {
        let value = match aggregates.get(&key) {
            Some(aggregate) => aggregate.clone(),
            None => Err(Failure::new_at(label, MissingGroupAggregate, &index)),
        };

        (index, value)
    }))
}

impl<K: KeyOperand> Prepare for BroadcastOperation<K> {
    type Prepared<'a> = K::Prepared<'a>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Prepare::prepare(&self.key, graphrecord, cache)
    }
}

impl<I, V, K> Apply<BroadcastOperation<K>> for GroupOperand<OperandHandle<Indexed<I, V>, Single>, K>
where
    I: IndexDomain,
    V: ValueType,
    K: KeyOperand<Subject = I>,
{
    type Output = OperandHandle<Indexed<I, V>, Multiple<Unordered>>;

    fn apply<'a>(
        _graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        prepared: <BroadcastOperation<K> as Prepare>::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        let aggregates: GrHashMap<<K::Key as IndexDomain>::Index<'a>, QueryResult<V::Value<'a>>> =
            values
                .filter_map(|(key, aggregate)| match aggregate {
                    Ok(Some((_, value))) => Some((key, value)),
                    Ok(None) => None,
                    Err(failure) => Some((key, Err(failure))),
                })
                .collect();

        Ok(broadcast_aggregates::<I, _, K>(aggregates, &prepared))
    }

    fn estimate(operation: &BroadcastOperation<K>, input: Estimate, stats: &Stats) -> Estimate {
        broadcast_estimate(operation, &input, stats)
    }
}

impl<I, V, K> Apply<BroadcastOperation<K>> for GroupOperand<OperandHandle<Bare<V>, Single>, K>
where
    I: IndexDomain,
    V: ValueType,
    K: KeyOperand<Subject = I>,
{
    type Output = OperandHandle<Indexed<I, V>, Multiple<Unordered>>;

    fn apply<'a>(
        _graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        prepared: <BroadcastOperation<K> as Prepare>::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        let aggregates: GrHashMap<<K::Key as IndexDomain>::Index<'a>, QueryResult<V::Value<'a>>> =
            values
                .filter_map(|(key, aggregate)| match aggregate {
                    Ok(Some(value)) => Some((key, value)),
                    Ok(None) => None,
                    Err(failure) => Some((key, Err(failure))),
                })
                .collect();

        Ok(broadcast_aggregates::<I, _, K>(aggregates, &prepared))
    }

    fn estimate(operation: &BroadcastOperation<K>, input: Estimate, stats: &Stats) -> Estimate {
        broadcast_estimate(operation, &input, stats)
    }
}

impl<I, V, K> Apply<BroadcastOperation<K>>
    for GroupOperand<OperandHandle<Indexed<I, V>, Definite>, K>
where
    I: IndexDomain,
    V: ValueType,
    K: KeyOperand<Subject = I>,
{
    type Output = OperandHandle<Indexed<I, V>, Multiple<Unordered>>;

    fn apply<'a>(
        _graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        prepared: <BroadcastOperation<K> as Prepare>::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        let aggregates: GrHashMap<<K::Key as IndexDomain>::Index<'a>, QueryResult<V::Value<'a>>> =
            values
                .map(|(key, aggregate)| {
                    let aggregate = aggregate.and_then(|(_index, value)| value);

                    (key, aggregate)
                })
                .collect();

        Ok(broadcast_aggregates::<I, _, K>(aggregates, &prepared))
    }

    fn estimate(operation: &BroadcastOperation<K>, input: Estimate, stats: &Stats) -> Estimate {
        broadcast_estimate(operation, &input, stats)
    }
}

impl<I, V, K> Apply<BroadcastOperation<K>> for GroupOperand<OperandHandle<Bare<V>, Definite>, K>
where
    I: IndexDomain,
    V: ValueType,
    K: KeyOperand<Subject = I>,
{
    type Output = OperandHandle<Indexed<I, V>, Multiple<Unordered>>;

    fn apply<'a>(
        _graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        prepared: <BroadcastOperation<K> as Prepare>::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        let aggregates: GrHashMap<<K::Key as IndexDomain>::Index<'a>, QueryResult<V::Value<'a>>> =
            values
                .map(|(key, aggregate)| (key, aggregate.and_then(|value| value)))
                .collect();

        Ok(broadcast_aggregates::<I, _, K>(aggregates, &prepared))
    }

    fn estimate(operation: &BroadcastOperation<K>, input: Estimate, stats: &Stats) -> Estimate {
        broadcast_estimate(operation, &input, stats)
    }
}

impl<O, K> Broadcast<K> for O
where
    K: KeyOperand,
    O: Apply<BroadcastOperation<K>>,
{
    type ReturnOperand = <O as Apply<BroadcastOperation<K>>>::Output;

    fn broadcast(&self, key: K) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            BroadcastOperation { key },
        ))
    }
}
