use crate::{
    Arity, Bare, BoxedIterator, Definite, ElementShape, EvaluateOperand, IndexDomain, Indexed,
    Multiple, Operand, QueryResult, ReturnShape, Single, Unordered, ValueType,
    execution::EvaluationCache,
    operands::OperandHandle,
    operations::{
        Absent, Alignment, Apply, ArgumentSource, KeyOperand, Keyed, Lookup, Operation, Prepare,
        Preserving,
    },
    optimizer::{Estimate, Stats},
};
use graphrecords_core::GraphRecord;
use graphrecords_utils::aliases::GrHashMap;
use std::{hash::Hash, marker::PhantomData, sync::Arc};

pub type GroupedIterator<'a, K, T> = BoxedIterator<'a, (K, T)>;

pub fn try_partition_by<'a, K, I>(
    items: BoxedIterator<'a, I>,
    key_of: impl Fn(&I) -> QueryResult<Option<K>>,
) -> QueryResult<GroupedIterator<'a, K, BoxedIterator<'a, I>>>
where
    K: Clone + Eq + Hash + 'a,
    I: 'a,
{
    let mut buckets: Vec<(K, Vec<I>)> = Vec::new();
    let mut positions: GrHashMap<K, usize> = GrHashMap::default();

    for item in items {
        let Some(key) = key_of(&item)? else {
            continue;
        };

        if let Some(&position) = positions.get(&key) {
            buckets[position].1.push(item);
        } else {
            positions.insert(key.clone(), buckets.len());
            buckets.push((key, vec![item]));
        }
    }

    Ok(Box::new(buckets.into_iter().map(|(key, items)| {
        (key, Box::new(items.into_iter()) as BoxedIterator<'a, I>)
    })))
}

pub struct Grouped<K, O>(PhantomData<(K, O)>);

impl<K: KeyOperand, O: Operand> ElementShape for Grouped<K, O> {
    type Element<'a> = (
        <K::Key as IndexDomain>::Index<'a>,
        QueryResult<O::ReturnValue<'a>>,
    );
}

impl<K, S, C> ReturnShape for Grouped<K, OperandHandle<S, C>>
where
    K: KeyOperand,
    S: ReturnShape,
    C: Arity,
{
    type ReturnElement<'a> = (
        <K::Key as IndexDomain>::Index<'a>,
        QueryResult<C::Container<'a, S::ReturnElement<'a>>>,
    );

    fn into_return_element(element: Self::Element<'_>) -> Self::ReturnElement<'_> {
        let (key, partition) = element;
        let partition =
            partition.map(|partition| C::map_elements(partition, S::into_return_element));

        (key, partition)
    }
}

pub type GroupOperand<O, K> = OperandHandle<Grouped<K, O>, Multiple<Unordered>>;

type GroupedArgumentMap<'a, K, V> =
    Arc<GrHashMap<<<K as KeyOperand>::Key as IndexDomain>::Index<'a>, Option<QueryResult<V>>>>;

impl<I, V, K> Prepare for GroupOperand<OperandHandle<Indexed<I, V>, Single>, K>
where
    I: IndexDomain,
    V: ValueType,
    K: KeyOperand,
{
    type Prepared<'a>
        = GroupedArgumentMap<'a, K, V::Value<'a>>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(Arc::new(
            self.evaluate(graphrecord, cache)?
                .map(|(key, value)| {
                    let value = match value {
                        Ok(Some((_index, value))) => Some(value),
                        Ok(None) => None,
                        Err(failure) => Some(Err(failure)),
                    };

                    (key, value)
                })
                .collect(),
        ))
    }
}

impl<I, V, K> ArgumentSource<Keyed<K::Key>>
    for GroupOperand<OperandHandle<Indexed<I, V>, Single>, K>
where
    I: IndexDomain,
    V: ValueType,
    K: KeyOperand,
{
    type Retention = Preserving;
    type Value<'a> = V::Value<'a>;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        address: &<Keyed<K::Key> as Alignment>::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<Self::Value<'a>>>
    where
        Self: 'a,
    {
        match prepared.get(address) {
            Some(Some(value)) => Lookup::Present(value),
            Some(None) => Lookup::Absent(Absent::Empty),
            None => Lookup::Absent(Absent::Uncovered),
        }
    }
}

impl<V, K> Prepare for GroupOperand<OperandHandle<Bare<V>, Single>, K>
where
    V: ValueType,
    K: KeyOperand,
{
    type Prepared<'a>
        = GroupedArgumentMap<'a, K, V::Value<'a>>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(Arc::new(
            self.evaluate(graphrecord, cache)?
                .map(|(key, value)| {
                    let value = match value {
                        Ok(value) => value,
                        Err(failure) => Some(Err(failure)),
                    };

                    (key, value)
                })
                .collect(),
        ))
    }
}

impl<V, K> ArgumentSource<Keyed<K::Key>> for GroupOperand<OperandHandle<Bare<V>, Single>, K>
where
    V: ValueType,
    K: KeyOperand,
{
    type Retention = Preserving;
    type Value<'a> = V::Value<'a>;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        address: &<Keyed<K::Key> as Alignment>::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<Self::Value<'a>>>
    where
        Self: 'a,
    {
        match prepared.get(address) {
            Some(Some(value)) => Lookup::Present(value),
            Some(None) => Lookup::Absent(Absent::Empty),
            None => Lookup::Absent(Absent::Uncovered),
        }
    }
}

impl<I, V, K> Prepare for GroupOperand<OperandHandle<Indexed<I, V>, Definite>, K>
where
    I: IndexDomain,
    V: ValueType,
    K: KeyOperand,
{
    type Prepared<'a>
        = GroupedArgumentMap<'a, K, V::Value<'a>>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(Arc::new(
            self.evaluate(graphrecord, cache)?
                .map(|(key, value)| {
                    let value = value.and_then(|(_index, value)| value);

                    (key, Some(value))
                })
                .collect(),
        ))
    }
}

impl<I, V, K> ArgumentSource<Keyed<K::Key>>
    for GroupOperand<OperandHandle<Indexed<I, V>, Definite>, K>
where
    I: IndexDomain,
    V: ValueType,
    K: KeyOperand,
{
    type Retention = Preserving;
    type Value<'a> = V::Value<'a>;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        address: &<Keyed<K::Key> as Alignment>::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<Self::Value<'a>>>
    where
        Self: 'a,
    {
        match prepared.get(address) {
            Some(Some(value)) => Lookup::Present(value),
            Some(None) => Lookup::Absent(Absent::Empty),
            None => Lookup::Absent(Absent::Uncovered),
        }
    }
}

impl<V, K> Prepare for GroupOperand<OperandHandle<Bare<V>, Definite>, K>
where
    V: ValueType,
    K: KeyOperand,
{
    type Prepared<'a>
        = GroupedArgumentMap<'a, K, V::Value<'a>>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(Arc::new(
            self.evaluate(graphrecord, cache)?
                .map(|(key, value)| (key, Some(value.and_then(|value| value))))
                .collect(),
        ))
    }
}

impl<V, K> ArgumentSource<Keyed<K::Key>> for GroupOperand<OperandHandle<Bare<V>, Definite>, K>
where
    V: ValueType,
    K: KeyOperand,
{
    type Retention = Preserving;
    type Value<'a> = V::Value<'a>;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        address: &<Keyed<K::Key> as Alignment>::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<Self::Value<'a>>>
    where
        Self: 'a,
    {
        match prepared.get(address) {
            Some(Some(value)) => Lookup::Present(value),
            Some(None) => Lookup::Absent(Absent::Empty),
            None => Lookup::Absent(Absent::Uncovered),
        }
    }
}

impl<O, K, P> Apply<P> for OperandHandle<Grouped<K, O>, Multiple<Unordered>>
where
    O: Apply<P>,
    K: KeyOperand,
    P: Operation,
{
    type Output = OperandHandle<Grouped<K, <O as Apply<P>>::Output>, Multiple<Unordered>>;

    fn apply<'a>(
        graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        prepared: P::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        Ok(Box::new(values.map(move |(key, partition)| {
            let result =
                partition.and_then(|partition| O::apply(graphrecord, partition, prepared.clone()));

            (key, result)
        })))
    }

    fn estimate(operation: &P, input: Estimate, stats: &Stats) -> Estimate {
        Estimate {
            per_group: input
                .per_group
                .map(|inner| Box::new(O::estimate(operation, *inner, stats))),
            ..input
        }
    }
}
