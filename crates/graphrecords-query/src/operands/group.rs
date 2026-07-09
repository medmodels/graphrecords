use crate::{
    BoxedIterator, ElementShape, EvaluateOperand, Multiple, Operand, QueryResult,
    operands::OperandHandle,
    operations::{Apply, GroupKey, Operation},
    optimizer::{EstimateCost, GroupCost, Stats},
};
use graphrecords_core::GraphRecord;
use graphrecords_utils::aliases::GrHashMap;
use std::{hash::Hash, marker::PhantomData};

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

impl<K: GroupKey, O: Operand> ElementShape for Grouped<K, O> {
    type Cost = GroupCost<O::Cost>;
    type Element<'a> = (K::Key<'a>, QueryResult<O::ReturnValue<'a>>);
}

pub type GroupOperand<O, K> = OperandHandle<Grouped<K, O>, Multiple>;

impl<O, K, P> Apply<P> for OperandHandle<Grouped<K, O>, Multiple>
where
    O: Apply<P>,
    K: GroupKey,
    P: Operation,
{
    type Output = OperandHandle<Grouped<K, <O as Apply<P>>::Output>, Multiple>;

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
}

impl<O, K, P> EstimateCost<P> for OperandHandle<Grouped<K, O>, Multiple>
where
    O: Apply<P>,
    K: GroupKey,
    P: Operation,
{
    type OutputCost = GroupCost<<O as EstimateCost<P>>::OutputCost>;

    fn estimate(
        operation: &P,
        input_cost: <Self as Operand>::Cost,
        stats: &Stats,
    ) -> Self::OutputCost {
        input_cost.map(|per_group| O::estimate(operation, per_group, stats))
    }
}
