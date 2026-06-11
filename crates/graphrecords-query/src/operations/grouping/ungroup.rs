use crate::{
    BoxedIterator, EvaluateContext, EvaluateOperand, Explain, IndexDomain, Operand, QueryResult,
    execution::EvaluationCache,
    operands::{
        BareValueOperand, BareValuesOperand, GroupOperand, GroupedIterator, ValueOperand,
        ValuesOperand,
    },
    operations::GroupKey,
    optimizer::{
        Cardinality, Cost, MatchInputs, OptimizePlan, OptimizerHints, PlanNode, Session, Stats,
        Transformed,
    },
    traits::Ungroup,
};
use graphrecords_core::{GraphRecord, graphrecord::GraphRecordValue};

pub trait Ungroupable: Operand {
    type Ungrouped<K: GroupKey>: Operand<Cost = Cardinality>;

    fn flatten<'a, K: GroupKey>(
        grouped: GroupedIterator<'a, K::Key, QueryResult<Self::ReturnValue<'a>>>,
    ) -> QueryResult<<Self::Ungrouped<K> as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a;
}

impl<I: IndexDomain> Ungroupable for ValueOperand<I> {
    type Ungrouped<K: GroupKey> = ValuesOperand<I>;

    fn flatten<'a, K: GroupKey>(
        grouped: GroupedIterator<'a, K::Key, QueryResult<Self::ReturnValue<'a>>>,
    ) -> QueryResult<BoxedIterator<'a, (I::Index<'a>, QueryResult<GraphRecordValue>)>>
    where
        Self: 'a,
    {
        let partitions = grouped
            .map(|(_key, partition)| partition)
            .collect::<QueryResult<Vec<_>>>()?;

        Ok(Box::new(partitions.into_iter().flatten()))
    }
}

impl<I: IndexDomain> Ungroupable for ValuesOperand<I> {
    type Ungrouped<K: GroupKey> = Self;

    fn flatten<'a, K: GroupKey>(
        grouped: GroupedIterator<'a, K::Key, QueryResult<Self::ReturnValue<'a>>>,
    ) -> QueryResult<<Self::Ungrouped<K> as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        let partitions = grouped
            .map(|(_key, partition)| partition)
            .collect::<QueryResult<Vec<_>>>()?;

        Ok(Box::new(partitions.into_iter().flatten()))
    }
}

impl Ungroupable for BareValueOperand {
    type Ungrouped<K: GroupKey> = BareValuesOperand;

    fn flatten<'a, K: GroupKey>(
        grouped: GroupedIterator<'a, K::Key, QueryResult<Self::ReturnValue<'a>>>,
    ) -> QueryResult<BoxedIterator<'a, QueryResult<GraphRecordValue>>>
    where
        Self: 'a,
    {
        let partitions = grouped
            .map(|(_key, partition)| partition)
            .collect::<QueryResult<Vec<_>>>()?;

        Ok(Box::new(partitions.into_iter().flatten()))
    }
}

impl Ungroupable for BareValuesOperand {
    type Ungrouped<K: GroupKey> = Self;

    fn flatten<'a, K: GroupKey>(
        grouped: GroupedIterator<'a, K::Key, QueryResult<Self::ReturnValue<'a>>>,
    ) -> QueryResult<<Self::Ungrouped<K> as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        let partitions = grouped
            .map(|(_key, partition)| partition)
            .collect::<QueryResult<Vec<_>>>()?;

        Ok(Box::new(partitions.into_iter().flatten()))
    }
}

impl<Inner: Ungroupable, KInner: GroupKey> Ungroupable for GroupOperand<Inner, KInner> {
    type Ungrouped<KOuter: GroupKey> = GroupOperand<Inner::Ungrouped<KInner>, KOuter>;

    fn flatten<'a, KOuter: GroupKey>(
        grouped: GroupedIterator<'a, KOuter::Key, QueryResult<Self::ReturnValue<'a>>>,
    ) -> QueryResult<<Self::Ungrouped<KOuter> as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        Ok(Box::new(grouped.map(|(key, inner)| {
            (key, inner.and_then(Inner::flatten::<KInner>))
        })))
    }
}

#[derive(PlanNode, MatchInputs, OptimizerHints, Explain)]
#[explain(label = "Ungroup")]
pub struct UngroupContext<O: Ungroupable, K: GroupKey> {
    #[input]
    group: GroupOperand<O, K>,
}

impl<O: Ungroupable, K: GroupKey> UngroupContext<O, K> {
    #[must_use]
    pub const fn new(group: GroupOperand<O, K>) -> Self {
        Self { group }
    }
}

impl<O: Ungroupable, K: GroupKey> Cost<O::Ungrouped<K>> for UngroupContext<O, K> {
    fn cost(&self, stats: &Stats) -> <O::Ungrouped<K> as Operand>::Cost {
        self.group.context().cost(stats)
    }
}

impl<O: Ungroupable, K: GroupKey> OptimizePlan for UngroupContext<O, K> {
    type Output = O::Ungrouped<K>;

    fn optimize(&self, original: &Self::Output, session: &Session) -> Transformed<Self::Output> {
        let group = session.optimize(&self.group);

        if !group.changed {
            return Transformed::unchanged(original.clone());
        }

        Transformed {
            value: <O::Ungrouped<K> as Operand>::new(Self { group: group.value }),
            changed: true,
        }
    }
}

impl<O: Ungroupable, K: GroupKey> EvaluateContext for UngroupContext<O, K> {
    type Operand = O::Ungrouped<K>;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<<O::Ungrouped<K> as EvaluateOperand>::ReturnValue<'a>> {
        let grouped = self.group.evaluate(graphrecord, cache)?;

        O::flatten::<K>(grouped)
    }
}

impl<O: Ungroupable, K: GroupKey> Ungroup for GroupOperand<O, K> {
    type ReturnOperand = O::Ungrouped<K>;

    fn ungroup(&self) -> Self::ReturnOperand {
        <O::Ungrouped<K> as Operand>::new(UngroupContext::new(self.clone()))
    }
}
