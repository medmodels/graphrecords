use crate::{
    Bare, Definite, EvaluateContext, EvaluateOperand, Explain, IndexDomain, Indexed, Multiple,
    Operand, OrderState, QueryResult, Single, Unordered, ValueType,
    execution::EvaluationCache,
    operands::{GroupOperand, GroupedIterator, OperandHandle},
    operations::GroupKey,
    optimizer::{
        Estimate, Estimated, MatchInputs, OptimizePlan, OptimizerHints, PlanNode, Session, Stats,
        Transformed,
    },
    traits::Ungroup,
};
use graphrecords_core::GraphRecord;

pub trait Ungroupable: Operand {
    type Ungrouped<K: GroupKey>: Operand;

    #[must_use]
    fn flatten_estimate(estimate: Estimate) -> Estimate {
        let per_group = estimate.per_group.as_deref();
        let elements = match (
            estimate.elements,
            per_group.and_then(|inner| inner.elements),
        ) {
            (Some(groups), Some(inner)) => Some(groups * inner),
            _ => None,
        };
        let distinct = match (
            estimate.elements,
            per_group.and_then(|inner| inner.distinct),
        ) {
            (Some(groups), Some(inner)) => Some(groups * inner),
            _ => None,
        };

        Estimate {
            elements,
            distinct: match (distinct, elements) {
                (Some(distinct), Some(elements)) => Some(distinct.min(elements)),
                (distinct, _) => distinct,
            },
            selectivity: per_group.and_then(|inner| inner.selectivity),
            per_group: None,
        }
    }

    fn flatten<'a, K: GroupKey>(
        grouped: GroupedIterator<'a, K::Key<'a>, QueryResult<Self::ReturnValue<'a>>>,
    ) -> QueryResult<<Self::Ungrouped<K> as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a;
}

impl<I: IndexDomain, V: ValueType, O: OrderState> Ungroupable
    for OperandHandle<Indexed<I, V>, Multiple<O>>
{
    type Ungrouped<K: GroupKey> = OperandHandle<Indexed<I, V>, Multiple<Unordered>>;

    fn flatten<'a, K: GroupKey>(
        grouped: GroupedIterator<'a, K::Key<'a>, QueryResult<Self::ReturnValue<'a>>>,
    ) -> QueryResult<<Self::Ungrouped<K> as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        let partitions: Vec<_> = grouped
            .map(|(_key, partition)| partition)
            .collect::<QueryResult<_>>()?;

        Ok(Box::new(partitions.into_iter().flatten()))
    }
}

impl<V: ValueType, O: OrderState> Ungroupable for OperandHandle<Bare<V>, Multiple<O>> {
    type Ungrouped<K: GroupKey> = OperandHandle<Bare<V>, Multiple<Unordered>>;

    fn flatten<'a, K: GroupKey>(
        grouped: GroupedIterator<'a, K::Key<'a>, QueryResult<Self::ReturnValue<'a>>>,
    ) -> QueryResult<<Self::Ungrouped<K> as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        let partitions: Vec<_> = grouped
            .map(|(_key, partition)| partition)
            .collect::<QueryResult<_>>()?;

        Ok(Box::new(partitions.into_iter().flatten()))
    }
}

impl<I: IndexDomain, V: ValueType> Ungroupable for OperandHandle<Indexed<I, V>, Single> {
    type Ungrouped<K: GroupKey> = OperandHandle<Indexed<I, V>, Multiple<Unordered>>;

    fn flatten_estimate(estimate: Estimate) -> Estimate {
        Estimate {
            elements: estimate.elements,
            distinct: estimate.elements,
            selectivity: estimate
                .per_group
                .as_deref()
                .and_then(|inner| inner.selectivity),
            per_group: None,
        }
    }

    fn flatten<'a, K: GroupKey>(
        grouped: GroupedIterator<'a, K::Key<'a>, QueryResult<Self::ReturnValue<'a>>>,
    ) -> QueryResult<<Self::Ungrouped<K> as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        let partitions: Vec<_> = grouped
            .map(|(_key, partition)| partition)
            .collect::<QueryResult<_>>()?;

        Ok(Box::new(partitions.into_iter().flatten()))
    }
}

impl<V: ValueType> Ungroupable for OperandHandle<Bare<V>, Single> {
    type Ungrouped<K: GroupKey> = OperandHandle<Bare<V>, Multiple<Unordered>>;

    fn flatten_estimate(estimate: Estimate) -> Estimate {
        Estimate {
            elements: estimate.elements,
            distinct: estimate.elements,
            selectivity: estimate
                .per_group
                .as_deref()
                .and_then(|inner| inner.selectivity),
            per_group: None,
        }
    }

    fn flatten<'a, K: GroupKey>(
        grouped: GroupedIterator<'a, K::Key<'a>, QueryResult<Self::ReturnValue<'a>>>,
    ) -> QueryResult<<Self::Ungrouped<K> as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        let partitions: Vec<_> = grouped
            .map(|(_key, partition)| partition)
            .collect::<QueryResult<_>>()?;

        Ok(Box::new(partitions.into_iter().flatten()))
    }
}

impl<I: IndexDomain, V: ValueType> Ungroupable for OperandHandle<Indexed<I, V>, Definite> {
    type Ungrouped<K: GroupKey> = OperandHandle<Indexed<I, V>, Multiple<Unordered>>;

    fn flatten_estimate(estimate: Estimate) -> Estimate {
        Estimate {
            elements: estimate.elements,
            distinct: estimate.elements,
            selectivity: estimate
                .per_group
                .as_deref()
                .and_then(|inner| inner.selectivity),
            per_group: None,
        }
    }

    fn flatten<'a, K: GroupKey>(
        grouped: GroupedIterator<'a, K::Key<'a>, QueryResult<Self::ReturnValue<'a>>>,
    ) -> QueryResult<<Self::Ungrouped<K> as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        let partitions: Vec<_> = grouped
            .map(|(_key, partition)| partition)
            .collect::<QueryResult<_>>()?;

        Ok(Box::new(partitions.into_iter()))
    }
}

impl<V: ValueType> Ungroupable for OperandHandle<Bare<V>, Definite> {
    type Ungrouped<K: GroupKey> = OperandHandle<Bare<V>, Multiple<Unordered>>;

    fn flatten_estimate(estimate: Estimate) -> Estimate {
        Estimate {
            elements: estimate.elements,
            distinct: estimate.elements,
            selectivity: estimate
                .per_group
                .as_deref()
                .and_then(|inner| inner.selectivity),
            per_group: None,
        }
    }

    fn flatten<'a, K: GroupKey>(
        grouped: GroupedIterator<'a, K::Key<'a>, QueryResult<Self::ReturnValue<'a>>>,
    ) -> QueryResult<<Self::Ungrouped<K> as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        let partitions: Vec<_> = grouped
            .map(|(_key, partition)| partition)
            .collect::<QueryResult<_>>()?;

        Ok(Box::new(partitions.into_iter()))
    }
}

impl<Inner: Ungroupable, KInner: GroupKey> Ungroupable for GroupOperand<Inner, KInner> {
    type Ungrouped<KOuter: GroupKey> = GroupOperand<Inner::Ungrouped<KInner>, KOuter>;

    fn flatten_estimate(estimate: Estimate) -> Estimate {
        Estimate {
            per_group: estimate
                .per_group
                .map(|inner| Box::new(Inner::flatten_estimate(*inner))),
            ..estimate
        }
    }

    fn flatten<'a, KOuter: GroupKey>(
        grouped: GroupedIterator<'a, KOuter::Key<'a>, QueryResult<Self::ReturnValue<'a>>>,
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

impl<O: Ungroupable, K: GroupKey> Estimated for UngroupContext<O, K> {
    fn estimate(&self, stats: &Stats) -> Estimate {
        O::flatten_estimate(self.group.context().estimate(stats))
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
