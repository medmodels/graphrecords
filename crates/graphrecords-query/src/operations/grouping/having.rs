use crate::{
    Bare, Definite, EvaluateOperand, Explain, IndexDomain, Indexed, Labeled, Multiple, Operand,
    OrderState, QueryResult, Single, ValueType,
    execution::EvaluationCache,
    operands::{GroupOperand, GroupedIterator, OperandHandle},
    operations::{
        Apply, ArgumentSource, KeyOperand, Keyed, Operation, OperationContext, Prepare, Retention,
    },
    optimizer::{
        Estimate, Estimated, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats,
    },
    traits::Having,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "Having")]
#[plan(optimizer_hints(empty = if_all))]
pub struct HavingOperation<P> {
    #[argument]
    predicate: P,
}

impl<P: Prepare> Prepare for HavingOperation<P> {
    type Prepared<'a>
        = P::Prepared<'a>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.predicate.prepare(graphrecord, cache)
    }
}

fn filter_groups<'a, D, P, T>(
    groups: GroupedIterator<'a, D::Index<'a>, QueryResult<T>>,
    prepared: P::Prepared<'a>,
) -> GroupedIterator<'a, D::Index<'a>, QueryResult<T>>
where
    D: IndexDomain,
    P: ArgumentSource<Keyed<D>, Value<'a> = bool> + 'a,
    T: 'a,
{
    let label = HavingOperation::<P>::LABEL;

    Box::new(groups.filter_map(move |(key, partition)| {
        let partition = match partition {
            Ok(partition) => partition,
            Err(failure) => return Some((key, Err(failure))),
        };
        let step = P::resolve(&prepared, &key, label);

        match <P::Retention as Retention>::collapse(step) {
            Some(Ok(true)) => Some((key, Ok(partition))),
            Some(Ok(false)) | None => None,
            Some(Err(failure)) => Some((key, Err(failure))),
        }
    }))
}

fn having_estimate<P: Estimated>(
    operation: &HavingOperation<P>,
    input: Estimate,
    stats: &Stats,
) -> Estimate {
    let predicate_estimate = operation.predicate.estimate(stats);
    let selectivity = predicate_estimate
        .per_group
        .as_deref()
        .and_then(|inner| inner.selectivity)
        .or(predicate_estimate.selectivity);
    let Some(selectivity) = selectivity else {
        return input;
    };
    let per_group = input.per_group.clone();
    let mut estimate = input.scaled(selectivity);
    estimate.per_group = per_group;

    estimate
}

impl<I, V, O, K, P> Apply<HavingOperation<P>>
    for GroupOperand<OperandHandle<Indexed<I, V>, Multiple<O>>, K>
where
    I: IndexDomain,
    V: ValueType,
    O: OrderState,
    K: KeyOperand,
    for<'a> P: ArgumentSource<Keyed<K::Key>, Value<'a> = bool>,
{
    type Output = Self;

    fn apply<'a>(
        _graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        prepared: <HavingOperation<P> as Prepare>::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        Ok(filter_groups::<K::Key, P, _>(values, prepared))
    }

    fn estimate(operation: &HavingOperation<P>, input: Estimate, stats: &Stats) -> Estimate {
        having_estimate(operation, input, stats)
    }
}

impl<V, O, K, P> Apply<HavingOperation<P>> for GroupOperand<OperandHandle<Bare<V>, Multiple<O>>, K>
where
    V: ValueType,
    O: OrderState,
    K: KeyOperand,
    for<'a> P: ArgumentSource<Keyed<K::Key>, Value<'a> = bool>,
{
    type Output = Self;

    fn apply<'a>(
        _graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        prepared: <HavingOperation<P> as Prepare>::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        Ok(filter_groups::<K::Key, P, _>(values, prepared))
    }

    fn estimate(operation: &HavingOperation<P>, input: Estimate, stats: &Stats) -> Estimate {
        having_estimate(operation, input, stats)
    }
}

impl<I, V, K, P> Apply<HavingOperation<P>> for GroupOperand<OperandHandle<Indexed<I, V>, Single>, K>
where
    I: IndexDomain,
    V: ValueType,
    K: KeyOperand,
    for<'a> P: ArgumentSource<Keyed<K::Key>, Value<'a> = bool>,
{
    type Output = Self;

    fn apply<'a>(
        _graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        prepared: <HavingOperation<P> as Prepare>::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        Ok(filter_groups::<K::Key, P, _>(values, prepared))
    }

    fn estimate(operation: &HavingOperation<P>, input: Estimate, stats: &Stats) -> Estimate {
        having_estimate(operation, input, stats)
    }
}

impl<V, K, P> Apply<HavingOperation<P>> for GroupOperand<OperandHandle<Bare<V>, Single>, K>
where
    V: ValueType,
    K: KeyOperand,
    for<'a> P: ArgumentSource<Keyed<K::Key>, Value<'a> = bool>,
{
    type Output = Self;

    fn apply<'a>(
        _graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        prepared: <HavingOperation<P> as Prepare>::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        Ok(filter_groups::<K::Key, P, _>(values, prepared))
    }

    fn estimate(operation: &HavingOperation<P>, input: Estimate, stats: &Stats) -> Estimate {
        having_estimate(operation, input, stats)
    }
}

impl<I, V, K, P> Apply<HavingOperation<P>>
    for GroupOperand<OperandHandle<Indexed<I, V>, Definite>, K>
where
    I: IndexDomain,
    V: ValueType,
    K: KeyOperand,
    for<'a> P: ArgumentSource<Keyed<K::Key>, Value<'a> = bool>,
{
    type Output = Self;

    fn apply<'a>(
        _graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        prepared: <HavingOperation<P> as Prepare>::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        Ok(filter_groups::<K::Key, P, _>(values, prepared))
    }

    fn estimate(operation: &HavingOperation<P>, input: Estimate, stats: &Stats) -> Estimate {
        having_estimate(operation, input, stats)
    }
}

impl<V, K, P> Apply<HavingOperation<P>> for GroupOperand<OperandHandle<Bare<V>, Definite>, K>
where
    V: ValueType,
    K: KeyOperand,
    for<'a> P: ArgumentSource<Keyed<K::Key>, Value<'a> = bool>,
{
    type Output = Self;

    fn apply<'a>(
        _graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        prepared: <HavingOperation<P> as Prepare>::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        Ok(filter_groups::<K::Key, P, _>(values, prepared))
    }

    fn estimate(operation: &HavingOperation<P>, input: Estimate, stats: &Stats) -> Estimate {
        having_estimate(operation, input, stats)
    }
}

impl<O, P> Having<P> for O
where
    O: Apply<HavingOperation<P>>,
    HavingOperation<P>: Operation,
{
    type ReturnOperand = <O as Apply<HavingOperation<P>>>::Output;

    fn having(&self, predicate: P) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            HavingOperation { predicate },
        ))
    }
}
