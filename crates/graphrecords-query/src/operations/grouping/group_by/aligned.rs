use crate::{
    EvaluateOperand, Explain, IndexDomain, Indexed, Labeled, Multiple, Operand, OrderState,
    QueryResult, ValueType,
    execution::EvaluationCache,
    operands::{GroupOperand, OperandHandle, try_partition_by},
    operations::{
        Apply, ArgumentSource, Kernel, KeyOperand, Keyed, KeyedStream, Operation, OperationContext,
        Prepare, Retention,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::GroupBy,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "GroupBy")]
pub struct GroupByOperation<K: KeyOperand> {
    #[argument]
    pub key: K,
}

impl<K: KeyOperand> Prepare for GroupByOperation<K> {
    type Prepared<'a> = K::Prepared<'a>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.key.prepare(graphrecord, cache)
    }
}

impl<I, V, K, O> Kernel<Indexed<I, V>, Multiple<O>> for GroupByOperation<K>
where
    I: IndexDomain,
    V: ValueType,
    O: OrderState,
    for<'a> K: KeyOperand<Subject = I> + ArgumentSource<Keyed<I>, Value<'a> = K::Key<'a>>,
{
    type Output = GroupOperand<OperandHandle<Indexed<I, V>, Multiple<O>>, K>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        keys: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let label = Self::LABEL;

        let groups = try_partition_by(values, move |(index, _)| {
            let step = K::resolve(&keys, index, label);

            <<K as ArgumentSource<Keyed<I>>>::Retention as Retention>::collapse(step).transpose()
        })?;

        Ok(Box::new(
            groups.map(|(key, partition)| (key, Ok(partition))),
        ))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        let (Some(elements), Some(keys)) = (input.elements, self.key.estimate(stats).distinct)
        else {
            return Estimate {
                per_group: Some(Box::new(Estimate::UNKNOWN)),
                ..Estimate::UNKNOWN
            };
        };

        let groups = keys.min(elements);
        let per_group_elements = if groups == 0 {
            0
        } else {
            elements.div_ceil(groups)
        };

        Estimate {
            elements: Some(groups),
            distinct: Some(groups),
            selectivity: None,
            per_group: Some(Box::new(Estimate {
                elements: Some(per_group_elements),
                distinct: input
                    .distinct
                    .map(|distinct| distinct.min(per_group_elements)),
                selectivity: input.selectivity,
                per_group: None,
            })),
        }
    }
}

impl<O, K> GroupBy<K> for O
where
    O: Apply<GroupByOperation<K>>,
    K: KeyOperand,
{
    type Output = <O as Apply<GroupByOperation<K>>>::Output;

    fn group_by(&self, key: K) -> Self::Output {
        Self::Output::new(OperationContext::new(
            self.clone(),
            GroupByOperation { key },
        ))
    }
}
