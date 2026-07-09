use crate::{
    AttributeName, AttributeSet, EvaluateOperand, Explain, IndexDomain, IndexValue, Indexed,
    Labeled, Multiple, Operand, OrderState, QueryResult, Scalar, Unit, ValueType,
    execution::EvaluationCache,
    operands::{GroupOperand, OperandHandle, try_partition_by},
    operations::{
        Apply, ArgumentSource, Kernel, KeyOperand, Keyed, KeyedStream, OnMissing, Operation,
        OperationContext, Prepare,
    },
    optimizer::{
        EstimateCost, GroupCost, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats,
        ValueCost,
    },
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
            K::resolve(&keys, index, label, OnMissing::Raise)
        })?;

        Ok(Box::new(
            groups.map(|(key, partition)| (key, Ok(partition))),
        ))
    }
}

impl<I, K, O> EstimateCost<GroupByOperation<K>> for OperandHandle<Indexed<I, Unit>, Multiple<O>>
where
    I: IndexDomain,
    K: KeyOperand<Subject = I>,
    O: OrderState,
{
    type OutputCost = <GroupOperand<Self, K> as Operand>::Cost;

    fn estimate(
        operation: &GroupByOperation<K>,
        input_cost: <Self as Operand>::Cost,
        stats: &Stats,
    ) -> Self::OutputCost {
        let (groups, per_group) = input_cost.split(operation.key.distinct_count(stats));

        GroupCost { groups, per_group }
    }
}

impl<I, K, O> EstimateCost<GroupByOperation<K>> for OperandHandle<Indexed<I, Scalar>, Multiple<O>>
where
    I: IndexDomain,
    K: KeyOperand<Subject = I>,
    O: OrderState,
{
    type OutputCost = <GroupOperand<Self, K> as Operand>::Cost;

    fn estimate(
        operation: &GroupByOperation<K>,
        input_cost: <Self as Operand>::Cost,
        stats: &Stats,
    ) -> Self::OutputCost {
        let (groups, rows) = input_cost.rows().split(operation.key.distinct_count(stats));

        GroupCost {
            groups,
            per_group: ValueCost::new(rows, input_cost.distinct()),
        }
    }
}

impl<I, K, O> EstimateCost<GroupByOperation<K>>
    for OperandHandle<Indexed<I, AttributeName>, Multiple<O>>
where
    I: IndexDomain,
    K: KeyOperand<Subject = I>,
    O: OrderState,
{
    type OutputCost = <GroupOperand<Self, K> as Operand>::Cost;

    fn estimate(
        operation: &GroupByOperation<K>,
        input_cost: <Self as Operand>::Cost,
        stats: &Stats,
    ) -> Self::OutputCost {
        let (groups, rows) = input_cost.rows().split(operation.key.distinct_count(stats));

        GroupCost {
            groups,
            per_group: ValueCost::new(rows, input_cost.distinct()),
        }
    }
}

impl<I, K, O> EstimateCost<GroupByOperation<K>>
    for OperandHandle<Indexed<I, AttributeSet>, Multiple<O>>
where
    I: IndexDomain,
    K: KeyOperand<Subject = I>,
    O: OrderState,
{
    type OutputCost = <GroupOperand<Self, K> as Operand>::Cost;

    fn estimate(
        operation: &GroupByOperation<K>,
        input_cost: <Self as Operand>::Cost,
        stats: &Stats,
    ) -> Self::OutputCost {
        let (groups, per_group) = input_cost.split(operation.key.distinct_count(stats));

        GroupCost { groups, per_group }
    }
}

impl<I, E, K, O> EstimateCost<GroupByOperation<K>>
    for OperandHandle<Indexed<I, IndexValue<E>>, Multiple<O>>
where
    I: IndexDomain,
    E: IndexDomain,
    K: KeyOperand<Subject = I>,
    O: OrderState,
{
    type OutputCost = <GroupOperand<Self, K> as Operand>::Cost;

    fn estimate(
        operation: &GroupByOperation<K>,
        input_cost: <Self as Operand>::Cost,
        stats: &Stats,
    ) -> Self::OutputCost {
        let (groups, rows) = input_cost.rows().split(operation.key.distinct_count(stats));

        GroupCost {
            groups,
            per_group: ValueCost::new(rows, input_cost.distinct()),
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
