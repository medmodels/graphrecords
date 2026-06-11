use crate::{
    EvaluateOperand, Explain, IndexDomain, Indexed, Labeled, Multiple, Operand, QueryResult,
    ValueType,
    execution::EvaluationCache,
    operands::{GroupOperand, OperandHandle, try_partition_by},
    operations::{
        Apply, Kernel, KeyOperand, KeyedStream, OnMissing, Operation, OperationContext, Prepare,
    },
    optimizer::{
        Cardinality, EstimateCost, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats,
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
    type Prepared<'a> = <K as Prepare>::Prepared<'a>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Prepare::prepare(&self.key, graphrecord, cache)
    }
}

fn resolve_key<'a, K>(
    prepared: &<K as Prepare>::Prepared<'a>,
    index: &<K::Subject as IndexDomain>::Index<'a>,
    label: &'static str,
) -> QueryResult<Option<K::Key>>
where
    K: KeyOperand,
{
    K::resolve(prepared, index, label, OnMissing::Raise)
}

impl<I, V, K> Kernel<Indexed<I, V>, Multiple> for GroupByOperation<K>
where
    I: IndexDomain,
    V: ValueType<Cost = Cardinality>,
    K: KeyOperand<Subject = I>,
{
    type Output = GroupOperand<OperandHandle<Indexed<I, V>, Multiple>, K>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple>,
        keys: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let label = <Self as Labeled>::LABEL;

        let groups = try_partition_by(values, move |(index, _)| {
            resolve_key::<K>(&keys, index, label)
        })?;

        Ok(Box::new(
            groups.map(|(key, partition)| (key, Ok(partition))),
        ))
    }
}

impl<I, V, K> EstimateCost<GroupByOperation<K>> for OperandHandle<Indexed<I, V>, Multiple>
where
    I: IndexDomain,
    V: ValueType<Cost = Cardinality>,
    K: KeyOperand<Subject = I>,
{
    type OutputCost = <GroupOperand<Self, K> as Operand>::Cost;

    fn estimate(
        _operation: &GroupByOperation<K>,
        input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        input_cost
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
