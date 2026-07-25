use crate::{
    Bare, Definite, EvaluateOperand, Explain, IndexDomain, Indexed, Multiple, Operand, OrderState,
    QueryResult, Single, Unordered, ValueType,
    execution::EvaluationCache,
    operands::{GroupOperand, OperandHandle},
    operations::{Apply, KeyOperand, Operation, OperationContext, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Ungroup,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "Ungroup")]
pub struct UngroupOperation;

fn multiple_estimate(input: &Estimate) -> Estimate {
    let per_group = input.per_group.as_deref();
    let elements = match (input.elements, per_group.and_then(|inner| inner.elements)) {
        (Some(groups), Some(elements_per_group)) => Some(groups * elements_per_group),
        _ => None,
    };
    let distinct = match (input.elements, per_group.and_then(|inner| inner.distinct)) {
        (Some(groups), Some(distinct_per_group)) => Some(groups * distinct_per_group),
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

fn single_estimate(input: &Estimate) -> Estimate {
    Estimate {
        elements: input.elements,
        distinct: input.elements,
        selectivity: input
            .per_group
            .as_deref()
            .and_then(|inner| inner.selectivity),
        per_group: None,
    }
}

impl Prepare for UngroupOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I, V, O, K> Apply<UngroupOperation>
    for GroupOperand<OperandHandle<Indexed<I, V>, Multiple<O>>, K>
where
    I: IndexDomain,
    V: ValueType,
    O: OrderState,
    K: KeyOperand,
{
    type Output = OperandHandle<Indexed<I, V>, Multiple<Unordered>>;

    fn apply<'a>(
        _graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        _prepared: <UngroupOperation as Prepare>::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        let partitions: Vec<_> = values
            .map(|(_key, partition)| partition)
            .collect::<QueryResult<_>>()?;

        Ok(Box::new(partitions.into_iter().flatten()))
    }

    fn estimate(_operation: &UngroupOperation, input: Estimate, _stats: &Stats) -> Estimate {
        multiple_estimate(&input)
    }
}

impl<V, O, K> Apply<UngroupOperation> for GroupOperand<OperandHandle<Bare<V>, Multiple<O>>, K>
where
    V: ValueType,
    O: OrderState,
    K: KeyOperand,
{
    type Output = OperandHandle<Bare<V>, Multiple<Unordered>>;

    fn apply<'a>(
        _graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        _prepared: <UngroupOperation as Prepare>::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        let partitions: Vec<_> = values
            .map(|(_key, partition)| partition)
            .collect::<QueryResult<_>>()?;

        Ok(Box::new(partitions.into_iter().flatten()))
    }

    fn estimate(_operation: &UngroupOperation, input: Estimate, _stats: &Stats) -> Estimate {
        multiple_estimate(&input)
    }
}

impl<I, V, K> Apply<UngroupOperation> for GroupOperand<OperandHandle<Indexed<I, V>, Single>, K>
where
    I: IndexDomain,
    V: ValueType,
    K: KeyOperand,
{
    type Output = OperandHandle<Indexed<I, V>, Multiple<Unordered>>;

    fn apply<'a>(
        _graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        _prepared: <UngroupOperation as Prepare>::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        let values: Vec<_> = values
            .map(|(_key, value)| value)
            .collect::<QueryResult<_>>()?;

        Ok(Box::new(values.into_iter().flatten()))
    }

    fn estimate(_operation: &UngroupOperation, input: Estimate, _stats: &Stats) -> Estimate {
        single_estimate(&input)
    }
}

impl<V, K> Apply<UngroupOperation> for GroupOperand<OperandHandle<Bare<V>, Single>, K>
where
    V: ValueType,
    K: KeyOperand,
{
    type Output = OperandHandle<Bare<V>, Multiple<Unordered>>;

    fn apply<'a>(
        _graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        _prepared: <UngroupOperation as Prepare>::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        let values: Vec<_> = values
            .map(|(_key, value)| value)
            .collect::<QueryResult<_>>()?;

        Ok(Box::new(values.into_iter().flatten()))
    }

    fn estimate(_operation: &UngroupOperation, input: Estimate, _stats: &Stats) -> Estimate {
        single_estimate(&input)
    }
}

impl<I, V, K> Apply<UngroupOperation> for GroupOperand<OperandHandle<Indexed<I, V>, Definite>, K>
where
    I: IndexDomain,
    V: ValueType,
    K: KeyOperand,
{
    type Output = OperandHandle<Indexed<I, V>, Multiple<Unordered>>;

    fn apply<'a>(
        _graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        _prepared: <UngroupOperation as Prepare>::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        let values: Vec<_> = values
            .map(|(_key, value)| value)
            .collect::<QueryResult<_>>()?;

        Ok(Box::new(values.into_iter()))
    }

    fn estimate(_operation: &UngroupOperation, input: Estimate, _stats: &Stats) -> Estimate {
        single_estimate(&input)
    }
}

impl<V, K> Apply<UngroupOperation> for GroupOperand<OperandHandle<Bare<V>, Definite>, K>
where
    V: ValueType,
    K: KeyOperand,
{
    type Output = OperandHandle<Bare<V>, Multiple<Unordered>>;

    fn apply<'a>(
        _graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        _prepared: <UngroupOperation as Prepare>::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        let values: Vec<_> = values
            .map(|(_key, value)| value)
            .collect::<QueryResult<_>>()?;

        Ok(Box::new(values.into_iter()))
    }

    fn estimate(_operation: &UngroupOperation, input: Estimate, _stats: &Stats) -> Estimate {
        single_estimate(&input)
    }
}

impl<O> Ungroup for O
where
    O: Apply<UngroupOperation>,
{
    type ReturnOperand = <O as Apply<UngroupOperation>>::Output;

    fn ungroup(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), UngroupOperation))
    }
}
