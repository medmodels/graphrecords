use super::IncomparableIndices;
use crate::{
    Bare, EvaluateOperand, Explain, Failure, IncomparableValues, IndexDomain, Indexed, Labeled,
    Multiple, Operand, Ordered, QueryResult, ValueType,
    execution::EvaluationCache,
    operands::OperandHandle,
    operations::{Apply, BareStream, Kernel, KeyedStream, Operation, OperationContext, Prepare},
    optimizer::{EstimateCost, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Sort,
};
use graphrecords_core::GraphRecord;
use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
};

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "Sort")]
pub struct SortOperation;

impl Prepare for SortOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I: IndexDomain, V: ValueType> EstimateCost<SortOperation>
    for OperandHandle<Indexed<I, V>, Multiple>
{
    type OutputCost = <Self as Operand>::Cost;

    fn estimate(
        _operation: &SortOperation,
        input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        input_cost
    }
}

impl<V: ValueType> EstimateCost<SortOperation> for OperandHandle<Bare<V>, Multiple> {
    type OutputCost = <Self as Operand>::Cost;

    fn estimate(
        _operation: &SortOperation,
        input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        input_cost
    }
}

impl<I, V> Kernel<Indexed<I, V>, Multiple> for SortOperation
where
    I: IndexDomain,
    V: ValueType,
    for<'a> V::Value<'a>: PartialOrd + Display + Debug + Send + Sync,
{
    type Output = OperandHandle<Ordered<Indexed<I, V>>, Multiple>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let mut collected: Vec<_> = values
            .map(|(index, result)| result.map(|value| (index, value)))
            .collect::<QueryResult<_>>()?;

        let mut incomparable_values = None;
        let mut incomparable_indices = None;

        collected.sort_by(|(left_index, left), (right_index, right)| {
            let Some(ordering) = left.partial_cmp(right) else {
                if incomparable_values.is_none() {
                    incomparable_values = Some((left.clone(), right.clone()));
                }

                return Ordering::Equal;
            };

            if ordering != Ordering::Equal {
                return ordering;
            }

            let Some(index_ordering) = left_index.partial_cmp(right_index) else {
                if incomparable_indices.is_none() {
                    incomparable_indices = Some((
                        left.clone(),
                        left_index.to_string(),
                        right_index.to_string(),
                    ));
                }

                return Ordering::Equal;
            };

            index_ordering
        });

        if let Some((first, second)) = incomparable_values {
            return Err(Failure::new(Self::LABEL, IncomparableValues { first: first.to_string(), second: second.to_string() }).help(
                "narrow the values down first using is_string(), is_int(), is_float(), is_bool(), is_datetime() or is_duration()",
            ));
        }

        if let Some((value, first, second)) = incomparable_indices {
            return Err(Failure::new(
                Self::LABEL,
                IncomparableIndices {
                    value: value.to_string(),
                    first,
                    second,
                },
            )
            .help(
                "to order them deterministically, sort by a key that distinguishes these elements",
            ));
        }

        Ok(Box::new(
            collected
                .into_iter()
                .map(|(index, value)| (index, Ok(value))),
        ))
    }
}

impl<V> Kernel<Bare<V>, Multiple> for SortOperation
where
    V: ValueType,
    for<'a> V::Value<'a>: PartialOrd + Display + Debug + Send + Sync,
{
    type Output = OperandHandle<Ordered<Bare<V>>, Multiple>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let mut collected: Vec<_> = values.collect::<QueryResult<_>>()?;

        let mut incomparable = None;

        collected.sort_by(|left, right| {
            let Some(ordering) = left.partial_cmp(right) else {
                if incomparable.is_none() {
                    incomparable = Some((left.clone(), right.clone()));
                }

                return Ordering::Equal;
            };

            ordering
        });

        if let Some((first, second)) = incomparable {
            return Err(Failure::new(Self::LABEL, IncomparableValues { first: first.to_string(), second: second.to_string() }).help(
                "narrow the values down first using is_string(), is_int(), is_float(), is_bool(), is_datetime() or is_duration()",
            ));
        }

        Ok(Box::new(collected.into_iter().map(Ok)))
    }
}

impl<S> Sort for S
where
    S: Apply<SortOperation>,
{
    type ReturnOperand = <S as Apply<SortOperation>>::Output;

    fn sort(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), SortOperation))
    }
}
