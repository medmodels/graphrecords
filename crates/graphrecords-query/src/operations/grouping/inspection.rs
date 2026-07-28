use crate::{
    Arity, ElementShape, EvaluateOperand, Explain, IndexDomain, Operand, QueryResult, Unordered,
    execution::EvaluationCache,
    operands::{FailuresOperand, OperandHandle, Partition},
    operations::{
        Apply, BucketFailureArity, GroupKernel, GroupKey, Operation, OperationContext, Prepare,
    },
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    traits::{BucketErrors, KeyErrors},
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Group)]
#[explain(label = "BucketErrors")]
pub struct BucketErrorsOperation;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Group)]
#[explain(label = "KeyErrors")]
#[plan(optimizer_hints(empty = if_any))]
pub struct KeyErrorsOperation;

impl Prepare for BucketErrorsOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl Prepare for KeyErrorsOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<M: IndexDomain, K: GroupKey, S: ElementShape, C: BucketFailureArity<S>>
    GroupKernel<M, K, OperandHandle<S, C>> for BucketErrorsOperation
{
    type Output = FailuresOperand<K, Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, OperandHandle<S, C>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let elements: Vec<_> = partition
            .buckets()
            .filter_map(|bucket| {
                C::bucket_failure(bucket.payload()).map(|failure| {
                    let index = K::resolve_key(graphrecord, bucket.key())?;

                    Ok((index, Ok(failure.clone())))
                })
            })
            .collect::<QueryResult<_>>()?;

        Ok(Box::new(elements.into_iter()))
    }
}

impl<M: IndexDomain, K: GroupKey, S: ElementShape, C: Arity> GroupKernel<M, K, OperandHandle<S, C>>
    for KeyErrorsOperation
{
    type Output = FailuresOperand<M, Unordered>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, OperandHandle<S, C>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let key_failures = partition.into_parts().1;

        Ok(Box::new(
            key_failures
                .into_iter()
                .map(|(member, failure)| (member, Ok(*failure))),
        ))
    }
}

impl<O: Apply<BucketErrorsOperation>> BucketErrors for O {
    type ReturnOperand = O::Output;

    fn bucket_errors(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), BucketErrorsOperation))
    }
}

impl<O: Apply<KeyErrorsOperation>> KeyErrors for O {
    type ReturnOperand = O::Output;

    fn key_errors(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), KeyErrorsOperation))
    }
}
