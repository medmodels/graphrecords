use crate::{
    Arity, Bare, Definite, ElementShape, EvaluateExpression, Explain, IndexDomain, Indexed,
    Labeled, Multiple, QueryResult, Single, Unordered,
    expressions::{ExpressionHandle, FailuresExpression, Partition},
    operations::{BucketFailureArity, Build, GroupKernel, Operation, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    registry::operation_manifest,
    traits::{BucketErrors, KeyErrors},
};
use graphrecords_core::GraphRecord;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Group)]
#[explain(label = "BucketErrors")]
#[plan(optimizer_hints(empty = if_any))]
pub struct BucketErrorsOperation;

impl<M: IndexDomain, K: IndexDomain, S: ElementShape, B: BucketFailureArity<S>>
    GroupKernel<M, K, ExpressionHandle<S, B>> for BucketErrorsOperation
{
    type Output = FailuresExpression<K, Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, ExpressionHandle<S, B>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let elements: Vec<_> = partition
            .buckets()
            .iter()
            .filter_map(|bucket| {
                B::bucket_failure(bucket.payload()).map(|failure| {
                    let index = K::resolve(graphrecord, bucket.key(), Self::LABEL)?;

                    Ok((index, Ok(failure.clone())))
                })
            })
            .collect::<QueryResult<_>>()?;

        Ok(Box::new(elements.into_iter()))
    }
}

impl<E: Build<BucketErrorsOperation>> BucketErrors for E {
    type Output = E::Output;

    fn bucket_errors(&self) -> Self::Output {
        self.build(BucketErrorsOperation)
    }
}

pub(super) mod bucket_errors {
    use super::{
        Bare, BucketErrors, BucketErrorsOperation, Definite, ExpressionHandle, FailuresExpression,
        Indexed, Multiple, Single, Unordered, operation_manifest,
    };

    operation_manifest! {
        BucketErrorsOperation {
            method: BucketErrors::bucket_errors;
            scope: group;

            kernel {
                group: <M: IndexDomain, K: IndexDomain>;
                parameters: <I: IndexDomain, V: ValueDomain, O: OrderState>;
                input: ExpressionHandle<Indexed<I, V>, Multiple<O>>;
                output: FailuresExpression<K, Unordered>;
            }

            kernel {
                group: <M: IndexDomain, K: IndexDomain>;
                parameters: <I: IndexDomain, V: ValueDomain>;
                input: ExpressionHandle<Indexed<I, V>, Single>;
                output: FailuresExpression<K, Unordered>;
            }

            kernel {
                group: <M: IndexDomain, K: IndexDomain>;
                parameters: <I: IndexDomain, V: ValueDomain>;
                input: ExpressionHandle<Indexed<I, V>, Definite>;
                output: FailuresExpression<K, Unordered>;
            }

            kernel {
                group: <M: IndexDomain, K: IndexDomain>;
                parameters: <V: BareValueDomain, O: OrderState>;
                input: ExpressionHandle<Bare<V>, Multiple<O>>;
                output: FailuresExpression<K, Unordered>;
            }

            kernel {
                group: <M: IndexDomain, K: IndexDomain>;
                parameters: <V: BareValueDomain>;
                input: ExpressionHandle<Bare<V>, Single>;
                output: FailuresExpression<K, Unordered>;
            }

            kernel {
                group: <M: IndexDomain, K: IndexDomain>;
                parameters: <V: BareValueDomain>;
                input: ExpressionHandle<Bare<V>, Definite>;
                output: FailuresExpression<K, Unordered>;
            }
        }
    }
}

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Group)]
#[explain(label = "KeyErrors")]
#[plan(optimizer_hints(empty = if_any))]
pub struct KeyErrorsOperation;

impl<M: IndexDomain, K: IndexDomain, S: ElementShape, C: Arity>
    GroupKernel<M, K, ExpressionHandle<S, C>> for KeyErrorsOperation
{
    type Output = FailuresExpression<M, Unordered>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, ExpressionHandle<S, C>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let key_failures = partition.into_parts().1;

        Ok(Box::new(
            key_failures
                .into_iter()
                .map(|(member, failure)| (member, Ok(*failure))),
        ))
    }
}

impl<E: Build<KeyErrorsOperation>> KeyErrors for E {
    type Output = E::Output;

    fn key_errors(&self) -> Self::Output {
        self.build(KeyErrorsOperation)
    }
}

pub(super) mod key_errors {
    use super::{
        ExpressionHandle, FailuresExpression, KeyErrors, KeyErrorsOperation, Unordered,
        operation_manifest,
    };

    operation_manifest! {
        KeyErrorsOperation {
            method: KeyErrors::key_errors;
            scope: group;

            kernel {
                group: <M: IndexDomain, K: IndexDomain>;
                parameters: <S: ElementShape, C: Arity>;
                input: ExpressionHandle<S, C>;
                output: FailuresExpression<M, Unordered>;
            }
        }
    }
}
