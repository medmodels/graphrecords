use crate::{
    Arity, Bare, ElementShape, Explain, IndexDomain, Indexed, Mask, Not, QueryResult, Series,
    element::{Pipeline, Preserving},
    expressions::ExpressionHandle,
    operations::{Build, ElementKernel, ElementPipeline, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
};
use graphrecords_core::GraphRecord;
use std::ops::Not as BitNot;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Element)]
#[explain(label = "Not")]
#[plan(optimizer_hints(commutes_with_filter, allows_limit_pushdown, empty = if_any))]
pub struct NotOperation;

impl<I: IndexDomain> ElementKernel<Indexed<I, Mask>> for NotOperation {
    type Emission = Preserving;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, Mask>, Self>> {
        Ok(Pipeline::unkeyed(|value: QueryResult<bool>| {
            value.map(|value| !value)
        }))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            selectivity: input.selectivity.map(|selectivity| 1.0 - selectivity),
            ..input
        }
    }
}

impl ElementKernel<Bare<Mask>> for NotOperation {
    type Emission = Preserving;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<Mask>, Self>> {
        Ok(Pipeline::new(|value: QueryResult<bool>| {
            value.map(|value| !value)
        }))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            selectivity: input.selectivity.map(|selectivity| 1.0 - selectivity),
            ..input
        }
    }
}

impl<E: Build<NotOperation>> Not for E {
    type Output = E::Output;

    fn not(&self) -> Self::Output {
        self.build(NotOperation)
    }
}

impl<S, C> BitNot for ExpressionHandle<S, C>
where
    S: ElementShape,
    C: Arity,
    Self: Not,
{
    type Output = <Self as Not>::Output;

    fn not(self) -> Self::Output {
        <Self as Not>::not(&self)
    }
}

impl<E> BitNot for Series<E>
where
    Self: Not,
{
    type Output = <Self as Not>::Output;

    fn not(self) -> Self::Output {
        <Self as Not>::not(&self)
    }
}

operation_manifest! {
    NotOperation {
        method: Not::not;
        scope: element;

        kernel {
            parameters: <I: IndexDomain>;
            input: Indexed<I, Mask>;
            output: Indexed<I, Mask>;
            emission: Preserving;
        }

        kernel {
            parameters: <>;
            input: Bare<Mask>;
            output: Bare<Mask>;
            emission: Preserving;
        }
    }
}
