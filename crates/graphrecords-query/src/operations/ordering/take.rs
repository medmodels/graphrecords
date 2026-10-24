use super::{fraction_elements, fraction_estimate};
use crate::{
    Bare, BareValueDomain, EvaluateExpression, Explain, IndexDomain, Indexed, Multiple, Ordered,
    QueryResult, ValueDomain,
    execution::EvaluationCache,
    expressions::ExpressionHandle,
    operations::{BareStream, Build, KeyedStream, LaneKernel, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::{Take, TakeFraction},
};
use graphrecords_core::GraphRecord;
use std::hash::Hasher;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "Take")]
#[plan(optimizer_hints(empty = if_any))]
pub struct TakeOperation {
    #[explain(label)]
    elements: usize,
}

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "TakeFraction")]
#[plan(optimizer_hints(empty = if_any))]
pub struct TakeFractionOperation {
    #[explain(label)]
    fraction: f64,
}

impl PlanIdentity for TakeFractionOperation {
    fn identity_eq(&self, other: &Self) -> bool {
        self.fraction.identity_eq(&other.fraction)
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        self.fraction.identity_hash(state);
    }
}

impl Prepare for TakeOperation {
    type Prepared<'a> = usize;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self.elements)
    }
}

impl Prepare for TakeFractionOperation {
    type Prepared<'a> = f64;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self.fraction)
    }
}

impl<I: IndexDomain, V: ValueDomain> LaneKernel<Indexed<I, V>, Multiple<Ordered>>
    for TakeOperation
{
    type Output = ExpressionHandle<Indexed<I, V>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<Ordered>>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        Ok(Box::new(values.take(prepared)))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            elements: input.elements.map(|elements| elements.min(self.elements)),
            distinct: input.distinct.map(|distinct| distinct.min(self.elements)),
            selectivity: None,
            ..input
        }
    }
}

impl<V: BareValueDomain> LaneKernel<Bare<V>, Multiple<Ordered>> for TakeOperation {
    type Output = ExpressionHandle<Bare<V>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<Ordered>>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        Ok(Box::new(values.take(prepared)))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            elements: input.elements.map(|elements| elements.min(self.elements)),
            distinct: input.distinct.map(|distinct| distinct.min(self.elements)),
            selectivity: None,
            ..input
        }
    }
}

impl<I: IndexDomain, V: ValueDomain> LaneKernel<Indexed<I, V>, Multiple<Ordered>>
    for TakeFractionOperation
{
    type Output = ExpressionHandle<Indexed<I, V>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<Ordered>>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let values: Vec<_> = values.collect();
        let elements = fraction_elements(prepared, values.len());

        Ok(Box::new(values.into_iter().take(elements)))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        fraction_estimate(self.fraction, input)
    }
}

impl<V: BareValueDomain> LaneKernel<Bare<V>, Multiple<Ordered>> for TakeFractionOperation {
    type Output = ExpressionHandle<Bare<V>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<Ordered>>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let values: Vec<_> = values.collect();
        let elements = fraction_elements(prepared, values.len());

        Ok(Box::new(values.into_iter().take(elements)))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        fraction_estimate(self.fraction, input)
    }
}

impl<E: Build<TakeOperation>> Take for E {
    type Output = E::Output;

    fn take(&self, elements: usize) -> Self::Output {
        self.build(TakeOperation { elements })
    }
}

impl<E: Build<TakeFractionOperation>> TakeFraction for E {
    type Output = E::Output;

    fn take_fraction(&self, fraction: f64) -> Self::Output {
        self.build(TakeFractionOperation { fraction })
    }
}

operation_manifest! {
    TakeOperation {
        method: Take::take;
        scope: lane;

        kernel {
            parameters: <I: IndexDomain, V: ValueDomain>;
            field: elements: usize;
            input: (Indexed<I, V>, Multiple<Ordered>);
            output: ExpressionHandle<Indexed<I, V>, Multiple<Ordered>>;
        }

        kernel {
            parameters: <V: BareValueDomain>;
            field: elements: usize;
            input: (Bare<V>, Multiple<Ordered>);
            output: ExpressionHandle<Bare<V>, Multiple<Ordered>>;
        }
    }
}

pub mod fraction {
    use super::TakeFractionOperation;
    use crate::{
        Bare, Indexed, Multiple, Ordered, expressions::ExpressionHandle,
        registry::operation_manifest, traits::TakeFraction,
    };

    operation_manifest! {
        TakeFractionOperation {
            method: TakeFraction::take_fraction;
            scope: lane;

            kernel {
                parameters: <I: IndexDomain, V: ValueDomain>;
                field: fraction: f64;
                input: (Indexed<I, V>, Multiple<Ordered>);
                output: ExpressionHandle<Indexed<I, V>, Multiple<Ordered>>;
            }

            kernel {
                parameters: <V: BareValueDomain>;
                field: fraction: f64;
                input: (Bare<V>, Multiple<Ordered>);
                output: ExpressionHandle<Bare<V>, Multiple<Ordered>>;
            }
        }
    }
}
