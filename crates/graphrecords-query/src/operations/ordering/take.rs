use crate::{
    Bare, BareValueDomain, EvaluateExpression, Explain, IndexDomain, Indexed, Multiple, Ordered,
    QueryResult, ValueDomain,
    execution::EvaluationCache,
    expressions::ExpressionHandle,
    operations::{BareStream, Build, KeyedStream, LaneKernel, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Take,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "Take")]
#[plan(optimizer_hints(empty = if_any))]
pub struct TakeOperation {
    #[explain(label)]
    elements: usize,
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

impl<E: Build<TakeOperation>> Take for E {
    type Output = E::Output;

    fn take(&self, elements: usize) -> Self::Output {
        self.build(TakeOperation { elements })
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
