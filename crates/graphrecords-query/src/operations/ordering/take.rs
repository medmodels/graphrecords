use crate::{
    Bare, BareValueDomain, EvaluateOperand, Explain, IndexDomain, Indexed, Multiple, Operand,
    Ordered, QueryResult, ValueDomain,
    execution::EvaluationCache,
    operands::OperandHandle,
    operations::{
        Apply, BareStream, KeyedStream, LaneKernel, Operation, OperationContext, Prepare,
    },
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
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self.elements)
    }
}

impl<I: IndexDomain, V: ValueDomain> LaneKernel<Indexed<I, V>, Multiple<Ordered>>
    for TakeOperation
{
    type Output = OperandHandle<Indexed<I, V>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<Ordered>>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
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
    type Output = OperandHandle<Bare<V>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<Ordered>>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
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

impl<O: Apply<TakeOperation>> Take for O {
    type ReturnOperand = O::Output;

    fn take(&self, elements: usize) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            TakeOperation { elements },
        ))
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
            output: OperandHandle<Indexed<I, V>, Multiple<Ordered>>;
        }
        kernel {
            parameters: <V: BareValueDomain>;
            field: elements: usize;
            input: (Bare<V>, Multiple<Ordered>);
            output: OperandHandle<Bare<V>, Multiple<Ordered>>;
        }
    }
}
