use crate::{
    Bare, EvaluateOperand, Explain, IndexDomain, Indexed, Multiple, Operand, Ordered, QueryResult,
    ValueType,
    execution::EvaluationCache,
    operands::OperandHandle,
    operations::{Apply, BareStream, Kernel, KeyedStream, Operation, OperationContext, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Take,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "Take")]
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

impl<I: IndexDomain, V: ValueType> Kernel<Indexed<I, V>, Multiple<Ordered>> for TakeOperation {
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
            ..input
        }
    }
}

impl<V: ValueType> Kernel<Bare<V>, Multiple<Ordered>> for TakeOperation {
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
            ..input
        }
    }
}

impl<O> Take for O
where
    O: Apply<TakeOperation>,
{
    type ReturnOperand = <O as Apply<TakeOperation>>::Output;

    fn take(&self, elements: usize) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            TakeOperation { elements },
        ))
    }
}
