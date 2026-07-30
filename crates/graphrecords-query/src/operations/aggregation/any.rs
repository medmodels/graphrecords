use crate::{
    Bare, EvaluateOperand, Explain, IndexDomain, Indexed, Mask, Multiple, Operand, OrderState,
    QueryResult,
    execution::EvaluationCache,
    operands::DefiniteBareBoolOperand,
    operations::{
        Apply, BareStream, KeyedStream, LaneKernel, Operation, OperationContext, Prepare,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Any,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "Any")]
pub struct AnyOperation;

impl Prepare for AnyOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

fn any(values: impl Iterator<Item = QueryResult<bool>>) -> QueryResult<bool> {
    for value in values {
        if value? {
            return Ok(true);
        }
    }

    Ok(false)
}

impl<I: IndexDomain, O: OrderState> LaneKernel<Indexed<I, Mask>, Multiple<O>> for AnyOperation {
    type Output = DefiniteBareBoolOperand;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, Mask, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(any(values.map(|(_, value)| value)))
    }

    fn estimate(&self, _input: Estimate, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl<O: OrderState> LaneKernel<Bare<Mask>, Multiple<O>> for AnyOperation {
    type Output = DefiniteBareBoolOperand;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, Mask, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(any(values))
    }

    fn estimate(&self, _input: Estimate, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl<O: Apply<AnyOperation>> Any for O {
    type ReturnOperand = O::Output;

    fn any(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), AnyOperation))
    }
}
