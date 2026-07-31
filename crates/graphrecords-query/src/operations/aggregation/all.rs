use crate::{
    Bare, EvaluateOperand, Explain, IndexDomain, Indexed, Mask, Multiple, Operand, OrderState,
    QueryResult,
    execution::EvaluationCache,
    operands::DefiniteBareBoolOperand,
    operations::{
        Apply, BareStream, KeyedStream, LaneKernel, Operation, OperationContext, Prepare,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::All,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "All")]
pub struct AllOperation;

impl Prepare for AllOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

fn all(values: impl Iterator<Item = QueryResult<bool>>) -> QueryResult<bool> {
    for value in values {
        if !value? {
            return Ok(false);
        }
    }

    Ok(true)
}

impl<I: IndexDomain, O: OrderState> LaneKernel<Indexed<I, Mask>, Multiple<O>> for AllOperation {
    type Output = DefiniteBareBoolOperand;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, Mask, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(all(values.map(|(_, value)| value)))
    }

    fn estimate(&self, _input: Estimate, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl<O: OrderState> LaneKernel<Bare<Mask>, Multiple<O>> for AllOperation {
    type Output = DefiniteBareBoolOperand;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, Mask, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(all(values))
    }

    fn estimate(&self, _input: Estimate, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl<O: Apply<AllOperation>> All for O {
    type ReturnOperand = O::Output;

    fn all(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), AllOperation))
    }
}

operation_manifest! {
    AllOperation {
        method: All::all;
        scope: lane;

        kernel {
            parameters: <
                I: IndexDomain,
                O: OrderState,
            >;
            input: (Indexed<I, Mask>, Multiple<O>);
            output: DefiniteBareBoolOperand;
        }

        kernel {
            parameters: <O: OrderState>;
            input: (Bare<Mask>, Multiple<O>);
            output: DefiniteBareBoolOperand;
        }
    }
}
