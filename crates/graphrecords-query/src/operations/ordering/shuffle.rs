use crate::{
    Bare, BareValueDomain, EvaluateOperand, Explain, IndexDomain, Indexed, Multiple, Operand,
    OrderState, Ordered, QueryResult, ValueDomain,
    execution::EvaluationCache,
    operands::OperandHandle,
    operations::{
        Apply, BareStream, KeyedStream, LaneKernel, Operation, OperationContext, Prepare,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Shuffle,
};
use graphrecords_core::GraphRecord;
use rand::seq::SliceRandom;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "Shuffle")]
#[plan(optimizer_hints(volatile, empty = if_any))]
pub struct ShuffleOperation;

impl Prepare for ShuffleOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I: IndexDomain, V: ValueDomain, O: OrderState> LaneKernel<Indexed<I, V>, Multiple<O>>
    for ShuffleOperation
{
    type Output = OperandHandle<Indexed<I, V>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let mut values: Vec<_> = values.collect();
        values.shuffle(&mut rand::rng());

        Ok(Box::new(values.into_iter()))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<V: BareValueDomain, O: OrderState> LaneKernel<Bare<V>, Multiple<O>> for ShuffleOperation {
    type Output = OperandHandle<Bare<V>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let mut values: Vec<_> = values.collect();
        values.shuffle(&mut rand::rng());

        Ok(Box::new(values.into_iter()))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<O: Apply<ShuffleOperation>> Shuffle for O {
    type ReturnOperand = O::Output;

    fn shuffle(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), ShuffleOperation))
    }
}

operation_manifest! {
    ShuffleOperation {
        method: Shuffle::shuffle;
        scope: lane;

        kernel {
            parameters: <I: IndexDomain, V: ValueDomain, O: OrderState>;
            input: (Indexed<I, V>, Multiple<O>);
            output: OperandHandle<Indexed<I, V>, Multiple<Ordered>>;
        }
        kernel {
            parameters: <V: BareValueDomain, O: OrderState>;
            input: (Bare<V>, Multiple<O>);
            output: OperandHandle<Bare<V>, Multiple<Ordered>>;
        }
    }
}
