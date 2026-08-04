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
    traits::ReverseOrder,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "ReverseOrder")]
#[plan(optimizer_hints(empty = if_any))]
pub struct ReverseOrderOperation;

impl Prepare for ReverseOrderOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I: IndexDomain, V: ValueDomain> LaneKernel<Indexed<I, V>, Multiple<Ordered>>
    for ReverseOrderOperation
{
    type Output = OperandHandle<Indexed<I, V>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<Ordered>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        #[expect(
            clippy::needless_collect,
            reason = "the erased stream is not a DoubleEndedIterator"
        )]
        let values: Vec<_> = values.collect();

        Ok(Box::new(values.into_iter().rev()))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<V: BareValueDomain> LaneKernel<Bare<V>, Multiple<Ordered>> for ReverseOrderOperation {
    type Output = OperandHandle<Bare<V>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<Ordered>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        #[expect(
            clippy::needless_collect,
            reason = "the erased stream is not a DoubleEndedIterator"
        )]
        let values: Vec<_> = values.collect();

        Ok(Box::new(values.into_iter().rev()))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<O: Apply<ReverseOrderOperation>> ReverseOrder for O {
    type ReturnOperand = O::Output;

    fn reverse_order(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), ReverseOrderOperation))
    }
}

operation_manifest! {
    ReverseOrderOperation {
        method: ReverseOrder::reverse_order;
        scope: lane;

        kernel {
            parameters: <I: IndexDomain, V: ValueDomain>;
            input: (Indexed<I, V>, Multiple<Ordered>);
            output: OperandHandle<Indexed<I, V>, Multiple<Ordered>>;
        }
        kernel {
            parameters: <V: BareValueDomain>;
            input: (Bare<V>, Multiple<Ordered>);
            output: OperandHandle<Bare<V>, Multiple<Ordered>>;
        }
    }
}
