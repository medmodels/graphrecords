use crate::{
    Bare, Definite, EvaluateOperand, Explain, IndexDomain, Indexed, Multiple, Operand, OrderState,
    QueryResult, Single, ValueType,
    execution::EvaluationCache,
    operands::DefiniteBareValueOperand,
    operations::{
        Apply, BareStream, KeyedStream, LaneKernel, Operation, OperationContext, Prepare,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Count,
};
use graphrecords_core::{GraphRecord, graphrecord::GraphRecordValue};

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "Count")]
pub struct CountOperation;

impl Prepare for CountOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I: IndexDomain, V: ValueType, O: OrderState> LaneKernel<Indexed<I, V>, Multiple<O>>
    for CountOperation
{
    type Output = DefiniteBareValueOperand;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: KeyedStream<'a, I, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let count = values.try_fold(0_i64, |count, (_, item)| item.map(|_| count + 1));

        Ok(count.map(GraphRecordValue::Int))
    }

    fn estimate(&self, _input: Estimate, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl<V: ValueType, O: OrderState> LaneKernel<Bare<V>, Multiple<O>> for CountOperation {
    type Output = DefiniteBareValueOperand;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let count = values.try_fold(0_i64, |count, item| item.map(|_| count + 1));

        Ok(count.map(GraphRecordValue::Int))
    }

    fn estimate(&self, _input: Estimate, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl<I: IndexDomain, V: ValueType> LaneKernel<Indexed<I, V>, Single> for CountOperation {
    type Output = DefiniteBareValueOperand;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, V, Single>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let count = match value {
            Some((_, item)) => item.map(|_| 1_i64),
            None => Ok(0_i64),
        };

        Ok(count.map(GraphRecordValue::Int))
    }

    fn estimate(&self, _input: Estimate, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl<V: ValueType> LaneKernel<Bare<V>, Single> for CountOperation {
    type Output = DefiniteBareValueOperand;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: BareStream<'a, V, Single>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let count = match value {
            Some(item) => item.map(|_| 1_i64),
            None => Ok(0_i64),
        };

        Ok(count.map(GraphRecordValue::Int))
    }

    fn estimate(&self, _input: Estimate, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl<I: IndexDomain, V: ValueType> LaneKernel<Indexed<I, V>, Definite> for CountOperation {
    type Output = DefiniteBareValueOperand;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, V, Definite>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(value.1.map(|_| GraphRecordValue::Int(1)))
    }

    fn estimate(&self, _input: Estimate, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl<V: ValueType> LaneKernel<Bare<V>, Definite> for CountOperation {
    type Output = DefiniteBareValueOperand;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: BareStream<'a, V, Definite>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(value.map(|_| GraphRecordValue::Int(1)))
    }

    fn estimate(&self, _input: Estimate, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl<O: Apply<CountOperation>> Count for O {
    type ReturnOperand = O::Output;

    fn count(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), CountOperation))
    }
}
