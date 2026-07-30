use crate::{
    Bare, EvaluateOperand, Explain, IndexDomain, Indexed, Multiple, Operand, OrderState,
    QueryResult,
    capabilities::ValueUniqueCount,
    execution::EvaluationCache,
    operands::DefiniteBareValueOperand,
    operations::{
        Apply, BareStream, KeyedStream, LaneKernel, Operation, OperationContext, Prepare,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::UniqueCount,
};
use graphrecords_core::{GraphRecord, graphrecord::GraphRecordValue};
use graphrecords_utils::aliases::GrHashSet;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "NUnique")]
pub struct UniqueCountOperation;

impl Prepare for UniqueCountOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I: IndexDomain, V: ValueUniqueCount, O: OrderState> LaneKernel<Indexed<I, V>, Multiple<O>>
    for UniqueCountOperation
{
    type Output = DefiniteBareValueOperand;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: KeyedStream<'a, I, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let count = values.try_fold(
            (GrHashSet::default(), 0),
            |(mut unique, count), (_, value)| {
                let value = value?;
                let inserted = unique.insert(V::unique_count_key(&value));

                Ok((unique, count + i64::from(inserted)))
            },
        );

        Ok(count.map(|(_, count)| GraphRecordValue::Int(count)))
    }

    fn estimate(&self, _input: Estimate, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl<V: ValueUniqueCount, O: OrderState> LaneKernel<Bare<V>, Multiple<O>> for UniqueCountOperation {
    type Output = DefiniteBareValueOperand;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let count = values.try_fold((GrHashSet::default(), 0), |(mut unique, count), value| {
            let value = value?;
            let inserted = unique.insert(V::unique_count_key(&value));

            Ok((unique, count + i64::from(inserted)))
        });

        Ok(count.map(|(_, count)| GraphRecordValue::Int(count)))
    }

    fn estimate(&self, _input: Estimate, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl<O: Apply<UniqueCountOperation>> UniqueCount for O {
    type ReturnOperand = O::Output;

    fn n_unique(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), UniqueCountOperation))
    }
}
