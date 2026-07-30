use crate::{
    Bare, EvaluateOperand, Explain, IndexDomain, Indexed, Mask, Multiple, Operand, OrderState,
    QueryResult,
    capabilities::ValueEquivalence,
    execution::EvaluationCache,
    operands::OperandHandle,
    operations::{
        Apply, BareStream, KeyedStream, LaneKernel, Operation, OperationContext, Prepare,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::IsDuplicated,
};
use graphrecords_core::GraphRecord;
use graphrecords_utils::aliases::GrHashMap;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "IsDuplicated")]
#[plan(optimizer_hints(empty = if_any))]
pub struct IsDuplicatedOperation;

impl Prepare for IsDuplicatedOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I: IndexDomain, V: ValueEquivalence, O: OrderState> LaneKernel<Indexed<I, V>, Multiple<O>>
    for IsDuplicatedOperation
{
    type Output = OperandHandle<Indexed<I, Mask>, Multiple<O>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let values: Vec<_> = values.collect();
        let mut counts: GrHashMap<_, usize> = GrHashMap::default();

        for (_, outcome) in &values {
            if let Ok(value) = outcome {
                *counts.entry(V::equivalence_key(value)).or_insert(0) += 1;
            }
        }

        Ok(Box::new(values.into_iter().map(move |(index, outcome)| {
            let outcome = outcome.map(|value| {
                counts
                    .get(&V::equivalence_key(&value))
                    .copied()
                    .expect("every successful value was counted")
                    > 1
            });

            (index, outcome)
        })))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            elements: input.elements,
            distinct: None,
            selectivity: None,
            per_group: None,
        }
    }
}

impl<V: ValueEquivalence, O: OrderState> LaneKernel<Bare<V>, Multiple<O>>
    for IsDuplicatedOperation
{
    type Output = OperandHandle<Bare<Mask>, Multiple<O>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let values: Vec<_> = values.collect();
        let mut counts: GrHashMap<_, usize> = GrHashMap::default();

        for value in values.iter().flatten() {
            *counts.entry(V::equivalence_key(value)).or_insert(0) += 1;
        }

        Ok(Box::new(values.into_iter().map(move |outcome| {
            outcome.map(|value| {
                counts
                    .get(&V::equivalence_key(&value))
                    .copied()
                    .expect("every successful value was counted")
                    > 1
            })
        })))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            elements: input.elements,
            distinct: None,
            selectivity: None,
            per_group: None,
        }
    }
}

impl<O: Apply<IsDuplicatedOperation>> IsDuplicated for O {
    type ReturnOperand = O::Output;

    fn is_duplicated(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), IsDuplicatedOperation))
    }
}
