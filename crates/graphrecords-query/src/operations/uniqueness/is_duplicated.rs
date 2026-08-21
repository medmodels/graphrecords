use crate::{
    Bare, BareValueDomain, EvaluateExpression, Explain, IndexDomain, Indexed, Mask, Multiple,
    OrderState, QueryResult,
    capabilities::ValueEquivalence,
    expressions::ExpressionHandle,
    operations::{BareStream, Build, KeyedStream, LaneKernel, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::IsDuplicated,
};
use graphrecords_core::GraphRecord;
use graphrecords_utils::aliases::GrHashMap;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Lane)]
#[explain(label = "IsDuplicated")]
#[plan(optimizer_hints(empty = if_any))]
pub struct IsDuplicatedOperation;

impl<I: IndexDomain, V: ValueEquivalence, O: OrderState> LaneKernel<Indexed<I, V>, Multiple<O>>
    for IsDuplicatedOperation
{
    type Output = ExpressionHandle<Indexed<I, Mask>, Multiple<O>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
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

impl<V: ValueEquivalence + BareValueDomain, O: OrderState> LaneKernel<Bare<V>, Multiple<O>>
    for IsDuplicatedOperation
{
    type Output = ExpressionHandle<Bare<Mask>, Multiple<O>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
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

impl<E: Build<IsDuplicatedOperation>> IsDuplicated for E {
    type Output = E::Output;

    fn is_duplicated(&self) -> Self::Output {
        self.build(IsDuplicatedOperation)
    }
}

operation_manifest! {
    IsDuplicatedOperation {
        method: IsDuplicated::is_duplicated;
        scope: lane;

        kernel {
            parameters: <
                I: IndexDomain,
                V: ValueEquivalence,
                O: OrderState,
            >;
            input: (Indexed<I, V>, Multiple<O>);
            output: ExpressionHandle<Indexed<I, Mask>, Multiple<O>>;
        }

        kernel {
            parameters: <
                V: ValueEquivalence + BareValueDomain,
                O: OrderState,
            >;
            input: (Bare<V>, Multiple<O>);
            output: ExpressionHandle<Bare<Mask>, Multiple<O>>;
        }
    }
}
