use crate::{
    Bare, EvaluateOperand, Explain, IndexDomain, Indexed, Multiple, Operand, OrderState,
    QueryResult,
    capabilities::ValueMode,
    execution::EvaluationCache,
    operands::OperandHandle,
    operations::{
        Apply, BareStream, KeyedStream, LaneKernel, Operation, OperationContext, Prepare,
    },
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    traits::Mode,
};
use graphrecords_core::GraphRecord;
use graphrecords_utils::aliases::GrHashMap;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "Mode")]
#[plan(optimizer_hints(empty = if_any))]
pub struct ModeOperation;

impl Prepare for ModeOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

fn modal_values<'a, V: ValueMode>(
    values: impl Iterator<Item = QueryResult<V::Value<'a>>>,
) -> Vec<QueryResult<V::Value<'a>>> {
    let mut counts: GrHashMap<_, usize> = GrHashMap::default();
    let mut encountered = Vec::new();

    for outcome in values {
        let value = match outcome {
            Ok(value) => value,
            Err(failure) => return vec![Err(failure)],
        };
        let key = V::equivalence_key(&value);

        if let Some(count) = counts.get_mut(&key) {
            *count += 1;
        } else {
            counts.insert(key, 1);
            encountered.push(value);
        }
    }

    let Some(maximum_count) = counts.values().copied().max() else {
        return Vec::new();
    };

    encountered
        .into_iter()
        .filter_map(|value| {
            (counts.get(&V::equivalence_key(&value)) == Some(&maximum_count)).then_some(Ok(value))
        })
        .collect()
}

impl<I: IndexDomain, V: ValueMode, O: OrderState> LaneKernel<Indexed<I, V>, Multiple<O>>
    for ModeOperation
{
    type Output = OperandHandle<Bare<V>, Multiple<O>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(Box::new(
            modal_values::<V>(values.map(|(_, value)| value)).into_iter(),
        ))
    }
}

impl<V: ValueMode, O: OrderState> LaneKernel<Bare<V>, Multiple<O>> for ModeOperation {
    type Output = OperandHandle<Bare<V>, Multiple<O>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(Box::new(modal_values::<V>(values).into_iter()))
    }
}

impl<O: Apply<ModeOperation>> Mode for O {
    type ReturnOperand = O::Output;

    fn mode(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), ModeOperation))
    }
}
