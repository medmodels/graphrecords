use crate::{
    Bare, BareValueDomain, EvaluateExpression, Explain, IndexDomain, Indexed, Multiple, OrderState,
    QueryResult,
    capabilities::ValueMode,
    expressions::ExpressionHandle,
    operations::{BareStream, Build, KeyedStream, LaneKernel, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Mode,
};
use graphrecords_core::GraphRecord;
use graphrecords_utils::aliases::GrHashMap;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Lane)]
#[explain(label = "Mode")]
#[plan(optimizer_hints(empty = if_any))]
pub struct ModeOperation;

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

impl<I: IndexDomain, V: ValueMode + BareValueDomain, O: OrderState>
    LaneKernel<Indexed<I, V>, Multiple<O>> for ModeOperation
{
    type Output = ExpressionHandle<Bare<V>, Multiple<O>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        Ok(Box::new(
            modal_values::<V>(values.map(|(_, value)| value)).into_iter(),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            elements: None,
            distinct: input.distinct,
            selectivity: None,
            per_group: None,
        }
    }
}

impl<V: ValueMode + BareValueDomain, O: OrderState> LaneKernel<Bare<V>, Multiple<O>>
    for ModeOperation
{
    type Output = ExpressionHandle<Bare<V>, Multiple<O>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        Ok(Box::new(modal_values::<V>(values).into_iter()))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            elements: None,
            distinct: input.distinct,
            selectivity: None,
            per_group: None,
        }
    }
}

impl<E: Build<ModeOperation>> Mode for E {
    type Output = E::Output;

    fn mode(&self) -> Self::Output {
        self.build(ModeOperation)
    }
}

operation_manifest! {
    ModeOperation {
        method: Mode::mode;
        scope: lane;

        kernel {
            parameters: <
                I: IndexDomain,
                V: ValueMode + BareValueDomain,
                O: OrderState,
            >;
            input: (Indexed<I, V>, Multiple<O>);
            output: ExpressionHandle<Bare<V>, Multiple<O>>;
        }

        kernel {
            parameters: <
                V: ValueMode + BareValueDomain,
                O: OrderState,
            >;
            input: (Bare<V>, Multiple<O>);
            output: ExpressionHandle<Bare<V>, Multiple<O>>;
        }
    }
}
