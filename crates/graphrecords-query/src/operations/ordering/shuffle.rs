use super::shuffle_prefix;
use crate::{
    Bare, BareValueDomain, EvaluateExpression, Explain, IndexDomain, Indexed, Multiple, OrderState,
    Ordered, QueryResult, ValueDomain,
    execution::EvaluationCache,
    expressions::ExpressionHandle,
    operations::{BareStream, Build, KeyedStream, LaneKernel, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::{Shuffle, ShuffleWithSeed},
};
use graphrecords_core::GraphRecord;
use rand::{SeedableRng, rngs::Xoshiro256PlusPlus};

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Lane)]
#[explain(label = "Shuffle")]
#[plan(optimizer_hints(volatile, empty = if_any))]
pub struct ShuffleOperation;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "ShuffleWithSeed")]
#[plan(optimizer_hints(empty = if_any))]
pub struct ShuffleWithSeedOperation {
    #[explain(label)]
    seed: u64,
}

impl Prepare for ShuffleWithSeedOperation {
    type Prepared<'a> = u64;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self.seed)
    }
}

impl<I: IndexDomain, V: ValueDomain, O: OrderState> LaneKernel<Indexed<I, V>, Multiple<O>>
    for ShuffleOperation
{
    type Output = ExpressionHandle<Indexed<I, V>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let values: Vec<_> = values.collect();
        let elements = values.len();

        Ok(Box::new(
            shuffle_prefix(values, elements, &mut rand::rng()).into_iter(),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<V: BareValueDomain, O: OrderState> LaneKernel<Bare<V>, Multiple<O>> for ShuffleOperation {
    type Output = ExpressionHandle<Bare<V>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let values: Vec<_> = values.collect();
        let elements = values.len();

        Ok(Box::new(
            shuffle_prefix(values, elements, &mut rand::rng()).into_iter(),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<I: IndexDomain, V: ValueDomain, O: OrderState> LaneKernel<Indexed<I, V>, Multiple<O>>
    for ShuffleWithSeedOperation
{
    type Output = ExpressionHandle<Indexed<I, V>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let values: Vec<_> = values.collect();
        let elements = values.len();
        let mut generator = Xoshiro256PlusPlus::seed_from_u64(prepared);

        Ok(Box::new(
            shuffle_prefix(values, elements, &mut generator).into_iter(),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<V: BareValueDomain, O: OrderState> LaneKernel<Bare<V>, Multiple<O>>
    for ShuffleWithSeedOperation
{
    type Output = ExpressionHandle<Bare<V>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let values: Vec<_> = values.collect();
        let elements = values.len();
        let mut generator = Xoshiro256PlusPlus::seed_from_u64(prepared);

        Ok(Box::new(
            shuffle_prefix(values, elements, &mut generator).into_iter(),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<E: Build<ShuffleOperation>> Shuffle for E {
    type Output = E::Output;

    fn shuffle(&self) -> Self::Output {
        self.build(ShuffleOperation)
    }
}

impl<E: Build<ShuffleWithSeedOperation>> ShuffleWithSeed for E {
    type Output = E::Output;

    fn shuffle_with_seed(&self, seed: u64) -> Self::Output {
        self.build(ShuffleWithSeedOperation { seed })
    }
}

operation_manifest! {
    ShuffleOperation {
        method: Shuffle::shuffle;
        scope: lane;

        kernel {
            parameters: <I: IndexDomain, V: ValueDomain, O: OrderState>;
            input: (Indexed<I, V>, Multiple<O>);
            output: ExpressionHandle<Indexed<I, V>, Multiple<Ordered>>;
        }

        kernel {
            parameters: <V: BareValueDomain, O: OrderState>;
            input: (Bare<V>, Multiple<O>);
            output: ExpressionHandle<Bare<V>, Multiple<Ordered>>;
        }
    }
}

pub mod with_seed {
    use super::ShuffleWithSeedOperation;
    use crate::{
        Bare, Indexed, Multiple, Ordered, expressions::ExpressionHandle,
        registry::operation_manifest, traits::ShuffleWithSeed,
    };

    operation_manifest! {
        ShuffleWithSeedOperation {
            method: ShuffleWithSeed::shuffle_with_seed;
            scope: lane;

            kernel {
                parameters: <I: IndexDomain, V: ValueDomain, O: OrderState>;
                field: seed: u64;
                input: (Indexed<I, V>, Multiple<O>);
                output: ExpressionHandle<Indexed<I, V>, Multiple<Ordered>>;
            }

            kernel {
                parameters: <V: BareValueDomain, O: OrderState>;
                field: seed: u64;
                input: (Bare<V>, Multiple<O>);
                output: ExpressionHandle<Bare<V>, Multiple<Ordered>>;
            }
        }
    }
}
