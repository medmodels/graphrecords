use crate::{
    Bare, BareValueDomain, EvaluateExpression, Explain, IndexDomain, Indexed, Multiple, OrderState,
    QueryResult, Single, ValueDomain,
    execution::EvaluationCache,
    expressions::ExpressionHandle,
    operations::{
        BareStream, Build, KeyedStream, LaneKernel, Operation, Prepare, ordering::shuffle_prefix,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::{Random, RandomWithSeed},
};
use graphrecords_core::GraphRecord;
use rand::{SeedableRng, rngs::Xoshiro256PlusPlus, seq::IteratorRandom};

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Lane)]
#[explain(label = "Random")]
#[plan(optimizer_hints(volatile, empty = if_any))]
pub struct RandomOperation;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "RandomWithSeed")]
#[plan(optimizer_hints(empty = if_any))]
pub struct RandomWithSeedOperation {
    #[explain(label)]
    seed: u64,
}

impl Prepare for RandomWithSeedOperation {
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
    for RandomOperation
{
    type Output = ExpressionHandle<Indexed<I, V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        Ok(values.choose(&mut rand::rng()))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<V: BareValueDomain, O: OrderState> LaneKernel<Bare<V>, Multiple<O>> for RandomOperation {
    type Output = ExpressionHandle<Bare<V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        Ok(values.choose(&mut rand::rng()))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<I: IndexDomain, V: ValueDomain, O: OrderState> LaneKernel<Indexed<I, V>, Multiple<O>>
    for RandomWithSeedOperation
{
    type Output = ExpressionHandle<Indexed<I, V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let mut generator = Xoshiro256PlusPlus::seed_from_u64(prepared);

        Ok(shuffle_prefix(values.collect(), 1, &mut generator).pop())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<V: BareValueDomain, O: OrderState> LaneKernel<Bare<V>, Multiple<O>>
    for RandomWithSeedOperation
{
    type Output = ExpressionHandle<Bare<V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let mut generator = Xoshiro256PlusPlus::seed_from_u64(prepared);

        Ok(shuffle_prefix(values.collect(), 1, &mut generator).pop())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<E: Build<RandomOperation>> Random for E {
    type Output = E::Output;

    fn random(&self) -> Self::Output {
        self.build(RandomOperation)
    }
}

impl<E: Build<RandomWithSeedOperation>> RandomWithSeed for E {
    type Output = E::Output;

    fn random_with_seed(&self, seed: u64) -> Self::Output {
        self.build(RandomWithSeedOperation { seed })
    }
}

operation_manifest! {
    RandomOperation {
        method: Random::random;
        scope: lane;

        kernel {
            parameters: <
                I: IndexDomain,
                V: ValueDomain,
                O: OrderState,
            >;
            input: (Indexed<I, V>, Multiple<O>);
            output: ExpressionHandle<Indexed<I, V>, Single>;
        }

        kernel {
            parameters: <
                V: BareValueDomain,
                O: OrderState,
            >;
            input: (Bare<V>, Multiple<O>);
            output: ExpressionHandle<Bare<V>, Single>;
        }
    }
}

pub mod with_seed {
    use super::RandomWithSeedOperation;
    use crate::{
        Bare, Indexed, Multiple, Single, expressions::ExpressionHandle,
        registry::operation_manifest, traits::RandomWithSeed,
    };

    operation_manifest! {
        RandomWithSeedOperation {
            method: RandomWithSeed::random_with_seed;
            scope: lane;

            kernel {
                parameters: <
                    I: IndexDomain,
                    V: ValueDomain,
                    O: OrderState,
                >;
                field: seed: u64;
                input: (Indexed<I, V>, Multiple<O>);
                output: ExpressionHandle<Indexed<I, V>, Single>;
            }

            kernel {
                parameters: <
                    V: BareValueDomain,
                    O: OrderState,
                >;
                field: seed: u64;
                input: (Bare<V>, Multiple<O>);
                output: ExpressionHandle<Bare<V>, Single>;
            }
        }
    }
}
