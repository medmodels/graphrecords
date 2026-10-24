use super::{fraction_elements, fraction_estimate, shuffle_prefix};
use crate::{
    Bare, BareValueDomain, EvaluateExpression, Explain, IndexDomain, Indexed, Multiple, OrderState,
    Ordered, QueryResult, ValueDomain,
    execution::EvaluationCache,
    expressions::ExpressionHandle,
    operations::{BareStream, Build, KeyedStream, LaneKernel, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::{SampleFraction, SampleFractionWithSeed},
};
use graphrecords_core::GraphRecord;
use rand::{SeedableRng, rngs::Xoshiro256PlusPlus};
use std::hash::{Hash, Hasher};

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "SampleFraction")]
#[plan(optimizer_hints(volatile, empty = if_any))]
pub struct SampleFractionOperation {
    #[explain(label)]
    fraction: f64,
}

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "SampleFractionWithSeed")]
#[plan(optimizer_hints(empty = if_any))]
pub struct SampleFractionWithSeedOperation {
    #[explain(label)]
    fraction: f64,
    #[explain(label)]
    seed: u64,
}

impl PlanIdentity for SampleFractionOperation {
    fn identity_eq(&self, other: &Self) -> bool {
        self.fraction.identity_eq(&other.fraction)
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        self.fraction.identity_hash(state);
    }
}

impl PlanIdentity for SampleFractionWithSeedOperation {
    fn identity_eq(&self, other: &Self) -> bool {
        self.fraction.identity_eq(&other.fraction) && self.seed == other.seed
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        self.fraction.identity_hash(state);
        self.seed.hash(state);
    }
}

impl Prepare for SampleFractionOperation {
    type Prepared<'a> = f64;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self.fraction)
    }
}

impl Prepare for SampleFractionWithSeedOperation {
    type Prepared<'a> = (f64, u64);

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok((self.fraction, self.seed))
    }
}

impl<I: IndexDomain, V: ValueDomain, O: OrderState> LaneKernel<Indexed<I, V>, Multiple<O>>
    for SampleFractionOperation
{
    type Output = ExpressionHandle<Indexed<I, V>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let values: Vec<_> = values.collect();
        let elements = fraction_elements(prepared, values.len());

        Ok(Box::new(
            shuffle_prefix(values, elements, &mut rand::rng()).into_iter(),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        fraction_estimate(self.fraction, input)
    }
}

impl<V: BareValueDomain, O: OrderState> LaneKernel<Bare<V>, Multiple<O>>
    for SampleFractionOperation
{
    type Output = ExpressionHandle<Bare<V>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let values: Vec<_> = values.collect();
        let elements = fraction_elements(prepared, values.len());

        Ok(Box::new(
            shuffle_prefix(values, elements, &mut rand::rng()).into_iter(),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        fraction_estimate(self.fraction, input)
    }
}

impl<I: IndexDomain, V: ValueDomain, O: OrderState> LaneKernel<Indexed<I, V>, Multiple<O>>
    for SampleFractionWithSeedOperation
{
    type Output = ExpressionHandle<Indexed<I, V>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let (fraction, seed) = prepared;
        let values: Vec<_> = values.collect();
        let elements = fraction_elements(fraction, values.len());
        let mut generator = Xoshiro256PlusPlus::seed_from_u64(seed);

        Ok(Box::new(
            shuffle_prefix(values, elements, &mut generator).into_iter(),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        fraction_estimate(self.fraction, input)
    }
}

impl<V: BareValueDomain, O: OrderState> LaneKernel<Bare<V>, Multiple<O>>
    for SampleFractionWithSeedOperation
{
    type Output = ExpressionHandle<Bare<V>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let (fraction, seed) = prepared;
        let values: Vec<_> = values.collect();
        let elements = fraction_elements(fraction, values.len());
        let mut generator = Xoshiro256PlusPlus::seed_from_u64(seed);

        Ok(Box::new(
            shuffle_prefix(values, elements, &mut generator).into_iter(),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        fraction_estimate(self.fraction, input)
    }
}

impl<E: Build<SampleFractionOperation>> SampleFraction for E {
    type Output = E::Output;

    fn sample_fraction(&self, fraction: f64) -> Self::Output {
        self.build(SampleFractionOperation { fraction })
    }
}

impl<E: Build<SampleFractionWithSeedOperation>> SampleFractionWithSeed for E {
    type Output = E::Output;

    fn sample_fraction_with_seed(&self, fraction: f64, seed: u64) -> Self::Output {
        self.build(SampleFractionWithSeedOperation { fraction, seed })
    }
}

operation_manifest! {
    SampleFractionOperation {
        method: SampleFraction::sample_fraction;
        scope: lane;

        kernel {
            parameters: <I: IndexDomain, V: ValueDomain, O: OrderState>;
            field: fraction: f64;
            input: (Indexed<I, V>, Multiple<O>);
            output: ExpressionHandle<Indexed<I, V>, Multiple<Ordered>>;
        }

        kernel {
            parameters: <V: BareValueDomain, O: OrderState>;
            field: fraction: f64;
            input: (Bare<V>, Multiple<O>);
            output: ExpressionHandle<Bare<V>, Multiple<Ordered>>;
        }
    }
}

pub mod with_seed {
    use super::SampleFractionWithSeedOperation;
    use crate::{
        Bare, Indexed, Multiple, Ordered, expressions::ExpressionHandle,
        registry::operation_manifest, traits::SampleFractionWithSeed,
    };

    operation_manifest! {
        SampleFractionWithSeedOperation {
            method: SampleFractionWithSeed::sample_fraction_with_seed;
            scope: lane;

            kernel {
                parameters: <I: IndexDomain, V: ValueDomain, O: OrderState>;
                field: fraction: f64;
                field: seed: u64;
                input: (Indexed<I, V>, Multiple<O>);
                output: ExpressionHandle<Indexed<I, V>, Multiple<Ordered>>;
            }

            kernel {
                parameters: <V: BareValueDomain, O: OrderState>;
                field: fraction: f64;
                field: seed: u64;
                input: (Bare<V>, Multiple<O>);
                output: ExpressionHandle<Bare<V>, Multiple<Ordered>>;
            }
        }
    }
}
