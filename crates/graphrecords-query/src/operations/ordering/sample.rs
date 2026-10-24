use super::shuffle_prefix;
use crate::{
    Bare, BareValueDomain, EvaluateExpression, Explain, IndexDomain, Indexed, Multiple, OrderState,
    Ordered, QueryResult, ValueDomain,
    execution::EvaluationCache,
    expressions::ExpressionHandle,
    operations::{BareStream, Build, KeyedStream, LaneKernel, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::{Sample, SampleWithSeed},
};
use graphrecords_core::GraphRecord;
use rand::{SeedableRng, rngs::Xoshiro256PlusPlus};

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "Sample")]
#[plan(optimizer_hints(volatile, empty = if_any))]
pub struct SampleOperation {
    #[explain(label)]
    elements: usize,
}

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "SampleWithSeed")]
#[plan(optimizer_hints(empty = if_any))]
pub struct SampleWithSeedOperation {
    #[explain(label)]
    elements: usize,
    #[explain(label)]
    seed: u64,
}

impl Prepare for SampleOperation {
    type Prepared<'a> = usize;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self.elements)
    }
}

impl Prepare for SampleWithSeedOperation {
    type Prepared<'a> = (usize, u64);

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok((self.elements, self.seed))
    }
}

impl<I: IndexDomain, V: ValueDomain, O: OrderState> LaneKernel<Indexed<I, V>, Multiple<O>>
    for SampleOperation
{
    type Output = ExpressionHandle<Indexed<I, V>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        Ok(Box::new(
            shuffle_prefix(values.collect(), prepared, &mut rand::rng()).into_iter(),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            elements: input.elements.map(|elements| elements.min(self.elements)),
            distinct: input.distinct.map(|distinct| distinct.min(self.elements)),
            selectivity: None,
            ..input
        }
    }
}

impl<V: BareValueDomain, O: OrderState> LaneKernel<Bare<V>, Multiple<O>> for SampleOperation {
    type Output = ExpressionHandle<Bare<V>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        Ok(Box::new(
            shuffle_prefix(values.collect(), prepared, &mut rand::rng()).into_iter(),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            elements: input.elements.map(|elements| elements.min(self.elements)),
            distinct: input.distinct.map(|distinct| distinct.min(self.elements)),
            selectivity: None,
            ..input
        }
    }
}

impl<I: IndexDomain, V: ValueDomain, O: OrderState> LaneKernel<Indexed<I, V>, Multiple<O>>
    for SampleWithSeedOperation
{
    type Output = ExpressionHandle<Indexed<I, V>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let (elements, seed) = prepared;
        let mut generator = Xoshiro256PlusPlus::seed_from_u64(seed);

        Ok(Box::new(
            shuffle_prefix(values.collect(), elements, &mut generator).into_iter(),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            elements: input.elements.map(|elements| elements.min(self.elements)),
            distinct: input.distinct.map(|distinct| distinct.min(self.elements)),
            selectivity: None,
            ..input
        }
    }
}

impl<V: BareValueDomain, O: OrderState> LaneKernel<Bare<V>, Multiple<O>>
    for SampleWithSeedOperation
{
    type Output = ExpressionHandle<Bare<V>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let (elements, seed) = prepared;
        let mut generator = Xoshiro256PlusPlus::seed_from_u64(seed);

        Ok(Box::new(
            shuffle_prefix(values.collect(), elements, &mut generator).into_iter(),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            elements: input.elements.map(|elements| elements.min(self.elements)),
            distinct: input.distinct.map(|distinct| distinct.min(self.elements)),
            selectivity: None,
            ..input
        }
    }
}

impl<E: Build<SampleOperation>> Sample for E {
    type Output = E::Output;

    fn sample(&self, elements: usize) -> Self::Output {
        self.build(SampleOperation { elements })
    }
}

impl<E: Build<SampleWithSeedOperation>> SampleWithSeed for E {
    type Output = E::Output;

    fn sample_with_seed(&self, elements: usize, seed: u64) -> Self::Output {
        self.build(SampleWithSeedOperation { elements, seed })
    }
}

operation_manifest! {
    SampleOperation {
        method: Sample::sample;
        scope: lane;

        kernel {
            parameters: <I: IndexDomain, V: ValueDomain, O: OrderState>;
            field: elements: usize;
            input: (Indexed<I, V>, Multiple<O>);
            output: ExpressionHandle<Indexed<I, V>, Multiple<Ordered>>;
        }

        kernel {
            parameters: <V: BareValueDomain, O: OrderState>;
            field: elements: usize;
            input: (Bare<V>, Multiple<O>);
            output: ExpressionHandle<Bare<V>, Multiple<Ordered>>;
        }
    }
}

pub mod with_seed {
    use super::SampleWithSeedOperation;
    use crate::{
        Bare, Indexed, Multiple, Ordered, expressions::ExpressionHandle,
        registry::operation_manifest, traits::SampleWithSeed,
    };

    operation_manifest! {
        SampleWithSeedOperation {
            method: SampleWithSeed::sample_with_seed;
            scope: lane;

            kernel {
                parameters: <I: IndexDomain, V: ValueDomain, O: OrderState>;
                field: elements: usize;
                field: seed: u64;
                input: (Indexed<I, V>, Multiple<O>);
                output: ExpressionHandle<Indexed<I, V>, Multiple<Ordered>>;
            }

            kernel {
                parameters: <V: BareValueDomain, O: OrderState>;
                field: elements: usize;
                field: seed: u64;
                input: (Bare<V>, Multiple<O>);
                output: ExpressionHandle<Bare<V>, Multiple<Ordered>>;
            }
        }
    }
}
