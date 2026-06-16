use crate::{
    Cache, EvaluateContext, EvaluateOperand, Explain, Operand, QueryResult,
    execution::{Cacheable, EvaluationCache},
    optimizer::{
        Cost, MatchInputs, OptimizePlan, OptimizerHints, PlanNode, Session, Stats, Transformed,
    },
};
use graphrecords_core::GraphRecord;
use std::{collections::hash_map::DefaultHasher, hash::Hasher};

#[derive(PlanNode, MatchInputs, OptimizerHints, Explain)]
#[explain(label = "Cache")]
pub struct CacheContext<O: Operand> {
    #[input]
    input: O,
}

impl<O: Operand> CacheContext<O> {
    #[must_use]
    pub const fn new(input: O) -> Self {
        Self { input }
    }
}

impl<O: Operand> Cache for O
where
    for<'a> <O as EvaluateOperand>::ReturnValue<'a>: Cacheable<'a>,
{
    fn cache(&self) -> Self {
        Self::new(CacheContext::new(self.clone()))
    }
}

impl<O: Operand> Cost<O> for CacheContext<O> {
    fn cost(&self, stats: &Stats) -> O::Cost {
        self.input.context().cost(stats)
    }
}

impl<O: Operand> OptimizePlan for CacheContext<O>
where
    for<'a> <O as EvaluateOperand>::ReturnValue<'a>: Cacheable<'a>,
{
    type Output = O;

    fn optimize(&self, original: &Self::Output, session: &Session) -> Transformed<Self::Output> {
        let input = session.optimize(&self.input);

        if !input.changed {
            return Transformed::unchanged(original.clone());
        }

        Transformed {
            value: O::new(Self { input: input.value }),
            changed: true,
        }
    }
}

impl<O: Operand> EvaluateContext for CacheContext<O>
where
    for<'a> <O as EvaluateOperand>::ReturnValue<'a>: Cacheable<'a>,
{
    type Operand = O;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<<O as EvaluateOperand>::ReturnValue<'a>> {
        let mut hasher = DefaultHasher::new();
        self.dyn_hash(&mut hasher);
        let key = hasher.finish();

        cache.materialize(key, || self.input.evaluate(graphrecord, cache))
    }
}
