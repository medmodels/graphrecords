use crate::{
    Cache, EvaluateContext, EvaluateExpression, Explain, Failure, Labeled, QueryResult, Series,
    error::execution::EvaluationCacheGraphRecordMismatch,
    execution::{CacheSlot, CacheableExpression, EvaluationCache},
    optimizer::{
        Estimate, Estimated, MatchInputs, OptimizePlan, OptimizerHints, PlanNode, Session, Stats,
        Transformed,
    },
};
use graphrecords_core::GraphRecord;
use std::{
    any::Any,
    hash::{Hash, Hasher},
};

#[derive(MatchInputs, OptimizerHints, Explain)]
#[explain(label = "Cache")]
pub struct CacheContext<E: CacheableExpression> {
    #[input]
    input: E,
    slot: CacheSlot,
}

impl<E: CacheableExpression> CacheContext<E> {
    #[must_use]
    pub fn new(input: E) -> Self {
        Self {
            input,
            slot: CacheSlot::new(),
        }
    }
}

impl<E: CacheableExpression> Cache for E {
    fn cache(&self) -> Self {
        Self::new(CacheContext::new(self.clone()))
    }
}

impl<E: Cache> Cache for Series<E> {
    fn cache(&self) -> Self {
        self.bind(self.expression().cache())
    }
}

impl<E: CacheableExpression> PlanNode for CacheContext<E> {
    fn inputs(&self) -> Vec<&dyn PlanNode> {
        vec![self.input.as_plan_node()]
    }

    fn dyn_eq(&self, other: &dyn PlanNode) -> bool {
        let Some(other) = other.downcast::<Self>() else {
            return false;
        };

        self.input.as_plan_node().dyn_eq(other.input.as_plan_node())
    }

    fn dyn_hash(&self, mut state: &mut dyn Hasher) {
        self.type_id().hash(&mut state);
        self.input.as_plan_node().dyn_hash(state);
    }
}

impl<E: CacheableExpression> Estimated for CacheContext<E> {
    fn estimate(&self, stats: &Stats) -> Estimate {
        self.input.context().estimate(stats)
    }
}

impl<E: CacheableExpression> OptimizePlan for CacheContext<E> {
    type Output = E;

    fn optimize(&self, original: &Self::Output, session: &Session) -> Transformed<Self::Output> {
        let input = session.optimize(&self.input);

        if !input.is_changed() {
            return Transformed::unchanged(original.clone());
        }

        let input = input.into_parts().0;

        Transformed::changed(E::new(Self {
            input,
            slot: self.slot.clone(),
        }))
    }
}

impl<E: CacheableExpression> EvaluateContext for CacheContext<E> {
    type Expression = E;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache,
    ) -> QueryResult<<E as EvaluateExpression>::ReturnValue<'a>> {
        if !cache.is_bound_to(graphrecord) {
            return Err(Failure::new(
                EvaluationCacheGraphRecordMismatch,
                Self::LABEL,
            ));
        }

        cache.materialize::<E>(graphrecord, &self.slot, || {
            self.input.evaluate(graphrecord, cache)
        })
    }
}
