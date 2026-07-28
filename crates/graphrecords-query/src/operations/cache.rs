use crate::{
    Cache, EvaluateContext, EvaluateOperand, Explain, Failure, Labeled, QueryResult,
    execution::{CacheSlot, CacheableOperand, EvaluationCache, EvaluationCacheGraphRecordMismatch},
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
pub struct CacheContext<O: CacheableOperand> {
    #[input]
    input: O,
    slot: CacheSlot,
}

impl<O: CacheableOperand> CacheContext<O> {
    #[must_use]
    pub fn new(input: O) -> Self {
        Self {
            input,
            slot: CacheSlot::new(),
        }
    }
}

impl<O: CacheableOperand> Cache for O {
    fn cache(&self) -> Self {
        Self::new(CacheContext::new(self.clone()))
    }
}

impl<O: CacheableOperand> PlanNode for CacheContext<O> {
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
        Any::type_id(self).hash(&mut state);
        self.input.as_plan_node().dyn_hash(state);
    }
}

impl<O: CacheableOperand> Estimated for CacheContext<O> {
    fn estimate(&self, stats: &Stats) -> Estimate {
        self.input.context().estimate(stats)
    }
}

impl<O: CacheableOperand> OptimizePlan for CacheContext<O> {
    type Output = O;

    fn optimize(&self, original: &Self::Output, session: &Session) -> Transformed<Self::Output> {
        let input = session.optimize(&self.input);

        if !input.changed {
            return Transformed::unchanged(original.clone());
        }

        Transformed {
            value: O::new(Self {
                input: input.value,
                slot: self.slot.clone(),
            }),
            changed: true,
        }
    }
}

impl<O: CacheableOperand> EvaluateContext for CacheContext<O> {
    type Operand = O;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<<O as EvaluateOperand>::ReturnValue<'a>> {
        if !cache.is_bound_to(graphrecord) {
            return Err(Failure::new(
                Self::LABEL,
                EvaluationCacheGraphRecordMismatch,
            ));
        }

        cache.materialize::<O>(&self.slot, || self.input.evaluate(graphrecord, cache))
    }
}
