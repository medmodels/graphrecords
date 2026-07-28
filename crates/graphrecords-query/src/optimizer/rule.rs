use super::{engine::Session, plan::PlanNode, stats::Stats};
use crate::Operand;
use std::marker::PhantomData;

pub(super) type ErasedRule<O> =
    Box<dyn for<'a> Fn(O, &Session<'a>) -> Transformed<O> + Send + Sync>;

pub struct Transformed<T> {
    pub value: T,
    pub changed: bool,
}

impl<T> Transformed<T> {
    #[must_use]
    pub const fn changed(value: T) -> Self {
        Self {
            value,
            changed: true,
        }
    }

    #[must_use]
    pub const fn unchanged(value: T) -> Self {
        Self {
            value,
            changed: false,
        }
    }
}

pub trait Rule<O: Operand>: 'static + Send + Sync {
    fn apply(&self, operand: O, stats: &Stats) -> Transformed<O>;
}

#[must_use]
pub fn rule<C, O, F>(rewrite: F) -> impl Rule<O>
where
    C: PlanNode,
    O: Operand + 'static,
    F: Fn(&C, &Stats) -> Option<O> + Send + Sync + 'static,
{
    ContextRule {
        rewrite,
        matched: PhantomData,
    }
}

struct ContextRule<C, O, F> {
    rewrite: F,
    matched: PhantomData<fn() -> (C, O)>,
}

impl<C, O, F> Rule<O> for ContextRule<C, O, F>
where
    C: PlanNode,
    O: Operand + 'static,
    F: Fn(&C, &Stats) -> Option<O> + Send + Sync + 'static,
{
    fn apply(&self, operand: O, stats: &Stats) -> Transformed<O> {
        let Some(context) = operand.as_plan_node().downcast::<C>() else {
            return Transformed::unchanged(operand);
        };

        match (self.rewrite)(context, stats) {
            Some(rewritten) => Transformed::changed(rewritten),
            None => Transformed::unchanged(operand),
        }
    }
}
