use super::{engine::Session, plan::PlanNode, stats::Stats};
use crate::Expression;
use std::marker::PhantomData;

pub(super) type ErasedRule<E> =
    Box<dyn for<'a> Fn(E, &Session<'a>) -> Transformed<E> + Send + Sync>;

pub struct Transformed<T> {
    value: T,
    changed: bool,
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

    #[must_use]
    pub const fn value(&self) -> &T {
        &self.value
    }

    #[must_use]
    pub const fn is_changed(&self) -> bool {
        self.changed
    }

    #[must_use]
    pub fn into_parts(self) -> (T, bool) {
        (self.value, self.changed)
    }
}

pub trait Rule<E: Expression>: 'static + Send + Sync {
    fn apply(&self, expression: E, stats: &Stats) -> Transformed<E>;
}

#[must_use]
pub fn rule<C, E, F>(rewrite: F) -> impl Rule<E>
where
    C: PlanNode,
    E: Expression + 'static,
    F: Fn(&C, &Stats) -> Option<E> + Send + Sync + 'static,
{
    ContextRule {
        rewrite,
        matched: PhantomData,
    }
}

struct ContextRule<C, E, F> {
    rewrite: F,
    matched: PhantomData<fn() -> (C, E)>,
}

impl<C, E, F> Rule<E> for ContextRule<C, E, F>
where
    C: PlanNode,
    E: Expression + 'static,
    F: Fn(&C, &Stats) -> Option<E> + Send + Sync + 'static,
{
    fn apply(&self, expression: E, stats: &Stats) -> Transformed<E> {
        let Some(context) = expression.as_plan_node().downcast::<C>() else {
            return Transformed::unchanged(expression);
        };

        match (self.rewrite)(context, stats) {
            Some(rewritten) => Transformed::changed(rewritten),
            None => Transformed::unchanged(expression),
        }
    }
}
