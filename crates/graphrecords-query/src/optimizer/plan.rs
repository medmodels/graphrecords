use super::{engine::Session, rule::Transformed};
use crate::Operand;
pub use graphrecords_macros::{
    MatchInputs, OperationInputs, OptimizePlan, OptimizerHints, PlanIdentity, PlanInputs, PlanNode,
};
use std::{
    any::Any,
    hash::{Hash, Hasher},
};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum EmptyRule {
    Never,
    IfAnyInput,
    IfAllInputs,
}

pub trait OptimizerHints {
    fn commutes_with_filter(&self) -> bool {
        false
    }

    fn allows_limit_pushdown(&self) -> bool {
        false
    }

    fn is_volatile(&self) -> bool {
        false
    }

    fn empty_rule(&self) -> EmptyRule {
        EmptyRule::Never
    }
}

#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a plan node",
    note = "implement `PlanNode` for `{Self}` or derive it with `#[derive(PlanNode)]`"
)]
pub trait PlanNode: Any + OptimizerHints {
    fn inputs(&self) -> Vec<&dyn PlanNode> {
        Vec::new()
    }

    fn contains_volatile(&self) -> bool {
        self.is_volatile() || self.inputs().into_iter().any(PlanNode::contains_volatile)
    }

    #[allow(unused_variables)]
    fn dyn_eq(&self, other: &dyn PlanNode) -> bool {
        false
    }

    fn dyn_hash(&self, mut state: &mut dyn Hasher) {
        self.type_id().hash(&mut state);
    }
}

impl dyn PlanNode {
    pub fn downcast<T: PlanNode>(&self) -> Option<&T> {
        (self as &dyn Any).downcast_ref()
    }
}

#[diagnostic::on_unimplemented(
    message = "`{Self}` does not expose its inputs",
    note = "implement `MatchInputs` for `{Self}` or derive it with `#[derive(MatchInputs)]`"
)]
pub trait MatchInputs {
    type Inputs<'a>
    where
        Self: 'a;

    fn inputs(&self) -> Self::Inputs<'_>;
}

#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot produce its operand",
    note = "implement `OptimizePlan` for `{Self}` or derive it with `#[derive(OptimizePlan)]`",
    note = "the derive requires a `#[plan(operand = ...)]` attribute"
)]
pub trait OptimizePlan {
    type Output: Operand;

    fn optimize(&self, original: &Self::Output, session: &Session) -> Transformed<Self::Output>;
}

pub trait PlanIdentity {
    fn identity_eq(&self, other: &Self) -> bool;

    fn identity_hash<H: Hasher>(&self, state: &mut H);
}

impl<T: Operand> PlanIdentity for T {
    fn identity_eq(&self, other: &Self) -> bool {
        self.as_plan_node().dyn_eq(other.as_plan_node())
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        self.as_plan_node().dyn_hash(state);
    }
}

pub trait PlanInputs: Clone {
    fn inputs(&self) -> Vec<&dyn PlanNode> {
        Vec::new()
    }

    #[allow(unused_variables)]
    fn optimize(&self, session: &Session) -> Transformed<Self> {
        Transformed::unchanged(self.clone())
    }
}

impl<T: Operand> PlanInputs for T {
    fn inputs(&self) -> Vec<&dyn PlanNode> {
        vec![self.as_plan_node()]
    }

    fn optimize(&self, session: &Session) -> Transformed<Self> {
        session.optimize(self)
    }
}

pub trait OperationInputs: 'static + PlanIdentity + PlanInputs + OptimizerHints {
    type Inputs<'a, I: 'a>;

    fn inputs<'a, I: 'a>(&'a self, primary: &'a I) -> Self::Inputs<'a, I>;
}
