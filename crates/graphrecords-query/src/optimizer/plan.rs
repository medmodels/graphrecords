use super::{engine::Session, rule::Transformed};
use crate::Expression;
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

#[diagnostic::on_unimplemented(
    message = "`{Self}` does not declare optimizer hints",
    note = "implement `OptimizerHints` for `{Self}` or derive it with `#[derive(OptimizerHints)]`"
)]
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
    message = "`{Self}` cannot produce its expression",
    note = "implement `OptimizePlan` for `{Self}` or derive it with `#[derive(OptimizePlan)]`",
    note = "the derive requires a `#[plan(expression = ...)]` attribute"
)]
pub trait OptimizePlan {
    type Output: Expression;

    fn optimize(&self, original: &Self::Output, session: &Session) -> Transformed<Self::Output>;
}

pub trait PlanIdentity {
    fn identity_eq(&self, other: &Self) -> bool;

    fn identity_hash<H: Hasher>(&self, state: &mut H);
}

impl<E: Expression> PlanIdentity for E {
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

impl<E: Expression> PlanInputs for E {
    fn inputs(&self) -> Vec<&dyn PlanNode> {
        vec![self.as_plan_node()]
    }

    fn optimize(&self, session: &Session) -> Transformed<Self> {
        session.optimize(self)
    }
}

#[diagnostic::on_unimplemented(
    message = "`{Self}` does not expose its operation inputs",
    note = "implement `OperationInputs` for `{Self}` or derive it with `#[derive(OperationInputs)]`"
)]
pub trait OperationInputs: 'static + PlanIdentity + PlanInputs + OptimizerHints {
    type Inputs<'a, I: 'a>;

    fn inputs<'a, I: 'a>(&'a self, primary: &'a I) -> Self::Inputs<'a, I>;
}
