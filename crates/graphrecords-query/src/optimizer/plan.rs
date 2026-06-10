use super::{engine::Session, rule::Transformed};
use crate::Operand;
pub use graphrecords_macros::{HasInputs, OptimizeInputs, OptimizerHints, PlanNode};
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

    fn is_distinct(&self) -> bool {
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
    note = "implement `PlanNode` for `{Self}`, or derive it with `#[derive(PlanNode)]`"
)]
pub trait PlanNode: Any + OptimizerHints {
    fn inputs(&self) -> Vec<&dyn PlanNode> {
        Vec::new()
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
        (self as &dyn Any).downcast_ref::<T>()
    }
}

#[diagnostic::on_unimplemented(
    message = "`{Self}` does not expose its inputs",
    note = "implement `HasInputs` for `{Self}`, or derive it with `#[derive(PlanNode)]`"
)]
pub trait HasInputs {
    type Inputs<'a>
    where
        Self: 'a;

    fn inputs(&self) -> Self::Inputs<'_>;
}

#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot produce its operand",
    note = "implement `OptimizeInputs` for `{Self}`, or derive it with `#[derive(PlanNode)]` and a `#[plan_node(operand = \"...\")]` attribute"
)]
pub trait OptimizeInputs {
    type Output: Operand;

    fn optimize_inputs(
        &self,
        original: &Self::Output,
        session: &Session,
    ) -> Transformed<Self::Output>;
}
