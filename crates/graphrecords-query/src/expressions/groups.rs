use super::{DefiniteElementExpression, ElementExpression, ElementsExpression, Expression};
use crate::{
    EvaluateContext, EvaluateExpression, Explain, QueryResult, Unordered,
    execution::EvaluationCache,
    optimizer::{
        Count, CountKind, Estimate, Estimated, MatchInputs, OptimizePlan, OptimizerHints, PlanNode,
        Stats,
    },
};
use graphrecords_core::{GraphRecord, StateView, graphrecord::GroupIndex};

pub type GroupsExpression<O> = ElementsExpression<GroupIndex, O>;
pub type GroupExpression = ElementExpression<GroupIndex>;
pub type DefiniteGroupExpression = DefiniteElementExpression<GroupIndex>;

#[derive(PlanNode, MatchInputs, OptimizePlan, OptimizerHints, Explain)]
#[plan(expression = GroupsExpression<Unordered>)]
pub struct AllGroups;

#[must_use]
pub fn groups() -> GroupsExpression<Unordered> {
    GroupsExpression::new(AllGroups)
}

impl Estimated for AllGroups {
    fn estimate(&self, stats: &Stats) -> Estimate {
        let groups = stats.get::<Count>(&CountKind::Groups);

        Estimate::values(groups, groups)
    }
}

impl EvaluateContext for AllGroups {
    type Expression = GroupsExpression<Unordered>;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<<Self::Expression as EvaluateExpression>::ReturnValue<'a>> {
        Ok(Box::new(
            StateView::of(graphrecord)
                .group_addresses()
                .map(|address| (address, Ok(()))),
        ))
    }
}
