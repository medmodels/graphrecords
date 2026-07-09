use super::{Direction, Optimizer, OptimizerBuilder, Pattern, PhaseLabel, Rule, capture, matching};
use crate::{
    IndexDomain, Operand, Sorted,
    operands::{BoolMaskOperand, GroupOperand, ValueOperand, ValuesOperand},
    operations::{
        FirstOperation, GroupByOperation, NotOperation, OperationContext, SortByOperation,
        SortOperation,
    },
    traits::{First, GroupBy, Sort, SortBy},
};
use graphrecords_core::graphrecord::{EdgeIndex, NodeIndex};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, PhaseLabel)]
pub enum BuiltinPhase {
    Source,
    Simplify,
    Reorder,
    Pushdown,
    Cse,
    Limit,
    Graph,
}

pub struct EliminateDoubleNegation;

fn eliminate_double_negation<I: IndexDomain>() -> impl Rule<BoolMaskOperand<I>> {
    matching::<OperationContext<BoolMaskOperand<I>, NotOperation>, _>((matching::<
        OperationContext<BoolMaskOperand<I>, NotOperation>,
        _,
    >((capture(),)),))
    .rewrite(|((inner,),), _stats| Some(inner))
}

pub struct SortBelowGroup;

fn sort_below_group_for_nodes()
-> impl Rule<GroupOperand<ValueOperand<NodeIndex>, ValuesOperand<NodeIndex>>> {
    matching::<
        OperationContext<
            GroupOperand<ValuesOperand<NodeIndex, Sorted>, ValuesOperand<NodeIndex>>,
            FirstOperation,
        >,
        _,
    >((matching::<
        OperationContext<
            ValuesOperand<NodeIndex, Sorted>,
            GroupByOperation<ValuesOperand<NodeIndex>>,
        >,
        _,
    >((
        matching::<OperationContext<ValuesOperand<NodeIndex>, SortOperation>, _>((capture(),)),
        capture(),
    )),))
    .rewrite(|(((values,), key),), stats| {
        let grouped = values.group_by(key);

        if grouped.context().cost(stats).per_group.rows().0 <= 1 {
            return None;
        }

        Some(grouped.sort().first())
    })
}

fn sort_below_group_for_edges()
-> impl Rule<GroupOperand<ValueOperand<EdgeIndex>, ValuesOperand<EdgeIndex>>> {
    matching::<
        OperationContext<
            GroupOperand<ValuesOperand<EdgeIndex, Sorted>, ValuesOperand<EdgeIndex>>,
            FirstOperation,
        >,
        _,
    >((matching::<
        OperationContext<
            ValuesOperand<EdgeIndex, Sorted>,
            GroupByOperation<ValuesOperand<EdgeIndex>>,
        >,
        _,
    >((
        matching::<OperationContext<ValuesOperand<EdgeIndex>, SortOperation>, _>((capture(),)),
        capture(),
    )),))
    .rewrite(|(((values,), key),), stats| {
        let grouped = values.group_by(key);

        if grouped.context().cost(stats).per_group.rows().0 <= 1 {
            return None;
        }

        Some(grouped.sort().first())
    })
}

pub struct SortByBelowGroup;

fn sort_by_below_group_for_nodes()
-> impl Rule<GroupOperand<ValueOperand<NodeIndex>, ValuesOperand<NodeIndex>>> {
    matching::<
        OperationContext<
            GroupOperand<ValuesOperand<NodeIndex, Sorted>, ValuesOperand<NodeIndex>>,
            FirstOperation,
        >,
        _,
    >((matching::<
        OperationContext<
            ValuesOperand<NodeIndex, Sorted>,
            GroupByOperation<ValuesOperand<NodeIndex>>,
        >,
        _,
    >((
        matching::<
            OperationContext<ValuesOperand<NodeIndex>, SortByOperation<ValuesOperand<NodeIndex>>>,
            _,
        >((capture(), capture())),
        capture(),
    )),))
    .rewrite(|(((values, sort_key), group_key),), stats| {
        let grouped = values.group_by(group_key);

        if grouped.context().cost(stats).per_group.rows().0 <= 1 {
            return None;
        }

        Some(grouped.sort_by(sort_key).first())
    })
}

fn sort_by_below_group_for_edges()
-> impl Rule<GroupOperand<ValueOperand<EdgeIndex>, ValuesOperand<EdgeIndex>>> {
    matching::<
        OperationContext<
            GroupOperand<ValuesOperand<EdgeIndex, Sorted>, ValuesOperand<EdgeIndex>>,
            FirstOperation,
        >,
        _,
    >((matching::<
        OperationContext<
            ValuesOperand<EdgeIndex, Sorted>,
            GroupByOperation<ValuesOperand<EdgeIndex>>,
        >,
        _,
    >((
        matching::<
            OperationContext<ValuesOperand<EdgeIndex>, SortByOperation<ValuesOperand<EdgeIndex>>>,
            _,
        >((capture(), capture())),
        capture(),
    )),))
    .rewrite(|(((values, sort_key), group_key),), stats| {
        let grouped = values.group_by(group_key);

        if grouped.context().cost(stats).per_group.rows().0 <= 1 {
            return None;
        }

        Some(grouped.sort_by(sort_key).first())
    })
}

impl Optimizer {
    #[must_use]
    pub fn builtin() -> Self {
        let mut builder = Self::builder();

        register_builtins(&mut builder);

        #[allow(clippy::missing_panics_doc)]
        builder
            .build()
            .expect("Builtin phases and rules must form a valid optimizer")
    }
}

pub fn register_builtins(builder: &mut OptimizerBuilder) {
    use BuiltinPhase::{Cse, Graph, Limit, Pushdown, Reorder, Simplify, Source};

    builder
        .add_phase(Source)
        .direction(Direction::TopDown)
        .fixpoint();
    builder
        .add_phase(Simplify)
        .direction(Direction::BottomUp)
        .fixpoint()
        .after(Source);
    builder
        .add_phase(Reorder)
        .direction(Direction::BottomUp)
        .fixpoint()
        .after(Simplify);
    builder
        .add_phase(Pushdown)
        .direction(Direction::TopDown)
        .fixpoint()
        .after(Reorder);
    builder
        .add_phase(Cse)
        .direction(Direction::Manual)
        .once()
        .after(Pushdown);
    builder
        .add_phase(Limit)
        .direction(Direction::TopDown)
        .fixpoint()
        .after(Cse);
    builder
        .add_phase(Graph)
        .direction(Direction::BottomUp)
        .fixpoint()
        .after(Limit);

    builder
        .add_rule(Simplify, eliminate_double_negation::<NodeIndex>())
        .label::<EliminateDoubleNegation>();

    builder
        .add_rule(Simplify, eliminate_double_negation::<EdgeIndex>())
        .label::<EliminateDoubleNegation>();

    builder
        .add_rule(Reorder, sort_below_group_for_nodes())
        .label::<SortBelowGroup>();

    builder
        .add_rule(Reorder, sort_below_group_for_edges())
        .label::<SortBelowGroup>();

    builder
        .add_rule(Reorder, sort_by_below_group_for_nodes())
        .label::<SortByBelowGroup>();

    builder
        .add_rule(Reorder, sort_by_below_group_for_edges())
        .label::<SortByBelowGroup>();
}
