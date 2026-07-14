use super::{Direction, Optimizer, OptimizerBuilder, Pattern, PhaseLabel, Rule, capture, matching};
use crate::{
    IndexDomain, Operand, OrderState, Ordered, Unordered,
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

fn eliminate_double_negation<I: IndexDomain, O: OrderState>() -> impl Rule<BoolMaskOperand<I, O>> {
    matching::<OperationContext<BoolMaskOperand<I, O>, NotOperation>, _>((matching::<
        OperationContext<BoolMaskOperand<I, O>, NotOperation>,
        _,
    >((capture(),)),))
    .rewrite(|((inner,),), _stats| Some(inner))
}

pub struct SortBelowGroup;

fn sort_below_group<I: IndexDomain>()
-> impl Rule<GroupOperand<ValueOperand<I>, ValuesOperand<I, Unordered>>> {
    matching::<
        OperationContext<
            GroupOperand<ValuesOperand<I, Ordered>, ValuesOperand<I, Unordered>>,
            FirstOperation,
        >,
        _,
    >((matching::<
        OperationContext<ValuesOperand<I, Ordered>, GroupByOperation<ValuesOperand<I, Unordered>>>,
        _,
    >((
        matching::<OperationContext<ValuesOperand<I, Unordered>, SortOperation>, _>((capture(),)),
        capture(),
    )),))
    .rewrite(|(((values,), key),), stats| {
        let grouped = values.group_by(key);

        let per_group_elements = grouped
            .context()
            .estimate(stats)
            .per_group
            .and_then(|inner| inner.elements);

        match per_group_elements {
            Some(elements) if elements > 1 => Some(grouped.sort().first()),
            _ => None,
        }
    })
}

pub struct SortByBelowGroup;

fn sort_by_below_group<I: IndexDomain>()
-> impl Rule<GroupOperand<ValueOperand<I>, ValuesOperand<I, Unordered>>> {
    matching::<
        OperationContext<
            GroupOperand<ValuesOperand<I, Ordered>, ValuesOperand<I, Unordered>>,
            FirstOperation,
        >,
        _,
    >((matching::<
        OperationContext<ValuesOperand<I, Ordered>, GroupByOperation<ValuesOperand<I, Unordered>>>,
        _,
    >((
        matching::<
            OperationContext<
                ValuesOperand<I, Unordered>,
                SortByOperation<ValuesOperand<I, Unordered>>,
            >,
            _,
        >((capture(), capture())),
        capture(),
    )),))
    .rewrite(|(((values, sort_key), group_key),), stats| {
        let grouped = values.group_by(group_key);

        let per_group_elements = grouped
            .context()
            .estimate(stats)
            .per_group
            .and_then(|inner| inner.elements);

        match per_group_elements {
            Some(elements) if elements > 1 => Some(grouped.sort_by(sort_key).first()),
            _ => None,
        }
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
        .add_rule(
            Simplify,
            eliminate_double_negation::<NodeIndex, Unordered>(),
        )
        .label::<EliminateDoubleNegation>();

    builder
        .add_rule(Simplify, eliminate_double_negation::<NodeIndex, Ordered>())
        .label::<EliminateDoubleNegation>();

    builder
        .add_rule(
            Simplify,
            eliminate_double_negation::<EdgeIndex, Unordered>(),
        )
        .label::<EliminateDoubleNegation>();

    builder
        .add_rule(Simplify, eliminate_double_negation::<EdgeIndex, Ordered>())
        .label::<EliminateDoubleNegation>();

    builder
        .add_rule(Reorder, sort_below_group::<NodeIndex>())
        .label::<SortBelowGroup>();

    builder
        .add_rule(Reorder, sort_below_group::<EdgeIndex>())
        .label::<SortBelowGroup>();

    builder
        .add_rule(Reorder, sort_by_below_group::<NodeIndex>())
        .label::<SortByBelowGroup>();

    builder
        .add_rule(Reorder, sort_by_below_group::<EdgeIndex>())
        .label::<SortByBelowGroup>();
}
