use super::{Capture, Direction, Optimizer, Pattern, PhaseLabel, Rule, matching};
use crate::{
    EdgeOperand, NodeOperand, RootOperand,
    bool::{BoolMaskOperand, NotContext},
};

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

fn eliminate_double_negation<O: RootOperand>() -> impl Rule<BoolMaskOperand<O>> {
    let inner = Capture::new();

    matching::<NotContext<O>, _>((matching::<NotContext<O>, _>((inner.bind(),)),))
        .rewrite(move |_stats| Some(inner.get()))
}

impl Optimizer {
    #[must_use]
    pub fn builtin() -> Self {
        let mut optimizer = Self::new();

        register_builtins(&mut optimizer);

        optimizer
    }
}

pub fn register_builtins(optimizer: &mut Optimizer) {
    use BuiltinPhase::{Cse, Graph, Limit, Pushdown, Reorder, Simplify, Source};

    optimizer
        .add_phase(Source)
        .direction(Direction::TopDown)
        .fixpoint();
    optimizer
        .add_phase(Simplify)
        .direction(Direction::BottomUp)
        .fixpoint()
        .after(Source);
    optimizer
        .add_phase(Reorder)
        .direction(Direction::BottomUp)
        .fixpoint()
        .after(Simplify);
    optimizer
        .add_phase(Pushdown)
        .direction(Direction::TopDown)
        .fixpoint()
        .after(Reorder);
    optimizer
        .add_phase(Cse)
        .direction(Direction::Manual)
        .once()
        .after(Pushdown);
    optimizer
        .add_phase(Limit)
        .direction(Direction::TopDown)
        .fixpoint()
        .after(Cse);
    optimizer
        .add_phase(Graph)
        .direction(Direction::BottomUp)
        .fixpoint()
        .after(Limit);

    optimizer
        .add_rule(Simplify, eliminate_double_negation::<NodeOperand>())
        .label::<EliminateDoubleNegation>();

    optimizer
        .add_rule(Simplify, eliminate_double_negation::<EdgeOperand>())
        .label::<EliminateDoubleNegation>();
}
