use super::{Direction, Optimizer, OptimizerBuilder, Pattern, PhaseLabel, Rule, capture, matching};
use crate::{
    IndexDomain, OrderState, Ordered, Unordered,
    operands::BoolMaskOperand,
    operations::{NotOperation, OperationContext},
};
use graphrecords_core::graphrecord::{EdgeIndex, NodeIndex};
use std::sync::OnceLock;

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
    matching::<OperationContext<_, NotOperation>, _>((matching::<
        OperationContext<_, NotOperation>,
        _,
    >((capture(),)),))
    .rewrite(|((inner,),), _| Some(inner))
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

    #[must_use]
    pub fn shared_builtin() -> &'static Self {
        static BUILTIN: OnceLock<Optimizer> = OnceLock::new();

        BUILTIN.get_or_init(Self::builtin)
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
}
