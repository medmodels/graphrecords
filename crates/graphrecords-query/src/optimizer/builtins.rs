use super::{
    Direction, MatchInputs, OperationInputs, Optimizer, OptimizerBuilder, Pattern, PhaseLabel,
    Rule, capture, matching, rule,
};
#[cfg(feature = "dynamic")]
use crate::dynamic::register_dyn_builtins;
use crate::{
    Arity, Bare, ElementShape, Indexed, Mask, Multiple, Operand, Ordered, Unordered,
    element::{ElementTransition, Preserving},
    operands::{BoolMaskOperand, OperandHandle},
    operations::{
        Apply, DiscardIndexOperation, DiscardValueOperation, ElementKernel, NotOperation,
        OperationContext, TakeOperation,
    },
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

fn eliminate_double_negation<O: Apply<NotOperation, Output = O>>() -> impl Rule<O> {
    matching::<OperationContext<_, NotOperation>, _>((matching::<
        OperationContext<_, NotOperation>,
        _,
    >((capture(),)),))
    .rewrite(|((inner,),), _| Some(inner))
}

pub struct PushDownTake;

fn push_down_take<S, C, P>() -> impl Rule<OperandHandle<P::OutShape, C>>
where
    S: ElementShape + ElementTransition<P::OutShape, Preserving>,
    C: Arity,
    P: ElementKernel<S, Emission = Preserving>
        + for<'a> OperationInputs<Inputs<'a, OperandHandle<S, C>> = (&'a OperandHandle<S, C>,)>,
    OperandHandle<S, C>: Apply<TakeOperation, Output = OperandHandle<S, C>>,
    OperandHandle<P::OutShape, C>: Apply<TakeOperation, Output = OperandHandle<P::OutShape, C>>,
{
    rule(
        |outer: &OperationContext<OperandHandle<P::OutShape, C>, TakeOperation>,
         _|
         -> Option<OperandHandle<P::OutShape, C>> {
            let (operand,) = MatchInputs::inputs(outer);
            let inner = operand
                .as_plan_node()
                .downcast::<OperationContext<OperandHandle<S, C>, P>>()?;

            if !inner.operation().allows_limit_pushdown() {
                return None;
            }

            let (input,) = MatchInputs::inputs(inner);
            let taken: OperandHandle<S, C> = Operand::new(OperationContext::new(
                input.clone(),
                outer.operation().clone(),
            ));

            let pushed =
                OperationContext::<OperandHandle<S, C>, P>::new(taken, inner.operation().clone());

            Some(Operand::new(pushed))
        },
    )
}

impl Optimizer {
    #[must_use]
    pub fn builtin() -> Self {
        let mut builder = Self::builder();

        register_builtins(&mut builder);

        #[cfg(feature = "dynamic")]
        register_dyn_builtins(&mut builder);

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
            eliminate_double_negation::<BoolMaskOperand<NodeIndex, Unordered>>(),
        )
        .label::<EliminateDoubleNegation>();

    builder
        .add_rule(
            Simplify,
            eliminate_double_negation::<BoolMaskOperand<NodeIndex, Ordered>>(),
        )
        .label::<EliminateDoubleNegation>();

    builder
        .add_rule(
            Simplify,
            eliminate_double_negation::<BoolMaskOperand<EdgeIndex, Unordered>>(),
        )
        .label::<EliminateDoubleNegation>();

    builder
        .add_rule(
            Simplify,
            eliminate_double_negation::<BoolMaskOperand<EdgeIndex, Ordered>>(),
        )
        .label::<EliminateDoubleNegation>();

    builder
        .add_rule(
            Limit,
            push_down_take::<Indexed<NodeIndex, Mask>, Multiple<Ordered>, NotOperation>(),
        )
        .label::<PushDownTake>();

    builder
        .add_rule(
            Limit,
            push_down_take::<Indexed<EdgeIndex, Mask>, Multiple<Ordered>, NotOperation>(),
        )
        .label::<PushDownTake>();

    builder
        .add_rule(
            Limit,
            push_down_take::<Bare<Mask>, Multiple<Ordered>, NotOperation>(),
        )
        .label::<PushDownTake>();

    builder
        .add_rule(
            Limit,
            push_down_take::<Indexed<NodeIndex, Mask>, Multiple<Ordered>, DiscardIndexOperation>(),
        )
        .label::<PushDownTake>();

    builder
        .add_rule(
            Limit,
            push_down_take::<Indexed<EdgeIndex, Mask>, Multiple<Ordered>, DiscardIndexOperation>(),
        )
        .label::<PushDownTake>();

    builder
        .add_rule(
            Limit,
            push_down_take::<Indexed<NodeIndex, Mask>, Multiple<Ordered>, DiscardValueOperation>(),
        )
        .label::<PushDownTake>();

    builder
        .add_rule(
            Limit,
            push_down_take::<Indexed<EdgeIndex, Mask>, Multiple<Ordered>, DiscardValueOperation>(),
        )
        .label::<PushDownTake>();
}
