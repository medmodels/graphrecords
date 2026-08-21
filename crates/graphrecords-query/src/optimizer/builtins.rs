use super::{
    Direction, MatchInputs, OperationInputs, Optimizer, OptimizerBuilder, Pattern, PhaseLabel,
    Rule, capture, matching, rule,
};
#[cfg(feature = "dynamic")]
use crate::dynamic::register_dyn_builtins;
use crate::{
    Arity, Bare, ElementShape, Expression, Indexed, Mask, Multiple, Ordered, Unordered,
    element::{ElementTransition, Preserving},
    expressions::{BoolMaskExpression, ExpressionHandle},
    operations::{
        Apply, DiscardIndexOperation, DiscardValueOperation, ElementKernel, NotOperation,
        OperationContext, TakeOperation,
    },
};
use graphrecords_core::graphrecord::{EdgeIndex, NodeIndex};
use std::sync::{Arc, OnceLock};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, PhaseLabel)]
pub enum BuiltinPhase {
    Source,
    Simplify,
    Reorder,
    Pushdown,
    CommonSubexpressionElimination,
    Limit,
    Graph,
}

pub struct EliminateDoubleNegation;

fn eliminate_double_negation<E: Apply<NotOperation, Output = E>>() -> impl Rule<E> {
    matching::<OperationContext<_, NotOperation>, _>((matching::<
        OperationContext<_, NotOperation>,
        _,
    >((capture(),)),))
    .rewrite(|((inner,),), _| Some(inner))
}

pub struct PushDownTake;

fn push_down_take<S, C, P>() -> impl Rule<ExpressionHandle<P::OutShape, C>>
where
    S: ElementShape + ElementTransition<P::OutShape, Preserving>,
    C: Arity,
    P: ElementKernel<S, Emission = Preserving>
        + for<'a> OperationInputs<Inputs<'a, ExpressionHandle<S, C>> = (&'a ExpressionHandle<S, C>,)>,
    ExpressionHandle<S, C>: Apply<TakeOperation, Output = ExpressionHandle<S, C>>,
    ExpressionHandle<P::OutShape, C>:
        Apply<TakeOperation, Output = ExpressionHandle<P::OutShape, C>>,
{
    rule(
        |outer: &OperationContext<ExpressionHandle<P::OutShape, C>, TakeOperation>,
         _|
         -> Option<ExpressionHandle<P::OutShape, C>> {
            let (expression,) = MatchInputs::inputs(outer);
            let inner = expression
                .as_plan_node()
                .downcast::<OperationContext<ExpressionHandle<S, C>, P>>()?;

            if !inner.operation().allows_limit_pushdown() {
                return None;
            }

            let (input,) = MatchInputs::inputs(inner);
            let taken: ExpressionHandle<S, C> = Expression::new(OperationContext::new(
                input.clone(),
                outer.operation().clone(),
            ));

            let pushed = OperationContext::<ExpressionHandle<S, C>, P>::new(
                taken,
                inner.operation().clone(),
            );

            Some(Expression::new(pushed))
        },
    )
}

impl Optimizer {
    #[must_use]
    pub fn builtin() -> &'static Arc<Self> {
        static BUILTIN: OnceLock<Arc<Optimizer>> = OnceLock::new();

        BUILTIN.get_or_init(|| Arc::new(Self::build_builtin()))
    }

    fn build_builtin() -> Self {
        let mut builder = Self::builder();

        register_builtins(&mut builder);

        #[cfg(feature = "dynamic")]
        register_dyn_builtins(&mut builder);

        builder
            .build()
            .expect("Builtin phases and rules must form a valid optimizer")
    }
}

pub fn register_builtins(builder: &mut OptimizerBuilder) {
    use BuiltinPhase::{
        CommonSubexpressionElimination, Graph, Limit, Pushdown, Reorder, Simplify, Source,
    };

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
        .add_phase(CommonSubexpressionElimination)
        .direction(Direction::Manual)
        .once()
        .after(Pushdown);
    builder
        .add_phase(Limit)
        .direction(Direction::TopDown)
        .fixpoint()
        .after(CommonSubexpressionElimination);
    builder
        .add_phase(Graph)
        .direction(Direction::BottomUp)
        .fixpoint()
        .after(Limit);

    builder
        .add_rule(
            Simplify,
            eliminate_double_negation::<BoolMaskExpression<NodeIndex, Unordered>>(),
        )
        .label::<EliminateDoubleNegation>();

    builder
        .add_rule(
            Simplify,
            eliminate_double_negation::<BoolMaskExpression<NodeIndex, Ordered>>(),
        )
        .label::<EliminateDoubleNegation>();

    builder
        .add_rule(
            Simplify,
            eliminate_double_negation::<BoolMaskExpression<EdgeIndex, Unordered>>(),
        )
        .label::<EliminateDoubleNegation>();

    builder
        .add_rule(
            Simplify,
            eliminate_double_negation::<BoolMaskExpression<EdgeIndex, Ordered>>(),
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
