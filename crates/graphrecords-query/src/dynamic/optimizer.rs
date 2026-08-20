use super::{
    DynArity, DynElementOperation, DynGroupHandle, DynGroupedOperationContext, DynIndex,
    DynLaneOperation, DynPayloadOutput, DynStreamShape, DynValue,
};
use crate::{
    Bare, Definite, ElementShape, Expression, Indexed, Mask, Multiple, Ordered, Single, Unit,
    Unordered,
    element::{ElementTransition, Preserving},
    expressions::ExpressionHandle,
    operations::{
        Apply, DiscardIndexOperation, DiscardValueOperation, ElementKernel, NotOperation,
        OperationContext, TakeOperation,
    },
    optimizer::{
        BuiltinPhase, EliminateDoubleNegation, MatchInputs, OperationInputs, OptimizerBuilder,
        OptimizerHints, Pattern, PushDownTake, Rule, capture, matching, rule,
    },
};

type ErasedPreserving<P, S, T> = DynElementOperation<P, S, T, Preserving>;

type ErasedTake<S, C> = DynLaneOperation<TakeOperation, S, C, ExpressionHandle<S, C>>;

fn eliminate_erased_double_negation<S, C>() -> impl Rule<ExpressionHandle<S, C>>
where
    S: ElementShape + ElementTransition<S, Preserving>,
    C: DynArity,
    ErasedPreserving<NotOperation, S, S>: ElementKernel<S, Emission = Preserving, OutShape = S>
        + for<'a> OperationInputs<Inputs<'a, ExpressionHandle<S, C>> = (&'a ExpressionHandle<S, C>,)>,
{
    matching::<OperationContext<ExpressionHandle<S, C>, ErasedPreserving<NotOperation, S, S>>, _>((
        matching::<OperationContext<ExpressionHandle<S, C>, ErasedPreserving<NotOperation, S, S>>, _>(
            (capture(),),
        ),
    ))
    .rewrite(|((inner,),), _| Some(inner))
}

fn eliminate_grouped_double_negation<S, C>() -> impl Rule<DynGroupHandle>
where
    S: DynStreamShape + ElementTransition<S, Preserving>,
    C: DynArity,
    ErasedPreserving<NotOperation, S, S>: ElementKernel<S, Emission = Preserving, OutShape = S>
        + for<'a> OperationInputs<Inputs<'a, DynGroupHandle> = (&'a DynGroupHandle,)>,
    ExpressionHandle<S, C>: DynPayloadOutput,
{
    matching::<DynGroupedOperationContext<ErasedPreserving<NotOperation, S, S>, S, C>, _>((
        matching::<DynGroupedOperationContext<ErasedPreserving<NotOperation, S, S>, S, C>, _>((
            capture(),
        )),
    ))
    .rewrite(|((inner,),), _| Some(inner))
}

fn push_down_erased_take<S, T, C, P>() -> impl Rule<ExpressionHandle<T, C>>
where
    S: ElementShape + ElementTransition<T, Preserving>,
    T: ElementShape,
    C: DynArity,
    ErasedPreserving<P, S, T>: ElementKernel<S, Emission = Preserving, OutShape = T>
        + for<'a> OperationInputs<Inputs<'a, ExpressionHandle<S, C>> = (&'a ExpressionHandle<S, C>,)>,
    ExpressionHandle<S, C>: Apply<ErasedTake<S, C>, Output = ExpressionHandle<S, C>>,
    ExpressionHandle<T, C>: Apply<ErasedTake<T, C>, Output = ExpressionHandle<T, C>>,
{
    rule(
        |outer: &OperationContext<ExpressionHandle<T, C>, ErasedTake<T, C>>, _| {
            let (expression,) = MatchInputs::inputs(outer);
            let inner = expression
                .as_plan_node()
                .downcast::<OperationContext<ExpressionHandle<S, C>, ErasedPreserving<P, S, T>>>(
                )?;

            if !inner.operation().allows_limit_pushdown() {
                return None;
            }

            let (input,) = MatchInputs::inputs(inner);
            let taken = Expression::new(OperationContext::new(
                input.clone(),
                ErasedTake::new(outer.operation().operation().clone()),
            ));

            let pushed = OperationContext::<ExpressionHandle<S, C>, ErasedPreserving<P, S, T>>::new(
                taken,
                inner.operation().clone(),
            );

            Some(Expression::new(pushed))
        },
    )
}

pub fn register_dyn_builtins(builder: &mut OptimizerBuilder) {
    use BuiltinPhase::{Limit, Simplify};

    builder
        .add_rule(
            Simplify,
            eliminate_erased_double_negation::<Indexed<DynIndex, Mask>, Multiple<Ordered>>(),
        )
        .label::<EliminateDoubleNegation>();

    builder
        .add_rule(
            Simplify,
            eliminate_erased_double_negation::<Indexed<DynIndex, Mask>, Multiple<Unordered>>(),
        )
        .label::<EliminateDoubleNegation>();

    builder
        .add_rule(
            Simplify,
            eliminate_erased_double_negation::<Indexed<DynIndex, Mask>, Single>(),
        )
        .label::<EliminateDoubleNegation>();

    builder
        .add_rule(
            Simplify,
            eliminate_erased_double_negation::<Indexed<DynIndex, Mask>, Definite>(),
        )
        .label::<EliminateDoubleNegation>();

    builder
        .add_rule(
            Simplify,
            eliminate_erased_double_negation::<Bare<Mask>, Multiple<Ordered>>(),
        )
        .label::<EliminateDoubleNegation>();

    builder
        .add_rule(
            Simplify,
            eliminate_erased_double_negation::<Bare<Mask>, Multiple<Unordered>>(),
        )
        .label::<EliminateDoubleNegation>();

    builder
        .add_rule(
            Simplify,
            eliminate_erased_double_negation::<Bare<Mask>, Single>(),
        )
        .label::<EliminateDoubleNegation>();

    builder
        .add_rule(
            Simplify,
            eliminate_erased_double_negation::<Bare<Mask>, Definite>(),
        )
        .label::<EliminateDoubleNegation>();

    builder
        .add_rule(
            Simplify,
            eliminate_grouped_double_negation::<Indexed<DynIndex, Mask>, Multiple<Ordered>>(),
        )
        .label::<EliminateDoubleNegation>();

    builder
        .add_rule(
            Simplify,
            eliminate_grouped_double_negation::<Indexed<DynIndex, Mask>, Multiple<Unordered>>(),
        )
        .label::<EliminateDoubleNegation>();

    builder
        .add_rule(
            Simplify,
            eliminate_grouped_double_negation::<Indexed<DynIndex, Mask>, Single>(),
        )
        .label::<EliminateDoubleNegation>();

    builder
        .add_rule(
            Simplify,
            eliminate_grouped_double_negation::<Indexed<DynIndex, Mask>, Definite>(),
        )
        .label::<EliminateDoubleNegation>();

    builder
        .add_rule(
            Simplify,
            eliminate_grouped_double_negation::<Bare<Mask>, Multiple<Ordered>>(),
        )
        .label::<EliminateDoubleNegation>();

    builder
        .add_rule(
            Simplify,
            eliminate_grouped_double_negation::<Bare<Mask>, Multiple<Unordered>>(),
        )
        .label::<EliminateDoubleNegation>();

    builder
        .add_rule(
            Simplify,
            eliminate_grouped_double_negation::<Bare<Mask>, Single>(),
        )
        .label::<EliminateDoubleNegation>();

    builder
        .add_rule(
            Simplify,
            eliminate_grouped_double_negation::<Bare<Mask>, Definite>(),
        )
        .label::<EliminateDoubleNegation>();

    builder
        .add_rule(
            Limit,
            push_down_erased_take::<
                Indexed<DynIndex, Mask>,
                Indexed<DynIndex, Mask>,
                Multiple<Ordered>,
                NotOperation,
            >(),
        )
        .label::<PushDownTake>();

    builder
        .add_rule(
            Limit,
            push_down_erased_take::<Bare<Mask>, Bare<Mask>, Multiple<Ordered>, NotOperation>(),
        )
        .label::<PushDownTake>();

    builder
        .add_rule(
            Limit,
            push_down_erased_take::<
                Indexed<DynIndex, DynValue>,
                Bare<DynValue>,
                Multiple<Ordered>,
                DiscardIndexOperation,
            >(),
        )
        .label::<PushDownTake>();

    builder
        .add_rule(
            Limit,
            push_down_erased_take::<
                Indexed<DynIndex, Mask>,
                Bare<Mask>,
                Multiple<Ordered>,
                DiscardIndexOperation,
            >(),
        )
        .label::<PushDownTake>();

    builder
        .add_rule(
            Limit,
            push_down_erased_take::<
                Indexed<DynIndex, DynValue>,
                Indexed<DynIndex, Unit>,
                Multiple<Ordered>,
                DiscardValueOperation,
            >(),
        )
        .label::<PushDownTake>();

    builder
        .add_rule(
            Limit,
            push_down_erased_take::<
                Indexed<DynIndex, Mask>,
                Indexed<DynIndex, Unit>,
                Multiple<Ordered>,
                DiscardValueOperation,
            >(),
        )
        .label::<PushDownTake>();

    builder
        .add_rule(
            Limit,
            push_down_erased_take::<
                Indexed<DynIndex, Unit>,
                Indexed<DynIndex, Unit>,
                Multiple<Ordered>,
                DiscardValueOperation,
            >(),
        )
        .label::<PushDownTake>();
}
