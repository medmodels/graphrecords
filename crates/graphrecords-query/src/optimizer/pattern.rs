use super::{
    plan::{MatchInputs, OptimizePlan, PlanNode},
    rule::{Rule, Transformed},
    stats::Stats,
};
use crate::Expression;
use std::marker::PhantomData;

pub trait Pattern<E: Expression> {
    type Bindings;

    fn try_match(&self, expression: &E) -> Option<Self::Bindings>;

    fn rewrite<F>(self, rewrite: F) -> impl Rule<E>
    where
        Self: Sized + Send + Sync + 'static,
        E: 'static,
        F: Fn(Self::Bindings, &Stats) -> Option<E> + Send + Sync + 'static,
    {
        PatternRule {
            pattern: self,
            rewrite,
        }
    }

    fn guard<G>(self, guard: G) -> GuardedPattern<Self, G>
    where
        Self: Sized,
        G: Fn(&Stats) -> bool,
    {
        GuardedPattern {
            pattern: self,
            guard,
        }
    }

    fn or<Q>(self, other: Q) -> impl Pattern<E, Bindings = Self::Bindings>
    where
        Self: Sized,
        Q: Pattern<E, Bindings = Self::Bindings>,
    {
        OrPattern {
            left: self,
            right: other,
        }
    }
}

struct PatternRule<P, F> {
    pattern: P,
    rewrite: F,
}

impl<E, P, F> Rule<E> for PatternRule<P, F>
where
    E: Expression,
    P: Pattern<E> + Send + Sync + 'static,
    F: Fn(P::Bindings, &Stats) -> Option<E> + Send + Sync + 'static,
{
    fn apply(&self, expression: E, stats: &Stats) -> Transformed<E> {
        let Some(bindings) = self.pattern.try_match(&expression) else {
            return Transformed::unchanged(expression);
        };

        match (self.rewrite)(bindings, stats) {
            Some(rewritten) => Transformed::changed(rewritten),
            None => Transformed::unchanged(expression),
        }
    }
}

pub struct GuardedPattern<P, G> {
    pattern: P,
    guard: G,
}

impl<P, G> GuardedPattern<P, G> {
    pub fn rewrite<E, F>(self, rewrite: F) -> impl Rule<E>
    where
        E: Expression + 'static,
        P: Pattern<E> + Send + Sync + 'static,
        G: Fn(&Stats) -> bool + Send + Sync + 'static,
        F: Fn(P::Bindings, &Stats) -> Option<E> + Send + Sync + 'static,
    {
        let guard = self.guard;

        PatternRule {
            pattern: self.pattern,
            rewrite: move |bindings, stats: &Stats| {
                if guard(stats) {
                    rewrite(bindings, stats)
                } else {
                    None
                }
            },
        }
    }
}

struct OrPattern<P, Q> {
    left: P,
    right: Q,
}

impl<E: Expression, P: Pattern<E>, Q: Pattern<E, Bindings = P::Bindings>> Pattern<E>
    for OrPattern<P, Q>
{
    type Bindings = P::Bindings;

    fn try_match(&self, expression: &E) -> Option<Self::Bindings> {
        self.left
            .try_match(expression)
            .or_else(|| self.right.try_match(expression))
    }
}

pub struct NotPattern<P> {
    inner: P,
}

#[must_use]
pub const fn not<P>(inner: P) -> NotPattern<P> {
    NotPattern { inner }
}

impl<E: Expression, P: Pattern<E>> Pattern<E> for NotPattern<P> {
    type Bindings = ();

    fn try_match(&self, expression: &E) -> Option<Self::Bindings> {
        self.inner.try_match(expression).is_none().then_some(())
    }
}

pub struct Wildcard;

#[must_use]
pub const fn any() -> Wildcard {
    Wildcard
}

impl<E: Expression> Pattern<E> for Wildcard {
    type Bindings = ();

    fn try_match(&self, _expression: &E) -> Option<Self::Bindings> {
        Some(())
    }
}

pub struct Capture;

#[must_use]
pub const fn capture() -> Capture {
    Capture
}

impl<E: Expression> Pattern<E> for Capture {
    type Bindings = E;

    fn try_match(&self, expression: &E) -> Option<Self::Bindings> {
        Some(expression.clone())
    }
}

pub struct Matching<C, P> {
    patterns: P,
    matched: PhantomData<fn() -> C>,
}

#[must_use]
pub const fn matching<C, P>(patterns: P) -> Matching<C, P> {
    Matching {
        patterns,
        matched: PhantomData,
    }
}

impl<C, P, B> Pattern<C::Output> for Matching<C, P>
where
    C: PlanNode + MatchInputs + OptimizePlan,
    P: for<'a> MatchAgainst<C::Inputs<'a>, Bindings = B>,
{
    type Bindings = B;

    fn try_match(&self, expression: &C::Output) -> Option<Self::Bindings> {
        let context = expression.as_plan_node().downcast::<C>()?;

        self.patterns.match_against(MatchInputs::inputs(context))
    }
}

impl<C, P> Matching<C, P> {
    pub fn rewrite_matched<B, F>(self, rewrite: F) -> impl Rule<C::Output>
    where
        C: PlanNode + MatchInputs + OptimizePlan,
        P: for<'a> MatchAgainst<C::Inputs<'a>, Bindings = B> + Send + Sync + 'static,
        B: 'static,
        F: Fn(&C, B, &Stats) -> Option<C::Output> + Send + Sync + 'static,
    {
        MatchingRewriteRule {
            pattern: self,
            rewrite,
        }
    }
}

struct MatchingRewriteRule<C, P, F> {
    pattern: Matching<C, P>,
    rewrite: F,
}

impl<C, P, B, F> Rule<C::Output> for MatchingRewriteRule<C, P, F>
where
    C: PlanNode + MatchInputs + OptimizePlan,
    P: for<'a> MatchAgainst<C::Inputs<'a>, Bindings = B> + Send + Sync + 'static,
    B: 'static,
    F: Fn(&C, B, &Stats) -> Option<C::Output> + Send + Sync + 'static,
{
    fn apply(&self, expression: C::Output, stats: &Stats) -> Transformed<C::Output> {
        let Some(context) = expression.as_plan_node().downcast::<C>() else {
            return Transformed::unchanged(expression);
        };

        let Some(bindings) = self
            .pattern
            .patterns
            .match_against(MatchInputs::inputs(context))
        else {
            return Transformed::unchanged(expression);
        };

        match (self.rewrite)(context, bindings, stats) {
            Some(rewritten) => Transformed::changed(rewritten),
            None => Transformed::unchanged(expression),
        }
    }
}

pub trait MatchAgainst<I> {
    type Bindings;

    fn match_against(&self, inputs: I) -> Option<Self::Bindings>;
}

impl MatchAgainst<()> for () {
    type Bindings = ();

    fn match_against(&self, _inputs: ()) -> Option<Self::Bindings> {
        Some(())
    }
}

macro_rules! impl_match_against {
    ($($index:tt $expression:ident $pattern:ident),+) => {
        impl<'inputs, $($expression,)+ $($pattern,)+> MatchAgainst<($(&'inputs $expression,)+)>
            for ($($pattern,)+)
        where
            $($expression: Expression,)+
            $($pattern: Pattern<$expression>,)+
        {
            type Bindings = ($(<$pattern as Pattern<$expression>>::Bindings,)+);

            fn match_against(&self, inputs: ($(&$expression,)+)) -> Option<Self::Bindings> {
                Some(($( self.$index.try_match(inputs.$index)?, )+))
            }
        }
    };
}

impl_match_against!(0 O0 P0);
impl_match_against!(0 O0 P0, 1 O1 P1);
impl_match_against!(0 O0 P0, 1 O1 P1, 2 O2 P2);
impl_match_against!(0 O0 P0, 1 O1 P1, 2 O2 P2, 3 O3 P3);
impl_match_against!(0 O0 P0, 1 O1 P1, 2 O2 P2, 3 O3 P3, 4 O4 P4);
impl_match_against!(0 O0 P0, 1 O1 P1, 2 O2 P2, 3 O3 P3, 4 O4 P4, 5 O5 P5);
impl_match_against!(0 O0 P0, 1 O1 P1, 2 O2 P2, 3 O3 P3, 4 O4 P4, 5 O5 P5, 6 O6 P6);
impl_match_against!(0 O0 P0, 1 O1 P1, 2 O2 P2, 3 O3 P3, 4 O4 P4, 5 O5 P5, 6 O6 P6, 7 O7 P7);
impl_match_against!(
    0 O0 P0, 1 O1 P1, 2 O2 P2, 3 O3 P3, 4 O4 P4, 5 O5 P5, 6 O6 P6, 7 O7 P7, 8 O8 P8
);
impl_match_against!(
    0 O0 P0, 1 O1 P1, 2 O2 P2, 3 O3 P3, 4 O4 P4, 5 O5 P5, 6 O6 P6, 7 O7 P7, 8 O8 P8, 9 O9 P9
);
impl_match_against!(
    0 O0 P0, 1 O1 P1, 2 O2 P2, 3 O3 P3, 4 O4 P4, 5 O5 P5, 6 O6 P6, 7 O7 P7, 8 O8 P8, 9 O9 P9,
    10 O10 P10
);
impl_match_against!(
    0 O0 P0, 1 O1 P1, 2 O2 P2, 3 O3 P3, 4 O4 P4, 5 O5 P5, 6 O6 P6, 7 O7 P7, 8 O8 P8, 9 O9 P9,
    10 O10 P10, 11 O11 P11
);
