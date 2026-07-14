use super::{
    plan::{MatchInputs, OptimizePlan, PlanNode},
    rule::{Rule, Transformed},
    stats::Stats,
};
use crate::Operand;
use std::marker::PhantomData;

pub trait Pattern<O: Operand> {
    type Bindings;

    fn try_match(&self, operand: &O) -> Option<Self::Bindings>;

    fn rewrite<F>(self, rewrite: F) -> impl Rule<O>
    where
        Self: Sized + 'static,
        O: 'static,
        F: Fn(Self::Bindings, &Stats) -> Option<O> + 'static,
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

    fn or<Q>(self, other: Q) -> impl Pattern<O, Bindings = Self::Bindings>
    where
        Self: Sized,
        Q: Pattern<O, Bindings = Self::Bindings>,
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

impl<O, P, F> Rule<O> for PatternRule<P, F>
where
    O: Operand,
    P: Pattern<O> + 'static,
    F: Fn(P::Bindings, &Stats) -> Option<O> + 'static,
{
    fn apply(&self, operand: O, stats: &Stats) -> Transformed<O> {
        let Some(bindings) = self.pattern.try_match(&operand) else {
            return Transformed::unchanged(operand);
        };

        match (self.rewrite)(bindings, stats) {
            Some(rewritten) => Transformed::changed(rewritten),
            None => Transformed::unchanged(operand),
        }
    }
}

pub struct GuardedPattern<P, G> {
    pattern: P,
    guard: G,
}

impl<P, G> GuardedPattern<P, G> {
    pub fn rewrite<O, F>(self, rewrite: F) -> impl Rule<O>
    where
        O: Operand + 'static,
        P: Pattern<O> + 'static,
        G: Fn(&Stats) -> bool + 'static,
        F: Fn(P::Bindings, &Stats) -> Option<O> + 'static,
    {
        let guard = self.guard;

        PatternRule {
            pattern: self.pattern,
            rewrite: move |bindings: P::Bindings, stats: &Stats| {
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

impl<O, P, Q> Pattern<O> for OrPattern<P, Q>
where
    O: Operand,
    P: Pattern<O>,
    Q: Pattern<O, Bindings = P::Bindings>,
{
    type Bindings = P::Bindings;

    fn try_match(&self, operand: &O) -> Option<Self::Bindings> {
        self.left
            .try_match(operand)
            .or_else(|| self.right.try_match(operand))
    }
}

pub struct NotPattern<P> {
    inner: P,
}

#[must_use]
pub const fn not<P>(inner: P) -> NotPattern<P> {
    NotPattern { inner }
}

impl<O, P> Pattern<O> for NotPattern<P>
where
    O: Operand,
    P: Pattern<O>,
{
    type Bindings = ();

    fn try_match(&self, operand: &O) -> Option<Self::Bindings> {
        self.inner.try_match(operand).is_none().then_some(())
    }
}

pub struct Wildcard;

#[must_use]
pub const fn any() -> Wildcard {
    Wildcard
}

impl<O: Operand> Pattern<O> for Wildcard {
    type Bindings = ();

    fn try_match(&self, _operand: &O) -> Option<Self::Bindings> {
        Some(())
    }
}

pub struct Capture;

#[must_use]
pub const fn capture() -> Capture {
    Capture
}

impl<O: Operand> Pattern<O> for Capture {
    type Bindings = O;

    fn try_match(&self, operand: &O) -> Option<Self::Bindings> {
        Some(operand.clone())
    }
}

pub struct Matching<C, P> {
    patterns: P,
    matched: PhantomData<C>,
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

    fn try_match(&self, operand: &C::Output) -> Option<Self::Bindings> {
        let context = operand.downcast::<C>()?;

        self.patterns.match_against(MatchInputs::inputs(context))
    }
}

impl<C, P> Matching<C, P> {
    pub fn rewrite_matched<B, F>(self, rewrite: F) -> impl Rule<C::Output>
    where
        C: PlanNode + MatchInputs + OptimizePlan,
        P: for<'a> MatchAgainst<C::Inputs<'a>, Bindings = B> + 'static,
        B: 'static,
        F: Fn(&C, B, &Stats) -> Option<C::Output> + 'static,
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
    P: for<'a> MatchAgainst<C::Inputs<'a>, Bindings = B> + 'static,
    B: 'static,
    F: Fn(&C, B, &Stats) -> Option<C::Output> + 'static,
{
    fn apply(&self, operand: C::Output, stats: &Stats) -> Transformed<C::Output> {
        let Some(context) = operand.downcast::<C>() else {
            return Transformed::unchanged(operand);
        };

        let Some(bindings) = self
            .pattern
            .patterns
            .match_against(MatchInputs::inputs(context))
        else {
            return Transformed::unchanged(operand);
        };

        match (self.rewrite)(context, bindings, stats) {
            Some(rewritten) => Transformed::changed(rewritten),
            None => Transformed::unchanged(operand),
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
    ($($index:tt $operand:ident $pattern:ident),+) => {
        impl<'inputs, $($operand,)+ $($pattern,)+> MatchAgainst<($(&'inputs $operand,)+)>
            for ($($pattern,)+)
        where
            $($operand: Operand,)+
            $($pattern: Pattern<$operand>,)+
        {
            type Bindings = ($(<$pattern as Pattern<$operand>>::Bindings,)+);

            fn match_against(&self, inputs: ($(&$operand,)+)) -> Option<Self::Bindings> {
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
