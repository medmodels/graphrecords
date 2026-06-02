use super::{
    cost::Stats,
    plan::{HasInputs, OptimizeInputs, PlanNode},
    rule::{Rule, Transformed},
};
use crate::Operand;
use std::{cell::RefCell, marker::PhantomData, rc::Rc};

pub trait Pattern<O: Operand> {
    fn matches(&self, operand: &O) -> bool;

    fn reset(&self) {}

    fn rewrite<F>(self, rewrite: F) -> impl Rule<O>
    where
        Self: Sized + 'static,
        O: 'static,
        F: Fn(&Stats) -> Option<O> + 'static,
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

    fn or<Q>(self, other: Q) -> impl Pattern<O>
    where
        Self: Sized,
        Q: Pattern<O>,
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
    F: Fn(&Stats) -> Option<O> + 'static,
{
    fn apply(&self, operand: O, stats: &Stats) -> Transformed<O> {
        self.pattern.reset();

        if !self.pattern.matches(&operand) {
            return Transformed::unchanged(operand);
        }

        match (self.rewrite)(stats) {
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
        F: Fn(&Stats) -> Option<O> + 'static,
    {
        PatternRule {
            pattern: self.pattern,
            rewrite: guarded_rewrite(self.guard, rewrite),
        }
    }
}

fn guarded_rewrite<O>(
    guard: impl Fn(&Stats) -> bool + 'static,
    rewrite: impl Fn(&Stats) -> Option<O> + 'static,
) -> impl Fn(&Stats) -> Option<O> {
    move |stats| if guard(stats) { rewrite(stats) } else { None }
}

struct OrPattern<P, Q> {
    left: P,
    right: Q,
}

impl<O, P, Q> Pattern<O> for OrPattern<P, Q>
where
    O: Operand,
    P: Pattern<O>,
    Q: Pattern<O>,
{
    fn matches(&self, operand: &O) -> bool {
        if self.left.matches(operand) {
            return true;
        }

        self.left.reset();

        self.right.matches(operand)
    }

    fn reset(&self) {
        self.left.reset();
        self.right.reset();
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
    fn matches(&self, operand: &O) -> bool {
        let matched = self.inner.matches(operand);

        self.inner.reset();

        !matched
    }

    fn reset(&self) {
        self.inner.reset();
    }
}

pub struct Wildcard;

#[must_use]
pub const fn any() -> Wildcard {
    Wildcard
}

impl<O: Operand> Pattern<O> for Wildcard {
    fn matches(&self, _operand: &O) -> bool {
        true
    }
}

pub struct Capture<O>(Rc<RefCell<Option<O>>>);

impl<O: Operand + Clone> Default for Capture<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: Operand + Clone> Capture<O> {
    #[must_use]
    pub fn new() -> Self {
        Self(Rc::new(RefCell::new(None)))
    }

    #[must_use]
    pub fn bind(&self) -> Self {
        Self(Rc::clone(&self.0))
    }

    /// # Panics
    ///
    /// Panics if called before a successful match has populated the capture.
    #[must_use]
    pub fn get(&self) -> O {
        self.0
            .borrow()
            .clone()
            .expect("capture is populated after a successful match")
    }
}

impl<O: Operand + Clone> Pattern<O> for Capture<O> {
    fn matches(&self, operand: &O) -> bool {
        let mut slot = self.0.borrow_mut();

        if let Some(existing) = &*slot {
            existing.as_plan_node().dyn_eq(operand.as_plan_node())
        } else {
            *slot = Some(operand.clone());
            true
        }
    }

    fn reset(&self) {
        *self.0.borrow_mut() = None;
    }
}

pub struct Matching<C, Children> {
    children: Children,
    matched: PhantomData<C>,
}

#[must_use]
pub const fn matching<C, Children>(children: Children) -> Matching<C, Children> {
    Matching {
        children,
        matched: PhantomData,
    }
}

impl<C, Children> Pattern<C::Output> for Matching<C, Children>
where
    C: PlanNode + HasInputs + OptimizeInputs,
    Children: for<'a> MatchAgainst<C::Inputs<'a>>,
{
    fn matches(&self, operand: &C::Output) -> bool {
        match operand.downcast::<C>() {
            Some(context) => self.children.match_against(HasInputs::inputs(context)),
            None => false,
        }
    }

    fn reset(&self) {
        self.children.reset_all();
    }
}

impl<C, Children> Matching<C, Children> {
    pub fn rewrite_matched<F>(self, rewrite: F) -> impl Rule<C::Output>
    where
        C: PlanNode + HasInputs + OptimizeInputs,
        Children: for<'a> MatchAgainst<C::Inputs<'a>> + 'static,
        F: Fn(&C, &Stats) -> Option<C::Output> + 'static,
    {
        MatchingRewriteRule {
            pattern: self,
            rewrite,
        }
    }
}

struct MatchingRewriteRule<C, Children, F> {
    pattern: Matching<C, Children>,
    rewrite: F,
}

impl<C, Children, F> Rule<C::Output> for MatchingRewriteRule<C, Children, F>
where
    C: PlanNode + HasInputs + OptimizeInputs,
    Children: for<'a> MatchAgainst<C::Inputs<'a>> + 'static,
    F: Fn(&C, &Stats) -> Option<C::Output> + 'static,
{
    fn apply(&self, operand: C::Output, stats: &Stats) -> Transformed<C::Output> {
        self.pattern.reset();

        if !self.pattern.matches(&operand) {
            return Transformed::unchanged(operand);
        }

        let Some(context) = operand.downcast::<C>() else {
            return Transformed::unchanged(operand);
        };

        match (self.rewrite)(context, stats) {
            Some(rewritten) => Transformed::changed(rewritten),
            None => Transformed::unchanged(operand),
        }
    }
}

pub trait MatchAgainst<Children> {
    fn match_against(&self, children: Children) -> bool;

    fn reset_all(&self);
}

impl MatchAgainst<()> for () {
    fn match_against(&self, _children: ()) -> bool {
        true
    }

    fn reset_all(&self) {}
}

macro_rules! impl_match_against {
    ($($index:tt $operand:ident $pattern:ident),+) => {
        impl<'children, $($operand,)+ $($pattern,)+> MatchAgainst<($(&'children $operand,)+)>
            for ($($pattern,)+)
        where
            $($operand: Operand,)+
            $($pattern: Pattern<$operand>,)+
        {
            fn match_against(&self, children: ($(&$operand,)+)) -> bool {
                true $( && self.$index.matches(children.$index) )+
            }

            fn reset_all(&self) {
                $( self.$index.reset(); )+
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
