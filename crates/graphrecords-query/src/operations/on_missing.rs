use crate::{
    Explain, Failure, IndexDomain, OrderState, QueryResult,
    execution::EvaluationCache,
    explain::ExplainFormatter,
    operands::{
        BareValueOperand, BoolMaskOperand, NestedBoolMaskOperand, ValueOperand, ValuesOperand,
    },
    operations::{Absent, ArgumentSource, Drop, Keyed, Looked, OnMissing, Prepare, Raise, Replace},
    optimizer::{PlanIdentity, PlanInputs, PlanNode},
    traits::MaybeAbsent,
};
use graphrecords_core::GraphRecord;
use std::{fmt, hash::Hasher, marker::PhantomData};

pub trait MissingPolicy<I: IndexDomain, S: ArgumentSource<Keyed<I>>>:
    Clone + 'static + Explain + PlanIdentity + PlanInputs
{
    type Prepared<'a>: Clone + 'a
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>>;

    fn resolve_absent<'a>(
        prepared: &Self::Prepared<'a>,
        index: &I::Index<'a>,
        label: &'static str,
        absent: Absent,
    ) -> QueryResult<Option<S::Value<'a>>>;
}

impl<I: IndexDomain, S: ArgumentSource<Keyed<I>>> MissingPolicy<I, S> for Drop {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<()> {
        Ok(())
    }

    fn resolve_absent<'a>(
        _prepared: &(),
        _index: &I::Index<'a>,
        _label: &'static str,
        _absent: Absent,
    ) -> QueryResult<Option<S::Value<'a>>> {
        Ok(None)
    }
}

impl<I: IndexDomain, S: ArgumentSource<Keyed<I>>> MissingPolicy<I, S> for Raise {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<()> {
        Ok(())
    }

    fn resolve_absent<'a>(
        _prepared: &(),
        index: &I::Index<'a>,
        label: &'static str,
        absent: Absent,
    ) -> QueryResult<Option<S::Value<'a>>> {
        Err(Failure::new(label, absent).at(index).help(
            "the argument has no value at this index; supply `on_missing(Drop)` or `on_missing(Replace(...))`",
        ))
    }
}

impl<I, S, R> MissingPolicy<I, S> for Replace<R>
where
    I: IndexDomain,
    R: ArgumentSource<Keyed<I>> + Clone,
    for<'a> S: ArgumentSource<Keyed<I>, Value<'a> = R::Value<'a>>,
{
    type Prepared<'a> = R::Prepared<'a>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<R::Prepared<'a>> {
        self.0.prepare(graphrecord, cache)
    }

    fn resolve_absent<'a>(
        prepared: &R::Prepared<'a>,
        index: &I::Index<'a>,
        label: &'static str,
        _absent: Absent,
    ) -> QueryResult<Option<S::Value<'a>>> {
        match R::lookup(prepared, index) {
            Looked::Present(wrapped) => wrapped.clone().map(Some),
            Looked::Absent(absent) => Err(Failure::new(label, absent)
                .at(index)
                .help("the replacement operand has no value at this index")),
        }
    }
}

impl<I: IndexDomain, O: OrderState> MaybeAbsent<I> for ValuesOperand<I, O> {}
impl<I1: IndexDomain, I2: IndexDomain> MaybeAbsent<I1> for ValueOperand<I2> {}
impl<I: IndexDomain> MaybeAbsent<I> for BareValueOperand {}
impl<I: IndexDomain, O: OrderState> MaybeAbsent<I> for BoolMaskOperand<I, O> {}
impl<I: IndexDomain, T: 'static + Clone, O: OrderState> MaybeAbsent<I>
    for NestedBoolMaskOperand<I, T, O>
{
}

pub struct WithMissing<I: IndexDomain, S: MaybeAbsent<I>, P> {
    inner: S,
    policy: P,
    index: PhantomData<fn() -> I>,
}

impl<I: IndexDomain, S: MaybeAbsent<I>, P> WithMissing<I, S, P> {
    #[must_use]
    pub fn new(inner: S, policy: P) -> Self {
        Self {
            inner,
            policy,
            index: PhantomData,
        }
    }

    pub(crate) const fn inner(&self) -> &S {
        &self.inner
    }
}

impl<I: IndexDomain, S: MaybeAbsent<I> + Clone, P: Clone> Clone for WithMissing<I, S, P> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            policy: self.policy.clone(),
            index: PhantomData,
        }
    }
}

impl<I: IndexDomain, S: MaybeAbsent<I>, P: Explain> Explain for WithMissing<I, S, P> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        self.inner.describe(formatter)?;
        fmt::Write::write_str(formatter, " on_missing(")?;
        self.policy.describe(formatter)?;
        fmt::Write::write_str(formatter, ")")
    }
}

impl<I: IndexDomain, S: MaybeAbsent<I>, P: PlanIdentity> PlanIdentity for WithMissing<I, S, P> {
    fn identity_eq(&self, other: &Self) -> bool {
        self.inner.identity_eq(&other.inner) && self.policy.identity_eq(&other.policy)
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        self.inner.identity_hash(state);
        self.policy.identity_hash(state);
    }
}

impl<I: IndexDomain, S: MaybeAbsent<I>, P: PlanInputs> PlanInputs for WithMissing<I, S, P> {
    fn inputs(&self) -> Vec<&dyn PlanNode> {
        let mut inputs = PlanInputs::inputs(&self.inner);
        inputs.extend(PlanInputs::inputs(&self.policy));

        inputs
    }
}

impl<I: IndexDomain, S: MaybeAbsent<I> + Clone, P> Prepare for WithMissing<I, S, P>
where
    P: MissingPolicy<I, S>,
{
    type Prepared<'a>
        = (S::Prepared<'a>, P::Prepared<'a>)
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok((
            self.inner.prepare(graphrecord, cache)?,
            self.policy.prepare(graphrecord, cache)?,
        ))
    }
}

impl<I: IndexDomain, S: MaybeAbsent<I> + Clone, P> ArgumentSource<Keyed<I>> for WithMissing<I, S, P>
where
    P: MissingPolicy<I, S>,
{
    type Value<'a> = S::Value<'a>;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        index: &I::Index<'a>,
    ) -> Looked<'prepared, QueryResult<Self::Value<'a>>>
    where
        Self: 'a,
    {
        S::lookup(&prepared.0, index)
    }

    fn resolve<'a>(
        prepared: &Self::Prepared<'a>,
        index: &I::Index<'a>,
        label: &'static str,
        _default: OnMissing,
    ) -> QueryResult<Option<Self::Value<'a>>>
    where
        Self: 'a,
    {
        match S::lookup(&prepared.0, index) {
            Looked::Present(wrapped) => wrapped.clone().map(Some),
            Looked::Absent(absent) => P::resolve_absent(&prepared.1, index, label, absent),
        }
    }
}
