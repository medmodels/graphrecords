use crate::{
    Bare, BareValueDomain, Explain, IndexDomain, Indexed, Multiple, OrderState, QueryResult,
    Single, ValueDomain,
    element::{Dropping, ElementEmission, Retention},
    execution::EvaluationCache,
    explain::ExplainFormatter,
    expressions::ExpressionHandle,
    operations::{
        Alignment, ArgumentSource, Keyed, Lookup, Prepare, SourceDomain,
        policy::{Drop, Replace},
    },
    optimizer::{Estimate, Estimated, PlanIdentity, PlanInputs, PlanNode, Stats},
};
use graphrecords_core::GraphRecord;
use std::{
    fmt::{self, Write},
    hash::Hasher,
    marker::PhantomData,
};

pub trait OnMissing<A: Alignment>: ArgumentSource<A> {
    fn on_missing<P>(&self, policy: P) -> WithMissing<A, Self, P>
    where
        Self: Sized + Clone,
        P: MissingPolicy<A, Self::ValueDomain>,
    {
        WithMissing::new(self.clone(), policy)
    }
}

pub trait MissingPolicy<A: Alignment, V: ValueDomain>:
    Prepare + Clone + Explain + PlanIdentity + PlanInputs
{
    type Retention: Retention;

    fn resolve_absent<'a>(
        graphrecord: &'a GraphRecord,
        prepared: &Self::Prepared<'a>,
        address: &A::Address,
        label: &'static str,
    ) -> <Self::Retention as ElementEmission>::Step<QueryResult<V::Value<'a>>>
    where
        V: 'a;
}

impl<A: Alignment, V: ValueDomain> MissingPolicy<A, V> for Drop {
    type Retention = Dropping;

    fn resolve_absent<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: &Self::Prepared<'a>,
        _address: &A::Address,
        _label: &'static str,
    ) -> <Self::Retention as ElementEmission>::Step<QueryResult<V::Value<'a>>>
    where
        V: 'a,
    {
        None
    }
}

impl<A, V, R> MissingPolicy<A, V> for Replace<R>
where
    A: Alignment,
    V: ValueDomain,
    R: ArgumentSource<A, V> + Clone,
{
    type Retention = R::Retention;

    fn resolve_absent<'a>(
        graphrecord: &'a GraphRecord,
        prepared: &Self::Prepared<'a>,
        address: &A::Address,
        label: &'static str,
    ) -> <Self::Retention as ElementEmission>::Step<QueryResult<V::Value<'a>>>
    where
        V: 'a,
    {
        R::resolve(graphrecord, prepared, address, label)
    }
}

impl<I: IndexDomain, V: ValueDomain, O: OrderState> OnMissing<Keyed<I>>
    for ExpressionHandle<Indexed<I, V>, Multiple<O>>
{
}
impl<A: Alignment, V: BareValueDomain> OnMissing<A> for ExpressionHandle<Bare<V>, Single> {}

pub struct WithMissing<A: Alignment, S: OnMissing<A>, P> {
    inner: S,
    policy: P,
    alignment: PhantomData<fn() -> A>,
}

impl<A: Alignment, S: OnMissing<A>, P> WithMissing<A, S, P> {
    #[must_use]
    pub const fn new(inner: S, policy: P) -> Self {
        Self {
            inner,
            policy,
            alignment: PhantomData,
        }
    }

    pub(crate) fn into_inner(self) -> S {
        self.inner
    }
}

impl<A: Alignment, S: OnMissing<A> + Clone, P: Clone> Clone for WithMissing<A, S, P> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            policy: self.policy.clone(),
            alignment: PhantomData,
        }
    }
}

impl<A: Alignment, S: OnMissing<A>, P: Explain> Explain for WithMissing<A, S, P> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        self.inner.describe(formatter)?;
        formatter.write_str(" on_missing(")?;
        self.policy.describe(formatter)?;
        formatter.write_str(")")
    }
}

impl<A: Alignment, S: OnMissing<A>, P: PlanIdentity> PlanIdentity for WithMissing<A, S, P> {
    fn identity_eq(&self, other: &Self) -> bool {
        self.inner.identity_eq(&other.inner) && self.policy.identity_eq(&other.policy)
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        self.inner.identity_hash(state);
        self.policy.identity_hash(state);
    }
}

impl<A: Alignment, S: OnMissing<A>, P: PlanInputs> PlanInputs for WithMissing<A, S, P> {
    fn inputs(&self) -> Vec<&dyn PlanNode> {
        let mut inputs = self.inner.inputs();
        inputs.extend(self.policy.inputs());

        inputs
    }
}

impl<A: Alignment, S: OnMissing<A>, P: Prepare> Prepare for WithMissing<A, S, P> {
    type Prepared<'a>
        = (S::Prepared<'a>, P::Prepared<'a>)
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok((
            self.inner.prepare(graphrecord, cache)?,
            self.policy.prepare(graphrecord, cache)?,
        ))
    }
}

impl<A: Alignment, S: OnMissing<A>, P> Estimated for WithMissing<A, S, P> {
    fn estimate(&self, stats: &Stats) -> Estimate {
        self.inner.estimate(stats)
    }
}

impl<A: Alignment, S: OnMissing<A>, P> SourceDomain for WithMissing<A, S, P> {
    type ValueDomain = S::ValueDomain;
}

impl<A: Alignment, S: OnMissing<A>, P: MissingPolicy<A, S::ValueDomain>> ArgumentSource<A>
    for WithMissing<A, S, P>
{
    type Retention = P::Retention;

    fn lookup<'a>(
        graphrecord: &'a GraphRecord,
        prepared: &Self::Prepared<'a>,
        address: &A::Address,
        label: &'static str,
    ) -> Lookup<QueryResult<<S::ValueDomain as ValueDomain>::Value<'a>>>
    where
        Self: 'a,
    {
        S::lookup(graphrecord, &prepared.0, address, label)
    }

    fn resolve<'a>(
        graphrecord: &'a GraphRecord,
        prepared: &Self::Prepared<'a>,
        address: &A::Address,
        label: &'static str,
    ) -> <Self::Retention as ElementEmission>::Step<
        QueryResult<<S::ValueDomain as ValueDomain>::Value<'a>>,
    >
    where
        Self: 'a,
    {
        match S::lookup(graphrecord, &prepared.0, address, label) {
            Lookup::Present(wrapped) => P::Retention::keep(wrapped),
            Lookup::Absent(_) => P::resolve_absent(graphrecord, &prepared.1, address, label),
        }
    }
}
