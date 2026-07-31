use crate::{
    Arity, Bare, BareValueType, Explain, IndexDomain, Indexed, Multiple, OrderState, QueryResult,
    Single, ValueType,
    element::{Dropping, ElementEmission, Retention},
    execution::EvaluationCache,
    explain::ExplainFormatter,
    operands::OperandHandle,
    operations::{
        Alignment, ArgumentSource, Drop, IndexedElementSource, Keyed, Lookup, Prepare, Replace,
    },
    optimizer::{Estimate, Estimated, PlanIdentity, PlanInputs, PlanNode, Stats},
};
use graphrecords_core::GraphRecord;
use std::{
    fmt::{self, Write},
    hash::Hasher,
    marker::PhantomData,
};

pub trait MaybeAbsent<A: Alignment>: ArgumentSource<A> {
    fn on_missing<P>(self, policy: P) -> WithMissing<A, Self, P>
    where
        Self: Sized,
        P: MissingPolicy<A, Self>,
    {
        WithMissing::new(self, policy)
    }
}

pub trait MissingPolicy<A: Alignment, S: ArgumentSource<A>>:
    Clone + 'static + Explain + PlanIdentity + PlanInputs
{
    type Retention: Retention;

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
        address: &A::Address<'a>,
        label: &'static str,
    ) -> <Self::Retention as ElementEmission>::Step<QueryResult<S::Value<'a>>>
    where
        S: 'a;
}

impl<A: Alignment, S: ArgumentSource<A>> MissingPolicy<A, S> for Drop {
    type Prepared<'a> = ();
    type Retention = Dropping;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }

    fn resolve_absent<'a>(
        _prepared: &Self::Prepared<'a>,
        _address: &A::Address<'a>,
        _label: &'static str,
    ) -> <Self::Retention as ElementEmission>::Step<QueryResult<S::Value<'a>>>
    where
        S: 'a,
    {
        None
    }
}

impl<A, S, R> MissingPolicy<A, S> for Replace<R>
where
    A: Alignment,
    R: ArgumentSource<A> + Clone,
    for<'a> S: ArgumentSource<A, Value<'a> = R::Value<'a>>,
{
    type Prepared<'a> = R::Prepared<'a>;
    type Retention = R::Retention;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.0.prepare(graphrecord, cache)
    }

    fn resolve_absent<'a>(
        prepared: &Self::Prepared<'a>,
        address: &A::Address<'a>,
        label: &'static str,
    ) -> <Self::Retention as ElementEmission>::Step<QueryResult<S::Value<'a>>>
    where
        S: 'a,
    {
        R::resolve(prepared, address, label)
    }
}

impl<I: IndexDomain, V: ValueType, O: OrderState> MaybeAbsent<Keyed<I>>
    for OperandHandle<Indexed<I, V>, Multiple<O>>
{
}
impl<A: Alignment, I: IndexDomain, V: ValueType> MaybeAbsent<A>
    for OperandHandle<Indexed<I, V>, Single>
{
}
impl<A: Alignment, V: BareValueType> MaybeAbsent<A> for OperandHandle<Bare<V>, Single> {}

pub struct WithMissing<A: Alignment, S: MaybeAbsent<A>, P> {
    inner: S,
    policy: P,
    alignment: PhantomData<fn() -> A>,
}

impl<A: Alignment, S: MaybeAbsent<A>, P> WithMissing<A, S, P> {
    #[must_use]
    pub fn new(inner: S, policy: P) -> Self {
        Self {
            inner,
            policy,
            alignment: PhantomData,
        }
    }
}

impl<A: Alignment, S: MaybeAbsent<A> + Clone, P: Clone> Clone for WithMissing<A, S, P> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            policy: self.policy.clone(),
            alignment: PhantomData,
        }
    }
}

impl<A: Alignment, S: MaybeAbsent<A>, P: Explain> Explain for WithMissing<A, S, P> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        self.inner.describe(formatter)?;
        formatter.write_str(" on_missing(")?;
        self.policy.describe(formatter)?;
        formatter.write_str(")")
    }
}

impl<A: Alignment, S: MaybeAbsent<A>, P: PlanIdentity> PlanIdentity for WithMissing<A, S, P> {
    fn identity_eq(&self, other: &Self) -> bool {
        self.inner.identity_eq(&other.inner) && self.policy.identity_eq(&other.policy)
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        self.inner.identity_hash(state);
        self.policy.identity_hash(state);
    }
}

impl<A: Alignment, S: MaybeAbsent<A>, P: PlanInputs> PlanInputs for WithMissing<A, S, P> {
    fn inputs(&self) -> Vec<&dyn PlanNode> {
        let mut inputs = self.inner.inputs();
        inputs.extend(self.policy.inputs());

        inputs
    }
}

impl<A: Alignment, S: MaybeAbsent<A>, P: MissingPolicy<A, S>> Prepare for WithMissing<A, S, P> {
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

impl<A: Alignment, S: MaybeAbsent<A>, P> Estimated for WithMissing<A, S, P> {
    fn estimate(&self, stats: &Stats) -> Estimate {
        self.inner.estimate(stats)
    }
}

impl<A: Alignment, S: MaybeAbsent<A>, P: MissingPolicy<A, S>> ArgumentSource<A>
    for WithMissing<A, S, P>
{
    type OwnedValue = S::OwnedValue;
    type Retention = P::Retention;
    type Value<'a> = S::Value<'a>;

    fn to_owned_value(value: &Self::Value<'_>) -> Self::OwnedValue {
        S::to_owned_value(value)
    }

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        address: &A::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<Self::Value<'a>>>
    where
        Self: 'a,
    {
        S::lookup(&prepared.0, address)
    }

    fn resolve<'a>(
        prepared: &Self::Prepared<'a>,
        address: &A::Address<'a>,
        label: &'static str,
    ) -> <Self::Retention as ElementEmission>::Step<QueryResult<Self::Value<'a>>>
    where
        Self: 'a,
    {
        match S::lookup(&prepared.0, address) {
            Lookup::Present(wrapped) => P::Retention::keep(wrapped.clone()),
            Lookup::Absent(_) => P::resolve_absent(&prepared.1, address, label),
        }
    }
}

impl<A, S, P, I> IndexedElementSource<I> for WithMissing<A, S, P>
where
    A: Alignment,
    S: MaybeAbsent<A> + IndexedElementSource<I>,
    P: MissingPolicy<A, S>,
    I: IndexDomain,
{
    type Arity = S::Arity;
    type Value<'a>
        = <S as IndexedElementSource<I>>::Value<'a>
    where
        Self: 'a;

    fn elements<'a>(
        prepared: Self::Prepared<'a>,
    ) -> <Self::Arity as Arity>::Container<'a, (I::Index<'a>, QueryResult<Self::Value<'a>>)>
    where
        Self: 'a,
    {
        S::elements(prepared.0)
    }
}
