mod collection;
mod constant;
mod series;

use crate::{
    Arity, Bare, BareValueDomain, Definite, Diagnostic, ElementShape, EvaluateExpression, Explain,
    Failure, IndexDomain, Indexed, Multiple, OrderState, QueryResult, Single, ValueDomain,
    element::{ElementEmission, Preserving, Retention},
    error::{
        argument::{Absent, ArgumentMissing},
        index::DuplicateIndex,
    },
    execution::EvaluationCache,
    explain::ExplainFormatter,
    expressions::ExpressionHandle,
    optimizer::{
        Estimate, Estimated, PlanIdentity, PlanInputs, PlanNode, Session, Stats, Transformed,
    },
};
use graphrecords_core::GraphRecord;
use graphrecords_utils::aliases::{GrHashMap, GrHashSet};
pub use series::PreparedSeriesArgument;
use std::{
    any::Any,
    fmt,
    hash::{Hash, Hasher},
    iter::once,
    marker::PhantomData,
    sync::Arc,
};

const LABEL: &str = "ArgumentPreparation";

pub trait Prepare: 'static + Send + Sync {
    type Prepared<'a>: Clone + 'a
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>>;
}

pub trait Alignment: 'static {
    type Address;

    fn raise_at(
        cause: impl Diagnostic,
        graphrecord: &GraphRecord,
        address: &Self::Address,
        operation: &'static str,
    ) -> Box<Failure>;
}

pub struct Keyed<I: IndexDomain>(PhantomData<I>);

impl<I: IndexDomain> Alignment for Keyed<I> {
    type Address = I::Address;

    fn raise_at(
        cause: impl Diagnostic,
        graphrecord: &GraphRecord,
        address: &Self::Address,
        operation: &'static str,
    ) -> Box<Failure> {
        Failure::new_at_address::<I, _>(cause, graphrecord, address, operation)
    }
}

pub struct Unaligned;

impl Alignment for Unaligned {
    type Address = ();

    fn raise_at(
        cause: impl Diagnostic,
        _graphrecord: &GraphRecord,
        _address: &Self::Address,
        operation: &'static str,
    ) -> Box<Failure> {
        Failure::new(cause, operation)
    }
}

pub trait SourceDomain {
    type ValueDomain: ValueDomain;
}

pub enum Lookup<W> {
    Present(W),
    Absent(Absent),
}

pub trait ArgumentSource<A: Alignment, V: ValueDomain = <Self as SourceDomain>::ValueDomain>:
    SourceDomain + Prepare + Explain + PlanIdentity + PlanInputs + Estimated
{
    type Retention: Retention;

    fn lookup<'a>(
        graphrecord: &'a GraphRecord,
        prepared: &Self::Prepared<'a>,
        address: &A::Address,
        label: &'static str,
    ) -> Lookup<QueryResult<V::Value<'a>>>
    where
        Self: 'a;

    fn resolve<'a>(
        graphrecord: &'a GraphRecord,
        prepared: &Self::Prepared<'a>,
        address: &A::Address,
        label: &'static str,
    ) -> <Self::Retention as ElementEmission>::Step<QueryResult<V::Value<'a>>>
    where
        Self: 'a,
    {
        match Self::lookup(graphrecord, prepared, address, label) {
            Lookup::Present(wrapped) => Self::Retention::keep(wrapped),
            Lookup::Absent(absent) => Self::Retention::absent(|| {
                A::raise_at(ArgumentMissing::new(absent), graphrecord, address, label)
            }),
        }
    }
}

pub struct PreparedArgument<'a, A, V, R>
where
    A: Alignment,
    V: ValueDomain,
    R: Retention,
{
    plan: Arc<dyn PreparedArgumentPlan<'a, A, V, R> + 'a>,
}

impl<A, V, R> Clone for PreparedArgument<'_, A, V, R>
where
    A: Alignment,
    V: ValueDomain,
    R: Retention,
{
    fn clone(&self) -> Self {
        Self {
            plan: Arc::clone(&self.plan),
        }
    }
}

pub trait ArgumentPlan<A, V, R>: Any + Send + Sync
where
    A: Alignment,
    V: ValueDomain,
    R: Retention,
{
    fn as_any(&self) -> &dyn Any;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache,
    ) -> QueryResult<PreparedArgument<'a, A, V, R>>;

    fn inputs(&self) -> Vec<&dyn PlanNode>;

    fn identity_eq(&self, other: &dyn ArgumentPlan<A, V, R>) -> bool;

    fn identity_hash(&self, state: &mut dyn Hasher);

    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result;

    fn estimate(&self, stats: &Stats) -> Estimate;

    fn optimize(&self, session: &Session) -> Transformed<Argument<A, V, R>>;
}

pub struct Argument<A, V, R>
where
    A: Alignment,
    V: ValueDomain,
    R: Retention,
{
    plan: Arc<dyn ArgumentPlan<A, V, R>>,
}

impl<A, V, R> Argument<A, V, R>
where
    A: Alignment,
    V: ValueDomain,
    R: Retention,
{
    fn new<S>(source: S) -> Self
    where
        S: ArgumentSource<A, V, Retention = R>,
    {
        Self {
            plan: Arc::new(SourceArgumentPlan { source }),
        }
    }
}

impl<A, V, R> Clone for Argument<A, V, R>
where
    A: Alignment,
    V: ValueDomain,
    R: Retention,
{
    fn clone(&self) -> Self {
        Self {
            plan: Arc::clone(&self.plan),
        }
    }
}

pub trait IntoArgument<A, V>: ArgumentSource<A, V> + Sized
where
    A: Alignment,
    V: ValueDomain,
{
    fn into_argument(self) -> Argument<A, V, Self::Retention>;
}

impl<A, V, S> IntoArgument<A, V> for S
where
    A: Alignment,
    V: ValueDomain,
    S: ArgumentSource<A, V>,
{
    fn into_argument(self) -> Argument<A, V, Self::Retention> {
        Argument::new(self)
    }
}

trait PreparedArgumentPlan<'a, A, V, R>
where
    A: Alignment,
    V: ValueDomain,
    R: Retention,
{
    fn lookup(
        &self,
        graphrecord: &'a GraphRecord,
        address: &A::Address,
        label: &'static str,
    ) -> Lookup<QueryResult<V::Value<'a>>>;

    fn resolve(
        &self,
        graphrecord: &'a GraphRecord,
        address: &A::Address,
        label: &'static str,
    ) -> R::Step<QueryResult<V::Value<'a>>>;
}

struct PreparedSourceArgument<'a, A, V, R, S>
where
    A: Alignment,
    V: ValueDomain,
    R: Retention,
    S: ArgumentSource<A, V, Retention = R> + 'a,
{
    prepared: S::Prepared<'a>,
    #[allow(clippy::type_complexity)]
    source: PhantomData<fn() -> (A, V, R, S)>,
}

impl<'a, A, V, R, S> PreparedArgumentPlan<'a, A, V, R> for PreparedSourceArgument<'a, A, V, R, S>
where
    A: Alignment,
    V: ValueDomain,
    R: Retention,
    S: ArgumentSource<A, V, Retention = R> + 'a,
{
    fn lookup(
        &self,
        graphrecord: &'a GraphRecord,
        address: &A::Address,
        label: &'static str,
    ) -> Lookup<QueryResult<V::Value<'a>>> {
        S::lookup(graphrecord, &self.prepared, address, label)
    }

    fn resolve(
        &self,
        graphrecord: &'a GraphRecord,
        address: &A::Address,
        label: &'static str,
    ) -> R::Step<QueryResult<V::Value<'a>>> {
        S::resolve(graphrecord, &self.prepared, address, label)
    }
}

struct SourceArgumentPlan<S> {
    source: S,
}

impl<A, V, R, S> ArgumentPlan<A, V, R> for SourceArgumentPlan<S>
where
    A: Alignment,
    V: ValueDomain,
    R: Retention,
    S: ArgumentSource<A, V, Retention = R>,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache,
    ) -> QueryResult<PreparedArgument<'a, A, V, R>> {
        Ok(PreparedArgument {
            plan: Arc::new(PreparedSourceArgument::<_, _, _, S> {
                prepared: self.source.prepare(graphrecord, cache)?,
                source: PhantomData,
            }),
        })
    }

    fn inputs(&self) -> Vec<&dyn PlanNode> {
        self.source.inputs()
    }

    fn identity_eq(&self, other: &dyn ArgumentPlan<A, V, R>) -> bool {
        other
            .as_any()
            .downcast_ref::<Self>()
            .is_some_and(|other| self.source.identity_eq(&other.source))
    }

    fn identity_hash(&self, mut state: &mut dyn Hasher) {
        self.source.identity_hash(&mut state);
    }

    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        self.source.describe(formatter)
    }

    fn estimate(&self, stats: &Stats) -> Estimate {
        self.source.estimate(stats)
    }

    fn optimize(&self, session: &Session) -> Transformed<Argument<A, V, R>> {
        let source = PlanInputs::optimize(&self.source, session);
        let (source, changed) = source.into_parts();
        let argument = Argument::new(source);

        if changed {
            Transformed::changed(argument)
        } else {
            Transformed::unchanged(argument)
        }
    }
}

impl<A, V, R> Explain for Argument<A, V, R>
where
    A: Alignment,
    V: ValueDomain,
    R: Retention,
{
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        self.plan.describe(formatter)
    }
}

impl<A, V, R> PlanIdentity for Argument<A, V, R>
where
    A: Alignment,
    V: ValueDomain,
    R: Retention,
{
    fn identity_eq(&self, other: &Self) -> bool {
        self.plan.identity_eq(other.plan.as_ref())
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        self.plan.identity_hash(state);
    }
}

impl<A, V, R> PlanInputs for Argument<A, V, R>
where
    A: Alignment,
    V: ValueDomain,
    R: Retention,
{
    fn inputs(&self) -> Vec<&dyn PlanNode> {
        self.plan.inputs()
    }

    fn optimize(&self, session: &Session) -> Transformed<Self> {
        self.plan.optimize(session)
    }
}

impl<A, V, R> Estimated for Argument<A, V, R>
where
    A: Alignment,
    V: ValueDomain,
    R: Retention,
{
    fn estimate(&self, stats: &Stats) -> Estimate {
        self.plan.estimate(stats)
    }
}

impl<A, V, R> Prepare for Argument<A, V, R>
where
    A: Alignment,
    V: ValueDomain,
    R: Retention,
{
    type Prepared<'a>
        = PreparedArgument<'a, A, V, R>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.plan.prepare(graphrecord, cache)
    }
}

impl<A, V, R> SourceDomain for Argument<A, V, R>
where
    A: Alignment,
    V: ValueDomain,
    R: Retention,
{
    type ValueDomain = V;
}

impl<A, V, R> ArgumentSource<A, V> for Argument<A, V, R>
where
    A: Alignment,
    V: ValueDomain,
    R: Retention,
{
    type Retention = R;

    fn lookup<'a>(
        graphrecord: &'a GraphRecord,
        prepared: &Self::Prepared<'a>,
        address: &A::Address,
        label: &'static str,
    ) -> Lookup<QueryResult<V::Value<'a>>>
    where
        Self: 'a,
    {
        prepared.plan.lookup(graphrecord, address, label)
    }

    fn resolve<'a>(
        graphrecord: &'a GraphRecord,
        prepared: &Self::Prepared<'a>,
        address: &A::Address,
        label: &'static str,
    ) -> R::Step<QueryResult<V::Value<'a>>>
    where
        Self: 'a,
    {
        prepared.plan.resolve(graphrecord, address, label)
    }
}

pub type IndexedElementContainer<'a, I, V, C> =
    <C as Arity>::Container<'a, (<I as IndexDomain>::Address, QueryResult<V>)>;

pub trait IndexedElementSource:
    SourceDomain + Prepare + Explain + PlanIdentity + PlanInputs + Estimated
{
    type IndexDomain: IndexDomain;
    type Arity: Arity;

    fn elements<'a>(
        prepared: Self::Prepared<'a>,
    ) -> IndexedElementContainer<
        'a,
        Self::IndexDomain,
        <Self::ValueDomain as ValueDomain>::Value<'a>,
        Self::Arity,
    >
    where
        Self: 'a;
}

pub trait SetSource<V: ValueDomain>:
    Prepare + Explain + PlanIdentity + PlanInputs + Estimated
{
    fn set<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
        label: &'static str,
    ) -> QueryResult<GrHashSet<V::Value<'a>>>
    where
        Self: 'a,
        V::Value<'a>: Eq + Hash;
}

pub trait PreparedArity<S: ElementShape>: Arity {
    type Prepared<'a>: Clone + 'a
    where
        S: 'a;

    fn prepare<'a>(
        graphrecord: &'a GraphRecord,
        container: Self::Container<'a, S::Element<'a>>,
    ) -> QueryResult<Self::Prepared<'a>>
    where
        S: 'a;
}

pub trait AlignableArity<S: ElementShape, A: Alignment>: PreparedArity<S> {
    type Retention: Retention;

    fn lookup<'a>(
        graphrecord: &'a GraphRecord,
        prepared: &Self::Prepared<'a>,
        address: &A::Address,
        label: &'static str,
    ) -> Lookup<QueryResult<<S::ValueDomain as ValueDomain>::Value<'a>>>
    where
        S: 'a;
}

pub trait EnumerableArity<S: ElementShape, I: IndexDomain>: PreparedArity<S> {
    fn elements<'a>(
        prepared: Self::Prepared<'a>,
    ) -> IndexedElementContainer<'a, I, <S::ValueDomain as ValueDomain>::Value<'a>, Self>
    where
        S: 'a;
}

pub trait SetArity<S: ElementShape>: PreparedArity<S> {
    fn set<'a>(
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<GrHashSet<<S::ValueDomain as ValueDomain>::Value<'a>>>
    where
        S: 'a,
        <S::ValueDomain as ValueDomain>::Value<'a>: Eq + Hash;
}

pub struct PreparedIndexedMultiple<'a, I: IndexDomain, V: ValueDomain> {
    elements: Vec<(I::Address, QueryResult<V::Value<'a>>)>,
    positions: GrHashMap<I::Address, usize>,
}

impl<S: ElementShape, C: PreparedArity<S>> Prepare for ExpressionHandle<S, C> {
    type Prepared<'a>
        = C::Prepared<'a>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        C::prepare(graphrecord, self.evaluate(graphrecord, cache)?)
    }
}

impl<S: ElementShape, C: Arity> SourceDomain for ExpressionHandle<S, C> {
    type ValueDomain = S::ValueDomain;
}

impl<S: ElementShape, C: AlignableArity<S, A>, A: Alignment> ArgumentSource<A>
    for ExpressionHandle<S, C>
{
    type Retention = C::Retention;

    fn lookup<'a>(
        graphrecord: &'a GraphRecord,
        prepared: &Self::Prepared<'a>,
        address: &A::Address,
        label: &'static str,
    ) -> Lookup<QueryResult<<S::ValueDomain as ValueDomain>::Value<'a>>>
    where
        Self: 'a,
    {
        C::lookup(graphrecord, prepared, address, label)
    }
}

impl<I: IndexDomain, V: ValueDomain, C: EnumerableArity<Indexed<I, V>, I>> IndexedElementSource
    for ExpressionHandle<Indexed<I, V>, C>
{
    type Arity = C;
    type IndexDomain = I;

    fn elements<'a>(
        prepared: Self::Prepared<'a>,
    ) -> C::Container<'a, (I::Address, QueryResult<<V as ValueDomain>::Value<'a>>)>
    where
        Self: 'a,
    {
        C::elements(prepared)
    }
}

impl<S: ElementShape, C: SetArity<S>> SetSource<S::ValueDomain> for ExpressionHandle<S, C> {
    fn set<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
        _label: &'static str,
    ) -> QueryResult<GrHashSet<<S::ValueDomain as ValueDomain>::Value<'a>>>
    where
        Self: 'a,
        <S::ValueDomain as ValueDomain>::Value<'a>: Eq + Hash,
    {
        C::set(prepared)
    }
}

impl<I: IndexDomain, V: ValueDomain, O: OrderState> PreparedArity<Indexed<I, V>> for Multiple<O> {
    type Prepared<'a>
        = Arc<PreparedIndexedMultiple<'a, I, V>>
    where
        Indexed<I, V>: 'a;

    fn prepare<'a>(
        graphrecord: &'a GraphRecord,
        container: Self::Container<'a, <Indexed<I, V> as ElementShape>::Element<'a>>,
    ) -> QueryResult<Self::Prepared<'a>>
    where
        Indexed<I, V>: 'a,
    {
        let mut elements = Vec::new();
        let mut positions = GrHashMap::default();

        for (address, outcome) in container {
            if positions.contains_key(&address) {
                let index = I::index(graphrecord, &address);

                return Err(Failure::new_at::<I, _>(
                    DuplicateIndex::<I>::new(I::own_index(&index)),
                    &index,
                    LABEL,
                ));
            }

            positions.insert(address.clone(), elements.len());
            elements.push((address, outcome));
        }

        Ok(Arc::new(PreparedIndexedMultiple {
            elements,
            positions,
        }))
    }
}

impl<I: IndexDomain, V: ValueDomain, O: OrderState> AlignableArity<Indexed<I, V>, Keyed<I>>
    for Multiple<O>
{
    type Retention = Preserving;

    fn lookup<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: &Self::Prepared<'a>,
        address: &<Keyed<I> as Alignment>::Address,
        _label: &'static str,
    ) -> Lookup<QueryResult<V::Value<'a>>>
    where
        Indexed<I, V>: 'a,
    {
        match prepared.positions.get(address) {
            Some(position) => Lookup::Present(prepared.elements[*position].1.clone()),
            None => Lookup::Absent(Absent::Uncovered),
        }
    }
}

impl<I: IndexDomain, V: ValueDomain, O: OrderState> EnumerableArity<Indexed<I, V>, I>
    for Multiple<O>
{
    fn elements<'a>(
        prepared: Self::Prepared<'a>,
    ) -> Self::Container<'a, (I::Address, QueryResult<V::Value<'a>>)>
    where
        Indexed<I, V>: 'a,
    {
        let element_count = prepared.elements.len();

        Box::new((0..element_count).map(move |position| prepared.elements[position].clone()))
    }
}

impl<I, V, O> SetArity<Indexed<I, V>> for Multiple<O>
where
    I: IndexDomain,
    V: ValueDomain,
    O: OrderState,
    for<'a> V::Value<'a>: Eq + Hash,
{
    fn set<'a>(prepared: Self::Prepared<'a>) -> QueryResult<GrHashSet<V::Value<'a>>>
    where
        Indexed<I, V>: 'a,
    {
        prepared
            .elements
            .iter()
            .map(|element| element.1.clone())
            .collect()
    }
}

impl<I: IndexDomain, V: ValueDomain> PreparedArity<Indexed<I, V>> for Single {
    type Prepared<'a>
        = Option<(I::Address, QueryResult<V::Value<'a>>)>
    where
        Indexed<I, V>: 'a;

    fn prepare<'a>(
        _graphrecord: &'a GraphRecord,
        container: Self::Container<'a, <Indexed<I, V> as ElementShape>::Element<'a>>,
    ) -> QueryResult<Self::Prepared<'a>>
    where
        Indexed<I, V>: 'a,
    {
        Ok(container)
    }
}

impl<I: IndexDomain, V: ValueDomain> EnumerableArity<Indexed<I, V>, I> for Single {
    fn elements<'a>(
        prepared: Self::Prepared<'a>,
    ) -> Self::Container<'a, (I::Address, QueryResult<V::Value<'a>>)>
    where
        Indexed<I, V>: 'a,
    {
        prepared
    }
}

impl<I, V> SetArity<Indexed<I, V>> for Single
where
    I: IndexDomain,
    V: ValueDomain,
    for<'a> V::Value<'a>: Eq + Hash,
{
    fn set<'a>(prepared: Self::Prepared<'a>) -> QueryResult<GrHashSet<V::Value<'a>>>
    where
        Indexed<I, V>: 'a,
    {
        match prepared {
            Some(element) => Ok(once(element.1?).collect()),
            None => Ok(GrHashSet::default()),
        }
    }
}

impl<I: IndexDomain, V: ValueDomain> PreparedArity<Indexed<I, V>> for Definite {
    type Prepared<'a>
        = (I::Address, QueryResult<V::Value<'a>>)
    where
        Indexed<I, V>: 'a;

    fn prepare<'a>(
        _graphrecord: &'a GraphRecord,
        container: Self::Container<'a, <Indexed<I, V> as ElementShape>::Element<'a>>,
    ) -> QueryResult<Self::Prepared<'a>>
    where
        Indexed<I, V>: 'a,
    {
        Ok(container)
    }
}

impl<I: IndexDomain, V: ValueDomain> EnumerableArity<Indexed<I, V>, I> for Definite {
    fn elements<'a>(
        prepared: Self::Prepared<'a>,
    ) -> Self::Container<'a, (I::Address, QueryResult<V::Value<'a>>)>
    where
        Indexed<I, V>: 'a,
    {
        prepared
    }
}

impl<I, V> SetArity<Indexed<I, V>> for Definite
where
    I: IndexDomain,
    V: ValueDomain,
    for<'a> V::Value<'a>: Eq + Hash,
{
    fn set<'a>(prepared: Self::Prepared<'a>) -> QueryResult<GrHashSet<V::Value<'a>>>
    where
        Indexed<I, V>: 'a,
    {
        Ok(once(prepared.1?).collect())
    }
}

impl<V: BareValueDomain, O: OrderState> PreparedArity<Bare<V>> for Multiple<O> {
    type Prepared<'a>
        = Arc<Vec<QueryResult<V::Value<'a>>>>
    where
        Bare<V>: 'a;

    fn prepare<'a>(
        _graphrecord: &'a GraphRecord,
        container: Self::Container<'a, <Bare<V> as ElementShape>::Element<'a>>,
    ) -> QueryResult<Self::Prepared<'a>>
    where
        Bare<V>: 'a,
    {
        Ok(Arc::new(container.collect()))
    }
}

impl<V, O> SetArity<Bare<V>> for Multiple<O>
where
    V: BareValueDomain,
    O: OrderState,
    for<'a> V::Value<'a>: Eq + Hash,
{
    fn set<'a>(prepared: Self::Prepared<'a>) -> QueryResult<GrHashSet<V::Value<'a>>>
    where
        Bare<V>: 'a,
    {
        prepared.iter().cloned().collect()
    }
}

impl<V: BareValueDomain> PreparedArity<Bare<V>> for Single {
    type Prepared<'a>
        = Option<QueryResult<V::Value<'a>>>
    where
        Bare<V>: 'a;

    fn prepare<'a>(
        _graphrecord: &'a GraphRecord,
        container: Self::Container<'a, <Bare<V> as ElementShape>::Element<'a>>,
    ) -> QueryResult<Self::Prepared<'a>>
    where
        Bare<V>: 'a,
    {
        Ok(container)
    }
}

impl<A: Alignment, V: BareValueDomain> AlignableArity<Bare<V>, A> for Single {
    type Retention = Preserving;

    fn lookup<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: &Self::Prepared<'a>,
        _address: &A::Address,
        _label: &'static str,
    ) -> Lookup<QueryResult<V::Value<'a>>>
    where
        Bare<V>: 'a,
    {
        match prepared {
            Some(value) => Lookup::Present(value.clone()),
            None => Lookup::Absent(Absent::Empty),
        }
    }
}

impl<V> SetArity<Bare<V>> for Single
where
    V: BareValueDomain,
    for<'a> V::Value<'a>: Eq + Hash,
{
    fn set<'a>(prepared: Self::Prepared<'a>) -> QueryResult<GrHashSet<V::Value<'a>>>
    where
        Bare<V>: 'a,
    {
        match prepared {
            Some(outcome) => Ok(once(outcome?).collect()),
            None => Ok(GrHashSet::default()),
        }
    }
}

impl<V: BareValueDomain> PreparedArity<Bare<V>> for Definite {
    type Prepared<'a>
        = QueryResult<V::Value<'a>>
    where
        Bare<V>: 'a;

    fn prepare<'a>(
        _graphrecord: &'a GraphRecord,
        container: Self::Container<'a, <Bare<V> as ElementShape>::Element<'a>>,
    ) -> QueryResult<Self::Prepared<'a>>
    where
        Bare<V>: 'a,
    {
        Ok(container)
    }
}

impl<A: Alignment, V: BareValueDomain> AlignableArity<Bare<V>, A> for Definite {
    type Retention = Preserving;

    fn lookup<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: &Self::Prepared<'a>,
        _address: &A::Address,
        _label: &'static str,
    ) -> Lookup<QueryResult<V::Value<'a>>>
    where
        Bare<V>: 'a,
    {
        Lookup::Present(prepared.clone())
    }
}

impl<V> SetArity<Bare<V>> for Definite
where
    V: BareValueDomain,
    for<'a> V::Value<'a>: Eq + Hash,
{
    fn set<'a>(prepared: Self::Prepared<'a>) -> QueryResult<GrHashSet<V::Value<'a>>>
    where
        Bare<V>: 'a,
    {
        Ok(once(prepared?).collect())
    }
}
