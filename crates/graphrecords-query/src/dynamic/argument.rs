use super::{
    DynArityHandle, DynHandle, DynIndex, DynLaneHandle, DynOperand, DynValue, DynValueTarget,
};
use crate::{
    Arity, EdgeDirection, Explain, Mask, QueryResult, Unit, ValueDomain,
    cast::{
        Bool as BoolTarget, DateTime as DateTimeTarget, Duration as DurationTarget,
        Float as FloatTarget, Int as IntTarget, String as StringTarget,
    },
    element::{Dropping, ElementEmission, Preserving, Retention},
    execution::EvaluationCache,
    explain::ExplainFormatter,
    operations::{
        Alignment, Argument, ArgumentSource, IndexedElementContainer, IndexedElementSource,
        IntoArgument, Keyed, Lookup, Prepare, SourceDomain, Unaligned, WithMissing,
        policy::{Drop, Replace},
    },
    optimizer::{
        Estimate, Estimated, PlanIdentity, PlanInputs, PlanNode, Session, Stats, Transformed,
    },
    registry::{
        ArgumentDescriptor, ArgumentMissingPolicy, ArgumentValueSource, ValueArgumentDescriptor,
        ValueDescriptor,
    },
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{GraphRecordAttribute, Group},
};
use std::{
    fmt::{self, Display, Write},
    hash::{Hash, Hasher},
};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum DynCastTarget {
    Bool,
    DateTime,
    Duration,
    Float,
    Int,
    String,
}

#[derive(Clone)]
enum DynArgumentReplacement {
    Value(DynValue),
    Mask(bool),
    Operand(DynOperand),
}

#[derive(Clone)]
enum DynArgumentSourceKind {
    Value(DynValue),
    Values(Vec<DynValue>),
    MaskValues(Vec<bool>),
    Mask(bool),
    Operand(DynOperand),
    DropMissing(DynOperand),
    ReplaceMissing {
        source: DynOperand,
        replacement: DynArgumentReplacement,
    },
}

#[derive(Clone)]
pub struct DynArgumentSource {
    kind: Box<DynArgumentSourceKind>,
}

#[derive(Clone)]
pub struct Keyable<S> {
    inner: S,
}

pub trait DynSetLiteral: ValueDomain {
    type Element: 'static + Clone + Eq + Hash + Display + Send + Sync;

    fn literal(source: &DynArgumentSource) -> Vec<Self::Element>;
}

pub trait DynArgumentBuilder<A: Alignment, R: Retention>: ValueDomain {
    type Dynamic: ValueDomain;

    fn build(source: &DynArgumentSource) -> Argument<A, Self::Dynamic, R>;
}

#[derive(Clone)]
pub enum DynInvokeArgument {
    Source(DynArgumentSource),
    Operand(DynOperand),
    CastTarget(DynCastTarget),
    ValueTarget(DynValueTarget),
    Attribute(GraphRecordAttribute),
    Group(Group),
    Direction(EdgeDirection),
    Position(usize),
}

impl DynArgumentReplacement {
    fn source(&self) -> ArgumentValueSource {
        match self {
            Self::Value(value) => ArgumentValueSource::Literal(value.descriptor()),
            Self::Mask(_) => ArgumentValueSource::Literal(ValueDescriptor::value::<Mask>()),
            Self::Operand(operand) => ArgumentValueSource::Operand(operand.descriptor().clone()),
        }
    }

    fn keyed_value(&self) -> Argument<Keyed<DynIndex>, DynValue, Preserving> {
        match self {
            Self::Value(value) => value.clone().into_argument(),
            Self::Operand(operand) => match &operand.handle {
                DynHandle::Lane(DynLaneHandle::IndexedValue(DynArityHandle::MultipleOrdered(
                    handle,
                ))) => handle.clone().into_argument(),
                DynHandle::Lane(DynLaneHandle::IndexedValue(
                    DynArityHandle::MultipleUnordered(handle),
                )) => handle.clone().into_argument(),
                DynHandle::Lane(DynLaneHandle::BareValue(DynArityHandle::Single(handle))) => {
                    handle.clone().into_argument()
                }
                DynHandle::Lane(DynLaneHandle::BareValue(DynArityHandle::Definite(handle))) => {
                    handle.clone().into_argument()
                }
                _ => panic!("argument conversion violated the keyed dynamic-value source roster"),
            },
            Self::Mask(_) => {
                panic!("argument conversion paired a mask replacement with a dynamic-value source")
            }
        }
    }

    fn unaligned_value(&self) -> Argument<Unaligned, DynValue, Preserving> {
        match self {
            Self::Value(value) => value.clone().into_argument(),
            Self::Operand(operand) => match &operand.handle {
                DynHandle::Lane(DynLaneHandle::BareValue(DynArityHandle::Single(handle))) => {
                    handle.clone().into_argument()
                }
                DynHandle::Lane(DynLaneHandle::BareValue(DynArityHandle::Definite(handle))) => {
                    handle.clone().into_argument()
                }
                _ => {
                    panic!("argument conversion violated the unaligned dynamic-value source roster")
                }
            },
            Self::Mask(_) => {
                panic!("argument conversion paired a mask replacement with a dynamic-value source")
            }
        }
    }

    fn keyed_mask(&self) -> Argument<Keyed<DynIndex>, Mask, Preserving> {
        match self {
            Self::Mask(value) => (*value).into_argument(),
            Self::Operand(operand) => match &operand.handle {
                DynHandle::Lane(DynLaneHandle::IndexedMask(DynArityHandle::MultipleOrdered(
                    handle,
                ))) => handle.clone().into_argument(),
                DynHandle::Lane(DynLaneHandle::IndexedMask(DynArityHandle::MultipleUnordered(
                    handle,
                ))) => handle.clone().into_argument(),
                DynHandle::Lane(DynLaneHandle::BareMask(DynArityHandle::Single(handle))) => {
                    handle.clone().into_argument()
                }
                DynHandle::Lane(DynLaneHandle::BareMask(DynArityHandle::Definite(handle))) => {
                    handle.clone().into_argument()
                }
                _ => panic!("argument conversion violated the keyed mask source roster"),
            },
            Self::Value(_) => {
                panic!("argument conversion paired a dynamic-value replacement with a mask source")
            }
        }
    }

    fn unaligned_mask(&self) -> Argument<Unaligned, Mask, Preserving> {
        match self {
            Self::Mask(value) => (*value).into_argument(),
            Self::Operand(operand) => match &operand.handle {
                DynHandle::Lane(DynLaneHandle::BareMask(DynArityHandle::Single(handle))) => {
                    handle.clone().into_argument()
                }
                DynHandle::Lane(DynLaneHandle::BareMask(DynArityHandle::Definite(handle))) => {
                    handle.clone().into_argument()
                }
                _ => panic!("argument conversion violated the unaligned mask source roster"),
            },
            Self::Value(_) => {
                panic!("argument conversion paired a dynamic-value replacement with a mask source")
            }
        }
    }
}

impl DynArgumentSource {
    #[must_use]
    pub fn value(value: DynValue) -> Self {
        Self {
            kind: Box::new(DynArgumentSourceKind::Value(value)),
        }
    }

    #[must_use]
    pub fn mask(value: bool) -> Self {
        Self {
            kind: Box::new(DynArgumentSourceKind::Mask(value)),
        }
    }

    #[must_use]
    pub fn values(values: Vec<DynValue>) -> Self {
        Self {
            kind: Box::new(DynArgumentSourceKind::Values(values)),
        }
    }

    #[must_use]
    pub fn mask_values(values: Vec<bool>) -> Self {
        Self {
            kind: Box::new(DynArgumentSourceKind::MaskValues(values)),
        }
    }

    #[must_use]
    pub fn operand(operand: DynOperand) -> Self {
        Self {
            kind: Box::new(DynArgumentSourceKind::Operand(operand)),
        }
    }

    #[must_use]
    pub fn drop_missing(source: DynOperand) -> Self {
        Self {
            kind: Box::new(DynArgumentSourceKind::DropMissing(source)),
        }
    }

    #[must_use]
    pub fn replace_missing_with_value(source: DynOperand, replacement: DynValue) -> Self {
        Self {
            kind: Box::new(DynArgumentSourceKind::ReplaceMissing {
                source,
                replacement: DynArgumentReplacement::Value(replacement),
            }),
        }
    }

    #[must_use]
    pub fn replace_missing_with_mask(source: DynOperand, replacement: bool) -> Self {
        Self {
            kind: Box::new(DynArgumentSourceKind::ReplaceMissing {
                source,
                replacement: DynArgumentReplacement::Mask(replacement),
            }),
        }
    }

    #[must_use]
    pub fn replace_missing_with_operand(source: DynOperand, replacement: DynOperand) -> Self {
        Self {
            kind: Box::new(DynArgumentSourceKind::ReplaceMissing {
                source,
                replacement: DynArgumentReplacement::Operand(replacement),
            }),
        }
    }

    #[must_use]
    pub fn descriptor(&self) -> ValueArgumentDescriptor {
        match self.kind.as_ref() {
            DynArgumentSourceKind::Value(value) => {
                ValueArgumentDescriptor::literal(value.descriptor())
            }
            DynArgumentSourceKind::Mask(_) | DynArgumentSourceKind::MaskValues(_) => {
                ValueArgumentDescriptor::literal(ValueDescriptor::value::<Mask>())
            }
            DynArgumentSourceKind::Values(values) => ValueArgumentDescriptor::literal(
                values
                    .first()
                    .map_or_else(ValueDescriptor::unit, DynValue::descriptor),
            ),
            DynArgumentSourceKind::Operand(operand) => {
                ValueArgumentDescriptor::operand(operand.descriptor().clone())
            }
            DynArgumentSourceKind::DropMissing(operand) => {
                ValueArgumentDescriptor::operand(operand.descriptor().clone())
                    .with_missing(ArgumentMissingPolicy::Drop)
            }
            DynArgumentSourceKind::ReplaceMissing {
                source,
                replacement,
            } => ValueArgumentDescriptor::operand(source.descriptor().clone())
                .with_missing(ArgumentMissingPolicy::Replace(replacement.source())),
        }
    }

    pub(crate) fn is_literal_set(&self) -> bool {
        matches!(
            self.kind.as_ref(),
            DynArgumentSourceKind::Values(_) | DynArgumentSourceKind::MaskValues(_)
        )
    }

    pub(crate) fn as_operand(&self) -> &DynOperand {
        let DynArgumentSourceKind::Operand(operand) = self.kind.as_ref() else {
            panic!("argument conversion violated the operand-backed dynamic set-source corner")
        };

        operand
    }

    #[must_use]
    pub fn is_dropping(&self) -> bool {
        matches!(self.kind.as_ref(), DynArgumentSourceKind::DropMissing(_))
    }

    #[must_use]
    pub fn is_mask(&self) -> bool {
        self.descriptor().value().domain().is::<Mask>()
    }
}

impl DynInvokeArgument {
    #[must_use]
    pub fn descriptor(&self) -> ArgumentDescriptor {
        match self {
            Self::Source(source) => ArgumentDescriptor::Value(source.descriptor()),
            Self::Operand(operand) => ArgumentDescriptor::Operand(operand.descriptor().clone()),
            Self::CastTarget(target) => match target {
                DynCastTarget::Bool => ArgumentDescriptor::selector::<BoolTarget>(),
                DynCastTarget::DateTime => ArgumentDescriptor::selector::<DateTimeTarget>(),
                DynCastTarget::Duration => ArgumentDescriptor::selector::<DurationTarget>(),
                DynCastTarget::Float => ArgumentDescriptor::selector::<FloatTarget>(),
                DynCastTarget::Int => ArgumentDescriptor::selector::<IntTarget>(),
                DynCastTarget::String => ArgumentDescriptor::selector::<StringTarget>(),
            },
            Self::ValueTarget(target) => target.argument_descriptor(),
            Self::Attribute(_) => ArgumentDescriptor::field::<GraphRecordAttribute>(),
            Self::Group(_) => ArgumentDescriptor::field::<Group>(),
            Self::Direction(_) => ArgumentDescriptor::field::<EdgeDirection>(),
            Self::Position(_) => ArgumentDescriptor::field::<usize>(),
        }
    }
}

impl DynArgumentBuilder<Keyed<DynIndex>, Preserving> for DynValue {
    type Dynamic = Self;

    fn build(source: &DynArgumentSource) -> Argument<Keyed<DynIndex>, Self::Dynamic, Preserving> {
        match source.kind.as_ref() {
            DynArgumentSourceKind::Value(value) => value.clone().into_argument(),
            DynArgumentSourceKind::Operand(operand) => {
                DynArgumentReplacement::Operand(operand.clone()).keyed_value()
            }
            DynArgumentSourceKind::ReplaceMissing {
                source,
                replacement,
            } => {
                let replacement = replacement.keyed_value();
                match &source.handle {
                    DynHandle::Lane(DynLaneHandle::IndexedValue(
                        DynArityHandle::MultipleOrdered(handle),
                    )) => {
                        WithMissing::new(handle.clone(), Replace::new(replacement)).into_argument()
                    }
                    DynHandle::Lane(DynLaneHandle::IndexedValue(
                        DynArityHandle::MultipleUnordered(handle),
                    )) => {
                        WithMissing::new(handle.clone(), Replace::new(replacement)).into_argument()
                    }
                    _ => panic!(
                        "argument conversion violated the replaceable keyed dynamic-value source roster"
                    ),
                }
            }
            DynArgumentSourceKind::Mask(_)
            | DynArgumentSourceKind::Values(_)
            | DynArgumentSourceKind::MaskValues(_)
            | DynArgumentSourceKind::DropMissing(_) => {
                panic!("argument conversion violated the preserving keyed dynamic-value corner")
            }
        }
    }
}

impl DynArgumentBuilder<Keyed<DynIndex>, Dropping> for DynValue {
    type Dynamic = Self;

    fn build(source: &DynArgumentSource) -> Argument<Keyed<DynIndex>, Self::Dynamic, Dropping> {
        let DynArgumentSourceKind::DropMissing(operand) = source.kind.as_ref() else {
            panic!("argument conversion violated the dropping keyed dynamic-value corner")
        };
        match &operand.handle {
            DynHandle::Lane(DynLaneHandle::IndexedValue(DynArityHandle::MultipleOrdered(
                handle,
            ))) => WithMissing::new(handle.clone(), Drop).into_argument(),
            DynHandle::Lane(DynLaneHandle::IndexedValue(DynArityHandle::MultipleUnordered(
                handle,
            ))) => WithMissing::new(handle.clone(), Drop).into_argument(),
            _ => panic!("argument conversion violated the droppable keyed dynamic-value roster"),
        }
    }
}

impl DynArgumentBuilder<Unaligned, Preserving> for DynValue {
    type Dynamic = Self;

    fn build(source: &DynArgumentSource) -> Argument<Unaligned, Self::Dynamic, Preserving> {
        match source.kind.as_ref() {
            DynArgumentSourceKind::Value(value) => value.clone().into_argument(),
            DynArgumentSourceKind::Operand(operand) => {
                DynArgumentReplacement::Operand(operand.clone()).unaligned_value()
            }
            DynArgumentSourceKind::ReplaceMissing {
                source,
                replacement,
            } => {
                let replacement = replacement.unaligned_value();
                let DynHandle::Lane(DynLaneHandle::BareValue(DynArityHandle::Single(handle))) =
                    &source.handle
                else {
                    panic!(
                        "argument conversion violated the replaceable unaligned dynamic-value source roster"
                    )
                };
                WithMissing::new(handle.clone(), Replace::new(replacement)).into_argument()
            }
            DynArgumentSourceKind::Mask(_)
            | DynArgumentSourceKind::Values(_)
            | DynArgumentSourceKind::MaskValues(_)
            | DynArgumentSourceKind::DropMissing(_) => {
                panic!("argument conversion violated the preserving unaligned dynamic-value corner")
            }
        }
    }
}

impl DynArgumentBuilder<Unaligned, Dropping> for DynValue {
    type Dynamic = Self;

    fn build(source: &DynArgumentSource) -> Argument<Unaligned, Self::Dynamic, Dropping> {
        let DynArgumentSourceKind::DropMissing(operand) = source.kind.as_ref() else {
            panic!("argument conversion violated the dropping unaligned dynamic-value corner")
        };
        let DynHandle::Lane(DynLaneHandle::BareValue(DynArityHandle::Single(handle))) =
            &operand.handle
        else {
            panic!("argument conversion violated the droppable unaligned dynamic-value roster")
        };
        WithMissing::new(handle.clone(), Drop).into_argument()
    }
}

impl DynArgumentBuilder<Keyed<DynIndex>, Preserving> for Mask {
    type Dynamic = Self;

    fn build(source: &DynArgumentSource) -> Argument<Keyed<DynIndex>, Self::Dynamic, Preserving> {
        match source.kind.as_ref() {
            DynArgumentSourceKind::Mask(value) => (*value).into_argument(),
            DynArgumentSourceKind::Operand(operand) => {
                DynArgumentReplacement::Operand(operand.clone()).keyed_mask()
            }
            DynArgumentSourceKind::ReplaceMissing {
                source,
                replacement,
            } => {
                let replacement = replacement.keyed_mask();
                match &source.handle {
                    DynHandle::Lane(DynLaneHandle::IndexedMask(
                        DynArityHandle::MultipleOrdered(handle),
                    )) => {
                        WithMissing::new(handle.clone(), Replace::new(replacement)).into_argument()
                    }
                    DynHandle::Lane(DynLaneHandle::IndexedMask(
                        DynArityHandle::MultipleUnordered(handle),
                    )) => {
                        WithMissing::new(handle.clone(), Replace::new(replacement)).into_argument()
                    }
                    _ => panic!(
                        "argument conversion violated the replaceable keyed mask source roster"
                    ),
                }
            }
            DynArgumentSourceKind::Value(_)
            | DynArgumentSourceKind::Values(_)
            | DynArgumentSourceKind::MaskValues(_)
            | DynArgumentSourceKind::DropMissing(_) => {
                panic!("argument conversion violated the preserving keyed mask corner")
            }
        }
    }
}

impl DynArgumentBuilder<Keyed<DynIndex>, Dropping> for Mask {
    type Dynamic = Self;

    fn build(source: &DynArgumentSource) -> Argument<Keyed<DynIndex>, Self::Dynamic, Dropping> {
        let DynArgumentSourceKind::DropMissing(operand) = source.kind.as_ref() else {
            panic!("argument conversion violated the dropping keyed mask corner")
        };
        match &operand.handle {
            DynHandle::Lane(DynLaneHandle::IndexedMask(DynArityHandle::MultipleOrdered(
                handle,
            ))) => WithMissing::new(handle.clone(), Drop).into_argument(),
            DynHandle::Lane(DynLaneHandle::IndexedMask(DynArityHandle::MultipleUnordered(
                handle,
            ))) => WithMissing::new(handle.clone(), Drop).into_argument(),
            _ => panic!("argument conversion violated the droppable keyed mask roster"),
        }
    }
}

impl DynArgumentBuilder<Unaligned, Preserving> for Mask {
    type Dynamic = Self;

    fn build(source: &DynArgumentSource) -> Argument<Unaligned, Self::Dynamic, Preserving> {
        match source.kind.as_ref() {
            DynArgumentSourceKind::Mask(value) => (*value).into_argument(),
            DynArgumentSourceKind::Operand(operand) => {
                DynArgumentReplacement::Operand(operand.clone()).unaligned_mask()
            }
            DynArgumentSourceKind::ReplaceMissing {
                source,
                replacement,
            } => {
                let replacement = replacement.unaligned_mask();
                let DynHandle::Lane(DynLaneHandle::BareMask(DynArityHandle::Single(handle))) =
                    &source.handle
                else {
                    panic!(
                        "argument conversion violated the replaceable unaligned mask source roster"
                    )
                };
                WithMissing::new(handle.clone(), Replace::new(replacement)).into_argument()
            }
            DynArgumentSourceKind::Value(_)
            | DynArgumentSourceKind::Values(_)
            | DynArgumentSourceKind::MaskValues(_)
            | DynArgumentSourceKind::DropMissing(_) => {
                panic!("argument conversion violated the preserving unaligned mask corner")
            }
        }
    }
}

impl DynArgumentBuilder<Unaligned, Dropping> for Mask {
    type Dynamic = Self;

    fn build(source: &DynArgumentSource) -> Argument<Unaligned, Self::Dynamic, Dropping> {
        let DynArgumentSourceKind::DropMissing(operand) = source.kind.as_ref() else {
            panic!("argument conversion violated the dropping unaligned mask corner")
        };
        let DynHandle::Lane(DynLaneHandle::BareMask(DynArityHandle::Single(handle))) =
            &operand.handle
        else {
            panic!("argument conversion violated the droppable unaligned mask roster")
        };
        WithMissing::new(handle.clone(), Drop).into_argument()
    }
}

impl DynArgumentBuilder<Keyed<DynIndex>, Preserving> for Unit {
    type Dynamic = Self;

    fn build(_source: &DynArgumentSource) -> Argument<Keyed<DynIndex>, Self::Dynamic, Preserving> {
        panic!("registry admitted a unit value as a dynamic argument source")
    }
}

impl DynArgumentBuilder<Keyed<DynIndex>, Dropping> for Unit {
    type Dynamic = Self;

    fn build(_source: &DynArgumentSource) -> Argument<Keyed<DynIndex>, Self::Dynamic, Dropping> {
        panic!("registry admitted a unit value as a dynamic argument source")
    }
}

impl DynArgumentBuilder<Unaligned, Preserving> for Unit {
    type Dynamic = Self;

    fn build(_source: &DynArgumentSource) -> Argument<Unaligned, Self::Dynamic, Preserving> {
        panic!("registry admitted a unit value as a dynamic argument source")
    }
}

impl DynArgumentBuilder<Unaligned, Dropping> for Unit {
    type Dynamic = Self;

    fn build(_source: &DynArgumentSource) -> Argument<Unaligned, Self::Dynamic, Dropping> {
        panic!("registry admitted a unit value as a dynamic argument source")
    }
}

impl DynSetLiteral for DynValue {
    type Element = Self;

    fn literal(source: &DynArgumentSource) -> Vec<Self::Element> {
        let DynArgumentSourceKind::Values(values) = source.kind.as_ref() else {
            panic!("registry admitted a mask literal set for a dynamic-value receiver")
        };

        values.clone()
    }
}

impl DynSetLiteral for Mask {
    type Element = bool;

    fn literal(source: &DynArgumentSource) -> Vec<Self::Element> {
        let DynArgumentSourceKind::MaskValues(values) = source.kind.as_ref() else {
            panic!("registry admitted a dynamic-value literal set for a mask receiver")
        };

        values.clone()
    }
}

impl<S> Keyable<S> {
    pub const fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S: SourceDomain<ValueDomain = DynValue>> SourceDomain for Keyable<S> {
    type ValueDomain = DynValue;
}

impl<S: Prepare> Prepare for Keyable<S> {
    type Prepared<'a>
        = S::Prepared<'a>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.inner.prepare(graphrecord, cache)
    }
}

impl<S: Explain> Explain for Keyable<S> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        self.inner.describe(formatter)
    }
}

impl<S: PlanIdentity> PlanIdentity for Keyable<S> {
    fn identity_eq(&self, other: &Self) -> bool {
        self.inner.identity_eq(&other.inner)
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        self.inner.identity_hash(state);
    }
}

impl<S: PlanInputs> PlanInputs for Keyable<S> {
    fn inputs(&self) -> Vec<&dyn PlanNode> {
        self.inner.inputs()
    }

    fn optimize(&self, session: &Session) -> Transformed<Self> {
        let inner = self.inner.optimize(session);
        let (inner, changed) = inner.into_parts();
        let keyable = Self::new(inner);
        if changed {
            Transformed::changed(keyable)
        } else {
            Transformed::unchanged(keyable)
        }
    }
}

impl<S: Estimated> Estimated for Keyable<S> {
    fn estimate(&self, stats: &Stats) -> Estimate {
        self.inner.estimate(stats)
    }
}

impl<A, S> ArgumentSource<A> for Keyable<S>
where
    A: Alignment,
    S: ArgumentSource<A, DynValue> + SourceDomain<ValueDomain = DynValue>,
{
    type Retention = S::Retention;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        address: &A::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<DynValue>>
    where
        Self: 'a,
    {
        S::lookup(prepared, address)
    }

    fn resolve<'a>(
        prepared: &Self::Prepared<'a>,
        address: &A::Address<'a>,
        label: &'static str,
    ) -> <Self::Retention as ElementEmission>::Step<QueryResult<DynValue>>
    where
        Self: 'a,
    {
        <S::Retention as Retention>::map_step(S::resolve(prepared, address, label), |outcome| {
            outcome.and_then(DynValue::into_groupable)
        })
    }
}

impl<S> IndexedElementSource for Keyable<S>
where
    S: IndexedElementSource<ValueDomain = DynValue>,
{
    type Arity = S::Arity;
    type IndexDomain = S::IndexDomain;

    fn elements<'a>(
        prepared: Self::Prepared<'a>,
    ) -> IndexedElementContainer<'a, Self::IndexDomain, DynValue, Self::Arity>
    where
        Self: 'a,
    {
        Self::Arity::map_elements(S::elements(prepared), |(index, outcome)| {
            (index, outcome.and_then(DynValue::into_groupable))
        })
    }
}

impl Explain for DynValue {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{self}")
    }
}

impl PlanIdentity for DynValue {
    fn identity_eq(&self, other: &Self) -> bool {
        self == other
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        self.hash(state);
    }
}

impl PlanInputs for DynValue {}

impl Prepare for DynValue {
    type Prepared<'a> = QueryResult<Self>;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(Ok(self.clone()))
    }
}

impl Estimated for DynValue {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl SourceDomain for DynValue {
    type ValueDomain = Self;
}

impl<A: Alignment> ArgumentSource<A> for DynValue {
    type Retention = Preserving;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        _address: &A::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<Self>>
    where
        Self: 'a,
    {
        Lookup::Present(prepared)
    }
}
