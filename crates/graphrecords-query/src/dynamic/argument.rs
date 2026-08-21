use super::{
    DynArityHandle, DynExpression, DynHandle, DynIndex, DynLaneHandle, DynValue, DynValueView,
};
use crate::{
    Arity, EdgeDirection, Explain, FailureKind, FailureKindValue, IndexValue, Mask, Positional,
    QueryResult, Scalar, Series, Unit, ValueDomain,
    cast::{
        Bool as BoolTarget, DateTime as DateTimeTarget, Duration as DurationTarget,
        Float as FloatTarget, Int as IntTarget, String as StringTarget,
    },
    element::{Dropping, ElementEmission, Preserving, Retention},
    execution::EvaluationCache,
    explain::ExplainFormatter,
    operations::{
        Alignment, Argument, ArgumentSource, IndexedElementContainer, IndexedElementSource,
        IntoArgument, Keyed, Lookup, MissingPolicy, Prepare, SourceDomain, Unaligned, WithMissing,
        policy::{Drop, Replace},
    },
    optimizer::{
        Estimate, Estimated, PlanIdentity, PlanInputs, PlanNode, Session, Stats, Transformed,
    },
    registry::{
        ArgumentDescriptor, ArgumentMissingPolicy, ExpressionDescriptor, ValueArgumentDescriptor,
        ValueDescriptor,
    },
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{AttributeName, GroupIndex, NodeIndex, Value},
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

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum DynValueTarget {
    Value,
    ValueIndex,
    AttributeName,
    AttributeNameIndex,
    NodeIndex,
    GroupIndex,
    PositionalIndex,
    BoolIndex,
    Mask,
    FailureKind,
    FailureKindIndex,
}

#[derive(Clone)]
pub enum DynArgumentLane {
    Expression(DynExpression),
    Series(Box<Series<DynExpression>>),
}

#[derive(Clone)]
enum DynArgumentReplacement {
    Value(DynValue),
    Mask(bool),
    Lane(DynArgumentLane),
    Source(DynArgumentSource),
}

#[derive(Clone)]
enum DynArgumentSourceKind {
    Value(DynValue),
    Values(Vec<DynValue>),
    MaskValues(Vec<bool>),
    Mask(bool),
    Lane(DynArgumentLane),
    DropMissing(DynArgumentLane),
    ReplaceMissing {
        source: DynArgumentLane,
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
    Lane(DynArgumentLane),
    CastTarget(DynCastTarget),
    ValueTarget(DynValueTarget),
    Attribute(AttributeName),
    Group(GroupIndex),
    Direction(EdgeDirection),
    Position(usize),
}

impl DynArgumentLane {
    #[must_use]
    pub const fn descriptor(&self) -> &ExpressionDescriptor {
        match self {
            Self::Expression(expression) => expression.descriptor(),
            Self::Series(series) => series.expression().descriptor(),
        }
    }

    pub(crate) fn erase_mask_lane(&self) -> Self {
        match self {
            Self::Expression(expression) => Self::Expression(expression.erase_mask_lane()),
            Self::Series(series) => {
                Self::Series(Box::new(series.bind(series.expression().erase_mask_lane())))
            }
        }
    }

    fn keyed_value(&self) -> Argument<Keyed<DynIndex>, DynValue, Preserving> {
        match self {
            Self::Expression(expression) => match &expression.handle {
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
            Self::Series(series) => match &series.expression().handle {
                DynHandle::Lane(DynLaneHandle::IndexedValue(DynArityHandle::MultipleOrdered(
                    handle,
                ))) => series.bind(handle.clone()).into_argument(),
                DynHandle::Lane(DynLaneHandle::IndexedValue(
                    DynArityHandle::MultipleUnordered(handle),
                )) => series.bind(handle.clone()).into_argument(),
                DynHandle::Lane(DynLaneHandle::BareValue(DynArityHandle::Single(handle))) => {
                    series.bind(handle.clone()).into_argument()
                }
                DynHandle::Lane(DynLaneHandle::BareValue(DynArityHandle::Definite(handle))) => {
                    series.bind(handle.clone()).into_argument()
                }
                _ => panic!("argument conversion violated the keyed dynamic-value series roster"),
            },
        }
    }

    fn keyed_value_on_missing<P: MissingPolicy<Keyed<DynIndex>, DynValue>>(
        &self,
        policy: P,
    ) -> Argument<Keyed<DynIndex>, DynValue, P::Retention> {
        match self {
            Self::Expression(expression) => match &expression.handle {
                DynHandle::Lane(DynLaneHandle::IndexedValue(DynArityHandle::MultipleOrdered(
                    handle,
                ))) => WithMissing::new(handle.clone(), policy).into_argument(),
                DynHandle::Lane(DynLaneHandle::IndexedValue(
                    DynArityHandle::MultipleUnordered(handle),
                )) => WithMissing::new(handle.clone(), policy).into_argument(),
                DynHandle::Lane(DynLaneHandle::BareValue(DynArityHandle::Single(handle))) => {
                    WithMissing::new(handle.clone(), policy).into_argument()
                }
                _ => panic!(
                    "argument conversion violated the keyed dynamic-value on-missing source roster"
                ),
            },
            Self::Series(series) => match &series.expression().handle {
                DynHandle::Lane(DynLaneHandle::IndexedValue(DynArityHandle::MultipleOrdered(
                    handle,
                ))) => WithMissing::new(series.bind(handle.clone()), policy).into_argument(),
                DynHandle::Lane(DynLaneHandle::IndexedValue(
                    DynArityHandle::MultipleUnordered(handle),
                )) => WithMissing::new(series.bind(handle.clone()), policy).into_argument(),
                DynHandle::Lane(DynLaneHandle::BareValue(DynArityHandle::Single(handle))) => {
                    WithMissing::new(series.bind(handle.clone()), policy).into_argument()
                }
                _ => panic!(
                    "argument conversion violated the keyed dynamic-value on-missing series roster"
                ),
            },
        }
    }

    fn unaligned_value(&self) -> Argument<Unaligned, DynValue, Preserving> {
        match self {
            Self::Expression(expression) => match &expression.handle {
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
            Self::Series(series) => match &series.expression().handle {
                DynHandle::Lane(DynLaneHandle::BareValue(DynArityHandle::Single(handle))) => {
                    series.bind(handle.clone()).into_argument()
                }
                DynHandle::Lane(DynLaneHandle::BareValue(DynArityHandle::Definite(handle))) => {
                    series.bind(handle.clone()).into_argument()
                }
                _ => {
                    panic!("argument conversion violated the unaligned dynamic-value series roster")
                }
            },
        }
    }

    fn unaligned_value_on_missing<P: MissingPolicy<Unaligned, DynValue>>(
        &self,
        policy: P,
    ) -> Argument<Unaligned, DynValue, P::Retention> {
        match self {
            Self::Expression(expression) => {
                let DynHandle::Lane(DynLaneHandle::BareValue(DynArityHandle::Single(handle))) =
                    &expression.handle
                else {
                    panic!(
                        "argument conversion violated the unaligned dynamic-value on-missing source roster"
                    )
                };

                WithMissing::new(handle.clone(), policy).into_argument()
            }
            Self::Series(series) => {
                let DynHandle::Lane(DynLaneHandle::BareValue(DynArityHandle::Single(handle))) =
                    &series.expression().handle
                else {
                    panic!(
                        "argument conversion violated the unaligned dynamic-value on-missing series roster"
                    )
                };

                WithMissing::new(series.bind(handle.clone()), policy).into_argument()
            }
        }
    }

    fn keyed_mask(&self) -> Argument<Keyed<DynIndex>, Mask, Preserving> {
        match self {
            Self::Expression(expression) => match &expression.handle {
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
            Self::Series(series) => match &series.expression().handle {
                DynHandle::Lane(DynLaneHandle::IndexedMask(DynArityHandle::MultipleOrdered(
                    handle,
                ))) => series.bind(handle.clone()).into_argument(),
                DynHandle::Lane(DynLaneHandle::IndexedMask(DynArityHandle::MultipleUnordered(
                    handle,
                ))) => series.bind(handle.clone()).into_argument(),
                DynHandle::Lane(DynLaneHandle::BareMask(DynArityHandle::Single(handle))) => {
                    series.bind(handle.clone()).into_argument()
                }
                DynHandle::Lane(DynLaneHandle::BareMask(DynArityHandle::Definite(handle))) => {
                    series.bind(handle.clone()).into_argument()
                }
                _ => panic!("argument conversion violated the keyed mask series roster"),
            },
        }
    }

    fn keyed_mask_on_missing<P: MissingPolicy<Keyed<DynIndex>, Mask>>(
        &self,
        policy: P,
    ) -> Argument<Keyed<DynIndex>, Mask, P::Retention> {
        match self {
            Self::Expression(expression) => match &expression.handle {
                DynHandle::Lane(DynLaneHandle::IndexedMask(DynArityHandle::MultipleOrdered(
                    handle,
                ))) => WithMissing::new(handle.clone(), policy).into_argument(),
                DynHandle::Lane(DynLaneHandle::IndexedMask(DynArityHandle::MultipleUnordered(
                    handle,
                ))) => WithMissing::new(handle.clone(), policy).into_argument(),
                DynHandle::Lane(DynLaneHandle::BareMask(DynArityHandle::Single(handle))) => {
                    WithMissing::new(handle.clone(), policy).into_argument()
                }
                _ => panic!("argument conversion violated the keyed mask on-missing source roster"),
            },
            Self::Series(series) => match &series.expression().handle {
                DynHandle::Lane(DynLaneHandle::IndexedMask(DynArityHandle::MultipleOrdered(
                    handle,
                ))) => WithMissing::new(series.bind(handle.clone()), policy).into_argument(),
                DynHandle::Lane(DynLaneHandle::IndexedMask(DynArityHandle::MultipleUnordered(
                    handle,
                ))) => WithMissing::new(series.bind(handle.clone()), policy).into_argument(),
                DynHandle::Lane(DynLaneHandle::BareMask(DynArityHandle::Single(handle))) => {
                    WithMissing::new(series.bind(handle.clone()), policy).into_argument()
                }
                _ => panic!("argument conversion violated the keyed mask on-missing series roster"),
            },
        }
    }

    fn unaligned_mask(&self) -> Argument<Unaligned, Mask, Preserving> {
        match self {
            Self::Expression(expression) => match &expression.handle {
                DynHandle::Lane(DynLaneHandle::BareMask(DynArityHandle::Single(handle))) => {
                    handle.clone().into_argument()
                }
                DynHandle::Lane(DynLaneHandle::BareMask(DynArityHandle::Definite(handle))) => {
                    handle.clone().into_argument()
                }
                _ => panic!("argument conversion violated the unaligned mask source roster"),
            },
            Self::Series(series) => match &series.expression().handle {
                DynHandle::Lane(DynLaneHandle::BareMask(DynArityHandle::Single(handle))) => {
                    series.bind(handle.clone()).into_argument()
                }
                DynHandle::Lane(DynLaneHandle::BareMask(DynArityHandle::Definite(handle))) => {
                    series.bind(handle.clone()).into_argument()
                }
                _ => panic!("argument conversion violated the unaligned mask series roster"),
            },
        }
    }

    fn unaligned_mask_on_missing<P: MissingPolicy<Unaligned, Mask>>(
        &self,
        policy: P,
    ) -> Argument<Unaligned, Mask, P::Retention> {
        match self {
            Self::Expression(expression) => {
                let DynHandle::Lane(DynLaneHandle::BareMask(DynArityHandle::Single(handle))) =
                    &expression.handle
                else {
                    panic!(
                        "argument conversion violated the unaligned mask on-missing source roster"
                    )
                };

                WithMissing::new(handle.clone(), policy).into_argument()
            }
            Self::Series(series) => {
                let DynHandle::Lane(DynLaneHandle::BareMask(DynArityHandle::Single(handle))) =
                    &series.expression().handle
                else {
                    panic!(
                        "argument conversion violated the unaligned mask on-missing series roster"
                    )
                };

                WithMissing::new(series.bind(handle.clone()), policy).into_argument()
            }
        }
    }
}

impl DynArgumentReplacement {
    fn descriptor(&self) -> ValueArgumentDescriptor {
        match self {
            Self::Value(value) => ValueArgumentDescriptor::literal(value.descriptor()),
            Self::Mask(_) => ValueArgumentDescriptor::literal(ValueDescriptor::value::<Mask>()),
            Self::Lane(lane) => ValueArgumentDescriptor::expression(lane.descriptor().clone()),
            Self::Source(source) => source.descriptor(),
        }
    }

    fn is_dropping(&self) -> bool {
        match self {
            Self::Source(source) => source.is_dropping(),
            Self::Value(_) | Self::Mask(_) | Self::Lane(_) => false,
        }
    }

    fn keyed_value(&self) -> Argument<Keyed<DynIndex>, DynValue, Preserving> {
        match self {
            Self::Value(value) => value.clone().into_argument(),
            Self::Lane(lane) => lane.keyed_value(),
            Self::Source(source) => DynValue::build(source),
            Self::Mask(_) => {
                panic!("argument conversion paired a mask replacement with a dynamic-value source")
            }
        }
    }

    fn keyed_value_dropping(&self) -> Argument<Keyed<DynIndex>, DynValue, Dropping> {
        let Self::Source(source) = self else {
            panic!("argument conversion routed a preserving replacement through the dropping road")
        };

        DynValue::build(source)
    }

    fn unaligned_value(&self) -> Argument<Unaligned, DynValue, Preserving> {
        match self {
            Self::Value(value) => value.clone().into_argument(),
            Self::Lane(lane) => lane.unaligned_value(),
            Self::Source(source) => DynValue::build(source),
            Self::Mask(_) => {
                panic!("argument conversion paired a mask replacement with a dynamic-value source")
            }
        }
    }

    fn unaligned_value_dropping(&self) -> Argument<Unaligned, DynValue, Dropping> {
        let Self::Source(source) = self else {
            panic!("argument conversion routed a preserving replacement through the dropping road")
        };

        DynValue::build(source)
    }

    fn keyed_mask(&self) -> Argument<Keyed<DynIndex>, Mask, Preserving> {
        match self {
            Self::Mask(value) => (*value).into_argument(),
            Self::Lane(lane) => lane.keyed_mask(),
            Self::Source(source) => Mask::build(source),
            Self::Value(_) => {
                panic!("argument conversion paired a dynamic-value replacement with a mask source")
            }
        }
    }

    fn keyed_mask_dropping(&self) -> Argument<Keyed<DynIndex>, Mask, Dropping> {
        let Self::Source(source) = self else {
            panic!("argument conversion routed a preserving replacement through the dropping road")
        };

        Mask::build(source)
    }

    fn unaligned_mask(&self) -> Argument<Unaligned, Mask, Preserving> {
        match self {
            Self::Mask(value) => (*value).into_argument(),
            Self::Lane(lane) => lane.unaligned_mask(),
            Self::Source(source) => Mask::build(source),
            Self::Value(_) => {
                panic!("argument conversion paired a dynamic-value replacement with a mask source")
            }
        }
    }

    fn unaligned_mask_dropping(&self) -> Argument<Unaligned, Mask, Dropping> {
        let Self::Source(source) = self else {
            panic!("argument conversion routed a preserving replacement through the dropping road")
        };

        Mask::build(source)
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
    pub fn lane(lane: DynArgumentLane) -> Self {
        Self {
            kind: Box::new(DynArgumentSourceKind::Lane(lane)),
        }
    }

    #[must_use]
    pub fn drop_missing(source: DynArgumentLane) -> Self {
        Self {
            kind: Box::new(DynArgumentSourceKind::DropMissing(source)),
        }
    }

    #[must_use]
    pub fn replace_missing_with_value(source: DynArgumentLane, replacement: DynValue) -> Self {
        Self {
            kind: Box::new(DynArgumentSourceKind::ReplaceMissing {
                source,
                replacement: DynArgumentReplacement::Value(replacement),
            }),
        }
    }

    #[must_use]
    pub fn replace_missing_with_mask(source: DynArgumentLane, replacement: bool) -> Self {
        Self {
            kind: Box::new(DynArgumentSourceKind::ReplaceMissing {
                source,
                replacement: DynArgumentReplacement::Mask(replacement),
            }),
        }
    }

    #[must_use]
    pub fn replace_missing_with_lane(
        source: DynArgumentLane,
        replacement: DynArgumentLane,
    ) -> Self {
        Self {
            kind: Box::new(DynArgumentSourceKind::ReplaceMissing {
                source,
                replacement: DynArgumentReplacement::Lane(replacement),
            }),
        }
    }

    #[must_use]
    pub fn replace_missing_with_source(source: DynArgumentLane, replacement: Self) -> Self {
        Self {
            kind: Box::new(DynArgumentSourceKind::ReplaceMissing {
                source,
                replacement: DynArgumentReplacement::Source(replacement),
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
            DynArgumentSourceKind::Lane(lane) => {
                ValueArgumentDescriptor::expression(lane.descriptor().clone())
            }
            DynArgumentSourceKind::DropMissing(lane) => {
                ValueArgumentDescriptor::expression(lane.descriptor().clone())
                    .with_missing(ArgumentMissingPolicy::Drop)
            }
            DynArgumentSourceKind::ReplaceMissing {
                source,
                replacement,
            } => ValueArgumentDescriptor::expression(source.descriptor().clone()).with_missing(
                ArgumentMissingPolicy::Replace(Box::new(replacement.descriptor())),
            ),
        }
    }

    pub(crate) fn is_literal_set(&self) -> bool {
        matches!(
            self.kind.as_ref(),
            DynArgumentSourceKind::Values(_) | DynArgumentSourceKind::MaskValues(_)
        )
    }

    pub(crate) fn as_lane(&self) -> &DynArgumentLane {
        let DynArgumentSourceKind::Lane(lane) = self.kind.as_ref() else {
            panic!("argument conversion violated the lane-backed dynamic set-source corner")
        };

        lane
    }

    #[must_use]
    pub fn is_dropping(&self) -> bool {
        match self.kind.as_ref() {
            DynArgumentSourceKind::DropMissing(_) => true,
            DynArgumentSourceKind::ReplaceMissing { replacement, .. } => replacement.is_dropping(),
            DynArgumentSourceKind::Value(_)
            | DynArgumentSourceKind::Values(_)
            | DynArgumentSourceKind::MaskValues(_)
            | DynArgumentSourceKind::Mask(_)
            | DynArgumentSourceKind::Lane(_) => false,
        }
    }

    #[must_use]
    pub fn dropping_lane(&self) -> Option<&DynArgumentLane> {
        match self.kind.as_ref() {
            DynArgumentSourceKind::DropMissing(lane) => Some(lane),
            DynArgumentSourceKind::Value(_)
            | DynArgumentSourceKind::Values(_)
            | DynArgumentSourceKind::MaskValues(_)
            | DynArgumentSourceKind::Mask(_)
            | DynArgumentSourceKind::Lane(_)
            | DynArgumentSourceKind::ReplaceMissing { .. } => None,
        }
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
            Self::Lane(lane) => ArgumentDescriptor::Expression(lane.descriptor().clone()),
            Self::CastTarget(target) => match target {
                DynCastTarget::Bool => ArgumentDescriptor::selector::<BoolTarget>(),
                DynCastTarget::DateTime => ArgumentDescriptor::selector::<DateTimeTarget>(),
                DynCastTarget::Duration => ArgumentDescriptor::selector::<DurationTarget>(),
                DynCastTarget::Float => ArgumentDescriptor::selector::<FloatTarget>(),
                DynCastTarget::Int => ArgumentDescriptor::selector::<IntTarget>(),
                DynCastTarget::String => ArgumentDescriptor::selector::<StringTarget>(),
            },
            Self::ValueTarget(target) => match target {
                DynValueTarget::Value => ArgumentDescriptor::selector::<Scalar>(),
                DynValueTarget::ValueIndex => ArgumentDescriptor::selector::<IndexValue<Value>>(),
                DynValueTarget::AttributeName => ArgumentDescriptor::selector::<AttributeName>(),
                DynValueTarget::AttributeNameIndex => {
                    ArgumentDescriptor::selector::<IndexValue<AttributeName>>()
                }
                DynValueTarget::NodeIndex => {
                    ArgumentDescriptor::selector::<IndexValue<NodeIndex>>()
                }
                DynValueTarget::GroupIndex => {
                    ArgumentDescriptor::selector::<IndexValue<GroupIndex>>()
                }
                DynValueTarget::PositionalIndex => {
                    ArgumentDescriptor::selector::<IndexValue<Positional>>()
                }
                DynValueTarget::BoolIndex => ArgumentDescriptor::selector::<IndexValue<bool>>(),
                DynValueTarget::Mask => ArgumentDescriptor::selector::<Mask>(),
                DynValueTarget::FailureKind => ArgumentDescriptor::selector::<FailureKindValue>(),
                DynValueTarget::FailureKindIndex => {
                    ArgumentDescriptor::selector::<IndexValue<FailureKind>>()
                }
            },
            Self::Attribute(_) => ArgumentDescriptor::field::<AttributeName>(),
            Self::Group(_) => ArgumentDescriptor::field::<GroupIndex>(),
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
            DynArgumentSourceKind::Lane(lane) => lane.keyed_value(),
            DynArgumentSourceKind::ReplaceMissing {
                source,
                replacement,
            } => source.keyed_value_on_missing(Replace(replacement.keyed_value())),
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
        match source.kind.as_ref() {
            DynArgumentSourceKind::DropMissing(lane) => lane.keyed_value_on_missing(Drop),
            DynArgumentSourceKind::ReplaceMissing {
                source,
                replacement,
            } => source.keyed_value_on_missing(Replace(replacement.keyed_value_dropping())),
            DynArgumentSourceKind::Value(_)
            | DynArgumentSourceKind::Values(_)
            | DynArgumentSourceKind::MaskValues(_)
            | DynArgumentSourceKind::Mask(_)
            | DynArgumentSourceKind::Lane(_) => {
                panic!("argument conversion violated the dropping keyed dynamic-value corner")
            }
        }
    }
}

impl DynArgumentBuilder<Unaligned, Preserving> for DynValue {
    type Dynamic = Self;

    fn build(source: &DynArgumentSource) -> Argument<Unaligned, Self::Dynamic, Preserving> {
        match source.kind.as_ref() {
            DynArgumentSourceKind::Value(value) => value.clone().into_argument(),
            DynArgumentSourceKind::Lane(lane) => lane.unaligned_value(),
            DynArgumentSourceKind::ReplaceMissing {
                source,
                replacement,
            } => source.unaligned_value_on_missing(Replace(replacement.unaligned_value())),
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
        match source.kind.as_ref() {
            DynArgumentSourceKind::DropMissing(lane) => lane.unaligned_value_on_missing(Drop),
            DynArgumentSourceKind::ReplaceMissing {
                source,
                replacement,
            } => source.unaligned_value_on_missing(Replace(replacement.unaligned_value_dropping())),
            DynArgumentSourceKind::Value(_)
            | DynArgumentSourceKind::Values(_)
            | DynArgumentSourceKind::MaskValues(_)
            | DynArgumentSourceKind::Mask(_)
            | DynArgumentSourceKind::Lane(_) => {
                panic!("argument conversion violated the dropping unaligned dynamic-value corner")
            }
        }
    }
}

impl DynArgumentBuilder<Keyed<DynIndex>, Preserving> for Mask {
    type Dynamic = Self;

    fn build(source: &DynArgumentSource) -> Argument<Keyed<DynIndex>, Self::Dynamic, Preserving> {
        match source.kind.as_ref() {
            DynArgumentSourceKind::Mask(value) => (*value).into_argument(),
            DynArgumentSourceKind::Lane(lane) => lane.keyed_mask(),
            DynArgumentSourceKind::ReplaceMissing {
                source,
                replacement,
            } => source.keyed_mask_on_missing(Replace(replacement.keyed_mask())),
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
        match source.kind.as_ref() {
            DynArgumentSourceKind::DropMissing(lane) => lane.keyed_mask_on_missing(Drop),
            DynArgumentSourceKind::ReplaceMissing {
                source,
                replacement,
            } => source.keyed_mask_on_missing(Replace(replacement.keyed_mask_dropping())),
            DynArgumentSourceKind::Value(_)
            | DynArgumentSourceKind::Values(_)
            | DynArgumentSourceKind::MaskValues(_)
            | DynArgumentSourceKind::Mask(_)
            | DynArgumentSourceKind::Lane(_) => {
                panic!("argument conversion violated the dropping keyed mask corner")
            }
        }
    }
}

impl DynArgumentBuilder<Unaligned, Preserving> for Mask {
    type Dynamic = Self;

    fn build(source: &DynArgumentSource) -> Argument<Unaligned, Self::Dynamic, Preserving> {
        match source.kind.as_ref() {
            DynArgumentSourceKind::Mask(value) => (*value).into_argument(),
            DynArgumentSourceKind::Lane(lane) => lane.unaligned_mask(),
            DynArgumentSourceKind::ReplaceMissing {
                source,
                replacement,
            } => source.unaligned_mask_on_missing(Replace(replacement.unaligned_mask())),
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
        match source.kind.as_ref() {
            DynArgumentSourceKind::DropMissing(lane) => lane.unaligned_mask_on_missing(Drop),
            DynArgumentSourceKind::ReplaceMissing {
                source,
                replacement,
            } => source.unaligned_mask_on_missing(Replace(replacement.unaligned_mask_dropping())),
            DynArgumentSourceKind::Value(_)
            | DynArgumentSourceKind::Values(_)
            | DynArgumentSourceKind::MaskValues(_)
            | DynArgumentSourceKind::Mask(_)
            | DynArgumentSourceKind::Lane(_) => {
                panic!("argument conversion violated the dropping unaligned mask corner")
            }
        }
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

impl<S: Prepare> Prepare for Keyable<S> {
    type Prepared<'a>
        = S::Prepared<'a>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.inner.prepare(graphrecord, cache)
    }
}

impl<S: Estimated> Estimated for Keyable<S> {
    fn estimate(&self, stats: &Stats) -> Estimate {
        self.inner.estimate(stats)
    }
}

impl<S: SourceDomain<ValueDomain = DynValue>> SourceDomain for Keyable<S> {
    type ValueDomain = DynValue;
}

impl<A, S> ArgumentSource<A> for Keyable<S>
where
    A: Alignment,
    S: ArgumentSource<A, DynValue> + SourceDomain<ValueDomain = DynValue>,
{
    type Retention = S::Retention;

    fn lookup<'a>(
        graphrecord: &'a GraphRecord,
        prepared: &Self::Prepared<'a>,
        address: &A::Address,
        label: &'static str,
    ) -> Lookup<QueryResult<DynValueView<'a>>>
    where
        Self: 'a,
    {
        S::lookup(graphrecord, prepared, address, label)
    }

    fn resolve<'a>(
        graphrecord: &'a GraphRecord,
        prepared: &Self::Prepared<'a>,
        address: &A::Address,
        label: &'static str,
    ) -> <Self::Retention as ElementEmission>::Step<QueryResult<DynValueView<'a>>>
    where
        Self: 'a,
    {
        <S::Retention as Retention>::map_step(
            S::resolve(graphrecord, prepared, address, label),
            |outcome| outcome.and_then(DynValueView::into_groupable),
        )
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
    ) -> IndexedElementContainer<'a, Self::IndexDomain, DynValueView<'a>, Self::Arity>
    where
        Self: 'a,
    {
        Self::Arity::map_elements(S::elements(prepared), |(address, outcome)| {
            (address, outcome.and_then(DynValueView::into_groupable))
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
    type Prepared<'a> = &'a Self;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self)
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

    fn lookup<'a>(
        graphrecord: &'a GraphRecord,
        prepared: &Self::Prepared<'a>,
        _address: &A::Address,
        label: &'static str,
    ) -> Lookup<QueryResult<DynValueView<'a>>>
    where
        Self: 'a,
    {
        Lookup::Present(Self::from_owned(graphrecord, prepared, label))
    }
}
