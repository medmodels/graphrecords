use super::{DynIndex, DynIndexAddress, DynIndexOwned, DynIndexView};
use crate::{
    BareValueDomain, Failure, FailureKind, FailureKindValue, FailureValue, IndexDomain, IndexValue,
    Mask, Positional, QueryResult, ReturnValueDomain, Scalar, ValueDomain,
    capabilities::{
        EnsureSortable, PayloadKind, ValueAbsolute, ValueAdd, ValueCast, ValueCeil, ValueClip,
        ValueCubeRoot, ValueDivide, ValueEquality, ValueEquivalence, ValueExponential, ValueFloor,
        ValueGrouping, ValueInt, ValueKindTest, ValueLogarithm, ValueMedian, ValueMode,
        ValueModulo, ValueMultiply, ValueNegate, ValueOrdering, ValuePower, ValueRound,
        ValueScalar, ValueScalarKindTest, ValueSign, ValueSquareRoot, ValueString, ValueSubtract,
        ValueTransition,
    },
    cast::{
        Bool as BoolTarget, DateTime as DateTimeTarget, Duration as DurationTarget,
        Float as FloatTarget, Int as IntTarget, String as StringTarget,
    },
    error::{conversion::InvalidTransition, dispatch::UnsupportedValueRole},
    registry::ValueDescriptor,
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{
        AttributeName, AttributeNameView, EdgeIndex, GroupIndex, NodeIndex, Value, ValueView,
    },
};
use std::{
    cmp::Ordering,
    fmt::{self, Display, Formatter},
    hash::{Hash, Hasher},
    mem::discriminant,
    ptr,
};

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum DynEntityReferenceKind {
    Node(NodeIndex),
    Edge(EdgeIndex),
    Group(GroupIndex),
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct DynEntityReference {
    kind: DynEntityReferenceKind,
}

impl DynEntityReference {
    #[must_use]
    pub(crate) const fn node(index: NodeIndex) -> Self {
        Self {
            kind: DynEntityReferenceKind::Node(index),
        }
    }

    #[must_use]
    pub(crate) const fn edge(index: EdgeIndex) -> Self {
        Self {
            kind: DynEntityReferenceKind::Edge(index),
        }
    }

    #[must_use]
    pub(crate) const fn group(index: GroupIndex) -> Self {
        Self {
            kind: DynEntityReferenceKind::Group(index),
        }
    }

    #[must_use]
    pub const fn node_index(&self) -> Option<&NodeIndex> {
        match &self.kind {
            DynEntityReferenceKind::Node(index) => Some(index),
            DynEntityReferenceKind::Edge(_) | DynEntityReferenceKind::Group(_) => None,
        }
    }

    #[must_use]
    pub const fn edge_index(&self) -> Option<&EdgeIndex> {
        match &self.kind {
            DynEntityReferenceKind::Edge(index) => Some(index),
            DynEntityReferenceKind::Node(_) | DynEntityReferenceKind::Group(_) => None,
        }
    }

    #[must_use]
    pub const fn group_index(&self) -> Option<&GroupIndex> {
        match &self.kind {
            DynEntityReferenceKind::Group(index) => Some(index),
            DynEntityReferenceKind::Node(_) | DynEntityReferenceKind::Edge(_) => None,
        }
    }

    fn from_index(index: &DynIndexView<'_>) -> Self {
        match index {
            DynIndexView::Node(index) => Self::node(NodeIndex::own_index(index)),
            DynIndexView::Edge(index) => Self::edge(EdgeIndex::own_index(index)),
            DynIndexView::Group(index) => Self::group(GroupIndex::own_index(index)),
            index => {
                let index_domain = index.description();
                panic!(
                    "registry admitted an entity reference over dynamic index domain {index_domain}"
                )
            }
        }
    }

    fn resolve(
        &self,
        graphrecord: &GraphRecord,
        label: &'static str,
    ) -> QueryResult<DynIndexAddress> {
        match &self.kind {
            DynEntityReferenceKind::Node(index) => {
                NodeIndex::resolve(graphrecord, index, label).map(DynIndexAddress::Node)
            }
            DynEntityReferenceKind::Edge(index) => {
                EdgeIndex::resolve(graphrecord, index, label).map(DynIndexAddress::Edge)
            }
            DynEntityReferenceKind::Group(index) => {
                GroupIndex::resolve(graphrecord, index, label).map(DynIndexAddress::Group)
            }
        }
    }

    fn descriptor(&self) -> ValueDescriptor {
        match self.kind {
            DynEntityReferenceKind::Node(_) => ValueDescriptor::entity_reference::<NodeIndex>(),
            DynEntityReferenceKind::Edge(_) => ValueDescriptor::entity_reference::<EdgeIndex>(),
            DynEntityReferenceKind::Group(_) => ValueDescriptor::entity_reference::<GroupIndex>(),
        }
    }
}

impl Display for DynEntityReference {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match &self.kind {
            DynEntityReferenceKind::Node(node) => node.fmt(formatter),
            DynEntityReferenceKind::Edge(edge) => edge.fmt(formatter),
            DynEntityReferenceKind::Group(group_index) => group_index.fmt(formatter),
        }
    }
}

pub struct DynEntityRef<'a> {
    graphrecord: &'a GraphRecord,
    address: DynIndexAddress,
}

impl<'a> DynEntityRef<'a> {
    #[must_use]
    pub const fn new(graphrecord: &'a GraphRecord, address: DynIndexAddress) -> Self {
        Self {
            graphrecord,
            address,
        }
    }

    #[must_use]
    pub const fn graphrecord(&self) -> &'a GraphRecord {
        self.graphrecord
    }

    #[must_use]
    pub const fn address(&self) -> &DynIndexAddress {
        &self.address
    }

    #[must_use]
    pub fn index(&self) -> DynIndexView<'a> {
        DynIndex::index(self.graphrecord, &self.address)
    }

    #[must_use]
    pub fn into_owned(self) -> DynEntityReference {
        DynEntityReference::from_index(&self.index())
    }

    const fn description(&self) -> &'static str {
        match self.address {
            DynIndexAddress::Node(_) => "node entity reference",
            DynIndexAddress::Edge(_) => "edge entity reference",
            _ => panic!("registry admitted an entity reference over a non-entity index domain"),
        }
    }
}

impl Clone for DynEntityRef<'_> {
    fn clone(&self) -> Self {
        Self {
            graphrecord: self.graphrecord,
            address: self.address.clone(),
        }
    }
}

impl PartialEq for DynEntityRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        ptr::eq(self.graphrecord, other.graphrecord) && self.address == other.address
    }
}

impl Eq for DynEntityRef<'_> {}

impl Hash for DynEntityRef<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        ptr::from_ref(self.graphrecord).hash(state);
        self.address.hash(state);
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum DynEquivalenceKey<'a> {
    Scalar(ValueView<'a>),
    Attribute(AttributeNameView<'a>),
    Index(DynIndexView<'a>),
    FailureKind(FailureKind),
}

#[derive(Clone, Debug)]
pub enum DynValue {
    Scalar(Value),
    Attribute(AttributeName),
    Index(DynIndexOwned),
    EntityReference(DynEntityReference),
    Failure(Box<Failure>),
    FailureKind(FailureKind),
}

#[derive(Clone)]
pub enum DynValueView<'a> {
    Scalar(ValueView<'a>),
    Attribute(AttributeNameView<'a>),
    Index(DynIndexView<'a>),
    EntityReference(DynEntityRef<'a>),
    Failure(Box<Failure>),
    FailureKind(FailureKind),
}

#[derive(Clone, Debug)]
pub enum DynCachedValue {
    Scalar(Value),
    Attribute(AttributeName),
    Index(DynIndexOwned),
    EntityReference(DynIndexAddress),
    Failure(Box<Failure>),
    FailureKind(FailureKind),
}

macro_rules! implement_arithmetic_capability {
    ($trait:ident, $method:ident) => {
        impl $trait for DynValue {
            fn $method<'a>(
                value: Self::Value<'a>,
                argument: Self::Value<'a>,
                label: &'static str,
            ) -> QueryResult<Self::Value<'a>> {
                match (value, argument) {
                    (DynValueView::Scalar(value), DynValueView::Scalar(argument)) => {
                        <Scalar as $trait>::$method(value, argument, label)
                            .map(DynValueView::Scalar)
                    }
                    (DynValueView::Attribute(value), DynValueView::Attribute(argument)) => {
                        <AttributeName as $trait>::$method(value, argument, label)
                            .map(DynValueView::Attribute)
                    }
                    (DynValueView::Index(value), DynValueView::Index(argument)) => {
                        let result = match (value, argument) {
                            (
                                DynIndexView::Positional(value),
                                DynIndexView::Positional(argument),
                            ) => {
                                <IndexValue<Positional> as $trait>::$method(value, argument, label)
                                    .map(DynIndexView::Positional)
                            }
                            (DynIndexView::Node(value), DynIndexView::Node(argument)) => {
                                <IndexValue<NodeIndex> as $trait>::$method(value, argument, label)
                                    .map(DynIndexView::Node)
                            }
                            (DynIndexView::Attribute(value), DynIndexView::Attribute(argument)) => {
                                <IndexValue<AttributeName> as $trait>::$method(
                                    value,
                                    argument,
                                    label,
                                )
                                .map(DynIndexView::Attribute)
                            }
                            (DynIndexView::Value(value), DynIndexView::Value(argument)) => {
                                <IndexValue<Value> as $trait>::$method(value, argument, label)
                                    .map(DynIndexView::Value)
                            }
                            (value, argument) => {
                                let capability = stringify!($trait);
                                let value_domain = value.description();
                                let argument_domain = argument.description();
                                panic!(
                                    "registry admitted {capability} for incompatible dynamic index domains {value_domain} and {argument_domain}"
                                )
                            }
                        }?;

                        Ok(DynValueView::Index(result))
                    }
                    (value, argument) => {
                        let capability = stringify!($trait);
                        let value_role = value.description();
                        let argument_role = argument.description();
                        panic!(
                            "registry admitted {capability} for incompatible dynamic value roles {value_role} and {argument_role}"
                        )
                    }
                }
            }
        }
    };
}

macro_rules! implement_value_binary_capability {
    ($trait:ident, $method:ident) => {
        impl $trait for DynValue {
            fn $method<'a>(
                value: Self::Value<'a>,
                argument: Self::Value<'a>,
                label: &'static str,
            ) -> QueryResult<Self::Value<'a>> {
                match (value, argument) {
                    (DynValueView::Scalar(value), DynValueView::Scalar(argument)) => {
                        <Scalar as $trait>::$method(value, argument, label)
                            .map(DynValueView::Scalar)
                    }
                    (
                        DynValueView::Index(DynIndexView::Value(value)),
                        DynValueView::Index(DynIndexView::Value(argument)),
                    ) => <IndexValue<Value> as $trait>::$method(value, argument, label)
                        .map(DynIndexView::Value)
                        .map(DynValueView::Index),
                    (value, argument) => {
                        let capability = stringify!($trait);
                        let value_role = value.description();
                        let argument_role = argument.description();
                        panic!(
                            "registry admitted {capability} for incompatible dynamic value roles {value_role} and {argument_role}"
                        )
                    }
                }
            }
        }
    };
}

macro_rules! implement_attribute_numeric_capability {
    ($trait:ident, $method:ident) => {
        impl $trait for DynValue {
            fn $method<'a>(
                value: Self::Value<'a>,
                label: &'static str,
            ) -> QueryResult<Self::Value<'a>> {
                match value {
                    DynValueView::Scalar(value) => {
                        <Scalar as $trait>::$method(value, label).map(DynValueView::Scalar)
                    }
                    DynValueView::Attribute(value) => {
                        <AttributeName as $trait>::$method(value, label)
                            .map(DynValueView::Attribute)
                    }
                    DynValueView::Index(DynIndexView::Node(value)) => {
                        <IndexValue<NodeIndex> as $trait>::$method(value, label)
                            .map(DynIndexView::Node)
                            .map(DynValueView::Index)
                    }
                    DynValueView::Index(DynIndexView::Attribute(value)) => {
                        <IndexValue<AttributeName> as $trait>::$method(value, label)
                            .map(DynIndexView::Attribute)
                            .map(DynValueView::Index)
                    }
                    DynValueView::Index(DynIndexView::Value(value)) => {
                        <IndexValue<Value> as $trait>::$method(value, label)
                            .map(DynIndexView::Value)
                            .map(DynValueView::Index)
                    }
                    value => {
                        let capability = stringify!($trait);
                        let value_role = value.description();
                        panic!("registry admitted {capability} for dynamic value role {value_role}")
                    }
                }
            }
        }
    };
}

macro_rules! implement_value_unary_capability {
    ($trait:ident, $method:ident) => {
        impl $trait for DynValue {
            fn $method<'a>(
                value: Self::Value<'a>,
                label: &'static str,
            ) -> QueryResult<Self::Value<'a>> {
                match value {
                    DynValueView::Scalar(value) => {
                        <Scalar as $trait>::$method(value, label).map(DynValueView::Scalar)
                    }
                    DynValueView::Index(DynIndexView::Value(value)) => {
                        <IndexValue<Value> as $trait>::$method(value, label)
                            .map(DynIndexView::Value)
                            .map(DynValueView::Index)
                    }
                    value => {
                        let capability = stringify!($trait);
                        let value_role = value.description();
                        panic!("registry admitted {capability} for dynamic value role {value_role}")
                    }
                }
            }
        }
    };
}

macro_rules! implement_value_cast {
    ($target:ty) => {
        impl ValueCast<$target> for DynValue {
            fn cast<'a>(
                value: Self::Value<'a>,
                target: &$target,
                label: &'static str,
            ) -> QueryResult<Self::Value<'a>> {
                match value {
                    DynValueView::Scalar(value) => {
                        <Scalar as ValueCast<$target>>::cast(value, target, label)
                            .map(DynValueView::Scalar)
                    }
                    DynValueView::Index(DynIndexView::Value(value)) => {
                        <IndexValue<Value> as ValueCast<$target>>::cast(value, target, label)
                            .map(DynIndexView::Value)
                            .map(DynValueView::Index)
                    }
                    value => {
                        let value_role = value.description();
                        panic!(
                            "registry admitted ValueCast<{target}> for dynamic value role {value_role}"
                        )
                    }
                }
            }
        }
    };
}

macro_rules! implement_value_and_attribute_cast {
    ($target:ty) => {
        impl ValueCast<$target> for DynValue {
            fn cast<'a>(
                value: Self::Value<'a>,
                target: &$target,
                label: &'static str,
            ) -> QueryResult<Self::Value<'a>> {
                match value {
                    DynValueView::Scalar(value) => {
                        <Scalar as ValueCast<$target>>::cast(value, target, label)
                            .map(DynValueView::Scalar)
                    }
                    DynValueView::Attribute(value) => {
                        <AttributeName as ValueCast<$target>>::cast(value, target, label)
                            .map(DynValueView::Attribute)
                    }
                    DynValueView::Index(DynIndexView::Node(value)) => {
                        <IndexValue<NodeIndex> as ValueCast<$target>>::cast(value, target, label)
                            .map(DynIndexView::Node)
                            .map(DynValueView::Index)
                    }
                    DynValueView::Index(DynIndexView::Attribute(value)) => {
                        <IndexValue<AttributeName> as ValueCast<$target>>::cast(
                            value,
                            target,
                            label,
                        )
                        .map(DynIndexView::Attribute)
                        .map(DynValueView::Index)
                    }
                    DynValueView::Index(DynIndexView::Value(value)) => {
                        <IndexValue<Value> as ValueCast<$target>>::cast(value, target, label)
                            .map(DynIndexView::Value)
                            .map(DynValueView::Index)
                    }
                    value => {
                        let value_role = value.description();
                        panic!(
                            "registry admitted ValueCast<{target}> for dynamic value role {value_role}"
                        )
                    }
                }
            }
        }
    };
}

impl DynValue {
    pub(crate) fn descriptor(&self) -> ValueDescriptor {
        match self {
            Self::Scalar(_) => ValueDescriptor::value::<Scalar>(),
            Self::Attribute(_) => ValueDescriptor::value::<AttributeName>(),
            Self::Index(index) => ValueDescriptor::index(index.descriptor()),
            Self::EntityReference(reference) => reference.descriptor(),
            Self::Failure(_) => ValueDescriptor::value::<FailureValue>(),
            Self::FailureKind(_) => ValueDescriptor::value::<FailureKindValue>(),
        }
    }
}

impl DynValueView<'_> {
    pub(crate) const fn description(&self) -> &'static str {
        match self {
            Self::Scalar(_) => "scalar",
            Self::Attribute(_) => "attribute",
            Self::Index(index) => index.description(),
            Self::EntityReference(reference) => reference.description(),
            Self::Failure(_) => "failure",
            Self::FailureKind(_) => "failure kind",
        }
    }

    pub(crate) fn into_groupable(self) -> QueryResult<Self> {
        if matches!(self, Self::Failure(_)) {
            return Err(Failure::new(
                UnsupportedValueRole::new("grouping", self.description()),
                "grouping key",
            ));
        }

        Ok(self)
    }
}

impl PartialEq for DynValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Scalar(first), Self::Scalar(second)) => first == second,
            (Self::Attribute(first), Self::Attribute(second)) => first == second,
            (Self::Index(first), Self::Index(second)) => first == second,
            (Self::EntityReference(first), Self::EntityReference(second)) => first == second,
            (Self::Failure(_), Self::Failure(_)) => {
                panic!("registry admitted equality for a dynamic failure-value lane")
            }
            (Self::FailureKind(first), Self::FailureKind(second)) => first == second,
            _ => false,
        }
    }
}

impl Eq for DynValue {}

impl Hash for DynValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        discriminant(self).hash(state);
        match self {
            Self::Scalar(value) => value.hash(state),
            Self::Attribute(value) => value.hash(state),
            Self::Index(index) => index.hash(state),
            Self::EntityReference(reference) => reference.hash(state),
            Self::Failure(_) => {}
            Self::FailureKind(kind) => kind.hash(state),
        }
    }
}

impl Display for DynValue {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Scalar(value) => value.fmt(formatter),
            Self::Attribute(value) => value.fmt(formatter),
            Self::Index(index) => index.fmt(formatter),
            Self::EntityReference(reference) => reference.fmt(formatter),
            Self::Failure(failure) => failure.fmt(formatter),
            Self::FailureKind(kind) => kind.fmt(formatter),
        }
    }
}

impl PartialEq for DynValueView<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Scalar(first), Self::Scalar(second)) => first == second,
            (Self::Attribute(first), Self::Attribute(second)) => first == second,
            (Self::Index(first), Self::Index(second)) => first == second,
            (Self::EntityReference(first), Self::EntityReference(second)) => first == second,
            (Self::Failure(_), Self::Failure(_)) => {
                panic!("registry admitted equality for a dynamic failure-value lane")
            }
            (Self::FailureKind(first), Self::FailureKind(second)) => first == second,
            _ => false,
        }
    }
}

impl Eq for DynValueView<'_> {}

impl Hash for DynValueView<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        discriminant(self).hash(state);
        match self {
            Self::Scalar(value) => value.hash(state),
            Self::Attribute(value) => value.hash(state),
            Self::Index(index) => index.hash(state),
            Self::EntityReference(reference) => reference.hash(state),
            Self::Failure(_) => {}
            Self::FailureKind(kind) => kind.hash(state),
        }
    }
}

impl PartialOrd for DynValueView<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Scalar(first), Self::Scalar(second)) => first.partial_cmp(second),
            (Self::Attribute(first), Self::Attribute(second)) => first.partial_cmp(second),
            (Self::Index(first), Self::Index(second)) => first.partial_cmp(second),
            _ => None,
        }
    }
}

impl ValueDomain for DynValue {
    type Cached = DynCachedValue;
    type Owned = Self;
    type Value<'a> = DynValueView<'a>;

    fn into_owned(value: Self::Value<'_>) -> Self::Owned {
        match value {
            DynValueView::Scalar(value) => Self::Scalar(Scalar::into_owned(value)),
            DynValueView::Attribute(value) => Self::Attribute(AttributeName::into_owned(value)),
            DynValueView::Index(index) => Self::Index(IndexValue::<DynIndex>::into_owned(index)),
            DynValueView::EntityReference(reference) => {
                Self::EntityReference(reference.into_owned())
            }
            DynValueView::Failure(failure) => Self::Failure(failure),
            DynValueView::FailureKind(kind) => Self::FailureKind(kind),
        }
    }

    fn from_owned<'a>(
        graphrecord: &'a GraphRecord,
        owned: &'a Self::Owned,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        match owned {
            Self::Scalar(value) => {
                Scalar::from_owned(graphrecord, value, label).map(DynValueView::Scalar)
            }
            Self::Attribute(value) => {
                AttributeName::from_owned(graphrecord, value, label).map(DynValueView::Attribute)
            }
            Self::Index(index) => IndexValue::<DynIndex>::from_owned(graphrecord, index, label)
                .map(DynValueView::Index),
            Self::EntityReference(reference) => {
                reference.resolve(graphrecord, label).map(|address| {
                    DynValueView::EntityReference(DynEntityRef::new(graphrecord, address))
                })
            }
            Self::Failure(failure) => Ok(DynValueView::Failure(failure.clone())),
            Self::FailureKind(kind) => Ok(DynValueView::FailureKind(*kind)),
        }
    }

    fn into_cached(value: Self::Value<'_>) -> Self::Cached {
        match value {
            DynValueView::Scalar(value) => DynCachedValue::Scalar(Scalar::into_cached(value)),
            DynValueView::Attribute(value) => {
                DynCachedValue::Attribute(AttributeName::into_cached(value))
            }
            DynValueView::Index(index) => {
                DynCachedValue::Index(IndexValue::<DynIndex>::into_cached(index))
            }
            DynValueView::EntityReference(reference) => {
                DynCachedValue::EntityReference(reference.address)
            }
            DynValueView::Failure(failure) => DynCachedValue::Failure(failure),
            DynValueView::FailureKind(kind) => DynCachedValue::FailureKind(kind),
        }
    }

    fn from_cached<'a>(graphrecord: &'a GraphRecord, cached: &'a Self::Cached) -> Self::Value<'a> {
        match cached {
            DynCachedValue::Scalar(value) => {
                DynValueView::Scalar(Scalar::from_cached(graphrecord, value))
            }
            DynCachedValue::Attribute(value) => {
                DynValueView::Attribute(AttributeName::from_cached(graphrecord, value))
            }
            DynCachedValue::Index(index) => {
                DynValueView::Index(IndexValue::<DynIndex>::from_cached(graphrecord, index))
            }
            DynCachedValue::EntityReference(address) => {
                DynValueView::EntityReference(DynEntityRef::new(graphrecord, address.clone()))
            }
            DynCachedValue::Failure(failure) => DynValueView::Failure(failure.clone()),
            DynCachedValue::FailureKind(kind) => DynValueView::FailureKind(*kind),
        }
    }
}

impl BareValueDomain for DynValue {}

impl ReturnValueDomain for DynValue {}

impl ValueMedian for DynValue {
    fn validate_median(value: &Self::Value<'_>, label: &'static str) -> QueryResult<()> {
        match value {
            DynValueView::Scalar(value) => Scalar::validate_median(value, label),
            DynValueView::Index(DynIndexView::Value(value)) => {
                IndexValue::<Value>::validate_median(value, label)
            }
            value => {
                let value_role = value.description();
                panic!("registry admitted ValueMedian for dynamic value role {value_role}")
            }
        }
    }

    fn find_incomparable_median_values<'a, 'b: 'a>(
        values: impl Iterator<Item = &'a Self::Value<'b>>,
    ) -> Option<(usize, usize)> {
        let values: Vec<_> = values.collect();
        let first = values.first()?;

        match first {
            DynValueView::Scalar(_)
                if values
                    .iter()
                    .all(|value| matches!(value, DynValueView::Scalar(_))) =>
            {
                Scalar::find_incomparable_median_values(values.into_iter().map(|value| {
                    let DynValueView::Scalar(value) = value else {
                        unreachable!("median values were checked as scalar dynamic values")
                    };
                    value
                }))
            }
            DynValueView::Index(DynIndexView::Value(_))
                if values
                    .iter()
                    .all(|value| matches!(value, DynValueView::Index(DynIndexView::Value(_)))) =>
            {
                IndexValue::<Value>::find_incomparable_median_values(values.into_iter().map(
                    |value| {
                        let DynValueView::Index(DynIndexView::Value(value)) = value else {
                            unreachable!("median values were checked as value index dynamic values")
                        };
                        value
                    },
                ))
            }
            _ => panic!("registry admitted ValueMedian for mixed dynamic value roles"),
        }
    }

    fn median<'a>(
        lower: Self::Value<'a>,
        upper: Option<Self::Value<'a>>,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        match (lower, upper) {
            (DynValueView::Scalar(lower), upper) => {
                let upper = match upper {
                    Some(DynValueView::Scalar(upper)) => Some(upper),
                    None => None,
                    Some(upper) => {
                        let upper_role = upper.description();
                        panic!(
                            "registry admitted ValueMedian for incompatible dynamic value roles scalar and {upper_role}"
                        )
                    }
                };

                Scalar::median(lower, upper, label).map(DynValueView::Scalar)
            }
            (DynValueView::Index(DynIndexView::Value(lower)), upper) => {
                let upper = match upper {
                    Some(DynValueView::Index(DynIndexView::Value(upper))) => Some(upper),
                    None => None,
                    Some(upper) => {
                        let upper_role = upper.description();
                        panic!(
                            "registry admitted ValueMedian for incompatible dynamic value roles value index and {upper_role}"
                        )
                    }
                };

                IndexValue::<Value>::median(lower, upper, label)
                    .map(DynIndexView::Value)
                    .map(DynValueView::Index)
            }
            (lower, _) => {
                let lower_role = lower.description();
                panic!("registry admitted ValueMedian for dynamic value role {lower_role}")
            }
        }
    }
}

impl ValueMode for DynValue {}

impl ValueScalar for DynValue {
    fn into_scalar(value: Self::Value<'_>, label: &'static str) -> QueryResult<Value> {
        match value {
            DynValueView::Scalar(value) => Scalar::into_scalar(value, label),
            DynValueView::Index(DynIndexView::Value(value)) => {
                IndexValue::<Value>::into_scalar(value, label)
            }
            value => {
                let value_role = value.description();
                panic!("registry admitted ValueScalar for dynamic value role {value_role}")
            }
        }
    }

    fn from_scalar<'a>(original: &Self::Value<'_>, value: Value) -> Self::Value<'a> {
        match original {
            DynValueView::Scalar(original) => {
                DynValueView::Scalar(Scalar::from_scalar(original, value))
            }
            DynValueView::Index(DynIndexView::Value(original)) => DynValueView::Index(
                DynIndexView::Value(IndexValue::<Value>::from_scalar(original, value)),
            ),
            original => {
                let original = original.description();
                panic!("registry supplied ValueScalar::from_scalar with dynamic role {original}")
            }
        }
    }
}

implement_arithmetic_capability!(ValueAdd, add);
implement_value_binary_capability!(ValueDivide, divide);
implement_arithmetic_capability!(ValueModulo, modulo);
implement_arithmetic_capability!(ValueMultiply, multiply);
implement_arithmetic_capability!(ValuePower, power);
implement_arithmetic_capability!(ValueSubtract, subtract);

impl ValueEquality for DynValue {
    fn equal<'a>(value: &Self::Value<'a>, argument: &Self::Value<'a>) -> bool {
        match (value, argument) {
            (DynValueView::Scalar(value), DynValueView::Scalar(argument)) => {
                Scalar::equal(value, argument)
            }
            (DynValueView::Attribute(value), DynValueView::Attribute(argument)) => {
                AttributeName::equal(value, argument)
            }
            (DynValueView::Index(value), DynValueView::Index(argument)) => {
                IndexValue::<DynIndex>::equal(value, argument)
            }
            (DynValueView::FailureKind(value), DynValueView::FailureKind(argument)) => {
                FailureKindValue::equal(value, argument)
            }
            _ => {
                let value_role = value.description();
                let argument_role = argument.description();
                panic!(
                    "registry admitted ValueEquality for incompatible dynamic value roles {value_role} and {argument_role}"
                )
            }
        }
    }
}

impl ValueOrdering for DynValue {
    fn ordering<'a>(value: &Self::Value<'a>, argument: &Self::Value<'a>) -> Option<Ordering> {
        match (value, argument) {
            (DynValueView::Scalar(value), DynValueView::Scalar(argument)) => {
                Scalar::ordering(value, argument)
            }
            (DynValueView::Attribute(value), DynValueView::Attribute(argument)) => {
                AttributeName::ordering(value, argument)
            }
            (DynValueView::Index(value), DynValueView::Index(argument))
                if value.supports_value_ordering()
                    && argument.supports_value_ordering()
                    && value.has_same_domain(argument) =>
            {
                IndexValue::<DynIndex>::ordering(value, argument)
            }
            _ => {
                let value_role = value.description();
                let argument_role = argument.description();
                panic!(
                    "registry admitted ValueOrdering for incompatible dynamic value roles {value_role} and {argument_role}"
                )
            }
        }
    }
}

implement_value_cast!(BoolTarget);
implement_value_cast!(DateTimeTarget);
implement_value_cast!(DurationTarget);
implement_value_cast!(FloatTarget);
implement_value_and_attribute_cast!(IntTarget);
implement_value_and_attribute_cast!(StringTarget);

impl ValueEquivalence for DynValue {
    type Key<'a> = DynEquivalenceKey<'a>;

    fn equivalence_key<'a>(value: &Self::Value<'a>) -> Self::Key<'a> {
        match value {
            DynValueView::Scalar(value) => {
                DynEquivalenceKey::Scalar(Scalar::equivalence_key(value))
            }
            DynValueView::Attribute(value) => {
                DynEquivalenceKey::Attribute(AttributeName::equivalence_key(value))
            }
            DynValueView::Index(value) => {
                DynEquivalenceKey::Index(IndexValue::<DynIndex>::equivalence_key(value))
            }
            DynValueView::FailureKind(value) => {
                DynEquivalenceKey::FailureKind(FailureKindValue::equivalence_key(value))
            }
            value => {
                let value_role = value.description();
                panic!("registry admitted ValueEquivalence for dynamic value role {value_role}")
            }
        }
    }
}

impl ValueGrouping for DynValue {
    type KeyDomain = DynIndex;

    fn to_group_key(value: &Self::Value<'_>) -> <Self::KeyDomain as IndexDomain>::Owned {
        match value {
            DynValueView::Scalar(value) => DynIndexOwned::Value(Scalar::to_group_key(value)),
            DynValueView::Attribute(value) => {
                DynIndexOwned::Attribute(AttributeName::to_group_key(value))
            }
            DynValueView::Index(value) => IndexValue::<DynIndex>::to_group_key(value),
            DynValueView::EntityReference(reference) => DynIndex::own_index(&reference.index()),
            DynValueView::FailureKind(value) => {
                DynIndexOwned::FailureKind(FailureKindValue::to_group_key(value))
            }
            DynValueView::Failure(_) => {
                panic!("registry admitted ValueGrouping for a dynamic failure-value lane")
            }
        }
    }
}

impl ValueInt for DynValue {
    fn into_int(value: Self::Value<'_>, label: &'static str) -> QueryResult<i64> {
        match value {
            DynValueView::Scalar(value) => Scalar::into_int(value, label),
            DynValueView::Attribute(value) => AttributeName::into_int(value, label),
            DynValueView::Index(DynIndexView::Positional(value)) => {
                IndexValue::<Positional>::into_int(value, label)
            }
            DynValueView::Index(DynIndexView::Node(value)) => {
                IndexValue::<NodeIndex>::into_int(value, label)
            }
            DynValueView::Index(DynIndexView::Attribute(value)) => {
                IndexValue::<AttributeName>::into_int(value, label)
            }
            DynValueView::Index(DynIndexView::Value(value)) => {
                IndexValue::<Value>::into_int(value, label)
            }
            value => {
                let value_role = value.description();
                panic!("registry admitted ValueInt for dynamic value role {value_role}")
            }
        }
    }
}

impl ValueKindTest for DynValue {
    fn kind(value: &Self::Value<'_>) -> PayloadKind {
        match value {
            DynValueView::Scalar(value) => Scalar::kind(value),
            DynValueView::Attribute(value) => AttributeName::kind(value),
            DynValueView::Index(DynIndexView::Node(value)) => IndexValue::<NodeIndex>::kind(value),
            DynValueView::Index(DynIndexView::Attribute(value)) => {
                IndexValue::<AttributeName>::kind(value)
            }
            DynValueView::Index(DynIndexView::Value(value)) => IndexValue::<Value>::kind(value),
            value => {
                let value_role = value.description();
                panic!("registry admitted ValueKindTest for dynamic value role {value_role}")
            }
        }
    }
}

impl ValueScalarKindTest for DynValue {}

implement_attribute_numeric_capability!(ValueAbsolute, absolute);
implement_value_unary_capability!(ValueCeil, ceil);

impl ValueClip for DynValue {
    fn clip<'a>(
        value: Self::Value<'a>,
        lower: Self::Value<'a>,
        upper: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        match (value, lower, upper) {
            (
                DynValueView::Scalar(value),
                DynValueView::Scalar(lower),
                DynValueView::Scalar(upper),
            ) => Scalar::clip(value, lower, upper, label).map(DynValueView::Scalar),
            (
                DynValueView::Attribute(value),
                DynValueView::Attribute(lower),
                DynValueView::Attribute(upper),
            ) => AttributeName::clip(value, lower, upper, label).map(DynValueView::Attribute),
            (
                DynValueView::Index(DynIndexView::Positional(value)),
                DynValueView::Index(DynIndexView::Positional(lower)),
                DynValueView::Index(DynIndexView::Positional(upper)),
            ) => IndexValue::<Positional>::clip(value, lower, upper, label)
                .map(DynIndexView::Positional)
                .map(DynValueView::Index),
            (
                DynValueView::Index(DynIndexView::Node(value)),
                DynValueView::Index(DynIndexView::Node(lower)),
                DynValueView::Index(DynIndexView::Node(upper)),
            ) => IndexValue::<NodeIndex>::clip(value, lower, upper, label)
                .map(DynIndexView::Node)
                .map(DynValueView::Index),
            (
                DynValueView::Index(DynIndexView::Attribute(value)),
                DynValueView::Index(DynIndexView::Attribute(lower)),
                DynValueView::Index(DynIndexView::Attribute(upper)),
            ) => IndexValue::<AttributeName>::clip(value, lower, upper, label)
                .map(DynIndexView::Attribute)
                .map(DynValueView::Index),
            (
                DynValueView::Index(DynIndexView::Value(value)),
                DynValueView::Index(DynIndexView::Value(lower)),
                DynValueView::Index(DynIndexView::Value(upper)),
            ) => IndexValue::<Value>::clip(value, lower, upper, label)
                .map(DynIndexView::Value)
                .map(DynValueView::Index),
            (value, lower, upper) => {
                let value_role = value.description();
                let lower_role = lower.description();
                let upper_role = upper.description();
                panic!(
                    "registry admitted ValueClip for incompatible dynamic value roles {value_role}, {lower_role}, and {upper_role}"
                )
            }
        }
    }
}

implement_value_unary_capability!(ValueCubeRoot, cube_root);
implement_value_unary_capability!(ValueExponential, exponential);
implement_value_unary_capability!(ValueFloor, floor);
implement_value_unary_capability!(ValueLogarithm, logarithm);
implement_attribute_numeric_capability!(ValueNegate, negate);
implement_value_unary_capability!(ValueRound, round);
implement_attribute_numeric_capability!(ValueSign, sign);
implement_value_unary_capability!(ValueSquareRoot, square_root);

impl EnsureSortable for DynValueView<'_> {
    fn find_incomparable<'a>(values: impl Iterator<Item = &'a Self>) -> Option<(usize, usize)>
    where
        Self: 'a,
    {
        let values: Vec<_> = values.collect();
        let first = values.first()?;

        match first {
            Self::Scalar(_) if values.iter().all(|value| matches!(value, Self::Scalar(_))) => {
                ValueView::find_incomparable(values.into_iter().map(|value| {
                    let Self::Scalar(value) = value else {
                        unreachable!("dynamic values were checked as scalars")
                    };
                    value
                }))
            }
            Self::Attribute(_)
                if values
                    .iter()
                    .all(|value| matches!(value, Self::Attribute(_))) =>
            {
                AttributeNameView::find_incomparable(values.into_iter().map(|value| {
                    let Self::Attribute(value) = value else {
                        unreachable!("dynamic values were checked as attributes")
                    };
                    value
                }))
            }
            Self::Index(_) if values.iter().all(|value| matches!(value, Self::Index(_))) => {
                DynIndexView::find_incomparable(values.into_iter().map(|value| {
                    let Self::Index(value) = value else {
                        unreachable!("dynamic values were checked as indices")
                    };
                    value
                }))
            }
            Self::EntityReference(_) | Self::Failure(_) | Self::FailureKind(_) => {
                panic!("registry admitted EnsureSortable for an unsortable dynamic value role")
            }
            _ => panic!("registry admitted EnsureSortable for mixed dynamic value roles"),
        }
    }
}

impl ValueString for DynValue {
    fn as_str<'a>(value: &'a Self::Value<'_>, label: &'static str) -> QueryResult<&'a str> {
        match value {
            DynValueView::Scalar(value) => Scalar::as_str(value, label),
            DynValueView::Attribute(value) => AttributeName::as_str(value, label),
            DynValueView::Index(DynIndexView::Node(value)) => {
                IndexValue::<NodeIndex>::as_str(value, label)
            }
            DynValueView::Index(DynIndexView::Group(value)) => {
                IndexValue::<GroupIndex>::as_str(value, label)
            }
            DynValueView::Index(DynIndexView::Attribute(value)) => {
                IndexValue::<AttributeName>::as_str(value, label)
            }
            DynValueView::Index(DynIndexView::Value(value)) => {
                IndexValue::<Value>::as_str(value, label)
            }
            value => {
                let value_role = value.description();
                panic!("registry admitted ValueString for dynamic value role {value_role}")
            }
        }
    }

    fn with_string<'a>(original: &Self::Value<'_>, string: String) -> Self::Value<'a> {
        match original {
            DynValueView::Scalar(original) => {
                DynValueView::Scalar(Scalar::with_string(original, string))
            }
            DynValueView::Attribute(original) => {
                DynValueView::Attribute(AttributeName::with_string(original, string))
            }
            DynValueView::Index(DynIndexView::Node(original)) => DynValueView::Index(
                DynIndexView::Node(IndexValue::<NodeIndex>::with_string(original, string)),
            ),
            DynValueView::Index(DynIndexView::Group(original)) => DynValueView::Index(
                DynIndexView::Group(IndexValue::<GroupIndex>::with_string(original, string)),
            ),
            DynValueView::Index(DynIndexView::Attribute(original)) => DynValueView::Index(
                DynIndexView::Attribute(IndexValue::<AttributeName>::with_string(original, string)),
            ),
            DynValueView::Index(DynIndexView::Value(original)) => DynValueView::Index(
                DynIndexView::Value(IndexValue::<Value>::with_string(original, string)),
            ),
            original => {
                let original = original.description();
                panic!("registry supplied ValueString::with_string with dynamic role {original}")
            }
        }
    }
}

impl ValueTransition<DynValue> for Mask {
    fn transition<'a>(
        value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<DynValue as ValueDomain>::Value<'a>> {
        Ok(DynValueView::Index(DynIndexView::Bool(value)))
    }
}

const ATTRIBUTE_NAME_TARGET: &str = "AttributeName";
const ATTRIBUTE_NAME_INDEX_TARGET: &str = "IndexValue<AttributeName>";
const BOOL_INDEX_TARGET: &str = "IndexValue<bool>";
const FAILURE_KIND_INDEX_TARGET: &str = "IndexValue<FailureKind>";
const FAILURE_KIND_VALUE_TARGET: &str = "FailureKindValue";
const GROUP_TARGET: &str = "IndexValue<GroupIndex>";
const MASK_TARGET: &str = "Mask";
const NODE_INDEX_TARGET: &str = "IndexValue<NodeIndex>";
const POSITIONAL_INDEX_TARGET: &str = "IndexValue<Positional>";
const SCALAR_TARGET: &str = "Scalar";
const VALUE_INDEX_TARGET: &str = "IndexValue<Value>";

impl ValueTransition<AttributeName> for DynValue {
    fn transition<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<<AttributeName as ValueDomain>::Value<'a>> {
        match value {
            DynValueView::Scalar(value) => {
                <Scalar as ValueTransition<AttributeName>>::transition(value, label)
            }
            DynValueView::Index(DynIndexView::Value(value)) => {
                <IndexValue<Value> as ValueTransition<AttributeName>>::transition(value, label)
            }
            DynValueView::Index(DynIndexView::Node(value)) => {
                <IndexValue<NodeIndex> as ValueTransition<AttributeName>>::transition(value, label)
            }
            DynValueView::Index(DynIndexView::Group(value)) => {
                <IndexValue<GroupIndex> as ValueTransition<AttributeName>>::transition(value, label)
            }
            DynValueView::Index(DynIndexView::Attribute(value)) => {
                <IndexValue<AttributeName> as ValueTransition<AttributeName>>::transition(
                    value, label,
                )
            }
            DynValueView::Index(DynIndexView::Positional(value)) => {
                <IndexValue<Positional> as ValueTransition<AttributeName>>::transition(value, label)
            }
            value => {
                let value_role = value.description();
                Err(Failure::new(
                    InvalidTransition::new(value_role, ATTRIBUTE_NAME_TARGET),
                    label,
                ))
            }
        }
    }
}

impl ValueTransition<FailureKindValue> for DynValue {
    fn transition<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<<FailureKindValue as ValueDomain>::Value<'a>> {
        match value {
            DynValueView::Index(DynIndexView::FailureKind(value)) => {
                <IndexValue<FailureKind> as ValueTransition<FailureKindValue>>::transition(
                    value, label,
                )
            }
            value => {
                let value_role = value.description();
                Err(Failure::new(
                    InvalidTransition::new(value_role, FAILURE_KIND_VALUE_TARGET),
                    label,
                ))
            }
        }
    }
}

impl ValueTransition<IndexValue<AttributeName>> for DynValue {
    fn transition<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<<IndexValue<AttributeName> as ValueDomain>::Value<'a>> {
        match value {
            DynValueView::Scalar(value) => {
                <Scalar as ValueTransition<IndexValue<AttributeName>>>::transition(value, label)
            }
            DynValueView::Index(DynIndexView::Value(value)) => {
                <IndexValue<Value> as ValueTransition<IndexValue<AttributeName>>>::transition(
                    value, label,
                )
            }
            DynValueView::Attribute(value) => <AttributeName as ValueTransition<
                IndexValue<AttributeName>,
            >>::transition(value, label),
            DynValueView::Index(DynIndexView::Node(value)) => {
                <IndexValue<NodeIndex> as ValueTransition<IndexValue<AttributeName>>>::transition(
                    value, label,
                )
            }
            DynValueView::Index(DynIndexView::Group(value)) => {
                <IndexValue<GroupIndex> as ValueTransition<IndexValue<AttributeName>>>::transition(
                    value, label,
                )
            }
            DynValueView::Index(DynIndexView::Positional(value)) => {
                <IndexValue<Positional> as ValueTransition<IndexValue<AttributeName>>>::transition(
                    value, label,
                )
            }
            value => {
                let value_role = value.description();
                Err(Failure::new(
                    InvalidTransition::new(value_role, ATTRIBUTE_NAME_INDEX_TARGET),
                    label,
                ))
            }
        }
    }
}

impl ValueTransition<IndexValue<bool>> for DynValue {
    fn transition<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<<IndexValue<bool> as ValueDomain>::Value<'a>> {
        match value {
            DynValueView::Scalar(value) => {
                <Scalar as ValueTransition<IndexValue<bool>>>::transition(value, label)
            }
            DynValueView::Index(DynIndexView::Value(value)) => {
                <IndexValue<Value> as ValueTransition<IndexValue<bool>>>::transition(value, label)
            }
            value => {
                let value_role = value.description();
                Err(Failure::new(
                    InvalidTransition::new(value_role, BOOL_INDEX_TARGET),
                    label,
                ))
            }
        }
    }
}

impl ValueTransition<IndexValue<FailureKind>> for DynValue {
    fn transition<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<<IndexValue<FailureKind> as ValueDomain>::Value<'a>> {
        match value {
            DynValueView::FailureKind(value) => <FailureKindValue as ValueTransition<
                IndexValue<FailureKind>,
            >>::transition(value, label),
            value => {
                let value_role = value.description();
                Err(Failure::new(
                    InvalidTransition::new(value_role, FAILURE_KIND_INDEX_TARGET),
                    label,
                ))
            }
        }
    }
}

impl ValueTransition<IndexValue<GroupIndex>> for DynValue {
    fn transition<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<<IndexValue<GroupIndex> as ValueDomain>::Value<'a>> {
        match value {
            DynValueView::Scalar(value) => {
                <Scalar as ValueTransition<IndexValue<GroupIndex>>>::transition(value, label)
            }
            DynValueView::Index(DynIndexView::Value(value)) => {
                <IndexValue<Value> as ValueTransition<IndexValue<GroupIndex>>>::transition(
                    value, label,
                )
            }
            DynValueView::Attribute(value) => {
                <AttributeName as ValueTransition<IndexValue<GroupIndex>>>::transition(value, label)
            }
            DynValueView::Index(DynIndexView::Attribute(value)) => {
                <IndexValue<AttributeName> as ValueTransition<IndexValue<GroupIndex>>>::transition(
                    value, label,
                )
            }
            DynValueView::Index(DynIndexView::Positional(value)) => {
                <IndexValue<Positional> as ValueTransition<IndexValue<GroupIndex>>>::transition(
                    value, label,
                )
            }
            value => {
                let value_role = value.description();
                Err(Failure::new(
                    InvalidTransition::new(value_role, GROUP_TARGET),
                    label,
                ))
            }
        }
    }
}

impl ValueTransition<IndexValue<NodeIndex>> for DynValue {
    fn transition<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<<IndexValue<NodeIndex> as ValueDomain>::Value<'a>> {
        match value {
            DynValueView::Scalar(value) => {
                <Scalar as ValueTransition<IndexValue<NodeIndex>>>::transition(value, label)
            }
            DynValueView::Index(DynIndexView::Value(value)) => {
                <IndexValue<Value> as ValueTransition<IndexValue<NodeIndex>>>::transition(
                    value, label,
                )
            }
            DynValueView::Attribute(value) => {
                <AttributeName as ValueTransition<IndexValue<NodeIndex>>>::transition(value, label)
            }
            DynValueView::Index(DynIndexView::Attribute(value)) => {
                <IndexValue<AttributeName> as ValueTransition<IndexValue<NodeIndex>>>::transition(
                    value, label,
                )
            }
            DynValueView::Index(DynIndexView::Positional(value)) => {
                <IndexValue<Positional> as ValueTransition<IndexValue<NodeIndex>>>::transition(
                    value, label,
                )
            }
            value => {
                let value_role = value.description();
                Err(Failure::new(
                    InvalidTransition::new(value_role, NODE_INDEX_TARGET),
                    label,
                ))
            }
        }
    }
}

impl ValueTransition<IndexValue<Positional>> for DynValue {
    fn transition<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<<IndexValue<Positional> as ValueDomain>::Value<'a>> {
        match value {
            DynValueView::Scalar(value) => {
                <Scalar as ValueTransition<IndexValue<Positional>>>::transition(value, label)
            }
            DynValueView::Index(DynIndexView::Value(value)) => {
                <IndexValue<Value> as ValueTransition<IndexValue<Positional>>>::transition(
                    value, label,
                )
            }
            DynValueView::Attribute(value) => {
                <AttributeName as ValueTransition<IndexValue<Positional>>>::transition(value, label)
            }
            DynValueView::Index(DynIndexView::Node(value)) => {
                <IndexValue<NodeIndex> as ValueTransition<IndexValue<Positional>>>::transition(
                    value, label,
                )
            }
            DynValueView::Index(DynIndexView::Group(value)) => {
                <IndexValue<GroupIndex> as ValueTransition<IndexValue<Positional>>>::transition(
                    value, label,
                )
            }
            DynValueView::Index(DynIndexView::Attribute(value)) => {
                <IndexValue<AttributeName> as ValueTransition<IndexValue<Positional>>>::transition(
                    value, label,
                )
            }
            value => {
                let value_role = value.description();
                Err(Failure::new(
                    InvalidTransition::new(value_role, POSITIONAL_INDEX_TARGET),
                    label,
                ))
            }
        }
    }
}

impl ValueTransition<IndexValue<Value>> for DynValue {
    fn transition<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<<IndexValue<Value> as ValueDomain>::Value<'a>> {
        match value {
            DynValueView::Scalar(value) => {
                <Scalar as ValueTransition<IndexValue<Value>>>::transition(value, label)
            }
            DynValueView::Attribute(value) => {
                <AttributeName as ValueTransition<IndexValue<Value>>>::transition(value, label)
            }
            DynValueView::Index(DynIndexView::Node(value)) => {
                <IndexValue<NodeIndex> as ValueTransition<IndexValue<Value>>>::transition(
                    value, label,
                )
            }
            DynValueView::Index(DynIndexView::Group(value)) => {
                <IndexValue<GroupIndex> as ValueTransition<IndexValue<Value>>>::transition(
                    value, label,
                )
            }
            DynValueView::Index(DynIndexView::Attribute(value)) => {
                <IndexValue<AttributeName> as ValueTransition<IndexValue<Value>>>::transition(
                    value, label,
                )
            }
            DynValueView::Index(DynIndexView::Bool(value)) => {
                <IndexValue<bool> as ValueTransition<IndexValue<Value>>>::transition(value, label)
            }
            DynValueView::Index(DynIndexView::Positional(value)) => {
                <IndexValue<Positional> as ValueTransition<IndexValue<Value>>>::transition(
                    value, label,
                )
            }
            value => {
                let value_role = value.description();
                Err(Failure::new(
                    InvalidTransition::new(value_role, VALUE_INDEX_TARGET),
                    label,
                ))
            }
        }
    }
}

impl ValueTransition<Mask> for DynValue {
    fn transition<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<<Mask as ValueDomain>::Value<'a>> {
        match value {
            DynValueView::Scalar(value) => {
                <Scalar as ValueTransition<Mask>>::transition(value, label)
            }
            DynValueView::Index(DynIndexView::Value(value)) => {
                <IndexValue<Value> as ValueTransition<Mask>>::transition(value, label)
            }
            DynValueView::Index(DynIndexView::Bool(value)) => {
                <IndexValue<bool> as ValueTransition<Mask>>::transition(value, label)
            }
            value => {
                let value_role = value.description();
                Err(Failure::new(
                    InvalidTransition::new(value_role, MASK_TARGET),
                    label,
                ))
            }
        }
    }
}

impl ValueTransition<Scalar> for DynValue {
    fn transition<'a>(
        value: Self::Value<'a>,
        label: &'static str,
    ) -> QueryResult<<Scalar as ValueDomain>::Value<'a>> {
        match value {
            DynValueView::Index(DynIndexView::Value(value)) => {
                <IndexValue<Value> as ValueTransition<Scalar>>::transition(value, label)
            }
            DynValueView::Attribute(value) => {
                <AttributeName as ValueTransition<Scalar>>::transition(value, label)
            }
            DynValueView::Index(DynIndexView::Node(value)) => {
                <IndexValue<NodeIndex> as ValueTransition<Scalar>>::transition(value, label)
            }
            DynValueView::Index(DynIndexView::Group(value)) => {
                <IndexValue<GroupIndex> as ValueTransition<Scalar>>::transition(value, label)
            }
            DynValueView::Index(DynIndexView::Attribute(value)) => {
                <IndexValue<AttributeName> as ValueTransition<Scalar>>::transition(value, label)
            }
            DynValueView::Index(DynIndexView::Bool(value)) => {
                <IndexValue<bool> as ValueTransition<Scalar>>::transition(value, label)
            }
            DynValueView::Index(DynIndexView::Positional(value)) => {
                <IndexValue<Positional> as ValueTransition<Scalar>>::transition(value, label)
            }
            value => {
                let value_role = value.description();
                Err(Failure::new(
                    InvalidTransition::new(value_role, SCALAR_TARGET),
                    label,
                ))
            }
        }
    }
}
