use super::{DynIndex, DynIndexOwned};
use crate::{
    BareValueDomain, Failure, FailureKind, FailureKindValue, FailureValue, IndexDomain, IndexValue,
    Mask, Positional, QueryResult, ReturnValueDomain, Scalar, ValueDomain,
    capabilities::{
        EnsureSortable, GroupingValue, IntValue, PayloadKind, StringValue, ValueAbsolute, ValueAdd,
        ValueCast, ValueCeil, ValueClip, ValueCubeRoot, ValueDivide, ValueEquality,
        ValueEquivalence, ValueExponential, ValueFloor, ValueKindTest, ValueLogarithm, ValueMedian,
        ValueMode, ValueModulo, ValueMultiply, ValueNegate, ValueOrdering, ValuePower, ValueRound,
        ValueScalar, ValueScalarKindTest, ValueSign, ValueSquareRoot, ValueSubtract,
        ValueTransition,
    },
    cast::{
        Bool as BoolTarget, DateTime as DateTimeTarget, Duration as DurationTarget,
        Float as FloatTarget, Int as IntTarget, String as StringTarget,
    },
    error::dispatch::UnsupportedValueRole,
    registry::ValueDescriptor,
};
use graphrecords_core::graphrecord::{AttributeName, EdgeIndex, NodeIndex, Value};
use std::{
    cmp::Ordering,
    fmt::{self, Display, Formatter},
    hash::{Hash, Hasher},
    mem::discriminant,
};

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum DynEntityReferenceKind {
    Node(NodeIndex),
    Edge(EdgeIndex),
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
    pub const fn node_index(&self) -> Option<&NodeIndex> {
        match &self.kind {
            DynEntityReferenceKind::Node(index) => Some(index),
            DynEntityReferenceKind::Edge(_) => None,
        }
    }

    #[must_use]
    pub const fn edge_index(&self) -> Option<&EdgeIndex> {
        match &self.kind {
            DynEntityReferenceKind::Node(_) => None,
            DynEntityReferenceKind::Edge(index) => Some(index),
        }
    }

    fn descriptor(&self) -> ValueDescriptor {
        match self.kind {
            DynEntityReferenceKind::Node(_) => ValueDescriptor::entity_reference::<NodeIndex>(),
            DynEntityReferenceKind::Edge(_) => ValueDescriptor::entity_reference::<EdgeIndex>(),
        }
    }

    const fn description(&self) -> &'static str {
        match self.kind {
            DynEntityReferenceKind::Node(_) => "node entity reference",
            DynEntityReferenceKind::Edge(_) => "edge entity reference",
        }
    }
}

impl Display for DynEntityReference {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match &self.kind {
            DynEntityReferenceKind::Node(node) => node.fmt(formatter),
            DynEntityReferenceKind::Edge(edge) => edge.fmt(formatter),
        }
    }
}

impl PartialOrd for DynEntityReference {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (&self.kind, &other.kind) {
            (DynEntityReferenceKind::Node(first), DynEntityReferenceKind::Node(second)) => {
                first.partial_cmp(second)
            }
            (DynEntityReferenceKind::Edge(first), DynEntityReferenceKind::Edge(second)) => {
                first.partial_cmp(second)
            }
            _ => None,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum DynEquivalenceKey {
    Scalar(Value),
    Attribute(AttributeName),
    Index(DynIndexOwned),
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

macro_rules! implement_arithmetic_capability {
    ($trait:ident, $method:ident) => {
        impl $trait for DynValue {
            fn $method<'a>(
                label: &'static str,
                value: Self::Value<'a>,
                argument: Self::Value<'a>,
            ) -> QueryResult<Self::Value<'a>> {
                match (value, argument) {
                    (Self::Scalar(value), Self::Scalar(argument)) => {
                        <Scalar as $trait>::$method(label, value, argument).map(Self::Scalar)
                    }
                    (Self::Attribute(value), Self::Attribute(argument)) => {
                        <AttributeName as $trait>::$method(label, value, argument)
                            .map(Self::Attribute)
                    }
                    (Self::Index(value), Self::Index(argument)) => {
                        let result = match (value, argument) {
                            (
                                DynIndexOwned::Positional(value),
                                DynIndexOwned::Positional(argument),
                            ) => {
                                <IndexValue<Positional> as $trait>::$method(label, value, argument)
                                    .map(DynIndexOwned::Positional)
                            }
                            (DynIndexOwned::Node(value), DynIndexOwned::Node(argument)) => {
                                <IndexValue<NodeIndex> as $trait>::$method(label, value, argument)
                                    .map(DynIndexOwned::Node)
                            }
                            (DynIndexOwned::Edge(value), DynIndexOwned::Edge(argument)) => {
                                <IndexValue<EdgeIndex> as $trait>::$method(label, value, argument)
                                    .map(DynIndexOwned::Edge)
                            }
                            (
                                DynIndexOwned::Attribute(value),
                                DynIndexOwned::Attribute(argument),
                            ) => <IndexValue<AttributeName> as $trait>::$method(
                                label, value, argument,
                            )
                            .map(DynIndexOwned::Attribute),
                            (DynIndexOwned::Value(value), DynIndexOwned::Value(argument)) => {
                                <IndexValue<Value> as $trait>::$method(
                                    label, value, argument,
                                )
                                .map(DynIndexOwned::Value)
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

                        Ok(Self::Index(result))
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
                label: &'static str,
                value: Self::Value<'a>,
                argument: Self::Value<'a>,
            ) -> QueryResult<Self::Value<'a>> {
                match (value, argument) {
                    (Self::Scalar(value), Self::Scalar(argument)) => {
                        <Scalar as $trait>::$method(label, value, argument).map(Self::Scalar)
                    }
                    (
                        Self::Index(DynIndexOwned::Value(value)),
                        Self::Index(DynIndexOwned::Value(argument)),
                    ) => <IndexValue<Value> as $trait>::$method(label, value, argument)
                        .map(DynIndexOwned::Value)
                        .map(Self::Index),
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
                label: &'static str,
                value: Self::Value<'a>,
            ) -> QueryResult<Self::Value<'a>> {
                match value {
                    Self::Scalar(value) => {
                        <Scalar as $trait>::$method(label, value).map(Self::Scalar)
                    }
                    Self::Attribute(value) => {
                        <AttributeName as $trait>::$method(label, value).map(Self::Attribute)
                    }
                    Self::Index(DynIndexOwned::Node(value)) => {
                        <IndexValue<NodeIndex> as $trait>::$method(label, value)
                            .map(DynIndexOwned::Node)
                            .map(Self::Index)
                    }
                    Self::Index(DynIndexOwned::Attribute(value)) => {
                        <IndexValue<AttributeName> as $trait>::$method(label, value)
                            .map(DynIndexOwned::Attribute)
                            .map(Self::Index)
                    }
                    Self::Index(DynIndexOwned::Value(value)) => {
                        <IndexValue<Value> as $trait>::$method(label, value)
                            .map(DynIndexOwned::Value)
                            .map(Self::Index)
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
                label: &'static str,
                value: Self::Value<'a>,
            ) -> QueryResult<Self::Value<'a>> {
                match value {
                    Self::Scalar(value) => {
                        <Scalar as $trait>::$method(label, value).map(Self::Scalar)
                    }
                    Self::Index(DynIndexOwned::Value(value)) => {
                        <IndexValue<Value> as $trait>::$method(label, value)
                            .map(DynIndexOwned::Value)
                            .map(Self::Index)
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
                label: &'static str,
                value: Self::Value<'a>,
                target: &$target,
            ) -> QueryResult<Self::Value<'a>> {
                match value {
                    Self::Scalar(value) => {
                        <Scalar as ValueCast<$target>>::cast(label, value, target).map(Self::Scalar)
                    }
                    Self::Index(DynIndexOwned::Value(value)) => {
                        <IndexValue<Value> as ValueCast<$target>>::cast(
                            label, value, target,
                        )
                        .map(DynIndexOwned::Value)
                        .map(Self::Index)
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
                label: &'static str,
                value: Self::Value<'a>,
                target: &$target,
            ) -> QueryResult<Self::Value<'a>> {
                match value {
                    Self::Scalar(value) => {
                        <Scalar as ValueCast<$target>>::cast(label, value, target).map(Self::Scalar)
                    }
                    Self::Attribute(value) => {
                        <AttributeName as ValueCast<$target>>::cast(label, value, target)
                            .map(Self::Attribute)
                    }
                    Self::Index(DynIndexOwned::Node(value)) => {
                        <IndexValue<NodeIndex> as ValueCast<$target>>::cast(label, value, target)
                            .map(DynIndexOwned::Node)
                            .map(Self::Index)
                    }
                    Self::Index(DynIndexOwned::Attribute(value)) => {
                        <IndexValue<AttributeName> as ValueCast<$target>>::cast(
                            label, value, target,
                        )
                        .map(DynIndexOwned::Attribute)
                        .map(Self::Index)
                    }
                    Self::Index(DynIndexOwned::Value(value)) => {
                        <IndexValue<Value> as ValueCast<$target>>::cast(
                            label, value, target,
                        )
                        .map(DynIndexOwned::Value)
                        .map(Self::Index)
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
                "grouping key",
                UnsupportedValueRole::new("grouping", self.description()),
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

impl PartialOrd for DynValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Scalar(first), Self::Scalar(second)) => first.partial_cmp(second),
            (Self::Attribute(first), Self::Attribute(second)) => first.partial_cmp(second),
            (Self::Index(first), Self::Index(second)) => first.partial_cmp(second),
            (Self::EntityReference(first), Self::EntityReference(second)) => {
                first.partial_cmp(second)
            }
            _ => None,
        }
    }
}

impl ValueDomain for DynValue {
    type Owned = Self;
    type Value<'a> = Self;

    fn into_owned(value: Self::Value<'_>) -> Self::Owned {
        value
    }

    fn from_owned(owned: &Self::Owned) -> Self::Value<'_> {
        owned.clone()
    }
}

impl BareValueDomain for DynValue {}

impl ReturnValueDomain for DynValue {}

impl ValueMedian for DynValue {
    fn validate_median(label: &'static str, value: &Self::Value<'_>) -> QueryResult<()> {
        match value {
            Self::Scalar(value) => Scalar::validate_median(label, value),
            Self::Index(DynIndexOwned::Value(value)) => {
                IndexValue::<Value>::validate_median(label, value)
            }
            value => {
                let value_role = value.description();
                panic!("registry admitted ValueMedian for dynamic value role {value_role}")
            }
        }
    }

    fn find_incomparable_median_values<'a, 'b>(
        values: impl Iterator<Item = &'a Self::Value<'b>>,
    ) -> Option<(usize, usize)>
    where
        Self::Value<'b>: 'a,
    {
        let values: Vec<_> = values.collect();
        let first = values.first()?;

        match first {
            Self::Scalar(_) if values.iter().all(|value| matches!(value, Self::Scalar(_))) => {
                Scalar::find_incomparable_median_values(values.into_iter().map(|value| {
                    let Self::Scalar(value) = value else {
                        unreachable!("median values were checked as scalar dynamic values")
                    };
                    value
                }))
            }
            Self::Index(DynIndexOwned::Value(_))
                if values.iter().all(|value| {
                    matches!(value, Self::Index(DynIndexOwned::Value(_)))
                }) => IndexValue::<Value>::find_incomparable_median_values(
                values.into_iter().map(|value| {
                    let Self::Index(DynIndexOwned::Value(value)) = value else {
                        unreachable!(
                            "median values were checked as graphrecord-value index dynamic values"
                        )
                    };
                    value
                }),
            ),
            _ => panic!("registry admitted ValueMedian for mixed dynamic value roles"),
        }
    }

    fn median<'a>(
        label: &'static str,
        lower: Self::Value<'a>,
        upper: Option<Self::Value<'a>>,
    ) -> QueryResult<Self::Value<'a>> {
        match (lower, upper) {
            (Self::Scalar(lower), upper) => {
                let upper = match upper {
                    Some(Self::Scalar(upper)) => Some(upper),
                    None => None,
                    Some(upper) => {
                        let upper_role = upper.description();
                        panic!(
                            "registry admitted ValueMedian for incompatible dynamic value roles scalar and {upper_role}"
                        )
                    }
                };

                Scalar::median(label, lower, upper).map(Self::Scalar)
            }
            (Self::Index(DynIndexOwned::Value(lower)), upper) => {
                let upper = match upper {
                    Some(Self::Index(DynIndexOwned::Value(upper))) => Some(upper),
                    None => None,
                    Some(upper) => {
                        let upper_role = upper.description();
                        panic!(
                            "registry admitted ValueMedian for incompatible dynamic value roles graphrecord-value index and {upper_role}"
                        )
                    }
                };

                IndexValue::<Value>::median(label, lower, upper)
                    .map(DynIndexOwned::Value)
                    .map(Self::Index)
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
    fn into_scalar(label: &'static str, value: Self::Value<'_>) -> QueryResult<Value> {
        match value {
            Self::Scalar(value) => Scalar::into_scalar(label, value),
            Self::Index(DynIndexOwned::Value(value)) => {
                IndexValue::<Value>::into_scalar(label, value)
            }
            value => {
                let value_role = value.description();
                panic!("registry admitted ValueScalar for dynamic value role {value_role}")
            }
        }
    }

    fn from_scalar<'a>(role: &Self::Value<'_>, value: Value) -> Self::Value<'a> {
        match role {
            Self::Scalar(role) => Self::Scalar(Scalar::from_scalar(role, value)),
            Self::Index(DynIndexOwned::Value(role)) => Self::Index(DynIndexOwned::Value(
                IndexValue::<Value>::from_scalar(role, value),
            )),
            role => {
                let role = role.description();
                panic!("registry supplied ValueScalar::from_scalar with dynamic role {role}")
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
            (Self::Scalar(value), Self::Scalar(argument)) => Scalar::equal(value, argument),
            (Self::Attribute(value), Self::Attribute(argument)) => {
                AttributeName::equal(value, argument)
            }
            (Self::Index(value), Self::Index(argument)) => {
                IndexValue::<DynIndex>::equal(value, argument)
            }
            (Self::FailureKind(value), Self::FailureKind(argument)) => {
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
            (Self::Scalar(value), Self::Scalar(argument)) => Scalar::ordering(value, argument),
            (Self::Attribute(value), Self::Attribute(argument)) => {
                AttributeName::ordering(value, argument)
            }
            (Self::Index(value), Self::Index(argument))
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
    type Key = DynEquivalenceKey;

    fn equivalence_key(value: &Self::Value<'_>) -> Self::Key {
        match value {
            Self::Scalar(value) => Self::Key::Scalar(Scalar::equivalence_key(value)),
            Self::Attribute(value) => Self::Key::Attribute(AttributeName::equivalence_key(value)),
            Self::Index(value) => Self::Key::Index(IndexValue::<DynIndex>::equivalence_key(value)),
            Self::FailureKind(value) => {
                Self::Key::FailureKind(FailureKindValue::equivalence_key(value))
            }
            value => {
                let value_role = value.description();
                panic!("registry admitted ValueEquivalence for dynamic value role {value_role}")
            }
        }
    }
}

impl GroupingValue for DynValue {
    type Key = DynIndex;

    fn to_group_key(value: &Self::Value<'_>) -> <Self::Key as IndexDomain>::Owned {
        match value {
            Self::Scalar(value) => DynIndexOwned::Value(Scalar::to_group_key(value)),
            Self::Attribute(value) => DynIndexOwned::Attribute(AttributeName::to_group_key(value)),
            Self::Index(value) => IndexValue::<DynIndex>::to_group_key(value),
            Self::EntityReference(reference) => match &reference.kind {
                DynEntityReferenceKind::Node(value) => DynIndexOwned::Node(value.clone()),
                DynEntityReferenceKind::Edge(value) => DynIndexOwned::Edge(*value),
            },
            Self::FailureKind(value) => {
                DynIndexOwned::FailureKind(FailureKindValue::to_group_key(value))
            }
            Self::Failure(_) => {
                panic!("registry admitted GroupingValue for a dynamic failure-value lane")
            }
        }
    }
}

impl ValueTransition<DynValue> for Mask {
    fn transition<'a>(
        _label: &'static str,
        value: Self::Value<'a>,
    ) -> QueryResult<<DynValue as ValueDomain>::Value<'a>> {
        Ok(DynValue::Index(DynIndexOwned::Bool(value)))
    }
}

impl IntValue for DynValue {
    fn into_int(label: &'static str, value: Self::Value<'_>) -> QueryResult<i64> {
        match value {
            Self::Scalar(value) => Scalar::into_int(label, value),
            Self::Attribute(value) => AttributeName::into_int(label, value),
            Self::Index(DynIndexOwned::Positional(value)) => {
                IndexValue::<Positional>::into_int(label, value)
            }
            Self::Index(DynIndexOwned::Node(value)) => {
                IndexValue::<NodeIndex>::into_int(label, value)
            }
            Self::Index(DynIndexOwned::Edge(value)) => {
                IndexValue::<EdgeIndex>::into_int(label, value)
            }
            Self::Index(DynIndexOwned::Attribute(value)) => {
                IndexValue::<AttributeName>::into_int(label, value)
            }
            Self::Index(DynIndexOwned::Value(value)) => IndexValue::<Value>::into_int(label, value),
            value => {
                let value_role = value.description();
                panic!("registry admitted IntValue for dynamic value role {value_role}")
            }
        }
    }
}

impl ValueKindTest for DynValue {
    fn kind(value: &Self::Value<'_>) -> PayloadKind {
        match value {
            Self::Scalar(value) => Scalar::kind(value),
            Self::Attribute(value) => AttributeName::kind(value),
            Self::Index(DynIndexOwned::Node(value)) => IndexValue::<NodeIndex>::kind(value),
            Self::Index(DynIndexOwned::Attribute(value)) => {
                IndexValue::<AttributeName>::kind(value)
            }
            Self::Index(DynIndexOwned::Value(value)) => IndexValue::<Value>::kind(value),
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
        label: &'static str,
        value: Self::Value<'a>,
        lower: Self::Value<'a>,
        upper: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        match (value, lower, upper) {
            (Self::Scalar(value), Self::Scalar(lower), Self::Scalar(upper)) => {
                Scalar::clip(label, value, lower, upper).map(Self::Scalar)
            }
            (Self::Attribute(value), Self::Attribute(lower), Self::Attribute(upper)) => {
                AttributeName::clip(label, value, lower, upper).map(Self::Attribute)
            }
            (
                Self::Index(DynIndexOwned::Positional(value)),
                Self::Index(DynIndexOwned::Positional(lower)),
                Self::Index(DynIndexOwned::Positional(upper)),
            ) => IndexValue::<Positional>::clip(label, value, lower, upper)
                .map(DynIndexOwned::Positional)
                .map(Self::Index),
            (
                Self::Index(DynIndexOwned::Node(value)),
                Self::Index(DynIndexOwned::Node(lower)),
                Self::Index(DynIndexOwned::Node(upper)),
            ) => IndexValue::<NodeIndex>::clip(label, value, lower, upper)
                .map(DynIndexOwned::Node)
                .map(Self::Index),
            (
                Self::Index(DynIndexOwned::Edge(value)),
                Self::Index(DynIndexOwned::Edge(lower)),
                Self::Index(DynIndexOwned::Edge(upper)),
            ) => IndexValue::<EdgeIndex>::clip(label, value, lower, upper)
                .map(DynIndexOwned::Edge)
                .map(Self::Index),
            (
                Self::Index(DynIndexOwned::Attribute(value)),
                Self::Index(DynIndexOwned::Attribute(lower)),
                Self::Index(DynIndexOwned::Attribute(upper)),
            ) => IndexValue::<AttributeName>::clip(label, value, lower, upper)
                .map(DynIndexOwned::Attribute)
                .map(Self::Index),
            (
                Self::Index(DynIndexOwned::Value(value)),
                Self::Index(DynIndexOwned::Value(lower)),
                Self::Index(DynIndexOwned::Value(upper)),
            ) => IndexValue::<Value>::clip(label, value, lower, upper)
                .map(DynIndexOwned::Value)
                .map(Self::Index),
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

impl EnsureSortable for DynValue {
    fn find_incomparable<'a>(values: impl Iterator<Item = &'a Self>) -> Option<(usize, usize)> {
        let values: Vec<_> = values.collect();
        let first = values.first()?;

        match first {
            Self::Scalar(_) if values.iter().all(|value| matches!(value, Self::Scalar(_))) => {
                Value::find_incomparable(values.into_iter().map(|value| {
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
                AttributeName::find_incomparable(values.into_iter().map(|value| {
                    let Self::Attribute(value) = value else {
                        unreachable!("dynamic values were checked as attributes")
                    };
                    value
                }))
            }
            Self::Index(_) if values.iter().all(|value| matches!(value, Self::Index(_))) => {
                DynIndexOwned::find_incomparable(values.into_iter().map(|value| {
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

impl StringValue for DynValue {
    fn into_string(label: &'static str, value: Self::Value<'_>) -> QueryResult<String> {
        match value {
            Self::Scalar(value) => Scalar::into_string(label, value),
            Self::Attribute(value) => AttributeName::into_string(label, value),
            Self::Index(DynIndexOwned::Node(value)) => {
                IndexValue::<NodeIndex>::into_string(label, value)
            }
            Self::Index(DynIndexOwned::Attribute(value)) => {
                IndexValue::<AttributeName>::into_string(label, value)
            }
            Self::Index(DynIndexOwned::Value(value)) => {
                IndexValue::<Value>::into_string(label, value)
            }
            value => {
                let value_role = value.description();
                panic!("registry admitted StringValue for dynamic value role {value_role}")
            }
        }
    }

    fn from_string<'a>(role: &Self::Value<'_>, value: String) -> Self::Value<'a> {
        match role {
            Self::Scalar(role) => Self::Scalar(Scalar::from_string(role, value)),
            Self::Attribute(role) => Self::Attribute(AttributeName::from_string(role, value)),
            Self::Index(DynIndexOwned::Node(role)) => Self::Index(DynIndexOwned::Node(
                IndexValue::<NodeIndex>::from_string(role, value),
            )),
            Self::Index(DynIndexOwned::Attribute(role)) => Self::Index(DynIndexOwned::Attribute(
                IndexValue::<AttributeName>::from_string(role, value),
            )),
            Self::Index(DynIndexOwned::Value(role)) => Self::Index(DynIndexOwned::Value(
                IndexValue::<Value>::from_string(role, value),
            )),
            role => {
                let role = role.description();
                panic!("registry supplied StringValue::from_string with dynamic role {role}")
            }
        }
    }
}
