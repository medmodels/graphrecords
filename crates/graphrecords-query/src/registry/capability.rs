use super::descriptor::{DomainDescriptor, IndexDescriptor, ValueDescriptor, ValueRole};
use crate::{
    BareValueDomain, EdgeEndpointRole, EntityDomain, FailureKind, FailureKindValue, FailureValue,
    IndexDomain, IndexValue, Mask, Positional, Scalar, ValueDomain,
    capabilities::{
        EnsureSortable, GroupingValue, IntValue, StringValue, ValueAbsolute, ValueAdd, ValueCast,
        ValueCeil, ValueClip, ValueCubeRoot, ValueDivide, ValueEquality, ValueEquivalence,
        ValueExponential, ValueFloor, ValueKindTest, ValueLogarithm, ValueMedian, ValueMode,
        ValueModulo, ValueMultiply, ValueNegate, ValueOrdering, ValuePower, ValueRound,
        ValueScalar, ValueScalarKindTest, ValueSign, ValueSquareRoot, ValueSubtract,
    },
    cast::{
        Bool as BoolTarget, DateTime as DateTimeTarget, Duration as DurationTarget,
        Float as FloatTarget, Int as IntTarget, String as StringTarget,
    },
    index::{EntityAttributes, GroupKey, IndicesInGroup},
};
use graphrecords_core::graphrecord::{AttributeName, EdgeIndex, NodeIndex, Value};
use graphrecords_utils::aliases::{GrHashMap, GrHashSet};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum CapabilityIdentifier {
    Absolute,
    Add,
    BareValue,
    CastBool,
    CastDateTime,
    CastDuration,
    CastFloat,
    CastInt,
    CastString,
    Ceil,
    Clip,
    CubeRoot,
    Divide,
    Entity,
    EntityAttributes,
    Equality,
    Equivalence,
    Exponential,
    Floor,
    Grouping,
    GroupKey,
    IndicesInGroup,
    Int,
    KindTest,
    Logarithm,
    Median,
    Mode,
    Modulo,
    Multiply,
    Negate,
    Ordering,
    Power,
    Round,
    Scalar,
    ScalarKindTest,
    Sign,
    Sortable,
    SquareRoot,
    String,
    Subtract,
}

enum ValueCapabilityMember {
    Value(DomainDescriptor),
    Index(Option<DomainDescriptor>),
    IndexCapable(CapabilityIdentifier),
    EntityReference,
    EntityReferenceCapable(CapabilityIdentifier),
}

impl ValueCapabilityMember {
    fn matches(&self, value: &ValueDescriptor, registry: &CapabilityRegistry) -> bool {
        match (self, value.role()) {
            (Self::Value(domain), ValueRole::Value) => value.domain() == domain,
            (Self::Index(None), ValueRole::Index(index)) => registry.contains_index(index),
            (Self::Index(Some(domain)), ValueRole::Index(IndexDescriptor::Domain(candidate))) => {
                candidate == domain
            }
            (Self::IndexCapable(capability), ValueRole::Index(index)) => {
                registry.index_has(*capability, index)
            }
            (Self::EntityReference, ValueRole::EntityReference(index)) => {
                registry.index_has(CapabilityIdentifier::Entity, index)
            }
            (Self::EntityReferenceCapable(capability), ValueRole::EntityReference(index)) => {
                registry.index_has(CapabilityIdentifier::Entity, index)
                    && registry.index_has(*capability, index)
            }
            _ => false,
        }
    }
}

#[derive(Default)]
pub struct CapabilityRegistry {
    index_domains: GrHashSet<DomainDescriptor>,
    value_domains: GrHashSet<DomainDescriptor>,
    value_members: GrHashMap<CapabilityIdentifier, Vec<ValueCapabilityMember>>,
    index_members: GrHashMap<CapabilityIdentifier, GrHashSet<DomainDescriptor>>,
    group_keys: GrHashMap<DomainDescriptor, IndexDescriptor>,
}

impl CapabilityRegistry {
    #[must_use]
    pub fn builtins() -> Self {
        let mut registry = Self::default();

        registry.register_index_domain::<Value>();
        registry.register_index_domain::<bool>();
        registry.register_index_domain::<AttributeName>();
        registry.register_index_domain::<FailureKind>();
        registry.register_index_domain::<Positional>();
        registry.register_index_domain::<NodeIndex>();
        registry.register_index_domain::<EdgeIndex>();
        registry.register_index_domain::<EdgeEndpointRole>();

        registry.register_value_domain::<Scalar>();
        registry.register_value_domain::<Mask>();
        registry.register_value_domain::<AttributeName>();
        registry.register_value_domain::<FailureValue>();
        registry.register_value_domain::<FailureKindValue>();

        registry.register_value_add_domain::<Scalar>();
        registry.register_value_add_domain::<AttributeName>();
        registry.register_value_add_index::<Positional>();
        registry.register_value_add_index::<NodeIndex>();
        registry.register_value_add_index::<AttributeName>();
        registry.register_value_add_index::<EdgeIndex>();
        registry.register_value_add_index::<Value>();

        registry.register_value_multiply_domain::<Scalar>();
        registry.register_value_multiply_domain::<AttributeName>();
        registry.register_value_multiply_index::<Positional>();
        registry.register_value_multiply_index::<NodeIndex>();
        registry.register_value_multiply_index::<AttributeName>();
        registry.register_value_multiply_index::<EdgeIndex>();
        registry.register_value_multiply_index::<Value>();

        registry.register_value_scalar_domain::<Scalar>();
        registry.register_value_scalar_index::<Value>();

        registry.register_value_equivalence_domain::<Scalar>();
        registry.register_value_equivalence_domain::<Mask>();
        registry.register_value_equivalence_domain::<AttributeName>();
        registry.register_value_equivalence_indices();
        registry.register_value_equivalence_domain::<FailureKindValue>();

        registry.register_value_median_domain::<Scalar>();
        registry.register_value_median_index::<Value>();

        registry.register_value_mode_domain::<Scalar>();
        registry.register_value_mode_domain::<Mask>();
        registry.register_value_mode_domain::<AttributeName>();
        registry.register_value_mode_indices();

        registry.register_value_ordering_domain::<Scalar>();
        registry.register_value_ordering_domain::<AttributeName>();
        registry.register_value_ordering_index::<Positional>();
        registry.register_value_ordering_index::<NodeIndex>();
        registry.register_value_ordering_index::<AttributeName>();
        registry.register_value_ordering_index::<EdgeIndex>();
        registry.register_value_ordering_index::<Value>();
        registry.register_value_ordering_index::<bool>();
        registry.register_value_ordering_indices();

        registry.register_bare_value_domain::<Scalar>();
        registry.register_bare_value_domain::<Mask>();
        registry.register_bare_value_domain::<AttributeName>();
        registry.register_bare_index_values();
        registry.register_bare_entity_references();
        registry.register_bare_value_domain::<FailureValue>();
        registry.register_bare_value_domain::<FailureKindValue>();

        registry.register_entity_domain::<EdgeIndex>();
        registry.register_entity_domain::<NodeIndex>();

        registry.register_group_key::<Value>();
        registry.register_group_key::<bool>();
        registry.register_group_key::<AttributeName>();
        registry.register_group_key::<FailureKind>();
        registry.register_group_key::<Positional>();
        registry.register_group_key::<NodeIndex>();
        registry.register_group_key::<EdgeIndex>();
        registry.register_group_key::<EdgeEndpointRole>();

        registry.register_entity_attributes::<NodeIndex>();
        registry.register_entity_attributes::<EdgeIndex>();

        registry.register_indices_in_group::<NodeIndex>();
        registry.register_indices_in_group::<EdgeIndex>();

        registry.register_index_sortable::<Value>();
        registry.register_index_sortable::<bool>();
        registry.register_index_sortable::<AttributeName>();
        registry.register_index_sortable::<Positional>();
        registry.register_index_sortable::<NodeIndex>();
        registry.register_index_sortable::<EdgeIndex>();

        registry.register_value_absolute_domain::<Scalar>();
        registry.register_value_absolute_domain::<AttributeName>();
        registry.register_value_absolute_index::<NodeIndex>();
        registry.register_value_absolute_index::<AttributeName>();
        registry.register_value_absolute_index::<Value>();

        registry.register_value_cast_bool_domain::<Scalar>();
        registry.register_value_cast_bool_index::<Value>();

        registry.register_value_cast_date_time_domain::<Scalar>();
        registry.register_value_cast_date_time_index::<Value>();

        registry.register_value_cast_duration_domain::<Scalar>();
        registry.register_value_cast_duration_index::<Value>();

        registry.register_value_cast_float_domain::<Scalar>();
        registry.register_value_cast_float_index::<Value>();

        registry.register_value_cast_int_domain::<Scalar>();
        registry.register_value_cast_int_domain::<AttributeName>();
        registry.register_value_cast_int_index::<Value>();
        registry.register_value_cast_int_index::<NodeIndex>();
        registry.register_value_cast_int_index::<AttributeName>();

        registry.register_value_cast_string_domain::<Scalar>();
        registry.register_value_cast_string_domain::<AttributeName>();
        registry.register_value_cast_string_index::<Value>();
        registry.register_value_cast_string_index::<NodeIndex>();
        registry.register_value_cast_string_index::<AttributeName>();

        registry.register_value_ceil_domain::<Scalar>();
        registry.register_value_ceil_index::<Value>();

        registry.register_value_clip_domain::<Scalar>();
        registry.register_value_clip_domain::<AttributeName>();
        registry.register_value_clip_index::<Positional>();
        registry.register_value_clip_index::<NodeIndex>();
        registry.register_value_clip_index::<AttributeName>();
        registry.register_value_clip_index::<EdgeIndex>();
        registry.register_value_clip_index::<Value>();

        registry.register_value_cube_root_domain::<Scalar>();
        registry.register_value_cube_root_index::<Value>();

        registry.register_value_divide_domain::<Scalar>();
        registry.register_value_divide_index::<Value>();

        registry.register_value_equality_domain::<Scalar>();
        registry.register_value_equality_domain::<AttributeName>();
        registry.register_value_equality_domain::<Mask>();
        registry.register_value_equality_domain::<FailureKindValue>();
        registry.register_value_equality_indices();

        registry.register_value_exponential_domain::<Scalar>();
        registry.register_value_exponential_index::<Value>();

        registry.register_value_floor_domain::<Scalar>();
        registry.register_value_floor_index::<Value>();

        registry.register_value_grouping_domain::<Scalar>();
        registry.register_value_grouping_domain::<Mask>();
        registry.register_value_grouping_domain::<AttributeName>();
        registry.register_value_grouping_domain::<FailureKindValue>();
        registry.register_value_grouping_indices();
        registry.register_value_grouping_entity_references();

        registry.register_value_int_domain::<Scalar>();
        registry.register_value_int_domain::<AttributeName>();
        registry.register_value_int_index::<Value>();
        registry.register_value_int_index::<NodeIndex>();
        registry.register_value_int_index::<AttributeName>();
        registry.register_value_int_index::<EdgeIndex>();
        registry.register_value_int_index::<Positional>();

        registry.register_value_kind_test_domain::<Scalar>();
        registry.register_value_kind_test_domain::<AttributeName>();
        registry.register_value_kind_test_index::<Value>();
        registry.register_value_kind_test_index::<NodeIndex>();
        registry.register_value_kind_test_index::<AttributeName>();

        registry.register_value_logarithm_domain::<Scalar>();
        registry.register_value_logarithm_index::<Value>();

        registry.register_value_modulo_domain::<Scalar>();
        registry.register_value_modulo_domain::<AttributeName>();
        registry.register_value_modulo_index::<Positional>();
        registry.register_value_modulo_index::<NodeIndex>();
        registry.register_value_modulo_index::<AttributeName>();
        registry.register_value_modulo_index::<EdgeIndex>();
        registry.register_value_modulo_index::<Value>();

        registry.register_value_negate_domain::<Scalar>();
        registry.register_value_negate_domain::<AttributeName>();
        registry.register_value_negate_index::<NodeIndex>();
        registry.register_value_negate_index::<AttributeName>();
        registry.register_value_negate_index::<Value>();

        registry.register_value_power_domain::<Scalar>();
        registry.register_value_power_domain::<AttributeName>();
        registry.register_value_power_index::<Positional>();
        registry.register_value_power_index::<NodeIndex>();
        registry.register_value_power_index::<AttributeName>();
        registry.register_value_power_index::<EdgeIndex>();
        registry.register_value_power_index::<Value>();

        registry.register_value_round_domain::<Scalar>();
        registry.register_value_round_index::<Value>();

        registry.register_value_scalar_kind_test_domain::<Scalar>();
        registry.register_value_scalar_kind_test_index::<Value>();

        registry.register_value_sign_domain::<Scalar>();
        registry.register_value_sign_domain::<AttributeName>();
        registry.register_value_sign_index::<NodeIndex>();
        registry.register_value_sign_index::<AttributeName>();
        registry.register_value_sign_index::<Value>();

        registry.register_value_sortable_domain::<Scalar>();
        registry.register_value_sortable_domain::<Mask>();
        registry.register_value_sortable_domain::<AttributeName>();
        registry.register_value_sortable_indices();

        registry.register_value_square_root_domain::<Scalar>();
        registry.register_value_square_root_index::<Value>();

        registry.register_value_string_domain::<Scalar>();
        registry.register_value_string_domain::<AttributeName>();
        registry.register_value_string_index::<Value>();
        registry.register_value_string_index::<NodeIndex>();
        registry.register_value_string_index::<AttributeName>();

        registry.register_value_subtract_domain::<Scalar>();
        registry.register_value_subtract_domain::<AttributeName>();
        registry.register_value_subtract_index::<Positional>();
        registry.register_value_subtract_index::<NodeIndex>();
        registry.register_value_subtract_index::<AttributeName>();
        registry.register_value_subtract_index::<EdgeIndex>();
        registry.register_value_subtract_index::<Value>();

        registry
    }

    fn register_index_domain<I: IndexDomain>(&mut self) {
        self.index_domains.insert(DomainDescriptor::of::<I>());
    }

    fn register_value_domain<V: ValueDomain>(&mut self) {
        self.value_domains.insert(DomainDescriptor::of::<V>());
    }

    fn register_value_add_domain<V: ValueAdd>(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::Add)
            .or_default()
            .push(ValueCapabilityMember::Value(DomainDescriptor::of::<V>()));
    }

    fn register_value_add_index<I: IndexDomain>(&mut self)
    where
        IndexValue<I>: ValueAdd,
    {
        self.value_members
            .entry(CapabilityIdentifier::Add)
            .or_default()
            .push(ValueCapabilityMember::Index(Some(
                DomainDescriptor::of::<I>(),
            )));
    }

    fn register_value_multiply_domain<V: ValueMultiply>(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::Multiply)
            .or_default()
            .push(ValueCapabilityMember::Value(DomainDescriptor::of::<V>()));
    }

    fn register_value_multiply_index<I: IndexDomain>(&mut self)
    where
        IndexValue<I>: ValueMultiply,
    {
        self.value_members
            .entry(CapabilityIdentifier::Multiply)
            .or_default()
            .push(ValueCapabilityMember::Index(Some(
                DomainDescriptor::of::<I>(),
            )));
    }

    fn register_value_scalar_domain<V: ValueScalar>(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::Scalar)
            .or_default()
            .push(ValueCapabilityMember::Value(DomainDescriptor::of::<V>()));
    }

    fn register_value_scalar_index<I: IndexDomain>(&mut self)
    where
        IndexValue<I>: ValueScalar,
    {
        self.value_members
            .entry(CapabilityIdentifier::Scalar)
            .or_default()
            .push(ValueCapabilityMember::Index(Some(
                DomainDescriptor::of::<I>(),
            )));
    }

    fn register_value_equivalence_domain<V: ValueEquivalence>(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::Equivalence)
            .or_default()
            .push(ValueCapabilityMember::Value(DomainDescriptor::of::<V>()));
    }

    fn register_value_equivalence_indices(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::Equivalence)
            .or_default()
            .push(ValueCapabilityMember::Index(None));
    }

    fn register_value_median_domain<V: ValueMedian>(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::Median)
            .or_default()
            .push(ValueCapabilityMember::Value(DomainDescriptor::of::<V>()));
    }

    fn register_value_median_index<I: IndexDomain>(&mut self)
    where
        IndexValue<I>: ValueMedian,
    {
        self.value_members
            .entry(CapabilityIdentifier::Median)
            .or_default()
            .push(ValueCapabilityMember::Index(Some(
                DomainDescriptor::of::<I>(),
            )));
    }

    fn register_value_mode_domain<V: ValueMode>(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::Mode)
            .or_default()
            .push(ValueCapabilityMember::Value(DomainDescriptor::of::<V>()));
    }

    fn register_value_mode_indices(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::Mode)
            .or_default()
            .push(ValueCapabilityMember::Index(None));
    }

    fn register_value_ordering_domain<V: ValueOrdering>(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::Ordering)
            .or_default()
            .push(ValueCapabilityMember::Value(DomainDescriptor::of::<V>()));
    }

    fn register_value_ordering_index<I: IndexDomain>(&mut self)
    where
        IndexValue<I>: ValueOrdering,
    {
        self.index_members
            .entry(CapabilityIdentifier::Ordering)
            .or_default()
            .insert(DomainDescriptor::of::<I>());
    }

    fn register_value_ordering_indices(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::Ordering)
            .or_default()
            .push(ValueCapabilityMember::IndexCapable(
                CapabilityIdentifier::Ordering,
            ));
    }

    fn register_bare_value_domain<V: BareValueDomain>(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::BareValue)
            .or_default()
            .push(ValueCapabilityMember::Value(DomainDescriptor::of::<V>()));
    }

    fn register_bare_index_values(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::BareValue)
            .or_default()
            .push(ValueCapabilityMember::Index(None));
    }

    fn register_bare_entity_references(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::BareValue)
            .or_default()
            .push(ValueCapabilityMember::EntityReference);
    }

    fn register_entity_domain<I: EntityDomain>(&mut self) {
        self.index_members
            .entry(CapabilityIdentifier::Entity)
            .or_default()
            .insert(DomainDescriptor::of::<I>());
    }

    fn register_group_key<I: GroupKey>(&mut self) {
        self.index_members
            .entry(CapabilityIdentifier::GroupKey)
            .or_default()
            .insert(DomainDescriptor::of::<I>());
    }

    fn register_entity_attributes<I: EntityAttributes>(&mut self) {
        self.index_members
            .entry(CapabilityIdentifier::EntityAttributes)
            .or_default()
            .insert(DomainDescriptor::of::<I>());
    }

    fn register_indices_in_group<I: IndicesInGroup>(&mut self) {
        self.index_members
            .entry(CapabilityIdentifier::IndicesInGroup)
            .or_default()
            .insert(DomainDescriptor::of::<I>());
    }

    fn register_index_sortable<I: IndexDomain>(&mut self)
    where
        for<'a> I::Index<'a>: EnsureSortable,
    {
        self.index_members
            .entry(CapabilityIdentifier::Sortable)
            .or_default()
            .insert(DomainDescriptor::of::<I>());
    }

    fn register_value_absolute_domain<V: ValueAbsolute>(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::Absolute)
            .or_default()
            .push(ValueCapabilityMember::Value(DomainDescriptor::of::<V>()));
    }

    fn register_value_absolute_index<I: IndexDomain>(&mut self)
    where
        IndexValue<I>: ValueAbsolute,
    {
        self.value_members
            .entry(CapabilityIdentifier::Absolute)
            .or_default()
            .push(ValueCapabilityMember::Index(Some(
                DomainDescriptor::of::<I>(),
            )));
    }

    fn register_value_cast_bool_domain<V: ValueCast<BoolTarget>>(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::CastBool)
            .or_default()
            .push(ValueCapabilityMember::Value(DomainDescriptor::of::<V>()));
    }

    fn register_value_cast_bool_index<I: IndexDomain>(&mut self)
    where
        IndexValue<I>: ValueCast<BoolTarget>,
    {
        self.value_members
            .entry(CapabilityIdentifier::CastBool)
            .or_default()
            .push(ValueCapabilityMember::Index(Some(
                DomainDescriptor::of::<I>(),
            )));
    }

    fn register_value_cast_date_time_domain<V: ValueCast<DateTimeTarget>>(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::CastDateTime)
            .or_default()
            .push(ValueCapabilityMember::Value(DomainDescriptor::of::<V>()));
    }

    fn register_value_cast_date_time_index<I: IndexDomain>(&mut self)
    where
        IndexValue<I>: ValueCast<DateTimeTarget>,
    {
        self.value_members
            .entry(CapabilityIdentifier::CastDateTime)
            .or_default()
            .push(ValueCapabilityMember::Index(Some(
                DomainDescriptor::of::<I>(),
            )));
    }

    fn register_value_cast_duration_domain<V: ValueCast<DurationTarget>>(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::CastDuration)
            .or_default()
            .push(ValueCapabilityMember::Value(DomainDescriptor::of::<V>()));
    }

    fn register_value_cast_duration_index<I: IndexDomain>(&mut self)
    where
        IndexValue<I>: ValueCast<DurationTarget>,
    {
        self.value_members
            .entry(CapabilityIdentifier::CastDuration)
            .or_default()
            .push(ValueCapabilityMember::Index(Some(
                DomainDescriptor::of::<I>(),
            )));
    }

    fn register_value_cast_float_domain<V: ValueCast<FloatTarget>>(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::CastFloat)
            .or_default()
            .push(ValueCapabilityMember::Value(DomainDescriptor::of::<V>()));
    }

    fn register_value_cast_float_index<I: IndexDomain>(&mut self)
    where
        IndexValue<I>: ValueCast<FloatTarget>,
    {
        self.value_members
            .entry(CapabilityIdentifier::CastFloat)
            .or_default()
            .push(ValueCapabilityMember::Index(Some(
                DomainDescriptor::of::<I>(),
            )));
    }

    fn register_value_cast_int_domain<V: ValueCast<IntTarget>>(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::CastInt)
            .or_default()
            .push(ValueCapabilityMember::Value(DomainDescriptor::of::<V>()));
    }

    fn register_value_cast_int_index<I: IndexDomain>(&mut self)
    where
        IndexValue<I>: ValueCast<IntTarget>,
    {
        self.value_members
            .entry(CapabilityIdentifier::CastInt)
            .or_default()
            .push(ValueCapabilityMember::Index(Some(
                DomainDescriptor::of::<I>(),
            )));
    }

    fn register_value_cast_string_domain<V: ValueCast<StringTarget>>(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::CastString)
            .or_default()
            .push(ValueCapabilityMember::Value(DomainDescriptor::of::<V>()));
    }

    fn register_value_cast_string_index<I: IndexDomain>(&mut self)
    where
        IndexValue<I>: ValueCast<StringTarget>,
    {
        self.value_members
            .entry(CapabilityIdentifier::CastString)
            .or_default()
            .push(ValueCapabilityMember::Index(Some(
                DomainDescriptor::of::<I>(),
            )));
    }

    fn register_value_ceil_domain<V: ValueCeil>(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::Ceil)
            .or_default()
            .push(ValueCapabilityMember::Value(DomainDescriptor::of::<V>()));
    }

    fn register_value_ceil_index<I: IndexDomain>(&mut self)
    where
        IndexValue<I>: ValueCeil,
    {
        self.value_members
            .entry(CapabilityIdentifier::Ceil)
            .or_default()
            .push(ValueCapabilityMember::Index(Some(
                DomainDescriptor::of::<I>(),
            )));
    }

    fn register_value_clip_domain<V: ValueClip>(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::Clip)
            .or_default()
            .push(ValueCapabilityMember::Value(DomainDescriptor::of::<V>()));
    }

    fn register_value_clip_index<I: IndexDomain>(&mut self)
    where
        IndexValue<I>: ValueClip,
    {
        self.value_members
            .entry(CapabilityIdentifier::Clip)
            .or_default()
            .push(ValueCapabilityMember::Index(Some(
                DomainDescriptor::of::<I>(),
            )));
    }

    fn register_value_cube_root_domain<V: ValueCubeRoot>(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::CubeRoot)
            .or_default()
            .push(ValueCapabilityMember::Value(DomainDescriptor::of::<V>()));
    }

    fn register_value_cube_root_index<I: IndexDomain>(&mut self)
    where
        IndexValue<I>: ValueCubeRoot,
    {
        self.value_members
            .entry(CapabilityIdentifier::CubeRoot)
            .or_default()
            .push(ValueCapabilityMember::Index(Some(
                DomainDescriptor::of::<I>(),
            )));
    }

    fn register_value_divide_domain<V: ValueDivide>(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::Divide)
            .or_default()
            .push(ValueCapabilityMember::Value(DomainDescriptor::of::<V>()));
    }

    fn register_value_divide_index<I: IndexDomain>(&mut self)
    where
        IndexValue<I>: ValueDivide,
    {
        self.value_members
            .entry(CapabilityIdentifier::Divide)
            .or_default()
            .push(ValueCapabilityMember::Index(Some(
                DomainDescriptor::of::<I>(),
            )));
    }

    fn register_value_equality_domain<V: ValueEquality>(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::Equality)
            .or_default()
            .push(ValueCapabilityMember::Value(DomainDescriptor::of::<V>()));
    }

    fn register_value_equality_indices(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::Equality)
            .or_default()
            .push(ValueCapabilityMember::Index(None));
    }

    fn register_value_exponential_domain<V: ValueExponential>(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::Exponential)
            .or_default()
            .push(ValueCapabilityMember::Value(DomainDescriptor::of::<V>()));
    }

    fn register_value_exponential_index<I: IndexDomain>(&mut self)
    where
        IndexValue<I>: ValueExponential,
    {
        self.value_members
            .entry(CapabilityIdentifier::Exponential)
            .or_default()
            .push(ValueCapabilityMember::Index(Some(
                DomainDescriptor::of::<I>(),
            )));
    }

    fn register_value_floor_domain<V: ValueFloor>(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::Floor)
            .or_default()
            .push(ValueCapabilityMember::Value(DomainDescriptor::of::<V>()));
    }

    fn register_value_floor_index<I: IndexDomain>(&mut self)
    where
        IndexValue<I>: ValueFloor,
    {
        self.value_members
            .entry(CapabilityIdentifier::Floor)
            .or_default()
            .push(ValueCapabilityMember::Index(Some(
                DomainDescriptor::of::<I>(),
            )));
    }

    fn register_value_grouping_domain<V: GroupingValue>(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::Grouping)
            .or_default()
            .push(ValueCapabilityMember::Value(DomainDescriptor::of::<V>()));
        self.group_keys.insert(
            DomainDescriptor::of::<V>(),
            IndexDescriptor::domain::<V::Key>(),
        );
    }

    fn register_value_grouping_indices(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::Grouping)
            .or_default()
            .push(ValueCapabilityMember::IndexCapable(
                CapabilityIdentifier::GroupKey,
            ));
    }

    fn register_value_grouping_entity_references(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::Grouping)
            .or_default()
            .push(ValueCapabilityMember::EntityReferenceCapable(
                CapabilityIdentifier::GroupKey,
            ));
    }

    fn register_value_int_domain<V: IntValue>(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::Int)
            .or_default()
            .push(ValueCapabilityMember::Value(DomainDescriptor::of::<V>()));
    }

    fn register_value_int_index<I: IndexDomain>(&mut self)
    where
        IndexValue<I>: IntValue,
    {
        self.value_members
            .entry(CapabilityIdentifier::Int)
            .or_default()
            .push(ValueCapabilityMember::Index(Some(
                DomainDescriptor::of::<I>(),
            )));
    }

    fn register_value_kind_test_domain<V: ValueKindTest>(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::KindTest)
            .or_default()
            .push(ValueCapabilityMember::Value(DomainDescriptor::of::<V>()));
    }

    fn register_value_kind_test_index<I: IndexDomain>(&mut self)
    where
        IndexValue<I>: ValueKindTest,
    {
        self.value_members
            .entry(CapabilityIdentifier::KindTest)
            .or_default()
            .push(ValueCapabilityMember::Index(Some(
                DomainDescriptor::of::<I>(),
            )));
    }

    fn register_value_logarithm_domain<V: ValueLogarithm>(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::Logarithm)
            .or_default()
            .push(ValueCapabilityMember::Value(DomainDescriptor::of::<V>()));
    }

    fn register_value_logarithm_index<I: IndexDomain>(&mut self)
    where
        IndexValue<I>: ValueLogarithm,
    {
        self.value_members
            .entry(CapabilityIdentifier::Logarithm)
            .or_default()
            .push(ValueCapabilityMember::Index(Some(
                DomainDescriptor::of::<I>(),
            )));
    }

    fn register_value_modulo_domain<V: ValueModulo>(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::Modulo)
            .or_default()
            .push(ValueCapabilityMember::Value(DomainDescriptor::of::<V>()));
    }

    fn register_value_modulo_index<I: IndexDomain>(&mut self)
    where
        IndexValue<I>: ValueModulo,
    {
        self.value_members
            .entry(CapabilityIdentifier::Modulo)
            .or_default()
            .push(ValueCapabilityMember::Index(Some(
                DomainDescriptor::of::<I>(),
            )));
    }

    fn register_value_negate_domain<V: ValueNegate>(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::Negate)
            .or_default()
            .push(ValueCapabilityMember::Value(DomainDescriptor::of::<V>()));
    }

    fn register_value_negate_index<I: IndexDomain>(&mut self)
    where
        IndexValue<I>: ValueNegate,
    {
        self.value_members
            .entry(CapabilityIdentifier::Negate)
            .or_default()
            .push(ValueCapabilityMember::Index(Some(
                DomainDescriptor::of::<I>(),
            )));
    }

    fn register_value_power_domain<V: ValuePower>(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::Power)
            .or_default()
            .push(ValueCapabilityMember::Value(DomainDescriptor::of::<V>()));
    }

    fn register_value_power_index<I: IndexDomain>(&mut self)
    where
        IndexValue<I>: ValuePower,
    {
        self.value_members
            .entry(CapabilityIdentifier::Power)
            .or_default()
            .push(ValueCapabilityMember::Index(Some(
                DomainDescriptor::of::<I>(),
            )));
    }

    fn register_value_round_domain<V: ValueRound>(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::Round)
            .or_default()
            .push(ValueCapabilityMember::Value(DomainDescriptor::of::<V>()));
    }

    fn register_value_round_index<I: IndexDomain>(&mut self)
    where
        IndexValue<I>: ValueRound,
    {
        self.value_members
            .entry(CapabilityIdentifier::Round)
            .or_default()
            .push(ValueCapabilityMember::Index(Some(
                DomainDescriptor::of::<I>(),
            )));
    }

    fn register_value_scalar_kind_test_domain<V: ValueScalarKindTest>(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::ScalarKindTest)
            .or_default()
            .push(ValueCapabilityMember::Value(DomainDescriptor::of::<V>()));
    }

    fn register_value_scalar_kind_test_index<I: IndexDomain>(&mut self)
    where
        IndexValue<I>: ValueScalarKindTest,
    {
        self.value_members
            .entry(CapabilityIdentifier::ScalarKindTest)
            .or_default()
            .push(ValueCapabilityMember::Index(Some(
                DomainDescriptor::of::<I>(),
            )));
    }

    fn register_value_sign_domain<V: ValueSign>(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::Sign)
            .or_default()
            .push(ValueCapabilityMember::Value(DomainDescriptor::of::<V>()));
    }

    fn register_value_sign_index<I: IndexDomain>(&mut self)
    where
        IndexValue<I>: ValueSign,
    {
        self.value_members
            .entry(CapabilityIdentifier::Sign)
            .or_default()
            .push(ValueCapabilityMember::Index(Some(
                DomainDescriptor::of::<I>(),
            )));
    }

    fn register_value_sortable_domain<V: ValueDomain>(&mut self)
    where
        for<'a> V::Value<'a>: EnsureSortable,
    {
        self.value_members
            .entry(CapabilityIdentifier::Sortable)
            .or_default()
            .push(ValueCapabilityMember::Value(DomainDescriptor::of::<V>()));
    }

    fn register_value_sortable_indices(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::Sortable)
            .or_default()
            .push(ValueCapabilityMember::IndexCapable(
                CapabilityIdentifier::Sortable,
            ));
    }

    fn register_value_square_root_domain<V: ValueSquareRoot>(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::SquareRoot)
            .or_default()
            .push(ValueCapabilityMember::Value(DomainDescriptor::of::<V>()));
    }

    fn register_value_square_root_index<I: IndexDomain>(&mut self)
    where
        IndexValue<I>: ValueSquareRoot,
    {
        self.value_members
            .entry(CapabilityIdentifier::SquareRoot)
            .or_default()
            .push(ValueCapabilityMember::Index(Some(
                DomainDescriptor::of::<I>(),
            )));
    }

    fn register_value_string_domain<V: StringValue>(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::String)
            .or_default()
            .push(ValueCapabilityMember::Value(DomainDescriptor::of::<V>()));
    }

    fn register_value_string_index<I: IndexDomain>(&mut self)
    where
        IndexValue<I>: StringValue,
    {
        self.value_members
            .entry(CapabilityIdentifier::String)
            .or_default()
            .push(ValueCapabilityMember::Index(Some(
                DomainDescriptor::of::<I>(),
            )));
    }

    fn register_value_subtract_domain<V: ValueSubtract>(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::Subtract)
            .or_default()
            .push(ValueCapabilityMember::Value(DomainDescriptor::of::<V>()));
    }

    fn register_value_subtract_index<I: IndexDomain>(&mut self)
    where
        IndexValue<I>: ValueSubtract,
    {
        self.value_members
            .entry(CapabilityIdentifier::Subtract)
            .or_default()
            .push(ValueCapabilityMember::Index(Some(
                DomainDescriptor::of::<I>(),
            )));
    }

    #[must_use]
    pub fn value_has(&self, capability: CapabilityIdentifier, value: &ValueDescriptor) -> bool {
        self.value_members
            .get(&capability)
            .is_some_and(|members| members.iter().any(|member| member.matches(value, self)))
    }

    #[must_use]
    pub fn group_key(&self, value: &ValueDescriptor) -> Option<IndexDescriptor> {
        match value.role() {
            ValueRole::Value => self.group_keys.get(value.domain()).cloned(),
            ValueRole::Index(index) | ValueRole::EntityReference(index) => Some(index.clone()),
            ValueRole::Unit => None,
        }
    }

    #[must_use]
    pub fn index_has(&self, capability: CapabilityIdentifier, index: &IndexDescriptor) -> bool {
        match (capability, index) {
            (
                CapabilityIdentifier::GroupKey
                | CapabilityIdentifier::Ordering
                | CapabilityIdentifier::Sortable,
                IndexDescriptor::Expanded { parent, child },
            ) => self.index_has(capability, parent) && self.index_has(capability, child),
            (_, IndexDescriptor::Expanded { .. } | IndexDescriptor::ExpandedSource { .. }) => false,
            (_, IndexDescriptor::Domain(domain)) => self
                .index_members
                .get(&capability)
                .is_some_and(|members| members.contains(domain)),
        }
    }

    pub(super) fn contains_index(&self, index: &IndexDescriptor) -> bool {
        match index {
            IndexDescriptor::Domain(domain) => self.index_domains.contains(domain),
            IndexDescriptor::Expanded { parent, child } => {
                self.contains_index(parent) && self.contains_index(child)
            }
            IndexDescriptor::ExpandedSource { .. } => false,
        }
    }

    pub(super) fn contains_value(&self, value: &ValueDescriptor) -> bool {
        match value.role() {
            ValueRole::Value => self.value_domains.contains(value.domain()),
            ValueRole::Index(index) => self.contains_index(index),
            ValueRole::EntityReference(index) => {
                self.index_has(CapabilityIdentifier::Entity, index)
            }
            ValueRole::Unit => true,
        }
    }
}
