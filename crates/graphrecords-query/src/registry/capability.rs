use super::descriptor::{DomainDescriptor, IndexDescriptor, ValueDescriptor, ValueRole};
use crate::{
    BareValueDomain, EdgeEndpointRole, EntityDomain, FailureKind, FailureKindValue, FailureValue,
    IndexDomain, IndexValue, Mask, Positional, Scalar, ValueDomain,
    capabilities::{
        EnsureSortable, ValueAbsolute, ValueAdd, ValueCast, ValueCeil, ValueClip, ValueCubeRoot,
        ValueDivide, ValueEquality, ValueEquivalence, ValueExponential, ValueFloor, ValueGrouping,
        ValueInt, ValueKindTest, ValueLogarithm, ValueMedian, ValueMode, ValueModulo,
        ValueMultiply, ValueNegate, ValueOrdering, ValuePower, ValueRound, ValueScalar,
        ValueScalarKindTest, ValueSign, ValueSquareRoot, ValueString, ValueSubtract,
        ValueTransition,
    },
    cast::{
        Bool as BoolTarget, DateTime as DateTimeTarget, Duration as DurationTarget,
        Float as FloatTarget, Int as IntTarget, String as StringTarget,
    },
    index::{EntityAttributes, GroupMembership},
};
use graphrecords_core::graphrecord::{AttributeName, EdgeIndex, Group, NodeIndex, Value};
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
    GroupMembership,
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
    TransitionAttributeName,
    TransitionAttributeNameIndex,
    TransitionBoolIndex,
    TransitionFailureKindIndex,
    TransitionFailureKindValue,
    TransitionGroupIndex,
    TransitionMask,
    TransitionNodeIndex,
    TransitionPositionalIndex,
    TransitionScalar,
    TransitionValueIndex,
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

macro_rules! register_value_capability {
    (domain $($method:ident => $capability:ident: $bound:path),+ $(,)?) => {
        $(
            fn $method<V: $bound>(&mut self) {
                self.value_members
                    .entry(CapabilityIdentifier::$capability)
                    .or_default()
                    .push(ValueCapabilityMember::Value(DomainDescriptor::of::<V>()));
            }
        )+
    };
    (index $($method:ident => $capability:ident: $bound:path),+ $(,)?) => {
        $(
            fn $method<I: IndexDomain>(&mut self)
            where
                IndexValue<I>: $bound,
            {
                self.value_members
                    .entry(CapabilityIdentifier::$capability)
                    .or_default()
                    .push(ValueCapabilityMember::Index(Some(
                        DomainDescriptor::of::<I>(),
                    )));
            }
        )+
    };
    (indices $($method:ident => $capability:ident),+ $(,)?) => {
        $(
            fn $method(&mut self) {
                self.value_members
                    .entry(CapabilityIdentifier::$capability)
                    .or_default()
                    .push(ValueCapabilityMember::Index(None));
            }
        )+
    };
    (capable_indices $($method:ident => $capability:ident: $required:ident),+ $(,)?) => {
        $(
            fn $method(&mut self) {
                self.value_members
                    .entry(CapabilityIdentifier::$capability)
                    .or_default()
                    .push(ValueCapabilityMember::IndexCapable(
                        CapabilityIdentifier::$required,
                    ));
            }
        )+
    };
}

impl CapabilityRegistry {
    register_value_capability! { domain
        register_bare_value_domain => BareValue: BareValueDomain,
        register_value_absolute_domain => Absolute: ValueAbsolute,
        register_value_add_domain => Add: ValueAdd,
        register_value_cast_bool_domain => CastBool: ValueCast<BoolTarget>,
        register_value_cast_date_time_domain => CastDateTime: ValueCast<DateTimeTarget>,
        register_value_cast_duration_domain => CastDuration: ValueCast<DurationTarget>,
        register_value_cast_float_domain => CastFloat: ValueCast<FloatTarget>,
        register_value_cast_int_domain => CastInt: ValueCast<IntTarget>,
        register_value_cast_string_domain => CastString: ValueCast<StringTarget>,
        register_value_ceil_domain => Ceil: ValueCeil,
        register_value_clip_domain => Clip: ValueClip,
        register_value_cube_root_domain => CubeRoot: ValueCubeRoot,
        register_value_divide_domain => Divide: ValueDivide,
        register_value_equality_domain => Equality: ValueEquality,
        register_value_equivalence_domain => Equivalence: ValueEquivalence,
        register_value_exponential_domain => Exponential: ValueExponential,
        register_value_floor_domain => Floor: ValueFloor,
        register_value_int_domain => Int: ValueInt,
        register_value_kind_test_domain => KindTest: ValueKindTest,
        register_value_logarithm_domain => Logarithm: ValueLogarithm,
        register_value_median_domain => Median: ValueMedian,
        register_value_mode_domain => Mode: ValueMode,
        register_value_modulo_domain => Modulo: ValueModulo,
        register_value_multiply_domain => Multiply: ValueMultiply,
        register_value_negate_domain => Negate: ValueNegate,
        register_value_ordering_domain => Ordering: ValueOrdering,
        register_value_power_domain => Power: ValuePower,
        register_value_round_domain => Round: ValueRound,
        register_value_scalar_domain => Scalar: ValueScalar,
        register_value_scalar_kind_test_domain => ScalarKindTest: ValueScalarKindTest,
        register_value_sign_domain => Sign: ValueSign,
        register_value_square_root_domain => SquareRoot: ValueSquareRoot,
        register_value_string_domain => String: ValueString,
        register_value_subtract_domain => Subtract: ValueSubtract,
        register_value_transition_attribute_name_domain => TransitionAttributeName: ValueTransition<AttributeName>,
        register_value_transition_attribute_name_index_domain => TransitionAttributeNameIndex: ValueTransition<IndexValue<AttributeName>>,
        register_value_transition_bool_index_domain => TransitionBoolIndex: ValueTransition<IndexValue<bool>>,
        register_value_transition_failure_kind_index_domain => TransitionFailureKindIndex: ValueTransition<IndexValue<FailureKind>>,
        register_value_transition_group_index_domain => TransitionGroupIndex: ValueTransition<IndexValue<Group>>,
        register_value_transition_mask_domain => TransitionMask: ValueTransition<Mask>,
        register_value_transition_node_index_domain => TransitionNodeIndex: ValueTransition<IndexValue<NodeIndex>>,
        register_value_transition_positional_index_domain => TransitionPositionalIndex: ValueTransition<IndexValue<Positional>>,
        register_value_transition_scalar_domain => TransitionScalar: ValueTransition<Scalar>,
        register_value_transition_value_index_domain => TransitionValueIndex: ValueTransition<IndexValue<Value>>,
    }

    register_value_capability! { index
        register_value_absolute_index => Absolute: ValueAbsolute,
        register_value_add_index => Add: ValueAdd,
        register_value_cast_bool_index => CastBool: ValueCast<BoolTarget>,
        register_value_cast_date_time_index => CastDateTime: ValueCast<DateTimeTarget>,
        register_value_cast_duration_index => CastDuration: ValueCast<DurationTarget>,
        register_value_cast_float_index => CastFloat: ValueCast<FloatTarget>,
        register_value_cast_int_index => CastInt: ValueCast<IntTarget>,
        register_value_cast_string_index => CastString: ValueCast<StringTarget>,
        register_value_ceil_index => Ceil: ValueCeil,
        register_value_clip_index => Clip: ValueClip,
        register_value_cube_root_index => CubeRoot: ValueCubeRoot,
        register_value_divide_index => Divide: ValueDivide,
        register_value_exponential_index => Exponential: ValueExponential,
        register_value_floor_index => Floor: ValueFloor,
        register_value_int_index => Int: ValueInt,
        register_value_kind_test_index => KindTest: ValueKindTest,
        register_value_logarithm_index => Logarithm: ValueLogarithm,
        register_value_median_index => Median: ValueMedian,
        register_value_modulo_index => Modulo: ValueModulo,
        register_value_multiply_index => Multiply: ValueMultiply,
        register_value_negate_index => Negate: ValueNegate,
        register_value_power_index => Power: ValuePower,
        register_value_round_index => Round: ValueRound,
        register_value_scalar_index => Scalar: ValueScalar,
        register_value_scalar_kind_test_index => ScalarKindTest: ValueScalarKindTest,
        register_value_sign_index => Sign: ValueSign,
        register_value_square_root_index => SquareRoot: ValueSquareRoot,
        register_value_string_index => String: ValueString,
        register_value_subtract_index => Subtract: ValueSubtract,
        register_value_transition_attribute_name_index => TransitionAttributeName: ValueTransition<AttributeName>,
        register_value_transition_attribute_name_index_index => TransitionAttributeNameIndex: ValueTransition<IndexValue<AttributeName>>,
        register_value_transition_bool_index_index => TransitionBoolIndex: ValueTransition<IndexValue<bool>>,
        register_value_transition_failure_kind_value_index => TransitionFailureKindValue: ValueTransition<FailureKindValue>,
        register_value_transition_group_index_index => TransitionGroupIndex: ValueTransition<IndexValue<Group>>,
        register_value_transition_mask_index => TransitionMask: ValueTransition<Mask>,
        register_value_transition_node_index_index => TransitionNodeIndex: ValueTransition<IndexValue<NodeIndex>>,
        register_value_transition_positional_index_index => TransitionPositionalIndex: ValueTransition<IndexValue<Positional>>,
        register_value_transition_scalar_index => TransitionScalar: ValueTransition<Scalar>,
        register_value_transition_value_index_index => TransitionValueIndex: ValueTransition<IndexValue<Value>>,
    }

    register_value_capability! { indices
        register_bare_indices => BareValue,
        register_value_equality_indices => Equality,
        register_value_equivalence_indices => Equivalence,
        register_value_mode_indices => Mode,
    }

    register_value_capability! { capable_indices
        register_value_grouping_indices => Grouping: GroupKey,
        register_value_ordering_indices => Ordering: Ordering,
        register_value_sortable_indices => Sortable: Sortable,
    }

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
        registry.register_index_domain::<Group>();
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
        registry.register_value_add_index::<Value>();

        registry.register_value_multiply_domain::<Scalar>();
        registry.register_value_multiply_domain::<AttributeName>();
        registry.register_value_multiply_index::<Positional>();
        registry.register_value_multiply_index::<NodeIndex>();
        registry.register_value_multiply_index::<AttributeName>();
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
        registry.register_index_ordering::<Positional>();
        registry.register_index_ordering::<NodeIndex>();
        registry.register_index_ordering::<AttributeName>();
        registry.register_index_ordering::<Value>();
        registry.register_index_ordering::<bool>();
        registry.register_value_ordering_indices();

        registry.register_bare_value_domain::<Scalar>();
        registry.register_bare_value_domain::<Mask>();
        registry.register_bare_value_domain::<AttributeName>();
        registry.register_bare_indices();
        registry.register_bare_entity_references();
        registry.register_bare_value_domain::<FailureValue>();
        registry.register_bare_value_domain::<FailureKindValue>();

        registry.register_entity_domain::<EdgeIndex>();
        registry.register_entity_domain::<NodeIndex>();
        registry.register_entity_domain::<Group>();

        registry.register_group_key::<Value>();
        registry.register_group_key::<bool>();
        registry.register_group_key::<AttributeName>();
        registry.register_group_key::<FailureKind>();
        registry.register_group_key::<Positional>();
        registry.register_group_key::<NodeIndex>();
        registry.register_group_key::<EdgeIndex>();
        registry.register_group_key::<Group>();
        registry.register_group_key::<EdgeEndpointRole>();

        registry.register_entity_attributes::<NodeIndex>();
        registry.register_entity_attributes::<EdgeIndex>();

        registry.register_group_membership::<NodeIndex>();
        registry.register_group_membership::<EdgeIndex>();

        registry.register_index_sortable::<Value>();
        registry.register_index_sortable::<bool>();
        registry.register_index_sortable::<AttributeName>();
        registry.register_index_sortable::<Positional>();
        registry.register_index_sortable::<NodeIndex>();
        registry.register_index_sortable::<Group>();

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
        registry.register_value_string_index::<Group>();
        registry.register_value_string_index::<AttributeName>();

        registry.register_value_subtract_domain::<Scalar>();
        registry.register_value_subtract_domain::<AttributeName>();
        registry.register_value_subtract_index::<Positional>();
        registry.register_value_subtract_index::<NodeIndex>();
        registry.register_value_subtract_index::<AttributeName>();
        registry.register_value_subtract_index::<Value>();

        registry.register_value_transition_attribute_name_domain::<Scalar>();
        registry.register_value_transition_attribute_name_index::<AttributeName>();
        registry.register_value_transition_attribute_name_index::<Group>();
        registry.register_value_transition_attribute_name_index::<NodeIndex>();
        registry.register_value_transition_attribute_name_index::<Positional>();
        registry.register_value_transition_attribute_name_index::<Value>();

        registry.register_value_transition_attribute_name_index_domain::<AttributeName>();
        registry.register_value_transition_attribute_name_index_domain::<Scalar>();
        registry.register_value_transition_attribute_name_index_index::<Group>();
        registry.register_value_transition_attribute_name_index_index::<NodeIndex>();
        registry.register_value_transition_attribute_name_index_index::<Positional>();
        registry.register_value_transition_attribute_name_index_index::<Value>();

        registry.register_value_transition_bool_index_domain::<Mask>();
        registry.register_value_transition_bool_index_domain::<Scalar>();
        registry.register_value_transition_bool_index_index::<Value>();

        registry.register_value_transition_failure_kind_index_domain::<FailureKindValue>();

        registry.register_value_transition_failure_kind_value_index::<FailureKind>();

        registry.register_value_transition_group_index_domain::<AttributeName>();
        registry.register_value_transition_group_index_domain::<Scalar>();
        registry.register_value_transition_group_index_index::<AttributeName>();
        registry.register_value_transition_group_index_index::<Positional>();
        registry.register_value_transition_group_index_index::<Value>();

        registry.register_value_transition_mask_domain::<Scalar>();
        registry.register_value_transition_mask_index::<Value>();
        registry.register_value_transition_mask_index::<bool>();

        registry.register_value_transition_node_index_domain::<AttributeName>();
        registry.register_value_transition_node_index_domain::<Scalar>();
        registry.register_value_transition_node_index_index::<AttributeName>();
        registry.register_value_transition_node_index_index::<Positional>();
        registry.register_value_transition_node_index_index::<Value>();

        registry.register_value_transition_positional_index_domain::<AttributeName>();
        registry.register_value_transition_positional_index_domain::<Scalar>();
        registry.register_value_transition_positional_index_index::<AttributeName>();
        registry.register_value_transition_positional_index_index::<Group>();
        registry.register_value_transition_positional_index_index::<NodeIndex>();
        registry.register_value_transition_positional_index_index::<Value>();

        registry.register_value_transition_scalar_domain::<AttributeName>();
        registry.register_value_transition_scalar_domain::<Mask>();
        registry.register_value_transition_scalar_index::<AttributeName>();
        registry.register_value_transition_scalar_index::<Group>();
        registry.register_value_transition_scalar_index::<NodeIndex>();
        registry.register_value_transition_scalar_index::<Positional>();
        registry.register_value_transition_scalar_index::<Value>();
        registry.register_value_transition_scalar_index::<bool>();

        registry.register_value_transition_value_index_domain::<AttributeName>();
        registry.register_value_transition_value_index_domain::<Mask>();
        registry.register_value_transition_value_index_domain::<Scalar>();
        registry.register_value_transition_value_index_index::<AttributeName>();
        registry.register_value_transition_value_index_index::<Group>();
        registry.register_value_transition_value_index_index::<NodeIndex>();
        registry.register_value_transition_value_index_index::<Positional>();
        registry.register_value_transition_value_index_index::<bool>();

        registry
    }

    fn register_index_domain<I: IndexDomain>(&mut self) {
        self.index_domains.insert(DomainDescriptor::of::<I>());
    }

    fn register_value_domain<V: ValueDomain>(&mut self) {
        self.value_domains.insert(DomainDescriptor::of::<V>());
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

    fn register_group_key<I: IndexDomain>(&mut self) {
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

    fn register_group_membership<I: GroupMembership>(&mut self) {
        self.index_members
            .entry(CapabilityIdentifier::GroupMembership)
            .or_default()
            .insert(DomainDescriptor::of::<I>());
    }

    fn register_index_ordering<I: IndexDomain>(&mut self)
    where
        IndexValue<I>: ValueOrdering,
    {
        self.index_members
            .entry(CapabilityIdentifier::Ordering)
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

    fn register_value_grouping_domain<V: ValueGrouping>(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::Grouping)
            .or_default()
            .push(ValueCapabilityMember::Value(DomainDescriptor::of::<V>()));
        self.group_keys.insert(
            DomainDescriptor::of::<V>(),
            IndexDescriptor::domain::<V::KeyDomain>(),
        );
    }

    fn register_value_grouping_entity_references(&mut self) {
        self.value_members
            .entry(CapabilityIdentifier::Grouping)
            .or_default()
            .push(ValueCapabilityMember::EntityReferenceCapable(
                CapabilityIdentifier::GroupKey,
            ));
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
            (_, IndexDescriptor::Expanded { .. } | IndexDescriptor::ExpandedParent { .. }) => false,
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
            IndexDescriptor::ExpandedParent { .. } => false,
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
