use super::describe::ArgumentRetention;
use crate::{
    Arity, BareValueDomain, ElementShape, EntityIndexDomain, Explain, FailureKind,
    FailureKindValue, IndexDomain, IndexValue, Indexed, Mask, Positional, QueryResult, Scalar,
    ValueDomain,
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
    element::Retention,
    execution::EvaluationCache,
    explain::ExplainFormatter,
    index::{EntityAttributes, GroupMembership},
    operations::{
        Alignment, ArgumentSource, EnumerableArity, IndexTiebreak, IndexedElementContainer, Lookup,
        Prepare, PreparedArity, SetSource, SourceDomain,
    },
    optimizer::{Estimate, Estimated, PlanIdentity, PlanInputs, Stats},
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{
        AttributeName, AttributeNameView, GroupAddress, GroupIndex, NodeIndex, Value, ValueView,
    },
};
use graphrecords_utils::aliases::GrHashSet;
use std::{
    cmp::Ordering,
    fmt::{self, Debug, Display, Formatter},
    hash::{Hash, Hasher},
    iter::empty,
    marker::PhantomData,
};

#[derive(Clone)]
pub struct IndexWitness;

impl IndexDomain for IndexWitness {
    type Address = i8;
    type Index<'a> = i8;
    type Owned = i8;

    fn index<'a>(_graphrecord: &'a GraphRecord, _address: &Self::Address) -> Self::Index<'a> {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn own_index(_index: &Self::Index<'_>) -> Self::Owned {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn borrow_index(_owned: &Self::Owned) -> Self::Index<'_> {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn resolve(
        _graphrecord: &GraphRecord,
        _owned: &Self::Owned,
        _label: &'static str,
    ) -> QueryResult<Self::Address> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl IndexTiebreak for IndexWitness {
    fn tiebreak<T, F: Fn(&T) -> &Self::Address>(
        _graphrecord: &GraphRecord,
        _run: &mut [T],
        _address: F,
    ) {
        unreachable!("operation manifest witnesses must never execute")
    }
}

#[derive(Clone)]
pub struct EntityWitness;

impl IndexDomain for EntityWitness {
    type Address = i8;
    type Index<'a> = i8;
    type Owned = i8;

    fn index<'a>(_graphrecord: &'a GraphRecord, _address: &Self::Address) -> Self::Index<'a> {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn own_index(_index: &Self::Index<'_>) -> Self::Owned {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn borrow_index(_owned: &Self::Owned) -> Self::Index<'_> {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn resolve(
        _graphrecord: &GraphRecord,
        _owned: &Self::Owned,
        _label: &'static str,
    ) -> QueryResult<Self::Address> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl EntityIndexDomain for EntityWitness {}

#[derive(Clone)]
pub struct EntityAttributesWitness;

impl IndexDomain for EntityAttributesWitness {
    type Address = i8;
    type Index<'a> = i8;
    type Owned = i8;

    fn index<'a>(_graphrecord: &'a GraphRecord, _address: &Self::Address) -> Self::Index<'a> {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn own_index(_index: &Self::Index<'_>) -> Self::Owned {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn borrow_index(_owned: &Self::Owned) -> Self::Index<'_> {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn resolve(
        _graphrecord: &GraphRecord,
        _owned: &Self::Owned,
        _label: &'static str,
    ) -> QueryResult<Self::Address> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl EntityIndexDomain for EntityAttributesWitness {}

impl EntityAttributes for EntityAttributesWitness {
    type AttributeAddress = i8;

    fn attribute_addresses(
        _graphrecord: &GraphRecord,
    ) -> impl Iterator<Item = Self::AttributeAddress> + '_ {
        if true {
            unreachable!("operation manifest witnesses must never execute")
        }

        empty()
    }

    fn attribute<'a>(
        _graphrecord: &'a GraphRecord,
        _address: &Self::Address,
        _attribute_address: Self::AttributeAddress,
    ) -> Option<ValueView<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn attribute_name(
        _graphrecord: &GraphRecord,
        _attribute_address: Self::AttributeAddress,
    ) -> AttributeNameView<'_> {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn resolve_attribute_address(
        _graphrecord: &GraphRecord,
        _attribute_name: &AttributeName,
    ) -> Option<Self::AttributeAddress> {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn attribute_cardinality(_stats: &Stats, _attribute: &AttributeName) -> usize {
        unreachable!("operation manifest witnesses must never execute")
    }
}

#[derive(Clone)]
pub struct GroupMembershipWitness;

impl IndexDomain for GroupMembershipWitness {
    type Address = i8;
    type Index<'a> = i8;
    type Owned = i8;

    fn index<'a>(_graphrecord: &'a GraphRecord, _address: &Self::Address) -> Self::Index<'a> {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn own_index(_index: &Self::Index<'_>) -> Self::Owned {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn borrow_index(_owned: &Self::Owned) -> Self::Index<'_> {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn resolve(
        _graphrecord: &GraphRecord,
        _owned: &Self::Owned,
        _label: &'static str,
    ) -> QueryResult<Self::Address> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl EntityIndexDomain for GroupMembershipWitness {}

impl GroupMembership for GroupMembershipWitness {
    fn addresses_in_group(
        _graphrecord: &GraphRecord,
        _group_address: GroupAddress,
    ) -> impl Iterator<Item = Self::Address> + '_ {
        if true {
            unreachable!("operation manifest witnesses must never execute")
        }

        empty()
    }

    fn group_addresses<'a>(
        _graphrecord: &'a GraphRecord,
        _address: &Self::Address,
    ) -> impl Iterator<Item = GroupAddress> + 'a {
        if true {
            unreachable!("operation manifest witnesses must never execute")
        }

        empty()
    }

    fn group_size(_stats: &Stats, _group_index: &GroupIndex) -> usize {
        unreachable!("operation manifest witnesses must never execute")
    }
}

#[derive(Clone)]
pub struct GroupMemberWitness;

impl IndexDomain for GroupMemberWitness {
    type Address = i8;
    type Index<'a> = i8;
    type Owned = i8;

    fn index<'a>(_graphrecord: &'a GraphRecord, _address: &Self::Address) -> Self::Index<'a> {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn own_index(_index: &Self::Index<'_>) -> Self::Owned {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn borrow_index(_owned: &Self::Owned) -> Self::Index<'_> {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn resolve(
        _graphrecord: &GraphRecord,
        _owned: &Self::Owned,
        _label: &'static str,
    ) -> QueryResult<Self::Address> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

#[derive(Clone)]
pub struct GroupKeyWitness;

impl IndexDomain for GroupKeyWitness {
    type Address = i8;
    type Index<'a> = i8;
    type Owned = i8;

    fn index<'a>(_graphrecord: &'a GraphRecord, _address: &Self::Address) -> Self::Index<'a> {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn own_index(_index: &Self::Index<'_>) -> Self::Owned {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn borrow_index(_owned: &Self::Owned) -> Self::Index<'_> {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn resolve(
        _graphrecord: &GraphRecord,
        _owned: &Self::Owned,
        _label: &'static str,
    ) -> QueryResult<Self::Address> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

pub struct ElementShapeWitness;

impl ElementShape for ElementShapeWitness {
    type Element<'a> = ();
    type ValueDomain = ValueWitness<ValueDomainCapability, ValueDomainOnly>;
}

pub struct ArityWitness;

impl Arity for ArityWitness {
    type AfterDrop = Self;
    type AfterOrderedExpansion = Self;
    type AfterUnorderedExpansion = Self;
    type Container<'a, X: 'a> = X;

    fn map_elements<'a, X: 'a, Y: 'a>(
        _container: Self::Container<'a, X>,
        _function: impl Fn(X) -> Y + 'a,
    ) -> Self::Container<'a, Y> {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn filter_map_elements<'a, X: 'a, Y: 'a>(
        _container: Self::Container<'a, X>,
        _function: impl Fn(X) -> Option<Y> + 'a,
    ) -> <Self::AfterDrop as Arity>::Container<'a, Y> {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn flat_map_ordered_elements<'a, X: 'a, Y: 'a>(
        _container: Self::Container<'a, X>,
        _function: impl Fn(X) -> Vec<Y> + 'a,
    ) -> <Self::AfterOrderedExpansion as Arity>::Container<'a, Y> {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn flat_map_unordered_elements<'a, X: 'a, Y: 'a>(
        _container: Self::Container<'a, X>,
        _function: impl Fn(X) -> Vec<Y> + 'a,
    ) -> <Self::AfterUnorderedExpansion as Arity>::Container<'a, Y> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

pub struct EnumerableArityWitness;

impl Arity for EnumerableArityWitness {
    type AfterDrop = Self;
    type AfterOrderedExpansion = Self;
    type AfterUnorderedExpansion = Self;
    type Container<'a, X: 'a> = X;

    fn map_elements<'a, X: 'a, Y: 'a>(
        _container: Self::Container<'a, X>,
        _function: impl Fn(X) -> Y + 'a,
    ) -> Self::Container<'a, Y> {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn filter_map_elements<'a, X: 'a, Y: 'a>(
        _container: Self::Container<'a, X>,
        _function: impl Fn(X) -> Option<Y> + 'a,
    ) -> <Self::AfterDrop as Arity>::Container<'a, Y> {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn flat_map_ordered_elements<'a, X: 'a, Y: 'a>(
        _container: Self::Container<'a, X>,
        _function: impl Fn(X) -> Vec<Y> + 'a,
    ) -> <Self::AfterOrderedExpansion as Arity>::Container<'a, Y> {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn flat_map_unordered_elements<'a, X: 'a, Y: 'a>(
        _container: Self::Container<'a, X>,
        _function: impl Fn(X) -> Vec<Y> + 'a,
    ) -> <Self::AfterUnorderedExpansion as Arity>::Container<'a, Y> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: ElementShape> PreparedArity<S> for EnumerableArityWitness {
    type Prepared<'a>
        = ()
    where
        S: 'a;

    fn prepare<'a>(
        _graphrecord: &'a GraphRecord,
        _container: Self::Container<'a, S::Element<'a>>,
    ) -> QueryResult<Self::Prepared<'a>>
    where
        S: 'a,
    {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<I: IndexDomain, V: ValueDomain> EnumerableArity<Indexed<I, V>, I> for EnumerableArityWitness {
    fn elements(
        _prepared: Self::Prepared<'_>,
    ) -> IndexedElementContainer<'_, I, V::Value<'_>, Self> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

pub struct ValueDomainOnly;
pub struct ValueDomainCapability;
pub struct BareValueCapability;
pub struct AbsoluteCapability;
pub struct AddCapability;
pub struct CastBoolCapability;
pub struct CastDateTimeCapability;
pub struct CastDurationCapability;
pub struct CastFloatCapability;
pub struct CastIntCapability;
pub struct CastStringCapability;
pub struct CeilCapability;
pub struct ClipCapability;
pub struct CubeRootCapability;
pub struct DivideCapability;
pub struct EqualityCapability;
pub struct EquivalenceCapability;
pub struct ExponentialCapability;
pub struct FloorCapability;
pub struct GroupingCapability;
pub struct IntCapability;
pub struct KindTestCapability;
pub struct LogarithmCapability;
pub struct MedianCapability;
pub struct ModeCapability;
pub struct ModuloCapability;
pub struct MultiplyCapability;
pub struct NegateCapability;
pub struct OrderingCapability;
pub struct PowerCapability;
pub struct RoundCapability;
pub struct ScalarCapability;
pub struct ScalarKindTestCapability;
pub struct SignCapability;
pub struct SortableCapability;
pub struct SquareRootCapability;
pub struct StringCapability;
pub struct SubtractCapability;
pub struct TransitionAttributeNameCapability;
pub struct TransitionAttributeNameIndexCapability;
pub struct TransitionBoolIndexCapability;
pub struct TransitionFailureKindIndexCapability;
pub struct TransitionFailureKindValueCapability;
pub struct TransitionGroupIndexCapability;
pub struct TransitionMaskCapability;
pub struct TransitionNodeIndexCapability;
pub struct TransitionPositionalIndexCapability;
pub struct TransitionScalarCapability;
pub struct TransitionValueIndexCapability;

pub struct WitnessValue<C>(PhantomData<fn() -> C>);

impl<C> Clone for WitnessValue<C> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<C> Debug for WitnessValue<C> {
    fn fmt(&self, _formatter: &mut Formatter<'_>) -> fmt::Result {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<C> Display for WitnessValue<C> {
    fn fmt(&self, _formatter: &mut Formatter<'_>) -> fmt::Result {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<C> PartialEq for WitnessValue<C> {
    fn eq(&self, _other: &Self) -> bool {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<C> Eq for WitnessValue<C> {}

impl<C> Hash for WitnessValue<C> {
    fn hash<H: Hasher>(&self, _state: &mut H) {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<C> PartialOrd for WitnessValue<C> {
    fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl EnsureSortable for WitnessValue<SortableCapability> {
    fn find_incomparable<'a>(_values: impl Iterator<Item = &'a Self>) -> Option<(usize, usize)> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl EnsureSortable for WitnessValue<(OrderingCapability, SortableCapability)> {
    fn find_incomparable<'a>(_values: impl Iterator<Item = &'a Self>) -> Option<(usize, usize)> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

pub struct ValueWitness<C, S>(PhantomData<(C, S)>);

impl<C: 'static, S: 'static> ValueDomain for ValueWitness<C, S> {
    type Cached = WitnessValue<C>;
    type Owned = WitnessValue<C>;
    type Value<'a> = WitnessValue<C>;

    fn into_owned(_value: Self::Value<'_>) -> Self::Owned {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn from_owned<'a>(
        _graphrecord: &'a GraphRecord,
        _owned: &'a Self::Owned,
        _label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn into_cached(_value: Self::Value<'_>) -> Self::Cached {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn from_cached<'a>(
        _graphrecord: &'a GraphRecord,
        _cached: &'a Self::Cached,
    ) -> Self::Value<'a> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<C: 'static> BareValueDomain for ValueWitness<C, BareValueCapability> {}

impl<S: 'static> ValueAdd for ValueWitness<AddCapability, S> {
    fn add<'a>(
        _value: Self::Value<'a>,
        _argument: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueMultiply for ValueWitness<MultiplyCapability, S> {
    fn multiply<'a>(
        _value: Self::Value<'a>,
        _argument: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueEquivalence for ValueWitness<EquivalenceCapability, S> {
    type Key<'a> = ();

    fn equivalence_key<'a>(_value: &Self::Value<'a>) -> Self::Key<'a> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueEquality for ValueWitness<OrderingCapability, S> {
    fn equal<'a>(_value: &Self::Value<'a>, _argument: &Self::Value<'a>) -> bool {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueOrdering for ValueWitness<OrderingCapability, S> {
    fn ordering<'a>(_value: &Self::Value<'a>, _argument: &Self::Value<'a>) -> Option<Ordering> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueEquality for ValueWitness<(OrderingCapability, SortableCapability), S> {
    fn equal<'a>(_value: &Self::Value<'a>, _argument: &Self::Value<'a>) -> bool {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueOrdering for ValueWitness<(OrderingCapability, SortableCapability), S> {
    fn ordering<'a>(_value: &Self::Value<'a>, _argument: &Self::Value<'a>) -> Option<Ordering> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueEquality for ValueWitness<MedianCapability, S> {
    fn equal<'a>(_value: &Self::Value<'a>, _argument: &Self::Value<'a>) -> bool {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueOrdering for ValueWitness<MedianCapability, S> {
    fn ordering<'a>(_value: &Self::Value<'a>, _argument: &Self::Value<'a>) -> Option<Ordering> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueMedian for ValueWitness<MedianCapability, S> {
    fn validate_median(_value: &Self::Value<'_>, _label: &'static str) -> QueryResult<()> {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn find_incomparable_median_values<'a, 'b: 'a>(
        _values: impl Iterator<Item = &'a Self::Value<'b>>,
    ) -> Option<(usize, usize)> {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn median<'a>(
        _lower: Self::Value<'a>,
        _upper: Option<Self::Value<'a>>,
        _label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueEquivalence for ValueWitness<ModeCapability, S> {
    type Key<'a> = ();

    fn equivalence_key<'a>(_value: &Self::Value<'a>) -> Self::Key<'a> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueMode for ValueWitness<ModeCapability, S> {}

impl<S: 'static> ValueScalar for ValueWitness<ScalarCapability, S> {
    fn into_scalar(_value: Self::Value<'_>, _label: &'static str) -> QueryResult<Value> {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn from_scalar<'a>(_original: &Self::Value<'_>, _value: Value) -> Self::Value<'a> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueAbsolute for ValueWitness<AbsoluteCapability, S> {
    fn absolute<'a>(_value: Self::Value<'a>, _label: &'static str) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueCast<BoolTarget> for ValueWitness<CastBoolCapability, S> {
    fn cast<'a>(
        _value: Self::Value<'a>,
        _target: &BoolTarget,
        _label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueCast<DateTimeTarget> for ValueWitness<CastDateTimeCapability, S> {
    fn cast<'a>(
        _value: Self::Value<'a>,
        _target: &DateTimeTarget,
        _label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueCast<DurationTarget> for ValueWitness<CastDurationCapability, S> {
    fn cast<'a>(
        _value: Self::Value<'a>,
        _target: &DurationTarget,
        _label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueCast<FloatTarget> for ValueWitness<CastFloatCapability, S> {
    fn cast<'a>(
        _value: Self::Value<'a>,
        _target: &FloatTarget,
        _label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueCast<IntTarget> for ValueWitness<CastIntCapability, S> {
    fn cast<'a>(
        _value: Self::Value<'a>,
        _target: &IntTarget,
        _label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueCast<StringTarget> for ValueWitness<CastStringCapability, S> {
    fn cast<'a>(
        _value: Self::Value<'a>,
        _target: &StringTarget,
        _label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueCeil for ValueWitness<CeilCapability, S> {
    fn ceil<'a>(_value: Self::Value<'a>, _label: &'static str) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueClip for ValueWitness<ClipCapability, S> {
    fn clip<'a>(
        _value: Self::Value<'a>,
        _lower: Self::Value<'a>,
        _upper: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueCubeRoot for ValueWitness<CubeRootCapability, S> {
    fn cube_root<'a>(
        _value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueDivide for ValueWitness<DivideCapability, S> {
    fn divide<'a>(
        _value: Self::Value<'a>,
        _argument: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueEquality for ValueWitness<EqualityCapability, S> {
    fn equal<'a>(_value: &Self::Value<'a>, _argument: &Self::Value<'a>) -> bool {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueExponential for ValueWitness<ExponentialCapability, S> {
    fn exponential<'a>(
        _value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueFloor for ValueWitness<FloorCapability, S> {
    fn floor<'a>(_value: Self::Value<'a>, _label: &'static str) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueEquivalence for ValueWitness<GroupingCapability, S> {
    type Key<'a> = ();

    fn equivalence_key<'a>(_value: &Self::Value<'a>) -> Self::Key<'a> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueGrouping for ValueWitness<GroupingCapability, S> {
    type KeyDomain = GroupKeyWitness;

    fn to_group_key(_value: &Self::Value<'_>) -> <Self::KeyDomain as IndexDomain>::Owned {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueInt for ValueWitness<IntCapability, S> {
    fn into_int(_value: Self::Value<'_>, _label: &'static str) -> QueryResult<i64> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueKindTest for ValueWitness<KindTestCapability, S> {
    fn kind(_value: &Self::Value<'_>) -> PayloadKind {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueLogarithm for ValueWitness<LogarithmCapability, S> {
    fn logarithm<'a>(
        _value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueModulo for ValueWitness<ModuloCapability, S> {
    fn modulo<'a>(
        _value: Self::Value<'a>,
        _argument: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueNegate for ValueWitness<NegateCapability, S> {
    fn negate<'a>(_value: Self::Value<'a>, _label: &'static str) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValuePower for ValueWitness<PowerCapability, S> {
    fn power<'a>(
        _value: Self::Value<'a>,
        _argument: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueRound for ValueWitness<RoundCapability, S> {
    fn round<'a>(_value: Self::Value<'a>, _label: &'static str) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueKindTest for ValueWitness<ScalarKindTestCapability, S> {
    fn kind(_value: &Self::Value<'_>) -> PayloadKind {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueScalarKindTest for ValueWitness<ScalarKindTestCapability, S> {}

impl<S: 'static> ValueSign for ValueWitness<SignCapability, S> {
    fn sign<'a>(_value: Self::Value<'a>, _label: &'static str) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueSquareRoot for ValueWitness<SquareRootCapability, S> {
    fn square_root<'a>(
        _value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueString for ValueWitness<StringCapability, S> {
    fn as_str<'a>(_value: &'a Self::Value<'_>, _label: &'static str) -> QueryResult<&'a str> {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn with_string<'a>(_original: &Self::Value<'_>, _string: String) -> Self::Value<'a> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueSubtract for ValueWitness<SubtractCapability, S> {
    fn subtract<'a>(
        _value: Self::Value<'a>,
        _argument: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueTransition<AttributeName>
    for ValueWitness<TransitionAttributeNameCapability, S>
{
    fn transition<'a>(
        _value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<AttributeName as ValueDomain>::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueTransition<IndexValue<AttributeName>>
    for ValueWitness<TransitionAttributeNameIndexCapability, S>
{
    fn transition<'a>(
        _value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<IndexValue<AttributeName> as ValueDomain>::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueTransition<IndexValue<bool>>
    for ValueWitness<TransitionBoolIndexCapability, S>
{
    fn transition<'a>(
        _value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<IndexValue<bool> as ValueDomain>::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueTransition<IndexValue<FailureKind>>
    for ValueWitness<TransitionFailureKindIndexCapability, S>
{
    fn transition<'a>(
        _value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<IndexValue<FailureKind> as ValueDomain>::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueTransition<FailureKindValue>
    for ValueWitness<TransitionFailureKindValueCapability, S>
{
    fn transition<'a>(
        _value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<FailureKindValue as ValueDomain>::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueTransition<IndexValue<GroupIndex>>
    for ValueWitness<TransitionGroupIndexCapability, S>
{
    fn transition<'a>(
        _value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<IndexValue<GroupIndex> as ValueDomain>::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueTransition<Mask> for ValueWitness<TransitionMaskCapability, S> {
    fn transition<'a>(
        _value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<Mask as ValueDomain>::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueTransition<IndexValue<NodeIndex>>
    for ValueWitness<TransitionNodeIndexCapability, S>
{
    fn transition<'a>(
        _value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<IndexValue<NodeIndex> as ValueDomain>::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueTransition<IndexValue<Positional>>
    for ValueWitness<TransitionPositionalIndexCapability, S>
{
    fn transition<'a>(
        _value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<IndexValue<Positional> as ValueDomain>::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueTransition<Scalar> for ValueWitness<TransitionScalarCapability, S> {
    fn transition<'a>(
        _value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<Scalar as ValueDomain>::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueTransition<IndexValue<Value>>
    for ValueWitness<TransitionValueIndexCapability, S>
{
    fn transition<'a>(
        _value: Self::Value<'a>,
        _label: &'static str,
    ) -> QueryResult<<IndexValue<Value> as ValueDomain>::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

type ArgumentWitnessMarker<A, V, R> = PhantomData<fn() -> (A, V, R)>;

pub struct ArgumentWitness<A, V, R = ArgumentRetention>(ArgumentWitnessMarker<A, V, R>);

impl<A, V, R> Clone for ArgumentWitness<A, V, R> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<A: Alignment, V: ValueDomain, R: 'static> SourceDomain for ArgumentWitness<A, V, R> {
    type ValueDomain = V;
}

impl<A: Alignment, V: ValueDomain, R: 'static> Prepare for ArgumentWitness<A, V, R> {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<A: Alignment, V: ValueDomain, R: 'static> Explain for ArgumentWitness<A, V, R> {
    fn describe<'a>(&'a self, _formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<A: Alignment, V: ValueDomain, R: 'static> PlanIdentity for ArgumentWitness<A, V, R> {
    fn identity_eq(&self, _other: &Self) -> bool {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn identity_hash<H: Hasher>(&self, _state: &mut H) {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<A: Alignment, V: ValueDomain, R: 'static> PlanInputs for ArgumentWitness<A, V, R> {}

impl<A: Alignment, V: ValueDomain, R: 'static> Estimated for ArgumentWitness<A, V, R> {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<A: Alignment, V: ValueDomain, R: Retention> ArgumentSource<A, V> for ArgumentWitness<A, V, R> {
    type Retention = R;

    fn lookup<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: &Self::Prepared<'a>,
        _address: &A::Address,
        _label: &'static str,
    ) -> Lookup<QueryResult<V::Value<'a>>>
    where
        Self: 'a,
    {
        unreachable!("operation manifest witnesses must never execute")
    }
}

pub struct SetSourceWitness<V>(PhantomData<fn() -> V>);

impl<V> Clone for SetSourceWitness<V> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<V: ValueDomain> Prepare for SetSourceWitness<V> {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<V: ValueDomain> Explain for SetSourceWitness<V> {
    fn describe<'a>(&'a self, _formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<V: ValueDomain> PlanIdentity for SetSourceWitness<V> {
    fn identity_eq(&self, _other: &Self) -> bool {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn identity_hash<H: Hasher>(&self, _state: &mut H) {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<V: ValueDomain> PlanInputs for SetSourceWitness<V> {}

impl<V: ValueDomain> Estimated for SetSourceWitness<V> {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<V: ValueDomain> SetSource<V> for SetSourceWitness<V> {
    fn set<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
        _label: &'static str,
    ) -> QueryResult<GrHashSet<V::Value<'a>>>
    where
        Self: 'a,
        V::Value<'a>: Eq + Hash,
    {
        unreachable!("operation manifest witnesses must never execute")
    }
}
