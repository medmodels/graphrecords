use super::describe::ArgumentRetention;
use crate::{
    Arity, BareValueDomain, ElementShape, EntityDomain, Explain, IndexDomain, Indexed, QueryResult,
    ValueDomain,
    capabilities::{
        EnsureSortable, GroupingValue, IntValue, PayloadKind, StringValue, ValueAbsolute, ValueAdd,
        ValueCast, ValueCeil, ValueClip, ValueCubeRoot, ValueDivide, ValueEquality,
        ValueEquivalence, ValueExponential, ValueFloor, ValueKindTest, ValueLogarithm, ValueMedian,
        ValueMode, ValueModulo, ValueMultiply, ValueNegate, ValueOrdering, ValuePower, ValueRound,
        ValueScalar, ValueScalarKindTest, ValueSign, ValueSquareRoot, ValueSubtract,
    },
    cast::{
        Bool as BoolTarget, DateTime as DateTimeTarget, Duration as DurationTarget,
        Float as FloatTarget, Int as IntTarget, String as StringTarget,
    },
    element::Retention,
    execution::EvaluationCache,
    explain::ExplainFormatter,
    index::{EntityAttributes, GroupKey, IndicesInGroup},
    operations::{
        Alignment, ArgumentSource, EnumerableArity, IndexedElementContainer, Lookup, Prepare,
        PreparedArity, SetSource, SourceDomain,
    },
    optimizer::{Estimate, Estimated, PlanIdentity, PlanInputs, Stats},
};
use graphrecords_core::{
    GraphRecord,
    errors::{GraphRecordError, GraphRecordResult},
    graphrecord::{AttributeMap, GraphRecordAttribute, GraphRecordValue, Group},
};
use graphrecords_utils::aliases::GrHashSet;
use std::{
    cmp::Ordering,
    fmt::{self, Debug, Display, Formatter},
    hash::{Hash, Hasher},
    marker::PhantomData,
};

#[derive(Clone)]
pub struct IndexWitness;

impl IndexDomain for IndexWitness {
    type Index<'a> = i8;
    type Owned = i8;

    fn to_owned(_index: &Self::Index<'_>) -> Self::Owned {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn from_owned(_owned: &Self::Owned) -> Self::Index<'_> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

#[derive(Clone)]
pub struct EntityWitness;

impl IndexDomain for EntityWitness {
    type Index<'a> = i8;
    type Owned = i8;

    fn to_owned(_index: &Self::Index<'_>) -> Self::Owned {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn from_owned(_owned: &Self::Owned) -> Self::Index<'_> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl EntityDomain for EntityWitness {
    fn resolve_index<'a>(
        _graphrecord: &'a GraphRecord,
        _index: &Self::Owned,
    ) -> GraphRecordResult<Self::Index<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

#[derive(Clone)]
pub struct EntityAttributesWitness;

impl IndexDomain for EntityAttributesWitness {
    type Index<'a> = i8;
    type Owned = i8;

    fn to_owned(_index: &Self::Index<'_>) -> Self::Owned {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn from_owned(_owned: &Self::Owned) -> Self::Index<'_> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl EntityDomain for EntityAttributesWitness {
    fn resolve_index<'a>(
        _graphrecord: &'a GraphRecord,
        _index: &Self::Owned,
    ) -> GraphRecordResult<Self::Index<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl EntityAttributes for EntityAttributesWitness {
    fn attributes<'a>(
        _graphrecord: &'a GraphRecord,
        _index: &Self::Index<'a>,
    ) -> Result<&'a AttributeMap, GraphRecordError> {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn attribute_cardinality(_stats: &Stats, _attribute: &GraphRecordAttribute) -> usize {
        unreachable!("operation manifest witnesses must never execute")
    }
}

#[derive(Clone)]
pub struct IndicesInGroupWitness;

impl IndexDomain for IndicesInGroupWitness {
    type Index<'a> = i8;
    type Owned = i8;

    fn to_owned(_index: &Self::Index<'_>) -> Self::Owned {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn from_owned(_owned: &Self::Owned) -> Self::Index<'_> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl EntityDomain for IndicesInGroupWitness {
    fn resolve_index<'a>(
        _graphrecord: &'a GraphRecord,
        _index: &Self::Owned,
    ) -> GraphRecordResult<Self::Index<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl IndicesInGroup for IndicesInGroupWitness {
    fn indices_in_group<'a>(
        _label: &'static str,
        _graphrecord: &'a GraphRecord,
        _group: &Group,
    ) -> QueryResult<GrHashSet<Self::Index<'a>>> {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn group_size(_stats: &Stats, _group: &Group) -> usize {
        unreachable!("operation manifest witnesses must never execute")
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SortableWitnessIndex;

impl Display for SortableWitnessIndex {
    fn fmt(&self, _formatter: &mut Formatter<'_>) -> fmt::Result {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl PartialOrd for SortableWitnessIndex {
    fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl EnsureSortable for SortableWitnessIndex {
    fn find_incomparable<'a>(_indices: impl Iterator<Item = &'a Self>) -> Option<(usize, usize)> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

#[derive(Clone)]
pub struct SortableIndexWitness;

impl IndexDomain for SortableIndexWitness {
    type Index<'a> = SortableWitnessIndex;
    type Owned = SortableWitnessIndex;

    fn to_owned(_index: &Self::Index<'_>) -> Self::Owned {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn from_owned(_owned: &Self::Owned) -> Self::Index<'_> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

#[derive(Clone)]
pub struct GroupMemberWitness;

impl IndexDomain for GroupMemberWitness {
    type Index<'a> = i8;
    type Owned = i8;

    fn to_owned(_index: &Self::Index<'_>) -> Self::Owned {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn from_owned(_owned: &Self::Owned) -> Self::Index<'_> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

#[derive(Clone)]
pub struct GroupKeyWitness;

impl IndexDomain for GroupKeyWitness {
    type Index<'a> = i8;
    type Owned = i8;

    fn to_owned(_index: &Self::Index<'_>) -> Self::Owned {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn from_owned(_owned: &Self::Owned) -> Self::Index<'_> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl GroupKey for GroupKeyWitness {
    fn resolve_key<'a>(
        _label: &'static str,
        _graphrecord: &'a GraphRecord,
        _key: &Self::Owned,
    ) -> QueryResult<Self::Index<'a>> {
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
    type Owned = WitnessValue<C>;
    type Value<'a> = WitnessValue<C>;

    fn into_owned(_value: Self::Value<'_>) -> Self::Owned {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn from_owned(_owned: &Self::Owned) -> Self::Value<'_> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<C: 'static> BareValueDomain for ValueWitness<C, BareValueCapability> {}

impl<S: 'static> ValueAdd for ValueWitness<AddCapability, S> {
    fn add<'a>(
        _label: &'static str,
        _value: Self::Value<'a>,
        _argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueMultiply for ValueWitness<MultiplyCapability, S> {
    fn multiply<'a>(
        _label: &'static str,
        _value: Self::Value<'a>,
        _argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueEquivalence for ValueWitness<EquivalenceCapability, S> {
    type Key = ();

    fn equivalence_key(_value: &Self::Value<'_>) -> Self::Key {
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
    fn validate_median(_label: &'static str, _value: &Self::Value<'_>) -> QueryResult<()> {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn find_incomparable_median_values<'a, 'b>(
        _values: impl Iterator<Item = &'a Self::Value<'b>>,
    ) -> Option<(usize, usize)>
    where
        Self::Value<'b>: 'a,
    {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn median<'a>(
        _label: &'static str,
        _lower: Self::Value<'a>,
        _upper: Option<Self::Value<'a>>,
    ) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueEquivalence for ValueWitness<ModeCapability, S> {
    type Key = ();

    fn equivalence_key(_value: &Self::Value<'_>) -> Self::Key {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueMode for ValueWitness<ModeCapability, S> {}

impl<S: 'static> ValueScalar for ValueWitness<ScalarCapability, S> {
    fn into_scalar(_label: &'static str, _value: Self::Value<'_>) -> QueryResult<GraphRecordValue> {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn from_scalar<'a>(_role: &Self::Value<'_>, _value: GraphRecordValue) -> Self::Value<'a> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueAbsolute for ValueWitness<AbsoluteCapability, S> {
    fn absolute<'a>(_label: &'static str, _value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueCast<BoolTarget> for ValueWitness<CastBoolCapability, S> {
    fn cast<'a>(
        _label: &'static str,
        _value: Self::Value<'a>,
        _target: &BoolTarget,
    ) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueCast<DateTimeTarget> for ValueWitness<CastDateTimeCapability, S> {
    fn cast<'a>(
        _label: &'static str,
        _value: Self::Value<'a>,
        _target: &DateTimeTarget,
    ) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueCast<DurationTarget> for ValueWitness<CastDurationCapability, S> {
    fn cast<'a>(
        _label: &'static str,
        _value: Self::Value<'a>,
        _target: &DurationTarget,
    ) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueCast<FloatTarget> for ValueWitness<CastFloatCapability, S> {
    fn cast<'a>(
        _label: &'static str,
        _value: Self::Value<'a>,
        _target: &FloatTarget,
    ) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueCast<IntTarget> for ValueWitness<CastIntCapability, S> {
    fn cast<'a>(
        _label: &'static str,
        _value: Self::Value<'a>,
        _target: &IntTarget,
    ) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueCast<StringTarget> for ValueWitness<CastStringCapability, S> {
    fn cast<'a>(
        _label: &'static str,
        _value: Self::Value<'a>,
        _target: &StringTarget,
    ) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueCeil for ValueWitness<CeilCapability, S> {
    fn ceil<'a>(_label: &'static str, _value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueClip for ValueWitness<ClipCapability, S> {
    fn clip<'a>(
        _label: &'static str,
        _value: Self::Value<'a>,
        _lower: Self::Value<'a>,
        _upper: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueCubeRoot for ValueWitness<CubeRootCapability, S> {
    fn cube_root<'a>(
        _label: &'static str,
        _value: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueDivide for ValueWitness<DivideCapability, S> {
    fn divide<'a>(
        _label: &'static str,
        _value: Self::Value<'a>,
        _argument: Self::Value<'a>,
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
        _label: &'static str,
        _value: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueFloor for ValueWitness<FloorCapability, S> {
    fn floor<'a>(_label: &'static str, _value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> GroupingValue for ValueWitness<GroupingCapability, S> {
    type Key = GroupKeyWitness;

    fn to_group_key(_value: &Self::Value<'_>) -> <Self::Key as IndexDomain>::Owned {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> IntValue for ValueWitness<IntCapability, S> {
    fn into_int(_label: &'static str, _value: Self::Value<'_>) -> QueryResult<i64> {
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
        _label: &'static str,
        _value: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueModulo for ValueWitness<ModuloCapability, S> {
    fn modulo<'a>(
        _label: &'static str,
        _value: Self::Value<'a>,
        _argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueNegate for ValueWitness<NegateCapability, S> {
    fn negate<'a>(_label: &'static str, _value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValuePower for ValueWitness<PowerCapability, S> {
    fn power<'a>(
        _label: &'static str,
        _value: Self::Value<'a>,
        _argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueRound for ValueWitness<RoundCapability, S> {
    fn round<'a>(_label: &'static str, _value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
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
    fn sign<'a>(_label: &'static str, _value: Self::Value<'a>) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueSquareRoot for ValueWitness<SquareRootCapability, S> {
    fn square_root<'a>(
        _label: &'static str,
        _value: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> StringValue for ValueWitness<StringCapability, S> {
    fn into_string(_label: &'static str, _value: Self::Value<'_>) -> QueryResult<String> {
        unreachable!("operation manifest witnesses must never execute")
    }

    fn from_string<'a>(_role: &Self::Value<'_>, _value: String) -> Self::Value<'a> {
        unreachable!("operation manifest witnesses must never execute")
    }
}

impl<S: 'static> ValueSubtract for ValueWitness<SubtractCapability, S> {
    fn subtract<'a>(
        _label: &'static str,
        _value: Self::Value<'a>,
        _argument: Self::Value<'a>,
    ) -> QueryResult<Self::Value<'a>> {
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
        _cache: &'a EvaluationCache<'a>,
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

    fn lookup<'a, 'prepared>(
        _prepared: &'prepared Self::Prepared<'a>,
        _address: &A::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<V::Value<'a>>>
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
        _cache: &'a EvaluationCache<'a>,
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
    fn set<'a>(_prepared: Self::Prepared<'a>) -> QueryResult<GrHashSet<V::Value<'a>>>
    where
        Self: 'a,
        V::Value<'a>: Eq + Hash,
    {
        unreachable!("operation manifest witnesses must never execute")
    }
}

macro_rules! operation_value_capability_witness {
    ($capability:ident $(<$target:ident>)?, $shape:ty) => {
        $crate::registry::ValueWitness<
            $crate::registry::operation_value_capability_marker!($capability $(<$target>)?),
            $shape
        >
    };
}

macro_rules! operation_value_capability_marker {
    (ValueAbsolute) => {
        $crate::registry::AbsoluteCapability
    };
    (ValueAdd) => {
        $crate::registry::AddCapability
    };
    (ValueCast < Bool >) => {
        $crate::registry::CastBoolCapability
    };
    (ValueCast < DateTime >) => {
        $crate::registry::CastDateTimeCapability
    };
    (ValueCast < Duration >) => {
        $crate::registry::CastDurationCapability
    };
    (ValueCast < Float >) => {
        $crate::registry::CastFloatCapability
    };
    (ValueCast < Int >) => {
        $crate::registry::CastIntCapability
    };
    (ValueCast < String >) => {
        $crate::registry::CastStringCapability
    };
    (ValueCeil) => {
        $crate::registry::CeilCapability
    };
    (ValueClip) => {
        $crate::registry::ClipCapability
    };
    (ValueCubeRoot) => {
        $crate::registry::CubeRootCapability
    };
    (ValueDivide) => {
        $crate::registry::DivideCapability
    };
    (ValueEquality) => {
        $crate::registry::EqualityCapability
    };
    (ValueEquivalence) => {
        $crate::registry::EquivalenceCapability
    };
    (ValueExponential) => {
        $crate::registry::ExponentialCapability
    };
    (ValueFloor) => {
        $crate::registry::FloorCapability
    };
    (GroupingValue) => {
        $crate::registry::GroupingCapability
    };
    (IntValue) => {
        $crate::registry::IntCapability
    };
    (ValueKindTest) => {
        $crate::registry::KindTestCapability
    };
    (ValueLogarithm) => {
        $crate::registry::LogarithmCapability
    };
    (ValueMedian) => {
        $crate::registry::MedianCapability
    };
    (ValueMode) => {
        $crate::registry::ModeCapability
    };
    (ValueModulo) => {
        $crate::registry::ModuloCapability
    };
    (ValueMultiply) => {
        $crate::registry::MultiplyCapability
    };
    (ValueNegate) => {
        $crate::registry::NegateCapability
    };
    (ValueOrdering) => {
        $crate::registry::OrderingCapability
    };
    (ValuePower) => {
        $crate::registry::PowerCapability
    };
    (ValueRound) => {
        $crate::registry::RoundCapability
    };
    (ValueScalar) => {
        $crate::registry::ScalarCapability
    };
    (ValueScalarKindTest) => {
        $crate::registry::ScalarKindTestCapability
    };
    (ValueSign) => {
        $crate::registry::SignCapability
    };
    (EnsureSortable) => {
        $crate::registry::SortableCapability
    };
    (ValueSquareRoot) => {
        $crate::registry::SquareRootCapability
    };
    (StringValue) => {
        $crate::registry::StringCapability
    };
    (ValueSubtract) => {
        $crate::registry::SubtractCapability
    };
}

pub(crate) use operation_value_capability_marker;
pub(crate) use operation_value_capability_witness;
