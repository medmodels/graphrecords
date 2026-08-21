use super::{
    super::{
        AlignmentDescriptor, ArityDescriptor, ArityDescriptorTemplate, ArityPattern,
        CapabilityIdentifier, CapabilitySet, DomainDescriptor, EmissionKind, EmissionSpec,
        ExpressionDescriptorTemplate, ExpressionPattern, IndexDescriptor, IndexDescriptorTemplate,
        IndexPattern, LaneShapeDescriptorTemplate, OrderDescriptor, OrderDescriptorTemplate,
        OrderPattern, RetentionDescriptor, RetentionPattern, ShapePattern, ValueDescriptor,
        ValueDescriptorTemplate, ValuePattern,
    },
    witness::{
        AbsoluteCapability, AddCapability, BareValueCapability, CastBoolCapability,
        CastDateTimeCapability, CastDurationCapability, CastFloatCapability, CastIntCapability,
        CastStringCapability, CeilCapability, ClipCapability, CubeRootCapability, DivideCapability,
        EqualityCapability, EquivalenceCapability, ExponentialCapability, FloorCapability,
        GroupingCapability, IntCapability, KindTestCapability, LogarithmCapability,
        MedianCapability, ModeCapability, ModuloCapability, MultiplyCapability, NegateCapability,
        OrderingCapability, PowerCapability, RoundCapability, ScalarCapability,
        ScalarKindTestCapability, SignCapability, SortableCapability, SquareRootCapability,
        StringCapability, SubtractCapability, TransitionAttributeNameCapability,
        TransitionAttributeNameIndexCapability, TransitionBoolIndexCapability,
        TransitionFailureKindIndexCapability, TransitionFailureKindValueCapability,
        TransitionGroupIndexCapability, TransitionMaskCapability, TransitionNodeIndexCapability,
        TransitionPositionalIndexCapability, TransitionScalarCapability,
        TransitionValueIndexCapability,
    },
};
use crate::{
    Bare, BareValueDomain, Definite, EdgeEndpointRole, EntityReference, ExpandedIndex, FailureKind,
    FailureKindValue, FailureValue, IndexDomain, IndexValue, Indexed, Mask, Multiple, OrderState,
    Ordered, Positional, QueryResult, Scalar, Single, Unit, Unordered, ValueDomain,
    capabilities::{ValueEquivalence, ValueGrouping},
    element::{Arity, Dropping, ElementEmission, ElementShape, Expanding, Preserving, Retention},
    execution::EvaluationCache,
    expressions::{
        EvaluateExpression, Expression, ExpressionContext, ExpressionHandle, GroupedExpression,
    },
    index::EntityIndexDomain,
    operations::{Alignment, IndexTiebreak, Keyed, Unaligned},
    optimizer::{Estimate, PlanNode},
    sealed::Sealed,
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{AttributeName, EdgeIndex, GroupIndex, NodeIndex, Value},
};
use std::{marker::PhantomData, sync::Arc};

pub struct IndexPatternVariable<const N: usize>;

pub struct EntityPatternVariable<const N: usize>;

pub struct EntityAttributesPatternVariable<const N: usize>;

pub struct GroupMembershipPatternVariable<const N: usize>;

pub struct GroupKeyPatternVariable<const N: usize>;

pub struct ValuePatternVariable<const N: usize, M>(PhantomData<M>);

pub struct GroupingValuePatternVariable<const N: usize, K>(PhantomData<K>);

pub struct ShapePatternVariable<const N: usize>;

pub struct OrderPatternVariable<const N: usize>;

pub struct ArityPatternVariable<const N: usize>;

pub struct LanePatternVariable<const N: usize>;

pub struct GroupKeyOf<const N: usize>;

pub struct ArgumentRetention;

pub trait CapabilityMarkers {
    fn capabilities() -> Vec<CapabilityIdentifier>;

    fn argument_value_pattern() -> ValuePattern {
        let capabilities = Self::capabilities();

        if capabilities.is_empty() {
            return ValuePattern::Registered;
        }

        ValuePattern::Capable(CapabilitySet::new(capabilities))
    }
}

pub struct RegisteredOnly;

impl CapabilityMarkers for RegisteredOnly {
    fn capabilities() -> Vec<CapabilityIdentifier> {
        Vec::new()
    }
}

macro_rules! capability_marker {
    ($($marker:ty => $identifier:ident),+ $(,)?) => {
        $(
            impl CapabilityMarkers for $marker {
                fn capabilities() -> Vec<CapabilityIdentifier> {
                    vec![CapabilityIdentifier::$identifier]
                }
            }
        )+
    };
}

capability_marker!(
    BareValueCapability => BareValue,
    AbsoluteCapability => Absolute,
    AddCapability => Add,
    CastBoolCapability => CastBool,
    CastDateTimeCapability => CastDateTime,
    CastDurationCapability => CastDuration,
    CastFloatCapability => CastFloat,
    CastIntCapability => CastInt,
    CastStringCapability => CastString,
    CeilCapability => Ceil,
    ClipCapability => Clip,
    CubeRootCapability => CubeRoot,
    DivideCapability => Divide,
    EqualityCapability => Equality,
    EquivalenceCapability => Equivalence,
    ExponentialCapability => Exponential,
    FloorCapability => Floor,
    GroupingCapability => Grouping,
    IntCapability => Int,
    KindTestCapability => KindTest,
    LogarithmCapability => Logarithm,
    MedianCapability => Median,
    ModeCapability => Mode,
    ModuloCapability => Modulo,
    MultiplyCapability => Multiply,
    NegateCapability => Negate,
    OrderingCapability => Ordering,
    PowerCapability => Power,
    RoundCapability => Round,
    ScalarCapability => Scalar,
    ScalarKindTestCapability => ScalarKindTest,
    SignCapability => Sign,
    SortableCapability => Sortable,
    SquareRootCapability => SquareRoot,
    StringCapability => String,
    SubtractCapability => Subtract,
    TransitionAttributeNameCapability => TransitionAttributeName,
    TransitionAttributeNameIndexCapability => TransitionAttributeNameIndex,
    TransitionBoolIndexCapability => TransitionBoolIndex,
    TransitionFailureKindIndexCapability => TransitionFailureKindIndex,
    TransitionFailureKindValueCapability => TransitionFailureKindValue,
    TransitionGroupIndexCapability => TransitionGroupIndex,
    TransitionMaskCapability => TransitionMask,
    TransitionNodeIndexCapability => TransitionNodeIndex,
    TransitionPositionalIndexCapability => TransitionPositionalIndex,
    TransitionScalarCapability => TransitionScalar,
    TransitionValueIndexCapability => TransitionValueIndex,
);

impl<A: CapabilityMarkers, B: CapabilityMarkers> CapabilityMarkers for (A, B) {
    fn capabilities() -> Vec<CapabilityIdentifier> {
        let mut capabilities = A::capabilities();
        capabilities.extend(B::capabilities());
        capabilities
    }
}

impl<const N: usize> Clone for IndexPatternVariable<N> {
    fn clone(&self) -> Self {
        Self
    }
}

impl<const N: usize> IndexDomain for IndexPatternVariable<N> {
    type Address = i8;
    type Index<'a> = i8;
    type Owned = i8;

    fn index<'a>(_graphrecord: &'a GraphRecord, _address: &Self::Address) -> Self::Index<'a> {
        unreachable!("manifest pattern variables must never execute")
    }

    fn own_index(_index: &Self::Index<'_>) -> Self::Owned {
        unreachable!("manifest pattern variables must never execute")
    }

    fn borrow_index(_owned: &Self::Owned) -> Self::Index<'_> {
        unreachable!("manifest pattern variables must never execute")
    }

    fn resolve(
        _graphrecord: &GraphRecord,
        _owned: &Self::Owned,
        _label: &'static str,
    ) -> QueryResult<Self::Address> {
        unreachable!("manifest pattern variables must never execute")
    }
}

impl<const N: usize> IndexTiebreak for IndexPatternVariable<N> {
    fn tiebreak<T, F: Fn(&T) -> &Self::Address>(
        _graphrecord: &GraphRecord,
        _run: &mut [T],
        _address: F,
    ) {
        unreachable!("manifest pattern variables must never execute")
    }
}

impl<const N: usize> Clone for EntityPatternVariable<N> {
    fn clone(&self) -> Self {
        Self
    }
}

impl<const N: usize> IndexDomain for EntityPatternVariable<N> {
    type Address = i8;
    type Index<'a> = i8;
    type Owned = i8;

    fn index<'a>(_graphrecord: &'a GraphRecord, _address: &Self::Address) -> Self::Index<'a> {
        unreachable!("manifest pattern variables must never execute")
    }

    fn own_index(_index: &Self::Index<'_>) -> Self::Owned {
        unreachable!("manifest pattern variables must never execute")
    }

    fn borrow_index(_owned: &Self::Owned) -> Self::Index<'_> {
        unreachable!("manifest pattern variables must never execute")
    }

    fn resolve(
        _graphrecord: &GraphRecord,
        _owned: &Self::Owned,
        _label: &'static str,
    ) -> QueryResult<Self::Address> {
        unreachable!("manifest pattern variables must never execute")
    }
}

impl<const N: usize> EntityIndexDomain for EntityPatternVariable<N> {}

impl<const N: usize> Clone for EntityAttributesPatternVariable<N> {
    fn clone(&self) -> Self {
        Self
    }
}

impl<const N: usize> IndexDomain for EntityAttributesPatternVariable<N> {
    type Address = i8;
    type Index<'a> = i8;
    type Owned = i8;

    fn index<'a>(_graphrecord: &'a GraphRecord, _address: &Self::Address) -> Self::Index<'a> {
        unreachable!("manifest pattern variables must never execute")
    }

    fn own_index(_index: &Self::Index<'_>) -> Self::Owned {
        unreachable!("manifest pattern variables must never execute")
    }

    fn borrow_index(_owned: &Self::Owned) -> Self::Index<'_> {
        unreachable!("manifest pattern variables must never execute")
    }

    fn resolve(
        _graphrecord: &GraphRecord,
        _owned: &Self::Owned,
        _label: &'static str,
    ) -> QueryResult<Self::Address> {
        unreachable!("manifest pattern variables must never execute")
    }
}

impl<const N: usize> EntityIndexDomain for EntityAttributesPatternVariable<N> {}

impl<const N: usize> Clone for GroupMembershipPatternVariable<N> {
    fn clone(&self) -> Self {
        Self
    }
}

impl<const N: usize> IndexDomain for GroupMembershipPatternVariable<N> {
    type Address = i8;
    type Index<'a> = i8;
    type Owned = i8;

    fn index<'a>(_graphrecord: &'a GraphRecord, _address: &Self::Address) -> Self::Index<'a> {
        unreachable!("manifest pattern variables must never execute")
    }

    fn own_index(_index: &Self::Index<'_>) -> Self::Owned {
        unreachable!("manifest pattern variables must never execute")
    }

    fn borrow_index(_owned: &Self::Owned) -> Self::Index<'_> {
        unreachable!("manifest pattern variables must never execute")
    }

    fn resolve(
        _graphrecord: &GraphRecord,
        _owned: &Self::Owned,
        _label: &'static str,
    ) -> QueryResult<Self::Address> {
        unreachable!("manifest pattern variables must never execute")
    }
}

impl<const N: usize> EntityIndexDomain for GroupMembershipPatternVariable<N> {}

impl<const N: usize> Clone for GroupKeyPatternVariable<N> {
    fn clone(&self) -> Self {
        Self
    }
}

impl<const N: usize> IndexDomain for GroupKeyPatternVariable<N> {
    type Address = i8;
    type Index<'a> = i8;
    type Owned = i8;

    fn index<'a>(_graphrecord: &'a GraphRecord, _address: &Self::Address) -> Self::Index<'a> {
        unreachable!("manifest pattern variables must never execute")
    }

    fn own_index(_index: &Self::Index<'_>) -> Self::Owned {
        unreachable!("manifest pattern variables must never execute")
    }

    fn borrow_index(_owned: &Self::Owned) -> Self::Index<'_> {
        unreachable!("manifest pattern variables must never execute")
    }

    fn resolve(
        _graphrecord: &GraphRecord,
        _owned: &Self::Owned,
        _label: &'static str,
    ) -> QueryResult<Self::Address> {
        unreachable!("manifest pattern variables must never execute")
    }
}

impl<const N: usize, M: 'static> ValueDomain for ValuePatternVariable<N, M> {
    type Cached = ();
    type Owned = ();
    type Value<'a> = ();

    fn into_owned(_value: Self::Value<'_>) -> Self::Owned {
        unreachable!("manifest pattern variables must never execute")
    }

    fn from_owned<'a>(
        _graphrecord: &'a GraphRecord,
        _owned: &'a Self::Owned,
        _label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        unreachable!("manifest pattern variables must never execute")
    }

    fn into_cached(_value: Self::Value<'_>) -> Self::Cached {
        unreachable!("manifest pattern variables must never execute")
    }

    fn from_cached<'a>(
        _graphrecord: &'a GraphRecord,
        _cached: &'a Self::Cached,
    ) -> Self::Value<'a> {
        unreachable!("manifest pattern variables must never execute")
    }
}

impl<const N: usize, M: 'static> BareValueDomain for ValuePatternVariable<N, M> {}

impl<const N: usize, K: 'static> ValueDomain for GroupingValuePatternVariable<N, K> {
    type Cached = ();
    type Owned = ();
    type Value<'a> = ();

    fn into_owned(_value: Self::Value<'_>) -> Self::Owned {
        unreachable!("manifest pattern variables must never execute")
    }

    fn from_owned<'a>(
        _graphrecord: &'a GraphRecord,
        _owned: &'a Self::Owned,
        _label: &'static str,
    ) -> QueryResult<Self::Value<'a>> {
        unreachable!("manifest pattern variables must never execute")
    }

    fn into_cached(_value: Self::Value<'_>) -> Self::Cached {
        unreachable!("manifest pattern variables must never execute")
    }

    fn from_cached<'a>(
        _graphrecord: &'a GraphRecord,
        _cached: &'a Self::Cached,
    ) -> Self::Value<'a> {
        unreachable!("manifest pattern variables must never execute")
    }
}

impl<const N: usize> ValueEquivalence for ValuePatternVariable<N, GroupingCapability> {
    type Key<'a> = ();

    fn equivalence_key<'a>(_value: &Self::Value<'a>) -> Self::Key<'a> {
        unreachable!("manifest pattern variables must never execute")
    }
}

impl<const N: usize> ValueGrouping for ValuePatternVariable<N, GroupingCapability> {
    type KeyDomain = GroupKeyOf<N>;

    fn to_group_key(_value: &Self::Value<'_>) -> <Self::KeyDomain as IndexDomain>::Owned {
        unreachable!("manifest pattern variables must never execute")
    }
}

impl<const N: usize> ElementShape for ShapePatternVariable<N> {
    type Element<'a> = ();
    type ValueDomain = ValuePatternVariable<0, RegisteredOnly>;
}

impl<const N: usize> Sealed for OrderPatternVariable<N> {}

impl<const N: usize> OrderState for OrderPatternVariable<N> {}

impl<const N: usize> Arity for ArityPatternVariable<N> {
    type AfterDrop = Self;
    type AfterOrderedExpansion = Self;
    type AfterUnorderedExpansion = Self;
    type Container<'a, X: 'a> = X;

    fn map_elements<'a, X: 'a, Y: 'a>(
        _container: Self::Container<'a, X>,
        _function: impl Fn(X) -> Y + 'a,
    ) -> Self::Container<'a, Y> {
        unreachable!("manifest pattern variables must never execute")
    }

    fn filter_map_elements<'a, X: 'a, Y: 'a>(
        _container: Self::Container<'a, X>,
        _function: impl Fn(X) -> Option<Y> + 'a,
    ) -> <Self::AfterDrop as Arity>::Container<'a, Y> {
        unreachable!("manifest pattern variables must never execute")
    }

    fn flat_map_ordered_elements<'a, X: 'a, Y: 'a>(
        _container: Self::Container<'a, X>,
        _function: impl Fn(X) -> Vec<Y> + 'a,
    ) -> <Self::AfterOrderedExpansion as Arity>::Container<'a, Y> {
        unreachable!("manifest pattern variables must never execute")
    }

    fn flat_map_unordered_elements<'a, X: 'a, Y: 'a>(
        _container: Self::Container<'a, X>,
        _function: impl Fn(X) -> Vec<Y> + 'a,
    ) -> <Self::AfterUnorderedExpansion as Arity>::Container<'a, Y> {
        unreachable!("manifest pattern variables must never execute")
    }
}

impl<const N: usize> Clone for LanePatternVariable<N> {
    fn clone(&self) -> Self {
        Self
    }
}

impl<const N: usize> EvaluateExpression for LanePatternVariable<N> {
    type ReturnValue<'a> = ();

    fn evaluate<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::ReturnValue<'a>> {
        unreachable!("manifest pattern variables must never execute")
    }
}

impl<const N: usize> Expression for LanePatternVariable<N> {
    fn context(&self) -> &dyn ExpressionContext<Self> {
        unreachable!("manifest pattern variables must never execute")
    }

    fn as_plan_node(&self) -> &dyn PlanNode {
        unreachable!("manifest pattern variables must never execute")
    }

    fn from_context(_context: Arc<dyn ExpressionContext<Self>>) -> Self {
        unreachable!("manifest pattern variables must never execute")
    }
}

impl<const N: usize> Clone for GroupKeyOf<N> {
    fn clone(&self) -> Self {
        Self
    }
}

impl<const N: usize> IndexDomain for GroupKeyOf<N> {
    type Address = i8;
    type Index<'a> = i8;
    type Owned = i8;

    fn index<'a>(_graphrecord: &'a GraphRecord, _address: &Self::Address) -> Self::Index<'a> {
        unreachable!("manifest pattern variables must never execute")
    }

    fn own_index(_index: &Self::Index<'_>) -> Self::Owned {
        unreachable!("manifest pattern variables must never execute")
    }

    fn borrow_index(_owned: &Self::Owned) -> Self::Index<'_> {
        unreachable!("manifest pattern variables must never execute")
    }

    fn resolve(
        _graphrecord: &GraphRecord,
        _owned: &Self::Owned,
        _label: &'static str,
    ) -> QueryResult<Self::Address> {
        unreachable!("manifest pattern variables must never execute")
    }
}

impl Sealed for ArgumentRetention {}

impl ElementEmission for ArgumentRetention {
    type OutArity<C: Arity> = C;
    type Step<T> = T;

    fn map_step<T, U>(_step: Self::Step<T>, _function: impl Fn(T) -> U) -> Self::Step<U> {
        unreachable!("manifest pattern variables must never execute")
    }

    fn apply<'a, C: Arity, X: 'a, Y: 'a>(
        _container: C::Container<'a, X>,
        _function: impl Fn(X) -> Self::Step<Y> + 'a,
    ) -> <Self::OutArity<C> as Arity>::Container<'a, Y> {
        unreachable!("manifest pattern variables must never execute")
    }

    fn default_estimate(_input: Estimate) -> Estimate {
        unreachable!("manifest pattern variables must never execute")
    }
}

impl Retention for ArgumentRetention {
    type Then<R: Retention> = Self;

    fn keep<T>(_value: T) -> Self::Step<T> {
        unreachable!("manifest pattern variables must never execute")
    }

    fn absent<T, E>(_error: impl FnOnce() -> E) -> Self::Step<Result<T, E>> {
        unreachable!("manifest pattern variables must never execute")
    }

    fn map_step<T, U>(_step: Self::Step<T>, _function: impl FnOnce(T) -> U) -> Self::Step<U> {
        unreachable!("manifest pattern variables must never execute")
    }

    fn collapse<T>(_step: Self::Step<T>) -> Option<T> {
        unreachable!("manifest pattern variables must never execute")
    }

    fn and_then<R, T, U, E, F>(
        _step: Self::Step<Result<T, E>>,
        _function: F,
    ) -> <Self::Then<R> as ElementEmission>::Step<Result<U, E>>
    where
        R: Retention,
        F: FnOnce(T) -> R::Step<Result<U, E>>,
    {
        unreachable!("manifest pattern variables must never execute")
    }
}

pub trait DescribeIndex: 'static {
    fn index_pattern() -> IndexPattern;

    fn index_template() -> IndexDescriptorTemplate;
}

pub trait DescribeValue: 'static {
    fn value_pattern() -> ValuePattern;

    fn value_template() -> ValueDescriptorTemplate;
}

pub trait DescribeAlignment: Alignment {
    fn alignment_descriptor() -> AlignmentDescriptor;
}

impl<I: IndexDomain + DescribeIndex> DescribeAlignment for Keyed<I> {
    fn alignment_descriptor() -> AlignmentDescriptor {
        AlignmentDescriptor::Keyed(I::index_pattern())
    }
}

impl DescribeAlignment for Unaligned {
    fn alignment_descriptor() -> AlignmentDescriptor {
        AlignmentDescriptor::Unaligned
    }
}

pub trait DescribeRetention: Retention {
    fn retention_pattern() -> RetentionPattern;
}

impl DescribeRetention for Preserving {
    fn retention_pattern() -> RetentionPattern {
        RetentionPattern::Fixed(RetentionDescriptor::Preserving)
    }
}

impl DescribeRetention for Dropping {
    fn retention_pattern() -> RetentionPattern {
        RetentionPattern::Fixed(RetentionDescriptor::Dropping)
    }
}

pub trait DescribeShape: 'static {
    fn shape_pattern() -> ShapePattern;

    fn shape_template() -> LaneShapeDescriptorTemplate;
}

pub trait DescribeOrder: 'static {
    fn order_pattern() -> OrderPattern;

    fn order_template() -> OrderDescriptorTemplate;

    fn order_descriptor() -> OrderDescriptor;
}

pub trait DescribeArity: 'static {
    fn arity_pattern() -> ArityPattern;

    fn arity_template() -> ArityDescriptorTemplate;

    fn arity_descriptor() -> ArityDescriptor;
}

pub trait DescribeEmission: 'static {
    fn emission_spec() -> EmissionSpec;
}

pub trait DescribeExpression: 'static {
    fn expression_pattern() -> ExpressionPattern;

    fn expression_template() -> ExpressionDescriptorTemplate;
}

impl<const N: usize> DescribeIndex for IndexPatternVariable<N> {
    fn index_pattern() -> IndexPattern {
        IndexPattern::Variable(N, Box::new(IndexPattern::Registered))
    }

    fn index_template() -> IndexDescriptorTemplate {
        IndexDescriptorTemplate::Variable(N)
    }
}

impl<const N: usize> DescribeIndex for EntityPatternVariable<N> {
    fn index_pattern() -> IndexPattern {
        IndexPattern::Variable(N, Box::new(IndexPattern::Entity))
    }

    fn index_template() -> IndexDescriptorTemplate {
        IndexDescriptorTemplate::Variable(N)
    }
}

impl<const N: usize> DescribeIndex for EntityAttributesPatternVariable<N> {
    fn index_pattern() -> IndexPattern {
        IndexPattern::Variable(
            N,
            Box::new(IndexPattern::Capable(CapabilitySet::new(vec![
                CapabilityIdentifier::EntityAttributes,
            ]))),
        )
    }

    fn index_template() -> IndexDescriptorTemplate {
        IndexDescriptorTemplate::Variable(N)
    }
}

impl<const N: usize> DescribeIndex for GroupMembershipPatternVariable<N> {
    fn index_pattern() -> IndexPattern {
        IndexPattern::Variable(
            N,
            Box::new(IndexPattern::Capable(CapabilitySet::new(vec![
                CapabilityIdentifier::GroupMembership,
            ]))),
        )
    }

    fn index_template() -> IndexDescriptorTemplate {
        IndexDescriptorTemplate::Variable(N)
    }
}

impl<const N: usize> DescribeIndex for GroupKeyPatternVariable<N> {
    fn index_pattern() -> IndexPattern {
        IndexPattern::Variable(
            N,
            Box::new(IndexPattern::Capable(CapabilitySet::new(vec![
                CapabilityIdentifier::GroupKey,
            ]))),
        )
    }

    fn index_template() -> IndexDescriptorTemplate {
        IndexDescriptorTemplate::Variable(N)
    }
}

macro_rules! describe_concrete_index {
    ($($domain:ty),+ $(,)?) => {
        $(
            impl DescribeIndex for $domain {
                fn index_pattern() -> IndexPattern {
                    IndexPattern::Concrete(DomainDescriptor::of::<$domain>())
                }

                fn index_template() -> IndexDescriptorTemplate {
                    IndexDescriptorTemplate::Concrete(
                        IndexDescriptor::domain::<$domain>(),
                    )
                }
            }
        )+
    };
}

describe_concrete_index!(
    Value,
    bool,
    AttributeName,
    FailureKind,
    Positional,
    NodeIndex,
    EdgeIndex,
    GroupIndex,
    EdgeEndpointRole,
);

impl<P, C> DescribeIndex for ExpandedIndex<P, C>
where
    P: DescribeIndex + IndexDomain,
    C: DescribeIndex + IndexDomain,
{
    fn index_pattern() -> IndexPattern {
        IndexPattern::Expanded {
            parent: Box::new(P::index_pattern()),
            child: Box::new(C::index_pattern()),
        }
    }

    fn index_template() -> IndexDescriptorTemplate {
        IndexDescriptorTemplate::Expanded {
            parent: Box::new(P::index_template()),
            child: Box::new(C::index_template()),
        }
    }
}

impl<const N: usize> DescribeIndex for GroupKeyOf<N> {
    fn index_pattern() -> IndexPattern {
        unreachable!("group key templates never appear in an input pattern")
    }

    fn index_template() -> IndexDescriptorTemplate {
        IndexDescriptorTemplate::GroupKeyOf(N)
    }
}

impl<const N: usize, M: CapabilityMarkers + 'static> DescribeValue for ValuePatternVariable<N, M> {
    fn value_pattern() -> ValuePattern {
        let capabilities = M::capabilities();
        let bound = if capabilities.is_empty() {
            ValuePattern::Registered
        } else {
            ValuePattern::Capable(CapabilitySet::new(capabilities))
        };

        ValuePattern::Variable(N, Box::new(bound))
    }

    fn value_template() -> ValueDescriptorTemplate {
        ValueDescriptorTemplate::Variable(N)
    }
}

impl<const N: usize, K: DescribeIndex + IndexDomain> DescribeValue
    for GroupingValuePatternVariable<N, K>
{
    fn value_pattern() -> ValuePattern {
        ValuePattern::Variable(
            N,
            Box::new(ValuePattern::GroupKeyIs(Box::new(K::index_pattern()))),
        )
    }

    fn value_template() -> ValueDescriptorTemplate {
        ValueDescriptorTemplate::Variable(N)
    }
}

macro_rules! describe_concrete_value {
    ($($domain:ty),+ $(,)?) => {
        $(
            impl DescribeValue for $domain {
                fn value_pattern() -> ValuePattern {
                    ValuePattern::Concrete(ValueDescriptor::value::<$domain>())
                }

                fn value_template() -> ValueDescriptorTemplate {
                    ValueDescriptorTemplate::Concrete(ValueDescriptor::value::<$domain>())
                }
            }
        )+
    };
}

describe_concrete_value!(Scalar, Mask, AttributeName, FailureValue, FailureKindValue);

impl DescribeValue for Unit {
    fn value_pattern() -> ValuePattern {
        ValuePattern::Concrete(ValueDescriptor::unit())
    }

    fn value_template() -> ValueDescriptorTemplate {
        ValueDescriptorTemplate::Concrete(ValueDescriptor::unit())
    }
}

impl<I: DescribeIndex + IndexDomain> DescribeValue for IndexValue<I> {
    fn value_pattern() -> ValuePattern {
        ValuePattern::IndexValue(I::index_pattern())
    }

    fn value_template() -> ValueDescriptorTemplate {
        ValueDescriptorTemplate::Index(I::index_template())
    }
}

impl<E: DescribeIndex + EntityIndexDomain> DescribeValue for EntityReference<E> {
    fn value_pattern() -> ValuePattern {
        ValuePattern::EntityReference(E::index_pattern())
    }

    fn value_template() -> ValueDescriptorTemplate {
        ValueDescriptorTemplate::EntityReference(E::index_template())
    }
}

impl<const N: usize> DescribeShape for ShapePatternVariable<N> {
    fn shape_pattern() -> ShapePattern {
        ShapePattern::Variable(N, Box::new(ShapePattern::Any))
    }

    fn shape_template() -> LaneShapeDescriptorTemplate {
        LaneShapeDescriptorTemplate::Variable(N)
    }
}

impl<K, V> DescribeShape for Indexed<K, V>
where
    K: DescribeIndex + IndexDomain,
    V: DescribeValue + ValueDomain,
{
    fn shape_pattern() -> ShapePattern {
        ShapePattern::Indexed {
            index: K::index_pattern(),
            value: V::value_pattern(),
        }
    }

    fn shape_template() -> LaneShapeDescriptorTemplate {
        LaneShapeDescriptorTemplate::Indexed {
            index: K::index_template(),
            value: V::value_template(),
        }
    }
}

impl<V: DescribeValue + BareValueDomain> DescribeShape for Bare<V> {
    fn shape_pattern() -> ShapePattern {
        ShapePattern::Bare {
            value: V::value_pattern(),
        }
    }

    fn shape_template() -> LaneShapeDescriptorTemplate {
        LaneShapeDescriptorTemplate::Bare {
            value: V::value_template(),
        }
    }
}

impl<const N: usize> DescribeOrder for OrderPatternVariable<N> {
    fn order_pattern() -> OrderPattern {
        OrderPattern::Variable(N, Box::new(OrderPattern::Any))
    }

    fn order_template() -> OrderDescriptorTemplate {
        OrderDescriptorTemplate::Variable(N)
    }

    fn order_descriptor() -> OrderDescriptor {
        unreachable!("manifest pattern variables have no concrete order")
    }
}

impl DescribeOrder for Ordered {
    fn order_pattern() -> OrderPattern {
        OrderPattern::Ordered
    }

    fn order_template() -> OrderDescriptorTemplate {
        OrderDescriptorTemplate::Concrete(OrderDescriptor::Ordered)
    }

    fn order_descriptor() -> OrderDescriptor {
        OrderDescriptor::Ordered
    }
}

impl DescribeOrder for Unordered {
    fn order_pattern() -> OrderPattern {
        OrderPattern::Unordered
    }

    fn order_template() -> OrderDescriptorTemplate {
        OrderDescriptorTemplate::Concrete(OrderDescriptor::Unordered)
    }

    fn order_descriptor() -> OrderDescriptor {
        OrderDescriptor::Unordered
    }
}

impl<const N: usize> DescribeArity for ArityPatternVariable<N> {
    fn arity_pattern() -> ArityPattern {
        ArityPattern::Variable(N, Box::new(ArityPattern::Any))
    }

    fn arity_template() -> ArityDescriptorTemplate {
        ArityDescriptorTemplate::Variable(N)
    }

    fn arity_descriptor() -> ArityDescriptor {
        unreachable!("manifest pattern variables have no concrete arity")
    }
}

impl<O: DescribeOrder + OrderState> DescribeArity for Multiple<O> {
    fn arity_pattern() -> ArityPattern {
        ArityPattern::Multiple(O::order_pattern())
    }

    fn arity_template() -> ArityDescriptorTemplate {
        ArityDescriptorTemplate::Multiple(O::order_template())
    }

    fn arity_descriptor() -> ArityDescriptor {
        ArityDescriptor::Multiple {
            order: O::order_descriptor(),
        }
    }
}

impl DescribeArity for Single {
    fn arity_pattern() -> ArityPattern {
        ArityPattern::Single
    }

    fn arity_template() -> ArityDescriptorTemplate {
        ArityDescriptorTemplate::Single
    }

    fn arity_descriptor() -> ArityDescriptor {
        ArityDescriptor::Single
    }
}

impl DescribeArity for Definite {
    fn arity_pattern() -> ArityPattern {
        ArityPattern::Definite
    }

    fn arity_template() -> ArityDescriptorTemplate {
        ArityDescriptorTemplate::Definite
    }

    fn arity_descriptor() -> ArityDescriptor {
        ArityDescriptor::Definite
    }
}

impl DescribeEmission for ArgumentRetention {
    fn emission_spec() -> EmissionSpec {
        EmissionSpec::OfArgument
    }
}

impl DescribeEmission for Preserving {
    fn emission_spec() -> EmissionSpec {
        EmissionSpec::Fixed(EmissionKind::Preserving)
    }
}

impl DescribeEmission for Dropping {
    fn emission_spec() -> EmissionSpec {
        EmissionSpec::Fixed(EmissionKind::Dropping)
    }
}

impl DescribeEmission for Expanding<Ordered> {
    fn emission_spec() -> EmissionSpec {
        EmissionSpec::Fixed(EmissionKind::ExpandingOrdered)
    }
}

impl DescribeEmission for Expanding<Unordered> {
    fn emission_spec() -> EmissionSpec {
        EmissionSpec::Fixed(EmissionKind::ExpandingUnordered)
    }
}

impl<const N: usize> DescribeExpression for LanePatternVariable<N> {
    fn expression_pattern() -> ExpressionPattern {
        ExpressionPattern::Variable(
            N,
            Box::new(ExpressionPattern::Lane {
                shape: ShapePattern::Any,
                arity: ArityPattern::Any,
            }),
        )
    }

    fn expression_template() -> ExpressionDescriptorTemplate {
        ExpressionDescriptorTemplate::Variable(N)
    }
}

impl<S, C> DescribeExpression for ExpressionHandle<S, C>
where
    S: DescribeShape + ElementShape,
    C: DescribeArity + Arity,
{
    fn expression_pattern() -> ExpressionPattern {
        ExpressionPattern::Lane {
            shape: S::shape_pattern(),
            arity: C::arity_pattern(),
        }
    }

    fn expression_template() -> ExpressionDescriptorTemplate {
        ExpressionDescriptorTemplate::Lane {
            shape: S::shape_template(),
            arity: C::arity_template(),
        }
    }
}

impl<M, K, E> DescribeExpression for GroupedExpression<M, K, E>
where
    M: DescribeIndex + IndexDomain,
    K: DescribeIndex + IndexDomain,
    E: DescribeExpression + Expression,
{
    fn expression_pattern() -> ExpressionPattern {
        ExpressionPattern::Group {
            member: M::index_pattern(),
            key: K::index_pattern(),
            payload: Box::new(E::expression_pattern()),
        }
    }

    fn expression_template() -> ExpressionDescriptorTemplate {
        ExpressionDescriptorTemplate::Group {
            member: M::index_template(),
            key: K::index_template(),
            payload: Box::new(E::expression_template()),
        }
    }
}
