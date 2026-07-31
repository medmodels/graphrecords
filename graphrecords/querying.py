# ruff: noqa: D100, D101, D102, D103, D105, D107
from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Generic,
    List,
    Optional,
    Sequence,
    Tuple,
    TypeAlias,
    TypeVar,
    Union,
    overload,
)

from graphrecords._graphrecords.querying import (
    ArgumentAbsentError as ArgumentAbsentError,
)
from graphrecords._graphrecords.querying import (
    DivisionByZeroError as DivisionByZeroError,
)
from graphrecords._graphrecords.querying import (
    DuplicateExpandedChildIndexError as DuplicateExpandedChildIndexError,
)
from graphrecords._graphrecords.querying import (
    DuplicateIndexError as DuplicateIndexError,
)
from graphrecords._graphrecords.querying import (
    EmptySplitDelimiterError as EmptySplitDelimiterError,
)
from graphrecords._graphrecords.querying import (
    EvaluationCacheGraphRecordMismatchError as EvaluationCacheGraphRecordMismatchError,
)
from graphrecords._graphrecords.querying import (
    ExternalError as ExternalError,
)
from graphrecords._graphrecords.querying import (
    GraphRecordError as GraphRecordError,
)
from graphrecords._graphrecords.querying import (
    IncomparableIndicesError as IncomparableIndicesError,
)
from graphrecords._graphrecords.querying import (
    IncomparableValuesAtError as IncomparableValuesAtError,
)
from graphrecords._graphrecords.querying import (
    IncomparableValuesError as IncomparableValuesError,
)
from graphrecords._graphrecords.querying import (
    IntegerOverflowError as IntegerOverflowError,
)
from graphrecords._graphrecords.querying import (
    InvalidCastError as InvalidCastError,
)
from graphrecords._graphrecords.querying import (
    InvalidClipBoundsError as InvalidClipBoundsError,
)
from graphrecords._graphrecords.querying import (
    InvalidMedianValueError as InvalidMedianValueError,
)
from graphrecords._graphrecords.querying import (
    InvalidPaddingCharacterError as InvalidPaddingCharacterError,
)
from graphrecords._graphrecords.querying import (
    InvalidPartitionBucketArityError as InvalidPartitionBucketArityError,
)
from graphrecords._graphrecords.querying import (
    InvalidRegexPatternError as InvalidRegexPatternError,
)
from graphrecords._graphrecords.querying import (
    InvalidStandardDeviationValueError as InvalidStandardDeviationValueError,
)
from graphrecords._graphrecords.querying import (
    InvalidStringSliceError as InvalidStringSliceError,
)
from graphrecords._graphrecords.querying import (
    InvalidTransitionError as InvalidTransitionError,
)
from graphrecords._graphrecords.querying import (
    InvalidVarianceValueError as InvalidVarianceValueError,
)
from graphrecords._graphrecords.querying import (
    MissingAttributeError as MissingAttributeError,
)
from graphrecords._graphrecords.querying import (
    MissingGroupAggregateError as MissingGroupAggregateError,
)
from graphrecords._graphrecords.querying import (
    MissingTraversedAttributeError as MissingTraversedAttributeError,
)
from graphrecords._graphrecords.querying import (
    ModuloByZeroError as ModuloByZeroError,
)
from graphrecords._graphrecords.querying import (
    NegativeLengthError as NegativeLengthError,
)
from graphrecords._graphrecords.querying import (
    NegativeSquareRootError as NegativeSquareRootError,
)
from graphrecords._graphrecords.querying import (
    NoChildIndexError as NoChildIndexError,
)
from graphrecords._graphrecords.querying import (
    NonIntegerValueError as NonIntegerValueError,
)
from graphrecords._graphrecords.querying import (
    NonNumericValueError as NonNumericValueError,
)
from graphrecords._graphrecords.querying import (
    NonPositiveLogarithmError as NonPositiveLogarithmError,
)
from graphrecords._graphrecords.querying import (
    NonStringValueError as NonStringValueError,
)
from graphrecords._graphrecords.querying import (
    PyArgument,
    PyCastTarget,
    PyOperand,
    PyValueTarget,
)
from graphrecords._graphrecords.querying import (
    PyEdgeDirection as EdgeDirection,
)
from graphrecords._graphrecords.querying import (
    PyEdgeEndpointRole as EdgeEndpointRole,
)
from graphrecords._graphrecords.querying import (
    PyFailureKind as FailureKind,
)
from graphrecords._graphrecords.querying import (
    QueryError as QueryError,
)
from graphrecords._graphrecords.querying import (
    StringLengthOverflowError as StringLengthOverflowError,
)
from graphrecords._graphrecords.querying import (
    StringPaddingOverflowError as StringPaddingOverflowError,
)
from graphrecords._graphrecords.querying import (
    UnresolvedBucketFailuresError as UnresolvedBucketFailuresError,
)
from graphrecords._graphrecords.querying import (
    UnresolvedGroupKeyFailuresError as UnresolvedGroupKeyFailuresError,
)
from graphrecords._graphrecords.querying import (
    UnsupportedValueRoleError as UnsupportedValueRoleError,
)
from graphrecords.types import GraphRecordAttribute, GraphRecordValue

if TYPE_CHECKING:
    from graphrecords.graphrecord import GraphRecord


class Index: ...


K = TypeVar("K", bound=Index)
ChildType = TypeVar("ChildType", bound=Index)


class NodeIndex(Index): ...


class EdgeIndex(Index): ...


class Positional(Index): ...


class EndpointRole(Index): ...


class ValueIndex(Index): ...


class AttributeNameIndex(Index): ...


class BoolIndex(Index): ...


class FailureKindIndex(Index): ...


class Expanded(Index, Generic[K, ChildType]): ...


class Value: ...


class Unit(Value): ...


class Scalar(Value): ...


class Mask(Value): ...


class AttributeName(Value): ...


class FailureValue(Value): ...


class FailureKindValue(Value): ...


class NodeReference(Value): ...


class EdgeReference(Value): ...


V = TypeVar("V", bound=Value)


class IndexValue(Value, Generic[K]): ...


class Shape: ...


class Indexed(Shape, Generic[K, V]): ...


class Bare(Shape, Generic[V]): ...


class Container: ...


class Ordered: ...


class Unordered: ...


OrderType = TypeVar("OrderType")


class Multiple(Container, Generic[OrderType]): ...


class Single(Container): ...


class Definite(Container): ...


class Retention: ...


class Preserving(Retention): ...


class Dropping(Retention): ...


class GroupStack: ...


class Ungrouped(GroupStack): ...


S = TypeVar("S", bound=Shape)
C = TypeVar("C", bound=Container)
IndexType = TypeVar("IndexType", bound=Index)
ValueIndexType = TypeVar("ValueIndexType", bound=Index)
ContainerType = TypeVar("ContainerType", bound=Container)
PopulationContainerType = TypeVar("PopulationContainerType", bound=Container)
MemberIndexType = TypeVar("MemberIndexType", bound=Index)
KeyIndexType = TypeVar("KeyIndexType", bound=Index)
SecondMemberIndexType = TypeVar("SecondMemberIndexType", bound=Index)
SecondKeyIndexType = TypeVar("SecondKeyIndexType", bound=Index)
ThirdMemberIndexType = TypeVar("ThirdMemberIndexType", bound=Index)
ThirdKeyIndexType = TypeVar("ThirdKeyIndexType", bound=Index)
FourthMemberIndexType = TypeVar("FourthMemberIndexType", bound=Index)
FourthKeyIndexType = TypeVar("FourthKeyIndexType", bound=Index)
FifthMemberIndexType = TypeVar("FifthMemberIndexType", bound=Index)
FifthKeyIndexType = TypeVar("FifthKeyIndexType", bound=Index)
PopulationIndexType = TypeVar("PopulationIndexType", bound=Index)
GroupType = TypeVar("GroupType", bound=GroupStack)
OuterGroupType = TypeVar("OuterGroupType", bound=GroupStack)
TemplateValueType = TypeVar("TemplateValueType", bound=Value)
ExpandedValueType = TypeVar("ExpandedValueType", bound=Value)
TransitionValueType = TypeVar("TransitionValueType", bound=Value)
EntityType = TypeVar("EntityType", NodeIndex, EdgeIndex)
IntegerIndexType = TypeVar("IntegerIndexType", EdgeIndex, Positional)
SortableIndexType = TypeVar(
    "SortableIndexType",
    NodeIndex,
    EdgeIndex,
    Positional,
    ValueIndex,
    AttributeNameIndex,
    BoolIndex,
)
SortableChildIndexType = TypeVar(
    "SortableChildIndexType",
    NodeIndex,
    EdgeIndex,
    Positional,
    ValueIndex,
    AttributeNameIndex,
    BoolIndex,
)
BareValueType = TypeVar(
    "BareValueType",
    Scalar,
    Mask,
    AttributeName,
    FailureValue,
    FailureKindValue,
    NodeReference,
    EdgeReference,
    IndexValue[NodeIndex],
    IndexValue[EdgeIndex],
    IndexValue[Positional],
    IndexValue[ValueIndex],
    IndexValue[AttributeNameIndex],
    IndexValue[BoolIndex],
    IndexValue[EndpointRole],
    IndexValue[FailureKindIndex],
)
ReferenceType = TypeVar("ReferenceType", NodeReference, EdgeReference)
RetentionType = TypeVar("RetentionType", bound=Retention)
ArgumentOrderType = TypeVar("ArgumentOrderType")
ReplacementType = TypeVar("ReplacementType", covariant=True)
LiteralValueType = TypeVar("LiteralValueType", bound=Union["ScalarValue", FailureKind])
NumericValueType = TypeVar(
    "NumericValueType",
    Scalar,
    AttributeName,
    IndexValue[NodeIndex],
    IndexValue[ValueIndex],
    IndexValue[AttributeNameIndex],
)
RealNumericValueType = TypeVar("RealNumericValueType", Scalar, IndexValue[ValueIndex])
StringValueType = TypeVar(
    "StringValueType",
    Scalar,
    AttributeName,
    IndexValue[NodeIndex],
    IndexValue[ValueIndex],
    IndexValue[AttributeNameIndex],
)
StringArgumentValueType = TypeVar(
    "StringArgumentValueType",
    Scalar,
    AttributeName,
    IndexValue[NodeIndex],
    IndexValue[ValueIndex],
    IndexValue[AttributeNameIndex],
)
OldStringValueType = TypeVar(
    "OldStringValueType",
    Scalar,
    AttributeName,
    IndexValue[NodeIndex],
    IndexValue[ValueIndex],
    IndexValue[AttributeNameIndex],
)
NewStringValueType = TypeVar(
    "NewStringValueType",
    Scalar,
    AttributeName,
    IndexValue[NodeIndex],
    IndexValue[ValueIndex],
    IndexValue[AttributeNameIndex],
)
IntegerValueType = TypeVar(
    "IntegerValueType",
    Scalar,
    AttributeName,
    IndexValue[NodeIndex],
    IndexValue[EdgeIndex],
    IndexValue[Positional],
    IndexValue[ValueIndex],
    IndexValue[AttributeNameIndex],
)
InspectableValueType = TypeVar(
    "InspectableValueType",
    Scalar,
    AttributeName,
    IndexValue[NodeIndex],
    IndexValue[ValueIndex],
    IndexValue[AttributeNameIndex],
)
ScalarInspectableValueType = TypeVar(
    "ScalarInspectableValueType", Scalar, IndexValue[ValueIndex]
)
EquivalentValueType = TypeVar(
    "EquivalentValueType",
    Scalar,
    Mask,
    AttributeName,
    FailureKindValue,
    IndexValue[NodeIndex],
    IndexValue[EdgeIndex],
    IndexValue[Positional],
    IndexValue[ValueIndex],
    IndexValue[AttributeNameIndex],
    IndexValue[BoolIndex],
    IndexValue[EndpointRole],
    IndexValue[FailureKindIndex],
)
DroppedContainerType = TypeVar(
    "DroppedContainerType", Multiple[Ordered], Multiple[Unordered], Single
)
ArithmeticValueType = TypeVar(
    "ArithmeticValueType",
    Scalar,
    AttributeName,
    IndexValue[NodeIndex],
    IndexValue[EdgeIndex],
    IndexValue[Positional],
    IndexValue[ValueIndex],
    IndexValue[AttributeNameIndex],
)
EquatableValueType = TypeVar(
    "EquatableValueType",
    Scalar,
    Mask,
    AttributeName,
    FailureKindValue,
    IndexValue[NodeIndex],
    IndexValue[EdgeIndex],
    IndexValue[Positional],
    IndexValue[ValueIndex],
    IndexValue[AttributeNameIndex],
    IndexValue[BoolIndex],
    IndexValue[EndpointRole],
    IndexValue[FailureKindIndex],
)
ModeValueType = TypeVar(
    "ModeValueType",
    Scalar,
    Mask,
    AttributeName,
    IndexValue[NodeIndex],
    IndexValue[EdgeIndex],
    IndexValue[Positional],
    IndexValue[ValueIndex],
    IndexValue[AttributeNameIndex],
    IndexValue[BoolIndex],
    IndexValue[EndpointRole],
    IndexValue[FailureKindIndex],
)
MedianValueType = TypeVar("MedianValueType", Scalar, IndexValue[ValueIndex])
MultipliableValueType = TypeVar(
    "MultipliableValueType",
    Scalar,
    AttributeName,
    IndexValue[NodeIndex],
    IndexValue[EdgeIndex],
    IndexValue[Positional],
    IndexValue[ValueIndex],
    IndexValue[AttributeNameIndex],
)
OrderableValueType = TypeVar(
    "OrderableValueType",
    Scalar,
    AttributeName,
    IndexValue[NodeIndex],
    IndexValue[EdgeIndex],
    IndexValue[Positional],
    IndexValue[ValueIndex],
    IndexValue[AttributeNameIndex],
    IndexValue[BoolIndex],
    IndexValue[Expanded[NodeIndex, NodeIndex]],
    IndexValue[Expanded[NodeIndex, EdgeIndex]],
    IndexValue[Expanded[NodeIndex, Positional]],
    IndexValue[Expanded[NodeIndex, ValueIndex]],
    IndexValue[Expanded[NodeIndex, AttributeNameIndex]],
    IndexValue[Expanded[NodeIndex, BoolIndex]],
    IndexValue[Expanded[EdgeIndex, NodeIndex]],
    IndexValue[Expanded[EdgeIndex, EdgeIndex]],
    IndexValue[Expanded[EdgeIndex, Positional]],
    IndexValue[Expanded[EdgeIndex, ValueIndex]],
    IndexValue[Expanded[EdgeIndex, AttributeNameIndex]],
    IndexValue[Expanded[EdgeIndex, BoolIndex]],
    IndexValue[Expanded[Positional, NodeIndex]],
    IndexValue[Expanded[Positional, EdgeIndex]],
    IndexValue[Expanded[Positional, Positional]],
    IndexValue[Expanded[Positional, ValueIndex]],
    IndexValue[Expanded[Positional, AttributeNameIndex]],
    IndexValue[Expanded[Positional, BoolIndex]],
    IndexValue[Expanded[ValueIndex, NodeIndex]],
    IndexValue[Expanded[ValueIndex, EdgeIndex]],
    IndexValue[Expanded[ValueIndex, Positional]],
    IndexValue[Expanded[ValueIndex, ValueIndex]],
    IndexValue[Expanded[ValueIndex, AttributeNameIndex]],
    IndexValue[Expanded[ValueIndex, BoolIndex]],
    IndexValue[Expanded[AttributeNameIndex, NodeIndex]],
    IndexValue[Expanded[AttributeNameIndex, EdgeIndex]],
    IndexValue[Expanded[AttributeNameIndex, Positional]],
    IndexValue[Expanded[AttributeNameIndex, ValueIndex]],
    IndexValue[Expanded[AttributeNameIndex, AttributeNameIndex]],
    IndexValue[Expanded[AttributeNameIndex, BoolIndex]],
    IndexValue[Expanded[BoolIndex, NodeIndex]],
    IndexValue[Expanded[BoolIndex, EdgeIndex]],
    IndexValue[Expanded[BoolIndex, Positional]],
    IndexValue[Expanded[BoolIndex, ValueIndex]],
    IndexValue[Expanded[BoolIndex, AttributeNameIndex]],
    IndexValue[Expanded[BoolIndex, BoolIndex]],
)
SortKeyValueType = TypeVar(
    "SortKeyValueType",
    Scalar,
    Mask,
    AttributeName,
    NodeReference,
    EdgeReference,
    IndexValue[NodeIndex],
    IndexValue[EdgeIndex],
    IndexValue[Positional],
    IndexValue[ValueIndex],
    IndexValue[AttributeNameIndex],
    IndexValue[BoolIndex],
    IndexValue[Expanded[NodeIndex, NodeIndex]],
    IndexValue[Expanded[NodeIndex, EdgeIndex]],
    IndexValue[Expanded[NodeIndex, Positional]],
    IndexValue[Expanded[NodeIndex, ValueIndex]],
    IndexValue[Expanded[NodeIndex, AttributeNameIndex]],
    IndexValue[Expanded[NodeIndex, BoolIndex]],
    IndexValue[Expanded[EdgeIndex, NodeIndex]],
    IndexValue[Expanded[EdgeIndex, EdgeIndex]],
    IndexValue[Expanded[EdgeIndex, Positional]],
    IndexValue[Expanded[EdgeIndex, ValueIndex]],
    IndexValue[Expanded[EdgeIndex, AttributeNameIndex]],
    IndexValue[Expanded[EdgeIndex, BoolIndex]],
    IndexValue[Expanded[Positional, NodeIndex]],
    IndexValue[Expanded[Positional, EdgeIndex]],
    IndexValue[Expanded[Positional, Positional]],
    IndexValue[Expanded[Positional, ValueIndex]],
    IndexValue[Expanded[Positional, AttributeNameIndex]],
    IndexValue[Expanded[Positional, BoolIndex]],
    IndexValue[Expanded[ValueIndex, NodeIndex]],
    IndexValue[Expanded[ValueIndex, EdgeIndex]],
    IndexValue[Expanded[ValueIndex, Positional]],
    IndexValue[Expanded[ValueIndex, ValueIndex]],
    IndexValue[Expanded[ValueIndex, AttributeNameIndex]],
    IndexValue[Expanded[ValueIndex, BoolIndex]],
    IndexValue[Expanded[AttributeNameIndex, NodeIndex]],
    IndexValue[Expanded[AttributeNameIndex, EdgeIndex]],
    IndexValue[Expanded[AttributeNameIndex, Positional]],
    IndexValue[Expanded[AttributeNameIndex, ValueIndex]],
    IndexValue[Expanded[AttributeNameIndex, AttributeNameIndex]],
    IndexValue[Expanded[AttributeNameIndex, BoolIndex]],
    IndexValue[Expanded[BoolIndex, NodeIndex]],
    IndexValue[Expanded[BoolIndex, EdgeIndex]],
    IndexValue[Expanded[BoolIndex, Positional]],
    IndexValue[Expanded[BoolIndex, ValueIndex]],
    IndexValue[Expanded[BoolIndex, AttributeNameIndex]],
    IndexValue[Expanded[BoolIndex, BoolIndex]],
)
MembershipValueType = TypeVar(
    "MembershipValueType",
    Scalar,
    Mask,
    AttributeName,
    FailureKindValue,
    IndexValue[NodeIndex],
    IndexValue[EdgeIndex],
    IndexValue[Positional],
    IndexValue[ValueIndex],
    IndexValue[AttributeNameIndex],
    IndexValue[BoolIndex],
    IndexValue[EndpointRole],
    IndexValue[FailureKindIndex],
)
ScalarMembershipValueType = TypeVar(
    "ScalarMembershipValueType", Scalar, IndexValue[ValueIndex]
)
BooleanMembershipValueType = TypeVar(
    "BooleanMembershipValueType", Mask, IndexValue[BoolIndex]
)
AttributeMembershipValueType = TypeVar(
    "AttributeMembershipValueType",
    AttributeName,
    IndexValue[NodeIndex],
    IndexValue[AttributeNameIndex],
)
ClipValueType = TypeVar(
    "ClipValueType",
    Scalar,
    AttributeName,
    IndexValue[NodeIndex],
    IndexValue[EdgeIndex],
    IndexValue[Positional],
    IndexValue[ValueIndex],
    IndexValue[AttributeNameIndex],
)
ScalarClipValueType = TypeVar("ScalarClipValueType", Scalar, IndexValue[ValueIndex])
AttributeClipValueType = TypeVar(
    "AttributeClipValueType",
    AttributeName,
    IndexValue[NodeIndex],
    IndexValue[AttributeNameIndex],
)
IntegerClipValueType = TypeVar(
    "IntegerClipValueType", IndexValue[EdgeIndex], IndexValue[Positional]
)
CastableValueType = TypeVar(
    "CastableValueType",
    Scalar,
    AttributeName,
    IndexValue[NodeIndex],
    IndexValue[ValueIndex],
    IndexValue[AttributeNameIndex],
)
CastReceiverValueType = TypeVar(
    "CastReceiverValueType", bound=Value, contravariant=True
)
ScalarTransitionValueType = TypeVar(
    "ScalarTransitionValueType",
    Mask,
    AttributeName,
    IndexValue[NodeIndex],
    IndexValue[EdgeIndex],
    IndexValue[Positional],
    IndexValue[ValueIndex],
    IndexValue[AttributeNameIndex],
    IndexValue[BoolIndex],
)
ValueIndexTransitionValueType = TypeVar(
    "ValueIndexTransitionValueType",
    Scalar,
    Mask,
    AttributeName,
    IndexValue[NodeIndex],
    IndexValue[EdgeIndex],
    IndexValue[Positional],
    IndexValue[AttributeNameIndex],
    IndexValue[BoolIndex],
)
AttributeNameTransitionValueType = TypeVar(
    "AttributeNameTransitionValueType",
    Scalar,
    IndexValue[NodeIndex],
    IndexValue[EdgeIndex],
    IndexValue[Positional],
    IndexValue[ValueIndex],
    IndexValue[AttributeNameIndex],
)
NodeIndexTransitionValueType = TypeVar(
    "NodeIndexTransitionValueType",
    Scalar,
    AttributeName,
    IndexValue[EdgeIndex],
    IndexValue[Positional],
    IndexValue[ValueIndex],
    IndexValue[AttributeNameIndex],
)
AttributeNameIndexTransitionValueType = TypeVar(
    "AttributeNameIndexTransitionValueType",
    Scalar,
    AttributeName,
    IndexValue[NodeIndex],
    IndexValue[EdgeIndex],
    IndexValue[Positional],
    IndexValue[ValueIndex],
)
EdgeIndexTransitionValueType = TypeVar(
    "EdgeIndexTransitionValueType",
    Scalar,
    AttributeName,
    IndexValue[NodeIndex],
    IndexValue[Positional],
    IndexValue[ValueIndex],
    IndexValue[AttributeNameIndex],
)
PositionalTransitionValueType = TypeVar(
    "PositionalTransitionValueType",
    Scalar,
    AttributeName,
    IndexValue[NodeIndex],
    IndexValue[EdgeIndex],
    IndexValue[ValueIndex],
    IndexValue[AttributeNameIndex],
)
MaskTransitionValueType = TypeVar(
    "MaskTransitionValueType", Scalar, IndexValue[ValueIndex], IndexValue[BoolIndex]
)
BoolIndexTransitionValueType = TypeVar(
    "BoolIndexTransitionValueType", Scalar, Mask, IndexValue[ValueIndex]
)
GroupingValueType = TypeVar(
    "GroupingValueType",
    Scalar,
    Mask,
    AttributeName,
    FailureKindValue,
    NodeReference,
    EdgeReference,
    IndexValue[NodeIndex],
    IndexValue[EdgeIndex],
    IndexValue[Positional],
    IndexValue[EndpointRole],
    IndexValue[ValueIndex],
    IndexValue[AttributeNameIndex],
    IndexValue[BoolIndex],
    IndexValue[FailureKindIndex],
)


class Grouped(GroupStack, Generic[MemberIndexType, KeyIndexType, OuterGroupType]): ...


class ValueTarget(Generic[TransitionValueType]):
    Value: ClassVar[ValueTarget[Scalar]]
    ValueIndex: ClassVar[ValueTarget[IndexValue[ValueIndex]]]
    AttributeName: ClassVar[ValueTarget[AttributeName]]
    AttributeNameIndex: ClassVar[ValueTarget[IndexValue[AttributeNameIndex]]]
    NodeIndex: ClassVar[ValueTarget[IndexValue[NodeIndex]]]
    EdgeIndex: ClassVar[ValueTarget[IndexValue[EdgeIndex]]]
    PositionalIndex: ClassVar[ValueTarget[IndexValue[Positional]]]
    BoolIndex: ClassVar[ValueTarget[IndexValue[BoolIndex]]]
    Mask: ClassVar[ValueTarget[Mask]]
    FailureKind: ClassVar[ValueTarget[FailureKindValue]]
    FailureKindIndex: ClassVar[ValueTarget[IndexValue[FailureKindIndex]]]

    def __init__(self, target: PyValueTarget) -> None:
        self._target = target


ValueTarget.Value = ValueTarget(PyValueTarget.Value)
ValueTarget.ValueIndex = ValueTarget(PyValueTarget.ValueIndex)
ValueTarget.AttributeName = ValueTarget(PyValueTarget.AttributeName)
ValueTarget.AttributeNameIndex = ValueTarget(PyValueTarget.AttributeNameIndex)
ValueTarget.NodeIndex = ValueTarget(PyValueTarget.NodeIndex)
ValueTarget.EdgeIndex = ValueTarget(PyValueTarget.EdgeIndex)
ValueTarget.PositionalIndex = ValueTarget(PyValueTarget.PositionalIndex)
ValueTarget.BoolIndex = ValueTarget(PyValueTarget.BoolIndex)
ValueTarget.Mask = ValueTarget(PyValueTarget.Mask)
ValueTarget.FailureKind = ValueTarget(PyValueTarget.FailureKind)
ValueTarget.FailureKindIndex = ValueTarget(PyValueTarget.FailureKindIndex)


class CastTarget(Generic[CastReceiverValueType]):
    Bool: ClassVar[CastTarget[Union[Scalar, IndexValue[ValueIndex]]]]
    DateTime: ClassVar[CastTarget[Union[Scalar, IndexValue[ValueIndex]]]]
    Duration: ClassVar[CastTarget[Union[Scalar, IndexValue[ValueIndex]]]]
    Float: ClassVar[CastTarget[Union[Scalar, IndexValue[ValueIndex]]]]
    Int: ClassVar[
        CastTarget[
            Union[
                Scalar,
                AttributeName,
                IndexValue[ValueIndex],
                IndexValue[NodeIndex],
                IndexValue[AttributeNameIndex],
            ]
        ]
    ]
    String: ClassVar[
        CastTarget[
            Union[
                Scalar,
                AttributeName,
                IndexValue[ValueIndex],
                IndexValue[NodeIndex],
                IndexValue[AttributeNameIndex],
            ]
        ]
    ]

    def __init__(self, target: PyCastTarget) -> None:
        self._target = target


CastTarget.Bool = CastTarget(PyCastTarget.Bool)
CastTarget.DateTime = CastTarget(PyCastTarget.DateTime)
CastTarget.Duration = CastTarget(PyCastTarget.Duration)
CastTarget.Float = CastTarget(PyCastTarget.Float)
CastTarget.Int = CastTarget(PyCastTarget.Int)
CastTarget.String = CastTarget(PyCastTarget.String)

Attribute: TypeAlias = GraphRecordAttribute
ScalarValue: TypeAlias = GraphRecordValue
_BooleanValue: TypeAlias = bool


class Policy: ...


class Drop(Policy): ...


class Raise(Policy):
    @staticmethod
    def when(
        condition: Union[_BooleanValue, BareOperandArgument[Mask]],
    ) -> _RaiseWhen:
        return _RaiseWhen(condition)


class _RaiseWhen(Policy):
    def __init__(
        self, condition: Union[_BooleanValue, BareOperandArgument[Mask]]
    ) -> None:
        self._condition = condition


class Replace(Policy, Generic[ReplacementType]):
    def __init__(self, replacement: ReplacementType) -> None:
        self._replacement = replacement


class Argument(Generic[S, RetentionType]):
    _argument: PyArgument

    @classmethod
    def _from_py_argument(cls, argument: PyArgument) -> Argument[Any, Any]:
        new_argument = cls.__new__(cls)
        new_argument._argument = argument

        return new_argument


class Operand(Generic[S, C, GroupType]):
    _operand: PyOperand

    @classmethod
    def _from_py_operand(cls, operand: PyOperand) -> Operand[Any, Any, Any]:
        new_operand = cls.__new__(cls)
        new_operand._operand = operand

        return new_operand

    @staticmethod
    def _to_py_argument(
        value: Union[LiteralValueType, Operand[Any, Any, Any], Argument[Any, Any]],
    ) -> Union[PyOperand, PyArgument, LiteralValueType]:
        if isinstance(value, Operand):
            return value._operand

        if isinstance(value, Argument):
            return value._argument

        return value

    @overload
    def on_missing(
        self: Operand[Indexed[IndexType, V], Multiple[OrderType], Ungrouped],
        policy: Drop,
    ) -> Argument[Indexed[IndexType, V], Dropping]: ...

    @overload
    def on_missing(
        self: Operand[Bare[BareValueType], Single, Ungrouped], policy: Drop
    ) -> Argument[Bare[BareValueType], Dropping]: ...

    @overload
    def on_missing(
        self: Operand[Indexed[IndexType, V], Multiple[OrderType], Ungrouped],
        policy: Replace[
            Operand[Indexed[IndexType, V], Multiple[ArgumentOrderType], Ungrouped]
        ],
    ) -> Argument[Indexed[IndexType, V], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[
            Indexed[IndexType, BareValueType], Multiple[OrderType], Ungrouped
        ],
        policy: BareReplacement[BareValueType],
    ) -> Argument[Indexed[IndexType, BareValueType], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[Bare[BareValueType], Single, Ungrouped],
        policy: BareReplacement[BareValueType],
    ) -> Argument[Bare[BareValueType], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[Indexed[IndexType, Scalar], Multiple[OrderType], Ungrouped],
        policy: Replace[ScalarValue],
    ) -> Argument[Indexed[IndexType, Scalar], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[Bare[Scalar], Single, Ungrouped], policy: Replace[ScalarValue]
    ) -> Argument[Bare[Scalar], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[Indexed[IndexType, Mask], Multiple[OrderType], Ungrouped],
        policy: Replace[_BooleanValue],
    ) -> Argument[Indexed[IndexType, Mask], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[Bare[Mask], Single, Ungrouped],
        policy: Replace[_BooleanValue],
    ) -> Argument[Bare[Mask], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[
            Indexed[IndexType, AttributeName], Multiple[OrderType], Ungrouped
        ],
        policy: Replace[Attribute],
    ) -> Argument[Indexed[IndexType, AttributeName], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[Bare[AttributeName], Single, Ungrouped],
        policy: Replace[Attribute],
    ) -> Argument[Bare[AttributeName], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[
            Indexed[IndexType, FailureKindValue], Multiple[OrderType], Ungrouped
        ],
        policy: Replace[FailureKind],
    ) -> Argument[Indexed[IndexType, FailureKindValue], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[Bare[FailureKindValue], Single, Ungrouped],
        policy: Replace[FailureKind],
    ) -> Argument[Bare[FailureKindValue], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[
            Indexed[IndexType, IndexValue[NodeIndex]], Multiple[OrderType], Ungrouped
        ],
        policy: Replace[Attribute],
    ) -> Argument[Indexed[IndexType, IndexValue[NodeIndex]], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[Bare[IndexValue[NodeIndex]], Single, Ungrouped],
        policy: Replace[Attribute],
    ) -> Argument[Bare[IndexValue[NodeIndex]], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndex]], Multiple[OrderType], Ungrouped
        ],
        policy: Replace[ScalarValue],
    ) -> Argument[Indexed[IndexType, IndexValue[ValueIndex]], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[Bare[IndexValue[ValueIndex]], Single, Ungrouped],
        policy: Replace[ScalarValue],
    ) -> Argument[Bare[IndexValue[ValueIndex]], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[
            Indexed[IndexType, IndexValue[AttributeNameIndex]],
            Multiple[OrderType],
            Ungrouped,
        ],
        policy: Replace[Attribute],
    ) -> Argument[Indexed[IndexType, IndexValue[AttributeNameIndex]], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[Bare[IndexValue[AttributeNameIndex]], Single, Ungrouped],
        policy: Replace[Attribute],
    ) -> Argument[Bare[IndexValue[AttributeNameIndex]], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[
            Indexed[IndexType, IndexValue[BoolIndex]], Multiple[OrderType], Ungrouped
        ],
        policy: Replace[_BooleanValue],
    ) -> Argument[Indexed[IndexType, IndexValue[BoolIndex]], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[Bare[IndexValue[BoolIndex]], Single, Ungrouped],
        policy: Replace[_BooleanValue],
    ) -> Argument[Bare[IndexValue[BoolIndex]], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[
            Indexed[IndexType, IndexValue[IntegerIndexType]],
            Multiple[OrderType],
            Ungrouped,
        ],
        policy: Replace[int],
    ) -> Argument[Indexed[IndexType, IndexValue[IntegerIndexType]], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[Bare[IndexValue[IntegerIndexType]], Single, Ungrouped],
        policy: Replace[int],
    ) -> Argument[Bare[IndexValue[IntegerIndexType]], Preserving]: ...

    def on_missing(
        self,
        policy: Union[
            Drop,
            Replace[ScalarValue],
            Replace[FailureKind],
            Replace[Operand[Any, Any, Any]],
        ],
    ) -> Argument[Any, Any]:
        resolved = (
            self._operand.on_missing_replace(
                Operand._to_py_argument(policy._replacement)
            )
            if isinstance(policy, Replace)
            else self._operand.on_missing_drop()
        )

        return Argument._from_py_argument(resolved)

    def cache(self: Operand[S, C, GroupType]) -> Operand[S, C, GroupType]:
        return Operand._from_py_operand(self._operand.cache())

    @overload
    def filter(
        self: Operand[Indexed[IndexType, V], Definite, GroupType],
        mask: MaskArgument[IndexType, ArgumentOrderType],
    ) -> Operand[Indexed[IndexType, V], Single, GroupType]: ...

    @overload
    def filter(
        self: Operand[Indexed[IndexType, V], DroppedContainerType, GroupType],
        mask: MaskArgument[IndexType, ArgumentOrderType],
    ) -> Operand[Indexed[IndexType, V], DroppedContainerType, GroupType]: ...

    @overload
    def filter(
        self: Operand[Bare[BareValueType], Definite, GroupType],
        mask: BareMaskArgument,
    ) -> Operand[Bare[BareValueType], Single, GroupType]: ...

    @overload
    def filter(
        self: Operand[Bare[BareValueType], DroppedContainerType, GroupType],
        mask: BareMaskArgument,
    ) -> Operand[Bare[BareValueType], DroppedContainerType, GroupType]: ...

    @overload
    def filter(
        self: Operand[Bare[IndexValue[K]], Definite, GroupType],
        mask: BareMaskArgument,
    ) -> Operand[Bare[IndexValue[K]], Single, GroupType]: ...

    @overload
    def filter(
        self: Operand[Bare[IndexValue[K]], DroppedContainerType, GroupType],
        mask: BareMaskArgument,
    ) -> Operand[Bare[IndexValue[K]], DroppedContainerType, GroupType]: ...

    def filter(
        self,
        mask: Union[_BooleanValue, Operand[Any, Any, Any], Argument[Any, Any]],
    ) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(
            self._operand.filter(Operand._to_py_argument(mask))
        )

    @overload
    def and_(
        self: Operand[Indexed[IndexType, Mask], Definite, GroupType],
        other: IndexedDroppingArgument[IndexType, Mask],
    ) -> Operand[Indexed[IndexType, Mask], Single, GroupType]: ...

    @overload
    def and_(
        self: Operand[Indexed[IndexType, Mask], DroppedContainerType, GroupType],
        other: IndexedDroppingArgument[IndexType, Mask],
    ) -> Operand[Indexed[IndexType, Mask], DroppedContainerType, GroupType]: ...

    @overload
    def and_(
        self: Operand[Bare[Mask], Definite, GroupType],
        other: BareDroppingArgument[Mask],
    ) -> Operand[Bare[Mask], Single, GroupType]: ...

    @overload
    def and_(
        self: Operand[Bare[Mask], DroppedContainerType, GroupType],
        other: BareDroppingArgument[Mask],
    ) -> Operand[Bare[Mask], DroppedContainerType, GroupType]: ...

    @overload
    def and_(
        self: Operand[Indexed[IndexType, Mask], ContainerType, GroupType],
        other: Union[
            _BooleanValue, IndexedOperandArgument[IndexType, Mask, ArgumentOrderType]
        ],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def and_(
        self: Operand[Bare[Mask], ContainerType, GroupType],
        other: Union[_BooleanValue, BareOperandArgument[Mask]],
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    def and_(
        self,
        other: Union[_BooleanValue, Operand[Any, Any, Any], Argument[Any, Any]],
    ) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(
            self._operand.and_(Operand._to_py_argument(other))
        )

    @overload
    def or_(
        self: Operand[Indexed[IndexType, Mask], Definite, GroupType],
        other: IndexedDroppingArgument[IndexType, Mask],
    ) -> Operand[Indexed[IndexType, Mask], Single, GroupType]: ...

    @overload
    def or_(
        self: Operand[Indexed[IndexType, Mask], DroppedContainerType, GroupType],
        other: IndexedDroppingArgument[IndexType, Mask],
    ) -> Operand[Indexed[IndexType, Mask], DroppedContainerType, GroupType]: ...

    @overload
    def or_(
        self: Operand[Bare[Mask], Definite, GroupType],
        other: BareDroppingArgument[Mask],
    ) -> Operand[Bare[Mask], Single, GroupType]: ...

    @overload
    def or_(
        self: Operand[Bare[Mask], DroppedContainerType, GroupType],
        other: BareDroppingArgument[Mask],
    ) -> Operand[Bare[Mask], DroppedContainerType, GroupType]: ...

    @overload
    def or_(
        self: Operand[Indexed[IndexType, Mask], ContainerType, GroupType],
        other: Union[
            _BooleanValue, IndexedOperandArgument[IndexType, Mask, ArgumentOrderType]
        ],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def or_(
        self: Operand[Bare[Mask], ContainerType, GroupType],
        other: Union[_BooleanValue, BareOperandArgument[Mask]],
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    def or_(
        self,
        other: Union[_BooleanValue, Operand[Any, Any, Any], Argument[Any, Any]],
    ) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(
            self._operand.or_(Operand._to_py_argument(other))
        )

    @overload
    def xor(
        self: Operand[Indexed[IndexType, Mask], Definite, GroupType],
        other: IndexedDroppingArgument[IndexType, Mask],
    ) -> Operand[Indexed[IndexType, Mask], Single, GroupType]: ...

    @overload
    def xor(
        self: Operand[Indexed[IndexType, Mask], DroppedContainerType, GroupType],
        other: IndexedDroppingArgument[IndexType, Mask],
    ) -> Operand[Indexed[IndexType, Mask], DroppedContainerType, GroupType]: ...

    @overload
    def xor(
        self: Operand[Bare[Mask], Definite, GroupType],
        other: BareDroppingArgument[Mask],
    ) -> Operand[Bare[Mask], Single, GroupType]: ...

    @overload
    def xor(
        self: Operand[Bare[Mask], DroppedContainerType, GroupType],
        other: BareDroppingArgument[Mask],
    ) -> Operand[Bare[Mask], DroppedContainerType, GroupType]: ...

    @overload
    def xor(
        self: Operand[Indexed[IndexType, Mask], ContainerType, GroupType],
        other: Union[
            _BooleanValue, IndexedOperandArgument[IndexType, Mask, ArgumentOrderType]
        ],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def xor(
        self: Operand[Bare[Mask], ContainerType, GroupType],
        other: Union[_BooleanValue, BareOperandArgument[Mask]],
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    def xor(
        self,
        other: Union[_BooleanValue, Operand[Any, Any, Any], Argument[Any, Any]],
    ) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(
            self._operand.xor(Operand._to_py_argument(other))
        )

    @overload
    def not_(
        self: Operand[Indexed[IndexType, Mask], ContainerType, GroupType],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def not_(
        self: Operand[Bare[Mask], ContainerType, GroupType],
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    def not_(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.not_())

    @overload
    def first(
        self: Operand[Indexed[IndexType, V], Multiple[Ordered], GroupType],
    ) -> Operand[Indexed[IndexType, V], Single, GroupType]: ...

    @overload
    def first(
        self: Operand[Bare[BareValueType], Multiple[Ordered], GroupType],
    ) -> Operand[Bare[BareValueType], Single, GroupType]: ...

    def first(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.first())

    @overload
    def last(
        self: Operand[Indexed[IndexType, V], Multiple[Ordered], GroupType],
    ) -> Operand[Indexed[IndexType, V], Single, GroupType]: ...

    @overload
    def last(
        self: Operand[Bare[BareValueType], Multiple[Ordered], GroupType],
    ) -> Operand[Bare[BareValueType], Single, GroupType]: ...

    def last(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.last())

    @overload
    def reverse_order(
        self: Operand[Indexed[IndexType, V], Multiple[Ordered], GroupType],
    ) -> Operand[Indexed[IndexType, V], Multiple[Ordered], GroupType]: ...

    @overload
    def reverse_order(
        self: Operand[Bare[BareValueType], Multiple[Ordered], GroupType],
    ) -> Operand[Bare[BareValueType], Multiple[Ordered], GroupType]: ...

    def reverse_order(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.reverse_order())

    @overload
    def shuffle(
        self: Operand[Indexed[IndexType, V], Multiple[OrderType], GroupType],
    ) -> Operand[Indexed[IndexType, V], Multiple[Ordered], GroupType]: ...

    @overload
    def shuffle(
        self: Operand[Bare[BareValueType], Multiple[OrderType], GroupType],
    ) -> Operand[Bare[BareValueType], Multiple[Ordered], GroupType]: ...

    def shuffle(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.shuffle())

    @overload
    def unorder(
        self: Operand[Indexed[IndexType, V], Multiple[OrderType], GroupType],
    ) -> Operand[Indexed[IndexType, V], Multiple[Unordered], GroupType]: ...

    @overload
    def unorder(
        self: Operand[Bare[BareValueType], Multiple[OrderType], GroupType],
    ) -> Operand[Bare[BareValueType], Multiple[Unordered], GroupType]: ...

    def unorder(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.unorder())

    @overload
    def sort(
        self: Operand[
            Indexed[SortableIndexType, OrderableValueType],
            Multiple[OrderType],
            GroupType,
        ],
    ) -> Operand[
        Indexed[SortableIndexType, OrderableValueType], Multiple[Ordered], GroupType
    ]: ...

    @overload
    def sort(
        self: Operand[
            Indexed[
                Expanded[SortableIndexType, SortableChildIndexType], OrderableValueType
            ],
            Multiple[OrderType],
            GroupType,
        ],
    ) -> Operand[
        Indexed[
            Expanded[SortableIndexType, SortableChildIndexType], OrderableValueType
        ],
        Multiple[Ordered],
        GroupType,
    ]: ...

    @overload
    def sort(
        self: Operand[Bare[OrderableValueType], Multiple[OrderType], GroupType],
    ) -> Operand[Bare[OrderableValueType], Multiple[Ordered], GroupType]: ...

    def sort(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.sort())

    @overload
    def sort_by(
        self: Operand[Indexed[SortableIndexType, V], Multiple[OrderType], GroupType],
        key: IndexedAnyScalarArgument[
            SortableIndexType, SortKeyValueType, ArgumentOrderType
        ],
    ) -> Operand[Indexed[SortableIndexType, V], Multiple[Ordered], GroupType]: ...

    @overload
    def sort_by(
        self: Operand[
            Indexed[Expanded[SortableIndexType, SortableChildIndexType], V],
            Multiple[OrderType],
            GroupType,
        ],
        key: IndexedAnyScalarArgument[
            Expanded[SortableIndexType, SortableChildIndexType],
            SortKeyValueType,
            ArgumentOrderType,
        ],
    ) -> Operand[
        Indexed[Expanded[SortableIndexType, SortableChildIndexType], V],
        Multiple[Ordered],
        GroupType,
    ]: ...

    def sort_by(
        self,
        key: Union[ScalarValue, Operand[Any, Any, Any], Argument[Any, Any]],
    ) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(
            self._operand.sort_by(Operand._to_py_argument(key))
        )

    @overload
    def drop_duplicates(
        self: Operand[
            Indexed[IndexType, EquivalentValueType], Multiple[Ordered], GroupType
        ],
    ) -> Operand[
        Indexed[IndexType, EquivalentValueType], Multiple[Ordered], GroupType
    ]: ...

    @overload
    def drop_duplicates(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndexType]], Multiple[Ordered], GroupType
        ],
    ) -> Operand[
        Indexed[IndexType, IndexValue[ValueIndexType]], Multiple[Ordered], GroupType
    ]: ...

    def drop_duplicates(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.drop_duplicates())

    @overload
    def is_duplicated(
        self: Operand[
            Indexed[IndexType, EquivalentValueType], Multiple[OrderType], GroupType
        ],
    ) -> Operand[Indexed[IndexType, Mask], Multiple[OrderType], GroupType]: ...

    @overload
    def is_duplicated(
        self: Operand[Bare[EquivalentValueType], Multiple[OrderType], GroupType],
    ) -> Operand[Bare[Mask], Multiple[OrderType], GroupType]: ...

    @overload
    def is_duplicated(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndexType]],
            Multiple[OrderType],
            GroupType,
        ],
    ) -> Operand[Indexed[IndexType, Mask], Multiple[OrderType], GroupType]: ...

    @overload
    def is_duplicated(
        self: Operand[Bare[IndexValue[ValueIndexType]], Multiple[OrderType], GroupType],
    ) -> Operand[Bare[Mask], Multiple[OrderType], GroupType]: ...

    def is_duplicated(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.is_duplicated())

    @overload
    def unique(
        self: Operand[Bare[EquivalentValueType], Multiple[OrderType], GroupType],
    ) -> Operand[Bare[EquivalentValueType], Multiple[OrderType], GroupType]: ...

    @overload
    def unique(
        self: Operand[Bare[IndexValue[ValueIndexType]], Multiple[OrderType], GroupType],
    ) -> Operand[Bare[IndexValue[ValueIndexType]], Multiple[OrderType], GroupType]: ...

    def unique(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.unique())

    @overload
    def take(
        self: Operand[Indexed[IndexType, V], Multiple[Ordered], GroupType],
        elements: int,
    ) -> Operand[Indexed[IndexType, V], Multiple[Ordered], GroupType]: ...

    @overload
    def take(
        self: Operand[Bare[BareValueType], Multiple[Ordered], GroupType],
        elements: int,
    ) -> Operand[Bare[BareValueType], Multiple[Ordered], GroupType]: ...

    def take(self, elements: int) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.take(elements))

    @overload
    def is_bool(
        self: Operand[
            Indexed[IndexType, ScalarInspectableValueType], ContainerType, GroupType
        ],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def is_bool(
        self: Operand[Bare[ScalarInspectableValueType], ContainerType, GroupType],
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    def is_bool(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.is_bool())

    @overload
    def is_datetime(
        self: Operand[
            Indexed[IndexType, ScalarInspectableValueType], ContainerType, GroupType
        ],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def is_datetime(
        self: Operand[Bare[ScalarInspectableValueType], ContainerType, GroupType],
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    def is_datetime(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.is_datetime())

    @overload
    def is_duration(
        self: Operand[
            Indexed[IndexType, ScalarInspectableValueType], ContainerType, GroupType
        ],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def is_duration(
        self: Operand[Bare[ScalarInspectableValueType], ContainerType, GroupType],
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    def is_duration(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.is_duration())

    @overload
    def is_float(
        self: Operand[
            Indexed[IndexType, ScalarInspectableValueType], ContainerType, GroupType
        ],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def is_float(
        self: Operand[Bare[ScalarInspectableValueType], ContainerType, GroupType],
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    def is_float(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.is_float())

    @overload
    def is_null(
        self: Operand[
            Indexed[IndexType, ScalarInspectableValueType], ContainerType, GroupType
        ],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def is_null(
        self: Operand[Bare[ScalarInspectableValueType], ContainerType, GroupType],
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    def is_null(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.is_null())

    @overload
    def is_int(
        self: Operand[
            Indexed[IndexType, InspectableValueType], ContainerType, GroupType
        ],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def is_int(
        self: Operand[Bare[InspectableValueType], ContainerType, GroupType],
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    def is_int(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.is_int())

    @overload
    def is_string(
        self: Operand[
            Indexed[IndexType, InspectableValueType], ContainerType, GroupType
        ],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def is_string(
        self: Operand[Bare[InspectableValueType], ContainerType, GroupType],
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    def is_string(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.is_string())

    @overload
    def abs(
        self: Operand[Indexed[IndexType, NumericValueType], ContainerType, GroupType],
    ) -> Operand[Indexed[IndexType, NumericValueType], ContainerType, GroupType]: ...

    @overload
    def abs(
        self: Operand[Bare[NumericValueType], ContainerType, GroupType],
    ) -> Operand[Bare[NumericValueType], ContainerType, GroupType]: ...

    def abs(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.abs())

    @overload
    def neg(
        self: Operand[Indexed[IndexType, NumericValueType], ContainerType, GroupType],
    ) -> Operand[Indexed[IndexType, NumericValueType], ContainerType, GroupType]: ...

    @overload
    def neg(
        self: Operand[Bare[NumericValueType], ContainerType, GroupType],
    ) -> Operand[Bare[NumericValueType], ContainerType, GroupType]: ...

    def neg(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.neg())

    @overload
    def sign(
        self: Operand[Indexed[IndexType, NumericValueType], ContainerType, GroupType],
    ) -> Operand[Indexed[IndexType, NumericValueType], ContainerType, GroupType]: ...

    @overload
    def sign(
        self: Operand[Bare[NumericValueType], ContainerType, GroupType],
    ) -> Operand[Bare[NumericValueType], ContainerType, GroupType]: ...

    def sign(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.sign())

    @overload
    def ceil(
        self: Operand[
            Indexed[IndexType, RealNumericValueType], ContainerType, GroupType
        ],
    ) -> Operand[
        Indexed[IndexType, RealNumericValueType], ContainerType, GroupType
    ]: ...

    @overload
    def ceil(
        self: Operand[Bare[RealNumericValueType], ContainerType, GroupType],
    ) -> Operand[Bare[RealNumericValueType], ContainerType, GroupType]: ...

    def ceil(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.ceil())

    @overload
    def cbrt(
        self: Operand[
            Indexed[IndexType, RealNumericValueType], ContainerType, GroupType
        ],
    ) -> Operand[
        Indexed[IndexType, RealNumericValueType], ContainerType, GroupType
    ]: ...

    @overload
    def cbrt(
        self: Operand[Bare[RealNumericValueType], ContainerType, GroupType],
    ) -> Operand[Bare[RealNumericValueType], ContainerType, GroupType]: ...

    def cbrt(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.cbrt())

    @overload
    def exp(
        self: Operand[
            Indexed[IndexType, RealNumericValueType], ContainerType, GroupType
        ],
    ) -> Operand[
        Indexed[IndexType, RealNumericValueType], ContainerType, GroupType
    ]: ...

    @overload
    def exp(
        self: Operand[Bare[RealNumericValueType], ContainerType, GroupType],
    ) -> Operand[Bare[RealNumericValueType], ContainerType, GroupType]: ...

    def exp(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.exp())

    @overload
    def floor(
        self: Operand[
            Indexed[IndexType, RealNumericValueType], ContainerType, GroupType
        ],
    ) -> Operand[
        Indexed[IndexType, RealNumericValueType], ContainerType, GroupType
    ]: ...

    @overload
    def floor(
        self: Operand[Bare[RealNumericValueType], ContainerType, GroupType],
    ) -> Operand[Bare[RealNumericValueType], ContainerType, GroupType]: ...

    def floor(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.floor())

    @overload
    def log(
        self: Operand[
            Indexed[IndexType, RealNumericValueType], ContainerType, GroupType
        ],
    ) -> Operand[
        Indexed[IndexType, RealNumericValueType], ContainerType, GroupType
    ]: ...

    @overload
    def log(
        self: Operand[Bare[RealNumericValueType], ContainerType, GroupType],
    ) -> Operand[Bare[RealNumericValueType], ContainerType, GroupType]: ...

    def log(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.log())

    @overload
    def round(
        self: Operand[
            Indexed[IndexType, RealNumericValueType], ContainerType, GroupType
        ],
    ) -> Operand[
        Indexed[IndexType, RealNumericValueType], ContainerType, GroupType
    ]: ...

    @overload
    def round(
        self: Operand[Bare[RealNumericValueType], ContainerType, GroupType],
    ) -> Operand[Bare[RealNumericValueType], ContainerType, GroupType]: ...

    def round(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.round())

    @overload
    def sqrt(
        self: Operand[
            Indexed[IndexType, RealNumericValueType], ContainerType, GroupType
        ],
    ) -> Operand[
        Indexed[IndexType, RealNumericValueType], ContainerType, GroupType
    ]: ...

    @overload
    def sqrt(
        self: Operand[Bare[RealNumericValueType], ContainerType, GroupType],
    ) -> Operand[Bare[RealNumericValueType], ContainerType, GroupType]: ...

    def sqrt(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.sqrt())

    @overload
    def trim(
        self: Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType],
    ) -> Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType]: ...

    @overload
    def trim(
        self: Operand[Bare[StringValueType], ContainerType, GroupType],
    ) -> Operand[Bare[StringValueType], ContainerType, GroupType]: ...

    def trim(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.trim())

    @overload
    def trim_start(
        self: Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType],
    ) -> Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType]: ...

    @overload
    def trim_start(
        self: Operand[Bare[StringValueType], ContainerType, GroupType],
    ) -> Operand[Bare[StringValueType], ContainerType, GroupType]: ...

    def trim_start(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.trim_start())

    @overload
    def trim_end(
        self: Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType],
    ) -> Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType]: ...

    @overload
    def trim_end(
        self: Operand[Bare[StringValueType], ContainerType, GroupType],
    ) -> Operand[Bare[StringValueType], ContainerType, GroupType]: ...

    def trim_end(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.trim_end())

    @overload
    def lowercase(
        self: Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType],
    ) -> Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType]: ...

    @overload
    def lowercase(
        self: Operand[Bare[StringValueType], ContainerType, GroupType],
    ) -> Operand[Bare[StringValueType], ContainerType, GroupType]: ...

    def lowercase(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.lowercase())

    @overload
    def uppercase(
        self: Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType],
    ) -> Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType]: ...

    @overload
    def uppercase(
        self: Operand[Bare[StringValueType], ContainerType, GroupType],
    ) -> Operand[Bare[StringValueType], ContainerType, GroupType]: ...

    def uppercase(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.uppercase())

    @overload
    def reverse(
        self: Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType],
    ) -> Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType]: ...

    @overload
    def reverse(
        self: Operand[Bare[StringValueType], ContainerType, GroupType],
    ) -> Operand[Bare[StringValueType], ContainerType, GroupType]: ...

    def reverse(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.reverse())

    @overload
    def length(
        self: Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType],
    ) -> Operand[Indexed[IndexType, Scalar], ContainerType, GroupType]: ...

    @overload
    def length(
        self: Operand[Bare[StringValueType], ContainerType, GroupType],
    ) -> Operand[Bare[Scalar], ContainerType, GroupType]: ...

    def length(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.length())

    @overload
    def slice(
        self: Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType],
        start: int,
        end: int,
    ) -> Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType]: ...

    @overload
    def slice(
        self: Operand[Bare[StringValueType], ContainerType, GroupType],
        start: int,
        end: int,
    ) -> Operand[Bare[StringValueType], ContainerType, GroupType]: ...

    def slice(self, start: int, end: int) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.slice(start, end))

    @overload
    def starts_with(
        self: Operand[Indexed[IndexType, StringValueType], Definite, GroupType],
        prefix: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Operand[Indexed[IndexType, Mask], Single, GroupType]: ...

    @overload
    def starts_with(
        self: Operand[
            Indexed[IndexType, StringValueType], DroppedContainerType, GroupType
        ],
        prefix: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Operand[Indexed[IndexType, Mask], DroppedContainerType, GroupType]: ...

    @overload
    def starts_with(
        self: Operand[Bare[StringValueType], Definite, GroupType],
        prefix: BareDroppingArgument[StringArgumentValueType],
    ) -> Operand[Bare[Mask], Single, GroupType]: ...

    @overload
    def starts_with(
        self: Operand[Bare[StringValueType], DroppedContainerType, GroupType],
        prefix: BareDroppingArgument[StringArgumentValueType],
    ) -> Operand[Bare[Mask], DroppedContainerType, GroupType]: ...

    @overload
    def starts_with(
        self: Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType],
        prefix: IndexedOperandArgument[
            IndexType, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def starts_with(
        self: Operand[Bare[StringValueType], ContainerType, GroupType],
        prefix: BareOperandArgument[StringArgumentValueType],
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def starts_with(
        self: Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType],
        prefix: ScalarValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def starts_with(
        self: Operand[Bare[StringValueType], ContainerType, GroupType],
        prefix: ScalarValue,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    def starts_with(
        self,
        prefix: Union[ScalarValue, Operand[Any, Any, Any], Argument[Any, Any]],
    ) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(
            self._operand.starts_with(Operand._to_py_argument(prefix))
        )

    @overload
    def ends_with(
        self: Operand[Indexed[IndexType, StringValueType], Definite, GroupType],
        suffix: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Operand[Indexed[IndexType, Mask], Single, GroupType]: ...

    @overload
    def ends_with(
        self: Operand[
            Indexed[IndexType, StringValueType], DroppedContainerType, GroupType
        ],
        suffix: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Operand[Indexed[IndexType, Mask], DroppedContainerType, GroupType]: ...

    @overload
    def ends_with(
        self: Operand[Bare[StringValueType], Definite, GroupType],
        suffix: BareDroppingArgument[StringArgumentValueType],
    ) -> Operand[Bare[Mask], Single, GroupType]: ...

    @overload
    def ends_with(
        self: Operand[Bare[StringValueType], DroppedContainerType, GroupType],
        suffix: BareDroppingArgument[StringArgumentValueType],
    ) -> Operand[Bare[Mask], DroppedContainerType, GroupType]: ...

    @overload
    def ends_with(
        self: Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType],
        suffix: IndexedOperandArgument[
            IndexType, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def ends_with(
        self: Operand[Bare[StringValueType], ContainerType, GroupType],
        suffix: BareOperandArgument[StringArgumentValueType],
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def ends_with(
        self: Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType],
        suffix: ScalarValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def ends_with(
        self: Operand[Bare[StringValueType], ContainerType, GroupType],
        suffix: ScalarValue,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    def ends_with(
        self,
        suffix: Union[ScalarValue, Operand[Any, Any, Any], Argument[Any, Any]],
    ) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(
            self._operand.ends_with(Operand._to_py_argument(suffix))
        )

    @overload
    def contains(
        self: Operand[Indexed[IndexType, StringValueType], Definite, GroupType],
        part: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Operand[Indexed[IndexType, Mask], Single, GroupType]: ...

    @overload
    def contains(
        self: Operand[
            Indexed[IndexType, StringValueType], DroppedContainerType, GroupType
        ],
        part: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Operand[Indexed[IndexType, Mask], DroppedContainerType, GroupType]: ...

    @overload
    def contains(
        self: Operand[Bare[StringValueType], Definite, GroupType],
        part: BareDroppingArgument[StringArgumentValueType],
    ) -> Operand[Bare[Mask], Single, GroupType]: ...

    @overload
    def contains(
        self: Operand[Bare[StringValueType], DroppedContainerType, GroupType],
        part: BareDroppingArgument[StringArgumentValueType],
    ) -> Operand[Bare[Mask], DroppedContainerType, GroupType]: ...

    @overload
    def contains(
        self: Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType],
        part: IndexedOperandArgument[
            IndexType, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def contains(
        self: Operand[Bare[StringValueType], ContainerType, GroupType],
        part: BareOperandArgument[StringArgumentValueType],
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def contains(
        self: Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType],
        part: ScalarValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def contains(
        self: Operand[Bare[StringValueType], ContainerType, GroupType],
        part: ScalarValue,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    def contains(
        self,
        part: Union[ScalarValue, Operand[Any, Any, Any], Argument[Any, Any]],
    ) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(
            self._operand.contains(Operand._to_py_argument(part))
        )

    @overload
    def matches(
        self: Operand[Indexed[IndexType, StringValueType], Definite, GroupType],
        pattern: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Operand[Indexed[IndexType, Mask], Single, GroupType]: ...

    @overload
    def matches(
        self: Operand[
            Indexed[IndexType, StringValueType], DroppedContainerType, GroupType
        ],
        pattern: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Operand[Indexed[IndexType, Mask], DroppedContainerType, GroupType]: ...

    @overload
    def matches(
        self: Operand[Bare[StringValueType], Definite, GroupType],
        pattern: BareDroppingArgument[StringArgumentValueType],
    ) -> Operand[Bare[Mask], Single, GroupType]: ...

    @overload
    def matches(
        self: Operand[Bare[StringValueType], DroppedContainerType, GroupType],
        pattern: BareDroppingArgument[StringArgumentValueType],
    ) -> Operand[Bare[Mask], DroppedContainerType, GroupType]: ...

    @overload
    def matches(
        self: Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType],
        pattern: IndexedOperandArgument[
            IndexType, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def matches(
        self: Operand[Bare[StringValueType], ContainerType, GroupType],
        pattern: BareOperandArgument[StringArgumentValueType],
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def matches(
        self: Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType],
        pattern: ScalarValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def matches(
        self: Operand[Bare[StringValueType], ContainerType, GroupType],
        pattern: ScalarValue,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    def matches(
        self,
        pattern: Union[ScalarValue, Operand[Any, Any, Any], Argument[Any, Any]],
    ) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(
            self._operand.matches(Operand._to_py_argument(pattern))
        )

    @overload
    def strip_prefix(
        self: Operand[Indexed[IndexType, StringValueType], Definite, GroupType],
        prefix: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Operand[Indexed[IndexType, StringValueType], Single, GroupType]: ...

    @overload
    def strip_prefix(
        self: Operand[
            Indexed[IndexType, StringValueType], DroppedContainerType, GroupType
        ],
        prefix: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Operand[
        Indexed[IndexType, StringValueType], DroppedContainerType, GroupType
    ]: ...

    @overload
    def strip_prefix(
        self: Operand[Bare[StringValueType], Definite, GroupType],
        prefix: BareDroppingArgument[StringArgumentValueType],
    ) -> Operand[Bare[StringValueType], Single, GroupType]: ...

    @overload
    def strip_prefix(
        self: Operand[Bare[StringValueType], DroppedContainerType, GroupType],
        prefix: BareDroppingArgument[StringArgumentValueType],
    ) -> Operand[Bare[StringValueType], DroppedContainerType, GroupType]: ...

    @overload
    def strip_prefix(
        self: Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType],
        prefix: IndexedOperandArgument[
            IndexType, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType]: ...

    @overload
    def strip_prefix(
        self: Operand[Bare[StringValueType], ContainerType, GroupType],
        prefix: BareOperandArgument[StringArgumentValueType],
    ) -> Operand[Bare[StringValueType], ContainerType, GroupType]: ...

    @overload
    def strip_prefix(
        self: Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType],
        prefix: ScalarValue,
    ) -> Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType]: ...

    @overload
    def strip_prefix(
        self: Operand[Bare[StringValueType], ContainerType, GroupType],
        prefix: ScalarValue,
    ) -> Operand[Bare[StringValueType], ContainerType, GroupType]: ...

    def strip_prefix(
        self,
        prefix: Union[ScalarValue, Operand[Any, Any, Any], Argument[Any, Any]],
    ) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(
            self._operand.strip_prefix(Operand._to_py_argument(prefix))
        )

    @overload
    def strip_suffix(
        self: Operand[Indexed[IndexType, StringValueType], Definite, GroupType],
        suffix: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Operand[Indexed[IndexType, StringValueType], Single, GroupType]: ...

    @overload
    def strip_suffix(
        self: Operand[
            Indexed[IndexType, StringValueType], DroppedContainerType, GroupType
        ],
        suffix: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Operand[
        Indexed[IndexType, StringValueType], DroppedContainerType, GroupType
    ]: ...

    @overload
    def strip_suffix(
        self: Operand[Bare[StringValueType], Definite, GroupType],
        suffix: BareDroppingArgument[StringArgumentValueType],
    ) -> Operand[Bare[StringValueType], Single, GroupType]: ...

    @overload
    def strip_suffix(
        self: Operand[Bare[StringValueType], DroppedContainerType, GroupType],
        suffix: BareDroppingArgument[StringArgumentValueType],
    ) -> Operand[Bare[StringValueType], DroppedContainerType, GroupType]: ...

    @overload
    def strip_suffix(
        self: Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType],
        suffix: IndexedOperandArgument[
            IndexType, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType]: ...

    @overload
    def strip_suffix(
        self: Operand[Bare[StringValueType], ContainerType, GroupType],
        suffix: BareOperandArgument[StringArgumentValueType],
    ) -> Operand[Bare[StringValueType], ContainerType, GroupType]: ...

    @overload
    def strip_suffix(
        self: Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType],
        suffix: ScalarValue,
    ) -> Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType]: ...

    @overload
    def strip_suffix(
        self: Operand[Bare[StringValueType], ContainerType, GroupType],
        suffix: ScalarValue,
    ) -> Operand[Bare[StringValueType], ContainerType, GroupType]: ...

    def strip_suffix(
        self,
        suffix: Union[ScalarValue, Operand[Any, Any, Any], Argument[Any, Any]],
    ) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(
            self._operand.strip_suffix(Operand._to_py_argument(suffix))
        )

    @overload
    def replace(
        self: Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType],
        old: IndexedStringArgument[IndexType, OldStringValueType, ArgumentOrderType],
        new: IndexedStringArgument[IndexType, NewStringValueType, ArgumentOrderType],
    ) -> Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType]: ...

    @overload
    def replace(
        self: Operand[Indexed[IndexType, StringValueType], Definite, GroupType],
        old: IndexedDroppingArgument[IndexType, OldStringValueType],
        new: IndexedAnyStringArgument[IndexType, NewStringValueType, ArgumentOrderType],
    ) -> Operand[Indexed[IndexType, StringValueType], Single, GroupType]: ...

    @overload
    def replace(
        self: Operand[
            Indexed[IndexType, StringValueType], DroppedContainerType, GroupType
        ],
        old: IndexedDroppingArgument[IndexType, OldStringValueType],
        new: IndexedAnyStringArgument[IndexType, NewStringValueType, ArgumentOrderType],
    ) -> Operand[
        Indexed[IndexType, StringValueType], DroppedContainerType, GroupType
    ]: ...

    @overload
    def replace(
        self: Operand[Indexed[IndexType, StringValueType], Definite, GroupType],
        old: IndexedStringArgument[IndexType, OldStringValueType, ArgumentOrderType],
        new: IndexedDroppingArgument[IndexType, NewStringValueType],
    ) -> Operand[Indexed[IndexType, StringValueType], Single, GroupType]: ...

    @overload
    def replace(
        self: Operand[
            Indexed[IndexType, StringValueType], DroppedContainerType, GroupType
        ],
        old: IndexedStringArgument[IndexType, OldStringValueType, ArgumentOrderType],
        new: IndexedDroppingArgument[IndexType, NewStringValueType],
    ) -> Operand[
        Indexed[IndexType, StringValueType], DroppedContainerType, GroupType
    ]: ...

    @overload
    def replace(
        self: Operand[Bare[StringValueType], ContainerType, GroupType],
        old: BareStringArgument[OldStringValueType],
        new: BareStringArgument[NewStringValueType],
    ) -> Operand[Bare[StringValueType], ContainerType, GroupType]: ...

    @overload
    def replace(
        self: Operand[Bare[StringValueType], Definite, GroupType],
        old: BareDroppingArgument[OldStringValueType],
        new: BareAnyStringArgument[NewStringValueType],
    ) -> Operand[Bare[StringValueType], Single, GroupType]: ...

    @overload
    def replace(
        self: Operand[Bare[StringValueType], DroppedContainerType, GroupType],
        old: BareDroppingArgument[OldStringValueType],
        new: BareAnyStringArgument[NewStringValueType],
    ) -> Operand[Bare[StringValueType], DroppedContainerType, GroupType]: ...

    @overload
    def replace(
        self: Operand[Bare[StringValueType], Definite, GroupType],
        old: BareStringArgument[OldStringValueType],
        new: BareDroppingArgument[NewStringValueType],
    ) -> Operand[Bare[StringValueType], Single, GroupType]: ...

    @overload
    def replace(
        self: Operand[Bare[StringValueType], DroppedContainerType, GroupType],
        old: BareStringArgument[OldStringValueType],
        new: BareDroppingArgument[NewStringValueType],
    ) -> Operand[Bare[StringValueType], DroppedContainerType, GroupType]: ...

    def replace(
        self,
        old: Union[ScalarValue, Operand[Any, Any, Any], Argument[Any, Any]],
        new: Union[ScalarValue, Operand[Any, Any, Any], Argument[Any, Any]],
    ) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(
            self._operand.replace(
                Operand._to_py_argument(old), Operand._to_py_argument(new)
            )
        )

    @overload
    def replace_all(
        self: Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType],
        old: IndexedStringArgument[IndexType, OldStringValueType, ArgumentOrderType],
        new: IndexedStringArgument[IndexType, NewStringValueType, ArgumentOrderType],
    ) -> Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType]: ...

    @overload
    def replace_all(
        self: Operand[Indexed[IndexType, StringValueType], Definite, GroupType],
        old: IndexedDroppingArgument[IndexType, OldStringValueType],
        new: IndexedAnyStringArgument[IndexType, NewStringValueType, ArgumentOrderType],
    ) -> Operand[Indexed[IndexType, StringValueType], Single, GroupType]: ...

    @overload
    def replace_all(
        self: Operand[
            Indexed[IndexType, StringValueType], DroppedContainerType, GroupType
        ],
        old: IndexedDroppingArgument[IndexType, OldStringValueType],
        new: IndexedAnyStringArgument[IndexType, NewStringValueType, ArgumentOrderType],
    ) -> Operand[
        Indexed[IndexType, StringValueType], DroppedContainerType, GroupType
    ]: ...

    @overload
    def replace_all(
        self: Operand[Indexed[IndexType, StringValueType], Definite, GroupType],
        old: IndexedStringArgument[IndexType, OldStringValueType, ArgumentOrderType],
        new: IndexedDroppingArgument[IndexType, NewStringValueType],
    ) -> Operand[Indexed[IndexType, StringValueType], Single, GroupType]: ...

    @overload
    def replace_all(
        self: Operand[
            Indexed[IndexType, StringValueType], DroppedContainerType, GroupType
        ],
        old: IndexedStringArgument[IndexType, OldStringValueType, ArgumentOrderType],
        new: IndexedDroppingArgument[IndexType, NewStringValueType],
    ) -> Operand[
        Indexed[IndexType, StringValueType], DroppedContainerType, GroupType
    ]: ...

    @overload
    def replace_all(
        self: Operand[Bare[StringValueType], ContainerType, GroupType],
        old: BareStringArgument[OldStringValueType],
        new: BareStringArgument[NewStringValueType],
    ) -> Operand[Bare[StringValueType], ContainerType, GroupType]: ...

    @overload
    def replace_all(
        self: Operand[Bare[StringValueType], Definite, GroupType],
        old: BareDroppingArgument[OldStringValueType],
        new: BareAnyStringArgument[NewStringValueType],
    ) -> Operand[Bare[StringValueType], Single, GroupType]: ...

    @overload
    def replace_all(
        self: Operand[Bare[StringValueType], DroppedContainerType, GroupType],
        old: BareDroppingArgument[OldStringValueType],
        new: BareAnyStringArgument[NewStringValueType],
    ) -> Operand[Bare[StringValueType], DroppedContainerType, GroupType]: ...

    @overload
    def replace_all(
        self: Operand[Bare[StringValueType], Definite, GroupType],
        old: BareStringArgument[OldStringValueType],
        new: BareDroppingArgument[NewStringValueType],
    ) -> Operand[Bare[StringValueType], Single, GroupType]: ...

    @overload
    def replace_all(
        self: Operand[Bare[StringValueType], DroppedContainerType, GroupType],
        old: BareStringArgument[OldStringValueType],
        new: BareDroppingArgument[NewStringValueType],
    ) -> Operand[Bare[StringValueType], DroppedContainerType, GroupType]: ...

    def replace_all(
        self,
        old: Union[ScalarValue, Operand[Any, Any, Any], Argument[Any, Any]],
        new: Union[ScalarValue, Operand[Any, Any, Any], Argument[Any, Any]],
    ) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(
            self._operand.replace_all(
                Operand._to_py_argument(old), Operand._to_py_argument(new)
            )
        )

    @overload
    def pad_start(
        self: Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType],
        width: IndexedIntegerArgument[IndexType, IntegerValueType, ArgumentOrderType],
        character: IndexedStringArgument[
            IndexType, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType]: ...

    @overload
    def pad_start(
        self: Operand[Indexed[IndexType, StringValueType], Definite, GroupType],
        width: IndexedDroppingArgument[IndexType, IntegerValueType],
        character: IndexedAnyStringArgument[
            IndexType, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[Indexed[IndexType, StringValueType], Single, GroupType]: ...

    @overload
    def pad_start(
        self: Operand[
            Indexed[IndexType, StringValueType], DroppedContainerType, GroupType
        ],
        width: IndexedDroppingArgument[IndexType, IntegerValueType],
        character: IndexedAnyStringArgument[
            IndexType, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[IndexType, StringValueType], DroppedContainerType, GroupType
    ]: ...

    @overload
    def pad_start(
        self: Operand[Indexed[IndexType, StringValueType], Definite, GroupType],
        width: IndexedIntegerArgument[IndexType, IntegerValueType, ArgumentOrderType],
        character: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Operand[Indexed[IndexType, StringValueType], Single, GroupType]: ...

    @overload
    def pad_start(
        self: Operand[
            Indexed[IndexType, StringValueType], DroppedContainerType, GroupType
        ],
        width: IndexedIntegerArgument[IndexType, IntegerValueType, ArgumentOrderType],
        character: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Operand[
        Indexed[IndexType, StringValueType], DroppedContainerType, GroupType
    ]: ...

    @overload
    def pad_start(
        self: Operand[Bare[StringValueType], ContainerType, GroupType],
        width: BareIntegerArgument[IntegerValueType],
        character: BareStringArgument[StringArgumentValueType],
    ) -> Operand[Bare[StringValueType], ContainerType, GroupType]: ...

    @overload
    def pad_start(
        self: Operand[Bare[StringValueType], Definite, GroupType],
        width: BareDroppingArgument[IntegerValueType],
        character: BareAnyStringArgument[StringArgumentValueType],
    ) -> Operand[Bare[StringValueType], Single, GroupType]: ...

    @overload
    def pad_start(
        self: Operand[Bare[StringValueType], DroppedContainerType, GroupType],
        width: BareDroppingArgument[IntegerValueType],
        character: BareAnyStringArgument[StringArgumentValueType],
    ) -> Operand[Bare[StringValueType], DroppedContainerType, GroupType]: ...

    @overload
    def pad_start(
        self: Operand[Bare[StringValueType], Definite, GroupType],
        width: BareIntegerArgument[IntegerValueType],
        character: BareDroppingArgument[StringArgumentValueType],
    ) -> Operand[Bare[StringValueType], Single, GroupType]: ...

    @overload
    def pad_start(
        self: Operand[Bare[StringValueType], DroppedContainerType, GroupType],
        width: BareIntegerArgument[IntegerValueType],
        character: BareDroppingArgument[StringArgumentValueType],
    ) -> Operand[Bare[StringValueType], DroppedContainerType, GroupType]: ...

    def pad_start(
        self,
        width: Union[int, Operand[Any, Any, Any], Argument[Any, Any]],
        character: Union[ScalarValue, Operand[Any, Any, Any], Argument[Any, Any]],
    ) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(
            self._operand.pad_start(
                Operand._to_py_argument(width), Operand._to_py_argument(character)
            )
        )

    @overload
    def pad_end(
        self: Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType],
        width: IndexedIntegerArgument[IndexType, IntegerValueType, ArgumentOrderType],
        character: IndexedStringArgument[
            IndexType, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType]: ...

    @overload
    def pad_end(
        self: Operand[Indexed[IndexType, StringValueType], Definite, GroupType],
        width: IndexedDroppingArgument[IndexType, IntegerValueType],
        character: IndexedAnyStringArgument[
            IndexType, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[Indexed[IndexType, StringValueType], Single, GroupType]: ...

    @overload
    def pad_end(
        self: Operand[
            Indexed[IndexType, StringValueType], DroppedContainerType, GroupType
        ],
        width: IndexedDroppingArgument[IndexType, IntegerValueType],
        character: IndexedAnyStringArgument[
            IndexType, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[IndexType, StringValueType], DroppedContainerType, GroupType
    ]: ...

    @overload
    def pad_end(
        self: Operand[Indexed[IndexType, StringValueType], Definite, GroupType],
        width: IndexedIntegerArgument[IndexType, IntegerValueType, ArgumentOrderType],
        character: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Operand[Indexed[IndexType, StringValueType], Single, GroupType]: ...

    @overload
    def pad_end(
        self: Operand[
            Indexed[IndexType, StringValueType], DroppedContainerType, GroupType
        ],
        width: IndexedIntegerArgument[IndexType, IntegerValueType, ArgumentOrderType],
        character: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Operand[
        Indexed[IndexType, StringValueType], DroppedContainerType, GroupType
    ]: ...

    @overload
    def pad_end(
        self: Operand[Bare[StringValueType], ContainerType, GroupType],
        width: BareIntegerArgument[IntegerValueType],
        character: BareStringArgument[StringArgumentValueType],
    ) -> Operand[Bare[StringValueType], ContainerType, GroupType]: ...

    @overload
    def pad_end(
        self: Operand[Bare[StringValueType], Definite, GroupType],
        width: BareDroppingArgument[IntegerValueType],
        character: BareAnyStringArgument[StringArgumentValueType],
    ) -> Operand[Bare[StringValueType], Single, GroupType]: ...

    @overload
    def pad_end(
        self: Operand[Bare[StringValueType], DroppedContainerType, GroupType],
        width: BareDroppingArgument[IntegerValueType],
        character: BareAnyStringArgument[StringArgumentValueType],
    ) -> Operand[Bare[StringValueType], DroppedContainerType, GroupType]: ...

    @overload
    def pad_end(
        self: Operand[Bare[StringValueType], Definite, GroupType],
        width: BareIntegerArgument[IntegerValueType],
        character: BareDroppingArgument[StringArgumentValueType],
    ) -> Operand[Bare[StringValueType], Single, GroupType]: ...

    @overload
    def pad_end(
        self: Operand[Bare[StringValueType], DroppedContainerType, GroupType],
        width: BareIntegerArgument[IntegerValueType],
        character: BareDroppingArgument[StringArgumentValueType],
    ) -> Operand[Bare[StringValueType], DroppedContainerType, GroupType]: ...

    def pad_end(
        self,
        width: Union[int, Operand[Any, Any, Any], Argument[Any, Any]],
        character: Union[ScalarValue, Operand[Any, Any, Any], Argument[Any, Any]],
    ) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(
            self._operand.pad_end(
                Operand._to_py_argument(width), Operand._to_py_argument(character)
            )
        )

    @overload
    def split(
        self: Operand[Indexed[IndexType, StringValueType], ContainerType, GroupType],
        delimiter: IndexedAnyStringArgument[
            IndexType, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[Expanded[IndexType, Positional], StringValueType],
        Multiple[Ordered],
        GroupType,
    ]: ...

    @overload
    def split(
        self: Operand[Bare[StringValueType], ContainerType, GroupType],
        delimiter: BareAnyStringArgument[StringArgumentValueType],
    ) -> Operand[Bare[StringValueType], Multiple[Ordered], GroupType]: ...

    def split(
        self,
        delimiter: Union[ScalarValue, Operand[Any, Any, Any], Argument[Any, Any]],
    ) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(
            self._operand.split(Operand._to_py_argument(delimiter))
        )

    @overload
    def attribute(
        self: Operand[Indexed[EntityType, Unit], ContainerType, GroupType],
        attribute: Attribute,
    ) -> Operand[Indexed[EntityType, Scalar], ContainerType, GroupType]: ...

    @overload
    def attribute(
        self: Operand[Indexed[IndexType, ReferenceType], ContainerType, GroupType],
        attribute: Attribute,
    ) -> Operand[Indexed[IndexType, Scalar], ContainerType, GroupType]: ...

    def attribute(self, attribute: Attribute) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.attribute(attribute))

    @overload
    def attributes(
        self: Operand[Indexed[EntityType, Unit], ContainerType, GroupType],
    ) -> Operand[
        Indexed[Expanded[EntityType, AttributeNameIndex], AttributeName],
        Multiple[Unordered],
        GroupType,
    ]: ...

    @overload
    def attributes(
        self: Operand[Indexed[IndexType, ReferenceType], ContainerType, GroupType],
    ) -> Operand[
        Indexed[Expanded[IndexType, AttributeNameIndex], AttributeName],
        Multiple[Unordered],
        GroupType,
    ]: ...

    def attributes(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.attributes())

    @overload
    def resolve(
        self: Operand[
            Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, GroupType
        ],
    ) -> Operand[Indexed[IndexType, NodeReference], ContainerType, GroupType]: ...

    @overload
    def resolve(
        self: Operand[
            Indexed[IndexType, IndexValue[EdgeIndex]], ContainerType, GroupType
        ],
    ) -> Operand[Indexed[IndexType, EdgeReference], ContainerType, GroupType]: ...

    @overload
    def resolve(
        self: Operand[Bare[IndexValue[NodeIndex]], ContainerType, GroupType],
    ) -> Operand[Bare[NodeReference], ContainerType, GroupType]: ...

    @overload
    def resolve(
        self: Operand[Bare[IndexValue[EdgeIndex]], ContainerType, GroupType],
    ) -> Operand[Bare[EdgeReference], ContainerType, GroupType]: ...

    def resolve(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.resolve())

    @overload
    def select(
        self: Operand[
            Indexed[IndexType, NodeReference], Multiple[OrderType], GroupType
        ],
    ) -> Operand[Indexed[NodeIndex, Unit], Multiple[Unordered], GroupType]: ...

    @overload
    def select(
        self: Operand[
            Indexed[IndexType, EdgeReference], Multiple[OrderType], GroupType
        ],
    ) -> Operand[Indexed[EdgeIndex, Unit], Multiple[Unordered], GroupType]: ...

    @overload
    def select(
        self: Operand[Bare[NodeReference], Multiple[OrderType], GroupType],
    ) -> Operand[Indexed[NodeIndex, Unit], Multiple[Unordered], GroupType]: ...

    @overload
    def select(
        self: Operand[Bare[EdgeReference], Multiple[OrderType], GroupType],
    ) -> Operand[Indexed[EdgeIndex, Unit], Multiple[Unordered], GroupType]: ...

    @overload
    def select(
        self: Operand[Indexed[IndexType, NodeReference], Single, GroupType],
    ) -> Operand[Indexed[NodeIndex, Unit], Single, GroupType]: ...

    @overload
    def select(
        self: Operand[Indexed[IndexType, NodeReference], Definite, GroupType],
    ) -> Operand[Indexed[NodeIndex, Unit], Definite, GroupType]: ...

    @overload
    def select(
        self: Operand[Indexed[IndexType, EdgeReference], Single, GroupType],
    ) -> Operand[Indexed[EdgeIndex, Unit], Single, GroupType]: ...

    @overload
    def select(
        self: Operand[Indexed[IndexType, EdgeReference], Definite, GroupType],
    ) -> Operand[Indexed[EdgeIndex, Unit], Definite, GroupType]: ...

    @overload
    def select(
        self: Operand[Bare[NodeReference], Single, GroupType],
    ) -> Operand[Indexed[NodeIndex, Unit], Single, GroupType]: ...

    @overload
    def select(
        self: Operand[Bare[NodeReference], Definite, GroupType],
    ) -> Operand[Indexed[NodeIndex, Unit], Definite, GroupType]: ...

    @overload
    def select(
        self: Operand[Bare[EdgeReference], Single, GroupType],
    ) -> Operand[Indexed[EdgeIndex, Unit], Single, GroupType]: ...

    @overload
    def select(
        self: Operand[Bare[EdgeReference], Definite, GroupType],
    ) -> Operand[Indexed[EdgeIndex, Unit], Definite, GroupType]: ...

    def select(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.select())

    @overload
    def parent_index(
        self: Operand[
            Indexed[IndexType, IndexValue[Expanded[K, ChildType]]],
            ContainerType,
            GroupType,
        ],
    ) -> Operand[Indexed[IndexType, IndexValue[K]], ContainerType, GroupType]: ...

    @overload
    def parent_index(
        self: Operand[
            Bare[IndexValue[Expanded[K, ChildType]]], ContainerType, GroupType
        ],
    ) -> Operand[Bare[IndexValue[K]], ContainerType, GroupType]: ...

    def parent_index(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.parent_index())

    @overload
    def child_index(
        self: Operand[
            Indexed[IndexType, IndexValue[Expanded[K, ChildType]]],
            ContainerType,
            GroupType,
        ],
    ) -> Operand[
        Indexed[IndexType, IndexValue[ChildType]], ContainerType, GroupType
    ]: ...

    @overload
    def child_index(
        self: Operand[
            Bare[IndexValue[Expanded[K, ChildType]]], ContainerType, GroupType
        ],
    ) -> Operand[Bare[IndexValue[ChildType]], ContainerType, GroupType]: ...

    def child_index(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.child_index())

    @overload
    def has_attribute(
        self: Operand[Indexed[EntityType, Unit], ContainerType, GroupType],
        attribute: Attribute,
    ) -> Operand[Indexed[EntityType, Mask], ContainerType, GroupType]: ...

    @overload
    def has_attribute(
        self: Operand[Indexed[IndexType, ReferenceType], ContainerType, GroupType],
        attribute: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    def has_attribute(self, attribute: Attribute) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.has_attribute(attribute))

    @overload
    def in_group(
        self: Operand[Indexed[EntityType, Unit], ContainerType, GroupType],
        group: Attribute,
    ) -> Operand[Indexed[EntityType, Mask], ContainerType, GroupType]: ...

    @overload
    def in_group(
        self: Operand[Indexed[IndexType, ReferenceType], ContainerType, GroupType],
        group: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    def in_group(self, group: Attribute) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.in_group(group))

    @overload
    def add(
        self: Operand[Indexed[IndexType, ArithmeticValueType], Definite, GroupType],
        value: IndexedDroppingArgument[IndexType, ArithmeticValueType],
    ) -> Operand[Indexed[IndexType, ArithmeticValueType], Single, GroupType]: ...

    @overload
    def add(
        self: Operand[
            Indexed[IndexType, ArithmeticValueType], DroppedContainerType, GroupType
        ],
        value: IndexedDroppingArgument[IndexType, ArithmeticValueType],
    ) -> Operand[
        Indexed[IndexType, ArithmeticValueType], DroppedContainerType, GroupType
    ]: ...

    @overload
    def add(
        self: Operand[Bare[ArithmeticValueType], Definite, GroupType],
        value: BareDroppingArgument[ArithmeticValueType],
    ) -> Operand[Bare[ArithmeticValueType], Single, GroupType]: ...

    @overload
    def add(
        self: Operand[Bare[ArithmeticValueType], DroppedContainerType, GroupType],
        value: BareDroppingArgument[ArithmeticValueType],
    ) -> Operand[Bare[ArithmeticValueType], DroppedContainerType, GroupType]: ...

    @overload
    def add(
        self: Operand[
            Indexed[IndexType, ArithmeticValueType], ContainerType, GroupType
        ],
        value: IndexedOperandArgument[
            IndexType, ArithmeticValueType, ArgumentOrderType
        ],
    ) -> Operand[Indexed[IndexType, ArithmeticValueType], ContainerType, GroupType]: ...

    @overload
    def add(
        self: Operand[Bare[ArithmeticValueType], ContainerType, GroupType],
        value: BareOperandArgument[ArithmeticValueType],
    ) -> Operand[Bare[ArithmeticValueType], ContainerType, GroupType]: ...

    @overload
    def add(
        self: Operand[Indexed[IndexType, Scalar], ContainerType, GroupType],
        value: ScalarValue,
    ) -> Operand[Indexed[IndexType, Scalar], ContainerType, GroupType]: ...

    @overload
    def add(
        self: Operand[Bare[Scalar], ContainerType, GroupType],
        value: ScalarValue,
    ) -> Operand[Bare[Scalar], ContainerType, GroupType]: ...

    @overload
    def add(
        self: Operand[Indexed[IndexType, AttributeName], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, AttributeName], ContainerType, GroupType]: ...

    @overload
    def add(
        self: Operand[Bare[AttributeName], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Bare[AttributeName], ContainerType, GroupType]: ...

    @overload
    def add(
        self: Operand[
            Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, GroupType
        ],
        value: Attribute,
    ) -> Operand[
        Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, GroupType
    ]: ...

    @overload
    def add(
        self: Operand[Bare[IndexValue[NodeIndex]], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Bare[IndexValue[NodeIndex]], ContainerType, GroupType]: ...

    @overload
    def add(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndex]], ContainerType, GroupType
        ],
        value: ScalarValue,
    ) -> Operand[
        Indexed[IndexType, IndexValue[ValueIndex]], ContainerType, GroupType
    ]: ...

    @overload
    def add(
        self: Operand[Bare[IndexValue[ValueIndex]], ContainerType, GroupType],
        value: ScalarValue,
    ) -> Operand[Bare[IndexValue[ValueIndex]], ContainerType, GroupType]: ...

    @overload
    def add(
        self: Operand[
            Indexed[IndexType, IndexValue[AttributeNameIndex]], ContainerType, GroupType
        ],
        value: Attribute,
    ) -> Operand[
        Indexed[IndexType, IndexValue[AttributeNameIndex]], ContainerType, GroupType
    ]: ...

    @overload
    def add(
        self: Operand[Bare[IndexValue[AttributeNameIndex]], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Bare[IndexValue[AttributeNameIndex]], ContainerType, GroupType]: ...

    @overload
    def add(
        self: Operand[
            Indexed[IndexType, IndexValue[IntegerIndexType]], ContainerType, GroupType
        ],
        value: int,
    ) -> Operand[
        Indexed[IndexType, IndexValue[IntegerIndexType]], ContainerType, GroupType
    ]: ...

    @overload
    def add(
        self: Operand[Bare[IndexValue[IntegerIndexType]], ContainerType, GroupType],
        value: int,
    ) -> Operand[Bare[IndexValue[IntegerIndexType]], ContainerType, GroupType]: ...

    def add(
        self,
        value: Union[ScalarValue, Operand[Any, Any, Any], Argument[Any, Any]],
    ) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(
            self._operand.add(Operand._to_py_argument(value))
        )

    @overload
    def subtract(
        self: Operand[Indexed[IndexType, ArithmeticValueType], Definite, GroupType],
        value: IndexedDroppingArgument[IndexType, ArithmeticValueType],
    ) -> Operand[Indexed[IndexType, ArithmeticValueType], Single, GroupType]: ...

    @overload
    def subtract(
        self: Operand[
            Indexed[IndexType, ArithmeticValueType], DroppedContainerType, GroupType
        ],
        value: IndexedDroppingArgument[IndexType, ArithmeticValueType],
    ) -> Operand[
        Indexed[IndexType, ArithmeticValueType], DroppedContainerType, GroupType
    ]: ...

    @overload
    def subtract(
        self: Operand[Bare[ArithmeticValueType], Definite, GroupType],
        value: BareDroppingArgument[ArithmeticValueType],
    ) -> Operand[Bare[ArithmeticValueType], Single, GroupType]: ...

    @overload
    def subtract(
        self: Operand[Bare[ArithmeticValueType], DroppedContainerType, GroupType],
        value: BareDroppingArgument[ArithmeticValueType],
    ) -> Operand[Bare[ArithmeticValueType], DroppedContainerType, GroupType]: ...

    @overload
    def subtract(
        self: Operand[
            Indexed[IndexType, ArithmeticValueType], ContainerType, GroupType
        ],
        value: IndexedOperandArgument[
            IndexType, ArithmeticValueType, ArgumentOrderType
        ],
    ) -> Operand[Indexed[IndexType, ArithmeticValueType], ContainerType, GroupType]: ...

    @overload
    def subtract(
        self: Operand[Bare[ArithmeticValueType], ContainerType, GroupType],
        value: BareOperandArgument[ArithmeticValueType],
    ) -> Operand[Bare[ArithmeticValueType], ContainerType, GroupType]: ...

    @overload
    def subtract(
        self: Operand[Indexed[IndexType, Scalar], ContainerType, GroupType],
        value: ScalarValue,
    ) -> Operand[Indexed[IndexType, Scalar], ContainerType, GroupType]: ...

    @overload
    def subtract(
        self: Operand[Bare[Scalar], ContainerType, GroupType],
        value: ScalarValue,
    ) -> Operand[Bare[Scalar], ContainerType, GroupType]: ...

    @overload
    def subtract(
        self: Operand[Indexed[IndexType, AttributeName], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, AttributeName], ContainerType, GroupType]: ...

    @overload
    def subtract(
        self: Operand[Bare[AttributeName], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Bare[AttributeName], ContainerType, GroupType]: ...

    @overload
    def subtract(
        self: Operand[
            Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, GroupType
        ],
        value: Attribute,
    ) -> Operand[
        Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, GroupType
    ]: ...

    @overload
    def subtract(
        self: Operand[Bare[IndexValue[NodeIndex]], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Bare[IndexValue[NodeIndex]], ContainerType, GroupType]: ...

    @overload
    def subtract(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndex]], ContainerType, GroupType
        ],
        value: ScalarValue,
    ) -> Operand[
        Indexed[IndexType, IndexValue[ValueIndex]], ContainerType, GroupType
    ]: ...

    @overload
    def subtract(
        self: Operand[Bare[IndexValue[ValueIndex]], ContainerType, GroupType],
        value: ScalarValue,
    ) -> Operand[Bare[IndexValue[ValueIndex]], ContainerType, GroupType]: ...

    @overload
    def subtract(
        self: Operand[
            Indexed[IndexType, IndexValue[AttributeNameIndex]], ContainerType, GroupType
        ],
        value: Attribute,
    ) -> Operand[
        Indexed[IndexType, IndexValue[AttributeNameIndex]], ContainerType, GroupType
    ]: ...

    @overload
    def subtract(
        self: Operand[Bare[IndexValue[AttributeNameIndex]], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Bare[IndexValue[AttributeNameIndex]], ContainerType, GroupType]: ...

    @overload
    def subtract(
        self: Operand[
            Indexed[IndexType, IndexValue[IntegerIndexType]], ContainerType, GroupType
        ],
        value: int,
    ) -> Operand[
        Indexed[IndexType, IndexValue[IntegerIndexType]], ContainerType, GroupType
    ]: ...

    @overload
    def subtract(
        self: Operand[Bare[IndexValue[IntegerIndexType]], ContainerType, GroupType],
        value: int,
    ) -> Operand[Bare[IndexValue[IntegerIndexType]], ContainerType, GroupType]: ...

    def subtract(
        self,
        value: Union[ScalarValue, Operand[Any, Any, Any], Argument[Any, Any]],
    ) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(
            self._operand.subtract(Operand._to_py_argument(value))
        )

    @overload
    def multiply(
        self: Operand[Indexed[IndexType, ArithmeticValueType], Definite, GroupType],
        value: IndexedDroppingArgument[IndexType, ArithmeticValueType],
    ) -> Operand[Indexed[IndexType, ArithmeticValueType], Single, GroupType]: ...

    @overload
    def multiply(
        self: Operand[
            Indexed[IndexType, ArithmeticValueType], DroppedContainerType, GroupType
        ],
        value: IndexedDroppingArgument[IndexType, ArithmeticValueType],
    ) -> Operand[
        Indexed[IndexType, ArithmeticValueType], DroppedContainerType, GroupType
    ]: ...

    @overload
    def multiply(
        self: Operand[Bare[ArithmeticValueType], Definite, GroupType],
        value: BareDroppingArgument[ArithmeticValueType],
    ) -> Operand[Bare[ArithmeticValueType], Single, GroupType]: ...

    @overload
    def multiply(
        self: Operand[Bare[ArithmeticValueType], DroppedContainerType, GroupType],
        value: BareDroppingArgument[ArithmeticValueType],
    ) -> Operand[Bare[ArithmeticValueType], DroppedContainerType, GroupType]: ...

    @overload
    def multiply(
        self: Operand[
            Indexed[IndexType, ArithmeticValueType], ContainerType, GroupType
        ],
        value: IndexedOperandArgument[
            IndexType, ArithmeticValueType, ArgumentOrderType
        ],
    ) -> Operand[Indexed[IndexType, ArithmeticValueType], ContainerType, GroupType]: ...

    @overload
    def multiply(
        self: Operand[Bare[ArithmeticValueType], ContainerType, GroupType],
        value: BareOperandArgument[ArithmeticValueType],
    ) -> Operand[Bare[ArithmeticValueType], ContainerType, GroupType]: ...

    @overload
    def multiply(
        self: Operand[Indexed[IndexType, Scalar], ContainerType, GroupType],
        value: ScalarValue,
    ) -> Operand[Indexed[IndexType, Scalar], ContainerType, GroupType]: ...

    @overload
    def multiply(
        self: Operand[Bare[Scalar], ContainerType, GroupType],
        value: ScalarValue,
    ) -> Operand[Bare[Scalar], ContainerType, GroupType]: ...

    @overload
    def multiply(
        self: Operand[Indexed[IndexType, AttributeName], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, AttributeName], ContainerType, GroupType]: ...

    @overload
    def multiply(
        self: Operand[Bare[AttributeName], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Bare[AttributeName], ContainerType, GroupType]: ...

    @overload
    def multiply(
        self: Operand[
            Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, GroupType
        ],
        value: Attribute,
    ) -> Operand[
        Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, GroupType
    ]: ...

    @overload
    def multiply(
        self: Operand[Bare[IndexValue[NodeIndex]], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Bare[IndexValue[NodeIndex]], ContainerType, GroupType]: ...

    @overload
    def multiply(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndex]], ContainerType, GroupType
        ],
        value: ScalarValue,
    ) -> Operand[
        Indexed[IndexType, IndexValue[ValueIndex]], ContainerType, GroupType
    ]: ...

    @overload
    def multiply(
        self: Operand[Bare[IndexValue[ValueIndex]], ContainerType, GroupType],
        value: ScalarValue,
    ) -> Operand[Bare[IndexValue[ValueIndex]], ContainerType, GroupType]: ...

    @overload
    def multiply(
        self: Operand[
            Indexed[IndexType, IndexValue[AttributeNameIndex]], ContainerType, GroupType
        ],
        value: Attribute,
    ) -> Operand[
        Indexed[IndexType, IndexValue[AttributeNameIndex]], ContainerType, GroupType
    ]: ...

    @overload
    def multiply(
        self: Operand[Bare[IndexValue[AttributeNameIndex]], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Bare[IndexValue[AttributeNameIndex]], ContainerType, GroupType]: ...

    @overload
    def multiply(
        self: Operand[
            Indexed[IndexType, IndexValue[IntegerIndexType]], ContainerType, GroupType
        ],
        value: int,
    ) -> Operand[
        Indexed[IndexType, IndexValue[IntegerIndexType]], ContainerType, GroupType
    ]: ...

    @overload
    def multiply(
        self: Operand[Bare[IndexValue[IntegerIndexType]], ContainerType, GroupType],
        value: int,
    ) -> Operand[Bare[IndexValue[IntegerIndexType]], ContainerType, GroupType]: ...

    def multiply(
        self,
        value: Union[ScalarValue, Operand[Any, Any, Any], Argument[Any, Any]],
    ) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(
            self._operand.multiply(Operand._to_py_argument(value))
        )

    @overload
    def power(
        self: Operand[Indexed[IndexType, ArithmeticValueType], Definite, GroupType],
        value: IndexedDroppingArgument[IndexType, ArithmeticValueType],
    ) -> Operand[Indexed[IndexType, ArithmeticValueType], Single, GroupType]: ...

    @overload
    def power(
        self: Operand[
            Indexed[IndexType, ArithmeticValueType], DroppedContainerType, GroupType
        ],
        value: IndexedDroppingArgument[IndexType, ArithmeticValueType],
    ) -> Operand[
        Indexed[IndexType, ArithmeticValueType], DroppedContainerType, GroupType
    ]: ...

    @overload
    def power(
        self: Operand[Bare[ArithmeticValueType], Definite, GroupType],
        value: BareDroppingArgument[ArithmeticValueType],
    ) -> Operand[Bare[ArithmeticValueType], Single, GroupType]: ...

    @overload
    def power(
        self: Operand[Bare[ArithmeticValueType], DroppedContainerType, GroupType],
        value: BareDroppingArgument[ArithmeticValueType],
    ) -> Operand[Bare[ArithmeticValueType], DroppedContainerType, GroupType]: ...

    @overload
    def power(
        self: Operand[
            Indexed[IndexType, ArithmeticValueType], ContainerType, GroupType
        ],
        value: IndexedOperandArgument[
            IndexType, ArithmeticValueType, ArgumentOrderType
        ],
    ) -> Operand[Indexed[IndexType, ArithmeticValueType], ContainerType, GroupType]: ...

    @overload
    def power(
        self: Operand[Bare[ArithmeticValueType], ContainerType, GroupType],
        value: BareOperandArgument[ArithmeticValueType],
    ) -> Operand[Bare[ArithmeticValueType], ContainerType, GroupType]: ...

    @overload
    def power(
        self: Operand[Indexed[IndexType, Scalar], ContainerType, GroupType],
        value: ScalarValue,
    ) -> Operand[Indexed[IndexType, Scalar], ContainerType, GroupType]: ...

    @overload
    def power(
        self: Operand[Bare[Scalar], ContainerType, GroupType],
        value: ScalarValue,
    ) -> Operand[Bare[Scalar], ContainerType, GroupType]: ...

    @overload
    def power(
        self: Operand[Indexed[IndexType, AttributeName], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, AttributeName], ContainerType, GroupType]: ...

    @overload
    def power(
        self: Operand[Bare[AttributeName], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Bare[AttributeName], ContainerType, GroupType]: ...

    @overload
    def power(
        self: Operand[
            Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, GroupType
        ],
        value: Attribute,
    ) -> Operand[
        Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, GroupType
    ]: ...

    @overload
    def power(
        self: Operand[Bare[IndexValue[NodeIndex]], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Bare[IndexValue[NodeIndex]], ContainerType, GroupType]: ...

    @overload
    def power(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndex]], ContainerType, GroupType
        ],
        value: ScalarValue,
    ) -> Operand[
        Indexed[IndexType, IndexValue[ValueIndex]], ContainerType, GroupType
    ]: ...

    @overload
    def power(
        self: Operand[Bare[IndexValue[ValueIndex]], ContainerType, GroupType],
        value: ScalarValue,
    ) -> Operand[Bare[IndexValue[ValueIndex]], ContainerType, GroupType]: ...

    @overload
    def power(
        self: Operand[
            Indexed[IndexType, IndexValue[AttributeNameIndex]], ContainerType, GroupType
        ],
        value: Attribute,
    ) -> Operand[
        Indexed[IndexType, IndexValue[AttributeNameIndex]], ContainerType, GroupType
    ]: ...

    @overload
    def power(
        self: Operand[Bare[IndexValue[AttributeNameIndex]], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Bare[IndexValue[AttributeNameIndex]], ContainerType, GroupType]: ...

    @overload
    def power(
        self: Operand[
            Indexed[IndexType, IndexValue[IntegerIndexType]], ContainerType, GroupType
        ],
        value: int,
    ) -> Operand[
        Indexed[IndexType, IndexValue[IntegerIndexType]], ContainerType, GroupType
    ]: ...

    @overload
    def power(
        self: Operand[Bare[IndexValue[IntegerIndexType]], ContainerType, GroupType],
        value: int,
    ) -> Operand[Bare[IndexValue[IntegerIndexType]], ContainerType, GroupType]: ...

    def power(
        self,
        value: Union[ScalarValue, Operand[Any, Any, Any], Argument[Any, Any]],
    ) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(
            self._operand.power(Operand._to_py_argument(value))
        )

    @overload
    def modulo(
        self: Operand[Indexed[IndexType, ArithmeticValueType], Definite, GroupType],
        value: IndexedDroppingArgument[IndexType, ArithmeticValueType],
    ) -> Operand[Indexed[IndexType, ArithmeticValueType], Single, GroupType]: ...

    @overload
    def modulo(
        self: Operand[
            Indexed[IndexType, ArithmeticValueType], DroppedContainerType, GroupType
        ],
        value: IndexedDroppingArgument[IndexType, ArithmeticValueType],
    ) -> Operand[
        Indexed[IndexType, ArithmeticValueType], DroppedContainerType, GroupType
    ]: ...

    @overload
    def modulo(
        self: Operand[Bare[ArithmeticValueType], Definite, GroupType],
        value: BareDroppingArgument[ArithmeticValueType],
    ) -> Operand[Bare[ArithmeticValueType], Single, GroupType]: ...

    @overload
    def modulo(
        self: Operand[Bare[ArithmeticValueType], DroppedContainerType, GroupType],
        value: BareDroppingArgument[ArithmeticValueType],
    ) -> Operand[Bare[ArithmeticValueType], DroppedContainerType, GroupType]: ...

    @overload
    def modulo(
        self: Operand[
            Indexed[IndexType, ArithmeticValueType], ContainerType, GroupType
        ],
        value: IndexedOperandArgument[
            IndexType, ArithmeticValueType, ArgumentOrderType
        ],
    ) -> Operand[Indexed[IndexType, ArithmeticValueType], ContainerType, GroupType]: ...

    @overload
    def modulo(
        self: Operand[Bare[ArithmeticValueType], ContainerType, GroupType],
        value: BareOperandArgument[ArithmeticValueType],
    ) -> Operand[Bare[ArithmeticValueType], ContainerType, GroupType]: ...

    @overload
    def modulo(
        self: Operand[Indexed[IndexType, Scalar], ContainerType, GroupType],
        value: ScalarValue,
    ) -> Operand[Indexed[IndexType, Scalar], ContainerType, GroupType]: ...

    @overload
    def modulo(
        self: Operand[Bare[Scalar], ContainerType, GroupType],
        value: ScalarValue,
    ) -> Operand[Bare[Scalar], ContainerType, GroupType]: ...

    @overload
    def modulo(
        self: Operand[Indexed[IndexType, AttributeName], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, AttributeName], ContainerType, GroupType]: ...

    @overload
    def modulo(
        self: Operand[Bare[AttributeName], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Bare[AttributeName], ContainerType, GroupType]: ...

    @overload
    def modulo(
        self: Operand[
            Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, GroupType
        ],
        value: Attribute,
    ) -> Operand[
        Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, GroupType
    ]: ...

    @overload
    def modulo(
        self: Operand[Bare[IndexValue[NodeIndex]], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Bare[IndexValue[NodeIndex]], ContainerType, GroupType]: ...

    @overload
    def modulo(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndex]], ContainerType, GroupType
        ],
        value: ScalarValue,
    ) -> Operand[
        Indexed[IndexType, IndexValue[ValueIndex]], ContainerType, GroupType
    ]: ...

    @overload
    def modulo(
        self: Operand[Bare[IndexValue[ValueIndex]], ContainerType, GroupType],
        value: ScalarValue,
    ) -> Operand[Bare[IndexValue[ValueIndex]], ContainerType, GroupType]: ...

    @overload
    def modulo(
        self: Operand[
            Indexed[IndexType, IndexValue[AttributeNameIndex]], ContainerType, GroupType
        ],
        value: Attribute,
    ) -> Operand[
        Indexed[IndexType, IndexValue[AttributeNameIndex]], ContainerType, GroupType
    ]: ...

    @overload
    def modulo(
        self: Operand[Bare[IndexValue[AttributeNameIndex]], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Bare[IndexValue[AttributeNameIndex]], ContainerType, GroupType]: ...

    @overload
    def modulo(
        self: Operand[
            Indexed[IndexType, IndexValue[IntegerIndexType]], ContainerType, GroupType
        ],
        value: int,
    ) -> Operand[
        Indexed[IndexType, IndexValue[IntegerIndexType]], ContainerType, GroupType
    ]: ...

    @overload
    def modulo(
        self: Operand[Bare[IndexValue[IntegerIndexType]], ContainerType, GroupType],
        value: int,
    ) -> Operand[Bare[IndexValue[IntegerIndexType]], ContainerType, GroupType]: ...

    def modulo(
        self,
        value: Union[ScalarValue, Operand[Any, Any, Any], Argument[Any, Any]],
    ) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(
            self._operand.modulo(Operand._to_py_argument(value))
        )

    @overload
    def divide(
        self: Operand[Indexed[IndexType, RealNumericValueType], Definite, GroupType],
        value: IndexedDroppingArgument[IndexType, RealNumericValueType],
    ) -> Operand[Indexed[IndexType, RealNumericValueType], Single, GroupType]: ...

    @overload
    def divide(
        self: Operand[
            Indexed[IndexType, RealNumericValueType], DroppedContainerType, GroupType
        ],
        value: IndexedDroppingArgument[IndexType, RealNumericValueType],
    ) -> Operand[
        Indexed[IndexType, RealNumericValueType], DroppedContainerType, GroupType
    ]: ...

    @overload
    def divide(
        self: Operand[Bare[RealNumericValueType], Definite, GroupType],
        value: BareDroppingArgument[RealNumericValueType],
    ) -> Operand[Bare[RealNumericValueType], Single, GroupType]: ...

    @overload
    def divide(
        self: Operand[Bare[RealNumericValueType], DroppedContainerType, GroupType],
        value: BareDroppingArgument[RealNumericValueType],
    ) -> Operand[Bare[RealNumericValueType], DroppedContainerType, GroupType]: ...

    @overload
    def divide(
        self: Operand[
            Indexed[IndexType, RealNumericValueType], ContainerType, GroupType
        ],
        value: IndexedOperandArgument[
            IndexType, RealNumericValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[IndexType, RealNumericValueType], ContainerType, GroupType
    ]: ...

    @overload
    def divide(
        self: Operand[Bare[RealNumericValueType], ContainerType, GroupType],
        value: BareOperandArgument[RealNumericValueType],
    ) -> Operand[Bare[RealNumericValueType], ContainerType, GroupType]: ...

    @overload
    def divide(
        self: Operand[
            Indexed[IndexType, RealNumericValueType], ContainerType, GroupType
        ],
        value: ScalarValue,
    ) -> Operand[
        Indexed[IndexType, RealNumericValueType], ContainerType, GroupType
    ]: ...

    @overload
    def divide(
        self: Operand[Bare[RealNumericValueType], ContainerType, GroupType],
        value: ScalarValue,
    ) -> Operand[Bare[RealNumericValueType], ContainerType, GroupType]: ...

    def divide(
        self,
        value: Union[ScalarValue, Operand[Any, Any, Any], Argument[Any, Any]],
    ) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(
            self._operand.divide(Operand._to_py_argument(value))
        )

    @overload
    def clip(
        self: Operand[Indexed[IndexType, ClipValueType], Definite, GroupType],
        lower: IndexedDroppingArgument[IndexType, ClipValueType],
        upper: IndexedAnyArgument[IndexType, ClipValueType, ArgumentOrderType],
    ) -> Operand[Indexed[IndexType, ClipValueType], Single, GroupType]: ...

    @overload
    def clip(
        self: Operand[
            Indexed[IndexType, ClipValueType], DroppedContainerType, GroupType
        ],
        lower: IndexedDroppingArgument[IndexType, ClipValueType],
        upper: IndexedAnyArgument[IndexType, ClipValueType, ArgumentOrderType],
    ) -> Operand[
        Indexed[IndexType, ClipValueType], DroppedContainerType, GroupType
    ]: ...

    @overload
    def clip(
        self: Operand[Indexed[IndexType, ClipValueType], Definite, GroupType],
        lower: IndexedOperandArgument[IndexType, ClipValueType, ArgumentOrderType],
        upper: IndexedDroppingArgument[IndexType, ClipValueType],
    ) -> Operand[Indexed[IndexType, ClipValueType], Single, GroupType]: ...

    @overload
    def clip(
        self: Operand[
            Indexed[IndexType, ClipValueType], DroppedContainerType, GroupType
        ],
        lower: IndexedOperandArgument[IndexType, ClipValueType, ArgumentOrderType],
        upper: IndexedDroppingArgument[IndexType, ClipValueType],
    ) -> Operand[
        Indexed[IndexType, ClipValueType], DroppedContainerType, GroupType
    ]: ...

    @overload
    def clip(
        self: Operand[Bare[ClipValueType], Definite, GroupType],
        lower: BareDroppingArgument[ClipValueType],
        upper: BareAnyArgument[ClipValueType],
    ) -> Operand[Bare[ClipValueType], Single, GroupType]: ...

    @overload
    def clip(
        self: Operand[Bare[ClipValueType], DroppedContainerType, GroupType],
        lower: BareDroppingArgument[ClipValueType],
        upper: BareAnyArgument[ClipValueType],
    ) -> Operand[Bare[ClipValueType], DroppedContainerType, GroupType]: ...

    @overload
    def clip(
        self: Operand[Bare[ClipValueType], Definite, GroupType],
        lower: BareOperandArgument[ClipValueType],
        upper: BareDroppingArgument[ClipValueType],
    ) -> Operand[Bare[ClipValueType], Single, GroupType]: ...

    @overload
    def clip(
        self: Operand[Bare[ClipValueType], DroppedContainerType, GroupType],
        lower: BareOperandArgument[ClipValueType],
        upper: BareDroppingArgument[ClipValueType],
    ) -> Operand[Bare[ClipValueType], DroppedContainerType, GroupType]: ...

    @overload
    def clip(
        self: Operand[
            Indexed[IndexType, ScalarClipValueType], ContainerType, GroupType
        ],
        lower: IndexedScalarArgument[IndexType, ScalarClipValueType, ArgumentOrderType],
        upper: IndexedScalarArgument[IndexType, ScalarClipValueType, ArgumentOrderType],
    ) -> Operand[Indexed[IndexType, ScalarClipValueType], ContainerType, GroupType]: ...

    @overload
    def clip(
        self: Operand[Bare[ScalarClipValueType], ContainerType, GroupType],
        lower: BareScalarArgument[ScalarClipValueType],
        upper: BareScalarArgument[ScalarClipValueType],
    ) -> Operand[Bare[ScalarClipValueType], ContainerType, GroupType]: ...

    @overload
    def clip(
        self: Operand[
            Indexed[IndexType, AttributeClipValueType], ContainerType, GroupType
        ],
        lower: IndexedAttributeArgument[
            IndexType, AttributeClipValueType, ArgumentOrderType
        ],
        upper: IndexedAttributeArgument[
            IndexType, AttributeClipValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[IndexType, AttributeClipValueType], ContainerType, GroupType
    ]: ...

    @overload
    def clip(
        self: Operand[Bare[AttributeClipValueType], ContainerType, GroupType],
        lower: BareAttributeArgument[AttributeClipValueType],
        upper: BareAttributeArgument[AttributeClipValueType],
    ) -> Operand[Bare[AttributeClipValueType], ContainerType, GroupType]: ...

    @overload
    def clip(
        self: Operand[
            Indexed[IndexType, IntegerClipValueType], ContainerType, GroupType
        ],
        lower: IndexedIntegerArgument[
            IndexType, IntegerClipValueType, ArgumentOrderType
        ],
        upper: IndexedIntegerArgument[
            IndexType, IntegerClipValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[IndexType, IntegerClipValueType], ContainerType, GroupType
    ]: ...

    @overload
    def clip(
        self: Operand[Bare[IntegerClipValueType], ContainerType, GroupType],
        lower: BareIntegerArgument[IntegerClipValueType],
        upper: BareIntegerArgument[IntegerClipValueType],
    ) -> Operand[Bare[IntegerClipValueType], ContainerType, GroupType]: ...

    def clip(
        self,
        lower: Union[ScalarValue, Operand[Any, Any, Any], Argument[Any, Any]],
        upper: Union[ScalarValue, Operand[Any, Any, Any], Argument[Any, Any]],
    ) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(
            self._operand.clip(
                Operand._to_py_argument(lower), Operand._to_py_argument(upper)
            )
        )

    @overload
    def cast(
        self: Operand[Indexed[IndexType, CastableValueType], ContainerType, GroupType],
        target: CastTarget[CastableValueType],
    ) -> Operand[Indexed[IndexType, CastableValueType], ContainerType, GroupType]: ...

    @overload
    def cast(
        self: Operand[Bare[CastableValueType], ContainerType, GroupType],
        target: CastTarget[CastableValueType],
    ) -> Operand[Bare[CastableValueType], ContainerType, GroupType]: ...

    def cast(self, target: CastTarget[Any]) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.cast(target._target))

    @overload
    def __eq__(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndexType]], Definite, GroupType
        ],
        value: IndexedDroppingArgument[IndexType, IndexValue[ValueIndexType]],
    ) -> Operand[Indexed[IndexType, Mask], Single, GroupType]: ...

    @overload
    def __eq__(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndexType]],
            DroppedContainerType,
            GroupType,
        ],
        value: IndexedDroppingArgument[IndexType, IndexValue[ValueIndexType]],
    ) -> Operand[Indexed[IndexType, Mask], DroppedContainerType, GroupType]: ...

    @overload
    def __eq__(
        self: Operand[Bare[IndexValue[ValueIndexType]], Definite, GroupType],
        value: BareDroppingArgument[IndexValue[ValueIndexType]],
    ) -> Operand[Bare[Mask], Single, GroupType]: ...

    @overload
    def __eq__(
        self: Operand[
            Bare[IndexValue[ValueIndexType]], DroppedContainerType, GroupType
        ],
        value: BareDroppingArgument[IndexValue[ValueIndexType]],
    ) -> Operand[Bare[Mask], DroppedContainerType, GroupType]: ...

    @overload
    def __eq__(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndexType]], ContainerType, GroupType
        ],
        value: IndexedOperandArgument[
            IndexType, IndexValue[ValueIndexType], ArgumentOrderType
        ],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def __eq__(
        self: Operand[Bare[IndexValue[ValueIndexType]], ContainerType, GroupType],
        value: BareOperandArgument[IndexValue[ValueIndexType]],
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def __eq__(
        self: Operand[Indexed[IndexType, EquatableValueType], Definite, GroupType],
        value: IndexedDroppingArgument[IndexType, EquatableValueType],
    ) -> Operand[Indexed[IndexType, Mask], Single, GroupType]: ...

    @overload
    def __eq__(
        self: Operand[
            Indexed[IndexType, EquatableValueType], DroppedContainerType, GroupType
        ],
        value: IndexedDroppingArgument[IndexType, EquatableValueType],
    ) -> Operand[Indexed[IndexType, Mask], DroppedContainerType, GroupType]: ...

    @overload
    def __eq__(
        self: Operand[Bare[EquatableValueType], Definite, GroupType],
        value: BareDroppingArgument[EquatableValueType],
    ) -> Operand[Bare[Mask], Single, GroupType]: ...

    @overload
    def __eq__(
        self: Operand[Bare[EquatableValueType], DroppedContainerType, GroupType],
        value: BareDroppingArgument[EquatableValueType],
    ) -> Operand[Bare[Mask], DroppedContainerType, GroupType]: ...

    @overload
    def __eq__(
        self: Operand[Indexed[IndexType, EquatableValueType], ContainerType, GroupType],
        value: IndexedOperandArgument[IndexType, EquatableValueType, ArgumentOrderType],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def __eq__(
        self: Operand[Bare[EquatableValueType], ContainerType, GroupType],
        value: BareOperandArgument[EquatableValueType],
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def __eq__(
        self: Operand[Indexed[IndexType, Scalar], ContainerType, GroupType],
        value: ScalarValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def __eq__(
        self: Operand[Bare[Scalar], ContainerType, GroupType],
        value: ScalarValue,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def __eq__(
        self: Operand[Indexed[IndexType, Mask], ContainerType, GroupType],
        value: _BooleanValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def __eq__(
        self: Operand[Bare[Mask], ContainerType, GroupType],
        value: _BooleanValue,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def __eq__(
        self: Operand[Indexed[IndexType, AttributeName], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def __eq__(
        self: Operand[Bare[AttributeName], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def __eq__(
        self: Operand[Indexed[IndexType, FailureKindValue], ContainerType, GroupType],
        value: FailureKind,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def __eq__(
        self: Operand[Bare[FailureKindValue], ContainerType, GroupType],
        value: FailureKind,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def __eq__(
        self: Operand[
            Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, GroupType
        ],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def __eq__(
        self: Operand[Bare[IndexValue[NodeIndex]], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def __eq__(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndex]], ContainerType, GroupType
        ],
        value: ScalarValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def __eq__(
        self: Operand[Bare[IndexValue[ValueIndex]], ContainerType, GroupType],
        value: ScalarValue,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def __eq__(
        self: Operand[
            Indexed[IndexType, IndexValue[AttributeNameIndex]], ContainerType, GroupType
        ],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def __eq__(
        self: Operand[Bare[IndexValue[AttributeNameIndex]], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def __eq__(
        self: Operand[
            Indexed[IndexType, IndexValue[BoolIndex]], ContainerType, GroupType
        ],
        value: _BooleanValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def __eq__(
        self: Operand[Bare[IndexValue[BoolIndex]], ContainerType, GroupType],
        value: _BooleanValue,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def __eq__(
        self: Operand[
            Indexed[IndexType, IndexValue[IntegerIndexType]], ContainerType, GroupType
        ],
        value: int,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def __eq__(
        self: Operand[Bare[IndexValue[IntegerIndexType]], ContainerType, GroupType],
        value: int,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    def __eq__(
        self,
        value: Union[
            ScalarValue, FailureKind, Operand[Any, Any, Any], Argument[Any, Any]
        ],
    ) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(
            self._operand.equal_to(Operand._to_py_argument(value))
        )

    equal_to = __eq__

    @overload
    def __ne__(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndexType]], Definite, GroupType
        ],
        value: IndexedDroppingArgument[IndexType, IndexValue[ValueIndexType]],
    ) -> Operand[Indexed[IndexType, Mask], Single, GroupType]: ...

    @overload
    def __ne__(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndexType]],
            DroppedContainerType,
            GroupType,
        ],
        value: IndexedDroppingArgument[IndexType, IndexValue[ValueIndexType]],
    ) -> Operand[Indexed[IndexType, Mask], DroppedContainerType, GroupType]: ...

    @overload
    def __ne__(
        self: Operand[Bare[IndexValue[ValueIndexType]], Definite, GroupType],
        value: BareDroppingArgument[IndexValue[ValueIndexType]],
    ) -> Operand[Bare[Mask], Single, GroupType]: ...

    @overload
    def __ne__(
        self: Operand[
            Bare[IndexValue[ValueIndexType]], DroppedContainerType, GroupType
        ],
        value: BareDroppingArgument[IndexValue[ValueIndexType]],
    ) -> Operand[Bare[Mask], DroppedContainerType, GroupType]: ...

    @overload
    def __ne__(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndexType]], ContainerType, GroupType
        ],
        value: IndexedOperandArgument[
            IndexType, IndexValue[ValueIndexType], ArgumentOrderType
        ],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def __ne__(
        self: Operand[Bare[IndexValue[ValueIndexType]], ContainerType, GroupType],
        value: BareOperandArgument[IndexValue[ValueIndexType]],
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def __ne__(
        self: Operand[Indexed[IndexType, EquatableValueType], Definite, GroupType],
        value: IndexedDroppingArgument[IndexType, EquatableValueType],
    ) -> Operand[Indexed[IndexType, Mask], Single, GroupType]: ...

    @overload
    def __ne__(
        self: Operand[
            Indexed[IndexType, EquatableValueType], DroppedContainerType, GroupType
        ],
        value: IndexedDroppingArgument[IndexType, EquatableValueType],
    ) -> Operand[Indexed[IndexType, Mask], DroppedContainerType, GroupType]: ...

    @overload
    def __ne__(
        self: Operand[Bare[EquatableValueType], Definite, GroupType],
        value: BareDroppingArgument[EquatableValueType],
    ) -> Operand[Bare[Mask], Single, GroupType]: ...

    @overload
    def __ne__(
        self: Operand[Bare[EquatableValueType], DroppedContainerType, GroupType],
        value: BareDroppingArgument[EquatableValueType],
    ) -> Operand[Bare[Mask], DroppedContainerType, GroupType]: ...

    @overload
    def __ne__(
        self: Operand[Indexed[IndexType, EquatableValueType], ContainerType, GroupType],
        value: IndexedOperandArgument[IndexType, EquatableValueType, ArgumentOrderType],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def __ne__(
        self: Operand[Bare[EquatableValueType], ContainerType, GroupType],
        value: BareOperandArgument[EquatableValueType],
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def __ne__(
        self: Operand[Indexed[IndexType, Scalar], ContainerType, GroupType],
        value: ScalarValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def __ne__(
        self: Operand[Bare[Scalar], ContainerType, GroupType],
        value: ScalarValue,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def __ne__(
        self: Operand[Indexed[IndexType, Mask], ContainerType, GroupType],
        value: _BooleanValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def __ne__(
        self: Operand[Bare[Mask], ContainerType, GroupType],
        value: _BooleanValue,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def __ne__(
        self: Operand[Indexed[IndexType, AttributeName], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def __ne__(
        self: Operand[Bare[AttributeName], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def __ne__(
        self: Operand[Indexed[IndexType, FailureKindValue], ContainerType, GroupType],
        value: FailureKind,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def __ne__(
        self: Operand[Bare[FailureKindValue], ContainerType, GroupType],
        value: FailureKind,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def __ne__(
        self: Operand[
            Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, GroupType
        ],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def __ne__(
        self: Operand[Bare[IndexValue[NodeIndex]], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def __ne__(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndex]], ContainerType, GroupType
        ],
        value: ScalarValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def __ne__(
        self: Operand[Bare[IndexValue[ValueIndex]], ContainerType, GroupType],
        value: ScalarValue,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def __ne__(
        self: Operand[
            Indexed[IndexType, IndexValue[AttributeNameIndex]], ContainerType, GroupType
        ],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def __ne__(
        self: Operand[Bare[IndexValue[AttributeNameIndex]], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def __ne__(
        self: Operand[
            Indexed[IndexType, IndexValue[BoolIndex]], ContainerType, GroupType
        ],
        value: _BooleanValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def __ne__(
        self: Operand[Bare[IndexValue[BoolIndex]], ContainerType, GroupType],
        value: _BooleanValue,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def __ne__(
        self: Operand[
            Indexed[IndexType, IndexValue[IntegerIndexType]], ContainerType, GroupType
        ],
        value: int,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def __ne__(
        self: Operand[Bare[IndexValue[IntegerIndexType]], ContainerType, GroupType],
        value: int,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    def __ne__(
        self,
        value: Union[
            ScalarValue, FailureKind, Operand[Any, Any, Any], Argument[Any, Any]
        ],
    ) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(
            self._operand.not_equal_to(Operand._to_py_argument(value))
        )

    not_equal_to = __ne__

    @overload
    def greater_than(
        self: Operand[Indexed[IndexType, OrderableValueType], Definite, GroupType],
        value: IndexedDroppingArgument[IndexType, OrderableValueType],
    ) -> Operand[Indexed[IndexType, Mask], Single, GroupType]: ...

    @overload
    def greater_than(
        self: Operand[
            Indexed[IndexType, OrderableValueType], DroppedContainerType, GroupType
        ],
        value: IndexedDroppingArgument[IndexType, OrderableValueType],
    ) -> Operand[Indexed[IndexType, Mask], DroppedContainerType, GroupType]: ...

    @overload
    def greater_than(
        self: Operand[Bare[OrderableValueType], Definite, GroupType],
        value: BareDroppingArgument[OrderableValueType],
    ) -> Operand[Bare[Mask], Single, GroupType]: ...

    @overload
    def greater_than(
        self: Operand[Bare[OrderableValueType], DroppedContainerType, GroupType],
        value: BareDroppingArgument[OrderableValueType],
    ) -> Operand[Bare[Mask], DroppedContainerType, GroupType]: ...

    @overload
    def greater_than(
        self: Operand[Indexed[IndexType, OrderableValueType], ContainerType, GroupType],
        value: IndexedOperandArgument[IndexType, OrderableValueType, ArgumentOrderType],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def greater_than(
        self: Operand[Bare[OrderableValueType], ContainerType, GroupType],
        value: BareOperandArgument[OrderableValueType],
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def greater_than(
        self: Operand[Indexed[IndexType, Scalar], ContainerType, GroupType],
        value: ScalarValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def greater_than(
        self: Operand[Bare[Scalar], ContainerType, GroupType],
        value: ScalarValue,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def greater_than(
        self: Operand[Indexed[IndexType, AttributeName], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def greater_than(
        self: Operand[Bare[AttributeName], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def greater_than(
        self: Operand[
            Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, GroupType
        ],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def greater_than(
        self: Operand[Bare[IndexValue[NodeIndex]], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def greater_than(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndex]], ContainerType, GroupType
        ],
        value: ScalarValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def greater_than(
        self: Operand[Bare[IndexValue[ValueIndex]], ContainerType, GroupType],
        value: ScalarValue,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def greater_than(
        self: Operand[
            Indexed[IndexType, IndexValue[AttributeNameIndex]], ContainerType, GroupType
        ],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def greater_than(
        self: Operand[Bare[IndexValue[AttributeNameIndex]], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def greater_than(
        self: Operand[
            Indexed[IndexType, IndexValue[BoolIndex]], ContainerType, GroupType
        ],
        value: _BooleanValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def greater_than(
        self: Operand[Bare[IndexValue[BoolIndex]], ContainerType, GroupType],
        value: _BooleanValue,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def greater_than(
        self: Operand[
            Indexed[IndexType, IndexValue[IntegerIndexType]], ContainerType, GroupType
        ],
        value: int,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def greater_than(
        self: Operand[Bare[IndexValue[IntegerIndexType]], ContainerType, GroupType],
        value: int,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    def greater_than(
        self,
        value: Union[ScalarValue, Operand[Any, Any, Any], Argument[Any, Any]],
    ) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(
            self._operand.greater_than(Operand._to_py_argument(value))
        )

    @overload
    def greater_than_or_equal_to(
        self: Operand[Indexed[IndexType, OrderableValueType], Definite, GroupType],
        value: IndexedDroppingArgument[IndexType, OrderableValueType],
    ) -> Operand[Indexed[IndexType, Mask], Single, GroupType]: ...

    @overload
    def greater_than_or_equal_to(
        self: Operand[
            Indexed[IndexType, OrderableValueType], DroppedContainerType, GroupType
        ],
        value: IndexedDroppingArgument[IndexType, OrderableValueType],
    ) -> Operand[Indexed[IndexType, Mask], DroppedContainerType, GroupType]: ...

    @overload
    def greater_than_or_equal_to(
        self: Operand[Bare[OrderableValueType], Definite, GroupType],
        value: BareDroppingArgument[OrderableValueType],
    ) -> Operand[Bare[Mask], Single, GroupType]: ...

    @overload
    def greater_than_or_equal_to(
        self: Operand[Bare[OrderableValueType], DroppedContainerType, GroupType],
        value: BareDroppingArgument[OrderableValueType],
    ) -> Operand[Bare[Mask], DroppedContainerType, GroupType]: ...

    @overload
    def greater_than_or_equal_to(
        self: Operand[Indexed[IndexType, OrderableValueType], ContainerType, GroupType],
        value: IndexedOperandArgument[IndexType, OrderableValueType, ArgumentOrderType],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def greater_than_or_equal_to(
        self: Operand[Bare[OrderableValueType], ContainerType, GroupType],
        value: BareOperandArgument[OrderableValueType],
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def greater_than_or_equal_to(
        self: Operand[Indexed[IndexType, Scalar], ContainerType, GroupType],
        value: ScalarValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def greater_than_or_equal_to(
        self: Operand[Bare[Scalar], ContainerType, GroupType],
        value: ScalarValue,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def greater_than_or_equal_to(
        self: Operand[Indexed[IndexType, AttributeName], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def greater_than_or_equal_to(
        self: Operand[Bare[AttributeName], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def greater_than_or_equal_to(
        self: Operand[
            Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, GroupType
        ],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def greater_than_or_equal_to(
        self: Operand[Bare[IndexValue[NodeIndex]], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def greater_than_or_equal_to(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndex]], ContainerType, GroupType
        ],
        value: ScalarValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def greater_than_or_equal_to(
        self: Operand[Bare[IndexValue[ValueIndex]], ContainerType, GroupType],
        value: ScalarValue,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def greater_than_or_equal_to(
        self: Operand[
            Indexed[IndexType, IndexValue[AttributeNameIndex]], ContainerType, GroupType
        ],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def greater_than_or_equal_to(
        self: Operand[Bare[IndexValue[AttributeNameIndex]], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def greater_than_or_equal_to(
        self: Operand[
            Indexed[IndexType, IndexValue[BoolIndex]], ContainerType, GroupType
        ],
        value: _BooleanValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def greater_than_or_equal_to(
        self: Operand[Bare[IndexValue[BoolIndex]], ContainerType, GroupType],
        value: _BooleanValue,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def greater_than_or_equal_to(
        self: Operand[
            Indexed[IndexType, IndexValue[IntegerIndexType]], ContainerType, GroupType
        ],
        value: int,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def greater_than_or_equal_to(
        self: Operand[Bare[IndexValue[IntegerIndexType]], ContainerType, GroupType],
        value: int,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    def greater_than_or_equal_to(
        self,
        value: Union[ScalarValue, Operand[Any, Any, Any], Argument[Any, Any]],
    ) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(
            self._operand.greater_than_or_equal_to(Operand._to_py_argument(value))
        )

    @overload
    def less_than(
        self: Operand[Indexed[IndexType, OrderableValueType], Definite, GroupType],
        value: IndexedDroppingArgument[IndexType, OrderableValueType],
    ) -> Operand[Indexed[IndexType, Mask], Single, GroupType]: ...

    @overload
    def less_than(
        self: Operand[
            Indexed[IndexType, OrderableValueType], DroppedContainerType, GroupType
        ],
        value: IndexedDroppingArgument[IndexType, OrderableValueType],
    ) -> Operand[Indexed[IndexType, Mask], DroppedContainerType, GroupType]: ...

    @overload
    def less_than(
        self: Operand[Bare[OrderableValueType], Definite, GroupType],
        value: BareDroppingArgument[OrderableValueType],
    ) -> Operand[Bare[Mask], Single, GroupType]: ...

    @overload
    def less_than(
        self: Operand[Bare[OrderableValueType], DroppedContainerType, GroupType],
        value: BareDroppingArgument[OrderableValueType],
    ) -> Operand[Bare[Mask], DroppedContainerType, GroupType]: ...

    @overload
    def less_than(
        self: Operand[Indexed[IndexType, OrderableValueType], ContainerType, GroupType],
        value: IndexedOperandArgument[IndexType, OrderableValueType, ArgumentOrderType],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def less_than(
        self: Operand[Bare[OrderableValueType], ContainerType, GroupType],
        value: BareOperandArgument[OrderableValueType],
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def less_than(
        self: Operand[Indexed[IndexType, Scalar], ContainerType, GroupType],
        value: ScalarValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def less_than(
        self: Operand[Bare[Scalar], ContainerType, GroupType],
        value: ScalarValue,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def less_than(
        self: Operand[Indexed[IndexType, AttributeName], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def less_than(
        self: Operand[Bare[AttributeName], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def less_than(
        self: Operand[
            Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, GroupType
        ],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def less_than(
        self: Operand[Bare[IndexValue[NodeIndex]], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def less_than(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndex]], ContainerType, GroupType
        ],
        value: ScalarValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def less_than(
        self: Operand[Bare[IndexValue[ValueIndex]], ContainerType, GroupType],
        value: ScalarValue,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def less_than(
        self: Operand[
            Indexed[IndexType, IndexValue[AttributeNameIndex]], ContainerType, GroupType
        ],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def less_than(
        self: Operand[Bare[IndexValue[AttributeNameIndex]], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def less_than(
        self: Operand[
            Indexed[IndexType, IndexValue[BoolIndex]], ContainerType, GroupType
        ],
        value: _BooleanValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def less_than(
        self: Operand[Bare[IndexValue[BoolIndex]], ContainerType, GroupType],
        value: _BooleanValue,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def less_than(
        self: Operand[
            Indexed[IndexType, IndexValue[IntegerIndexType]], ContainerType, GroupType
        ],
        value: int,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def less_than(
        self: Operand[Bare[IndexValue[IntegerIndexType]], ContainerType, GroupType],
        value: int,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    def less_than(
        self,
        value: Union[ScalarValue, Operand[Any, Any, Any], Argument[Any, Any]],
    ) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(
            self._operand.less_than(Operand._to_py_argument(value))
        )

    @overload
    def less_than_or_equal_to(
        self: Operand[Indexed[IndexType, OrderableValueType], Definite, GroupType],
        value: IndexedDroppingArgument[IndexType, OrderableValueType],
    ) -> Operand[Indexed[IndexType, Mask], Single, GroupType]: ...

    @overload
    def less_than_or_equal_to(
        self: Operand[
            Indexed[IndexType, OrderableValueType], DroppedContainerType, GroupType
        ],
        value: IndexedDroppingArgument[IndexType, OrderableValueType],
    ) -> Operand[Indexed[IndexType, Mask], DroppedContainerType, GroupType]: ...

    @overload
    def less_than_or_equal_to(
        self: Operand[Bare[OrderableValueType], Definite, GroupType],
        value: BareDroppingArgument[OrderableValueType],
    ) -> Operand[Bare[Mask], Single, GroupType]: ...

    @overload
    def less_than_or_equal_to(
        self: Operand[Bare[OrderableValueType], DroppedContainerType, GroupType],
        value: BareDroppingArgument[OrderableValueType],
    ) -> Operand[Bare[Mask], DroppedContainerType, GroupType]: ...

    @overload
    def less_than_or_equal_to(
        self: Operand[Indexed[IndexType, OrderableValueType], ContainerType, GroupType],
        value: IndexedOperandArgument[IndexType, OrderableValueType, ArgumentOrderType],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def less_than_or_equal_to(
        self: Operand[Bare[OrderableValueType], ContainerType, GroupType],
        value: BareOperandArgument[OrderableValueType],
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def less_than_or_equal_to(
        self: Operand[Indexed[IndexType, Scalar], ContainerType, GroupType],
        value: ScalarValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def less_than_or_equal_to(
        self: Operand[Bare[Scalar], ContainerType, GroupType],
        value: ScalarValue,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def less_than_or_equal_to(
        self: Operand[Indexed[IndexType, AttributeName], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def less_than_or_equal_to(
        self: Operand[Bare[AttributeName], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def less_than_or_equal_to(
        self: Operand[
            Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, GroupType
        ],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def less_than_or_equal_to(
        self: Operand[Bare[IndexValue[NodeIndex]], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def less_than_or_equal_to(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndex]], ContainerType, GroupType
        ],
        value: ScalarValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def less_than_or_equal_to(
        self: Operand[Bare[IndexValue[ValueIndex]], ContainerType, GroupType],
        value: ScalarValue,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def less_than_or_equal_to(
        self: Operand[
            Indexed[IndexType, IndexValue[AttributeNameIndex]], ContainerType, GroupType
        ],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def less_than_or_equal_to(
        self: Operand[Bare[IndexValue[AttributeNameIndex]], ContainerType, GroupType],
        value: Attribute,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def less_than_or_equal_to(
        self: Operand[
            Indexed[IndexType, IndexValue[BoolIndex]], ContainerType, GroupType
        ],
        value: _BooleanValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def less_than_or_equal_to(
        self: Operand[Bare[IndexValue[BoolIndex]], ContainerType, GroupType],
        value: _BooleanValue,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def less_than_or_equal_to(
        self: Operand[
            Indexed[IndexType, IndexValue[IntegerIndexType]], ContainerType, GroupType
        ],
        value: int,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def less_than_or_equal_to(
        self: Operand[Bare[IndexValue[IntegerIndexType]], ContainerType, GroupType],
        value: int,
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    def less_than_or_equal_to(
        self,
        value: Union[ScalarValue, Operand[Any, Any, Any], Argument[Any, Any]],
    ) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(
            self._operand.less_than_or_equal_to(Operand._to_py_argument(value))
        )

    @overload
    def is_in(
        self: Operand[
            Indexed[IndexType, MembershipValueType], ContainerType, GroupType
        ],
        values: MembershipArgument[MembershipValueType],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def is_in(
        self: Operand[Bare[MembershipValueType], ContainerType, GroupType],
        values: MembershipArgument[MembershipValueType],
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def is_in(
        self: Operand[
            Indexed[IndexType, ScalarMembershipValueType], ContainerType, GroupType
        ],
        values: Sequence[ScalarValue],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def is_in(
        self: Operand[Bare[ScalarMembershipValueType], ContainerType, GroupType],
        values: Sequence[ScalarValue],
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def is_in(
        self: Operand[
            Indexed[IndexType, BooleanMembershipValueType], ContainerType, GroupType
        ],
        values: Sequence[_BooleanValue],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def is_in(
        self: Operand[Bare[BooleanMembershipValueType], ContainerType, GroupType],
        values: Sequence[_BooleanValue],
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def is_in(
        self: Operand[
            Indexed[IndexType, AttributeMembershipValueType], ContainerType, GroupType
        ],
        values: Sequence[Attribute],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def is_in(
        self: Operand[Bare[AttributeMembershipValueType], ContainerType, GroupType],
        values: Sequence[Attribute],
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def is_in(
        self: Operand[
            Indexed[IndexType, IndexValue[IntegerIndexType]], ContainerType, GroupType
        ],
        values: Sequence[int],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def is_in(
        self: Operand[Bare[IndexValue[IntegerIndexType]], ContainerType, GroupType],
        values: Sequence[int],
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    def is_in(
        self,
        values: Union[Sequence[ScalarValue], Operand[Any, Any, Any]],
    ) -> Operand[Any, Any, Any]:
        if isinstance(values, Operand):
            return Operand._from_py_operand(self._operand.is_in(values._operand))

        return Operand._from_py_operand(self._operand.is_in(values))

    @overload
    def index(
        self: Operand[Indexed[IndexType, Unit], ContainerType, GroupType],
    ) -> Operand[
        Indexed[IndexType, IndexValue[IndexType]], ContainerType, GroupType
    ]: ...

    @overload
    def index(
        self: Operand[Indexed[IndexType, NodeReference], ContainerType, GroupType],
    ) -> Operand[
        Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, GroupType
    ]: ...

    @overload
    def index(
        self: Operand[Indexed[IndexType, EdgeReference], ContainerType, GroupType],
    ) -> Operand[
        Indexed[IndexType, IndexValue[EdgeIndex]], ContainerType, GroupType
    ]: ...

    def index(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.index())

    @overload
    def discard_index(
        self: Operand[Indexed[IndexType, BareValueType], ContainerType, GroupType],
    ) -> Operand[Bare[BareValueType], ContainerType, GroupType]: ...

    @overload
    def discard_index(
        self: Operand[Indexed[IndexType, IndexValue[K]], ContainerType, GroupType],
    ) -> Operand[Bare[IndexValue[K]], ContainerType, GroupType]: ...

    def discard_index(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.discard_index())

    def discard_value(
        self: Operand[Indexed[IndexType, V], ContainerType, GroupType],
    ) -> Operand[Indexed[IndexType, Unit], ContainerType, GroupType]:
        return Operand._from_py_operand(self._operand.discard_value())

    @overload
    def enumerate(
        self: Operand[Indexed[IndexType, V], Multiple[Ordered], GroupType],
    ) -> Operand[Indexed[Positional, V], Multiple[Ordered], GroupType]: ...

    @overload
    def enumerate(
        self: Operand[Bare[BareValueType], Multiple[Ordered], GroupType],
    ) -> Operand[Indexed[Positional, BareValueType], Multiple[Ordered], GroupType]: ...

    def enumerate(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.enumerate())

    @overload
    def errors(
        self: Operand[Indexed[IndexType, V], Multiple[OrderType], GroupType],
    ) -> Operand[Indexed[IndexType, FailureValue], Multiple[OrderType], GroupType]: ...

    @overload
    def errors(
        self: Operand[Indexed[IndexType, V], Single, GroupType],
    ) -> Operand[Indexed[IndexType, FailureValue], Single, GroupType]: ...

    @overload
    def errors(
        self: Operand[Indexed[IndexType, V], Definite, GroupType],
    ) -> Operand[Indexed[IndexType, FailureValue], Single, GroupType]: ...

    @overload
    def errors(
        self: Operand[Bare[BareValueType], Multiple[OrderType], GroupType],
    ) -> Operand[Bare[FailureValue], Multiple[OrderType], GroupType]: ...

    @overload
    def errors(
        self: Operand[Bare[BareValueType], Single, GroupType],
    ) -> Operand[Bare[FailureValue], Single, GroupType]: ...

    @overload
    def errors(
        self: Operand[Bare[BareValueType], Definite, GroupType],
    ) -> Operand[Bare[FailureValue], Single, GroupType]: ...

    def errors(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.errors())

    @overload
    def on_error(
        self: Operand[Indexed[IndexType, V], Definite, GroupType], policy: Drop
    ) -> Operand[Indexed[IndexType, V], Single, GroupType]: ...

    @overload
    def on_error(
        self: Operand[Indexed[IndexType, V], DroppedContainerType, GroupType],
        policy: Drop,
    ) -> Operand[Indexed[IndexType, V], DroppedContainerType, GroupType]: ...

    @overload
    def on_error(
        self: Operand[Bare[BareValueType], Definite, GroupType], policy: Drop
    ) -> Operand[Bare[BareValueType], Single, GroupType]: ...

    @overload
    def on_error(
        self: Operand[Bare[BareValueType], DroppedContainerType, GroupType],
        policy: Drop,
    ) -> Operand[Bare[BareValueType], DroppedContainerType, GroupType]: ...

    @overload
    def on_error(
        self: Operand[Indexed[IndexType, V], ContainerType, GroupType],
        policy: Union[Raise, _RaiseWhen],
    ) -> Operand[Indexed[IndexType, V], ContainerType, GroupType]: ...

    @overload
    def on_error(
        self: Operand[Bare[BareValueType], ContainerType, GroupType],
        policy: Union[Raise, _RaiseWhen],
    ) -> Operand[Bare[BareValueType], ContainerType, GroupType]: ...

    @overload
    def on_error(
        self: Operand[Indexed[IndexType, V], Definite, GroupType],
        policy: Replace[IndexedDroppingArgument[IndexType, V]],
    ) -> Operand[Indexed[IndexType, V], Single, GroupType]: ...

    @overload
    def on_error(
        self: Operand[Indexed[IndexType, V], DroppedContainerType, GroupType],
        policy: Replace[IndexedDroppingArgument[IndexType, V]],
    ) -> Operand[Indexed[IndexType, V], DroppedContainerType, GroupType]: ...

    @overload
    def on_error(
        self: Operand[Bare[BareValueType], Definite, GroupType],
        policy: Replace[BareDroppingArgument[BareValueType]],
    ) -> Operand[Bare[BareValueType], Single, GroupType]: ...

    @overload
    def on_error(
        self: Operand[Bare[BareValueType], DroppedContainerType, GroupType],
        policy: Replace[BareDroppingArgument[BareValueType]],
    ) -> Operand[Bare[BareValueType], DroppedContainerType, GroupType]: ...

    @overload
    def on_error(
        self: Operand[Indexed[IndexType, V], ContainerType, GroupType],
        policy: Replace[IndexedOperandArgument[IndexType, V, ArgumentOrderType]],
    ) -> Operand[Indexed[IndexType, V], ContainerType, GroupType]: ...

    @overload
    def on_error(
        self: Operand[Bare[BareValueType], ContainerType, GroupType],
        policy: Replace[BareOperandArgument[BareValueType]],
    ) -> Operand[Bare[BareValueType], ContainerType, GroupType]: ...

    @overload
    def on_error(
        self: Operand[Indexed[IndexType, Scalar], ContainerType, GroupType],
        policy: Replace[ScalarValue],
    ) -> Operand[Indexed[IndexType, Scalar], ContainerType, GroupType]: ...

    @overload
    def on_error(
        self: Operand[Bare[Scalar], ContainerType, GroupType],
        policy: Replace[ScalarValue],
    ) -> Operand[Bare[Scalar], ContainerType, GroupType]: ...

    @overload
    def on_error(
        self: Operand[Indexed[IndexType, Mask], ContainerType, GroupType],
        policy: Replace[_BooleanValue],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, GroupType]: ...

    @overload
    def on_error(
        self: Operand[Bare[Mask], ContainerType, GroupType],
        policy: Replace[_BooleanValue],
    ) -> Operand[Bare[Mask], ContainerType, GroupType]: ...

    @overload
    def on_error(
        self: Operand[Indexed[IndexType, AttributeName], ContainerType, GroupType],
        policy: Replace[Attribute],
    ) -> Operand[Indexed[IndexType, AttributeName], ContainerType, GroupType]: ...

    @overload
    def on_error(
        self: Operand[Bare[AttributeName], ContainerType, GroupType],
        policy: Replace[Attribute],
    ) -> Operand[Bare[AttributeName], ContainerType, GroupType]: ...

    @overload
    def on_error(
        self: Operand[Indexed[IndexType, FailureKindValue], ContainerType, GroupType],
        policy: Replace[FailureKind],
    ) -> Operand[Indexed[IndexType, FailureKindValue], ContainerType, GroupType]: ...

    @overload
    def on_error(
        self: Operand[Bare[FailureKindValue], ContainerType, GroupType],
        policy: Replace[FailureKind],
    ) -> Operand[Bare[FailureKindValue], ContainerType, GroupType]: ...

    @overload
    def on_error(
        self: Operand[
            Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, GroupType
        ],
        policy: Replace[Attribute],
    ) -> Operand[
        Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, GroupType
    ]: ...

    @overload
    def on_error(
        self: Operand[Bare[IndexValue[NodeIndex]], ContainerType, GroupType],
        policy: Replace[Attribute],
    ) -> Operand[Bare[IndexValue[NodeIndex]], ContainerType, GroupType]: ...

    @overload
    def on_error(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndex]], ContainerType, GroupType
        ],
        policy: Replace[ScalarValue],
    ) -> Operand[
        Indexed[IndexType, IndexValue[ValueIndex]], ContainerType, GroupType
    ]: ...

    @overload
    def on_error(
        self: Operand[Bare[IndexValue[ValueIndex]], ContainerType, GroupType],
        policy: Replace[ScalarValue],
    ) -> Operand[Bare[IndexValue[ValueIndex]], ContainerType, GroupType]: ...

    @overload
    def on_error(
        self: Operand[
            Indexed[IndexType, IndexValue[AttributeNameIndex]], ContainerType, GroupType
        ],
        policy: Replace[Attribute],
    ) -> Operand[
        Indexed[IndexType, IndexValue[AttributeNameIndex]], ContainerType, GroupType
    ]: ...

    @overload
    def on_error(
        self: Operand[Bare[IndexValue[AttributeNameIndex]], ContainerType, GroupType],
        policy: Replace[Attribute],
    ) -> Operand[Bare[IndexValue[AttributeNameIndex]], ContainerType, GroupType]: ...

    @overload
    def on_error(
        self: Operand[
            Indexed[IndexType, IndexValue[BoolIndex]], ContainerType, GroupType
        ],
        policy: Replace[_BooleanValue],
    ) -> Operand[
        Indexed[IndexType, IndexValue[BoolIndex]], ContainerType, GroupType
    ]: ...

    @overload
    def on_error(
        self: Operand[Bare[IndexValue[BoolIndex]], ContainerType, GroupType],
        policy: Replace[_BooleanValue],
    ) -> Operand[Bare[IndexValue[BoolIndex]], ContainerType, GroupType]: ...

    @overload
    def on_error(
        self: Operand[
            Indexed[IndexType, IndexValue[IntegerIndexType]], ContainerType, GroupType
        ],
        policy: Replace[int],
    ) -> Operand[
        Indexed[IndexType, IndexValue[IntegerIndexType]], ContainerType, GroupType
    ]: ...

    @overload
    def on_error(
        self: Operand[Bare[IndexValue[IntegerIndexType]], ContainerType, GroupType],
        policy: Replace[int],
    ) -> Operand[Bare[IndexValue[IntegerIndexType]], ContainerType, GroupType]: ...

    def on_error(
        self,
        policy: Union[
            Drop,
            Raise,
            _RaiseWhen,
            Replace[
                Union[
                    ScalarValue, FailureKind, Operand[Any, Any, Any], Argument[Any, Any]
                ]
            ],
        ],
    ) -> Operand[Any, Any, Any]:
        if isinstance(policy, Drop):
            return Operand._from_py_operand(self._operand.on_error_drop())

        if isinstance(policy, _RaiseWhen):
            return Operand._from_py_operand(
                self._operand.raise_when(Operand._to_py_argument(policy._condition))
            )

        if isinstance(policy, Replace):
            return Operand._from_py_operand(
                self._operand.on_error_replace(
                    Operand._to_py_argument(policy._replacement)
                )
            )

        return Operand._from_py_operand(self._operand.on_error_raise())

    @overload
    def kind(
        self: Operand[Indexed[IndexType, FailureValue], ContainerType, GroupType],
    ) -> Operand[Indexed[IndexType, FailureKindValue], ContainerType, GroupType]: ...

    @overload
    def kind(
        self: Operand[Bare[FailureValue], ContainerType, GroupType],
    ) -> Operand[Bare[FailureKindValue], ContainerType, GroupType]: ...

    def kind(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.kind())

    @overload
    def name(
        self: Operand[Indexed[IndexType, FailureKindValue], ContainerType, GroupType],
    ) -> Operand[Indexed[IndexType, Scalar], ContainerType, GroupType]: ...

    @overload
    def name(
        self: Operand[Bare[FailureKindValue], ContainerType, GroupType],
    ) -> Operand[Bare[Scalar], ContainerType, GroupType]: ...

    def name(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.name())

    @overload
    def count(
        self: Operand[Indexed[IndexType, V], ContainerType, GroupType],
    ) -> Operand[Bare[Scalar], Definite, GroupType]: ...

    @overload
    def count(
        self: Operand[Bare[BareValueType], ContainerType, GroupType],
    ) -> Operand[Bare[Scalar], Definite, GroupType]: ...

    def count(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.count())

    @overload
    def sum(
        self: Operand[Indexed[IndexType, Scalar], Multiple[OrderType], GroupType],
    ) -> Operand[Bare[Scalar], Single, GroupType]: ...

    @overload
    def sum(
        self: Operand[Bare[Scalar], Multiple[OrderType], GroupType],
    ) -> Operand[Bare[Scalar], Single, GroupType]: ...

    @overload
    def sum(
        self: Operand[
            Indexed[IndexType, AttributeName], Multiple[OrderType], GroupType
        ],
    ) -> Operand[Bare[AttributeName], Single, GroupType]: ...

    @overload
    def sum(
        self: Operand[Bare[AttributeName], Multiple[OrderType], GroupType],
    ) -> Operand[Bare[AttributeName], Single, GroupType]: ...

    @overload
    def sum(
        self: Operand[
            Indexed[IndexType, IndexValue[NodeIndex]], Multiple[OrderType], GroupType
        ],
    ) -> Operand[Bare[IndexValue[NodeIndex]], Single, GroupType]: ...

    @overload
    def sum(
        self: Operand[Bare[IndexValue[NodeIndex]], Multiple[OrderType], GroupType],
    ) -> Operand[Bare[IndexValue[NodeIndex]], Single, GroupType]: ...

    @overload
    def sum(
        self: Operand[
            Indexed[IndexType, IndexValue[AttributeNameIndex]],
            Multiple[OrderType],
            GroupType,
        ],
    ) -> Operand[Bare[IndexValue[AttributeNameIndex]], Single, GroupType]: ...

    @overload
    def sum(
        self: Operand[
            Bare[IndexValue[AttributeNameIndex]], Multiple[OrderType], GroupType
        ],
    ) -> Operand[Bare[IndexValue[AttributeNameIndex]], Single, GroupType]: ...

    @overload
    def sum(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndex]], Multiple[OrderType], GroupType
        ],
    ) -> Operand[Bare[IndexValue[ValueIndex]], Single, GroupType]: ...

    @overload
    def sum(
        self: Operand[Bare[IndexValue[ValueIndex]], Multiple[OrderType], GroupType],
    ) -> Operand[Bare[IndexValue[ValueIndex]], Single, GroupType]: ...

    @overload
    def sum(
        self: Operand[
            Indexed[IndexType, IndexValue[IntegerIndexType]],
            Multiple[OrderType],
            GroupType,
        ],
    ) -> Operand[Bare[IndexValue[IntegerIndexType]], Single, GroupType]: ...

    @overload
    def sum(
        self: Operand[
            Bare[IndexValue[IntegerIndexType]], Multiple[OrderType], GroupType
        ],
    ) -> Operand[Bare[IndexValue[IntegerIndexType]], Single, GroupType]: ...

    def sum(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.sum())

    @overload
    def mean(
        self: Operand[
            Indexed[IndexType, RealNumericValueType], Multiple[OrderType], GroupType
        ],
    ) -> Operand[Bare[RealNumericValueType], Single, GroupType]: ...

    @overload
    def mean(
        self: Operand[Bare[RealNumericValueType], Multiple[OrderType], GroupType],
    ) -> Operand[Bare[RealNumericValueType], Single, GroupType]: ...

    def mean(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.mean())

    @overload
    def std(
        self: Operand[
            Indexed[IndexType, RealNumericValueType], Multiple[OrderType], GroupType
        ],
    ) -> Operand[Bare[Scalar], Single, GroupType]: ...

    @overload
    def std(
        self: Operand[Bare[RealNumericValueType], Multiple[OrderType], GroupType],
    ) -> Operand[Bare[Scalar], Single, GroupType]: ...

    def std(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.std())

    @overload
    def var(
        self: Operand[
            Indexed[IndexType, RealNumericValueType], Multiple[OrderType], GroupType
        ],
    ) -> Operand[Bare[Scalar], Single, GroupType]: ...

    @overload
    def var(
        self: Operand[Bare[RealNumericValueType], Multiple[OrderType], GroupType],
    ) -> Operand[Bare[Scalar], Single, GroupType]: ...

    def var(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.var())

    @overload
    def all(
        self: Operand[Indexed[IndexType, Mask], Multiple[OrderType], GroupType],
    ) -> Operand[Bare[Mask], Definite, GroupType]: ...

    @overload
    def all(
        self: Operand[Bare[Mask], Multiple[OrderType], GroupType],
    ) -> Operand[Bare[Mask], Definite, GroupType]: ...

    def all(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.all())

    @overload
    def any(
        self: Operand[Indexed[IndexType, Mask], Multiple[OrderType], GroupType],
    ) -> Operand[Bare[Mask], Definite, GroupType]: ...

    @overload
    def any(
        self: Operand[Bare[Mask], Multiple[OrderType], GroupType],
    ) -> Operand[Bare[Mask], Definite, GroupType]: ...

    def any(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.any())

    @overload
    def max(
        self: Operand[
            Indexed[IndexType, OrderableValueType], Multiple[OrderType], GroupType
        ],
    ) -> Operand[Bare[OrderableValueType], Single, GroupType]: ...

    @overload
    def max(
        self: Operand[Bare[OrderableValueType], Multiple[OrderType], GroupType],
    ) -> Operand[Bare[OrderableValueType], Single, GroupType]: ...

    def max(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.max())

    @overload
    def min(
        self: Operand[
            Indexed[IndexType, OrderableValueType], Multiple[OrderType], GroupType
        ],
    ) -> Operand[Bare[OrderableValueType], Single, GroupType]: ...

    @overload
    def min(
        self: Operand[Bare[OrderableValueType], Multiple[OrderType], GroupType],
    ) -> Operand[Bare[OrderableValueType], Single, GroupType]: ...

    def min(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.min())

    @overload
    def median(
        self: Operand[
            Indexed[IndexType, MedianValueType], Multiple[OrderType], GroupType
        ],
    ) -> Operand[Bare[MedianValueType], Single, GroupType]: ...

    @overload
    def median(
        self: Operand[Bare[MedianValueType], Multiple[OrderType], GroupType],
    ) -> Operand[Bare[MedianValueType], Single, GroupType]: ...

    def median(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.median())

    @overload
    def mode(
        self: Operand[
            Indexed[IndexType, ModeValueType], Multiple[OrderType], GroupType
        ],
    ) -> Operand[Bare[ModeValueType], Multiple[OrderType], GroupType]: ...

    @overload
    def mode(
        self: Operand[Bare[ModeValueType], Multiple[OrderType], GroupType],
    ) -> Operand[Bare[ModeValueType], Multiple[OrderType], GroupType]: ...

    @overload
    def mode(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndexType]],
            Multiple[OrderType],
            GroupType,
        ],
    ) -> Operand[Bare[IndexValue[ValueIndexType]], Multiple[OrderType], GroupType]: ...

    @overload
    def mode(
        self: Operand[Bare[IndexValue[ValueIndexType]], Multiple[OrderType], GroupType],
    ) -> Operand[Bare[IndexValue[ValueIndexType]], Multiple[OrderType], GroupType]: ...

    def mode(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.mode())

    @overload
    def product(
        self: Operand[
            Indexed[IndexType, MultipliableValueType], Multiple[OrderType], GroupType
        ],
    ) -> Operand[Bare[MultipliableValueType], Single, GroupType]: ...

    @overload
    def product(
        self: Operand[Bare[MultipliableValueType], Multiple[OrderType], GroupType],
    ) -> Operand[Bare[MultipliableValueType], Single, GroupType]: ...

    def product(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.product())

    @overload
    def n_unique(
        self: Operand[
            Indexed[IndexType, EquivalentValueType], Multiple[OrderType], GroupType
        ],
    ) -> Operand[Bare[Scalar], Definite, GroupType]: ...

    @overload
    def n_unique(
        self: Operand[Bare[EquivalentValueType], Multiple[OrderType], GroupType],
    ) -> Operand[Bare[Scalar], Definite, GroupType]: ...

    @overload
    def n_unique(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndexType]],
            Multiple[OrderType],
            GroupType,
        ],
    ) -> Operand[Bare[Scalar], Definite, GroupType]: ...

    @overload
    def n_unique(
        self: Operand[Bare[IndexValue[ValueIndexType]], Multiple[OrderType], GroupType],
    ) -> Operand[Bare[Scalar], Definite, GroupType]: ...

    def n_unique(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.n_unique())

    @overload
    def random(
        self: Operand[Indexed[IndexType, V], Multiple[OrderType], GroupType],
    ) -> Operand[Indexed[IndexType, V], Single, GroupType]: ...

    @overload
    def random(
        self: Operand[Bare[BareValueType], Multiple[OrderType], GroupType],
    ) -> Operand[Bare[BareValueType], Single, GroupType]: ...

    def random(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.random())

    def edges(
        self: Union[
            Operand[Indexed[NodeIndex, Unit], ContainerType, GroupType],
            Operand[Indexed[IndexType, NodeReference], ContainerType, GroupType],
        ],
        direction: EdgeDirection,
    ) -> Operand[Indexed[EdgeIndex, Unit], Multiple[Unordered], GroupType]:
        return Operand._from_py_operand(self._operand.edges(direction))

    def neighbors(
        self: Union[
            Operand[Indexed[NodeIndex, Unit], ContainerType, GroupType],
            Operand[Indexed[IndexType, NodeReference], ContainerType, GroupType],
        ],
        direction: EdgeDirection,
    ) -> Operand[Indexed[NodeIndex, Unit], Multiple[Unordered], GroupType]:
        return Operand._from_py_operand(self._operand.neighbors(direction))

    @overload
    def via_edges(
        self: Operand[Indexed[NodeIndex, Unit], ContainerType, GroupType],
        direction: EdgeDirection,
    ) -> Operand[
        Indexed[Expanded[NodeIndex, EdgeIndex], EdgeReference],
        Multiple[Unordered],
        GroupType,
    ]: ...

    @overload
    def via_edges(
        self: Operand[Indexed[IndexType, NodeReference], ContainerType, GroupType],
        direction: EdgeDirection,
    ) -> Operand[
        Indexed[Expanded[IndexType, EdgeIndex], EdgeReference],
        Multiple[Unordered],
        GroupType,
    ]: ...

    def via_edges(self, direction: EdgeDirection) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.via_edges(direction))

    @overload
    def via_neighbors(
        self: Operand[Indexed[NodeIndex, Unit], ContainerType, GroupType],
        direction: EdgeDirection,
    ) -> Operand[
        Indexed[Expanded[NodeIndex, NodeIndex], NodeReference],
        Multiple[Unordered],
        GroupType,
    ]: ...

    @overload
    def via_neighbors(
        self: Operand[Indexed[IndexType, NodeReference], ContainerType, GroupType],
        direction: EdgeDirection,
    ) -> Operand[
        Indexed[Expanded[IndexType, NodeIndex], NodeReference],
        Multiple[Unordered],
        GroupType,
    ]: ...

    def via_neighbors(self, direction: EdgeDirection) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.via_neighbors(direction))

    def nodes(
        self: Union[
            Operand[Indexed[EdgeIndex, Unit], ContainerType, GroupType],
            Operand[Indexed[IndexType, EdgeReference], ContainerType, GroupType],
        ],
    ) -> Operand[Indexed[NodeIndex, Unit], Multiple[Unordered], GroupType]:
        return Operand._from_py_operand(self._operand.nodes())

    @overload
    def via_nodes(
        self: Operand[Indexed[EdgeIndex, Unit], Multiple[Unordered], GroupType],
    ) -> Operand[
        Indexed[Expanded[EdgeIndex, EndpointRole], NodeReference],
        Multiple[Unordered],
        GroupType,
    ]: ...

    @overload
    def via_nodes(
        self: Operand[Indexed[EdgeIndex, Unit], Multiple[Ordered], GroupType],
    ) -> Operand[
        Indexed[Expanded[EdgeIndex, EndpointRole], NodeReference],
        Multiple[Ordered],
        GroupType,
    ]: ...

    @overload
    def via_nodes(
        self: Operand[Indexed[EdgeIndex, Unit], Single, GroupType],
    ) -> Operand[
        Indexed[Expanded[EdgeIndex, EndpointRole], NodeReference],
        Multiple[Ordered],
        GroupType,
    ]: ...

    @overload
    def via_nodes(
        self: Operand[Indexed[EdgeIndex, Unit], Definite, GroupType],
    ) -> Operand[
        Indexed[Expanded[EdgeIndex, EndpointRole], NodeReference],
        Multiple[Ordered],
        GroupType,
    ]: ...

    @overload
    def via_nodes(
        self: Operand[
            Indexed[IndexType, EdgeReference], Multiple[Unordered], GroupType
        ],
    ) -> Operand[
        Indexed[Expanded[IndexType, EndpointRole], NodeReference],
        Multiple[Unordered],
        GroupType,
    ]: ...

    @overload
    def via_nodes(
        self: Operand[Indexed[IndexType, EdgeReference], Multiple[Ordered], GroupType],
    ) -> Operand[
        Indexed[Expanded[IndexType, EndpointRole], NodeReference],
        Multiple[Ordered],
        GroupType,
    ]: ...

    @overload
    def via_nodes(
        self: Operand[Indexed[IndexType, EdgeReference], Single, GroupType],
    ) -> Operand[
        Indexed[Expanded[IndexType, EndpointRole], NodeReference],
        Multiple[Ordered],
        GroupType,
    ]: ...

    @overload
    def via_nodes(
        self: Operand[Indexed[IndexType, EdgeReference], Definite, GroupType],
    ) -> Operand[
        Indexed[Expanded[IndexType, EndpointRole], NodeReference],
        Multiple[Ordered],
        GroupType,
    ]: ...

    def via_nodes(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.via_nodes())

    @overload
    def source_node(
        self: Union[
            Operand[Indexed[EdgeIndex, Unit], Multiple[OrderType], GroupType],
            Operand[Indexed[IndexType, EdgeReference], Multiple[OrderType], GroupType],
        ],
    ) -> Operand[Indexed[NodeIndex, Unit], Multiple[Unordered], GroupType]: ...

    @overload
    def source_node(
        self: Union[
            Operand[Indexed[EdgeIndex, Unit], Single, GroupType],
            Operand[Indexed[IndexType, EdgeReference], Single, GroupType],
        ],
    ) -> Operand[Indexed[NodeIndex, Unit], Single, GroupType]: ...

    @overload
    def source_node(
        self: Union[
            Operand[Indexed[EdgeIndex, Unit], Definite, GroupType],
            Operand[Indexed[IndexType, EdgeReference], Definite, GroupType],
        ],
    ) -> Operand[Indexed[NodeIndex, Unit], Definite, GroupType]: ...

    def source_node(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.source_node())

    @overload
    def target_node(
        self: Union[
            Operand[Indexed[EdgeIndex, Unit], Multiple[OrderType], GroupType],
            Operand[Indexed[IndexType, EdgeReference], Multiple[OrderType], GroupType],
        ],
    ) -> Operand[Indexed[NodeIndex, Unit], Multiple[Unordered], GroupType]: ...

    @overload
    def target_node(
        self: Union[
            Operand[Indexed[EdgeIndex, Unit], Single, GroupType],
            Operand[Indexed[IndexType, EdgeReference], Single, GroupType],
        ],
    ) -> Operand[Indexed[NodeIndex, Unit], Single, GroupType]: ...

    @overload
    def target_node(
        self: Union[
            Operand[Indexed[EdgeIndex, Unit], Definite, GroupType],
            Operand[Indexed[IndexType, EdgeReference], Definite, GroupType],
        ],
    ) -> Operand[Indexed[NodeIndex, Unit], Definite, GroupType]: ...

    def target_node(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.target_node())

    @overload
    def via_source_node(
        self: Operand[Indexed[EdgeIndex, Unit], ContainerType, GroupType],
    ) -> Operand[Indexed[EdgeIndex, NodeReference], ContainerType, GroupType]: ...

    @overload
    def via_source_node(
        self: Operand[Indexed[IndexType, EdgeReference], ContainerType, GroupType],
    ) -> Operand[Indexed[IndexType, NodeReference], ContainerType, GroupType]: ...

    def via_source_node(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.via_source_node())

    @overload
    def via_target_node(
        self: Operand[Indexed[EdgeIndex, Unit], ContainerType, GroupType],
    ) -> Operand[Indexed[EdgeIndex, NodeReference], ContainerType, GroupType]: ...

    @overload
    def via_target_node(
        self: Operand[Indexed[IndexType, EdgeReference], ContainerType, GroupType],
    ) -> Operand[Indexed[IndexType, NodeReference], ContainerType, GroupType]: ...

    def via_target_node(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.via_target_node())

    @overload
    def group_by(
        self: Operand[Indexed[IndexType, V], ContainerType, GroupType],
        key: Union[ScalarValue, GroupingArgument[IndexType, Scalar, ArgumentOrderType]],
    ) -> Operand[
        Indexed[IndexType, V], ContainerType, Grouped[IndexType, ValueIndex, GroupType]
    ]: ...

    @overload
    def group_by(
        self: Operand[Indexed[IndexType, V], ContainerType, GroupType],
        key: GroupingArgument[IndexType, Mask, ArgumentOrderType],
    ) -> Operand[
        Indexed[IndexType, V], ContainerType, Grouped[IndexType, BoolIndex, GroupType]
    ]: ...

    @overload
    def group_by(
        self: Operand[Indexed[IndexType, V], ContainerType, GroupType],
        key: GroupingArgument[IndexType, AttributeName, ArgumentOrderType],
    ) -> Operand[
        Indexed[IndexType, V],
        ContainerType,
        Grouped[IndexType, AttributeNameIndex, GroupType],
    ]: ...

    @overload
    def group_by(
        self: Operand[Indexed[IndexType, V], ContainerType, GroupType],
        key: GroupingArgument[IndexType, FailureKindValue, ArgumentOrderType],
    ) -> Operand[
        Indexed[IndexType, V],
        ContainerType,
        Grouped[IndexType, FailureKindIndex, GroupType],
    ]: ...

    @overload
    def group_by(
        self: Operand[Indexed[IndexType, V], ContainerType, GroupType],
        key: GroupingArgument[IndexType, IndexValue[K], ArgumentOrderType],
    ) -> Operand[
        Indexed[IndexType, V], ContainerType, Grouped[IndexType, K, GroupType]
    ]: ...

    @overload
    def group_by(
        self: Operand[Indexed[IndexType, V], ContainerType, GroupType],
        key: GroupingArgument[IndexType, NodeReference, ArgumentOrderType],
    ) -> Operand[
        Indexed[IndexType, V], ContainerType, Grouped[IndexType, NodeIndex, GroupType]
    ]: ...

    @overload
    def group_by(
        self: Operand[Indexed[IndexType, V], ContainerType, GroupType],
        key: GroupingArgument[IndexType, EdgeReference, ArgumentOrderType],
    ) -> Operand[
        Indexed[IndexType, V], ContainerType, Grouped[IndexType, EdgeIndex, GroupType]
    ]: ...

    def group_by(
        self,
        key: Union[ScalarValue, Operand[Any, Any, Any], Argument[Any, Any]],
    ) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(
            self._operand.group_by(Operand._to_py_argument(key))
        )

    def having(
        self: Operand[S, C, Grouped[MemberIndexType, KeyIndexType, OuterGroupType]],
        predicate: MaskArgument[KeyIndexType, ArgumentOrderType],
    ) -> Operand[S, C, Grouped[MemberIndexType, KeyIndexType, OuterGroupType]]:
        return Operand._from_py_operand(
            self._operand.having(Operand._to_py_argument(predicate))
        )

    def keys(
        self: Operand[S, C, Grouped[MemberIndexType, KeyIndexType, OuterGroupType]],
    ) -> Operand[Indexed[KeyIndexType, Unit], Multiple[Unordered], OuterGroupType]:
        return Operand._from_py_operand(self._operand.keys())

    @overload
    def ungroup(
        self: Operand[
            Indexed[IndexType, V],
            ContainerType,
            Grouped[MemberIndexType, KeyIndexType, OuterGroupType],
        ],
    ) -> Operand[Indexed[IndexType, V], Multiple[Unordered], OuterGroupType]: ...

    @overload
    def ungroup(
        self: Operand[
            Bare[BareValueType],
            ContainerType,
            Grouped[MemberIndexType, KeyIndexType, OuterGroupType],
        ],
    ) -> Operand[Bare[BareValueType], Multiple[Unordered], OuterGroupType]: ...

    def ungroup(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.ungroup())

    @overload
    def ungroup_keyed(
        self: Operand[
            Indexed[IndexType, V],
            Single,
            Grouped[MemberIndexType, KeyIndexType, OuterGroupType],
        ],
    ) -> Operand[Indexed[KeyIndexType, V], Multiple[Unordered], OuterGroupType]: ...

    @overload
    def ungroup_keyed(
        self: Operand[
            Indexed[IndexType, V],
            Definite,
            Grouped[MemberIndexType, KeyIndexType, OuterGroupType],
        ],
    ) -> Operand[Indexed[KeyIndexType, V], Multiple[Unordered], OuterGroupType]: ...

    @overload
    def ungroup_keyed(
        self: Operand[
            Bare[BareValueType],
            Single,
            Grouped[MemberIndexType, KeyIndexType, OuterGroupType],
        ],
    ) -> Operand[
        Indexed[KeyIndexType, BareValueType], Multiple[Unordered], OuterGroupType
    ]: ...

    @overload
    def ungroup_keyed(
        self: Operand[
            Bare[BareValueType],
            Definite,
            Grouped[MemberIndexType, KeyIndexType, OuterGroupType],
        ],
    ) -> Operand[
        Indexed[KeyIndexType, BareValueType], Multiple[Unordered], OuterGroupType
    ]: ...

    def ungroup_keyed(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.ungroup_keyed())

    @overload
    def broadcast(
        self: Operand[
            Indexed[IndexType, V],
            Single,
            Grouped[MemberIndexType, KeyIndexType, OuterGroupType],
        ],
    ) -> Operand[Indexed[MemberIndexType, V], Multiple[Unordered], OuterGroupType]: ...

    @overload
    def broadcast(
        self: Operand[
            Indexed[IndexType, V],
            Definite,
            Grouped[MemberIndexType, KeyIndexType, OuterGroupType],
        ],
    ) -> Operand[Indexed[MemberIndexType, V], Multiple[Unordered], OuterGroupType]: ...

    @overload
    def broadcast(
        self: Operand[
            Bare[BareValueType],
            Single,
            Grouped[MemberIndexType, KeyIndexType, OuterGroupType],
        ],
    ) -> Operand[
        Indexed[MemberIndexType, BareValueType], Multiple[Unordered], OuterGroupType
    ]: ...

    @overload
    def broadcast(
        self: Operand[
            Bare[BareValueType],
            Definite,
            Grouped[MemberIndexType, KeyIndexType, OuterGroupType],
        ],
    ) -> Operand[
        Indexed[MemberIndexType, BareValueType], Multiple[Unordered], OuterGroupType
    ]: ...

    def broadcast(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.broadcast())

    @overload
    def broadcast_via(
        self: Union[
            Operand[
                Indexed[IndexType, V],
                Single,
                Grouped[MemberIndexType, ValueIndex, OuterGroupType],
            ],
            Operand[
                Indexed[IndexType, V],
                Definite,
                Grouped[MemberIndexType, ValueIndex, OuterGroupType],
            ],
        ],
        population: Operand[
            Indexed[PopulationIndexType, Scalar], PopulationContainerType, Ungrouped
        ],
    ) -> Operand[
        Indexed[PopulationIndexType, V], PopulationContainerType, OuterGroupType
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Operand[
                Bare[BareValueType],
                Single,
                Grouped[MemberIndexType, ValueIndex, OuterGroupType],
            ],
            Operand[
                Bare[BareValueType],
                Definite,
                Grouped[MemberIndexType, ValueIndex, OuterGroupType],
            ],
        ],
        population: Operand[
            Indexed[PopulationIndexType, Scalar], PopulationContainerType, Ungrouped
        ],
    ) -> Operand[
        Indexed[PopulationIndexType, BareValueType],
        PopulationContainerType,
        OuterGroupType,
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Operand[
                Indexed[IndexType, V],
                Single,
                Grouped[MemberIndexType, BoolIndex, OuterGroupType],
            ],
            Operand[
                Indexed[IndexType, V],
                Definite,
                Grouped[MemberIndexType, BoolIndex, OuterGroupType],
            ],
        ],
        population: Operand[
            Indexed[PopulationIndexType, Mask], PopulationContainerType, Ungrouped
        ],
    ) -> Operand[
        Indexed[PopulationIndexType, V], PopulationContainerType, OuterGroupType
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Operand[
                Bare[BareValueType],
                Single,
                Grouped[MemberIndexType, BoolIndex, OuterGroupType],
            ],
            Operand[
                Bare[BareValueType],
                Definite,
                Grouped[MemberIndexType, BoolIndex, OuterGroupType],
            ],
        ],
        population: Operand[
            Indexed[PopulationIndexType, Mask], PopulationContainerType, Ungrouped
        ],
    ) -> Operand[
        Indexed[PopulationIndexType, BareValueType],
        PopulationContainerType,
        OuterGroupType,
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Operand[
                Indexed[IndexType, V],
                Single,
                Grouped[MemberIndexType, AttributeNameIndex, OuterGroupType],
            ],
            Operand[
                Indexed[IndexType, V],
                Definite,
                Grouped[MemberIndexType, AttributeNameIndex, OuterGroupType],
            ],
        ],
        population: Operand[
            Indexed[PopulationIndexType, AttributeName],
            PopulationContainerType,
            Ungrouped,
        ],
    ) -> Operand[
        Indexed[PopulationIndexType, V], PopulationContainerType, OuterGroupType
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Operand[
                Bare[BareValueType],
                Single,
                Grouped[MemberIndexType, AttributeNameIndex, OuterGroupType],
            ],
            Operand[
                Bare[BareValueType],
                Definite,
                Grouped[MemberIndexType, AttributeNameIndex, OuterGroupType],
            ],
        ],
        population: Operand[
            Indexed[PopulationIndexType, AttributeName],
            PopulationContainerType,
            Ungrouped,
        ],
    ) -> Operand[
        Indexed[PopulationIndexType, BareValueType],
        PopulationContainerType,
        OuterGroupType,
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Operand[
                Indexed[IndexType, V],
                Single,
                Grouped[MemberIndexType, FailureKindIndex, OuterGroupType],
            ],
            Operand[
                Indexed[IndexType, V],
                Definite,
                Grouped[MemberIndexType, FailureKindIndex, OuterGroupType],
            ],
        ],
        population: Operand[
            Indexed[PopulationIndexType, FailureKindValue],
            PopulationContainerType,
            Ungrouped,
        ],
    ) -> Operand[
        Indexed[PopulationIndexType, V], PopulationContainerType, OuterGroupType
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Operand[
                Bare[BareValueType],
                Single,
                Grouped[MemberIndexType, FailureKindIndex, OuterGroupType],
            ],
            Operand[
                Bare[BareValueType],
                Definite,
                Grouped[MemberIndexType, FailureKindIndex, OuterGroupType],
            ],
        ],
        population: Operand[
            Indexed[PopulationIndexType, FailureKindValue],
            PopulationContainerType,
            Ungrouped,
        ],
    ) -> Operand[
        Indexed[PopulationIndexType, BareValueType],
        PopulationContainerType,
        OuterGroupType,
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Operand[
                Indexed[IndexType, V],
                Single,
                Grouped[MemberIndexType, KeyIndexType, OuterGroupType],
            ],
            Operand[
                Indexed[IndexType, V],
                Definite,
                Grouped[MemberIndexType, KeyIndexType, OuterGroupType],
            ],
        ],
        population: Operand[
            Indexed[PopulationIndexType, IndexValue[KeyIndexType]],
            PopulationContainerType,
            Ungrouped,
        ],
    ) -> Operand[
        Indexed[PopulationIndexType, V], PopulationContainerType, OuterGroupType
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Operand[
                Bare[BareValueType],
                Single,
                Grouped[MemberIndexType, KeyIndexType, OuterGroupType],
            ],
            Operand[
                Bare[BareValueType],
                Definite,
                Grouped[MemberIndexType, KeyIndexType, OuterGroupType],
            ],
        ],
        population: Operand[
            Indexed[PopulationIndexType, IndexValue[KeyIndexType]],
            PopulationContainerType,
            Ungrouped,
        ],
    ) -> Operand[
        Indexed[PopulationIndexType, BareValueType],
        PopulationContainerType,
        OuterGroupType,
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Operand[
                Indexed[IndexType, V],
                Single,
                Grouped[MemberIndexType, NodeIndex, OuterGroupType],
            ],
            Operand[
                Indexed[IndexType, V],
                Definite,
                Grouped[MemberIndexType, NodeIndex, OuterGroupType],
            ],
        ],
        population: Operand[
            Indexed[PopulationIndexType, NodeReference],
            PopulationContainerType,
            Ungrouped,
        ],
    ) -> Operand[
        Indexed[PopulationIndexType, V], PopulationContainerType, OuterGroupType
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Operand[
                Bare[BareValueType],
                Single,
                Grouped[MemberIndexType, NodeIndex, OuterGroupType],
            ],
            Operand[
                Bare[BareValueType],
                Definite,
                Grouped[MemberIndexType, NodeIndex, OuterGroupType],
            ],
        ],
        population: Operand[
            Indexed[PopulationIndexType, NodeReference],
            PopulationContainerType,
            Ungrouped,
        ],
    ) -> Operand[
        Indexed[PopulationIndexType, BareValueType],
        PopulationContainerType,
        OuterGroupType,
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Operand[
                Indexed[IndexType, V],
                Single,
                Grouped[MemberIndexType, EdgeIndex, OuterGroupType],
            ],
            Operand[
                Indexed[IndexType, V],
                Definite,
                Grouped[MemberIndexType, EdgeIndex, OuterGroupType],
            ],
        ],
        population: Operand[
            Indexed[PopulationIndexType, EdgeReference],
            PopulationContainerType,
            Ungrouped,
        ],
    ) -> Operand[
        Indexed[PopulationIndexType, V], PopulationContainerType, OuterGroupType
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Operand[
                Bare[BareValueType],
                Single,
                Grouped[MemberIndexType, EdgeIndex, OuterGroupType],
            ],
            Operand[
                Bare[BareValueType],
                Definite,
                Grouped[MemberIndexType, EdgeIndex, OuterGroupType],
            ],
        ],
        population: Operand[
            Indexed[PopulationIndexType, EdgeReference],
            PopulationContainerType,
            Ungrouped,
        ],
    ) -> Operand[
        Indexed[PopulationIndexType, BareValueType],
        PopulationContainerType,
        OuterGroupType,
    ]: ...

    def broadcast_via(
        self,
        population: Operand[Any, Any, Ungrouped],
    ) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(
            self._operand.broadcast_via(population._operand)
        )

    @overload
    def bucket_errors(
        self: Operand[
            Indexed[IndexType, V],
            ContainerType,
            Grouped[MemberIndexType, KeyIndexType, OuterGroupType],
        ],
    ) -> Operand[
        Indexed[KeyIndexType, FailureValue], Multiple[Unordered], OuterGroupType
    ]: ...

    @overload
    def bucket_errors(
        self: Operand[
            Bare[BareValueType],
            ContainerType,
            Grouped[MemberIndexType, KeyIndexType, OuterGroupType],
        ],
    ) -> Operand[
        Indexed[KeyIndexType, FailureValue], Multiple[Unordered], OuterGroupType
    ]: ...

    def bucket_errors(self) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.bucket_errors())

    def key_errors(
        self: Operand[S, C, Grouped[MemberIndexType, KeyIndexType, OuterGroupType]],
    ) -> Operand[
        Indexed[MemberIndexType, FailureValue], Multiple[Unordered], OuterGroupType
    ]:
        return Operand._from_py_operand(self._operand.key_errors())

    @overload
    def on_bucket_error(
        self: Operand[
            Indexed[IndexType, V],
            ContainerType,
            Grouped[MemberIndexType, KeyIndexType, OuterGroupType],
        ],
        policy: Union[Drop, Raise],
    ) -> Operand[
        Indexed[IndexType, V],
        ContainerType,
        Grouped[MemberIndexType, KeyIndexType, OuterGroupType],
    ]: ...

    @overload
    def on_bucket_error(
        self: Operand[
            Bare[BareValueType],
            ContainerType,
            Grouped[MemberIndexType, KeyIndexType, OuterGroupType],
        ],
        policy: Union[Drop, Raise],
    ) -> Operand[
        Bare[BareValueType],
        ContainerType,
        Grouped[MemberIndexType, KeyIndexType, OuterGroupType],
    ]: ...

    def on_bucket_error(self, policy: Union[Drop, Raise]) -> Operand[Any, Any, Any]:
        if isinstance(policy, Drop):
            return Operand._from_py_operand(self._operand.on_bucket_error_drop())

        return Operand._from_py_operand(self._operand.on_bucket_error_raise())

    @overload
    def on_key_error(
        self: Operand[
            Indexed[IndexType, V],
            ContainerType,
            Grouped[MemberIndexType, KeyIndexType, OuterGroupType],
        ],
        policy: Union[Drop, Raise],
    ) -> Operand[
        Indexed[IndexType, V],
        ContainerType,
        Grouped[MemberIndexType, KeyIndexType, OuterGroupType],
    ]: ...

    @overload
    def on_key_error(
        self: Operand[
            Bare[BareValueType],
            ContainerType,
            Grouped[MemberIndexType, KeyIndexType, OuterGroupType],
        ],
        policy: Union[Drop, Raise],
    ) -> Operand[
        Bare[BareValueType],
        ContainerType,
        Grouped[MemberIndexType, KeyIndexType, OuterGroupType],
    ]: ...

    def on_key_error(self, policy: Union[Drop, Raise]) -> Operand[Any, Any, Any]:
        if isinstance(policy, Drop):
            return Operand._from_py_operand(self._operand.on_key_error_drop())

        return Operand._from_py_operand(self._operand.on_key_error_raise())

    @overload
    def transition(
        self: Operand[Indexed[IndexType, Scalar], ContainerType, GroupType],
        target: ValueTarget[ScalarTransitionValueType],
    ) -> Operand[
        Indexed[IndexType, ScalarTransitionValueType], ContainerType, GroupType
    ]: ...

    @overload
    def transition(
        self: Operand[Bare[Scalar], ContainerType, GroupType],
        target: ValueTarget[ScalarTransitionValueType],
    ) -> Operand[Bare[ScalarTransitionValueType], ContainerType, GroupType]: ...

    @overload
    def transition(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndex]], ContainerType, GroupType
        ],
        target: ValueTarget[ValueIndexTransitionValueType],
    ) -> Operand[
        Indexed[IndexType, ValueIndexTransitionValueType], ContainerType, GroupType
    ]: ...

    @overload
    def transition(
        self: Operand[Bare[IndexValue[ValueIndex]], ContainerType, GroupType],
        target: ValueTarget[ValueIndexTransitionValueType],
    ) -> Operand[Bare[ValueIndexTransitionValueType], ContainerType, GroupType]: ...

    @overload
    def transition(
        self: Operand[Indexed[IndexType, AttributeName], ContainerType, GroupType],
        target: ValueTarget[AttributeNameTransitionValueType],
    ) -> Operand[
        Indexed[IndexType, AttributeNameTransitionValueType], ContainerType, GroupType
    ]: ...

    @overload
    def transition(
        self: Operand[Bare[AttributeName], ContainerType, GroupType],
        target: ValueTarget[AttributeNameTransitionValueType],
    ) -> Operand[Bare[AttributeNameTransitionValueType], ContainerType, GroupType]: ...

    @overload
    def transition(
        self: Operand[
            Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, GroupType
        ],
        target: ValueTarget[NodeIndexTransitionValueType],
    ) -> Operand[
        Indexed[IndexType, NodeIndexTransitionValueType], ContainerType, GroupType
    ]: ...

    @overload
    def transition(
        self: Operand[Bare[IndexValue[NodeIndex]], ContainerType, GroupType],
        target: ValueTarget[NodeIndexTransitionValueType],
    ) -> Operand[Bare[NodeIndexTransitionValueType], ContainerType, GroupType]: ...

    @overload
    def transition(
        self: Operand[
            Indexed[IndexType, IndexValue[AttributeNameIndex]], ContainerType, GroupType
        ],
        target: ValueTarget[AttributeNameIndexTransitionValueType],
    ) -> Operand[
        Indexed[IndexType, AttributeNameIndexTransitionValueType],
        ContainerType,
        GroupType,
    ]: ...

    @overload
    def transition(
        self: Operand[Bare[IndexValue[AttributeNameIndex]], ContainerType, GroupType],
        target: ValueTarget[AttributeNameIndexTransitionValueType],
    ) -> Operand[
        Bare[AttributeNameIndexTransitionValueType], ContainerType, GroupType
    ]: ...

    @overload
    def transition(
        self: Operand[
            Indexed[IndexType, IndexValue[EdgeIndex]], ContainerType, GroupType
        ],
        target: ValueTarget[EdgeIndexTransitionValueType],
    ) -> Operand[
        Indexed[IndexType, EdgeIndexTransitionValueType], ContainerType, GroupType
    ]: ...

    @overload
    def transition(
        self: Operand[Bare[IndexValue[EdgeIndex]], ContainerType, GroupType],
        target: ValueTarget[EdgeIndexTransitionValueType],
    ) -> Operand[Bare[EdgeIndexTransitionValueType], ContainerType, GroupType]: ...

    @overload
    def transition(
        self: Operand[
            Indexed[IndexType, IndexValue[Positional]], ContainerType, GroupType
        ],
        target: ValueTarget[PositionalTransitionValueType],
    ) -> Operand[
        Indexed[IndexType, PositionalTransitionValueType], ContainerType, GroupType
    ]: ...

    @overload
    def transition(
        self: Operand[Bare[IndexValue[Positional]], ContainerType, GroupType],
        target: ValueTarget[PositionalTransitionValueType],
    ) -> Operand[Bare[PositionalTransitionValueType], ContainerType, GroupType]: ...

    @overload
    def transition(
        self: Operand[Indexed[IndexType, Mask], ContainerType, GroupType],
        target: ValueTarget[MaskTransitionValueType],
    ) -> Operand[
        Indexed[IndexType, MaskTransitionValueType], ContainerType, GroupType
    ]: ...

    @overload
    def transition(
        self: Operand[Bare[Mask], ContainerType, GroupType],
        target: ValueTarget[MaskTransitionValueType],
    ) -> Operand[Bare[MaskTransitionValueType], ContainerType, GroupType]: ...

    @overload
    def transition(
        self: Operand[
            Indexed[IndexType, IndexValue[BoolIndex]], ContainerType, GroupType
        ],
        target: ValueTarget[BoolIndexTransitionValueType],
    ) -> Operand[
        Indexed[IndexType, BoolIndexTransitionValueType], ContainerType, GroupType
    ]: ...

    @overload
    def transition(
        self: Operand[Bare[IndexValue[BoolIndex]], ContainerType, GroupType],
        target: ValueTarget[BoolIndexTransitionValueType],
    ) -> Operand[Bare[BoolIndexTransitionValueType], ContainerType, GroupType]: ...

    @overload
    def transition(
        self: Operand[Indexed[IndexType, FailureKindValue], ContainerType, GroupType],
        target: ValueTarget[IndexValue[FailureKindIndex]],
    ) -> Operand[
        Indexed[IndexType, IndexValue[FailureKindIndex]], ContainerType, GroupType
    ]: ...

    @overload
    def transition(
        self: Operand[Bare[FailureKindValue], ContainerType, GroupType],
        target: ValueTarget[IndexValue[FailureKindIndex]],
    ) -> Operand[Bare[IndexValue[FailureKindIndex]], ContainerType, GroupType]: ...

    @overload
    def transition(
        self: Operand[
            Indexed[IndexType, IndexValue[FailureKindIndex]], ContainerType, GroupType
        ],
        target: ValueTarget[FailureKindValue],
    ) -> Operand[Indexed[IndexType, FailureKindValue], ContainerType, GroupType]: ...

    @overload
    def transition(
        self: Operand[Bare[IndexValue[FailureKindIndex]], ContainerType, GroupType],
        target: ValueTarget[FailureKindValue],
    ) -> Operand[Bare[FailureKindValue], ContainerType, GroupType]: ...

    def transition(self, target: ValueTarget[Any]) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(self._operand.transition(target._target))

    @overload
    def expand_to(
        self: Operand[
            Indexed[Expanded[IndexType, ChildType], TemplateValueType],
            ContainerType,
            GroupType,
        ],
        values: ScalarValue,
    ) -> Operand[
        Indexed[Expanded[IndexType, ChildType], Scalar], ContainerType, GroupType
    ]: ...

    @overload
    def expand_to(
        self: Operand[
            Indexed[Expanded[IndexType, ChildType], TemplateValueType],
            ContainerType,
            GroupType,
        ],
        values: IndexedOperandArgument[IndexType, ExpandedValueType, ArgumentOrderType],
    ) -> Operand[
        Indexed[Expanded[IndexType, ChildType], ExpandedValueType],
        ContainerType,
        GroupType,
    ]: ...

    @overload
    def expand_to(
        self: Operand[
            Indexed[Expanded[IndexType, ChildType], TemplateValueType],
            Definite,
            GroupType,
        ],
        values: IndexedDroppingArgument[IndexType, ExpandedValueType],
    ) -> Operand[
        Indexed[Expanded[IndexType, ChildType], ExpandedValueType], Single, GroupType
    ]: ...

    @overload
    def expand_to(
        self: Operand[
            Indexed[Expanded[IndexType, ChildType], TemplateValueType],
            DroppedContainerType,
            GroupType,
        ],
        values: IndexedDroppingArgument[IndexType, ExpandedValueType],
    ) -> Operand[
        Indexed[Expanded[IndexType, ChildType], ExpandedValueType],
        DroppedContainerType,
        GroupType,
    ]: ...

    def expand_to(
        self,
        values: Union[ScalarValue, Operand[Any, Any, Any], Argument[Any, Any]],
    ) -> Operand[Any, Any, Any]:
        return Operand._from_py_operand(
            self._operand.expand_to(Operand._to_py_argument(values))
        )

    __add__ = add
    __sub__ = subtract
    __mul__ = multiply
    __truediv__ = divide
    __pow__ = power
    __mod__ = modulo
    __gt__ = greater_than
    __ge__ = greater_than_or_equal_to
    __lt__ = less_than
    __le__ = less_than_or_equal_to
    __and__ = and_
    __or__ = or_
    __xor__ = xor
    __invert__ = not_
    __abs__ = abs
    __neg__ = neg


IndexedOperandArgument: TypeAlias = Union[
    Operand[Indexed[IndexType, V], Multiple[ArgumentOrderType], Ungrouped],
    Operand[Bare[V], Single, Ungrouped],
    Operand[Bare[V], Definite, Ungrouped],
    Argument[Indexed[IndexType, V], Preserving],
    Argument[Bare[V], Preserving],
]
BareOperandArgument: TypeAlias = Union[
    Operand[Bare[V], Single, Ungrouped],
    Operand[Bare[V], Definite, Ungrouped],
    Argument[Bare[V], Preserving],
]
MaskArgument: TypeAlias = Union[
    bool,
    Operand[Indexed[IndexType, Mask], Multiple[ArgumentOrderType], Ungrouped],
    Operand[Bare[Mask], Single, Ungrouped],
    Operand[Bare[Mask], Definite, Ungrouped],
    Argument[Indexed[IndexType, Mask], Preserving],
    Argument[Bare[Mask], Preserving],
    Argument[Indexed[IndexType, Mask], Dropping],
    Argument[Bare[Mask], Dropping],
]
BareMaskArgument: TypeAlias = Union[
    bool,
    Operand[Bare[Mask], Single, Ungrouped],
    Operand[Bare[Mask], Definite, Ungrouped],
    Argument[Bare[Mask], Preserving],
    Argument[Bare[Mask], Dropping],
]
IndexedDroppingArgument: TypeAlias = Union[
    Argument[Indexed[IndexType, V], Dropping], Argument[Bare[V], Dropping]
]
GroupingArgument: TypeAlias = Union[
    Operand[Indexed[IndexType, V], Multiple[ArgumentOrderType], Ungrouped],
    Operand[Bare[V], Single, Ungrouped],
    Operand[Bare[V], Definite, Ungrouped],
    Argument[Indexed[IndexType, V], Preserving],
    Argument[Bare[V], Preserving],
    Argument[Indexed[IndexType, V], Dropping],
    Argument[Bare[V], Dropping],
]
MembershipArgument: TypeAlias = Union[
    Operand[Indexed[Any, V], Any, Ungrouped], Operand[Bare[V], Any, Ungrouped]
]
BareDroppingArgument: TypeAlias = Argument[Bare[V], Dropping]
IndexedStringArgument: TypeAlias = Union[
    str, IndexedOperandArgument[IndexType, StringArgumentValueType, ArgumentOrderType]
]
BareStringArgument: TypeAlias = Union[str, BareOperandArgument[StringArgumentValueType]]
IndexedAnyStringArgument: TypeAlias = Union[
    IndexedStringArgument[IndexType, StringArgumentValueType, ArgumentOrderType],
    IndexedDroppingArgument[IndexType, StringArgumentValueType],
]
BareAnyStringArgument: TypeAlias = Union[
    BareStringArgument[StringArgumentValueType],
    BareDroppingArgument[StringArgumentValueType],
]
IndexedIntegerArgument: TypeAlias = Union[
    int, IndexedOperandArgument[IndexType, IntegerValueType, ArgumentOrderType]
]
BareIntegerArgument: TypeAlias = Union[int, BareOperandArgument[IntegerValueType]]
IndexedAnyIntegerArgument: TypeAlias = Union[
    IndexedIntegerArgument[IndexType, IntegerValueType, ArgumentOrderType],
    IndexedDroppingArgument[IndexType, IntegerValueType],
]
BareAnyIntegerArgument: TypeAlias = Union[
    BareIntegerArgument[IntegerValueType], BareDroppingArgument[IntegerValueType]
]
IndexedScalarArgument: TypeAlias = Union[
    ScalarValue, IndexedOperandArgument[IndexType, V, ArgumentOrderType]
]
BareScalarArgument: TypeAlias = Union[ScalarValue, BareOperandArgument[V]]
IndexedAnyScalarArgument: TypeAlias = Union[
    IndexedScalarArgument[IndexType, V, ArgumentOrderType],
    IndexedDroppingArgument[IndexType, V],
]
IndexedAttributeArgument: TypeAlias = Union[
    Attribute, IndexedOperandArgument[IndexType, V, ArgumentOrderType]
]
BareAttributeArgument: TypeAlias = Union[Attribute, BareOperandArgument[V]]
IndexedAnyArgument: TypeAlias = Union[
    IndexedOperandArgument[IndexType, V, ArgumentOrderType],
    IndexedDroppingArgument[IndexType, V],
]
BareAnyArgument: TypeAlias = Union[BareOperandArgument[V], BareDroppingArgument[V]]
BareReplacement: TypeAlias = Union[
    Replace[Operand[Bare[V], Single, Ungrouped]],
    Replace[Operand[Bare[V], Definite, Ungrouped]],
]

AttributesOperand: TypeAlias = Operand[
    Indexed[IndexType, AttributeName], Multiple[OrderType], Ungrouped
]
BareAttributesOperand: TypeAlias = Operand[
    Bare[AttributeName], Multiple[OrderType], Ungrouped
]
AttributeOperand: TypeAlias = Operand[
    Indexed[IndexType, AttributeName], Single, Ungrouped
]
BareAttributeOperand: TypeAlias = Operand[Bare[AttributeName], Single, Ungrouped]
DefiniteAttributeOperand: TypeAlias = Operand[
    Indexed[IndexType, AttributeName], Definite, Ungrouped
]
DefiniteBareAttributeOperand: TypeAlias = Operand[
    Bare[AttributeName], Definite, Ungrouped
]

BoolMaskOperand: TypeAlias = Operand[
    Indexed[IndexType, Mask], Multiple[OrderType], Ungrouped
]
BareBoolMaskOperand: TypeAlias = Operand[Bare[Mask], Multiple[OrderType], Ungrouped]
BoolOperand: TypeAlias = Operand[Indexed[IndexType, Mask], Single, Ungrouped]
BareBoolOperand: TypeAlias = Operand[Bare[Mask], Single, Ungrouped]
DefiniteBoolOperand: TypeAlias = Operand[Indexed[IndexType, Mask], Definite, Ungrouped]
DefiniteBareBoolOperand: TypeAlias = Operand[Bare[Mask], Definite, Ungrouped]

ElementsOperand: TypeAlias = Operand[
    Indexed[IndexType, Unit], Multiple[OrderType], Ungrouped
]
ElementOperand: TypeAlias = Operand[Indexed[IndexType, Unit], Single, Ungrouped]
DefiniteElementOperand: TypeAlias = Operand[
    Indexed[IndexType, Unit], Definite, Ungrouped
]

FailuresOperand: TypeAlias = Operand[
    Indexed[IndexType, FailureValue], Multiple[OrderType], Ungrouped
]
FailureKindsOperand: TypeAlias = Operand[
    Indexed[IndexType, FailureKindValue], Multiple[OrderType], Ungrouped
]
BareFailuresOperand: TypeAlias = Operand[
    Bare[FailureValue], Multiple[OrderType], Ungrouped
]
BareFailureKindsOperand: TypeAlias = Operand[
    Bare[FailureKindValue], Multiple[OrderType], Ungrouped
]
FailureOperand: TypeAlias = Operand[Indexed[IndexType, FailureValue], Single, Ungrouped]
FailureKindOperand: TypeAlias = Operand[
    Indexed[IndexType, FailureKindValue], Single, Ungrouped
]
BareFailureOperand: TypeAlias = Operand[Bare[FailureValue], Single, Ungrouped]
BareFailureKindOperand: TypeAlias = Operand[Bare[FailureKindValue], Single, Ungrouped]
DefiniteFailureOperand: TypeAlias = Operand[
    Indexed[IndexType, FailureValue], Definite, Ungrouped
]
DefiniteFailureKindOperand: TypeAlias = Operand[
    Indexed[IndexType, FailureKindValue], Definite, Ungrouped
]
DefiniteBareFailureOperand: TypeAlias = Operand[Bare[FailureValue], Definite, Ungrouped]
DefiniteBareFailureKindOperand: TypeAlias = Operand[
    Bare[FailureKindValue], Definite, Ungrouped
]

IndicesOperand: TypeAlias = Operand[
    Indexed[IndexType, IndexValue[IndexType]], Multiple[OrderType], Ungrouped
]
BareIndicesOperand: TypeAlias = Operand[
    Bare[IndexValue[IndexType]], Multiple[OrderType], Ungrouped
]
IndexOperand: TypeAlias = Operand[
    Indexed[IndexType, IndexValue[IndexType]], Single, Ungrouped
]
BareIndexOperand: TypeAlias = Operand[Bare[IndexValue[IndexType]], Single, Ungrouped]
DefiniteIndexOperand: TypeAlias = Operand[
    Indexed[IndexType, IndexValue[IndexType]], Definite, Ungrouped
]
DefiniteBareIndexOperand: TypeAlias = Operand[
    Bare[IndexValue[IndexType]], Definite, Ungrouped
]

ReferencesOperand: TypeAlias = Operand[
    Indexed[IndexType, ReferenceType], Multiple[OrderType], Ungrouped
]
BareReferencesOperand: TypeAlias = Operand[
    Bare[ReferenceType], Multiple[OrderType], Ungrouped
]
ReferenceOperand: TypeAlias = Operand[
    Indexed[IndexType, ReferenceType], Single, Ungrouped
]
BareReferenceOperand: TypeAlias = Operand[Bare[ReferenceType], Single, Ungrouped]
DefiniteReferenceOperand: TypeAlias = Operand[
    Indexed[IndexType, ReferenceType], Definite, Ungrouped
]
DefiniteBareReferenceOperand: TypeAlias = Operand[
    Bare[ReferenceType], Definite, Ungrouped
]
ReferenceIndicesOperand: TypeAlias = Operand[
    Indexed[IndexType, IndexValue[EntityType]], Multiple[OrderType], Ungrouped
]
ReferenceIndexOperand: TypeAlias = Operand[
    Indexed[IndexType, IndexValue[EntityType]], Single, Ungrouped
]
DefiniteReferenceIndexOperand: TypeAlias = Operand[
    Indexed[IndexType, IndexValue[EntityType]], Definite, Ungrouped
]

ValuesOperand: TypeAlias = Operand[
    Indexed[IndexType, Scalar], Multiple[OrderType], Ungrouped
]
BareValuesOperand: TypeAlias = Operand[Bare[Scalar], Multiple[OrderType], Ungrouped]
ValueOperand: TypeAlias = Operand[Indexed[IndexType, Scalar], Single, Ungrouped]
BareValueOperand: TypeAlias = Operand[Bare[Scalar], Single, Ungrouped]
DefiniteValueOperand: TypeAlias = Operand[
    Indexed[IndexType, Scalar], Definite, Ungrouped
]
DefiniteBareValueOperand: TypeAlias = Operand[Bare[Scalar], Definite, Ungrouped]

NodesOperand: TypeAlias = ElementsOperand[NodeIndex, Unordered]
OrderedNodesOperand: TypeAlias = ElementsOperand[NodeIndex, Ordered]
NodeOperand: TypeAlias = ElementOperand[NodeIndex]
DefiniteNodeOperand: TypeAlias = DefiniteElementOperand[NodeIndex]
EdgesOperand: TypeAlias = ElementsOperand[EdgeIndex, Unordered]
OrderedEdgesOperand: TypeAlias = ElementsOperand[EdgeIndex, Ordered]
EdgeOperand: TypeAlias = ElementOperand[EdgeIndex]
DefiniteEdgeOperand: TypeAlias = DefiniteElementOperand[EdgeIndex]

NodeIndicesOperand: TypeAlias = IndicesOperand[NodeIndex, Unordered]
OrderedNodeIndicesOperand: TypeAlias = IndicesOperand[NodeIndex, Ordered]
NodeIndexOperand: TypeAlias = IndexOperand[NodeIndex]
DefiniteNodeIndexOperand: TypeAlias = DefiniteIndexOperand[NodeIndex]
BareNodeIndicesOperand: TypeAlias = BareIndicesOperand[NodeIndex, Unordered]
OrderedBareNodeIndicesOperand: TypeAlias = BareIndicesOperand[NodeIndex, Ordered]
BareNodeIndexOperand: TypeAlias = BareIndexOperand[NodeIndex]
DefiniteBareNodeIndexOperand: TypeAlias = DefiniteBareIndexOperand[NodeIndex]
EdgeIndicesOperand: TypeAlias = IndicesOperand[EdgeIndex, Unordered]
OrderedEdgeIndicesOperand: TypeAlias = IndicesOperand[EdgeIndex, Ordered]
EdgeIndexOperand: TypeAlias = IndexOperand[EdgeIndex]
DefiniteEdgeIndexOperand: TypeAlias = DefiniteIndexOperand[EdgeIndex]
BareEdgeIndicesOperand: TypeAlias = BareIndicesOperand[EdgeIndex, Unordered]
OrderedBareEdgeIndicesOperand: TypeAlias = BareIndicesOperand[EdgeIndex, Ordered]
BareEdgeIndexOperand: TypeAlias = BareIndexOperand[EdgeIndex]
DefiniteBareEdgeIndexOperand: TypeAlias = DefiniteBareIndexOperand[EdgeIndex]

NodeValuesOperand: TypeAlias = ValuesOperand[NodeIndex, Unordered]
OrderedNodeValuesOperand: TypeAlias = ValuesOperand[NodeIndex, Ordered]
NodeValueOperand: TypeAlias = ValueOperand[NodeIndex]
DefiniteNodeValueOperand: TypeAlias = DefiniteValueOperand[NodeIndex]
EdgeValuesOperand: TypeAlias = ValuesOperand[EdgeIndex, Unordered]
OrderedEdgeValuesOperand: TypeAlias = ValuesOperand[EdgeIndex, Ordered]
EdgeValueOperand: TypeAlias = ValueOperand[EdgeIndex]
DefiniteEdgeValueOperand: TypeAlias = DefiniteValueOperand[EdgeIndex]

NodeAttributesOperand: TypeAlias = AttributesOperand[NodeIndex, Unordered]
OrderedNodeAttributesOperand: TypeAlias = AttributesOperand[NodeIndex, Ordered]
NodeAttributeOperand: TypeAlias = AttributeOperand[NodeIndex]
DefiniteNodeAttributeOperand: TypeAlias = DefiniteAttributeOperand[NodeIndex]
EdgeAttributesOperand: TypeAlias = AttributesOperand[EdgeIndex, Unordered]
OrderedEdgeAttributesOperand: TypeAlias = AttributesOperand[EdgeIndex, Ordered]
EdgeAttributeOperand: TypeAlias = AttributeOperand[EdgeIndex]
DefiniteEdgeAttributeOperand: TypeAlias = DefiniteAttributeOperand[EdgeIndex]

NodeAttributesTreeOperand: TypeAlias = Operand[
    Indexed[Expanded[NodeIndex, AttributeNameIndex], AttributeName],
    Multiple[Unordered],
    Ungrouped,
]
EdgeAttributesTreeOperand: TypeAlias = Operand[
    Indexed[Expanded[EdgeIndex, AttributeNameIndex], AttributeName],
    Multiple[Unordered],
    Ungrouped,
]

NodeQuery: TypeAlias = Callable[
    [NodesOperand],
    Union[
        NodeOperand,
        DefiniteNodeOperand,
        BareNodeIndexOperand,
        DefiniteBareNodeIndexOperand,
    ],
]
NodesQuery: TypeAlias = Callable[
    [NodesOperand],
    Union[
        OrderedNodesOperand,
        NodesOperand,
        OrderedBareNodeIndicesOperand,
        BareNodeIndicesOperand,
    ],
]
EdgeQuery: TypeAlias = Callable[
    [EdgesOperand],
    Union[
        EdgeOperand,
        DefiniteEdgeOperand,
        BareEdgeIndexOperand,
        DefiniteBareEdgeIndexOperand,
    ],
]
EdgesQuery: TypeAlias = Callable[
    [EdgesOperand],
    Union[
        OrderedEdgesOperand,
        EdgesOperand,
        OrderedBareEdgeIndicesOperand,
        BareEdgeIndicesOperand,
    ],
]


IndexPayload: TypeAlias = Union[
    GraphRecordValue,
    EdgeEndpointRole,
    FailureKind,
    Tuple["IndexPayload", Optional["IndexPayload"]],
]
GroupPayloadType = TypeVar("GroupPayloadType")
GroupResult: TypeAlias = Tuple[
    List[Tuple[IndexPayload, List[IndexPayload], Union[GroupPayloadType, QueryError]]],
    List[Tuple[IndexPayload, QueryError]],
]

MembershipResult: TypeAlias = List[Union[IndexPayload, QueryError]]
ValueResult: TypeAlias = List[Tuple[IndexPayload, Union[ScalarValue, QueryError]]]
AttributeResult: TypeAlias = List[Tuple[IndexPayload, Union[Attribute, QueryError]]]
MaskResult: TypeAlias = List[Tuple[IndexPayload, Union[bool, QueryError]]]
FailureResult: TypeAlias = List[Tuple[IndexPayload, QueryError]]
FailureKindResult: TypeAlias = List[Tuple[IndexPayload, Union[FailureKind, QueryError]]]
NodeIndexValueResult: TypeAlias = List[
    Tuple[IndexPayload, Union[Attribute, QueryError]]
]
EdgeIndexValueResult: TypeAlias = List[Tuple[IndexPayload, Union[int, QueryError]]]
PositionalIndexValueResult: TypeAlias = List[
    Tuple[IndexPayload, Union[int, QueryError]]
]
EndpointRoleIndexValueResult: TypeAlias = List[
    Tuple[IndexPayload, Union[EdgeEndpointRole, QueryError]]
]
ExpandedIndexValueResult: TypeAlias = List[
    Tuple[IndexPayload, Union[Tuple[IndexPayload, Optional[IndexPayload]], QueryError]]
]
BareScalarMultipleResult: TypeAlias = List[Union[ScalarValue, QueryError]]
BareAttributeMultipleResult: TypeAlias = List[Union[Attribute, QueryError]]
BareMaskMultipleResult: TypeAlias = List[Union[bool, QueryError]]
BareFailureMultipleResult: TypeAlias = List[QueryError]
BareFailureKindMultipleResult: TypeAlias = List[Union[FailureKind, QueryError]]
BareNodeIndexValueMultipleResult: TypeAlias = List[Union[Attribute, QueryError]]
BareEdgeIndexValueMultipleResult: TypeAlias = List[Union[int, QueryError]]
BarePositionalIndexValueMultipleResult: TypeAlias = List[Union[int, QueryError]]
BareEndpointRoleIndexValueMultipleResult: TypeAlias = List[
    Union[EdgeEndpointRole, QueryError]
]
BareExpandedIndexValueMultipleResult: TypeAlias = List[
    Union[Tuple[IndexPayload, Optional[IndexPayload]], QueryError]
]
BareScalarSingleResult: TypeAlias = Optional[Union[ScalarValue, QueryError]]
BareAttributeSingleResult: TypeAlias = Optional[Union[Attribute, QueryError]]
BareNodeIndexValueSingleResult: TypeAlias = Optional[Union[Attribute, QueryError]]
BareEdgeIndexValueSingleResult: TypeAlias = Optional[Union[int, QueryError]]
BarePositionalIndexValueSingleResult: TypeAlias = Optional[Union[int, QueryError]]
BareScalarDefiniteResult: TypeAlias = Union[ScalarValue, QueryError]
BareMaskDefiniteResult: TypeAlias = Union[bool, QueryError]
MembershipSingleResult: TypeAlias = Optional[Union[IndexPayload, QueryError]]
MembershipDefiniteResult: TypeAlias = Union[IndexPayload, QueryError]
IndexedScalarSingleResult: TypeAlias = Optional[
    Tuple[IndexPayload, Union[ScalarValue, QueryError]]
]
IndexedScalarDefiniteResult: TypeAlias = Tuple[
    IndexPayload, Union[ScalarValue, QueryError]
]


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[
            Bare[Scalar], Single, Grouped[MemberIndexType, KeyIndexType, Ungrouped]
        ],
    ],
) -> GroupResult[BareScalarSingleResult]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[
            Bare[Scalar],
            Single,
            Grouped[
                MemberIndexType,
                KeyIndexType,
                Grouped[SecondMemberIndexType, SecondKeyIndexType, Ungrouped],
            ],
        ],
    ],
) -> GroupResult[GroupResult[BareScalarSingleResult]]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[
            Bare[Scalar],
            Single,
            Grouped[
                MemberIndexType,
                KeyIndexType,
                Grouped[
                    SecondMemberIndexType,
                    SecondKeyIndexType,
                    Grouped[ThirdMemberIndexType, ThirdKeyIndexType, Ungrouped],
                ],
            ],
        ],
    ],
) -> GroupResult[GroupResult[GroupResult[BareScalarSingleResult]]]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[
            Bare[Scalar],
            Single,
            Grouped[
                MemberIndexType,
                KeyIndexType,
                Grouped[
                    SecondMemberIndexType,
                    SecondKeyIndexType,
                    Grouped[
                        ThirdMemberIndexType,
                        ThirdKeyIndexType,
                        Grouped[FourthMemberIndexType, FourthKeyIndexType, Ungrouped],
                    ],
                ],
            ],
        ],
    ],
) -> GroupResult[GroupResult[GroupResult[GroupResult[BareScalarSingleResult]]]]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[
            Bare[Scalar],
            Single,
            Grouped[
                MemberIndexType,
                KeyIndexType,
                Grouped[
                    SecondMemberIndexType,
                    SecondKeyIndexType,
                    Grouped[
                        ThirdMemberIndexType,
                        ThirdKeyIndexType,
                        Grouped[
                            FourthMemberIndexType,
                            FourthKeyIndexType,
                            Grouped[FifthMemberIndexType, FifthKeyIndexType, Ungrouped],
                        ],
                    ],
                ],
            ],
        ],
    ],
) -> GroupResult[
    GroupResult[GroupResult[GroupResult[GroupResult[BareScalarSingleResult]]]]
]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[
            Indexed[IndexType, Scalar],
            Multiple[OrderType],
            Grouped[MemberIndexType, KeyIndexType, Ungrouped],
        ],
    ],
) -> GroupResult[ValueResult]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[
            Indexed[IndexType, Scalar],
            Multiple[OrderType],
            Grouped[
                MemberIndexType,
                KeyIndexType,
                Grouped[SecondMemberIndexType, SecondKeyIndexType, Ungrouped],
            ],
        ],
    ],
) -> GroupResult[GroupResult[ValueResult]]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[
            Indexed[IndexType, Scalar],
            Multiple[OrderType],
            Grouped[
                MemberIndexType,
                KeyIndexType,
                Grouped[
                    SecondMemberIndexType,
                    SecondKeyIndexType,
                    Grouped[ThirdMemberIndexType, ThirdKeyIndexType, Ungrouped],
                ],
            ],
        ],
    ],
) -> GroupResult[GroupResult[GroupResult[ValueResult]]]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[
            Indexed[IndexType, Scalar],
            Multiple[OrderType],
            Grouped[
                MemberIndexType,
                KeyIndexType,
                Grouped[
                    SecondMemberIndexType,
                    SecondKeyIndexType,
                    Grouped[
                        ThirdMemberIndexType,
                        ThirdKeyIndexType,
                        Grouped[FourthMemberIndexType, FourthKeyIndexType, Ungrouped],
                    ],
                ],
            ],
        ],
    ],
) -> GroupResult[GroupResult[GroupResult[GroupResult[ValueResult]]]]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[
            Indexed[IndexType, Scalar],
            Multiple[OrderType],
            Grouped[
                MemberIndexType,
                KeyIndexType,
                Grouped[
                    SecondMemberIndexType,
                    SecondKeyIndexType,
                    Grouped[
                        ThirdMemberIndexType,
                        ThirdKeyIndexType,
                        Grouped[
                            FourthMemberIndexType,
                            FourthKeyIndexType,
                            Grouped[FifthMemberIndexType, FifthKeyIndexType, Ungrouped],
                        ],
                    ],
                ],
            ],
        ],
    ],
) -> GroupResult[GroupResult[GroupResult[GroupResult[GroupResult[ValueResult]]]]]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand], Operand[Indexed[IndexType, Scalar], Single, Ungrouped]
    ],
) -> IndexedScalarSingleResult: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand], Operand[Indexed[IndexType, Scalar], Definite, Ungrouped]
    ],
) -> IndexedScalarDefiniteResult: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand], Operand[Indexed[IndexType, Unit], Single, Ungrouped]
    ],
) -> MembershipSingleResult: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand], Operand[Indexed[IndexType, Unit], Definite, Ungrouped]
    ],
) -> MembershipDefiniteResult: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[Indexed[IndexType, Unit], Multiple[OrderType], Ungrouped],
    ],
) -> MembershipResult: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[Indexed[IndexType, Scalar], Multiple[OrderType], Ungrouped],
    ],
) -> ValueResult: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[Indexed[IndexType, AttributeName], Multiple[OrderType], Ungrouped],
    ],
) -> AttributeResult: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[Indexed[IndexType, Mask], Multiple[OrderType], Ungrouped],
    ],
) -> MaskResult: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[Indexed[IndexType, FailureValue], Multiple[OrderType], Ungrouped],
    ],
) -> FailureResult: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[Indexed[IndexType, FailureKindValue], Multiple[OrderType], Ungrouped],
    ],
) -> FailureKindResult: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[
            Indexed[IndexType, IndexValue[NodeIndex]], Multiple[OrderType], Ungrouped
        ],
    ],
) -> NodeIndexValueResult: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[
            Indexed[IndexType, IndexValue[EdgeIndex]], Multiple[OrderType], Ungrouped
        ],
    ],
) -> EdgeIndexValueResult: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[
            Indexed[IndexType, IndexValue[Positional]], Multiple[OrderType], Ungrouped
        ],
    ],
) -> PositionalIndexValueResult: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[
            Indexed[IndexType, IndexValue[EndpointRole]], Multiple[OrderType], Ungrouped
        ],
    ],
) -> EndpointRoleIndexValueResult: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[
            Indexed[IndexType, IndexValue[Expanded[K, ChildType]]],
            Multiple[OrderType],
            Ungrouped,
        ],
    ],
) -> ExpandedIndexValueResult: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand], Operand[Bare[Scalar], Multiple[OrderType], Ungrouped]
    ],
) -> BareScalarMultipleResult: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand], Operand[Bare[AttributeName], Multiple[OrderType], Ungrouped]
    ],
) -> BareAttributeMultipleResult: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand], Operand[Bare[Mask], Multiple[OrderType], Ungrouped]
    ],
) -> BareMaskMultipleResult: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand], Operand[Bare[FailureValue], Multiple[OrderType], Ungrouped]
    ],
) -> BareFailureMultipleResult: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand], Operand[Bare[FailureKindValue], Multiple[OrderType], Ungrouped]
    ],
) -> BareFailureKindMultipleResult: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[Bare[IndexValue[NodeIndex]], Multiple[OrderType], Ungrouped],
    ],
) -> BareNodeIndexValueMultipleResult: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[Bare[IndexValue[EdgeIndex]], Multiple[OrderType], Ungrouped],
    ],
) -> BareEdgeIndexValueMultipleResult: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[Bare[IndexValue[Positional]], Multiple[OrderType], Ungrouped],
    ],
) -> BarePositionalIndexValueMultipleResult: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[Bare[IndexValue[EndpointRole]], Multiple[OrderType], Ungrouped],
    ],
) -> BareEndpointRoleIndexValueMultipleResult: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[
            Bare[IndexValue[Expanded[K, ChildType]]], Multiple[OrderType], Ungrouped
        ],
    ],
) -> BareExpandedIndexValueMultipleResult: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[[NodesOperand], Operand[Bare[Scalar], Single, Ungrouped]],
) -> BareScalarSingleResult: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[[NodesOperand], Operand[Bare[AttributeName], Single, Ungrouped]],
) -> BareAttributeSingleResult: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand], Operand[Bare[IndexValue[NodeIndex]], Single, Ungrouped]
    ],
) -> BareNodeIndexValueSingleResult: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand], Operand[Bare[IndexValue[AttributeNameIndex]], Single, Ungrouped]
    ],
) -> BareAttributeSingleResult: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand], Operand[Bare[IndexValue[EdgeIndex]], Single, Ungrouped]
    ],
) -> BareEdgeIndexValueSingleResult: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand], Operand[Bare[IndexValue[Positional]], Single, Ungrouped]
    ],
) -> BarePositionalIndexValueSingleResult: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[[NodesOperand], Operand[Bare[Scalar], Definite, Ungrouped]],
) -> BareScalarDefiniteResult: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[[NodesOperand], Operand[Bare[Mask], Definite, Ungrouped]],
) -> BareMaskDefiniteResult: ...


def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[[NodesOperand], Operand[Any, Any, Any]],
) -> object:
    def adapter(operand: PyOperand) -> PyOperand:
        return query(Operand._from_py_operand(operand))._operand

    return graphrecord._graphrecord.query_nodes(adapter)


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[
            Bare[Scalar], Single, Grouped[MemberIndexType, KeyIndexType, Ungrouped]
        ],
    ],
) -> GroupResult[BareScalarSingleResult]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[
            Bare[Scalar],
            Single,
            Grouped[
                MemberIndexType,
                KeyIndexType,
                Grouped[SecondMemberIndexType, SecondKeyIndexType, Ungrouped],
            ],
        ],
    ],
) -> GroupResult[GroupResult[BareScalarSingleResult]]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[
            Bare[Scalar],
            Single,
            Grouped[
                MemberIndexType,
                KeyIndexType,
                Grouped[
                    SecondMemberIndexType,
                    SecondKeyIndexType,
                    Grouped[ThirdMemberIndexType, ThirdKeyIndexType, Ungrouped],
                ],
            ],
        ],
    ],
) -> GroupResult[GroupResult[GroupResult[BareScalarSingleResult]]]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[
            Bare[Scalar],
            Single,
            Grouped[
                MemberIndexType,
                KeyIndexType,
                Grouped[
                    SecondMemberIndexType,
                    SecondKeyIndexType,
                    Grouped[
                        ThirdMemberIndexType,
                        ThirdKeyIndexType,
                        Grouped[FourthMemberIndexType, FourthKeyIndexType, Ungrouped],
                    ],
                ],
            ],
        ],
    ],
) -> GroupResult[GroupResult[GroupResult[GroupResult[BareScalarSingleResult]]]]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[
            Bare[Scalar],
            Single,
            Grouped[
                MemberIndexType,
                KeyIndexType,
                Grouped[
                    SecondMemberIndexType,
                    SecondKeyIndexType,
                    Grouped[
                        ThirdMemberIndexType,
                        ThirdKeyIndexType,
                        Grouped[
                            FourthMemberIndexType,
                            FourthKeyIndexType,
                            Grouped[FifthMemberIndexType, FifthKeyIndexType, Ungrouped],
                        ],
                    ],
                ],
            ],
        ],
    ],
) -> GroupResult[
    GroupResult[GroupResult[GroupResult[GroupResult[BareScalarSingleResult]]]]
]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[
            Indexed[IndexType, Scalar],
            Multiple[OrderType],
            Grouped[MemberIndexType, KeyIndexType, Ungrouped],
        ],
    ],
) -> GroupResult[ValueResult]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[
            Indexed[IndexType, Scalar],
            Multiple[OrderType],
            Grouped[
                MemberIndexType,
                KeyIndexType,
                Grouped[SecondMemberIndexType, SecondKeyIndexType, Ungrouped],
            ],
        ],
    ],
) -> GroupResult[GroupResult[ValueResult]]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[
            Indexed[IndexType, Scalar],
            Multiple[OrderType],
            Grouped[
                MemberIndexType,
                KeyIndexType,
                Grouped[
                    SecondMemberIndexType,
                    SecondKeyIndexType,
                    Grouped[ThirdMemberIndexType, ThirdKeyIndexType, Ungrouped],
                ],
            ],
        ],
    ],
) -> GroupResult[GroupResult[GroupResult[ValueResult]]]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[
            Indexed[IndexType, Scalar],
            Multiple[OrderType],
            Grouped[
                MemberIndexType,
                KeyIndexType,
                Grouped[
                    SecondMemberIndexType,
                    SecondKeyIndexType,
                    Grouped[
                        ThirdMemberIndexType,
                        ThirdKeyIndexType,
                        Grouped[FourthMemberIndexType, FourthKeyIndexType, Ungrouped],
                    ],
                ],
            ],
        ],
    ],
) -> GroupResult[GroupResult[GroupResult[GroupResult[ValueResult]]]]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[
            Indexed[IndexType, Scalar],
            Multiple[OrderType],
            Grouped[
                MemberIndexType,
                KeyIndexType,
                Grouped[
                    SecondMemberIndexType,
                    SecondKeyIndexType,
                    Grouped[
                        ThirdMemberIndexType,
                        ThirdKeyIndexType,
                        Grouped[
                            FourthMemberIndexType,
                            FourthKeyIndexType,
                            Grouped[FifthMemberIndexType, FifthKeyIndexType, Ungrouped],
                        ],
                    ],
                ],
            ],
        ],
    ],
) -> GroupResult[GroupResult[GroupResult[GroupResult[GroupResult[ValueResult]]]]]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand], Operand[Indexed[IndexType, Scalar], Single, Ungrouped]
    ],
) -> IndexedScalarSingleResult: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand], Operand[Indexed[IndexType, Scalar], Definite, Ungrouped]
    ],
) -> IndexedScalarDefiniteResult: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand], Operand[Indexed[IndexType, Unit], Single, Ungrouped]
    ],
) -> MembershipSingleResult: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand], Operand[Indexed[IndexType, Unit], Definite, Ungrouped]
    ],
) -> MembershipDefiniteResult: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[Indexed[IndexType, Unit], Multiple[OrderType], Ungrouped],
    ],
) -> MembershipResult: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[Indexed[IndexType, Scalar], Multiple[OrderType], Ungrouped],
    ],
) -> ValueResult: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[Indexed[IndexType, AttributeName], Multiple[OrderType], Ungrouped],
    ],
) -> AttributeResult: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[Indexed[IndexType, Mask], Multiple[OrderType], Ungrouped],
    ],
) -> MaskResult: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[Indexed[IndexType, FailureValue], Multiple[OrderType], Ungrouped],
    ],
) -> FailureResult: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[Indexed[IndexType, FailureKindValue], Multiple[OrderType], Ungrouped],
    ],
) -> FailureKindResult: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[
            Indexed[IndexType, IndexValue[NodeIndex]], Multiple[OrderType], Ungrouped
        ],
    ],
) -> NodeIndexValueResult: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[
            Indexed[IndexType, IndexValue[EdgeIndex]], Multiple[OrderType], Ungrouped
        ],
    ],
) -> EdgeIndexValueResult: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[
            Indexed[IndexType, IndexValue[Positional]], Multiple[OrderType], Ungrouped
        ],
    ],
) -> PositionalIndexValueResult: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[
            Indexed[IndexType, IndexValue[EndpointRole]], Multiple[OrderType], Ungrouped
        ],
    ],
) -> EndpointRoleIndexValueResult: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[
            Indexed[IndexType, IndexValue[Expanded[K, ChildType]]],
            Multiple[OrderType],
            Ungrouped,
        ],
    ],
) -> ExpandedIndexValueResult: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand], Operand[Bare[Scalar], Multiple[OrderType], Ungrouped]
    ],
) -> BareScalarMultipleResult: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand], Operand[Bare[AttributeName], Multiple[OrderType], Ungrouped]
    ],
) -> BareAttributeMultipleResult: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand], Operand[Bare[Mask], Multiple[OrderType], Ungrouped]
    ],
) -> BareMaskMultipleResult: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand], Operand[Bare[FailureValue], Multiple[OrderType], Ungrouped]
    ],
) -> BareFailureMultipleResult: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand], Operand[Bare[FailureKindValue], Multiple[OrderType], Ungrouped]
    ],
) -> BareFailureKindMultipleResult: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[Bare[IndexValue[NodeIndex]], Multiple[OrderType], Ungrouped],
    ],
) -> BareNodeIndexValueMultipleResult: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[Bare[IndexValue[EdgeIndex]], Multiple[OrderType], Ungrouped],
    ],
) -> BareEdgeIndexValueMultipleResult: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[Bare[IndexValue[Positional]], Multiple[OrderType], Ungrouped],
    ],
) -> BarePositionalIndexValueMultipleResult: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[Bare[IndexValue[EndpointRole]], Multiple[OrderType], Ungrouped],
    ],
) -> BareEndpointRoleIndexValueMultipleResult: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[
            Bare[IndexValue[Expanded[K, ChildType]]], Multiple[OrderType], Ungrouped
        ],
    ],
) -> BareExpandedIndexValueMultipleResult: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[[EdgesOperand], Operand[Bare[Scalar], Single, Ungrouped]],
) -> BareScalarSingleResult: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[[EdgesOperand], Operand[Bare[AttributeName], Single, Ungrouped]],
) -> BareAttributeSingleResult: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand], Operand[Bare[IndexValue[NodeIndex]], Single, Ungrouped]
    ],
) -> BareNodeIndexValueSingleResult: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand], Operand[Bare[IndexValue[AttributeNameIndex]], Single, Ungrouped]
    ],
) -> BareAttributeSingleResult: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand], Operand[Bare[IndexValue[EdgeIndex]], Single, Ungrouped]
    ],
) -> BareEdgeIndexValueSingleResult: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand], Operand[Bare[IndexValue[Positional]], Single, Ungrouped]
    ],
) -> BarePositionalIndexValueSingleResult: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[[EdgesOperand], Operand[Bare[Scalar], Definite, Ungrouped]],
) -> BareScalarDefiniteResult: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[[EdgesOperand], Operand[Bare[Mask], Definite, Ungrouped]],
) -> BareMaskDefiniteResult: ...


def query_edges(
    graphrecord: GraphRecord,
    query: Callable[[EdgesOperand], Operand[Any, Any, Any]],
) -> object:
    def adapter(operand: PyOperand) -> PyOperand:
        return query(Operand._from_py_operand(operand))._operand

    return graphrecord._graphrecord.query_edges(adapter)
