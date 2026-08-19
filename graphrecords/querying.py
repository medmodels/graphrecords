# ruff: noqa: D100, D101, D102, D103, D105, D107
from __future__ import annotations

from dataclasses import dataclass
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
    Union,
    overload,
)

from typing_extensions import TypeVar, TypeVarTuple, Unpack

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
    PyEdgeDirection,
    PyEdgeEndpointRole,
    PyFailureKind,
    PyOperand,
    PyValueTarget,
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
from graphrecords.types import (
    EdgeIndex as EdgeIndexPayload,
)
from graphrecords.types import (
    Group,
    Identifier,
)
from graphrecords.types import (
    NodeIndex as NodeIndexPayload,
)
from graphrecords.types import (
    Value as ValuePayload,
)

if TYPE_CHECKING:
    from graphrecords.graphrecord import GraphRecord


EdgeDirection = PyEdgeDirection
EdgeEndpointRole = PyEdgeEndpointRole
FailureKind = PyFailureKind

Attribute: TypeAlias = Identifier
ScalarValue: TypeAlias = ValuePayload
_BooleanValue: TypeAlias = bool
IndexPayload: TypeAlias = Union[
    ValuePayload,
    EdgeEndpointRole,
    FailureKind,
    Tuple["IndexPayload", Optional["IndexPayload"]],
]


PayloadType = TypeVar("PayloadType", covariant=True)


class Index(Generic[PayloadType]): ...


K = TypeVar("K", bound=Index[IndexPayload], covariant=True)
ChildType = TypeVar("ChildType", bound=Index[IndexPayload], covariant=True)
ExpandedPayloadType = TypeVar(
    "ExpandedPayloadType",
    bound=IndexPayload,
    covariant=True,
    default=Tuple[IndexPayload, Optional[IndexPayload]],
)


class NodeIndex(Index[NodeIndexPayload]): ...


class EdgeIndex(Index[EdgeIndexPayload]): ...


class Positional(Index[int]): ...


class EndpointRole(Index[EdgeEndpointRole]): ...


class ValueIndex(Index[ScalarValue]): ...


class AttributeNameIndex(Index[Attribute]): ...


class BoolIndex(Index[bool]): ...


class FailureKindIndex(Index[FailureKind]): ...


class Expanded(
    Index[ExpandedPayloadType], Generic[K, ChildType, ExpandedPayloadType]
): ...


SortableIndex: TypeAlias = Union[
    NodeIndex,
    EdgeIndex,
    Positional,
    ValueIndex,
    AttributeNameIndex,
    BoolIndex,
    "Expanded[SortableIndex, SortableIndex]",
]


class Value: ...


class ReturnValue(Value, Generic[PayloadType]): ...


class Unit(Value): ...


class Scalar(ReturnValue[ScalarValue]): ...


class Mask(ReturnValue[bool]): ...


class AttributeName(ReturnValue[Attribute]): ...


class FailureValue(ReturnValue[QueryError]): ...


class FailureKindValue(ReturnValue[FailureKind]): ...


class NodeReference(ReturnValue[Attribute]): ...


class EdgeReference(ReturnValue[int]): ...


V = TypeVar("V", bound=Value, covariant=True)


class IndexValue(ReturnValue[IndexPayload], Generic[K]): ...


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


S = TypeVar("S", bound=Shape, covariant=True)
C = TypeVar("C", bound=Container)
IndexType = TypeVar("IndexType", bound=Index[IndexPayload])
ValueIndexType = TypeVar("ValueIndexType", bound=Index[IndexPayload])
ContainerType = TypeVar("ContainerType", bound=Container)
PopulationContainerType = TypeVar("PopulationContainerType", bound=Container)
MemberIndexType = TypeVar("MemberIndexType", bound=Index[IndexPayload], covariant=True)
KeyIndexType = TypeVar("KeyIndexType", bound=Index[IndexPayload], covariant=True)
PopulationIndexType = TypeVar("PopulationIndexType", bound=Index[IndexPayload])
IndexPayloadType = TypeVar("IndexPayloadType", bound=IndexPayload)
LaneIndexPayloadType = TypeVar("LaneIndexPayloadType", bound=IndexPayload)
ParentPayloadType = TypeVar("ParentPayloadType", bound=IndexPayload)
MemberPayloadType = TypeVar("MemberPayloadType", bound=IndexPayload)
KeyPayloadType = TypeVar("KeyPayloadType", bound=IndexPayload)
InnerMemberIndexType = TypeVar(
    "InnerMemberIndexType", bound=Index[IndexPayload], covariant=True
)
InnerKeyIndexType = TypeVar(
    "InnerKeyIndexType", bound=Index[IndexPayload], covariant=True
)
BucketPayloadType = TypeVar("BucketPayloadType")
LeafType = TypeVar("LeafType")
Levels = TypeVarTuple("Levels")
InnerLevels = TypeVarTuple("InnerLevels")
OuterLevels = TypeVarTuple("OuterLevels")
TemplateValueType = TypeVar("TemplateValueType", bound=Value)
ExpandedValueType = TypeVar("ExpandedValueType", bound=ReturnValue[object])
TransitionValueType = TypeVar("TransitionValueType", bound=Value)
EntityType = TypeVar("EntityType", NodeIndex, EdgeIndex)
IntegerIndexType = TypeVar("IntegerIndexType", EdgeIndex, Positional)
SortableIndexType = TypeVar("SortableIndexType", bound=SortableIndex)
BareValueType = TypeVar("BareValueType", bound=ReturnValue[object])
ReferenceType = TypeVar("ReferenceType", NodeReference, EdgeReference)
RetentionType = TypeVar("RetentionType", bound=Retention)
ArgumentOrderType = TypeVar("ArgumentOrderType")
ReplacementType = TypeVar("ReplacementType", covariant=True)
ReplaceableValueType = TypeVar("ReplaceableValueType", bound=ReturnValue[object])
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
    bound=Union[Scalar, AttributeName, IndexValue[SortableIndex]],
)
SortKeyValueType = TypeVar(
    "SortKeyValueType",
    bound=Union[
        Scalar,
        Mask,
        AttributeName,
        IndexValue[SortableIndex],
    ],
)
MembershipValueType = TypeVar(
    "MembershipValueType",
    bound=Union[
        Scalar,
        Mask,
        AttributeName,
        FailureKindValue,
        IndexValue[Index[IndexPayload]],
    ],
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
FailureKindMembershipValueType = TypeVar(
    "FailureKindMembershipValueType", FailureKindValue, IndexValue[FailureKindIndex]
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


class Grouped(Generic[MemberIndexType, KeyIndexType]): ...


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


class Operand(Generic[S, C, Unpack[Levels]]):
    _operand: PyOperand

    @classmethod
    def _from_py_operand(cls, operand: PyOperand) -> Operand[S, C, Unpack[Levels]]:
        new_operand = cls.__new__(cls)
        new_operand._operand = operand

        return new_operand

    @staticmethod
    def _to_py_argument(
        value: Union[
            LiteralValueType,
            Operand[Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
    ) -> Union[PyOperand, PyArgument, LiteralValueType]:
        if isinstance(value, Operand):
            return value._operand

        if isinstance(value, Argument):
            return value._argument

        return value

    @overload
    def on_missing(
        self: Operand[Indexed[IndexType, V], Multiple[OrderType]],
        policy: Drop,
    ) -> Argument[Indexed[IndexType, V], Dropping]: ...

    @overload
    def on_missing(
        self: Operand[Bare[BareValueType], Single], policy: Drop
    ) -> Argument[Bare[BareValueType], Dropping]: ...

    @overload
    def on_missing(
        self: Operand[Indexed[IndexType, V], Multiple[OrderType]],
        policy: Replace[Operand[Indexed[IndexType, V], Multiple[ArgumentOrderType]]],
    ) -> Argument[Indexed[IndexType, V], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[Indexed[IndexType, BareValueType], Multiple[OrderType]],
        policy: BareReplacement[BareValueType],
    ) -> Argument[Indexed[IndexType, BareValueType], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[Bare[BareValueType], Single],
        policy: BareReplacement[BareValueType],
    ) -> Argument[Bare[BareValueType], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[Indexed[IndexType, Scalar], Multiple[OrderType]],
        policy: Replace[ScalarValue],
    ) -> Argument[Indexed[IndexType, Scalar], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[Bare[Scalar], Single], policy: Replace[ScalarValue]
    ) -> Argument[Bare[Scalar], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[Indexed[IndexType, Mask], Multiple[OrderType]],
        policy: Replace[_BooleanValue],
    ) -> Argument[Indexed[IndexType, Mask], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[Bare[Mask], Single],
        policy: Replace[_BooleanValue],
    ) -> Argument[Bare[Mask], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[Indexed[IndexType, AttributeName], Multiple[OrderType]],
        policy: Replace[Attribute],
    ) -> Argument[Indexed[IndexType, AttributeName], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[Bare[AttributeName], Single],
        policy: Replace[Attribute],
    ) -> Argument[Bare[AttributeName], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[Indexed[IndexType, FailureKindValue], Multiple[OrderType]],
        policy: Replace[FailureKind],
    ) -> Argument[Indexed[IndexType, FailureKindValue], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[Bare[FailureKindValue], Single],
        policy: Replace[FailureKind],
    ) -> Argument[Bare[FailureKindValue], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[
            Indexed[IndexType, IndexValue[FailureKindIndex]], Multiple[OrderType]
        ],
        policy: Replace[FailureKind],
    ) -> Argument[Indexed[IndexType, IndexValue[FailureKindIndex]], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[Bare[IndexValue[FailureKindIndex]], Single],
        policy: Replace[FailureKind],
    ) -> Argument[Bare[IndexValue[FailureKindIndex]], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[Indexed[IndexType, IndexValue[NodeIndex]], Multiple[OrderType]],
        policy: Replace[Attribute],
    ) -> Argument[Indexed[IndexType, IndexValue[NodeIndex]], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[Bare[IndexValue[NodeIndex]], Single],
        policy: Replace[Attribute],
    ) -> Argument[Bare[IndexValue[NodeIndex]], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[Indexed[IndexType, IndexValue[ValueIndex]], Multiple[OrderType]],
        policy: Replace[ScalarValue],
    ) -> Argument[Indexed[IndexType, IndexValue[ValueIndex]], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[Bare[IndexValue[ValueIndex]], Single],
        policy: Replace[ScalarValue],
    ) -> Argument[Bare[IndexValue[ValueIndex]], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[
            Indexed[IndexType, IndexValue[AttributeNameIndex]],
            Multiple[OrderType],
        ],
        policy: Replace[Attribute],
    ) -> Argument[Indexed[IndexType, IndexValue[AttributeNameIndex]], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[Bare[IndexValue[AttributeNameIndex]], Single],
        policy: Replace[Attribute],
    ) -> Argument[Bare[IndexValue[AttributeNameIndex]], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[Indexed[IndexType, IndexValue[BoolIndex]], Multiple[OrderType]],
        policy: Replace[_BooleanValue],
    ) -> Argument[Indexed[IndexType, IndexValue[BoolIndex]], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[Bare[IndexValue[BoolIndex]], Single],
        policy: Replace[_BooleanValue],
    ) -> Argument[Bare[IndexValue[BoolIndex]], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[
            Indexed[IndexType, IndexValue[IntegerIndexType]],
            Multiple[OrderType],
        ],
        policy: Replace[int],
    ) -> Argument[Indexed[IndexType, IndexValue[IntegerIndexType]], Preserving]: ...

    @overload
    def on_missing(
        self: Operand[Bare[IndexValue[IntegerIndexType]], Single],
        policy: Replace[int],
    ) -> Argument[Bare[IndexValue[IntegerIndexType]], Preserving]: ...

    def on_missing(
        self,
        policy: Union[
            Drop,
            Replace[ScalarValue],
            Replace[FailureKind],
            Replace[Operand[Any, Any, Unpack[Tuple[Any, ...]]]],
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

    def cache(self) -> Operand[S, C, Unpack[Levels]]:
        return Operand._from_py_operand(self._operand.cache())

    @overload
    def filter(
        self: Operand[Indexed[IndexType, V], Definite, Unpack[Levels]],
        mask: MaskArgument[IndexType, ArgumentOrderType],
    ) -> Operand[Indexed[IndexType, V], Single, Unpack[Levels]]: ...

    @overload
    def filter(
        self: Operand[Indexed[IndexType, V], DroppedContainerType, Unpack[Levels]],
        mask: MaskArgument[IndexType, ArgumentOrderType],
    ) -> Operand[Indexed[IndexType, V], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def filter(
        self: Operand[Bare[BareValueType], Definite, Unpack[Levels]],
        mask: BareMaskArgument,
    ) -> Operand[Bare[BareValueType], Single, Unpack[Levels]]: ...

    @overload
    def filter(
        self: Operand[Bare[BareValueType], DroppedContainerType, Unpack[Levels]],
        mask: BareMaskArgument,
    ) -> Operand[Bare[BareValueType], DroppedContainerType, Unpack[Levels]]: ...

    def filter(
        self,
        mask: Union[
            _BooleanValue,
            Operand[Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(
            self._operand.filter(Operand._to_py_argument(mask))
        )

    @overload
    def and_(
        self: Operand[Indexed[IndexType, Mask], Definite, Unpack[Levels]],
        other: IndexedDroppingArgument[IndexType, Mask],
    ) -> Operand[Indexed[IndexType, Mask], Single, Unpack[Levels]]: ...

    @overload
    def and_(
        self: Operand[Indexed[IndexType, Mask], DroppedContainerType, Unpack[Levels]],
        other: IndexedDroppingArgument[IndexType, Mask],
    ) -> Operand[Indexed[IndexType, Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def and_(
        self: Operand[Bare[Mask], Definite, Unpack[Levels]],
        other: BareDroppingArgument[Mask],
    ) -> Operand[Bare[Mask], Single, Unpack[Levels]]: ...

    @overload
    def and_(
        self: Operand[Bare[Mask], DroppedContainerType, Unpack[Levels]],
        other: BareDroppingArgument[Mask],
    ) -> Operand[Bare[Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def and_(
        self: Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]],
        other: Union[
            _BooleanValue, IndexedOperandArgument[IndexType, Mask, ArgumentOrderType]
        ],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def and_(
        self: Operand[Bare[Mask], ContainerType, Unpack[Levels]],
        other: Union[_BooleanValue, BareOperandArgument[Mask]],
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def and_(
        self,
        other: Union[
            _BooleanValue,
            Operand[Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(
            self._operand.and_(Operand._to_py_argument(other))
        )

    @overload
    def or_(
        self: Operand[Indexed[IndexType, Mask], Definite, Unpack[Levels]],
        other: IndexedDroppingArgument[IndexType, Mask],
    ) -> Operand[Indexed[IndexType, Mask], Single, Unpack[Levels]]: ...

    @overload
    def or_(
        self: Operand[Indexed[IndexType, Mask], DroppedContainerType, Unpack[Levels]],
        other: IndexedDroppingArgument[IndexType, Mask],
    ) -> Operand[Indexed[IndexType, Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def or_(
        self: Operand[Bare[Mask], Definite, Unpack[Levels]],
        other: BareDroppingArgument[Mask],
    ) -> Operand[Bare[Mask], Single, Unpack[Levels]]: ...

    @overload
    def or_(
        self: Operand[Bare[Mask], DroppedContainerType, Unpack[Levels]],
        other: BareDroppingArgument[Mask],
    ) -> Operand[Bare[Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def or_(
        self: Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]],
        other: Union[
            _BooleanValue, IndexedOperandArgument[IndexType, Mask, ArgumentOrderType]
        ],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def or_(
        self: Operand[Bare[Mask], ContainerType, Unpack[Levels]],
        other: Union[_BooleanValue, BareOperandArgument[Mask]],
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def or_(
        self,
        other: Union[
            _BooleanValue,
            Operand[Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(
            self._operand.or_(Operand._to_py_argument(other))
        )

    @overload
    def xor(
        self: Operand[Indexed[IndexType, Mask], Definite, Unpack[Levels]],
        other: IndexedDroppingArgument[IndexType, Mask],
    ) -> Operand[Indexed[IndexType, Mask], Single, Unpack[Levels]]: ...

    @overload
    def xor(
        self: Operand[Indexed[IndexType, Mask], DroppedContainerType, Unpack[Levels]],
        other: IndexedDroppingArgument[IndexType, Mask],
    ) -> Operand[Indexed[IndexType, Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def xor(
        self: Operand[Bare[Mask], Definite, Unpack[Levels]],
        other: BareDroppingArgument[Mask],
    ) -> Operand[Bare[Mask], Single, Unpack[Levels]]: ...

    @overload
    def xor(
        self: Operand[Bare[Mask], DroppedContainerType, Unpack[Levels]],
        other: BareDroppingArgument[Mask],
    ) -> Operand[Bare[Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def xor(
        self: Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]],
        other: Union[
            _BooleanValue, IndexedOperandArgument[IndexType, Mask, ArgumentOrderType]
        ],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def xor(
        self: Operand[Bare[Mask], ContainerType, Unpack[Levels]],
        other: Union[_BooleanValue, BareOperandArgument[Mask]],
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def xor(
        self,
        other: Union[
            _BooleanValue,
            Operand[Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(
            self._operand.xor(Operand._to_py_argument(other))
        )

    @overload
    def not_(
        self: Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def not_(
        self: Operand[Bare[Mask], ContainerType, Unpack[Levels]],
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def not_(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.not_())

    @overload
    def first(
        self: Operand[Indexed[IndexType, V], Multiple[Ordered], Unpack[Levels]],
    ) -> Operand[Indexed[IndexType, V], Single, Unpack[Levels]]: ...

    @overload
    def first(
        self: Operand[Bare[BareValueType], Multiple[Ordered], Unpack[Levels]],
    ) -> Operand[Bare[BareValueType], Single, Unpack[Levels]]: ...

    def first(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.first())

    @overload
    def last(
        self: Operand[Indexed[IndexType, V], Multiple[Ordered], Unpack[Levels]],
    ) -> Operand[Indexed[IndexType, V], Single, Unpack[Levels]]: ...

    @overload
    def last(
        self: Operand[Bare[BareValueType], Multiple[Ordered], Unpack[Levels]],
    ) -> Operand[Bare[BareValueType], Single, Unpack[Levels]]: ...

    def last(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.last())

    @overload
    def reverse_order(
        self: Operand[Indexed[IndexType, V], Multiple[Ordered], Unpack[Levels]],
    ) -> Operand[Indexed[IndexType, V], Multiple[Ordered], Unpack[Levels]]: ...

    @overload
    def reverse_order(
        self: Operand[Bare[BareValueType], Multiple[Ordered], Unpack[Levels]],
    ) -> Operand[Bare[BareValueType], Multiple[Ordered], Unpack[Levels]]: ...

    def reverse_order(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.reverse_order())

    @overload
    def shuffle(
        self: Operand[Indexed[IndexType, V], Multiple[OrderType], Unpack[Levels]],
    ) -> Operand[Indexed[IndexType, V], Multiple[Ordered], Unpack[Levels]]: ...

    @overload
    def shuffle(
        self: Operand[Bare[BareValueType], Multiple[OrderType], Unpack[Levels]],
    ) -> Operand[Bare[BareValueType], Multiple[Ordered], Unpack[Levels]]: ...

    def shuffle(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.shuffle())

    @overload
    def unorder(
        self: Operand[Indexed[IndexType, V], Multiple[OrderType], Unpack[Levels]],
    ) -> Operand[Indexed[IndexType, V], Multiple[Unordered], Unpack[Levels]]: ...

    @overload
    def unorder(
        self: Operand[Bare[BareValueType], Multiple[OrderType], Unpack[Levels]],
    ) -> Operand[Bare[BareValueType], Multiple[Unordered], Unpack[Levels]]: ...

    def unorder(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.unorder())

    @overload
    def sort(
        self: Operand[
            Indexed[SortableIndexType, OrderableValueType],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Operand[
        Indexed[SortableIndexType, OrderableValueType],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def sort(
        self: Operand[Bare[OrderableValueType], Multiple[OrderType], Unpack[Levels]],
    ) -> Operand[Bare[OrderableValueType], Multiple[Ordered], Unpack[Levels]]: ...

    def sort(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.sort())

    def sort_by(
        self: Operand[
            Indexed[SortableIndexType, V], Multiple[OrderType], Unpack[Levels]
        ],
        key: IndexedAnyScalarArgument[
            SortableIndexType, SortKeyValueType, ArgumentOrderType
        ],
    ) -> Operand[Indexed[SortableIndexType, V], Multiple[Ordered], Unpack[Levels]]:
        return Operand._from_py_operand(
            self._operand.sort_by(Operand._to_py_argument(key))
        )

    @overload
    def drop_duplicates(
        self: Operand[
            Indexed[IndexType, EquivalentValueType], Multiple[Ordered], Unpack[Levels]
        ],
    ) -> Operand[
        Indexed[IndexType, EquivalentValueType], Multiple[Ordered], Unpack[Levels]
    ]: ...

    @overload
    def drop_duplicates(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndexType]],
            Multiple[Ordered],
            Unpack[Levels],
        ],
    ) -> Operand[
        Indexed[IndexType, IndexValue[ValueIndexType]],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    def drop_duplicates(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.drop_duplicates())

    @overload
    def is_duplicated(
        self: Operand[
            Indexed[IndexType, EquivalentValueType], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Operand[Indexed[IndexType, Mask], Multiple[OrderType], Unpack[Levels]]: ...

    @overload
    def is_duplicated(
        self: Operand[Bare[EquivalentValueType], Multiple[OrderType], Unpack[Levels]],
    ) -> Operand[Bare[Mask], Multiple[OrderType], Unpack[Levels]]: ...

    @overload
    def is_duplicated(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndexType]],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Operand[Indexed[IndexType, Mask], Multiple[OrderType], Unpack[Levels]]: ...

    @overload
    def is_duplicated(
        self: Operand[
            Bare[IndexValue[ValueIndexType]], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Operand[Bare[Mask], Multiple[OrderType], Unpack[Levels]]: ...

    def is_duplicated(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.is_duplicated())

    @overload
    def unique(
        self: Operand[Bare[EquivalentValueType], Multiple[OrderType], Unpack[Levels]],
    ) -> Operand[Bare[EquivalentValueType], Multiple[OrderType], Unpack[Levels]]: ...

    @overload
    def unique(
        self: Operand[
            Bare[IndexValue[ValueIndexType]], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Operand[
        Bare[IndexValue[ValueIndexType]], Multiple[OrderType], Unpack[Levels]
    ]: ...

    def unique(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.unique())

    @overload
    def take(
        self: Operand[Indexed[IndexType, V], Multiple[Ordered], Unpack[Levels]],
        elements: int,
    ) -> Operand[Indexed[IndexType, V], Multiple[Ordered], Unpack[Levels]]: ...

    @overload
    def take(
        self: Operand[Bare[BareValueType], Multiple[Ordered], Unpack[Levels]],
        elements: int,
    ) -> Operand[Bare[BareValueType], Multiple[Ordered], Unpack[Levels]]: ...

    def take(self, elements: int) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.take(elements))

    @overload
    def is_bool(
        self: Operand[
            Indexed[IndexType, ScalarInspectableValueType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def is_bool(
        self: Operand[Bare[ScalarInspectableValueType], ContainerType, Unpack[Levels]],
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def is_bool(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.is_bool())

    @overload
    def is_datetime(
        self: Operand[
            Indexed[IndexType, ScalarInspectableValueType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def is_datetime(
        self: Operand[Bare[ScalarInspectableValueType], ContainerType, Unpack[Levels]],
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def is_datetime(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.is_datetime())

    @overload
    def is_duration(
        self: Operand[
            Indexed[IndexType, ScalarInspectableValueType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def is_duration(
        self: Operand[Bare[ScalarInspectableValueType], ContainerType, Unpack[Levels]],
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def is_duration(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.is_duration())

    @overload
    def is_float(
        self: Operand[
            Indexed[IndexType, ScalarInspectableValueType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def is_float(
        self: Operand[Bare[ScalarInspectableValueType], ContainerType, Unpack[Levels]],
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def is_float(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.is_float())

    @overload
    def is_null(
        self: Operand[
            Indexed[IndexType, ScalarInspectableValueType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def is_null(
        self: Operand[Bare[ScalarInspectableValueType], ContainerType, Unpack[Levels]],
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def is_null(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.is_null())

    @overload
    def is_int(
        self: Operand[
            Indexed[IndexType, InspectableValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def is_int(
        self: Operand[Bare[InspectableValueType], ContainerType, Unpack[Levels]],
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def is_int(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.is_int())

    @overload
    def is_string(
        self: Operand[
            Indexed[IndexType, InspectableValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def is_string(
        self: Operand[Bare[InspectableValueType], ContainerType, Unpack[Levels]],
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def is_string(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.is_string())

    @overload
    def abs(
        self: Operand[
            Indexed[IndexType, NumericValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Operand[
        Indexed[IndexType, NumericValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def abs(
        self: Operand[Bare[NumericValueType], ContainerType, Unpack[Levels]],
    ) -> Operand[Bare[NumericValueType], ContainerType, Unpack[Levels]]: ...

    def abs(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.abs())

    @overload
    def neg(
        self: Operand[
            Indexed[IndexType, NumericValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Operand[
        Indexed[IndexType, NumericValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def neg(
        self: Operand[Bare[NumericValueType], ContainerType, Unpack[Levels]],
    ) -> Operand[Bare[NumericValueType], ContainerType, Unpack[Levels]]: ...

    def neg(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.neg())

    @overload
    def sign(
        self: Operand[
            Indexed[IndexType, NumericValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Operand[
        Indexed[IndexType, NumericValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def sign(
        self: Operand[Bare[NumericValueType], ContainerType, Unpack[Levels]],
    ) -> Operand[Bare[NumericValueType], ContainerType, Unpack[Levels]]: ...

    def sign(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.sign())

    @overload
    def ceil(
        self: Operand[
            Indexed[IndexType, RealNumericValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Operand[
        Indexed[IndexType, RealNumericValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def ceil(
        self: Operand[Bare[RealNumericValueType], ContainerType, Unpack[Levels]],
    ) -> Operand[Bare[RealNumericValueType], ContainerType, Unpack[Levels]]: ...

    def ceil(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.ceil())

    @overload
    def cbrt(
        self: Operand[
            Indexed[IndexType, RealNumericValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Operand[
        Indexed[IndexType, RealNumericValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def cbrt(
        self: Operand[Bare[RealNumericValueType], ContainerType, Unpack[Levels]],
    ) -> Operand[Bare[RealNumericValueType], ContainerType, Unpack[Levels]]: ...

    def cbrt(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.cbrt())

    @overload
    def exp(
        self: Operand[
            Indexed[IndexType, RealNumericValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Operand[
        Indexed[IndexType, RealNumericValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def exp(
        self: Operand[Bare[RealNumericValueType], ContainerType, Unpack[Levels]],
    ) -> Operand[Bare[RealNumericValueType], ContainerType, Unpack[Levels]]: ...

    def exp(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.exp())

    @overload
    def floor(
        self: Operand[
            Indexed[IndexType, RealNumericValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Operand[
        Indexed[IndexType, RealNumericValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def floor(
        self: Operand[Bare[RealNumericValueType], ContainerType, Unpack[Levels]],
    ) -> Operand[Bare[RealNumericValueType], ContainerType, Unpack[Levels]]: ...

    def floor(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.floor())

    @overload
    def log(
        self: Operand[
            Indexed[IndexType, RealNumericValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Operand[
        Indexed[IndexType, RealNumericValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def log(
        self: Operand[Bare[RealNumericValueType], ContainerType, Unpack[Levels]],
    ) -> Operand[Bare[RealNumericValueType], ContainerType, Unpack[Levels]]: ...

    def log(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.log())

    @overload
    def round(
        self: Operand[
            Indexed[IndexType, RealNumericValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Operand[
        Indexed[IndexType, RealNumericValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def round(
        self: Operand[Bare[RealNumericValueType], ContainerType, Unpack[Levels]],
    ) -> Operand[Bare[RealNumericValueType], ContainerType, Unpack[Levels]]: ...

    def round(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.round())

    @overload
    def sqrt(
        self: Operand[
            Indexed[IndexType, RealNumericValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Operand[
        Indexed[IndexType, RealNumericValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def sqrt(
        self: Operand[Bare[RealNumericValueType], ContainerType, Unpack[Levels]],
    ) -> Operand[Bare[RealNumericValueType], ContainerType, Unpack[Levels]]: ...

    def sqrt(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.sqrt())

    @overload
    def trim(
        self: Operand[
            Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Operand[
        Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def trim(
        self: Operand[Bare[StringValueType], ContainerType, Unpack[Levels]],
    ) -> Operand[Bare[StringValueType], ContainerType, Unpack[Levels]]: ...

    def trim(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.trim())

    @overload
    def trim_start(
        self: Operand[
            Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Operand[
        Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def trim_start(
        self: Operand[Bare[StringValueType], ContainerType, Unpack[Levels]],
    ) -> Operand[Bare[StringValueType], ContainerType, Unpack[Levels]]: ...

    def trim_start(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.trim_start())

    @overload
    def trim_end(
        self: Operand[
            Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Operand[
        Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def trim_end(
        self: Operand[Bare[StringValueType], ContainerType, Unpack[Levels]],
    ) -> Operand[Bare[StringValueType], ContainerType, Unpack[Levels]]: ...

    def trim_end(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.trim_end())

    @overload
    def lowercase(
        self: Operand[
            Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Operand[
        Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def lowercase(
        self: Operand[Bare[StringValueType], ContainerType, Unpack[Levels]],
    ) -> Operand[Bare[StringValueType], ContainerType, Unpack[Levels]]: ...

    def lowercase(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.lowercase())

    @overload
    def uppercase(
        self: Operand[
            Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Operand[
        Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def uppercase(
        self: Operand[Bare[StringValueType], ContainerType, Unpack[Levels]],
    ) -> Operand[Bare[StringValueType], ContainerType, Unpack[Levels]]: ...

    def uppercase(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.uppercase())

    @overload
    def reverse(
        self: Operand[
            Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Operand[
        Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def reverse(
        self: Operand[Bare[StringValueType], ContainerType, Unpack[Levels]],
    ) -> Operand[Bare[StringValueType], ContainerType, Unpack[Levels]]: ...

    def reverse(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.reverse())

    @overload
    def length(
        self: Operand[
            Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Operand[Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]]: ...

    @overload
    def length(
        self: Operand[Bare[StringValueType], ContainerType, Unpack[Levels]],
    ) -> Operand[Bare[Scalar], ContainerType, Unpack[Levels]]: ...

    def length(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.length())

    @overload
    def slice(
        self: Operand[
            Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
        ],
        start: int,
        end: int,
    ) -> Operand[
        Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def slice(
        self: Operand[Bare[StringValueType], ContainerType, Unpack[Levels]],
        start: int,
        end: int,
    ) -> Operand[Bare[StringValueType], ContainerType, Unpack[Levels]]: ...

    def slice(self, start: int, end: int) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.slice(start, end))

    @overload
    def starts_with(
        self: Operand[Indexed[IndexType, StringValueType], Definite, Unpack[Levels]],
        prefix: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Operand[Indexed[IndexType, Mask], Single, Unpack[Levels]]: ...

    @overload
    def starts_with(
        self: Operand[
            Indexed[IndexType, StringValueType], DroppedContainerType, Unpack[Levels]
        ],
        prefix: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Operand[Indexed[IndexType, Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def starts_with(
        self: Operand[Bare[StringValueType], Definite, Unpack[Levels]],
        prefix: BareDroppingArgument[StringArgumentValueType],
    ) -> Operand[Bare[Mask], Single, Unpack[Levels]]: ...

    @overload
    def starts_with(
        self: Operand[Bare[StringValueType], DroppedContainerType, Unpack[Levels]],
        prefix: BareDroppingArgument[StringArgumentValueType],
    ) -> Operand[Bare[Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def starts_with(
        self: Operand[
            Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
        ],
        prefix: IndexedOperandArgument[
            IndexType, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def starts_with(
        self: Operand[Bare[StringValueType], ContainerType, Unpack[Levels]],
        prefix: BareOperandArgument[StringArgumentValueType],
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def starts_with(
        self: Operand[
            Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
        ],
        prefix: ScalarValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def starts_with(
        self: Operand[Bare[StringValueType], ContainerType, Unpack[Levels]],
        prefix: ScalarValue,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def starts_with(
        self,
        prefix: Union[
            ScalarValue, Operand[Any, Any, Unpack[Tuple[Any, ...]]], Argument[Any, Any]
        ],
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(
            self._operand.starts_with(Operand._to_py_argument(prefix))
        )

    @overload
    def ends_with(
        self: Operand[Indexed[IndexType, StringValueType], Definite, Unpack[Levels]],
        suffix: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Operand[Indexed[IndexType, Mask], Single, Unpack[Levels]]: ...

    @overload
    def ends_with(
        self: Operand[
            Indexed[IndexType, StringValueType], DroppedContainerType, Unpack[Levels]
        ],
        suffix: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Operand[Indexed[IndexType, Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def ends_with(
        self: Operand[Bare[StringValueType], Definite, Unpack[Levels]],
        suffix: BareDroppingArgument[StringArgumentValueType],
    ) -> Operand[Bare[Mask], Single, Unpack[Levels]]: ...

    @overload
    def ends_with(
        self: Operand[Bare[StringValueType], DroppedContainerType, Unpack[Levels]],
        suffix: BareDroppingArgument[StringArgumentValueType],
    ) -> Operand[Bare[Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def ends_with(
        self: Operand[
            Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
        ],
        suffix: IndexedOperandArgument[
            IndexType, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def ends_with(
        self: Operand[Bare[StringValueType], ContainerType, Unpack[Levels]],
        suffix: BareOperandArgument[StringArgumentValueType],
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def ends_with(
        self: Operand[
            Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
        ],
        suffix: ScalarValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def ends_with(
        self: Operand[Bare[StringValueType], ContainerType, Unpack[Levels]],
        suffix: ScalarValue,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def ends_with(
        self,
        suffix: Union[
            ScalarValue, Operand[Any, Any, Unpack[Tuple[Any, ...]]], Argument[Any, Any]
        ],
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(
            self._operand.ends_with(Operand._to_py_argument(suffix))
        )

    @overload
    def contains(
        self: Operand[Indexed[IndexType, StringValueType], Definite, Unpack[Levels]],
        part: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Operand[Indexed[IndexType, Mask], Single, Unpack[Levels]]: ...

    @overload
    def contains(
        self: Operand[
            Indexed[IndexType, StringValueType], DroppedContainerType, Unpack[Levels]
        ],
        part: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Operand[Indexed[IndexType, Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def contains(
        self: Operand[Bare[StringValueType], Definite, Unpack[Levels]],
        part: BareDroppingArgument[StringArgumentValueType],
    ) -> Operand[Bare[Mask], Single, Unpack[Levels]]: ...

    @overload
    def contains(
        self: Operand[Bare[StringValueType], DroppedContainerType, Unpack[Levels]],
        part: BareDroppingArgument[StringArgumentValueType],
    ) -> Operand[Bare[Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def contains(
        self: Operand[
            Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
        ],
        part: IndexedOperandArgument[
            IndexType, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def contains(
        self: Operand[Bare[StringValueType], ContainerType, Unpack[Levels]],
        part: BareOperandArgument[StringArgumentValueType],
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def contains(
        self: Operand[
            Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
        ],
        part: ScalarValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def contains(
        self: Operand[Bare[StringValueType], ContainerType, Unpack[Levels]],
        part: ScalarValue,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def contains(
        self,
        part: Union[
            ScalarValue, Operand[Any, Any, Unpack[Tuple[Any, ...]]], Argument[Any, Any]
        ],
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(
            self._operand.contains(Operand._to_py_argument(part))
        )

    @overload
    def matches(
        self: Operand[Indexed[IndexType, StringValueType], Definite, Unpack[Levels]],
        pattern: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Operand[Indexed[IndexType, Mask], Single, Unpack[Levels]]: ...

    @overload
    def matches(
        self: Operand[
            Indexed[IndexType, StringValueType], DroppedContainerType, Unpack[Levels]
        ],
        pattern: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Operand[Indexed[IndexType, Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def matches(
        self: Operand[Bare[StringValueType], Definite, Unpack[Levels]],
        pattern: BareDroppingArgument[StringArgumentValueType],
    ) -> Operand[Bare[Mask], Single, Unpack[Levels]]: ...

    @overload
    def matches(
        self: Operand[Bare[StringValueType], DroppedContainerType, Unpack[Levels]],
        pattern: BareDroppingArgument[StringArgumentValueType],
    ) -> Operand[Bare[Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def matches(
        self: Operand[
            Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
        ],
        pattern: IndexedOperandArgument[
            IndexType, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def matches(
        self: Operand[Bare[StringValueType], ContainerType, Unpack[Levels]],
        pattern: BareOperandArgument[StringArgumentValueType],
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def matches(
        self: Operand[
            Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
        ],
        pattern: ScalarValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def matches(
        self: Operand[Bare[StringValueType], ContainerType, Unpack[Levels]],
        pattern: ScalarValue,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def matches(
        self,
        pattern: Union[
            ScalarValue, Operand[Any, Any, Unpack[Tuple[Any, ...]]], Argument[Any, Any]
        ],
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(
            self._operand.matches(Operand._to_py_argument(pattern))
        )

    @overload
    def strip_prefix(
        self: Operand[Indexed[IndexType, StringValueType], Definite, Unpack[Levels]],
        prefix: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Operand[Indexed[IndexType, StringValueType], Single, Unpack[Levels]]: ...

    @overload
    def strip_prefix(
        self: Operand[
            Indexed[IndexType, StringValueType], DroppedContainerType, Unpack[Levels]
        ],
        prefix: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Operand[
        Indexed[IndexType, StringValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def strip_prefix(
        self: Operand[Bare[StringValueType], Definite, Unpack[Levels]],
        prefix: BareDroppingArgument[StringArgumentValueType],
    ) -> Operand[Bare[StringValueType], Single, Unpack[Levels]]: ...

    @overload
    def strip_prefix(
        self: Operand[Bare[StringValueType], DroppedContainerType, Unpack[Levels]],
        prefix: BareDroppingArgument[StringArgumentValueType],
    ) -> Operand[Bare[StringValueType], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def strip_prefix(
        self: Operand[
            Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
        ],
        prefix: IndexedOperandArgument[
            IndexType, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def strip_prefix(
        self: Operand[Bare[StringValueType], ContainerType, Unpack[Levels]],
        prefix: BareOperandArgument[StringArgumentValueType],
    ) -> Operand[Bare[StringValueType], ContainerType, Unpack[Levels]]: ...

    @overload
    def strip_prefix(
        self: Operand[
            Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
        ],
        prefix: ScalarValue,
    ) -> Operand[
        Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def strip_prefix(
        self: Operand[Bare[StringValueType], ContainerType, Unpack[Levels]],
        prefix: ScalarValue,
    ) -> Operand[Bare[StringValueType], ContainerType, Unpack[Levels]]: ...

    def strip_prefix(
        self,
        prefix: Union[
            ScalarValue, Operand[Any, Any, Unpack[Tuple[Any, ...]]], Argument[Any, Any]
        ],
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(
            self._operand.strip_prefix(Operand._to_py_argument(prefix))
        )

    @overload
    def strip_suffix(
        self: Operand[Indexed[IndexType, StringValueType], Definite, Unpack[Levels]],
        suffix: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Operand[Indexed[IndexType, StringValueType], Single, Unpack[Levels]]: ...

    @overload
    def strip_suffix(
        self: Operand[
            Indexed[IndexType, StringValueType], DroppedContainerType, Unpack[Levels]
        ],
        suffix: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Operand[
        Indexed[IndexType, StringValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def strip_suffix(
        self: Operand[Bare[StringValueType], Definite, Unpack[Levels]],
        suffix: BareDroppingArgument[StringArgumentValueType],
    ) -> Operand[Bare[StringValueType], Single, Unpack[Levels]]: ...

    @overload
    def strip_suffix(
        self: Operand[Bare[StringValueType], DroppedContainerType, Unpack[Levels]],
        suffix: BareDroppingArgument[StringArgumentValueType],
    ) -> Operand[Bare[StringValueType], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def strip_suffix(
        self: Operand[
            Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
        ],
        suffix: IndexedOperandArgument[
            IndexType, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def strip_suffix(
        self: Operand[Bare[StringValueType], ContainerType, Unpack[Levels]],
        suffix: BareOperandArgument[StringArgumentValueType],
    ) -> Operand[Bare[StringValueType], ContainerType, Unpack[Levels]]: ...

    @overload
    def strip_suffix(
        self: Operand[
            Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
        ],
        suffix: ScalarValue,
    ) -> Operand[
        Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def strip_suffix(
        self: Operand[Bare[StringValueType], ContainerType, Unpack[Levels]],
        suffix: ScalarValue,
    ) -> Operand[Bare[StringValueType], ContainerType, Unpack[Levels]]: ...

    def strip_suffix(
        self,
        suffix: Union[
            ScalarValue, Operand[Any, Any, Unpack[Tuple[Any, ...]]], Argument[Any, Any]
        ],
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(
            self._operand.strip_suffix(Operand._to_py_argument(suffix))
        )

    @overload
    def replace(
        self: Operand[
            Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
        ],
        old: IndexedStringArgument[IndexType, OldStringValueType, ArgumentOrderType],
        new: IndexedStringArgument[IndexType, NewStringValueType, ArgumentOrderType],
    ) -> Operand[
        Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def replace(
        self: Operand[Indexed[IndexType, StringValueType], Definite, Unpack[Levels]],
        old: IndexedDroppingArgument[IndexType, OldStringValueType],
        new: IndexedAnyStringArgument[IndexType, NewStringValueType, ArgumentOrderType],
    ) -> Operand[Indexed[IndexType, StringValueType], Single, Unpack[Levels]]: ...

    @overload
    def replace(
        self: Operand[
            Indexed[IndexType, StringValueType], DroppedContainerType, Unpack[Levels]
        ],
        old: IndexedDroppingArgument[IndexType, OldStringValueType],
        new: IndexedAnyStringArgument[IndexType, NewStringValueType, ArgumentOrderType],
    ) -> Operand[
        Indexed[IndexType, StringValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def replace(
        self: Operand[Indexed[IndexType, StringValueType], Definite, Unpack[Levels]],
        old: IndexedStringArgument[IndexType, OldStringValueType, ArgumentOrderType],
        new: IndexedDroppingArgument[IndexType, NewStringValueType],
    ) -> Operand[Indexed[IndexType, StringValueType], Single, Unpack[Levels]]: ...

    @overload
    def replace(
        self: Operand[
            Indexed[IndexType, StringValueType], DroppedContainerType, Unpack[Levels]
        ],
        old: IndexedStringArgument[IndexType, OldStringValueType, ArgumentOrderType],
        new: IndexedDroppingArgument[IndexType, NewStringValueType],
    ) -> Operand[
        Indexed[IndexType, StringValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def replace(
        self: Operand[Bare[StringValueType], ContainerType, Unpack[Levels]],
        old: BareStringArgument[OldStringValueType],
        new: BareStringArgument[NewStringValueType],
    ) -> Operand[Bare[StringValueType], ContainerType, Unpack[Levels]]: ...

    @overload
    def replace(
        self: Operand[Bare[StringValueType], Definite, Unpack[Levels]],
        old: BareDroppingArgument[OldStringValueType],
        new: BareAnyStringArgument[NewStringValueType],
    ) -> Operand[Bare[StringValueType], Single, Unpack[Levels]]: ...

    @overload
    def replace(
        self: Operand[Bare[StringValueType], DroppedContainerType, Unpack[Levels]],
        old: BareDroppingArgument[OldStringValueType],
        new: BareAnyStringArgument[NewStringValueType],
    ) -> Operand[Bare[StringValueType], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def replace(
        self: Operand[Bare[StringValueType], Definite, Unpack[Levels]],
        old: BareStringArgument[OldStringValueType],
        new: BareDroppingArgument[NewStringValueType],
    ) -> Operand[Bare[StringValueType], Single, Unpack[Levels]]: ...

    @overload
    def replace(
        self: Operand[Bare[StringValueType], DroppedContainerType, Unpack[Levels]],
        old: BareStringArgument[OldStringValueType],
        new: BareDroppingArgument[NewStringValueType],
    ) -> Operand[Bare[StringValueType], DroppedContainerType, Unpack[Levels]]: ...

    def replace(
        self,
        old: Union[
            ScalarValue, Operand[Any, Any, Unpack[Tuple[Any, ...]]], Argument[Any, Any]
        ],
        new: Union[
            ScalarValue, Operand[Any, Any, Unpack[Tuple[Any, ...]]], Argument[Any, Any]
        ],
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(
            self._operand.replace(
                Operand._to_py_argument(old), Operand._to_py_argument(new)
            )
        )

    @overload
    def replace_all(
        self: Operand[
            Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
        ],
        old: IndexedStringArgument[IndexType, OldStringValueType, ArgumentOrderType],
        new: IndexedStringArgument[IndexType, NewStringValueType, ArgumentOrderType],
    ) -> Operand[
        Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def replace_all(
        self: Operand[Indexed[IndexType, StringValueType], Definite, Unpack[Levels]],
        old: IndexedDroppingArgument[IndexType, OldStringValueType],
        new: IndexedAnyStringArgument[IndexType, NewStringValueType, ArgumentOrderType],
    ) -> Operand[Indexed[IndexType, StringValueType], Single, Unpack[Levels]]: ...

    @overload
    def replace_all(
        self: Operand[
            Indexed[IndexType, StringValueType], DroppedContainerType, Unpack[Levels]
        ],
        old: IndexedDroppingArgument[IndexType, OldStringValueType],
        new: IndexedAnyStringArgument[IndexType, NewStringValueType, ArgumentOrderType],
    ) -> Operand[
        Indexed[IndexType, StringValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def replace_all(
        self: Operand[Indexed[IndexType, StringValueType], Definite, Unpack[Levels]],
        old: IndexedStringArgument[IndexType, OldStringValueType, ArgumentOrderType],
        new: IndexedDroppingArgument[IndexType, NewStringValueType],
    ) -> Operand[Indexed[IndexType, StringValueType], Single, Unpack[Levels]]: ...

    @overload
    def replace_all(
        self: Operand[
            Indexed[IndexType, StringValueType], DroppedContainerType, Unpack[Levels]
        ],
        old: IndexedStringArgument[IndexType, OldStringValueType, ArgumentOrderType],
        new: IndexedDroppingArgument[IndexType, NewStringValueType],
    ) -> Operand[
        Indexed[IndexType, StringValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def replace_all(
        self: Operand[Bare[StringValueType], ContainerType, Unpack[Levels]],
        old: BareStringArgument[OldStringValueType],
        new: BareStringArgument[NewStringValueType],
    ) -> Operand[Bare[StringValueType], ContainerType, Unpack[Levels]]: ...

    @overload
    def replace_all(
        self: Operand[Bare[StringValueType], Definite, Unpack[Levels]],
        old: BareDroppingArgument[OldStringValueType],
        new: BareAnyStringArgument[NewStringValueType],
    ) -> Operand[Bare[StringValueType], Single, Unpack[Levels]]: ...

    @overload
    def replace_all(
        self: Operand[Bare[StringValueType], DroppedContainerType, Unpack[Levels]],
        old: BareDroppingArgument[OldStringValueType],
        new: BareAnyStringArgument[NewStringValueType],
    ) -> Operand[Bare[StringValueType], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def replace_all(
        self: Operand[Bare[StringValueType], Definite, Unpack[Levels]],
        old: BareStringArgument[OldStringValueType],
        new: BareDroppingArgument[NewStringValueType],
    ) -> Operand[Bare[StringValueType], Single, Unpack[Levels]]: ...

    @overload
    def replace_all(
        self: Operand[Bare[StringValueType], DroppedContainerType, Unpack[Levels]],
        old: BareStringArgument[OldStringValueType],
        new: BareDroppingArgument[NewStringValueType],
    ) -> Operand[Bare[StringValueType], DroppedContainerType, Unpack[Levels]]: ...

    def replace_all(
        self,
        old: Union[
            ScalarValue, Operand[Any, Any, Unpack[Tuple[Any, ...]]], Argument[Any, Any]
        ],
        new: Union[
            ScalarValue, Operand[Any, Any, Unpack[Tuple[Any, ...]]], Argument[Any, Any]
        ],
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(
            self._operand.replace_all(
                Operand._to_py_argument(old), Operand._to_py_argument(new)
            )
        )

    @overload
    def pad_start(
        self: Operand[
            Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
        ],
        width: IndexedIntegerArgument[IndexType, IntegerValueType, ArgumentOrderType],
        character: IndexedStringArgument[
            IndexType, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def pad_start(
        self: Operand[Indexed[IndexType, StringValueType], Definite, Unpack[Levels]],
        width: IndexedDroppingArgument[IndexType, IntegerValueType],
        character: IndexedAnyStringArgument[
            IndexType, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[Indexed[IndexType, StringValueType], Single, Unpack[Levels]]: ...

    @overload
    def pad_start(
        self: Operand[
            Indexed[IndexType, StringValueType], DroppedContainerType, Unpack[Levels]
        ],
        width: IndexedDroppingArgument[IndexType, IntegerValueType],
        character: IndexedAnyStringArgument[
            IndexType, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[IndexType, StringValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def pad_start(
        self: Operand[Indexed[IndexType, StringValueType], Definite, Unpack[Levels]],
        width: IndexedIntegerArgument[IndexType, IntegerValueType, ArgumentOrderType],
        character: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Operand[Indexed[IndexType, StringValueType], Single, Unpack[Levels]]: ...

    @overload
    def pad_start(
        self: Operand[
            Indexed[IndexType, StringValueType], DroppedContainerType, Unpack[Levels]
        ],
        width: IndexedIntegerArgument[IndexType, IntegerValueType, ArgumentOrderType],
        character: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Operand[
        Indexed[IndexType, StringValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def pad_start(
        self: Operand[Bare[StringValueType], ContainerType, Unpack[Levels]],
        width: BareIntegerArgument[IntegerValueType],
        character: BareStringArgument[StringArgumentValueType],
    ) -> Operand[Bare[StringValueType], ContainerType, Unpack[Levels]]: ...

    @overload
    def pad_start(
        self: Operand[Bare[StringValueType], Definite, Unpack[Levels]],
        width: BareDroppingArgument[IntegerValueType],
        character: BareAnyStringArgument[StringArgumentValueType],
    ) -> Operand[Bare[StringValueType], Single, Unpack[Levels]]: ...

    @overload
    def pad_start(
        self: Operand[Bare[StringValueType], DroppedContainerType, Unpack[Levels]],
        width: BareDroppingArgument[IntegerValueType],
        character: BareAnyStringArgument[StringArgumentValueType],
    ) -> Operand[Bare[StringValueType], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def pad_start(
        self: Operand[Bare[StringValueType], Definite, Unpack[Levels]],
        width: BareIntegerArgument[IntegerValueType],
        character: BareDroppingArgument[StringArgumentValueType],
    ) -> Operand[Bare[StringValueType], Single, Unpack[Levels]]: ...

    @overload
    def pad_start(
        self: Operand[Bare[StringValueType], DroppedContainerType, Unpack[Levels]],
        width: BareIntegerArgument[IntegerValueType],
        character: BareDroppingArgument[StringArgumentValueType],
    ) -> Operand[Bare[StringValueType], DroppedContainerType, Unpack[Levels]]: ...

    def pad_start(
        self,
        width: Union[
            int, Operand[Any, Any, Unpack[Tuple[Any, ...]]], Argument[Any, Any]
        ],
        character: Union[
            ScalarValue, Operand[Any, Any, Unpack[Tuple[Any, ...]]], Argument[Any, Any]
        ],
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(
            self._operand.pad_start(
                Operand._to_py_argument(width), Operand._to_py_argument(character)
            )
        )

    @overload
    def pad_end(
        self: Operand[
            Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
        ],
        width: IndexedIntegerArgument[IndexType, IntegerValueType, ArgumentOrderType],
        character: IndexedStringArgument[
            IndexType, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def pad_end(
        self: Operand[Indexed[IndexType, StringValueType], Definite, Unpack[Levels]],
        width: IndexedDroppingArgument[IndexType, IntegerValueType],
        character: IndexedAnyStringArgument[
            IndexType, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[Indexed[IndexType, StringValueType], Single, Unpack[Levels]]: ...

    @overload
    def pad_end(
        self: Operand[
            Indexed[IndexType, StringValueType], DroppedContainerType, Unpack[Levels]
        ],
        width: IndexedDroppingArgument[IndexType, IntegerValueType],
        character: IndexedAnyStringArgument[
            IndexType, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[IndexType, StringValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def pad_end(
        self: Operand[Indexed[IndexType, StringValueType], Definite, Unpack[Levels]],
        width: IndexedIntegerArgument[IndexType, IntegerValueType, ArgumentOrderType],
        character: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Operand[Indexed[IndexType, StringValueType], Single, Unpack[Levels]]: ...

    @overload
    def pad_end(
        self: Operand[
            Indexed[IndexType, StringValueType], DroppedContainerType, Unpack[Levels]
        ],
        width: IndexedIntegerArgument[IndexType, IntegerValueType, ArgumentOrderType],
        character: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Operand[
        Indexed[IndexType, StringValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def pad_end(
        self: Operand[Bare[StringValueType], ContainerType, Unpack[Levels]],
        width: BareIntegerArgument[IntegerValueType],
        character: BareStringArgument[StringArgumentValueType],
    ) -> Operand[Bare[StringValueType], ContainerType, Unpack[Levels]]: ...

    @overload
    def pad_end(
        self: Operand[Bare[StringValueType], Definite, Unpack[Levels]],
        width: BareDroppingArgument[IntegerValueType],
        character: BareAnyStringArgument[StringArgumentValueType],
    ) -> Operand[Bare[StringValueType], Single, Unpack[Levels]]: ...

    @overload
    def pad_end(
        self: Operand[Bare[StringValueType], DroppedContainerType, Unpack[Levels]],
        width: BareDroppingArgument[IntegerValueType],
        character: BareAnyStringArgument[StringArgumentValueType],
    ) -> Operand[Bare[StringValueType], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def pad_end(
        self: Operand[Bare[StringValueType], Definite, Unpack[Levels]],
        width: BareIntegerArgument[IntegerValueType],
        character: BareDroppingArgument[StringArgumentValueType],
    ) -> Operand[Bare[StringValueType], Single, Unpack[Levels]]: ...

    @overload
    def pad_end(
        self: Operand[Bare[StringValueType], DroppedContainerType, Unpack[Levels]],
        width: BareIntegerArgument[IntegerValueType],
        character: BareDroppingArgument[StringArgumentValueType],
    ) -> Operand[Bare[StringValueType], DroppedContainerType, Unpack[Levels]]: ...

    def pad_end(
        self,
        width: Union[
            int, Operand[Any, Any, Unpack[Tuple[Any, ...]]], Argument[Any, Any]
        ],
        character: Union[
            ScalarValue, Operand[Any, Any, Unpack[Tuple[Any, ...]]], Argument[Any, Any]
        ],
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(
            self._operand.pad_end(
                Operand._to_py_argument(width), Operand._to_py_argument(character)
            )
        )

    @overload
    def split(
        self: Operand[
            Indexed[NodeIndex, StringValueType], Multiple[OrderType], Unpack[Levels]
        ],
        delimiter: IndexedAnyStringArgument[
            NodeIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[
            Expanded[NodeIndex, Positional, Tuple[NodeIndexPayload, Optional[int]]],
            StringValueType,
        ],
        Multiple[OrderType],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Operand[
            Indexed[EdgeIndex, StringValueType], Multiple[OrderType], Unpack[Levels]
        ],
        delimiter: IndexedAnyStringArgument[
            EdgeIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[
            Expanded[EdgeIndex, Positional, Tuple[EdgeIndexPayload, Optional[int]]],
            StringValueType,
        ],
        Multiple[OrderType],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Operand[
            Indexed[Positional, StringValueType], Multiple[OrderType], Unpack[Levels]
        ],
        delimiter: IndexedAnyStringArgument[
            Positional, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[
            Expanded[Positional, Positional, Tuple[int, Optional[int]]],
            StringValueType,
        ],
        Multiple[OrderType],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Operand[
            Indexed[EndpointRole, StringValueType], Multiple[OrderType], Unpack[Levels]
        ],
        delimiter: IndexedAnyStringArgument[
            EndpointRole, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[
            Expanded[EndpointRole, Positional, Tuple[EdgeEndpointRole, Optional[int]]],
            StringValueType,
        ],
        Multiple[OrderType],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Operand[
            Indexed[ValueIndex, StringValueType], Multiple[OrderType], Unpack[Levels]
        ],
        delimiter: IndexedAnyStringArgument[
            ValueIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[
            Expanded[ValueIndex, Positional, Tuple[ScalarValue, Optional[int]]],
            StringValueType,
        ],
        Multiple[OrderType],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Operand[
            Indexed[AttributeNameIndex, StringValueType],
            Multiple[OrderType],
            Unpack[Levels],
        ],
        delimiter: IndexedAnyStringArgument[
            AttributeNameIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[
            Expanded[AttributeNameIndex, Positional, Tuple[Attribute, Optional[int]]],
            StringValueType,
        ],
        Multiple[OrderType],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Operand[
            Indexed[BoolIndex, StringValueType], Multiple[OrderType], Unpack[Levels]
        ],
        delimiter: IndexedAnyStringArgument[
            BoolIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[
            Expanded[BoolIndex, Positional, Tuple[bool, Optional[int]]],
            StringValueType,
        ],
        Multiple[OrderType],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Operand[
            Indexed[FailureKindIndex, StringValueType],
            Multiple[OrderType],
            Unpack[Levels],
        ],
        delimiter: IndexedAnyStringArgument[
            FailureKindIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[
            Expanded[FailureKindIndex, Positional, Tuple[FailureKind, Optional[int]]],
            StringValueType,
        ],
        Multiple[OrderType],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Operand[
            Indexed[Expanded[K, ChildType, ParentPayloadType], StringValueType],
            Multiple[OrderType],
            Unpack[Levels],
        ],
        delimiter: IndexedAnyStringArgument[
            Expanded[K, ChildType, ParentPayloadType],
            StringArgumentValueType,
            ArgumentOrderType,
        ],
    ) -> Operand[
        Indexed[
            Expanded[
                Expanded[K, ChildType, ParentPayloadType],
                Positional,
                Tuple[ParentPayloadType, Optional[int]],
            ],
            StringValueType,
        ],
        Multiple[OrderType],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Operand[Indexed[NodeIndex, StringValueType], Single, Unpack[Levels]],
        delimiter: IndexedAnyStringArgument[
            NodeIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[
            Expanded[NodeIndex, Positional, Tuple[NodeIndexPayload, Optional[int]]],
            StringValueType,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Operand[Indexed[EdgeIndex, StringValueType], Single, Unpack[Levels]],
        delimiter: IndexedAnyStringArgument[
            EdgeIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[
            Expanded[EdgeIndex, Positional, Tuple[EdgeIndexPayload, Optional[int]]],
            StringValueType,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Operand[Indexed[Positional, StringValueType], Single, Unpack[Levels]],
        delimiter: IndexedAnyStringArgument[
            Positional, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[
            Expanded[Positional, Positional, Tuple[int, Optional[int]]],
            StringValueType,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Operand[Indexed[EndpointRole, StringValueType], Single, Unpack[Levels]],
        delimiter: IndexedAnyStringArgument[
            EndpointRole, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[
            Expanded[EndpointRole, Positional, Tuple[EdgeEndpointRole, Optional[int]]],
            StringValueType,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Operand[Indexed[ValueIndex, StringValueType], Single, Unpack[Levels]],
        delimiter: IndexedAnyStringArgument[
            ValueIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[
            Expanded[ValueIndex, Positional, Tuple[ScalarValue, Optional[int]]],
            StringValueType,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Operand[
            Indexed[AttributeNameIndex, StringValueType], Single, Unpack[Levels]
        ],
        delimiter: IndexedAnyStringArgument[
            AttributeNameIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[
            Expanded[AttributeNameIndex, Positional, Tuple[Attribute, Optional[int]]],
            StringValueType,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Operand[Indexed[BoolIndex, StringValueType], Single, Unpack[Levels]],
        delimiter: IndexedAnyStringArgument[
            BoolIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[
            Expanded[BoolIndex, Positional, Tuple[bool, Optional[int]]],
            StringValueType,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Operand[
            Indexed[FailureKindIndex, StringValueType], Single, Unpack[Levels]
        ],
        delimiter: IndexedAnyStringArgument[
            FailureKindIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[
            Expanded[FailureKindIndex, Positional, Tuple[FailureKind, Optional[int]]],
            StringValueType,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Operand[
            Indexed[Expanded[K, ChildType, ParentPayloadType], StringValueType],
            Single,
            Unpack[Levels],
        ],
        delimiter: IndexedAnyStringArgument[
            Expanded[K, ChildType, ParentPayloadType],
            StringArgumentValueType,
            ArgumentOrderType,
        ],
    ) -> Operand[
        Indexed[
            Expanded[
                Expanded[K, ChildType, ParentPayloadType],
                Positional,
                Tuple[ParentPayloadType, Optional[int]],
            ],
            StringValueType,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Operand[Indexed[NodeIndex, StringValueType], Definite, Unpack[Levels]],
        delimiter: IndexedAnyStringArgument[
            NodeIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[
            Expanded[NodeIndex, Positional, Tuple[NodeIndexPayload, Optional[int]]],
            StringValueType,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Operand[Indexed[EdgeIndex, StringValueType], Definite, Unpack[Levels]],
        delimiter: IndexedAnyStringArgument[
            EdgeIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[
            Expanded[EdgeIndex, Positional, Tuple[EdgeIndexPayload, Optional[int]]],
            StringValueType,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Operand[Indexed[Positional, StringValueType], Definite, Unpack[Levels]],
        delimiter: IndexedAnyStringArgument[
            Positional, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[
            Expanded[Positional, Positional, Tuple[int, Optional[int]]],
            StringValueType,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Operand[Indexed[EndpointRole, StringValueType], Definite, Unpack[Levels]],
        delimiter: IndexedAnyStringArgument[
            EndpointRole, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[
            Expanded[EndpointRole, Positional, Tuple[EdgeEndpointRole, Optional[int]]],
            StringValueType,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Operand[Indexed[ValueIndex, StringValueType], Definite, Unpack[Levels]],
        delimiter: IndexedAnyStringArgument[
            ValueIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[
            Expanded[ValueIndex, Positional, Tuple[ScalarValue, Optional[int]]],
            StringValueType,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Operand[
            Indexed[AttributeNameIndex, StringValueType], Definite, Unpack[Levels]
        ],
        delimiter: IndexedAnyStringArgument[
            AttributeNameIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[
            Expanded[AttributeNameIndex, Positional, Tuple[Attribute, Optional[int]]],
            StringValueType,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Operand[Indexed[BoolIndex, StringValueType], Definite, Unpack[Levels]],
        delimiter: IndexedAnyStringArgument[
            BoolIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[
            Expanded[BoolIndex, Positional, Tuple[bool, Optional[int]]],
            StringValueType,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Operand[
            Indexed[FailureKindIndex, StringValueType], Definite, Unpack[Levels]
        ],
        delimiter: IndexedAnyStringArgument[
            FailureKindIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[
            Expanded[FailureKindIndex, Positional, Tuple[FailureKind, Optional[int]]],
            StringValueType,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Operand[
            Indexed[Expanded[K, ChildType, ParentPayloadType], StringValueType],
            Definite,
            Unpack[Levels],
        ],
        delimiter: IndexedAnyStringArgument[
            Expanded[K, ChildType, ParentPayloadType],
            StringArgumentValueType,
            ArgumentOrderType,
        ],
    ) -> Operand[
        Indexed[
            Expanded[
                Expanded[K, ChildType, ParentPayloadType],
                Positional,
                Tuple[ParentPayloadType, Optional[int]],
            ],
            StringValueType,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Operand[Bare[StringValueType], Multiple[OrderType], Unpack[Levels]],
        delimiter: BareAnyStringArgument[StringArgumentValueType],
    ) -> Operand[Bare[StringValueType], Multiple[OrderType], Unpack[Levels]]: ...

    @overload
    def split(
        self: Operand[Bare[StringValueType], Single, Unpack[Levels]],
        delimiter: BareAnyStringArgument[StringArgumentValueType],
    ) -> Operand[Bare[StringValueType], Multiple[Ordered], Unpack[Levels]]: ...

    @overload
    def split(
        self: Operand[Bare[StringValueType], Definite, Unpack[Levels]],
        delimiter: BareAnyStringArgument[StringArgumentValueType],
    ) -> Operand[Bare[StringValueType], Multiple[Ordered], Unpack[Levels]]: ...

    def split(
        self,
        delimiter: Union[
            ScalarValue, Operand[Any, Any, Unpack[Tuple[Any, ...]]], Argument[Any, Any]
        ],
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(
            self._operand.split(Operand._to_py_argument(delimiter))
        )

    @overload
    def attribute(
        self: Operand[Indexed[EntityType, Unit], ContainerType, Unpack[Levels]],
        attribute: Attribute,
    ) -> Operand[Indexed[EntityType, Scalar], ContainerType, Unpack[Levels]]: ...

    @overload
    def attribute(
        self: Operand[Indexed[IndexType, ReferenceType], ContainerType, Unpack[Levels]],
        attribute: Attribute,
    ) -> Operand[Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]]: ...

    def attribute(
        self, attribute: Attribute
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.attribute(attribute))

    @overload
    def attributes(
        self: Operand[Indexed[NodeIndex, Unit], ContainerType, Unpack[Levels]],
    ) -> Operand[
        Indexed[
            Expanded[
                NodeIndex,
                AttributeNameIndex,
                Tuple[NodeIndexPayload, Optional[Attribute]],
            ],
            AttributeName,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def attributes(
        self: Operand[Indexed[EdgeIndex, Unit], ContainerType, Unpack[Levels]],
    ) -> Operand[
        Indexed[
            Expanded[
                EdgeIndex,
                AttributeNameIndex,
                Tuple[EdgeIndexPayload, Optional[Attribute]],
            ],
            AttributeName,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def attributes(
        self: Operand[Indexed[NodeIndex, ReferenceType], ContainerType, Unpack[Levels]],
    ) -> Operand[
        Indexed[
            Expanded[
                NodeIndex,
                AttributeNameIndex,
                Tuple[NodeIndexPayload, Optional[Attribute]],
            ],
            AttributeName,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def attributes(
        self: Operand[Indexed[EdgeIndex, ReferenceType], ContainerType, Unpack[Levels]],
    ) -> Operand[
        Indexed[
            Expanded[
                EdgeIndex,
                AttributeNameIndex,
                Tuple[EdgeIndexPayload, Optional[Attribute]],
            ],
            AttributeName,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def attributes(
        self: Operand[
            Indexed[Positional, ReferenceType], ContainerType, Unpack[Levels]
        ],
    ) -> Operand[
        Indexed[
            Expanded[Positional, AttributeNameIndex, Tuple[int, Optional[Attribute]]],
            AttributeName,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def attributes(
        self: Operand[
            Indexed[EndpointRole, ReferenceType], ContainerType, Unpack[Levels]
        ],
    ) -> Operand[
        Indexed[
            Expanded[
                EndpointRole,
                AttributeNameIndex,
                Tuple[EdgeEndpointRole, Optional[Attribute]],
            ],
            AttributeName,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def attributes(
        self: Operand[
            Indexed[ValueIndex, ReferenceType], ContainerType, Unpack[Levels]
        ],
    ) -> Operand[
        Indexed[
            Expanded[
                ValueIndex, AttributeNameIndex, Tuple[ScalarValue, Optional[Attribute]]
            ],
            AttributeName,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def attributes(
        self: Operand[
            Indexed[AttributeNameIndex, ReferenceType], ContainerType, Unpack[Levels]
        ],
    ) -> Operand[
        Indexed[
            Expanded[
                AttributeNameIndex,
                AttributeNameIndex,
                Tuple[Attribute, Optional[Attribute]],
            ],
            AttributeName,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def attributes(
        self: Operand[Indexed[BoolIndex, ReferenceType], ContainerType, Unpack[Levels]],
    ) -> Operand[
        Indexed[
            Expanded[BoolIndex, AttributeNameIndex, Tuple[bool, Optional[Attribute]]],
            AttributeName,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def attributes(
        self: Operand[
            Indexed[FailureKindIndex, ReferenceType], ContainerType, Unpack[Levels]
        ],
    ) -> Operand[
        Indexed[
            Expanded[
                FailureKindIndex,
                AttributeNameIndex,
                Tuple[FailureKind, Optional[Attribute]],
            ],
            AttributeName,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def attributes(
        self: Operand[
            Indexed[Expanded[K, ChildType, ParentPayloadType], ReferenceType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Operand[
        Indexed[
            Expanded[
                Expanded[K, ChildType, ParentPayloadType],
                AttributeNameIndex,
                Tuple[ParentPayloadType, Optional[Attribute]],
            ],
            AttributeName,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    def attributes(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.attributes())

    @overload
    def resolve(
        self: Operand[
            Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
        ],
    ) -> Operand[Indexed[IndexType, NodeReference], ContainerType, Unpack[Levels]]: ...

    @overload
    def resolve(
        self: Operand[
            Indexed[IndexType, IndexValue[EdgeIndex]], ContainerType, Unpack[Levels]
        ],
    ) -> Operand[Indexed[IndexType, EdgeReference], ContainerType, Unpack[Levels]]: ...

    @overload
    def resolve(
        self: Operand[Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]],
    ) -> Operand[Bare[NodeReference], ContainerType, Unpack[Levels]]: ...

    @overload
    def resolve(
        self: Operand[Bare[IndexValue[EdgeIndex]], ContainerType, Unpack[Levels]],
    ) -> Operand[Bare[EdgeReference], ContainerType, Unpack[Levels]]: ...

    def resolve(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.resolve())

    @overload
    def select(
        self: Operand[
            Indexed[IndexType, NodeReference], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Operand[Indexed[NodeIndex, Unit], Multiple[Unordered], Unpack[Levels]]: ...

    @overload
    def select(
        self: Operand[
            Indexed[IndexType, EdgeReference], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Operand[Indexed[EdgeIndex, Unit], Multiple[Unordered], Unpack[Levels]]: ...

    @overload
    def select(
        self: Operand[Bare[NodeReference], Multiple[OrderType], Unpack[Levels]],
    ) -> Operand[Indexed[NodeIndex, Unit], Multiple[Unordered], Unpack[Levels]]: ...

    @overload
    def select(
        self: Operand[Bare[EdgeReference], Multiple[OrderType], Unpack[Levels]],
    ) -> Operand[Indexed[EdgeIndex, Unit], Multiple[Unordered], Unpack[Levels]]: ...

    @overload
    def select(
        self: Operand[Indexed[IndexType, NodeReference], Single, Unpack[Levels]],
    ) -> Operand[Indexed[NodeIndex, Unit], Single, Unpack[Levels]]: ...

    @overload
    def select(
        self: Operand[Indexed[IndexType, NodeReference], Definite, Unpack[Levels]],
    ) -> Operand[Indexed[NodeIndex, Unit], Definite, Unpack[Levels]]: ...

    @overload
    def select(
        self: Operand[Indexed[IndexType, EdgeReference], Single, Unpack[Levels]],
    ) -> Operand[Indexed[EdgeIndex, Unit], Single, Unpack[Levels]]: ...

    @overload
    def select(
        self: Operand[Indexed[IndexType, EdgeReference], Definite, Unpack[Levels]],
    ) -> Operand[Indexed[EdgeIndex, Unit], Definite, Unpack[Levels]]: ...

    @overload
    def select(
        self: Operand[Bare[NodeReference], Single, Unpack[Levels]],
    ) -> Operand[Indexed[NodeIndex, Unit], Single, Unpack[Levels]]: ...

    @overload
    def select(
        self: Operand[Bare[NodeReference], Definite, Unpack[Levels]],
    ) -> Operand[Indexed[NodeIndex, Unit], Definite, Unpack[Levels]]: ...

    @overload
    def select(
        self: Operand[Bare[EdgeReference], Single, Unpack[Levels]],
    ) -> Operand[Indexed[EdgeIndex, Unit], Single, Unpack[Levels]]: ...

    @overload
    def select(
        self: Operand[Bare[EdgeReference], Definite, Unpack[Levels]],
    ) -> Operand[Indexed[EdgeIndex, Unit], Definite, Unpack[Levels]]: ...

    def select(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.select())

    @overload
    def parent_index(
        self: Operand[
            Indexed[IndexType, IndexValue[Expanded[K, ChildType, ParentPayloadType]]],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Operand[Indexed[IndexType, IndexValue[K]], ContainerType, Unpack[Levels]]: ...

    @overload
    def parent_index(
        self: Operand[
            Bare[IndexValue[Expanded[K, ChildType, ParentPayloadType]]],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Operand[Bare[IndexValue[K]], ContainerType, Unpack[Levels]]: ...

    def parent_index(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.parent_index())

    @overload
    def child_index(
        self: Operand[
            Indexed[IndexType, IndexValue[Expanded[K, ChildType, ParentPayloadType]]],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Operand[
        Indexed[IndexType, IndexValue[ChildType]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def child_index(
        self: Operand[
            Bare[IndexValue[Expanded[K, ChildType, ParentPayloadType]]],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Operand[Bare[IndexValue[ChildType]], ContainerType, Unpack[Levels]]: ...

    def child_index(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.child_index())

    @overload
    def has_attribute(
        self: Operand[Indexed[EntityType, Unit], ContainerType, Unpack[Levels]],
        attribute: Attribute,
    ) -> Operand[Indexed[EntityType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def has_attribute(
        self: Operand[Indexed[IndexType, ReferenceType], ContainerType, Unpack[Levels]],
        attribute: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    def has_attribute(
        self, attribute: Attribute
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.has_attribute(attribute))

    @overload
    def in_group(
        self: Operand[Indexed[EntityType, Unit], ContainerType, Unpack[Levels]],
        group: Group,
    ) -> Operand[Indexed[EntityType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def in_group(
        self: Operand[Indexed[IndexType, ReferenceType], ContainerType, Unpack[Levels]],
        group: Group,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    def in_group(self, group: Group) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.in_group(group))

    @overload
    def add(
        self: Operand[
            Indexed[IndexType, ArithmeticValueType], Definite, Unpack[Levels]
        ],
        value: IndexedDroppingArgument[IndexType, ArithmeticValueType],
    ) -> Operand[Indexed[IndexType, ArithmeticValueType], Single, Unpack[Levels]]: ...

    @overload
    def add(
        self: Operand[
            Indexed[IndexType, ArithmeticValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        value: IndexedDroppingArgument[IndexType, ArithmeticValueType],
    ) -> Operand[
        Indexed[IndexType, ArithmeticValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def add(
        self: Operand[Bare[ArithmeticValueType], Definite, Unpack[Levels]],
        value: BareDroppingArgument[ArithmeticValueType],
    ) -> Operand[Bare[ArithmeticValueType], Single, Unpack[Levels]]: ...

    @overload
    def add(
        self: Operand[Bare[ArithmeticValueType], DroppedContainerType, Unpack[Levels]],
        value: BareDroppingArgument[ArithmeticValueType],
    ) -> Operand[Bare[ArithmeticValueType], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def add(
        self: Operand[
            Indexed[IndexType, ArithmeticValueType], ContainerType, Unpack[Levels]
        ],
        value: IndexedOperandArgument[
            IndexType, ArithmeticValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[IndexType, ArithmeticValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def add(
        self: Operand[Bare[ArithmeticValueType], ContainerType, Unpack[Levels]],
        value: BareOperandArgument[ArithmeticValueType],
    ) -> Operand[Bare[ArithmeticValueType], ContainerType, Unpack[Levels]]: ...

    @overload
    def add(
        self: Operand[Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]],
        value: ScalarValue,
    ) -> Operand[Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]]: ...

    @overload
    def add(
        self: Operand[Bare[Scalar], ContainerType, Unpack[Levels]],
        value: ScalarValue,
    ) -> Operand[Bare[Scalar], ContainerType, Unpack[Levels]]: ...

    @overload
    def add(
        self: Operand[Indexed[IndexType, AttributeName], ContainerType, Unpack[Levels]],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, AttributeName], ContainerType, Unpack[Levels]]: ...

    @overload
    def add(
        self: Operand[Bare[AttributeName], ContainerType, Unpack[Levels]],
        value: Attribute,
    ) -> Operand[Bare[AttributeName], ContainerType, Unpack[Levels]]: ...

    @overload
    def add(
        self: Operand[
            Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
        ],
        value: Attribute,
    ) -> Operand[
        Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def add(
        self: Operand[Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]],
        value: Attribute,
    ) -> Operand[Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]]: ...

    @overload
    def add(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndex]], ContainerType, Unpack[Levels]
        ],
        value: ScalarValue,
    ) -> Operand[
        Indexed[IndexType, IndexValue[ValueIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def add(
        self: Operand[Bare[IndexValue[ValueIndex]], ContainerType, Unpack[Levels]],
        value: ScalarValue,
    ) -> Operand[Bare[IndexValue[ValueIndex]], ContainerType, Unpack[Levels]]: ...

    @overload
    def add(
        self: Operand[
            Indexed[IndexType, IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        value: Attribute,
    ) -> Operand[
        Indexed[IndexType, IndexValue[AttributeNameIndex]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def add(
        self: Operand[
            Bare[IndexValue[AttributeNameIndex]], ContainerType, Unpack[Levels]
        ],
        value: Attribute,
    ) -> Operand[
        Bare[IndexValue[AttributeNameIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def add(
        self: Operand[
            Indexed[IndexType, IndexValue[IntegerIndexType]],
            ContainerType,
            Unpack[Levels],
        ],
        value: int,
    ) -> Operand[
        Indexed[IndexType, IndexValue[IntegerIndexType]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def add(
        self: Operand[
            Bare[IndexValue[IntegerIndexType]], ContainerType, Unpack[Levels]
        ],
        value: int,
    ) -> Operand[Bare[IndexValue[IntegerIndexType]], ContainerType, Unpack[Levels]]: ...

    def add(
        self,
        value: Union[
            ScalarValue, Operand[Any, Any, Unpack[Tuple[Any, ...]]], Argument[Any, Any]
        ],
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(
            self._operand.add(Operand._to_py_argument(value))
        )

    @overload
    def subtract(
        self: Operand[
            Indexed[IndexType, ArithmeticValueType], Definite, Unpack[Levels]
        ],
        value: IndexedDroppingArgument[IndexType, ArithmeticValueType],
    ) -> Operand[Indexed[IndexType, ArithmeticValueType], Single, Unpack[Levels]]: ...

    @overload
    def subtract(
        self: Operand[
            Indexed[IndexType, ArithmeticValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        value: IndexedDroppingArgument[IndexType, ArithmeticValueType],
    ) -> Operand[
        Indexed[IndexType, ArithmeticValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def subtract(
        self: Operand[Bare[ArithmeticValueType], Definite, Unpack[Levels]],
        value: BareDroppingArgument[ArithmeticValueType],
    ) -> Operand[Bare[ArithmeticValueType], Single, Unpack[Levels]]: ...

    @overload
    def subtract(
        self: Operand[Bare[ArithmeticValueType], DroppedContainerType, Unpack[Levels]],
        value: BareDroppingArgument[ArithmeticValueType],
    ) -> Operand[Bare[ArithmeticValueType], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def subtract(
        self: Operand[
            Indexed[IndexType, ArithmeticValueType], ContainerType, Unpack[Levels]
        ],
        value: IndexedOperandArgument[
            IndexType, ArithmeticValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[IndexType, ArithmeticValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def subtract(
        self: Operand[Bare[ArithmeticValueType], ContainerType, Unpack[Levels]],
        value: BareOperandArgument[ArithmeticValueType],
    ) -> Operand[Bare[ArithmeticValueType], ContainerType, Unpack[Levels]]: ...

    @overload
    def subtract(
        self: Operand[Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]],
        value: ScalarValue,
    ) -> Operand[Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]]: ...

    @overload
    def subtract(
        self: Operand[Bare[Scalar], ContainerType, Unpack[Levels]],
        value: ScalarValue,
    ) -> Operand[Bare[Scalar], ContainerType, Unpack[Levels]]: ...

    @overload
    def subtract(
        self: Operand[Indexed[IndexType, AttributeName], ContainerType, Unpack[Levels]],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, AttributeName], ContainerType, Unpack[Levels]]: ...

    @overload
    def subtract(
        self: Operand[Bare[AttributeName], ContainerType, Unpack[Levels]],
        value: Attribute,
    ) -> Operand[Bare[AttributeName], ContainerType, Unpack[Levels]]: ...

    @overload
    def subtract(
        self: Operand[
            Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
        ],
        value: Attribute,
    ) -> Operand[
        Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def subtract(
        self: Operand[Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]],
        value: Attribute,
    ) -> Operand[Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]]: ...

    @overload
    def subtract(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndex]], ContainerType, Unpack[Levels]
        ],
        value: ScalarValue,
    ) -> Operand[
        Indexed[IndexType, IndexValue[ValueIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def subtract(
        self: Operand[Bare[IndexValue[ValueIndex]], ContainerType, Unpack[Levels]],
        value: ScalarValue,
    ) -> Operand[Bare[IndexValue[ValueIndex]], ContainerType, Unpack[Levels]]: ...

    @overload
    def subtract(
        self: Operand[
            Indexed[IndexType, IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        value: Attribute,
    ) -> Operand[
        Indexed[IndexType, IndexValue[AttributeNameIndex]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def subtract(
        self: Operand[
            Bare[IndexValue[AttributeNameIndex]], ContainerType, Unpack[Levels]
        ],
        value: Attribute,
    ) -> Operand[
        Bare[IndexValue[AttributeNameIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def subtract(
        self: Operand[
            Indexed[IndexType, IndexValue[IntegerIndexType]],
            ContainerType,
            Unpack[Levels],
        ],
        value: int,
    ) -> Operand[
        Indexed[IndexType, IndexValue[IntegerIndexType]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def subtract(
        self: Operand[
            Bare[IndexValue[IntegerIndexType]], ContainerType, Unpack[Levels]
        ],
        value: int,
    ) -> Operand[Bare[IndexValue[IntegerIndexType]], ContainerType, Unpack[Levels]]: ...

    def subtract(
        self,
        value: Union[
            ScalarValue, Operand[Any, Any, Unpack[Tuple[Any, ...]]], Argument[Any, Any]
        ],
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(
            self._operand.subtract(Operand._to_py_argument(value))
        )

    @overload
    def multiply(
        self: Operand[
            Indexed[IndexType, ArithmeticValueType], Definite, Unpack[Levels]
        ],
        value: IndexedDroppingArgument[IndexType, ArithmeticValueType],
    ) -> Operand[Indexed[IndexType, ArithmeticValueType], Single, Unpack[Levels]]: ...

    @overload
    def multiply(
        self: Operand[
            Indexed[IndexType, ArithmeticValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        value: IndexedDroppingArgument[IndexType, ArithmeticValueType],
    ) -> Operand[
        Indexed[IndexType, ArithmeticValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def multiply(
        self: Operand[Bare[ArithmeticValueType], Definite, Unpack[Levels]],
        value: BareDroppingArgument[ArithmeticValueType],
    ) -> Operand[Bare[ArithmeticValueType], Single, Unpack[Levels]]: ...

    @overload
    def multiply(
        self: Operand[Bare[ArithmeticValueType], DroppedContainerType, Unpack[Levels]],
        value: BareDroppingArgument[ArithmeticValueType],
    ) -> Operand[Bare[ArithmeticValueType], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def multiply(
        self: Operand[
            Indexed[IndexType, ArithmeticValueType], ContainerType, Unpack[Levels]
        ],
        value: IndexedOperandArgument[
            IndexType, ArithmeticValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[IndexType, ArithmeticValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def multiply(
        self: Operand[Bare[ArithmeticValueType], ContainerType, Unpack[Levels]],
        value: BareOperandArgument[ArithmeticValueType],
    ) -> Operand[Bare[ArithmeticValueType], ContainerType, Unpack[Levels]]: ...

    @overload
    def multiply(
        self: Operand[Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]],
        value: ScalarValue,
    ) -> Operand[Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]]: ...

    @overload
    def multiply(
        self: Operand[Bare[Scalar], ContainerType, Unpack[Levels]],
        value: ScalarValue,
    ) -> Operand[Bare[Scalar], ContainerType, Unpack[Levels]]: ...

    @overload
    def multiply(
        self: Operand[Indexed[IndexType, AttributeName], ContainerType, Unpack[Levels]],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, AttributeName], ContainerType, Unpack[Levels]]: ...

    @overload
    def multiply(
        self: Operand[Bare[AttributeName], ContainerType, Unpack[Levels]],
        value: Attribute,
    ) -> Operand[Bare[AttributeName], ContainerType, Unpack[Levels]]: ...

    @overload
    def multiply(
        self: Operand[
            Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
        ],
        value: Attribute,
    ) -> Operand[
        Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def multiply(
        self: Operand[Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]],
        value: Attribute,
    ) -> Operand[Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]]: ...

    @overload
    def multiply(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndex]], ContainerType, Unpack[Levels]
        ],
        value: ScalarValue,
    ) -> Operand[
        Indexed[IndexType, IndexValue[ValueIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def multiply(
        self: Operand[Bare[IndexValue[ValueIndex]], ContainerType, Unpack[Levels]],
        value: ScalarValue,
    ) -> Operand[Bare[IndexValue[ValueIndex]], ContainerType, Unpack[Levels]]: ...

    @overload
    def multiply(
        self: Operand[
            Indexed[IndexType, IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        value: Attribute,
    ) -> Operand[
        Indexed[IndexType, IndexValue[AttributeNameIndex]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def multiply(
        self: Operand[
            Bare[IndexValue[AttributeNameIndex]], ContainerType, Unpack[Levels]
        ],
        value: Attribute,
    ) -> Operand[
        Bare[IndexValue[AttributeNameIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def multiply(
        self: Operand[
            Indexed[IndexType, IndexValue[IntegerIndexType]],
            ContainerType,
            Unpack[Levels],
        ],
        value: int,
    ) -> Operand[
        Indexed[IndexType, IndexValue[IntegerIndexType]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def multiply(
        self: Operand[
            Bare[IndexValue[IntegerIndexType]], ContainerType, Unpack[Levels]
        ],
        value: int,
    ) -> Operand[Bare[IndexValue[IntegerIndexType]], ContainerType, Unpack[Levels]]: ...

    def multiply(
        self,
        value: Union[
            ScalarValue, Operand[Any, Any, Unpack[Tuple[Any, ...]]], Argument[Any, Any]
        ],
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(
            self._operand.multiply(Operand._to_py_argument(value))
        )

    @overload
    def power(
        self: Operand[
            Indexed[IndexType, ArithmeticValueType], Definite, Unpack[Levels]
        ],
        value: IndexedDroppingArgument[IndexType, ArithmeticValueType],
    ) -> Operand[Indexed[IndexType, ArithmeticValueType], Single, Unpack[Levels]]: ...

    @overload
    def power(
        self: Operand[
            Indexed[IndexType, ArithmeticValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        value: IndexedDroppingArgument[IndexType, ArithmeticValueType],
    ) -> Operand[
        Indexed[IndexType, ArithmeticValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def power(
        self: Operand[Bare[ArithmeticValueType], Definite, Unpack[Levels]],
        value: BareDroppingArgument[ArithmeticValueType],
    ) -> Operand[Bare[ArithmeticValueType], Single, Unpack[Levels]]: ...

    @overload
    def power(
        self: Operand[Bare[ArithmeticValueType], DroppedContainerType, Unpack[Levels]],
        value: BareDroppingArgument[ArithmeticValueType],
    ) -> Operand[Bare[ArithmeticValueType], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def power(
        self: Operand[
            Indexed[IndexType, ArithmeticValueType], ContainerType, Unpack[Levels]
        ],
        value: IndexedOperandArgument[
            IndexType, ArithmeticValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[IndexType, ArithmeticValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def power(
        self: Operand[Bare[ArithmeticValueType], ContainerType, Unpack[Levels]],
        value: BareOperandArgument[ArithmeticValueType],
    ) -> Operand[Bare[ArithmeticValueType], ContainerType, Unpack[Levels]]: ...

    @overload
    def power(
        self: Operand[Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]],
        value: ScalarValue,
    ) -> Operand[Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]]: ...

    @overload
    def power(
        self: Operand[Bare[Scalar], ContainerType, Unpack[Levels]],
        value: ScalarValue,
    ) -> Operand[Bare[Scalar], ContainerType, Unpack[Levels]]: ...

    @overload
    def power(
        self: Operand[Indexed[IndexType, AttributeName], ContainerType, Unpack[Levels]],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, AttributeName], ContainerType, Unpack[Levels]]: ...

    @overload
    def power(
        self: Operand[Bare[AttributeName], ContainerType, Unpack[Levels]],
        value: Attribute,
    ) -> Operand[Bare[AttributeName], ContainerType, Unpack[Levels]]: ...

    @overload
    def power(
        self: Operand[
            Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
        ],
        value: Attribute,
    ) -> Operand[
        Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def power(
        self: Operand[Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]],
        value: Attribute,
    ) -> Operand[Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]]: ...

    @overload
    def power(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndex]], ContainerType, Unpack[Levels]
        ],
        value: ScalarValue,
    ) -> Operand[
        Indexed[IndexType, IndexValue[ValueIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def power(
        self: Operand[Bare[IndexValue[ValueIndex]], ContainerType, Unpack[Levels]],
        value: ScalarValue,
    ) -> Operand[Bare[IndexValue[ValueIndex]], ContainerType, Unpack[Levels]]: ...

    @overload
    def power(
        self: Operand[
            Indexed[IndexType, IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        value: Attribute,
    ) -> Operand[
        Indexed[IndexType, IndexValue[AttributeNameIndex]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def power(
        self: Operand[
            Bare[IndexValue[AttributeNameIndex]], ContainerType, Unpack[Levels]
        ],
        value: Attribute,
    ) -> Operand[
        Bare[IndexValue[AttributeNameIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def power(
        self: Operand[
            Indexed[IndexType, IndexValue[IntegerIndexType]],
            ContainerType,
            Unpack[Levels],
        ],
        value: int,
    ) -> Operand[
        Indexed[IndexType, IndexValue[IntegerIndexType]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def power(
        self: Operand[
            Bare[IndexValue[IntegerIndexType]], ContainerType, Unpack[Levels]
        ],
        value: int,
    ) -> Operand[Bare[IndexValue[IntegerIndexType]], ContainerType, Unpack[Levels]]: ...

    def power(
        self,
        value: Union[
            ScalarValue, Operand[Any, Any, Unpack[Tuple[Any, ...]]], Argument[Any, Any]
        ],
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(
            self._operand.power(Operand._to_py_argument(value))
        )

    @overload
    def modulo(
        self: Operand[
            Indexed[IndexType, ArithmeticValueType], Definite, Unpack[Levels]
        ],
        value: IndexedDroppingArgument[IndexType, ArithmeticValueType],
    ) -> Operand[Indexed[IndexType, ArithmeticValueType], Single, Unpack[Levels]]: ...

    @overload
    def modulo(
        self: Operand[
            Indexed[IndexType, ArithmeticValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        value: IndexedDroppingArgument[IndexType, ArithmeticValueType],
    ) -> Operand[
        Indexed[IndexType, ArithmeticValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def modulo(
        self: Operand[Bare[ArithmeticValueType], Definite, Unpack[Levels]],
        value: BareDroppingArgument[ArithmeticValueType],
    ) -> Operand[Bare[ArithmeticValueType], Single, Unpack[Levels]]: ...

    @overload
    def modulo(
        self: Operand[Bare[ArithmeticValueType], DroppedContainerType, Unpack[Levels]],
        value: BareDroppingArgument[ArithmeticValueType],
    ) -> Operand[Bare[ArithmeticValueType], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def modulo(
        self: Operand[
            Indexed[IndexType, ArithmeticValueType], ContainerType, Unpack[Levels]
        ],
        value: IndexedOperandArgument[
            IndexType, ArithmeticValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[IndexType, ArithmeticValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def modulo(
        self: Operand[Bare[ArithmeticValueType], ContainerType, Unpack[Levels]],
        value: BareOperandArgument[ArithmeticValueType],
    ) -> Operand[Bare[ArithmeticValueType], ContainerType, Unpack[Levels]]: ...

    @overload
    def modulo(
        self: Operand[Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]],
        value: ScalarValue,
    ) -> Operand[Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]]: ...

    @overload
    def modulo(
        self: Operand[Bare[Scalar], ContainerType, Unpack[Levels]],
        value: ScalarValue,
    ) -> Operand[Bare[Scalar], ContainerType, Unpack[Levels]]: ...

    @overload
    def modulo(
        self: Operand[Indexed[IndexType, AttributeName], ContainerType, Unpack[Levels]],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, AttributeName], ContainerType, Unpack[Levels]]: ...

    @overload
    def modulo(
        self: Operand[Bare[AttributeName], ContainerType, Unpack[Levels]],
        value: Attribute,
    ) -> Operand[Bare[AttributeName], ContainerType, Unpack[Levels]]: ...

    @overload
    def modulo(
        self: Operand[
            Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
        ],
        value: Attribute,
    ) -> Operand[
        Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def modulo(
        self: Operand[Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]],
        value: Attribute,
    ) -> Operand[Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]]: ...

    @overload
    def modulo(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndex]], ContainerType, Unpack[Levels]
        ],
        value: ScalarValue,
    ) -> Operand[
        Indexed[IndexType, IndexValue[ValueIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def modulo(
        self: Operand[Bare[IndexValue[ValueIndex]], ContainerType, Unpack[Levels]],
        value: ScalarValue,
    ) -> Operand[Bare[IndexValue[ValueIndex]], ContainerType, Unpack[Levels]]: ...

    @overload
    def modulo(
        self: Operand[
            Indexed[IndexType, IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        value: Attribute,
    ) -> Operand[
        Indexed[IndexType, IndexValue[AttributeNameIndex]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def modulo(
        self: Operand[
            Bare[IndexValue[AttributeNameIndex]], ContainerType, Unpack[Levels]
        ],
        value: Attribute,
    ) -> Operand[
        Bare[IndexValue[AttributeNameIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def modulo(
        self: Operand[
            Indexed[IndexType, IndexValue[IntegerIndexType]],
            ContainerType,
            Unpack[Levels],
        ],
        value: int,
    ) -> Operand[
        Indexed[IndexType, IndexValue[IntegerIndexType]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def modulo(
        self: Operand[
            Bare[IndexValue[IntegerIndexType]], ContainerType, Unpack[Levels]
        ],
        value: int,
    ) -> Operand[Bare[IndexValue[IntegerIndexType]], ContainerType, Unpack[Levels]]: ...

    def modulo(
        self,
        value: Union[
            ScalarValue, Operand[Any, Any, Unpack[Tuple[Any, ...]]], Argument[Any, Any]
        ],
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(
            self._operand.modulo(Operand._to_py_argument(value))
        )

    @overload
    def divide(
        self: Operand[
            Indexed[IndexType, RealNumericValueType], Definite, Unpack[Levels]
        ],
        value: IndexedDroppingArgument[IndexType, RealNumericValueType],
    ) -> Operand[Indexed[IndexType, RealNumericValueType], Single, Unpack[Levels]]: ...

    @overload
    def divide(
        self: Operand[
            Indexed[IndexType, RealNumericValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        value: IndexedDroppingArgument[IndexType, RealNumericValueType],
    ) -> Operand[
        Indexed[IndexType, RealNumericValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def divide(
        self: Operand[Bare[RealNumericValueType], Definite, Unpack[Levels]],
        value: BareDroppingArgument[RealNumericValueType],
    ) -> Operand[Bare[RealNumericValueType], Single, Unpack[Levels]]: ...

    @overload
    def divide(
        self: Operand[Bare[RealNumericValueType], DroppedContainerType, Unpack[Levels]],
        value: BareDroppingArgument[RealNumericValueType],
    ) -> Operand[Bare[RealNumericValueType], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def divide(
        self: Operand[
            Indexed[IndexType, RealNumericValueType], ContainerType, Unpack[Levels]
        ],
        value: IndexedOperandArgument[
            IndexType, RealNumericValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[IndexType, RealNumericValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def divide(
        self: Operand[Bare[RealNumericValueType], ContainerType, Unpack[Levels]],
        value: BareOperandArgument[RealNumericValueType],
    ) -> Operand[Bare[RealNumericValueType], ContainerType, Unpack[Levels]]: ...

    @overload
    def divide(
        self: Operand[
            Indexed[IndexType, RealNumericValueType], ContainerType, Unpack[Levels]
        ],
        value: ScalarValue,
    ) -> Operand[
        Indexed[IndexType, RealNumericValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def divide(
        self: Operand[Bare[RealNumericValueType], ContainerType, Unpack[Levels]],
        value: ScalarValue,
    ) -> Operand[Bare[RealNumericValueType], ContainerType, Unpack[Levels]]: ...

    def divide(
        self,
        value: Union[
            ScalarValue, Operand[Any, Any, Unpack[Tuple[Any, ...]]], Argument[Any, Any]
        ],
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(
            self._operand.divide(Operand._to_py_argument(value))
        )

    @overload
    def clip(
        self: Operand[
            Indexed[IndexType, ScalarClipValueType], Definite, Unpack[Levels]
        ],
        lower: IndexedDroppingArgument[IndexType, ScalarClipValueType],
        upper: IndexedAnyScalarArgument[
            IndexType, ScalarClipValueType, ArgumentOrderType
        ],
    ) -> Operand[Indexed[IndexType, ScalarClipValueType], Single, Unpack[Levels]]: ...

    @overload
    def clip(
        self: Operand[
            Indexed[IndexType, ScalarClipValueType], Definite, Unpack[Levels]
        ],
        lower: IndexedScalarArgument[IndexType, ScalarClipValueType, ArgumentOrderType],
        upper: IndexedDroppingArgument[IndexType, ScalarClipValueType],
    ) -> Operand[Indexed[IndexType, ScalarClipValueType], Single, Unpack[Levels]]: ...

    @overload
    def clip(
        self: Operand[
            Indexed[IndexType, ScalarClipValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        lower: IndexedDroppingArgument[IndexType, ScalarClipValueType],
        upper: IndexedAnyScalarArgument[
            IndexType, ScalarClipValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[IndexType, ScalarClipValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def clip(
        self: Operand[
            Indexed[IndexType, ScalarClipValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        lower: IndexedScalarArgument[IndexType, ScalarClipValueType, ArgumentOrderType],
        upper: IndexedDroppingArgument[IndexType, ScalarClipValueType],
    ) -> Operand[
        Indexed[IndexType, ScalarClipValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def clip(
        self: Operand[Bare[ScalarClipValueType], Definite, Unpack[Levels]],
        lower: BareDroppingArgument[ScalarClipValueType],
        upper: BareAnyScalarArgument[ScalarClipValueType],
    ) -> Operand[Bare[ScalarClipValueType], Single, Unpack[Levels]]: ...

    @overload
    def clip(
        self: Operand[Bare[ScalarClipValueType], Definite, Unpack[Levels]],
        lower: BareScalarArgument[ScalarClipValueType],
        upper: BareDroppingArgument[ScalarClipValueType],
    ) -> Operand[Bare[ScalarClipValueType], Single, Unpack[Levels]]: ...

    @overload
    def clip(
        self: Operand[Bare[ScalarClipValueType], DroppedContainerType, Unpack[Levels]],
        lower: BareDroppingArgument[ScalarClipValueType],
        upper: BareAnyScalarArgument[ScalarClipValueType],
    ) -> Operand[Bare[ScalarClipValueType], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def clip(
        self: Operand[Bare[ScalarClipValueType], DroppedContainerType, Unpack[Levels]],
        lower: BareScalarArgument[ScalarClipValueType],
        upper: BareDroppingArgument[ScalarClipValueType],
    ) -> Operand[Bare[ScalarClipValueType], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def clip(
        self: Operand[
            Indexed[IndexType, AttributeClipValueType], Definite, Unpack[Levels]
        ],
        lower: IndexedDroppingArgument[IndexType, AttributeClipValueType],
        upper: IndexedAnyAttributeArgument[
            IndexType, AttributeClipValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[IndexType, AttributeClipValueType], Single, Unpack[Levels]
    ]: ...

    @overload
    def clip(
        self: Operand[
            Indexed[IndexType, AttributeClipValueType], Definite, Unpack[Levels]
        ],
        lower: IndexedAttributeArgument[
            IndexType, AttributeClipValueType, ArgumentOrderType
        ],
        upper: IndexedDroppingArgument[IndexType, AttributeClipValueType],
    ) -> Operand[
        Indexed[IndexType, AttributeClipValueType], Single, Unpack[Levels]
    ]: ...

    @overload
    def clip(
        self: Operand[
            Indexed[IndexType, AttributeClipValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        lower: IndexedDroppingArgument[IndexType, AttributeClipValueType],
        upper: IndexedAnyAttributeArgument[
            IndexType, AttributeClipValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[IndexType, AttributeClipValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def clip(
        self: Operand[
            Indexed[IndexType, AttributeClipValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        lower: IndexedAttributeArgument[
            IndexType, AttributeClipValueType, ArgumentOrderType
        ],
        upper: IndexedDroppingArgument[IndexType, AttributeClipValueType],
    ) -> Operand[
        Indexed[IndexType, AttributeClipValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def clip(
        self: Operand[Bare[AttributeClipValueType], Definite, Unpack[Levels]],
        lower: BareDroppingArgument[AttributeClipValueType],
        upper: BareAnyAttributeArgument[AttributeClipValueType],
    ) -> Operand[Bare[AttributeClipValueType], Single, Unpack[Levels]]: ...

    @overload
    def clip(
        self: Operand[Bare[AttributeClipValueType], Definite, Unpack[Levels]],
        lower: BareAttributeArgument[AttributeClipValueType],
        upper: BareDroppingArgument[AttributeClipValueType],
    ) -> Operand[Bare[AttributeClipValueType], Single, Unpack[Levels]]: ...

    @overload
    def clip(
        self: Operand[
            Bare[AttributeClipValueType], DroppedContainerType, Unpack[Levels]
        ],
        lower: BareDroppingArgument[AttributeClipValueType],
        upper: BareAnyAttributeArgument[AttributeClipValueType],
    ) -> Operand[
        Bare[AttributeClipValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def clip(
        self: Operand[
            Bare[AttributeClipValueType], DroppedContainerType, Unpack[Levels]
        ],
        lower: BareAttributeArgument[AttributeClipValueType],
        upper: BareDroppingArgument[AttributeClipValueType],
    ) -> Operand[
        Bare[AttributeClipValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def clip(
        self: Operand[
            Indexed[IndexType, IntegerClipValueType], Definite, Unpack[Levels]
        ],
        lower: IndexedDroppingArgument[IndexType, IntegerClipValueType],
        upper: IndexedAnyIntegerArgument[
            IndexType, IntegerClipValueType, ArgumentOrderType
        ],
    ) -> Operand[Indexed[IndexType, IntegerClipValueType], Single, Unpack[Levels]]: ...

    @overload
    def clip(
        self: Operand[
            Indexed[IndexType, IntegerClipValueType], Definite, Unpack[Levels]
        ],
        lower: IndexedIntegerArgument[
            IndexType, IntegerClipValueType, ArgumentOrderType
        ],
        upper: IndexedDroppingArgument[IndexType, IntegerClipValueType],
    ) -> Operand[Indexed[IndexType, IntegerClipValueType], Single, Unpack[Levels]]: ...

    @overload
    def clip(
        self: Operand[
            Indexed[IndexType, IntegerClipValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        lower: IndexedDroppingArgument[IndexType, IntegerClipValueType],
        upper: IndexedAnyIntegerArgument[
            IndexType, IntegerClipValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[IndexType, IntegerClipValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def clip(
        self: Operand[
            Indexed[IndexType, IntegerClipValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        lower: IndexedIntegerArgument[
            IndexType, IntegerClipValueType, ArgumentOrderType
        ],
        upper: IndexedDroppingArgument[IndexType, IntegerClipValueType],
    ) -> Operand[
        Indexed[IndexType, IntegerClipValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def clip(
        self: Operand[Bare[IntegerClipValueType], Definite, Unpack[Levels]],
        lower: BareDroppingArgument[IntegerClipValueType],
        upper: BareAnyIntegerArgument[IntegerClipValueType],
    ) -> Operand[Bare[IntegerClipValueType], Single, Unpack[Levels]]: ...

    @overload
    def clip(
        self: Operand[Bare[IntegerClipValueType], Definite, Unpack[Levels]],
        lower: BareIntegerArgument[IntegerClipValueType],
        upper: BareDroppingArgument[IntegerClipValueType],
    ) -> Operand[Bare[IntegerClipValueType], Single, Unpack[Levels]]: ...

    @overload
    def clip(
        self: Operand[Bare[IntegerClipValueType], DroppedContainerType, Unpack[Levels]],
        lower: BareDroppingArgument[IntegerClipValueType],
        upper: BareAnyIntegerArgument[IntegerClipValueType],
    ) -> Operand[Bare[IntegerClipValueType], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def clip(
        self: Operand[Bare[IntegerClipValueType], DroppedContainerType, Unpack[Levels]],
        lower: BareIntegerArgument[IntegerClipValueType],
        upper: BareDroppingArgument[IntegerClipValueType],
    ) -> Operand[Bare[IntegerClipValueType], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def clip(
        self: Operand[
            Indexed[IndexType, ScalarClipValueType], ContainerType, Unpack[Levels]
        ],
        lower: IndexedScalarArgument[IndexType, ScalarClipValueType, ArgumentOrderType],
        upper: IndexedScalarArgument[IndexType, ScalarClipValueType, ArgumentOrderType],
    ) -> Operand[
        Indexed[IndexType, ScalarClipValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def clip(
        self: Operand[Bare[ScalarClipValueType], ContainerType, Unpack[Levels]],
        lower: BareScalarArgument[ScalarClipValueType],
        upper: BareScalarArgument[ScalarClipValueType],
    ) -> Operand[Bare[ScalarClipValueType], ContainerType, Unpack[Levels]]: ...

    @overload
    def clip(
        self: Operand[
            Indexed[IndexType, AttributeClipValueType], ContainerType, Unpack[Levels]
        ],
        lower: IndexedAttributeArgument[
            IndexType, AttributeClipValueType, ArgumentOrderType
        ],
        upper: IndexedAttributeArgument[
            IndexType, AttributeClipValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[IndexType, AttributeClipValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def clip(
        self: Operand[Bare[AttributeClipValueType], ContainerType, Unpack[Levels]],
        lower: BareAttributeArgument[AttributeClipValueType],
        upper: BareAttributeArgument[AttributeClipValueType],
    ) -> Operand[Bare[AttributeClipValueType], ContainerType, Unpack[Levels]]: ...

    @overload
    def clip(
        self: Operand[
            Indexed[IndexType, IntegerClipValueType], ContainerType, Unpack[Levels]
        ],
        lower: IndexedIntegerArgument[
            IndexType, IntegerClipValueType, ArgumentOrderType
        ],
        upper: IndexedIntegerArgument[
            IndexType, IntegerClipValueType, ArgumentOrderType
        ],
    ) -> Operand[
        Indexed[IndexType, IntegerClipValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def clip(
        self: Operand[Bare[IntegerClipValueType], ContainerType, Unpack[Levels]],
        lower: BareIntegerArgument[IntegerClipValueType],
        upper: BareIntegerArgument[IntegerClipValueType],
    ) -> Operand[Bare[IntegerClipValueType], ContainerType, Unpack[Levels]]: ...

    def clip(
        self,
        lower: Union[
            ScalarValue, Operand[Any, Any, Unpack[Tuple[Any, ...]]], Argument[Any, Any]
        ],
        upper: Union[
            ScalarValue, Operand[Any, Any, Unpack[Tuple[Any, ...]]], Argument[Any, Any]
        ],
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(
            self._operand.clip(
                Operand._to_py_argument(lower), Operand._to_py_argument(upper)
            )
        )

    @overload
    def cast(
        self: Operand[
            Indexed[IndexType, CastableValueType], ContainerType, Unpack[Levels]
        ],
        target: CastTarget[CastableValueType],
    ) -> Operand[
        Indexed[IndexType, CastableValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def cast(
        self: Operand[Bare[CastableValueType], ContainerType, Unpack[Levels]],
        target: CastTarget[CastableValueType],
    ) -> Operand[Bare[CastableValueType], ContainerType, Unpack[Levels]]: ...

    def cast(
        self, target: CastTarget[Any]
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.cast(target._target))

    @overload
    def __eq__(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndexType]], Definite, Unpack[Levels]
        ],
        value: IndexedDroppingArgument[IndexType, IndexValue[ValueIndexType]],
    ) -> Operand[Indexed[IndexType, Mask], Single, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndexType]],
            DroppedContainerType,
            Unpack[Levels],
        ],
        value: IndexedDroppingArgument[IndexType, IndexValue[ValueIndexType]],
    ) -> Operand[Indexed[IndexType, Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Operand[Bare[IndexValue[ValueIndexType]], Definite, Unpack[Levels]],
        value: BareDroppingArgument[IndexValue[ValueIndexType]],
    ) -> Operand[Bare[Mask], Single, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Operand[
            Bare[IndexValue[ValueIndexType]], DroppedContainerType, Unpack[Levels]
        ],
        value: BareDroppingArgument[IndexValue[ValueIndexType]],
    ) -> Operand[Bare[Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndexType]],
            ContainerType,
            Unpack[Levels],
        ],
        value: IndexedOperandArgument[
            IndexType, IndexValue[ValueIndexType], ArgumentOrderType
        ],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Operand[Bare[IndexValue[ValueIndexType]], ContainerType, Unpack[Levels]],
        value: BareOperandArgument[IndexValue[ValueIndexType]],
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Operand[Indexed[IndexType, EquatableValueType], Definite, Unpack[Levels]],
        value: IndexedDroppingArgument[IndexType, EquatableValueType],
    ) -> Operand[Indexed[IndexType, Mask], Single, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Operand[
            Indexed[IndexType, EquatableValueType], DroppedContainerType, Unpack[Levels]
        ],
        value: IndexedDroppingArgument[IndexType, EquatableValueType],
    ) -> Operand[Indexed[IndexType, Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Operand[Bare[EquatableValueType], Definite, Unpack[Levels]],
        value: BareDroppingArgument[EquatableValueType],
    ) -> Operand[Bare[Mask], Single, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Operand[Bare[EquatableValueType], DroppedContainerType, Unpack[Levels]],
        value: BareDroppingArgument[EquatableValueType],
    ) -> Operand[Bare[Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Operand[
            Indexed[IndexType, EquatableValueType], ContainerType, Unpack[Levels]
        ],
        value: IndexedOperandArgument[IndexType, EquatableValueType, ArgumentOrderType],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Operand[Bare[EquatableValueType], ContainerType, Unpack[Levels]],
        value: BareOperandArgument[EquatableValueType],
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Operand[Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]],
        value: ScalarValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Operand[Bare[Scalar], ContainerType, Unpack[Levels]],
        value: ScalarValue,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]],
        value: _BooleanValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Operand[Bare[Mask], ContainerType, Unpack[Levels]],
        value: _BooleanValue,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Operand[Indexed[IndexType, AttributeName], ContainerType, Unpack[Levels]],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Operand[Bare[AttributeName], ContainerType, Unpack[Levels]],
        value: Attribute,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Operand[
            Indexed[IndexType, FailureKindValue], ContainerType, Unpack[Levels]
        ],
        value: FailureKind,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Operand[Bare[FailureKindValue], ContainerType, Unpack[Levels]],
        value: FailureKind,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Operand[
            Indexed[IndexType, IndexValue[FailureKindIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        value: FailureKind,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Operand[
            Bare[IndexValue[FailureKindIndex]], ContainerType, Unpack[Levels]
        ],
        value: FailureKind,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Operand[
            Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
        ],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Operand[Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]],
        value: Attribute,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndex]], ContainerType, Unpack[Levels]
        ],
        value: ScalarValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Operand[Bare[IndexValue[ValueIndex]], ContainerType, Unpack[Levels]],
        value: ScalarValue,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Operand[
            Indexed[IndexType, IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Operand[
            Bare[IndexValue[AttributeNameIndex]], ContainerType, Unpack[Levels]
        ],
        value: Attribute,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Operand[
            Indexed[IndexType, IndexValue[BoolIndex]], ContainerType, Unpack[Levels]
        ],
        value: _BooleanValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Operand[Bare[IndexValue[BoolIndex]], ContainerType, Unpack[Levels]],
        value: _BooleanValue,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Operand[
            Indexed[IndexType, IndexValue[IntegerIndexType]],
            ContainerType,
            Unpack[Levels],
        ],
        value: int,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Operand[
            Bare[IndexValue[IntegerIndexType]], ContainerType, Unpack[Levels]
        ],
        value: int,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def __eq__(
        self,
        value: Union[
            ScalarValue,
            FailureKind,
            Operand[Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(
            self._operand.equal_to(Operand._to_py_argument(value))
        )

    equal_to = __eq__

    @overload
    def __ne__(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndexType]], Definite, Unpack[Levels]
        ],
        value: IndexedDroppingArgument[IndexType, IndexValue[ValueIndexType]],
    ) -> Operand[Indexed[IndexType, Mask], Single, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndexType]],
            DroppedContainerType,
            Unpack[Levels],
        ],
        value: IndexedDroppingArgument[IndexType, IndexValue[ValueIndexType]],
    ) -> Operand[Indexed[IndexType, Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Operand[Bare[IndexValue[ValueIndexType]], Definite, Unpack[Levels]],
        value: BareDroppingArgument[IndexValue[ValueIndexType]],
    ) -> Operand[Bare[Mask], Single, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Operand[
            Bare[IndexValue[ValueIndexType]], DroppedContainerType, Unpack[Levels]
        ],
        value: BareDroppingArgument[IndexValue[ValueIndexType]],
    ) -> Operand[Bare[Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndexType]],
            ContainerType,
            Unpack[Levels],
        ],
        value: IndexedOperandArgument[
            IndexType, IndexValue[ValueIndexType], ArgumentOrderType
        ],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Operand[Bare[IndexValue[ValueIndexType]], ContainerType, Unpack[Levels]],
        value: BareOperandArgument[IndexValue[ValueIndexType]],
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Operand[Indexed[IndexType, EquatableValueType], Definite, Unpack[Levels]],
        value: IndexedDroppingArgument[IndexType, EquatableValueType],
    ) -> Operand[Indexed[IndexType, Mask], Single, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Operand[
            Indexed[IndexType, EquatableValueType], DroppedContainerType, Unpack[Levels]
        ],
        value: IndexedDroppingArgument[IndexType, EquatableValueType],
    ) -> Operand[Indexed[IndexType, Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Operand[Bare[EquatableValueType], Definite, Unpack[Levels]],
        value: BareDroppingArgument[EquatableValueType],
    ) -> Operand[Bare[Mask], Single, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Operand[Bare[EquatableValueType], DroppedContainerType, Unpack[Levels]],
        value: BareDroppingArgument[EquatableValueType],
    ) -> Operand[Bare[Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Operand[
            Indexed[IndexType, EquatableValueType], ContainerType, Unpack[Levels]
        ],
        value: IndexedOperandArgument[IndexType, EquatableValueType, ArgumentOrderType],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Operand[Bare[EquatableValueType], ContainerType, Unpack[Levels]],
        value: BareOperandArgument[EquatableValueType],
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Operand[Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]],
        value: ScalarValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Operand[Bare[Scalar], ContainerType, Unpack[Levels]],
        value: ScalarValue,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]],
        value: _BooleanValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Operand[Bare[Mask], ContainerType, Unpack[Levels]],
        value: _BooleanValue,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Operand[Indexed[IndexType, AttributeName], ContainerType, Unpack[Levels]],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Operand[Bare[AttributeName], ContainerType, Unpack[Levels]],
        value: Attribute,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Operand[
            Indexed[IndexType, FailureKindValue], ContainerType, Unpack[Levels]
        ],
        value: FailureKind,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Operand[Bare[FailureKindValue], ContainerType, Unpack[Levels]],
        value: FailureKind,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Operand[
            Indexed[IndexType, IndexValue[FailureKindIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        value: FailureKind,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Operand[
            Bare[IndexValue[FailureKindIndex]], ContainerType, Unpack[Levels]
        ],
        value: FailureKind,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Operand[
            Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
        ],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Operand[Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]],
        value: Attribute,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndex]], ContainerType, Unpack[Levels]
        ],
        value: ScalarValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Operand[Bare[IndexValue[ValueIndex]], ContainerType, Unpack[Levels]],
        value: ScalarValue,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Operand[
            Indexed[IndexType, IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Operand[
            Bare[IndexValue[AttributeNameIndex]], ContainerType, Unpack[Levels]
        ],
        value: Attribute,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Operand[
            Indexed[IndexType, IndexValue[BoolIndex]], ContainerType, Unpack[Levels]
        ],
        value: _BooleanValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Operand[Bare[IndexValue[BoolIndex]], ContainerType, Unpack[Levels]],
        value: _BooleanValue,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Operand[
            Indexed[IndexType, IndexValue[IntegerIndexType]],
            ContainerType,
            Unpack[Levels],
        ],
        value: int,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Operand[
            Bare[IndexValue[IntegerIndexType]], ContainerType, Unpack[Levels]
        ],
        value: int,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def __ne__(
        self,
        value: Union[
            ScalarValue,
            FailureKind,
            Operand[Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(
            self._operand.not_equal_to(Operand._to_py_argument(value))
        )

    not_equal_to = __ne__

    @overload
    def greater_than(
        self: Operand[Indexed[IndexType, OrderableValueType], Definite, Unpack[Levels]],
        value: IndexedDroppingArgument[IndexType, OrderableValueType],
    ) -> Operand[Indexed[IndexType, Mask], Single, Unpack[Levels]]: ...

    @overload
    def greater_than(
        self: Operand[
            Indexed[IndexType, OrderableValueType], DroppedContainerType, Unpack[Levels]
        ],
        value: IndexedDroppingArgument[IndexType, OrderableValueType],
    ) -> Operand[Indexed[IndexType, Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than(
        self: Operand[Bare[OrderableValueType], Definite, Unpack[Levels]],
        value: BareDroppingArgument[OrderableValueType],
    ) -> Operand[Bare[Mask], Single, Unpack[Levels]]: ...

    @overload
    def greater_than(
        self: Operand[Bare[OrderableValueType], DroppedContainerType, Unpack[Levels]],
        value: BareDroppingArgument[OrderableValueType],
    ) -> Operand[Bare[Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than(
        self: Operand[
            Indexed[IndexType, OrderableValueType], ContainerType, Unpack[Levels]
        ],
        value: IndexedOperandArgument[IndexType, OrderableValueType, ArgumentOrderType],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than(
        self: Operand[Bare[OrderableValueType], ContainerType, Unpack[Levels]],
        value: BareOperandArgument[OrderableValueType],
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than(
        self: Operand[Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]],
        value: ScalarValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than(
        self: Operand[Bare[Scalar], ContainerType, Unpack[Levels]],
        value: ScalarValue,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than(
        self: Operand[Indexed[IndexType, AttributeName], ContainerType, Unpack[Levels]],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than(
        self: Operand[Bare[AttributeName], ContainerType, Unpack[Levels]],
        value: Attribute,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than(
        self: Operand[
            Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
        ],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than(
        self: Operand[Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]],
        value: Attribute,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndex]], ContainerType, Unpack[Levels]
        ],
        value: ScalarValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than(
        self: Operand[Bare[IndexValue[ValueIndex]], ContainerType, Unpack[Levels]],
        value: ScalarValue,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than(
        self: Operand[
            Indexed[IndexType, IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than(
        self: Operand[
            Bare[IndexValue[AttributeNameIndex]], ContainerType, Unpack[Levels]
        ],
        value: Attribute,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than(
        self: Operand[
            Indexed[IndexType, IndexValue[BoolIndex]], ContainerType, Unpack[Levels]
        ],
        value: _BooleanValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than(
        self: Operand[Bare[IndexValue[BoolIndex]], ContainerType, Unpack[Levels]],
        value: _BooleanValue,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than(
        self: Operand[
            Indexed[IndexType, IndexValue[IntegerIndexType]],
            ContainerType,
            Unpack[Levels],
        ],
        value: int,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than(
        self: Operand[
            Bare[IndexValue[IntegerIndexType]], ContainerType, Unpack[Levels]
        ],
        value: int,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def greater_than(
        self,
        value: Union[
            ScalarValue, Operand[Any, Any, Unpack[Tuple[Any, ...]]], Argument[Any, Any]
        ],
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(
            self._operand.greater_than(Operand._to_py_argument(value))
        )

    @overload
    def greater_than_or_equal_to(
        self: Operand[Indexed[IndexType, OrderableValueType], Definite, Unpack[Levels]],
        value: IndexedDroppingArgument[IndexType, OrderableValueType],
    ) -> Operand[Indexed[IndexType, Mask], Single, Unpack[Levels]]: ...

    @overload
    def greater_than_or_equal_to(
        self: Operand[
            Indexed[IndexType, OrderableValueType], DroppedContainerType, Unpack[Levels]
        ],
        value: IndexedDroppingArgument[IndexType, OrderableValueType],
    ) -> Operand[Indexed[IndexType, Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than_or_equal_to(
        self: Operand[Bare[OrderableValueType], Definite, Unpack[Levels]],
        value: BareDroppingArgument[OrderableValueType],
    ) -> Operand[Bare[Mask], Single, Unpack[Levels]]: ...

    @overload
    def greater_than_or_equal_to(
        self: Operand[Bare[OrderableValueType], DroppedContainerType, Unpack[Levels]],
        value: BareDroppingArgument[OrderableValueType],
    ) -> Operand[Bare[Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than_or_equal_to(
        self: Operand[
            Indexed[IndexType, OrderableValueType], ContainerType, Unpack[Levels]
        ],
        value: IndexedOperandArgument[IndexType, OrderableValueType, ArgumentOrderType],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than_or_equal_to(
        self: Operand[Bare[OrderableValueType], ContainerType, Unpack[Levels]],
        value: BareOperandArgument[OrderableValueType],
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than_or_equal_to(
        self: Operand[Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]],
        value: ScalarValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than_or_equal_to(
        self: Operand[Bare[Scalar], ContainerType, Unpack[Levels]],
        value: ScalarValue,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than_or_equal_to(
        self: Operand[Indexed[IndexType, AttributeName], ContainerType, Unpack[Levels]],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than_or_equal_to(
        self: Operand[Bare[AttributeName], ContainerType, Unpack[Levels]],
        value: Attribute,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than_or_equal_to(
        self: Operand[
            Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
        ],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than_or_equal_to(
        self: Operand[Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]],
        value: Attribute,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than_or_equal_to(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndex]], ContainerType, Unpack[Levels]
        ],
        value: ScalarValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than_or_equal_to(
        self: Operand[Bare[IndexValue[ValueIndex]], ContainerType, Unpack[Levels]],
        value: ScalarValue,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than_or_equal_to(
        self: Operand[
            Indexed[IndexType, IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than_or_equal_to(
        self: Operand[
            Bare[IndexValue[AttributeNameIndex]], ContainerType, Unpack[Levels]
        ],
        value: Attribute,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than_or_equal_to(
        self: Operand[
            Indexed[IndexType, IndexValue[BoolIndex]], ContainerType, Unpack[Levels]
        ],
        value: _BooleanValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than_or_equal_to(
        self: Operand[Bare[IndexValue[BoolIndex]], ContainerType, Unpack[Levels]],
        value: _BooleanValue,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than_or_equal_to(
        self: Operand[
            Indexed[IndexType, IndexValue[IntegerIndexType]],
            ContainerType,
            Unpack[Levels],
        ],
        value: int,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than_or_equal_to(
        self: Operand[
            Bare[IndexValue[IntegerIndexType]], ContainerType, Unpack[Levels]
        ],
        value: int,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def greater_than_or_equal_to(
        self,
        value: Union[
            ScalarValue, Operand[Any, Any, Unpack[Tuple[Any, ...]]], Argument[Any, Any]
        ],
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(
            self._operand.greater_than_or_equal_to(Operand._to_py_argument(value))
        )

    @overload
    def less_than(
        self: Operand[Indexed[IndexType, OrderableValueType], Definite, Unpack[Levels]],
        value: IndexedDroppingArgument[IndexType, OrderableValueType],
    ) -> Operand[Indexed[IndexType, Mask], Single, Unpack[Levels]]: ...

    @overload
    def less_than(
        self: Operand[
            Indexed[IndexType, OrderableValueType], DroppedContainerType, Unpack[Levels]
        ],
        value: IndexedDroppingArgument[IndexType, OrderableValueType],
    ) -> Operand[Indexed[IndexType, Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def less_than(
        self: Operand[Bare[OrderableValueType], Definite, Unpack[Levels]],
        value: BareDroppingArgument[OrderableValueType],
    ) -> Operand[Bare[Mask], Single, Unpack[Levels]]: ...

    @overload
    def less_than(
        self: Operand[Bare[OrderableValueType], DroppedContainerType, Unpack[Levels]],
        value: BareDroppingArgument[OrderableValueType],
    ) -> Operand[Bare[Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def less_than(
        self: Operand[
            Indexed[IndexType, OrderableValueType], ContainerType, Unpack[Levels]
        ],
        value: IndexedOperandArgument[IndexType, OrderableValueType, ArgumentOrderType],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than(
        self: Operand[Bare[OrderableValueType], ContainerType, Unpack[Levels]],
        value: BareOperandArgument[OrderableValueType],
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than(
        self: Operand[Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]],
        value: ScalarValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than(
        self: Operand[Bare[Scalar], ContainerType, Unpack[Levels]],
        value: ScalarValue,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than(
        self: Operand[Indexed[IndexType, AttributeName], ContainerType, Unpack[Levels]],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than(
        self: Operand[Bare[AttributeName], ContainerType, Unpack[Levels]],
        value: Attribute,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than(
        self: Operand[
            Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
        ],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than(
        self: Operand[Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]],
        value: Attribute,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndex]], ContainerType, Unpack[Levels]
        ],
        value: ScalarValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than(
        self: Operand[Bare[IndexValue[ValueIndex]], ContainerType, Unpack[Levels]],
        value: ScalarValue,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than(
        self: Operand[
            Indexed[IndexType, IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than(
        self: Operand[
            Bare[IndexValue[AttributeNameIndex]], ContainerType, Unpack[Levels]
        ],
        value: Attribute,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than(
        self: Operand[
            Indexed[IndexType, IndexValue[BoolIndex]], ContainerType, Unpack[Levels]
        ],
        value: _BooleanValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than(
        self: Operand[Bare[IndexValue[BoolIndex]], ContainerType, Unpack[Levels]],
        value: _BooleanValue,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than(
        self: Operand[
            Indexed[IndexType, IndexValue[IntegerIndexType]],
            ContainerType,
            Unpack[Levels],
        ],
        value: int,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than(
        self: Operand[
            Bare[IndexValue[IntegerIndexType]], ContainerType, Unpack[Levels]
        ],
        value: int,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def less_than(
        self,
        value: Union[
            ScalarValue, Operand[Any, Any, Unpack[Tuple[Any, ...]]], Argument[Any, Any]
        ],
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(
            self._operand.less_than(Operand._to_py_argument(value))
        )

    @overload
    def less_than_or_equal_to(
        self: Operand[Indexed[IndexType, OrderableValueType], Definite, Unpack[Levels]],
        value: IndexedDroppingArgument[IndexType, OrderableValueType],
    ) -> Operand[Indexed[IndexType, Mask], Single, Unpack[Levels]]: ...

    @overload
    def less_than_or_equal_to(
        self: Operand[
            Indexed[IndexType, OrderableValueType], DroppedContainerType, Unpack[Levels]
        ],
        value: IndexedDroppingArgument[IndexType, OrderableValueType],
    ) -> Operand[Indexed[IndexType, Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def less_than_or_equal_to(
        self: Operand[Bare[OrderableValueType], Definite, Unpack[Levels]],
        value: BareDroppingArgument[OrderableValueType],
    ) -> Operand[Bare[Mask], Single, Unpack[Levels]]: ...

    @overload
    def less_than_or_equal_to(
        self: Operand[Bare[OrderableValueType], DroppedContainerType, Unpack[Levels]],
        value: BareDroppingArgument[OrderableValueType],
    ) -> Operand[Bare[Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def less_than_or_equal_to(
        self: Operand[
            Indexed[IndexType, OrderableValueType], ContainerType, Unpack[Levels]
        ],
        value: IndexedOperandArgument[IndexType, OrderableValueType, ArgumentOrderType],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than_or_equal_to(
        self: Operand[Bare[OrderableValueType], ContainerType, Unpack[Levels]],
        value: BareOperandArgument[OrderableValueType],
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than_or_equal_to(
        self: Operand[Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]],
        value: ScalarValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than_or_equal_to(
        self: Operand[Bare[Scalar], ContainerType, Unpack[Levels]],
        value: ScalarValue,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than_or_equal_to(
        self: Operand[Indexed[IndexType, AttributeName], ContainerType, Unpack[Levels]],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than_or_equal_to(
        self: Operand[Bare[AttributeName], ContainerType, Unpack[Levels]],
        value: Attribute,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than_or_equal_to(
        self: Operand[
            Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
        ],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than_or_equal_to(
        self: Operand[Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]],
        value: Attribute,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than_or_equal_to(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndex]], ContainerType, Unpack[Levels]
        ],
        value: ScalarValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than_or_equal_to(
        self: Operand[Bare[IndexValue[ValueIndex]], ContainerType, Unpack[Levels]],
        value: ScalarValue,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than_or_equal_to(
        self: Operand[
            Indexed[IndexType, IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        value: Attribute,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than_or_equal_to(
        self: Operand[
            Bare[IndexValue[AttributeNameIndex]], ContainerType, Unpack[Levels]
        ],
        value: Attribute,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than_or_equal_to(
        self: Operand[
            Indexed[IndexType, IndexValue[BoolIndex]], ContainerType, Unpack[Levels]
        ],
        value: _BooleanValue,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than_or_equal_to(
        self: Operand[Bare[IndexValue[BoolIndex]], ContainerType, Unpack[Levels]],
        value: _BooleanValue,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than_or_equal_to(
        self: Operand[
            Indexed[IndexType, IndexValue[IntegerIndexType]],
            ContainerType,
            Unpack[Levels],
        ],
        value: int,
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than_or_equal_to(
        self: Operand[
            Bare[IndexValue[IntegerIndexType]], ContainerType, Unpack[Levels]
        ],
        value: int,
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def less_than_or_equal_to(
        self,
        value: Union[
            ScalarValue, Operand[Any, Any, Unpack[Tuple[Any, ...]]], Argument[Any, Any]
        ],
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(
            self._operand.less_than_or_equal_to(Operand._to_py_argument(value))
        )

    @overload
    def is_in(
        self: Operand[
            Indexed[IndexType, MembershipValueType], ContainerType, Unpack[Levels]
        ],
        values: MembershipArgument[MembershipValueType],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def is_in(
        self: Operand[Bare[MembershipValueType], ContainerType, Unpack[Levels]],
        values: MembershipArgument[MembershipValueType],
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def is_in(
        self: Operand[
            Indexed[IndexType, ScalarMembershipValueType], ContainerType, Unpack[Levels]
        ],
        values: Sequence[ScalarValue],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def is_in(
        self: Operand[Bare[ScalarMembershipValueType], ContainerType, Unpack[Levels]],
        values: Sequence[ScalarValue],
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def is_in(
        self: Operand[
            Indexed[IndexType, BooleanMembershipValueType],
            ContainerType,
            Unpack[Levels],
        ],
        values: Sequence[_BooleanValue],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def is_in(
        self: Operand[Bare[BooleanMembershipValueType], ContainerType, Unpack[Levels]],
        values: Sequence[_BooleanValue],
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def is_in(
        self: Operand[
            Indexed[IndexType, AttributeMembershipValueType],
            ContainerType,
            Unpack[Levels],
        ],
        values: Sequence[Attribute],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def is_in(
        self: Operand[
            Bare[AttributeMembershipValueType], ContainerType, Unpack[Levels]
        ],
        values: Sequence[Attribute],
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def is_in(
        self: Operand[
            Indexed[IndexType, IndexValue[IntegerIndexType]],
            ContainerType,
            Unpack[Levels],
        ],
        values: Sequence[int],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def is_in(
        self: Operand[
            Bare[IndexValue[IntegerIndexType]], ContainerType, Unpack[Levels]
        ],
        values: Sequence[int],
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def is_in(
        self: Operand[
            Indexed[IndexType, FailureKindMembershipValueType],
            ContainerType,
            Unpack[Levels],
        ],
        values: Sequence[FailureKind],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def is_in(
        self: Operand[
            Bare[FailureKindMembershipValueType], ContainerType, Unpack[Levels]
        ],
        values: Sequence[FailureKind],
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def is_in(
        self,
        values: Union[
            Sequence[ScalarValue],
            Sequence[FailureKind],
            Operand[Any, Any, Unpack[Tuple[Any, ...]]],
        ],
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        if isinstance(values, Operand):
            return Operand._from_py_operand(self._operand.is_in(values._operand))

        return Operand._from_py_operand(self._operand.is_in(values))

    @overload
    def index(
        self: Operand[Indexed[IndexType, Unit], ContainerType, Unpack[Levels]],
    ) -> Operand[
        Indexed[IndexType, IndexValue[IndexType]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def index(
        self: Operand[Indexed[IndexType, NodeReference], ContainerType, Unpack[Levels]],
    ) -> Operand[
        Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def index(
        self: Operand[Indexed[IndexType, EdgeReference], ContainerType, Unpack[Levels]],
    ) -> Operand[
        Indexed[IndexType, IndexValue[EdgeIndex]], ContainerType, Unpack[Levels]
    ]: ...

    def index(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.index())

    def discard_index(
        self: Operand[Indexed[IndexType, BareValueType], ContainerType, Unpack[Levels]],
    ) -> Operand[Bare[BareValueType], ContainerType, Unpack[Levels]]:
        return Operand._from_py_operand(self._operand.discard_index())

    def discard_value(
        self: Operand[Indexed[IndexType, V], ContainerType, Unpack[Levels]],
    ) -> Operand[Indexed[IndexType, Unit], ContainerType, Unpack[Levels]]:
        return Operand._from_py_operand(self._operand.discard_value())

    @overload
    def enumerate(
        self: Operand[Indexed[IndexType, V], Multiple[Ordered], Unpack[Levels]],
    ) -> Operand[Indexed[Positional, V], Multiple[Ordered], Unpack[Levels]]: ...

    @overload
    def enumerate(
        self: Operand[Bare[BareValueType], Multiple[Ordered], Unpack[Levels]],
    ) -> Operand[
        Indexed[Positional, BareValueType], Multiple[Ordered], Unpack[Levels]
    ]: ...

    def enumerate(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.enumerate())

    @overload
    def errors(
        self: Operand[Indexed[IndexType, V], Multiple[OrderType], Unpack[Levels]],
    ) -> Operand[
        Indexed[IndexType, FailureValue], Multiple[OrderType], Unpack[Levels]
    ]: ...

    @overload
    def errors(
        self: Operand[Indexed[IndexType, V], Single, Unpack[Levels]],
    ) -> Operand[Indexed[IndexType, FailureValue], Single, Unpack[Levels]]: ...

    @overload
    def errors(
        self: Operand[Indexed[IndexType, V], Definite, Unpack[Levels]],
    ) -> Operand[Indexed[IndexType, FailureValue], Single, Unpack[Levels]]: ...

    @overload
    def errors(
        self: Operand[Bare[BareValueType], Multiple[OrderType], Unpack[Levels]],
    ) -> Operand[Bare[FailureValue], Multiple[OrderType], Unpack[Levels]]: ...

    @overload
    def errors(
        self: Operand[Bare[BareValueType], Single, Unpack[Levels]],
    ) -> Operand[Bare[FailureValue], Single, Unpack[Levels]]: ...

    @overload
    def errors(
        self: Operand[Bare[BareValueType], Definite, Unpack[Levels]],
    ) -> Operand[Bare[FailureValue], Single, Unpack[Levels]]: ...

    def errors(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.errors())

    @overload
    def on_error(
        self: Operand[Indexed[IndexType, V], Definite, Unpack[Levels]], policy: Drop
    ) -> Operand[Indexed[IndexType, V], Single, Unpack[Levels]]: ...

    @overload
    def on_error(
        self: Operand[Indexed[IndexType, V], DroppedContainerType, Unpack[Levels]],
        policy: Drop,
    ) -> Operand[Indexed[IndexType, V], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def on_error(
        self: Operand[Bare[BareValueType], Definite, Unpack[Levels]], policy: Drop
    ) -> Operand[Bare[BareValueType], Single, Unpack[Levels]]: ...

    @overload
    def on_error(
        self: Operand[Bare[BareValueType], DroppedContainerType, Unpack[Levels]],
        policy: Drop,
    ) -> Operand[Bare[BareValueType], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def on_error(
        self: Operand[Indexed[IndexType, V], ContainerType, Unpack[Levels]],
        policy: Union[Raise, _RaiseWhen],
    ) -> Operand[Indexed[IndexType, V], ContainerType, Unpack[Levels]]: ...

    @overload
    def on_error(
        self: Operand[Bare[BareValueType], ContainerType, Unpack[Levels]],
        policy: Union[Raise, _RaiseWhen],
    ) -> Operand[Bare[BareValueType], ContainerType, Unpack[Levels]]: ...

    @overload
    def on_error(
        self: Operand[
            Indexed[IndexType, ReplaceableValueType], Definite, Unpack[Levels]
        ],
        policy: Replace[IndexedDroppingArgument[IndexType, ReplaceableValueType]],
    ) -> Operand[Indexed[IndexType, ReplaceableValueType], Single, Unpack[Levels]]: ...

    @overload
    def on_error(
        self: Operand[
            Indexed[IndexType, ReplaceableValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        policy: Replace[IndexedDroppingArgument[IndexType, ReplaceableValueType]],
    ) -> Operand[
        Indexed[IndexType, ReplaceableValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def on_error(
        self: Operand[Bare[BareValueType], Definite, Unpack[Levels]],
        policy: Replace[BareDroppingArgument[BareValueType]],
    ) -> Operand[Bare[BareValueType], Single, Unpack[Levels]]: ...

    @overload
    def on_error(
        self: Operand[Bare[BareValueType], DroppedContainerType, Unpack[Levels]],
        policy: Replace[BareDroppingArgument[BareValueType]],
    ) -> Operand[Bare[BareValueType], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def on_error(
        self: Operand[
            Indexed[IndexType, ReplaceableValueType], ContainerType, Unpack[Levels]
        ],
        policy: Replace[
            IndexedOperandArgument[IndexType, ReplaceableValueType, ArgumentOrderType]
        ],
    ) -> Operand[
        Indexed[IndexType, ReplaceableValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def on_error(
        self: Operand[Bare[BareValueType], ContainerType, Unpack[Levels]],
        policy: Replace[BareOperandArgument[BareValueType]],
    ) -> Operand[Bare[BareValueType], ContainerType, Unpack[Levels]]: ...

    @overload
    def on_error(
        self: Operand[Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]],
        policy: Replace[ScalarValue],
    ) -> Operand[Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]]: ...

    @overload
    def on_error(
        self: Operand[Bare[Scalar], ContainerType, Unpack[Levels]],
        policy: Replace[ScalarValue],
    ) -> Operand[Bare[Scalar], ContainerType, Unpack[Levels]]: ...

    @overload
    def on_error(
        self: Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]],
        policy: Replace[_BooleanValue],
    ) -> Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def on_error(
        self: Operand[Bare[Mask], ContainerType, Unpack[Levels]],
        policy: Replace[_BooleanValue],
    ) -> Operand[Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def on_error(
        self: Operand[Indexed[IndexType, AttributeName], ContainerType, Unpack[Levels]],
        policy: Replace[Attribute],
    ) -> Operand[Indexed[IndexType, AttributeName], ContainerType, Unpack[Levels]]: ...

    @overload
    def on_error(
        self: Operand[Bare[AttributeName], ContainerType, Unpack[Levels]],
        policy: Replace[Attribute],
    ) -> Operand[Bare[AttributeName], ContainerType, Unpack[Levels]]: ...

    @overload
    def on_error(
        self: Operand[
            Indexed[IndexType, FailureKindValue], ContainerType, Unpack[Levels]
        ],
        policy: Replace[FailureKind],
    ) -> Operand[
        Indexed[IndexType, FailureKindValue], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def on_error(
        self: Operand[Bare[FailureKindValue], ContainerType, Unpack[Levels]],
        policy: Replace[FailureKind],
    ) -> Operand[Bare[FailureKindValue], ContainerType, Unpack[Levels]]: ...

    @overload
    def on_error(
        self: Operand[
            Indexed[IndexType, IndexValue[FailureKindIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        policy: Replace[FailureKind],
    ) -> Operand[
        Indexed[IndexType, IndexValue[FailureKindIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def on_error(
        self: Operand[
            Bare[IndexValue[FailureKindIndex]], ContainerType, Unpack[Levels]
        ],
        policy: Replace[FailureKind],
    ) -> Operand[Bare[IndexValue[FailureKindIndex]], ContainerType, Unpack[Levels]]: ...

    @overload
    def on_error(
        self: Operand[
            Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
        ],
        policy: Replace[Attribute],
    ) -> Operand[
        Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def on_error(
        self: Operand[Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]],
        policy: Replace[Attribute],
    ) -> Operand[Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]]: ...

    @overload
    def on_error(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndex]], ContainerType, Unpack[Levels]
        ],
        policy: Replace[ScalarValue],
    ) -> Operand[
        Indexed[IndexType, IndexValue[ValueIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def on_error(
        self: Operand[Bare[IndexValue[ValueIndex]], ContainerType, Unpack[Levels]],
        policy: Replace[ScalarValue],
    ) -> Operand[Bare[IndexValue[ValueIndex]], ContainerType, Unpack[Levels]]: ...

    @overload
    def on_error(
        self: Operand[
            Indexed[IndexType, IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        policy: Replace[Attribute],
    ) -> Operand[
        Indexed[IndexType, IndexValue[AttributeNameIndex]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def on_error(
        self: Operand[
            Bare[IndexValue[AttributeNameIndex]], ContainerType, Unpack[Levels]
        ],
        policy: Replace[Attribute],
    ) -> Operand[
        Bare[IndexValue[AttributeNameIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def on_error(
        self: Operand[
            Indexed[IndexType, IndexValue[BoolIndex]], ContainerType, Unpack[Levels]
        ],
        policy: Replace[_BooleanValue],
    ) -> Operand[
        Indexed[IndexType, IndexValue[BoolIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def on_error(
        self: Operand[Bare[IndexValue[BoolIndex]], ContainerType, Unpack[Levels]],
        policy: Replace[_BooleanValue],
    ) -> Operand[Bare[IndexValue[BoolIndex]], ContainerType, Unpack[Levels]]: ...

    @overload
    def on_error(
        self: Operand[
            Indexed[IndexType, IndexValue[IntegerIndexType]],
            ContainerType,
            Unpack[Levels],
        ],
        policy: Replace[int],
    ) -> Operand[
        Indexed[IndexType, IndexValue[IntegerIndexType]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def on_error(
        self: Operand[
            Bare[IndexValue[IntegerIndexType]], ContainerType, Unpack[Levels]
        ],
        policy: Replace[int],
    ) -> Operand[Bare[IndexValue[IntegerIndexType]], ContainerType, Unpack[Levels]]: ...

    def on_error(
        self,
        policy: Union[
            Drop,
            Raise,
            _RaiseWhen,
            Replace[
                Union[
                    ScalarValue,
                    FailureKind,
                    Operand[Any, Any, Unpack[Tuple[Any, ...]]],
                    Argument[Any, Any],
                ]
            ],
        ],
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
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
        self: Operand[Indexed[IndexType, FailureValue], ContainerType, Unpack[Levels]],
    ) -> Operand[
        Indexed[IndexType, FailureKindValue], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def kind(
        self: Operand[Bare[FailureValue], ContainerType, Unpack[Levels]],
    ) -> Operand[Bare[FailureKindValue], ContainerType, Unpack[Levels]]: ...

    def kind(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.kind())

    @overload
    def name(
        self: Operand[
            Indexed[IndexType, FailureKindValue], ContainerType, Unpack[Levels]
        ],
    ) -> Operand[Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]]: ...

    @overload
    def name(
        self: Operand[Bare[FailureKindValue], ContainerType, Unpack[Levels]],
    ) -> Operand[Bare[Scalar], ContainerType, Unpack[Levels]]: ...

    def name(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.name())

    @overload
    def count(
        self: Operand[Indexed[IndexType, V], ContainerType, Unpack[Levels]],
    ) -> Operand[Bare[Scalar], Definite, Unpack[Levels]]: ...

    @overload
    def count(
        self: Operand[Bare[BareValueType], ContainerType, Unpack[Levels]],
    ) -> Operand[Bare[Scalar], Definite, Unpack[Levels]]: ...

    def count(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.count())

    @overload
    def sum(
        self: Operand[Indexed[IndexType, Scalar], Multiple[OrderType], Unpack[Levels]],
    ) -> Operand[Bare[Scalar], Single, Unpack[Levels]]: ...

    @overload
    def sum(
        self: Operand[Bare[Scalar], Multiple[OrderType], Unpack[Levels]],
    ) -> Operand[Bare[Scalar], Single, Unpack[Levels]]: ...

    @overload
    def sum(
        self: Operand[
            Indexed[IndexType, AttributeName], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Operand[Bare[AttributeName], Single, Unpack[Levels]]: ...

    @overload
    def sum(
        self: Operand[Bare[AttributeName], Multiple[OrderType], Unpack[Levels]],
    ) -> Operand[Bare[AttributeName], Single, Unpack[Levels]]: ...

    @overload
    def sum(
        self: Operand[
            Indexed[IndexType, IndexValue[NodeIndex]],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Operand[Bare[IndexValue[NodeIndex]], Single, Unpack[Levels]]: ...

    @overload
    def sum(
        self: Operand[Bare[IndexValue[NodeIndex]], Multiple[OrderType], Unpack[Levels]],
    ) -> Operand[Bare[IndexValue[NodeIndex]], Single, Unpack[Levels]]: ...

    @overload
    def sum(
        self: Operand[
            Indexed[IndexType, IndexValue[AttributeNameIndex]],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Operand[Bare[IndexValue[AttributeNameIndex]], Single, Unpack[Levels]]: ...

    @overload
    def sum(
        self: Operand[
            Bare[IndexValue[AttributeNameIndex]], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Operand[Bare[IndexValue[AttributeNameIndex]], Single, Unpack[Levels]]: ...

    @overload
    def sum(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndex]],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Operand[Bare[IndexValue[ValueIndex]], Single, Unpack[Levels]]: ...

    @overload
    def sum(
        self: Operand[
            Bare[IndexValue[ValueIndex]], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Operand[Bare[IndexValue[ValueIndex]], Single, Unpack[Levels]]: ...

    @overload
    def sum(
        self: Operand[
            Indexed[IndexType, IndexValue[IntegerIndexType]],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Operand[Bare[IndexValue[IntegerIndexType]], Single, Unpack[Levels]]: ...

    @overload
    def sum(
        self: Operand[
            Bare[IndexValue[IntegerIndexType]], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Operand[Bare[IndexValue[IntegerIndexType]], Single, Unpack[Levels]]: ...

    def sum(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.sum())

    @overload
    def mean(
        self: Operand[
            Indexed[IndexType, RealNumericValueType],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Operand[Bare[RealNumericValueType], Single, Unpack[Levels]]: ...

    @overload
    def mean(
        self: Operand[Bare[RealNumericValueType], Multiple[OrderType], Unpack[Levels]],
    ) -> Operand[Bare[RealNumericValueType], Single, Unpack[Levels]]: ...

    def mean(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.mean())

    @overload
    def std(
        self: Operand[
            Indexed[IndexType, RealNumericValueType],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Operand[Bare[Scalar], Single, Unpack[Levels]]: ...

    @overload
    def std(
        self: Operand[Bare[RealNumericValueType], Multiple[OrderType], Unpack[Levels]],
    ) -> Operand[Bare[Scalar], Single, Unpack[Levels]]: ...

    def std(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.std())

    @overload
    def var(
        self: Operand[
            Indexed[IndexType, RealNumericValueType],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Operand[Bare[Scalar], Single, Unpack[Levels]]: ...

    @overload
    def var(
        self: Operand[Bare[RealNumericValueType], Multiple[OrderType], Unpack[Levels]],
    ) -> Operand[Bare[Scalar], Single, Unpack[Levels]]: ...

    def var(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.var())

    @overload
    def all(
        self: Operand[Indexed[IndexType, Mask], Multiple[OrderType], Unpack[Levels]],
    ) -> Operand[Bare[Mask], Definite, Unpack[Levels]]: ...

    @overload
    def all(
        self: Operand[Bare[Mask], Multiple[OrderType], Unpack[Levels]],
    ) -> Operand[Bare[Mask], Definite, Unpack[Levels]]: ...

    def all(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.all())

    @overload
    def any(
        self: Operand[Indexed[IndexType, Mask], Multiple[OrderType], Unpack[Levels]],
    ) -> Operand[Bare[Mask], Definite, Unpack[Levels]]: ...

    @overload
    def any(
        self: Operand[Bare[Mask], Multiple[OrderType], Unpack[Levels]],
    ) -> Operand[Bare[Mask], Definite, Unpack[Levels]]: ...

    def any(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.any())

    @overload
    def max(
        self: Operand[
            Indexed[IndexType, OrderableValueType], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Operand[Bare[OrderableValueType], Single, Unpack[Levels]]: ...

    @overload
    def max(
        self: Operand[Bare[OrderableValueType], Multiple[OrderType], Unpack[Levels]],
    ) -> Operand[Bare[OrderableValueType], Single, Unpack[Levels]]: ...

    def max(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.max())

    @overload
    def min(
        self: Operand[
            Indexed[IndexType, OrderableValueType], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Operand[Bare[OrderableValueType], Single, Unpack[Levels]]: ...

    @overload
    def min(
        self: Operand[Bare[OrderableValueType], Multiple[OrderType], Unpack[Levels]],
    ) -> Operand[Bare[OrderableValueType], Single, Unpack[Levels]]: ...

    def min(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.min())

    @overload
    def median(
        self: Operand[
            Indexed[IndexType, MedianValueType], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Operand[Bare[MedianValueType], Single, Unpack[Levels]]: ...

    @overload
    def median(
        self: Operand[Bare[MedianValueType], Multiple[OrderType], Unpack[Levels]],
    ) -> Operand[Bare[MedianValueType], Single, Unpack[Levels]]: ...

    def median(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.median())

    @overload
    def mode(
        self: Operand[
            Indexed[IndexType, ModeValueType], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Operand[Bare[ModeValueType], Multiple[OrderType], Unpack[Levels]]: ...

    @overload
    def mode(
        self: Operand[Bare[ModeValueType], Multiple[OrderType], Unpack[Levels]],
    ) -> Operand[Bare[ModeValueType], Multiple[OrderType], Unpack[Levels]]: ...

    @overload
    def mode(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndexType]],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Operand[
        Bare[IndexValue[ValueIndexType]], Multiple[OrderType], Unpack[Levels]
    ]: ...

    @overload
    def mode(
        self: Operand[
            Bare[IndexValue[ValueIndexType]], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Operand[
        Bare[IndexValue[ValueIndexType]], Multiple[OrderType], Unpack[Levels]
    ]: ...

    def mode(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.mode())

    @overload
    def product(
        self: Operand[
            Indexed[IndexType, MultipliableValueType],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Operand[Bare[MultipliableValueType], Single, Unpack[Levels]]: ...

    @overload
    def product(
        self: Operand[Bare[MultipliableValueType], Multiple[OrderType], Unpack[Levels]],
    ) -> Operand[Bare[MultipliableValueType], Single, Unpack[Levels]]: ...

    def product(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.product())

    @overload
    def n_unique(
        self: Operand[
            Indexed[IndexType, EquivalentValueType], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Operand[Bare[Scalar], Definite, Unpack[Levels]]: ...

    @overload
    def n_unique(
        self: Operand[Bare[EquivalentValueType], Multiple[OrderType], Unpack[Levels]],
    ) -> Operand[Bare[Scalar], Definite, Unpack[Levels]]: ...

    @overload
    def n_unique(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndexType]],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Operand[Bare[Scalar], Definite, Unpack[Levels]]: ...

    @overload
    def n_unique(
        self: Operand[
            Bare[IndexValue[ValueIndexType]], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Operand[Bare[Scalar], Definite, Unpack[Levels]]: ...

    def n_unique(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.n_unique())

    @overload
    def random(
        self: Operand[Indexed[IndexType, V], Multiple[OrderType], Unpack[Levels]],
    ) -> Operand[Indexed[IndexType, V], Single, Unpack[Levels]]: ...

    @overload
    def random(
        self: Operand[Bare[BareValueType], Multiple[OrderType], Unpack[Levels]],
    ) -> Operand[Bare[BareValueType], Single, Unpack[Levels]]: ...

    def random(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.random())

    def edges(
        self: Union[
            Operand[Indexed[NodeIndex, Unit], ContainerType, Unpack[Levels]],
            Operand[Indexed[IndexType, NodeReference], ContainerType, Unpack[Levels]],
        ],
        direction: EdgeDirection,
    ) -> Operand[Indexed[EdgeIndex, Unit], Multiple[Unordered], Unpack[Levels]]:
        return Operand._from_py_operand(self._operand.edges(direction))

    def neighbors(
        self: Union[
            Operand[Indexed[NodeIndex, Unit], ContainerType, Unpack[Levels]],
            Operand[Indexed[IndexType, NodeReference], ContainerType, Unpack[Levels]],
        ],
        direction: EdgeDirection,
    ) -> Operand[Indexed[NodeIndex, Unit], Multiple[Unordered], Unpack[Levels]]:
        return Operand._from_py_operand(self._operand.neighbors(direction))

    @overload
    def via_edges(
        self: Operand[Indexed[NodeIndex, Unit], ContainerType, Unpack[Levels]],
        direction: EdgeDirection,
    ) -> Operand[
        Indexed[
            Expanded[
                NodeIndex,
                EdgeIndex,
                Tuple[NodeIndexPayload, Optional[EdgeIndexPayload]],
            ],
            EdgeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_edges(
        self: Operand[Indexed[NodeIndex, NodeReference], ContainerType, Unpack[Levels]],
        direction: EdgeDirection,
    ) -> Operand[
        Indexed[
            Expanded[
                NodeIndex,
                EdgeIndex,
                Tuple[NodeIndexPayload, Optional[EdgeIndexPayload]],
            ],
            EdgeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_edges(
        self: Operand[Indexed[EdgeIndex, NodeReference], ContainerType, Unpack[Levels]],
        direction: EdgeDirection,
    ) -> Operand[
        Indexed[
            Expanded[
                EdgeIndex,
                EdgeIndex,
                Tuple[EdgeIndexPayload, Optional[EdgeIndexPayload]],
            ],
            EdgeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_edges(
        self: Operand[
            Indexed[Positional, NodeReference], ContainerType, Unpack[Levels]
        ],
        direction: EdgeDirection,
    ) -> Operand[
        Indexed[
            Expanded[Positional, EdgeIndex, Tuple[int, Optional[EdgeIndexPayload]]],
            EdgeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_edges(
        self: Operand[
            Indexed[EndpointRole, NodeReference], ContainerType, Unpack[Levels]
        ],
        direction: EdgeDirection,
    ) -> Operand[
        Indexed[
            Expanded[
                EndpointRole,
                EdgeIndex,
                Tuple[EdgeEndpointRole, Optional[EdgeIndexPayload]],
            ],
            EdgeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_edges(
        self: Operand[
            Indexed[ValueIndex, NodeReference], ContainerType, Unpack[Levels]
        ],
        direction: EdgeDirection,
    ) -> Operand[
        Indexed[
            Expanded[
                ValueIndex, EdgeIndex, Tuple[ScalarValue, Optional[EdgeIndexPayload]]
            ],
            EdgeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_edges(
        self: Operand[
            Indexed[AttributeNameIndex, NodeReference], ContainerType, Unpack[Levels]
        ],
        direction: EdgeDirection,
    ) -> Operand[
        Indexed[
            Expanded[
                AttributeNameIndex,
                EdgeIndex,
                Tuple[Attribute, Optional[EdgeIndexPayload]],
            ],
            EdgeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_edges(
        self: Operand[Indexed[BoolIndex, NodeReference], ContainerType, Unpack[Levels]],
        direction: EdgeDirection,
    ) -> Operand[
        Indexed[
            Expanded[BoolIndex, EdgeIndex, Tuple[bool, Optional[EdgeIndexPayload]]],
            EdgeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_edges(
        self: Operand[
            Indexed[FailureKindIndex, NodeReference], ContainerType, Unpack[Levels]
        ],
        direction: EdgeDirection,
    ) -> Operand[
        Indexed[
            Expanded[
                FailureKindIndex,
                EdgeIndex,
                Tuple[FailureKind, Optional[EdgeIndexPayload]],
            ],
            EdgeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_edges(
        self: Operand[
            Indexed[Expanded[K, ChildType, ParentPayloadType], NodeReference],
            ContainerType,
            Unpack[Levels],
        ],
        direction: EdgeDirection,
    ) -> Operand[
        Indexed[
            Expanded[
                Expanded[K, ChildType, ParentPayloadType],
                EdgeIndex,
                Tuple[ParentPayloadType, Optional[EdgeIndexPayload]],
            ],
            EdgeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    def via_edges(
        self, direction: EdgeDirection
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.via_edges(direction))

    @overload
    def via_neighbors(
        self: Operand[Indexed[NodeIndex, Unit], ContainerType, Unpack[Levels]],
        direction: EdgeDirection,
    ) -> Operand[
        Indexed[
            Expanded[
                NodeIndex,
                NodeIndex,
                Tuple[NodeIndexPayload, Optional[NodeIndexPayload]],
            ],
            NodeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_neighbors(
        self: Operand[Indexed[NodeIndex, NodeReference], ContainerType, Unpack[Levels]],
        direction: EdgeDirection,
    ) -> Operand[
        Indexed[
            Expanded[
                NodeIndex,
                NodeIndex,
                Tuple[NodeIndexPayload, Optional[NodeIndexPayload]],
            ],
            NodeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_neighbors(
        self: Operand[Indexed[EdgeIndex, NodeReference], ContainerType, Unpack[Levels]],
        direction: EdgeDirection,
    ) -> Operand[
        Indexed[
            Expanded[
                EdgeIndex,
                NodeIndex,
                Tuple[EdgeIndexPayload, Optional[NodeIndexPayload]],
            ],
            NodeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_neighbors(
        self: Operand[
            Indexed[Positional, NodeReference], ContainerType, Unpack[Levels]
        ],
        direction: EdgeDirection,
    ) -> Operand[
        Indexed[
            Expanded[Positional, NodeIndex, Tuple[int, Optional[NodeIndexPayload]]],
            NodeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_neighbors(
        self: Operand[
            Indexed[EndpointRole, NodeReference], ContainerType, Unpack[Levels]
        ],
        direction: EdgeDirection,
    ) -> Operand[
        Indexed[
            Expanded[
                EndpointRole,
                NodeIndex,
                Tuple[EdgeEndpointRole, Optional[NodeIndexPayload]],
            ],
            NodeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_neighbors(
        self: Operand[
            Indexed[ValueIndex, NodeReference], ContainerType, Unpack[Levels]
        ],
        direction: EdgeDirection,
    ) -> Operand[
        Indexed[
            Expanded[
                ValueIndex, NodeIndex, Tuple[ScalarValue, Optional[NodeIndexPayload]]
            ],
            NodeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_neighbors(
        self: Operand[
            Indexed[AttributeNameIndex, NodeReference], ContainerType, Unpack[Levels]
        ],
        direction: EdgeDirection,
    ) -> Operand[
        Indexed[
            Expanded[
                AttributeNameIndex,
                NodeIndex,
                Tuple[Attribute, Optional[NodeIndexPayload]],
            ],
            NodeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_neighbors(
        self: Operand[Indexed[BoolIndex, NodeReference], ContainerType, Unpack[Levels]],
        direction: EdgeDirection,
    ) -> Operand[
        Indexed[
            Expanded[BoolIndex, NodeIndex, Tuple[bool, Optional[NodeIndexPayload]]],
            NodeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_neighbors(
        self: Operand[
            Indexed[FailureKindIndex, NodeReference], ContainerType, Unpack[Levels]
        ],
        direction: EdgeDirection,
    ) -> Operand[
        Indexed[
            Expanded[
                FailureKindIndex,
                NodeIndex,
                Tuple[FailureKind, Optional[NodeIndexPayload]],
            ],
            NodeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_neighbors(
        self: Operand[
            Indexed[Expanded[K, ChildType, ParentPayloadType], NodeReference],
            ContainerType,
            Unpack[Levels],
        ],
        direction: EdgeDirection,
    ) -> Operand[
        Indexed[
            Expanded[
                Expanded[K, ChildType, ParentPayloadType],
                NodeIndex,
                Tuple[ParentPayloadType, Optional[NodeIndexPayload]],
            ],
            NodeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    def via_neighbors(
        self, direction: EdgeDirection
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.via_neighbors(direction))

    def nodes(
        self: Union[
            Operand[Indexed[EdgeIndex, Unit], ContainerType, Unpack[Levels]],
            Operand[Indexed[IndexType, EdgeReference], ContainerType, Unpack[Levels]],
        ],
    ) -> Operand[Indexed[NodeIndex, Unit], Multiple[Unordered], Unpack[Levels]]:
        return Operand._from_py_operand(self._operand.nodes())

    @overload
    def via_nodes(
        self: Operand[Indexed[EdgeIndex, Unit], Multiple[Unordered], Unpack[Levels]],
    ) -> Operand[
        Indexed[
            Expanded[
                EdgeIndex,
                EndpointRole,
                Tuple[EdgeIndexPayload, Optional[EdgeEndpointRole]],
            ],
            NodeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[Indexed[EdgeIndex, Unit], Multiple[Ordered], Unpack[Levels]],
    ) -> Operand[
        Indexed[
            Expanded[
                EdgeIndex,
                EndpointRole,
                Tuple[EdgeIndexPayload, Optional[EdgeEndpointRole]],
            ],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[Indexed[EdgeIndex, Unit], Single, Unpack[Levels]],
    ) -> Operand[
        Indexed[
            Expanded[
                EdgeIndex,
                EndpointRole,
                Tuple[EdgeIndexPayload, Optional[EdgeEndpointRole]],
            ],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[Indexed[EdgeIndex, Unit], Definite, Unpack[Levels]],
    ) -> Operand[
        Indexed[
            Expanded[
                EdgeIndex,
                EndpointRole,
                Tuple[EdgeIndexPayload, Optional[EdgeEndpointRole]],
            ],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[
            Indexed[NodeIndex, EdgeReference], Multiple[Unordered], Unpack[Levels]
        ],
    ) -> Operand[
        Indexed[
            Expanded[
                NodeIndex,
                EndpointRole,
                Tuple[NodeIndexPayload, Optional[EdgeEndpointRole]],
            ],
            NodeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[
            Indexed[EdgeIndex, EdgeReference], Multiple[Unordered], Unpack[Levels]
        ],
    ) -> Operand[
        Indexed[
            Expanded[
                EdgeIndex,
                EndpointRole,
                Tuple[EdgeIndexPayload, Optional[EdgeEndpointRole]],
            ],
            NodeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[
            Indexed[Positional, EdgeReference], Multiple[Unordered], Unpack[Levels]
        ],
    ) -> Operand[
        Indexed[
            Expanded[Positional, EndpointRole, Tuple[int, Optional[EdgeEndpointRole]]],
            NodeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[
            Indexed[EndpointRole, EdgeReference], Multiple[Unordered], Unpack[Levels]
        ],
    ) -> Operand[
        Indexed[
            Expanded[
                EndpointRole,
                EndpointRole,
                Tuple[EdgeEndpointRole, Optional[EdgeEndpointRole]],
            ],
            NodeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[
            Indexed[ValueIndex, EdgeReference], Multiple[Unordered], Unpack[Levels]
        ],
    ) -> Operand[
        Indexed[
            Expanded[
                ValueIndex, EndpointRole, Tuple[ScalarValue, Optional[EdgeEndpointRole]]
            ],
            NodeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[
            Indexed[AttributeNameIndex, EdgeReference],
            Multiple[Unordered],
            Unpack[Levels],
        ],
    ) -> Operand[
        Indexed[
            Expanded[
                AttributeNameIndex,
                EndpointRole,
                Tuple[Attribute, Optional[EdgeEndpointRole]],
            ],
            NodeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[
            Indexed[BoolIndex, EdgeReference], Multiple[Unordered], Unpack[Levels]
        ],
    ) -> Operand[
        Indexed[
            Expanded[BoolIndex, EndpointRole, Tuple[bool, Optional[EdgeEndpointRole]]],
            NodeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[
            Indexed[FailureKindIndex, EdgeReference],
            Multiple[Unordered],
            Unpack[Levels],
        ],
    ) -> Operand[
        Indexed[
            Expanded[
                FailureKindIndex,
                EndpointRole,
                Tuple[FailureKind, Optional[EdgeEndpointRole]],
            ],
            NodeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[
            Indexed[Expanded[K, ChildType, ParentPayloadType], EdgeReference],
            Multiple[Unordered],
            Unpack[Levels],
        ],
    ) -> Operand[
        Indexed[
            Expanded[
                Expanded[K, ChildType, ParentPayloadType],
                EndpointRole,
                Tuple[ParentPayloadType, Optional[EdgeEndpointRole]],
            ],
            NodeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[
            Indexed[NodeIndex, EdgeReference], Multiple[Ordered], Unpack[Levels]
        ],
    ) -> Operand[
        Indexed[
            Expanded[
                NodeIndex,
                EndpointRole,
                Tuple[NodeIndexPayload, Optional[EdgeEndpointRole]],
            ],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[
            Indexed[EdgeIndex, EdgeReference], Multiple[Ordered], Unpack[Levels]
        ],
    ) -> Operand[
        Indexed[
            Expanded[
                EdgeIndex,
                EndpointRole,
                Tuple[EdgeIndexPayload, Optional[EdgeEndpointRole]],
            ],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[
            Indexed[Positional, EdgeReference], Multiple[Ordered], Unpack[Levels]
        ],
    ) -> Operand[
        Indexed[
            Expanded[Positional, EndpointRole, Tuple[int, Optional[EdgeEndpointRole]]],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[
            Indexed[EndpointRole, EdgeReference], Multiple[Ordered], Unpack[Levels]
        ],
    ) -> Operand[
        Indexed[
            Expanded[
                EndpointRole,
                EndpointRole,
                Tuple[EdgeEndpointRole, Optional[EdgeEndpointRole]],
            ],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[
            Indexed[ValueIndex, EdgeReference], Multiple[Ordered], Unpack[Levels]
        ],
    ) -> Operand[
        Indexed[
            Expanded[
                ValueIndex, EndpointRole, Tuple[ScalarValue, Optional[EdgeEndpointRole]]
            ],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[
            Indexed[AttributeNameIndex, EdgeReference],
            Multiple[Ordered],
            Unpack[Levels],
        ],
    ) -> Operand[
        Indexed[
            Expanded[
                AttributeNameIndex,
                EndpointRole,
                Tuple[Attribute, Optional[EdgeEndpointRole]],
            ],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[
            Indexed[BoolIndex, EdgeReference], Multiple[Ordered], Unpack[Levels]
        ],
    ) -> Operand[
        Indexed[
            Expanded[BoolIndex, EndpointRole, Tuple[bool, Optional[EdgeEndpointRole]]],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[
            Indexed[FailureKindIndex, EdgeReference], Multiple[Ordered], Unpack[Levels]
        ],
    ) -> Operand[
        Indexed[
            Expanded[
                FailureKindIndex,
                EndpointRole,
                Tuple[FailureKind, Optional[EdgeEndpointRole]],
            ],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[
            Indexed[Expanded[K, ChildType, ParentPayloadType], EdgeReference],
            Multiple[Ordered],
            Unpack[Levels],
        ],
    ) -> Operand[
        Indexed[
            Expanded[
                Expanded[K, ChildType, ParentPayloadType],
                EndpointRole,
                Tuple[ParentPayloadType, Optional[EdgeEndpointRole]],
            ],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[Indexed[NodeIndex, EdgeReference], Single, Unpack[Levels]],
    ) -> Operand[
        Indexed[
            Expanded[
                NodeIndex,
                EndpointRole,
                Tuple[NodeIndexPayload, Optional[EdgeEndpointRole]],
            ],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[Indexed[EdgeIndex, EdgeReference], Single, Unpack[Levels]],
    ) -> Operand[
        Indexed[
            Expanded[
                EdgeIndex,
                EndpointRole,
                Tuple[EdgeIndexPayload, Optional[EdgeEndpointRole]],
            ],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[Indexed[Positional, EdgeReference], Single, Unpack[Levels]],
    ) -> Operand[
        Indexed[
            Expanded[Positional, EndpointRole, Tuple[int, Optional[EdgeEndpointRole]]],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[Indexed[EndpointRole, EdgeReference], Single, Unpack[Levels]],
    ) -> Operand[
        Indexed[
            Expanded[
                EndpointRole,
                EndpointRole,
                Tuple[EdgeEndpointRole, Optional[EdgeEndpointRole]],
            ],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[Indexed[ValueIndex, EdgeReference], Single, Unpack[Levels]],
    ) -> Operand[
        Indexed[
            Expanded[
                ValueIndex, EndpointRole, Tuple[ScalarValue, Optional[EdgeEndpointRole]]
            ],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[
            Indexed[AttributeNameIndex, EdgeReference], Single, Unpack[Levels]
        ],
    ) -> Operand[
        Indexed[
            Expanded[
                AttributeNameIndex,
                EndpointRole,
                Tuple[Attribute, Optional[EdgeEndpointRole]],
            ],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[Indexed[BoolIndex, EdgeReference], Single, Unpack[Levels]],
    ) -> Operand[
        Indexed[
            Expanded[BoolIndex, EndpointRole, Tuple[bool, Optional[EdgeEndpointRole]]],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[Indexed[FailureKindIndex, EdgeReference], Single, Unpack[Levels]],
    ) -> Operand[
        Indexed[
            Expanded[
                FailureKindIndex,
                EndpointRole,
                Tuple[FailureKind, Optional[EdgeEndpointRole]],
            ],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[
            Indexed[Expanded[K, ChildType, ParentPayloadType], EdgeReference],
            Single,
            Unpack[Levels],
        ],
    ) -> Operand[
        Indexed[
            Expanded[
                Expanded[K, ChildType, ParentPayloadType],
                EndpointRole,
                Tuple[ParentPayloadType, Optional[EdgeEndpointRole]],
            ],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[Indexed[NodeIndex, EdgeReference], Definite, Unpack[Levels]],
    ) -> Operand[
        Indexed[
            Expanded[
                NodeIndex,
                EndpointRole,
                Tuple[NodeIndexPayload, Optional[EdgeEndpointRole]],
            ],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[Indexed[EdgeIndex, EdgeReference], Definite, Unpack[Levels]],
    ) -> Operand[
        Indexed[
            Expanded[
                EdgeIndex,
                EndpointRole,
                Tuple[EdgeIndexPayload, Optional[EdgeEndpointRole]],
            ],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[Indexed[Positional, EdgeReference], Definite, Unpack[Levels]],
    ) -> Operand[
        Indexed[
            Expanded[Positional, EndpointRole, Tuple[int, Optional[EdgeEndpointRole]]],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[Indexed[EndpointRole, EdgeReference], Definite, Unpack[Levels]],
    ) -> Operand[
        Indexed[
            Expanded[
                EndpointRole,
                EndpointRole,
                Tuple[EdgeEndpointRole, Optional[EdgeEndpointRole]],
            ],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[Indexed[ValueIndex, EdgeReference], Definite, Unpack[Levels]],
    ) -> Operand[
        Indexed[
            Expanded[
                ValueIndex, EndpointRole, Tuple[ScalarValue, Optional[EdgeEndpointRole]]
            ],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[
            Indexed[AttributeNameIndex, EdgeReference], Definite, Unpack[Levels]
        ],
    ) -> Operand[
        Indexed[
            Expanded[
                AttributeNameIndex,
                EndpointRole,
                Tuple[Attribute, Optional[EdgeEndpointRole]],
            ],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[Indexed[BoolIndex, EdgeReference], Definite, Unpack[Levels]],
    ) -> Operand[
        Indexed[
            Expanded[BoolIndex, EndpointRole, Tuple[bool, Optional[EdgeEndpointRole]]],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[
            Indexed[FailureKindIndex, EdgeReference], Definite, Unpack[Levels]
        ],
    ) -> Operand[
        Indexed[
            Expanded[
                FailureKindIndex,
                EndpointRole,
                Tuple[FailureKind, Optional[EdgeEndpointRole]],
            ],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Operand[
            Indexed[Expanded[K, ChildType, ParentPayloadType], EdgeReference],
            Definite,
            Unpack[Levels],
        ],
    ) -> Operand[
        Indexed[
            Expanded[
                Expanded[K, ChildType, ParentPayloadType],
                EndpointRole,
                Tuple[ParentPayloadType, Optional[EdgeEndpointRole]],
            ],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    def via_nodes(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.via_nodes())

    @overload
    def source_node(
        self: Union[
            Operand[Indexed[EdgeIndex, Unit], Multiple[OrderType], Unpack[Levels]],
            Operand[
                Indexed[IndexType, EdgeReference], Multiple[OrderType], Unpack[Levels]
            ],
        ],
    ) -> Operand[Indexed[NodeIndex, Unit], Multiple[Unordered], Unpack[Levels]]: ...

    @overload
    def source_node(
        self: Union[
            Operand[Indexed[EdgeIndex, Unit], Single, Unpack[Levels]],
            Operand[Indexed[IndexType, EdgeReference], Single, Unpack[Levels]],
        ],
    ) -> Operand[Indexed[NodeIndex, Unit], Single, Unpack[Levels]]: ...

    @overload
    def source_node(
        self: Union[
            Operand[Indexed[EdgeIndex, Unit], Definite, Unpack[Levels]],
            Operand[Indexed[IndexType, EdgeReference], Definite, Unpack[Levels]],
        ],
    ) -> Operand[Indexed[NodeIndex, Unit], Definite, Unpack[Levels]]: ...

    def source_node(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.source_node())

    @overload
    def target_node(
        self: Union[
            Operand[Indexed[EdgeIndex, Unit], Multiple[OrderType], Unpack[Levels]],
            Operand[
                Indexed[IndexType, EdgeReference], Multiple[OrderType], Unpack[Levels]
            ],
        ],
    ) -> Operand[Indexed[NodeIndex, Unit], Multiple[Unordered], Unpack[Levels]]: ...

    @overload
    def target_node(
        self: Union[
            Operand[Indexed[EdgeIndex, Unit], Single, Unpack[Levels]],
            Operand[Indexed[IndexType, EdgeReference], Single, Unpack[Levels]],
        ],
    ) -> Operand[Indexed[NodeIndex, Unit], Single, Unpack[Levels]]: ...

    @overload
    def target_node(
        self: Union[
            Operand[Indexed[EdgeIndex, Unit], Definite, Unpack[Levels]],
            Operand[Indexed[IndexType, EdgeReference], Definite, Unpack[Levels]],
        ],
    ) -> Operand[Indexed[NodeIndex, Unit], Definite, Unpack[Levels]]: ...

    def target_node(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.target_node())

    @overload
    def via_source_node(
        self: Operand[Indexed[EdgeIndex, Unit], ContainerType, Unpack[Levels]],
    ) -> Operand[Indexed[EdgeIndex, NodeReference], ContainerType, Unpack[Levels]]: ...

    @overload
    def via_source_node(
        self: Operand[Indexed[IndexType, EdgeReference], ContainerType, Unpack[Levels]],
    ) -> Operand[Indexed[IndexType, NodeReference], ContainerType, Unpack[Levels]]: ...

    def via_source_node(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.via_source_node())

    @overload
    def via_target_node(
        self: Operand[Indexed[EdgeIndex, Unit], ContainerType, Unpack[Levels]],
    ) -> Operand[Indexed[EdgeIndex, NodeReference], ContainerType, Unpack[Levels]]: ...

    @overload
    def via_target_node(
        self: Operand[Indexed[IndexType, EdgeReference], ContainerType, Unpack[Levels]],
    ) -> Operand[Indexed[IndexType, NodeReference], ContainerType, Unpack[Levels]]: ...

    def via_target_node(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.via_target_node())

    @overload
    def group_by(
        self: Operand[Indexed[IndexType, V], ContainerType, Unpack[Levels]],
        key: Union[ScalarValue, GroupingArgument[IndexType, Scalar, ArgumentOrderType]],
    ) -> Operand[
        Indexed[IndexType, V],
        ContainerType,
        Unpack[Levels],
        Grouped[IndexType, ValueIndex],
    ]: ...

    @overload
    def group_by(
        self: Operand[Indexed[IndexType, V], ContainerType, Unpack[Levels]],
        key: GroupingArgument[IndexType, Mask, ArgumentOrderType],
    ) -> Operand[
        Indexed[IndexType, V],
        ContainerType,
        Unpack[Levels],
        Grouped[IndexType, BoolIndex],
    ]: ...

    @overload
    def group_by(
        self: Operand[Indexed[IndexType, V], ContainerType, Unpack[Levels]],
        key: GroupingArgument[IndexType, AttributeName, ArgumentOrderType],
    ) -> Operand[
        Indexed[IndexType, V],
        ContainerType,
        Unpack[Levels],
        Grouped[IndexType, AttributeNameIndex],
    ]: ...

    @overload
    def group_by(
        self: Operand[Indexed[IndexType, V], ContainerType, Unpack[Levels]],
        key: GroupingArgument[IndexType, FailureKindValue, ArgumentOrderType],
    ) -> Operand[
        Indexed[IndexType, V],
        ContainerType,
        Unpack[Levels],
        Grouped[IndexType, FailureKindIndex],
    ]: ...

    @overload
    def group_by(
        self: Operand[Indexed[IndexType, V], ContainerType, Unpack[Levels]],
        key: GroupingArgument[IndexType, IndexValue[K], ArgumentOrderType],
    ) -> Operand[
        Indexed[IndexType, V], ContainerType, Unpack[Levels], Grouped[IndexType, K]
    ]: ...

    @overload
    def group_by(
        self: Operand[Indexed[IndexType, V], ContainerType, Unpack[Levels]],
        key: GroupingArgument[IndexType, NodeReference, ArgumentOrderType],
    ) -> Operand[
        Indexed[IndexType, V],
        ContainerType,
        Unpack[Levels],
        Grouped[IndexType, NodeIndex],
    ]: ...

    @overload
    def group_by(
        self: Operand[Indexed[IndexType, V], ContainerType, Unpack[Levels]],
        key: GroupingArgument[IndexType, EdgeReference, ArgumentOrderType],
    ) -> Operand[
        Indexed[IndexType, V],
        ContainerType,
        Unpack[Levels],
        Grouped[IndexType, EdgeIndex],
    ]: ...

    def group_by(
        self,
        key: Union[
            ScalarValue, Operand[Any, Any, Unpack[Tuple[Any, ...]]], Argument[Any, Any]
        ],
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(
            self._operand.group_by(Operand._to_py_argument(key))
        )

    def having(
        self: Operand[
            S, C, Unpack[OuterLevels], Grouped[MemberIndexType, KeyIndexType]
        ],
        predicate: MaskArgument[KeyIndexType, ArgumentOrderType],
    ) -> Operand[S, C, Unpack[OuterLevels], Grouped[MemberIndexType, KeyIndexType]]:
        return Operand._from_py_operand(
            self._operand.having(Operand._to_py_argument(predicate))
        )

    def keys(
        self: Operand[
            S, C, Unpack[OuterLevels], Grouped[MemberIndexType, KeyIndexType]
        ],
    ) -> Operand[Indexed[KeyIndexType, Unit], Multiple[Unordered], Unpack[OuterLevels]]:
        return Operand._from_py_operand(self._operand.keys())

    @overload
    def ungroup(
        self: Operand[
            Indexed[IndexType, V],
            ContainerType,
            Unpack[OuterLevels],
            Grouped[MemberIndexType, KeyIndexType],
        ],
    ) -> Operand[Indexed[IndexType, V], Multiple[Unordered], Unpack[OuterLevels]]: ...

    @overload
    def ungroup(
        self: Operand[
            Bare[BareValueType],
            ContainerType,
            Unpack[OuterLevels],
            Grouped[MemberIndexType, KeyIndexType],
        ],
    ) -> Operand[Bare[BareValueType], Multiple[Unordered], Unpack[OuterLevels]]: ...

    def ungroup(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.ungroup())

    @overload
    def ungroup_keyed(
        self: Operand[
            Indexed[IndexType, V],
            Single,
            Unpack[OuterLevels],
            Grouped[MemberIndexType, KeyIndexType],
        ],
    ) -> Operand[
        Indexed[KeyIndexType, V], Multiple[Unordered], Unpack[OuterLevels]
    ]: ...

    @overload
    def ungroup_keyed(
        self: Operand[
            Indexed[IndexType, V],
            Definite,
            Unpack[OuterLevels],
            Grouped[MemberIndexType, KeyIndexType],
        ],
    ) -> Operand[
        Indexed[KeyIndexType, V], Multiple[Unordered], Unpack[OuterLevels]
    ]: ...

    @overload
    def ungroup_keyed(
        self: Operand[
            Bare[BareValueType],
            Single,
            Unpack[OuterLevels],
            Grouped[MemberIndexType, KeyIndexType],
        ],
    ) -> Operand[
        Indexed[KeyIndexType, BareValueType], Multiple[Unordered], Unpack[OuterLevels]
    ]: ...

    @overload
    def ungroup_keyed(
        self: Operand[
            Bare[BareValueType],
            Definite,
            Unpack[OuterLevels],
            Grouped[MemberIndexType, KeyIndexType],
        ],
    ) -> Operand[
        Indexed[KeyIndexType, BareValueType], Multiple[Unordered], Unpack[OuterLevels]
    ]: ...

    def ungroup_keyed(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.ungroup_keyed())

    @overload
    def broadcast(
        self: Operand[
            Indexed[IndexType, V],
            Single,
            Unpack[OuterLevels],
            Grouped[MemberIndexType, KeyIndexType],
        ],
    ) -> Operand[
        Indexed[MemberIndexType, V], Multiple[Unordered], Unpack[OuterLevels]
    ]: ...

    @overload
    def broadcast(
        self: Operand[
            Indexed[IndexType, V],
            Definite,
            Unpack[OuterLevels],
            Grouped[MemberIndexType, KeyIndexType],
        ],
    ) -> Operand[
        Indexed[MemberIndexType, V], Multiple[Unordered], Unpack[OuterLevels]
    ]: ...

    @overload
    def broadcast(
        self: Operand[
            Bare[BareValueType],
            Single,
            Unpack[OuterLevels],
            Grouped[MemberIndexType, KeyIndexType],
        ],
    ) -> Operand[
        Indexed[MemberIndexType, BareValueType],
        Multiple[Unordered],
        Unpack[OuterLevels],
    ]: ...

    @overload
    def broadcast(
        self: Operand[
            Bare[BareValueType],
            Definite,
            Unpack[OuterLevels],
            Grouped[MemberIndexType, KeyIndexType],
        ],
    ) -> Operand[
        Indexed[MemberIndexType, BareValueType],
        Multiple[Unordered],
        Unpack[OuterLevels],
    ]: ...

    def broadcast(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.broadcast())

    @overload
    def broadcast_via(
        self: Union[
            Operand[
                Indexed[IndexType, V],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, ValueIndex],
            ],
            Operand[
                Indexed[IndexType, V],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, ValueIndex],
            ],
        ],
        population: Operand[
            Indexed[PopulationIndexType, Scalar], PopulationContainerType
        ],
    ) -> Operand[
        Indexed[PopulationIndexType, V], PopulationContainerType, Unpack[OuterLevels]
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Operand[
                Bare[BareValueType],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, ValueIndex],
            ],
            Operand[
                Bare[BareValueType],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, ValueIndex],
            ],
        ],
        population: Operand[
            Indexed[PopulationIndexType, Scalar], PopulationContainerType
        ],
    ) -> Operand[
        Indexed[PopulationIndexType, BareValueType],
        PopulationContainerType,
        Unpack[OuterLevels],
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Operand[
                Indexed[IndexType, V],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, BoolIndex],
            ],
            Operand[
                Indexed[IndexType, V],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, BoolIndex],
            ],
        ],
        population: Operand[
            Indexed[PopulationIndexType, Mask], PopulationContainerType
        ],
    ) -> Operand[
        Indexed[PopulationIndexType, V], PopulationContainerType, Unpack[OuterLevels]
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Operand[
                Bare[BareValueType],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, BoolIndex],
            ],
            Operand[
                Bare[BareValueType],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, BoolIndex],
            ],
        ],
        population: Operand[
            Indexed[PopulationIndexType, Mask], PopulationContainerType
        ],
    ) -> Operand[
        Indexed[PopulationIndexType, BareValueType],
        PopulationContainerType,
        Unpack[OuterLevels],
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Operand[
                Indexed[IndexType, V],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, AttributeNameIndex],
            ],
            Operand[
                Indexed[IndexType, V],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, AttributeNameIndex],
            ],
        ],
        population: Operand[
            Indexed[PopulationIndexType, AttributeName],
            PopulationContainerType,
        ],
    ) -> Operand[
        Indexed[PopulationIndexType, V], PopulationContainerType, Unpack[OuterLevels]
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Operand[
                Bare[BareValueType],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, AttributeNameIndex],
            ],
            Operand[
                Bare[BareValueType],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, AttributeNameIndex],
            ],
        ],
        population: Operand[
            Indexed[PopulationIndexType, AttributeName],
            PopulationContainerType,
        ],
    ) -> Operand[
        Indexed[PopulationIndexType, BareValueType],
        PopulationContainerType,
        Unpack[OuterLevels],
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Operand[
                Indexed[IndexType, V],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, FailureKindIndex],
            ],
            Operand[
                Indexed[IndexType, V],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, FailureKindIndex],
            ],
        ],
        population: Operand[
            Indexed[PopulationIndexType, FailureKindValue],
            PopulationContainerType,
        ],
    ) -> Operand[
        Indexed[PopulationIndexType, V], PopulationContainerType, Unpack[OuterLevels]
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Operand[
                Bare[BareValueType],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, FailureKindIndex],
            ],
            Operand[
                Bare[BareValueType],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, FailureKindIndex],
            ],
        ],
        population: Operand[
            Indexed[PopulationIndexType, FailureKindValue],
            PopulationContainerType,
        ],
    ) -> Operand[
        Indexed[PopulationIndexType, BareValueType],
        PopulationContainerType,
        Unpack[OuterLevels],
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Operand[
                Indexed[IndexType, V],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, KeyIndexType],
            ],
            Operand[
                Indexed[IndexType, V],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, KeyIndexType],
            ],
        ],
        population: Operand[
            Indexed[PopulationIndexType, IndexValue[KeyIndexType]],
            PopulationContainerType,
        ],
    ) -> Operand[
        Indexed[PopulationIndexType, V], PopulationContainerType, Unpack[OuterLevels]
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Operand[
                Bare[BareValueType],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, KeyIndexType],
            ],
            Operand[
                Bare[BareValueType],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, KeyIndexType],
            ],
        ],
        population: Operand[
            Indexed[PopulationIndexType, IndexValue[KeyIndexType]],
            PopulationContainerType,
        ],
    ) -> Operand[
        Indexed[PopulationIndexType, BareValueType],
        PopulationContainerType,
        Unpack[OuterLevels],
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Operand[
                Indexed[IndexType, V],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, NodeIndex],
            ],
            Operand[
                Indexed[IndexType, V],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, NodeIndex],
            ],
        ],
        population: Operand[
            Indexed[PopulationIndexType, NodeReference],
            PopulationContainerType,
        ],
    ) -> Operand[
        Indexed[PopulationIndexType, V], PopulationContainerType, Unpack[OuterLevels]
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Operand[
                Bare[BareValueType],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, NodeIndex],
            ],
            Operand[
                Bare[BareValueType],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, NodeIndex],
            ],
        ],
        population: Operand[
            Indexed[PopulationIndexType, NodeReference],
            PopulationContainerType,
        ],
    ) -> Operand[
        Indexed[PopulationIndexType, BareValueType],
        PopulationContainerType,
        Unpack[OuterLevels],
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Operand[
                Indexed[IndexType, V],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, EdgeIndex],
            ],
            Operand[
                Indexed[IndexType, V],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, EdgeIndex],
            ],
        ],
        population: Operand[
            Indexed[PopulationIndexType, EdgeReference],
            PopulationContainerType,
        ],
    ) -> Operand[
        Indexed[PopulationIndexType, V], PopulationContainerType, Unpack[OuterLevels]
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Operand[
                Bare[BareValueType],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, EdgeIndex],
            ],
            Operand[
                Bare[BareValueType],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, EdgeIndex],
            ],
        ],
        population: Operand[
            Indexed[PopulationIndexType, EdgeReference],
            PopulationContainerType,
        ],
    ) -> Operand[
        Indexed[PopulationIndexType, BareValueType],
        PopulationContainerType,
        Unpack[OuterLevels],
    ]: ...

    def broadcast_via(
        self,
        population: Operand[Any, Any],
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(
            self._operand.broadcast_via(population._operand)
        )

    @overload
    def bucket_errors(
        self: Operand[
            Indexed[IndexType, V],
            ContainerType,
            Unpack[OuterLevels],
            Grouped[MemberIndexType, KeyIndexType],
        ],
    ) -> Operand[
        Indexed[KeyIndexType, FailureValue], Multiple[Unordered], Unpack[OuterLevels]
    ]: ...

    @overload
    def bucket_errors(
        self: Operand[
            Bare[BareValueType],
            ContainerType,
            Unpack[OuterLevels],
            Grouped[MemberIndexType, KeyIndexType],
        ],
    ) -> Operand[
        Indexed[KeyIndexType, FailureValue], Multiple[Unordered], Unpack[OuterLevels]
    ]: ...

    def bucket_errors(self) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.bucket_errors())

    def key_errors(
        self: Operand[
            S, C, Unpack[OuterLevels], Grouped[MemberIndexType, KeyIndexType]
        ],
    ) -> Operand[
        Indexed[MemberIndexType, FailureValue], Multiple[Unordered], Unpack[OuterLevels]
    ]:
        return Operand._from_py_operand(self._operand.key_errors())

    @overload
    def on_bucket_error(
        self: Operand[
            Indexed[IndexType, V],
            ContainerType,
            Unpack[OuterLevels],
            Grouped[MemberIndexType, KeyIndexType],
        ],
        policy: Union[Drop, Raise],
    ) -> Operand[
        Indexed[IndexType, V],
        ContainerType,
        Unpack[OuterLevels],
        Grouped[MemberIndexType, KeyIndexType],
    ]: ...

    @overload
    def on_bucket_error(
        self: Operand[
            Bare[BareValueType],
            ContainerType,
            Unpack[OuterLevels],
            Grouped[MemberIndexType, KeyIndexType],
        ],
        policy: Union[Drop, Raise],
    ) -> Operand[
        Bare[BareValueType],
        ContainerType,
        Unpack[OuterLevels],
        Grouped[MemberIndexType, KeyIndexType],
    ]: ...

    def on_bucket_error(
        self, policy: Union[Drop, Raise]
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        if isinstance(policy, Drop):
            return Operand._from_py_operand(self._operand.on_bucket_error_drop())

        return Operand._from_py_operand(self._operand.on_bucket_error_raise())

    @overload
    def on_key_error(
        self: Operand[
            Indexed[IndexType, V],
            ContainerType,
            Unpack[OuterLevels],
            Grouped[MemberIndexType, KeyIndexType],
        ],
        policy: Union[Drop, Raise],
    ) -> Operand[
        Indexed[IndexType, V],
        ContainerType,
        Unpack[OuterLevels],
        Grouped[MemberIndexType, KeyIndexType],
    ]: ...

    @overload
    def on_key_error(
        self: Operand[
            Bare[BareValueType],
            ContainerType,
            Unpack[OuterLevels],
            Grouped[MemberIndexType, KeyIndexType],
        ],
        policy: Union[Drop, Raise],
    ) -> Operand[
        Bare[BareValueType],
        ContainerType,
        Unpack[OuterLevels],
        Grouped[MemberIndexType, KeyIndexType],
    ]: ...

    def on_key_error(
        self, policy: Union[Drop, Raise]
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        if isinstance(policy, Drop):
            return Operand._from_py_operand(self._operand.on_key_error_drop())

        return Operand._from_py_operand(self._operand.on_key_error_raise())

    @overload
    def transition(
        self: Operand[Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]],
        target: ValueTarget[ScalarTransitionValueType],
    ) -> Operand[
        Indexed[IndexType, ScalarTransitionValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def transition(
        self: Operand[Bare[Scalar], ContainerType, Unpack[Levels]],
        target: ValueTarget[ScalarTransitionValueType],
    ) -> Operand[Bare[ScalarTransitionValueType], ContainerType, Unpack[Levels]]: ...

    @overload
    def transition(
        self: Operand[
            Indexed[IndexType, IndexValue[ValueIndex]], ContainerType, Unpack[Levels]
        ],
        target: ValueTarget[ValueIndexTransitionValueType],
    ) -> Operand[
        Indexed[IndexType, ValueIndexTransitionValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def transition(
        self: Operand[Bare[IndexValue[ValueIndex]], ContainerType, Unpack[Levels]],
        target: ValueTarget[ValueIndexTransitionValueType],
    ) -> Operand[
        Bare[ValueIndexTransitionValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def transition(
        self: Operand[Indexed[IndexType, AttributeName], ContainerType, Unpack[Levels]],
        target: ValueTarget[AttributeNameTransitionValueType],
    ) -> Operand[
        Indexed[IndexType, AttributeNameTransitionValueType],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def transition(
        self: Operand[Bare[AttributeName], ContainerType, Unpack[Levels]],
        target: ValueTarget[AttributeNameTransitionValueType],
    ) -> Operand[
        Bare[AttributeNameTransitionValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def transition(
        self: Operand[
            Indexed[IndexType, IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
        ],
        target: ValueTarget[NodeIndexTransitionValueType],
    ) -> Operand[
        Indexed[IndexType, NodeIndexTransitionValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def transition(
        self: Operand[Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]],
        target: ValueTarget[NodeIndexTransitionValueType],
    ) -> Operand[Bare[NodeIndexTransitionValueType], ContainerType, Unpack[Levels]]: ...

    @overload
    def transition(
        self: Operand[
            Indexed[IndexType, IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        target: ValueTarget[AttributeNameIndexTransitionValueType],
    ) -> Operand[
        Indexed[IndexType, AttributeNameIndexTransitionValueType],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def transition(
        self: Operand[
            Bare[IndexValue[AttributeNameIndex]], ContainerType, Unpack[Levels]
        ],
        target: ValueTarget[AttributeNameIndexTransitionValueType],
    ) -> Operand[
        Bare[AttributeNameIndexTransitionValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def transition(
        self: Operand[
            Indexed[IndexType, IndexValue[EdgeIndex]], ContainerType, Unpack[Levels]
        ],
        target: ValueTarget[EdgeIndexTransitionValueType],
    ) -> Operand[
        Indexed[IndexType, EdgeIndexTransitionValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def transition(
        self: Operand[Bare[IndexValue[EdgeIndex]], ContainerType, Unpack[Levels]],
        target: ValueTarget[EdgeIndexTransitionValueType],
    ) -> Operand[Bare[EdgeIndexTransitionValueType], ContainerType, Unpack[Levels]]: ...

    @overload
    def transition(
        self: Operand[
            Indexed[IndexType, IndexValue[Positional]], ContainerType, Unpack[Levels]
        ],
        target: ValueTarget[PositionalTransitionValueType],
    ) -> Operand[
        Indexed[IndexType, PositionalTransitionValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def transition(
        self: Operand[Bare[IndexValue[Positional]], ContainerType, Unpack[Levels]],
        target: ValueTarget[PositionalTransitionValueType],
    ) -> Operand[
        Bare[PositionalTransitionValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def transition(
        self: Operand[Indexed[IndexType, Mask], ContainerType, Unpack[Levels]],
        target: ValueTarget[MaskTransitionValueType],
    ) -> Operand[
        Indexed[IndexType, MaskTransitionValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def transition(
        self: Operand[Bare[Mask], ContainerType, Unpack[Levels]],
        target: ValueTarget[MaskTransitionValueType],
    ) -> Operand[Bare[MaskTransitionValueType], ContainerType, Unpack[Levels]]: ...

    @overload
    def transition(
        self: Operand[
            Indexed[IndexType, IndexValue[BoolIndex]], ContainerType, Unpack[Levels]
        ],
        target: ValueTarget[BoolIndexTransitionValueType],
    ) -> Operand[
        Indexed[IndexType, BoolIndexTransitionValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def transition(
        self: Operand[Bare[IndexValue[BoolIndex]], ContainerType, Unpack[Levels]],
        target: ValueTarget[BoolIndexTransitionValueType],
    ) -> Operand[Bare[BoolIndexTransitionValueType], ContainerType, Unpack[Levels]]: ...

    @overload
    def transition(
        self: Operand[
            Indexed[IndexType, FailureKindValue], ContainerType, Unpack[Levels]
        ],
        target: ValueTarget[IndexValue[FailureKindIndex]],
    ) -> Operand[
        Indexed[IndexType, IndexValue[FailureKindIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def transition(
        self: Operand[Bare[FailureKindValue], ContainerType, Unpack[Levels]],
        target: ValueTarget[IndexValue[FailureKindIndex]],
    ) -> Operand[Bare[IndexValue[FailureKindIndex]], ContainerType, Unpack[Levels]]: ...

    @overload
    def transition(
        self: Operand[
            Indexed[IndexType, IndexValue[FailureKindIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        target: ValueTarget[FailureKindValue],
    ) -> Operand[
        Indexed[IndexType, FailureKindValue], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def transition(
        self: Operand[
            Bare[IndexValue[FailureKindIndex]], ContainerType, Unpack[Levels]
        ],
        target: ValueTarget[FailureKindValue],
    ) -> Operand[Bare[FailureKindValue], ContainerType, Unpack[Levels]]: ...

    def transition(
        self, target: ValueTarget[Any]
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
        return Operand._from_py_operand(self._operand.transition(target._target))

    @overload
    def expand_to(
        self: Operand[
            Indexed[
                Expanded[IndexType, ChildType, ParentPayloadType], TemplateValueType
            ],
            ContainerType,
            Unpack[Levels],
        ],
        values: ScalarValue,
    ) -> Operand[
        Indexed[Expanded[IndexType, ChildType, ParentPayloadType], Scalar],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def expand_to(
        self: Operand[
            Indexed[
                Expanded[IndexType, ChildType, ParentPayloadType], TemplateValueType
            ],
            ContainerType,
            Unpack[Levels],
        ],
        values: IndexedOperandArgument[IndexType, ExpandedValueType, ArgumentOrderType],
    ) -> Operand[
        Indexed[Expanded[IndexType, ChildType, ParentPayloadType], ExpandedValueType],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def expand_to(
        self: Operand[
            Indexed[
                Expanded[IndexType, ChildType, ParentPayloadType], TemplateValueType
            ],
            Definite,
            Unpack[Levels],
        ],
        values: IndexedDroppingArgument[IndexType, ExpandedValueType],
    ) -> Operand[
        Indexed[Expanded[IndexType, ChildType, ParentPayloadType], ExpandedValueType],
        Single,
        Unpack[Levels],
    ]: ...

    @overload
    def expand_to(
        self: Operand[
            Indexed[
                Expanded[IndexType, ChildType, ParentPayloadType], TemplateValueType
            ],
            DroppedContainerType,
            Unpack[Levels],
        ],
        values: IndexedDroppingArgument[IndexType, ExpandedValueType],
    ) -> Operand[
        Indexed[Expanded[IndexType, ChildType, ParentPayloadType], ExpandedValueType],
        DroppedContainerType,
        Unpack[Levels],
    ]: ...

    def expand_to(
        self,
        values: Union[
            ScalarValue, Operand[Any, Any, Unpack[Tuple[Any, ...]]], Argument[Any, Any]
        ],
    ) -> Operand[Any, Any, Unpack[Tuple[Any, ...]]]:
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
    Operand[Indexed[IndexType, V], Multiple[ArgumentOrderType]],
    Operand[Bare[V], Single],
    Operand[Bare[V], Definite],
    Argument[Indexed[IndexType, V], Preserving],
]
BareOperandArgument: TypeAlias = Union[
    Operand[Bare[V], Single],
    Operand[Bare[V], Definite],
    Argument[Bare[V], Preserving],
]
MaskArgument: TypeAlias = Union[
    bool,
    Operand[Indexed[IndexType, Mask], Multiple[ArgumentOrderType]],
    Operand[Bare[Mask], Single],
    Operand[Bare[Mask], Definite],
    Argument[Indexed[IndexType, Mask], Preserving],
    Argument[Indexed[IndexType, Mask], Dropping],
]
BareMaskArgument: TypeAlias = Union[
    bool,
    Operand[Bare[Mask], Single],
    Operand[Bare[Mask], Definite],
    Argument[Bare[Mask], Preserving],
    Argument[Bare[Mask], Dropping],
]
IndexedDroppingArgument: TypeAlias = Argument[Indexed[IndexType, V], Dropping]
BareDroppingArgument: TypeAlias = Argument[Bare[V], Dropping]
GroupingArgument: TypeAlias = Union[
    Operand[Indexed[IndexType, V], Multiple[ArgumentOrderType]],
    Operand[Bare[V], Single],
    Operand[Bare[V], Definite],
    Argument[Indexed[IndexType, V], Preserving],
    Argument[Indexed[IndexType, V], Dropping],
]
MembershipArgument: TypeAlias = Union[
    Operand[Indexed[Any, V], Any], Operand[Bare[V], Any]
]
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
BareAnyScalarArgument: TypeAlias = Union[BareScalarArgument[V], BareDroppingArgument[V]]
IndexedAnyScalarArgument: TypeAlias = Union[
    IndexedScalarArgument[IndexType, V, ArgumentOrderType],
    IndexedDroppingArgument[IndexType, V],
]
IndexedAttributeArgument: TypeAlias = Union[
    Attribute, IndexedOperandArgument[IndexType, V, ArgumentOrderType]
]
BareAttributeArgument: TypeAlias = Union[Attribute, BareOperandArgument[V]]
IndexedAnyAttributeArgument: TypeAlias = Union[
    IndexedAttributeArgument[IndexType, V, ArgumentOrderType],
    IndexedDroppingArgument[IndexType, V],
]
BareAnyAttributeArgument: TypeAlias = Union[
    BareAttributeArgument[V], BareDroppingArgument[V]
]
BareReplacement: TypeAlias = Union[
    Replace[Operand[Bare[V], Single]],
    Replace[Operand[Bare[V], Definite]],
]

AttributesOperand: TypeAlias = Operand[
    Indexed[IndexType, AttributeName], Multiple[OrderType]
]
BareAttributesOperand: TypeAlias = Operand[Bare[AttributeName], Multiple[OrderType]]
AttributeOperand: TypeAlias = Operand[Indexed[IndexType, AttributeName], Single]
BareAttributeOperand: TypeAlias = Operand[Bare[AttributeName], Single]
DefiniteAttributeOperand: TypeAlias = Operand[
    Indexed[IndexType, AttributeName], Definite
]
DefiniteBareAttributeOperand: TypeAlias = Operand[Bare[AttributeName], Definite]

BoolMaskOperand: TypeAlias = Operand[Indexed[IndexType, Mask], Multiple[OrderType]]
BareBoolMaskOperand: TypeAlias = Operand[Bare[Mask], Multiple[OrderType]]
BoolOperand: TypeAlias = Operand[Indexed[IndexType, Mask], Single]
BareBoolOperand: TypeAlias = Operand[Bare[Mask], Single]
DefiniteBoolOperand: TypeAlias = Operand[Indexed[IndexType, Mask], Definite]
DefiniteBareBoolOperand: TypeAlias = Operand[Bare[Mask], Definite]

ElementsOperand: TypeAlias = Operand[Indexed[IndexType, Unit], Multiple[OrderType]]
ElementOperand: TypeAlias = Operand[Indexed[IndexType, Unit], Single]
DefiniteElementOperand: TypeAlias = Operand[Indexed[IndexType, Unit], Definite]

FailuresOperand: TypeAlias = Operand[
    Indexed[IndexType, FailureValue], Multiple[OrderType]
]
FailureKindsOperand: TypeAlias = Operand[
    Indexed[IndexType, FailureKindValue], Multiple[OrderType]
]
BareFailuresOperand: TypeAlias = Operand[Bare[FailureValue], Multiple[OrderType]]
BareFailureKindsOperand: TypeAlias = Operand[
    Bare[FailureKindValue], Multiple[OrderType]
]
FailureOperand: TypeAlias = Operand[Indexed[IndexType, FailureValue], Single]
FailureKindOperand: TypeAlias = Operand[Indexed[IndexType, FailureKindValue], Single]
BareFailureOperand: TypeAlias = Operand[Bare[FailureValue], Single]
BareFailureKindOperand: TypeAlias = Operand[Bare[FailureKindValue], Single]
DefiniteFailureOperand: TypeAlias = Operand[Indexed[IndexType, FailureValue], Definite]
DefiniteFailureKindOperand: TypeAlias = Operand[
    Indexed[IndexType, FailureKindValue], Definite
]
DefiniteBareFailureOperand: TypeAlias = Operand[Bare[FailureValue], Definite]
DefiniteBareFailureKindOperand: TypeAlias = Operand[Bare[FailureKindValue], Definite]

IndicesOperand: TypeAlias = Operand[
    Indexed[IndexType, IndexValue[IndexType]], Multiple[OrderType]
]
BareIndicesOperand: TypeAlias = Operand[
    Bare[IndexValue[IndexType]], Multiple[OrderType]
]
IndexOperand: TypeAlias = Operand[Indexed[IndexType, IndexValue[IndexType]], Single]
BareIndexOperand: TypeAlias = Operand[Bare[IndexValue[IndexType]], Single]
DefiniteIndexOperand: TypeAlias = Operand[
    Indexed[IndexType, IndexValue[IndexType]], Definite
]
DefiniteBareIndexOperand: TypeAlias = Operand[Bare[IndexValue[IndexType]], Definite]

ReferencesOperand: TypeAlias = Operand[
    Indexed[IndexType, ReferenceType], Multiple[OrderType]
]
BareReferencesOperand: TypeAlias = Operand[Bare[ReferenceType], Multiple[OrderType]]
ReferenceOperand: TypeAlias = Operand[Indexed[IndexType, ReferenceType], Single]
BareReferenceOperand: TypeAlias = Operand[Bare[ReferenceType], Single]
DefiniteReferenceOperand: TypeAlias = Operand[
    Indexed[IndexType, ReferenceType], Definite
]
DefiniteBareReferenceOperand: TypeAlias = Operand[Bare[ReferenceType], Definite]
ReferenceIndicesOperand: TypeAlias = Operand[
    Indexed[IndexType, IndexValue[EntityType]], Multiple[OrderType]
]
ReferenceIndexOperand: TypeAlias = Operand[
    Indexed[IndexType, IndexValue[EntityType]], Single
]
DefiniteReferenceIndexOperand: TypeAlias = Operand[
    Indexed[IndexType, IndexValue[EntityType]], Definite
]

ValuesOperand: TypeAlias = Operand[Indexed[IndexType, Scalar], Multiple[OrderType]]
BareValuesOperand: TypeAlias = Operand[Bare[Scalar], Multiple[OrderType]]
ValueOperand: TypeAlias = Operand[Indexed[IndexType, Scalar], Single]
BareValueOperand: TypeAlias = Operand[Bare[Scalar], Single]
DefiniteValueOperand: TypeAlias = Operand[Indexed[IndexType, Scalar], Definite]
DefiniteBareValueOperand: TypeAlias = Operand[Bare[Scalar], Definite]

NodeAttributesOperand: TypeAlias = AttributesOperand[NodeIndex, Unordered]
OrderedNodeAttributesOperand: TypeAlias = AttributesOperand[NodeIndex, Ordered]
NodeAttributeOperand: TypeAlias = AttributeOperand[NodeIndex]
DefiniteNodeAttributeOperand: TypeAlias = DefiniteAttributeOperand[NodeIndex]
EdgeAttributesOperand: TypeAlias = AttributesOperand[EdgeIndex, Unordered]
OrderedEdgeAttributesOperand: TypeAlias = AttributesOperand[EdgeIndex, Ordered]
EdgeAttributeOperand: TypeAlias = AttributeOperand[EdgeIndex]
DefiniteEdgeAttributeOperand: TypeAlias = DefiniteAttributeOperand[EdgeIndex]

NodeAttributesTreeOperand: TypeAlias = Operand[
    Indexed[
        Expanded[
            NodeIndex, AttributeNameIndex, Tuple[NodeIndexPayload, Optional[Attribute]]
        ],
        AttributeName,
    ],
    Multiple[Unordered],
]
EdgeAttributesTreeOperand: TypeAlias = Operand[
    Indexed[
        Expanded[
            EdgeIndex, AttributeNameIndex, Tuple[EdgeIndexPayload, Optional[Attribute]]
        ],
        AttributeName,
    ],
    Multiple[Unordered],
]

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


@dataclass(frozen=True, slots=True, repr=False)
class GroupKeyFailure(Generic[MemberIndexType]):
    _member: Any
    error: QueryError

    @property
    def member(
        self: GroupKeyFailure[Index[MemberPayloadType]],
    ) -> MemberPayloadType:
        return self._member

    def __repr__(self) -> str:
        return f"GroupKeyFailure(member={self._member!r}, error={self.error!r})"


@dataclass(frozen=True, slots=True, repr=False)
class GroupBucket(Generic[MemberIndexType, KeyIndexType, BucketPayloadType]):
    _key: Any
    _members: Any
    payload: Union[BucketPayloadType, QueryError]

    @property
    def key(
        self: GroupBucket[MemberIndexType, Index[KeyPayloadType], BucketPayloadType],
    ) -> KeyPayloadType:
        return self._key

    @property
    def members(
        self: GroupBucket[Index[MemberPayloadType], KeyIndexType, BucketPayloadType],
    ) -> List[MemberPayloadType]:
        return self._members

    def __repr__(self) -> str:
        return (
            f"GroupBucket(key={self._key!r}, members={self._members!r}, "
            f"payload={self.payload!r})"
        )


@dataclass(frozen=True, slots=True, repr=False)
class GroupResult(Generic[LeafType, Unpack[Levels]]):
    _buckets: Any
    _key_failures: Any

    @overload
    def buckets(
        self: GroupResult[LeafType, Grouped[MemberIndexType, KeyIndexType]],
    ) -> List[GroupBucket[MemberIndexType, KeyIndexType, LeafType]]: ...

    @overload
    def buckets(
        self: GroupResult[
            LeafType,
            Grouped[MemberIndexType, KeyIndexType],
            Grouped[InnerMemberIndexType, InnerKeyIndexType],
            Unpack[InnerLevels],
        ],
    ) -> List[
        GroupBucket[
            MemberIndexType,
            KeyIndexType,
            GroupResult[
                LeafType,
                Grouped[InnerMemberIndexType, InnerKeyIndexType],
                Unpack[InnerLevels],
            ],
        ]
    ]: ...

    def buckets(self) -> List[Any]:
        return self._buckets

    def __repr__(self) -> str:
        return (
            f"GroupResult(buckets={self._buckets!r}, "
            f"key_failures={self._key_failures!r})"
        )

    def key_failures(
        self: GroupResult[
            LeafType, Grouped[MemberIndexType, KeyIndexType], Unpack[InnerLevels]
        ],
    ) -> List[GroupKeyFailure[MemberIndexType]]:
        return self._key_failures

    @staticmethod
    def _from_buckets(
        buckets: List[Tuple[IndexPayload, List[IndexPayload], object]],
        key_failures: List[Tuple[IndexPayload, QueryError]],
        group_depth: int,
    ) -> GroupResult[Any, Unpack[Tuple[Any, ...]]]:
        return GroupResult(
            [
                GroupBucket(
                    _key=key,
                    _members=members,
                    payload=GroupResult._from_terminal(payload, group_depth - 1),
                )
                for key, members, payload in buckets
            ],
            [
                GroupKeyFailure(_member=member, error=error)
                for member, error in key_failures
            ],
        )

    @staticmethod
    def _from_terminal(payload: object, group_depth: int) -> object:
        if group_depth == 0 or not isinstance(payload, tuple):
            return payload

        return GroupResult._from_buckets(payload[0], payload[1], group_depth)


MembershipResult: TypeAlias = List[Union[IndexPayloadType, QueryError]]
MembershipSingleResult: TypeAlias = Optional[Union[IndexPayloadType, QueryError]]
MembershipDefiniteResult: TypeAlias = Union[IndexPayloadType, QueryError]
IndexedResult: TypeAlias = List[Tuple[IndexPayloadType, Union[PayloadType, QueryError]]]
IndexedSingleResult: TypeAlias = Optional[
    Tuple[IndexPayloadType, Union[PayloadType, QueryError]]
]
IndexedDefiniteResult: TypeAlias = Tuple[
    IndexPayloadType, Union[PayloadType, QueryError]
]
BareResult: TypeAlias = List[Union[PayloadType, QueryError]]
BareSingleResult: TypeAlias = Optional[Union[PayloadType, QueryError]]
BareDefiniteResult: TypeAlias = Union[PayloadType, QueryError]

ValueResult: TypeAlias = IndexedResult[IndexPayloadType, ScalarValue]
AttributeResult: TypeAlias = IndexedResult[IndexPayloadType, Attribute]
MaskResult: TypeAlias = IndexedResult[IndexPayloadType, bool]
FailureResult: TypeAlias = IndexedResult[IndexPayloadType, QueryError]
FailureKindResult: TypeAlias = IndexedResult[IndexPayloadType, FailureKind]
IndexedScalarSingleResult: TypeAlias = IndexedSingleResult[
    IndexPayloadType, ScalarValue
]
IndexedScalarDefiniteResult: TypeAlias = IndexedDefiniteResult[
    IndexPayloadType, ScalarValue
]
BareScalarMultipleResult: TypeAlias = BareResult[ScalarValue]
BareAttributeMultipleResult: TypeAlias = BareResult[Attribute]
BareMaskMultipleResult: TypeAlias = BareResult[bool]
BareFailureMultipleResult: TypeAlias = BareResult[QueryError]
BareFailureKindMultipleResult: TypeAlias = BareResult[FailureKind]
BareScalarSingleResult: TypeAlias = BareSingleResult[ScalarValue]
BareAttributeSingleResult: TypeAlias = BareSingleResult[Attribute]
BareScalarDefiniteResult: TypeAlias = BareDefiniteResult[ScalarValue]
BareMaskDefiniteResult: TypeAlias = BareDefiniteResult[bool]


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[Indexed[Index[IndexPayloadType], Unit], Multiple[OrderType]],
    ],
) -> MembershipResult[IndexPayloadType]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[
            Indexed[Index[IndexPayloadType], Unit],
            Multiple[OrderType],
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[Levels],
        ],
    ],
) -> GroupResult[
    MembershipResult[IndexPayloadType],
    Grouped[MemberIndexType, KeyIndexType],
    Unpack[Levels],
]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[Indexed[Index[IndexPayloadType], Unit], Single],
    ],
) -> MembershipSingleResult[IndexPayloadType]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[
            Indexed[Index[IndexPayloadType], Unit],
            Single,
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[Levels],
        ],
    ],
) -> GroupResult[
    MembershipSingleResult[IndexPayloadType],
    Grouped[MemberIndexType, KeyIndexType],
    Unpack[Levels],
]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[Indexed[Index[IndexPayloadType], Unit], Definite],
    ],
) -> MembershipDefiniteResult[IndexPayloadType]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[
            Indexed[Index[IndexPayloadType], Unit],
            Definite,
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[Levels],
        ],
    ],
) -> GroupResult[
    MembershipDefiniteResult[IndexPayloadType],
    Grouped[MemberIndexType, KeyIndexType],
    Unpack[Levels],
]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[
            Indexed[Index[LaneIndexPayloadType], IndexValue[Index[IndexPayloadType]]],
            Multiple[OrderType],
        ],
    ],
) -> IndexedResult[LaneIndexPayloadType, IndexPayloadType]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[
            Indexed[Index[LaneIndexPayloadType], IndexValue[Index[IndexPayloadType]]],
            Multiple[OrderType],
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[Levels],
        ],
    ],
) -> GroupResult[
    IndexedResult[LaneIndexPayloadType, IndexPayloadType],
    Grouped[MemberIndexType, KeyIndexType],
    Unpack[Levels],
]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[
            Indexed[Index[LaneIndexPayloadType], IndexValue[Index[IndexPayloadType]]],
            Single,
        ],
    ],
) -> IndexedSingleResult[LaneIndexPayloadType, IndexPayloadType]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[
            Indexed[Index[LaneIndexPayloadType], IndexValue[Index[IndexPayloadType]]],
            Single,
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[Levels],
        ],
    ],
) -> GroupResult[
    IndexedSingleResult[LaneIndexPayloadType, IndexPayloadType],
    Grouped[MemberIndexType, KeyIndexType],
    Unpack[Levels],
]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[
            Indexed[Index[LaneIndexPayloadType], IndexValue[Index[IndexPayloadType]]],
            Definite,
        ],
    ],
) -> IndexedDefiniteResult[LaneIndexPayloadType, IndexPayloadType]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[
            Indexed[Index[LaneIndexPayloadType], IndexValue[Index[IndexPayloadType]]],
            Definite,
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[Levels],
        ],
    ],
) -> GroupResult[
    IndexedDefiniteResult[LaneIndexPayloadType, IndexPayloadType],
    Grouped[MemberIndexType, KeyIndexType],
    Unpack[Levels],
]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[
            Indexed[Index[LaneIndexPayloadType], ReturnValue[PayloadType]],
            Multiple[OrderType],
        ],
    ],
) -> IndexedResult[LaneIndexPayloadType, PayloadType]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[
            Indexed[Index[LaneIndexPayloadType], ReturnValue[PayloadType]],
            Multiple[OrderType],
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[Levels],
        ],
    ],
) -> GroupResult[
    IndexedResult[LaneIndexPayloadType, PayloadType],
    Grouped[MemberIndexType, KeyIndexType],
    Unpack[Levels],
]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[Indexed[Index[LaneIndexPayloadType], ReturnValue[PayloadType]], Single],
    ],
) -> IndexedSingleResult[LaneIndexPayloadType, PayloadType]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[
            Indexed[Index[LaneIndexPayloadType], ReturnValue[PayloadType]],
            Single,
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[Levels],
        ],
    ],
) -> GroupResult[
    IndexedSingleResult[LaneIndexPayloadType, PayloadType],
    Grouped[MemberIndexType, KeyIndexType],
    Unpack[Levels],
]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[
            Indexed[Index[LaneIndexPayloadType], ReturnValue[PayloadType]], Definite
        ],
    ],
) -> IndexedDefiniteResult[LaneIndexPayloadType, PayloadType]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[
            Indexed[Index[LaneIndexPayloadType], ReturnValue[PayloadType]],
            Definite,
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[Levels],
        ],
    ],
) -> GroupResult[
    IndexedDefiniteResult[LaneIndexPayloadType, PayloadType],
    Grouped[MemberIndexType, KeyIndexType],
    Unpack[Levels],
]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[Bare[IndexValue[Index[IndexPayloadType]]], Multiple[OrderType]],
    ],
) -> BareResult[IndexPayloadType]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[
            Bare[IndexValue[Index[IndexPayloadType]]],
            Multiple[OrderType],
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[Levels],
        ],
    ],
) -> GroupResult[
    BareResult[IndexPayloadType], Grouped[MemberIndexType, KeyIndexType], Unpack[Levels]
]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[Bare[IndexValue[Index[IndexPayloadType]]], Single],
    ],
) -> BareSingleResult[IndexPayloadType]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[
            Bare[IndexValue[Index[IndexPayloadType]]],
            Single,
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[Levels],
        ],
    ],
) -> GroupResult[
    BareSingleResult[IndexPayloadType],
    Grouped[MemberIndexType, KeyIndexType],
    Unpack[Levels],
]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[Bare[IndexValue[Index[IndexPayloadType]]], Definite],
    ],
) -> BareDefiniteResult[IndexPayloadType]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[
            Bare[IndexValue[Index[IndexPayloadType]]],
            Definite,
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[Levels],
        ],
    ],
) -> GroupResult[
    BareDefiniteResult[IndexPayloadType],
    Grouped[MemberIndexType, KeyIndexType],
    Unpack[Levels],
]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[Bare[ReturnValue[PayloadType]], Multiple[OrderType]],
    ],
) -> BareResult[PayloadType]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[
            Bare[ReturnValue[PayloadType]],
            Multiple[OrderType],
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[Levels],
        ],
    ],
) -> GroupResult[
    BareResult[PayloadType], Grouped[MemberIndexType, KeyIndexType], Unpack[Levels]
]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[Bare[ReturnValue[PayloadType]], Single],
    ],
) -> BareSingleResult[PayloadType]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[
            Bare[ReturnValue[PayloadType]],
            Single,
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[Levels],
        ],
    ],
) -> GroupResult[
    BareSingleResult[PayloadType],
    Grouped[MemberIndexType, KeyIndexType],
    Unpack[Levels],
]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[Bare[ReturnValue[PayloadType]], Definite],
    ],
) -> BareDefiniteResult[PayloadType]: ...


@overload
def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[
        [NodesOperand],
        Operand[
            Bare[ReturnValue[PayloadType]],
            Definite,
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[Levels],
        ],
    ],
) -> GroupResult[
    BareDefiniteResult[PayloadType],
    Grouped[MemberIndexType, KeyIndexType],
    Unpack[Levels],
]: ...


def query_nodes(
    graphrecord: GraphRecord,
    query: Callable[[NodesOperand], Operand[Any, Any, Unpack[Tuple[Any, ...]]]],
) -> object:
    group_depth = 0

    def adapter(operand: PyOperand) -> PyOperand:
        nonlocal group_depth
        returned = query(Operand._from_py_operand(operand))._operand
        group_depth = returned.group_depth

        return returned

    terminal = graphrecord._graphrecord.query_nodes(adapter)

    if group_depth == 0:
        return terminal

    return GroupResult._from_terminal(terminal, group_depth)


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[Indexed[Index[IndexPayloadType], Unit], Multiple[OrderType]],
    ],
) -> MembershipResult[IndexPayloadType]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[
            Indexed[Index[IndexPayloadType], Unit],
            Multiple[OrderType],
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[Levels],
        ],
    ],
) -> GroupResult[
    MembershipResult[IndexPayloadType],
    Grouped[MemberIndexType, KeyIndexType],
    Unpack[Levels],
]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[Indexed[Index[IndexPayloadType], Unit], Single],
    ],
) -> MembershipSingleResult[IndexPayloadType]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[
            Indexed[Index[IndexPayloadType], Unit],
            Single,
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[Levels],
        ],
    ],
) -> GroupResult[
    MembershipSingleResult[IndexPayloadType],
    Grouped[MemberIndexType, KeyIndexType],
    Unpack[Levels],
]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[Indexed[Index[IndexPayloadType], Unit], Definite],
    ],
) -> MembershipDefiniteResult[IndexPayloadType]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[
            Indexed[Index[IndexPayloadType], Unit],
            Definite,
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[Levels],
        ],
    ],
) -> GroupResult[
    MembershipDefiniteResult[IndexPayloadType],
    Grouped[MemberIndexType, KeyIndexType],
    Unpack[Levels],
]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[
            Indexed[Index[LaneIndexPayloadType], IndexValue[Index[IndexPayloadType]]],
            Multiple[OrderType],
        ],
    ],
) -> IndexedResult[LaneIndexPayloadType, IndexPayloadType]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[
            Indexed[Index[LaneIndexPayloadType], IndexValue[Index[IndexPayloadType]]],
            Multiple[OrderType],
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[Levels],
        ],
    ],
) -> GroupResult[
    IndexedResult[LaneIndexPayloadType, IndexPayloadType],
    Grouped[MemberIndexType, KeyIndexType],
    Unpack[Levels],
]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[
            Indexed[Index[LaneIndexPayloadType], IndexValue[Index[IndexPayloadType]]],
            Single,
        ],
    ],
) -> IndexedSingleResult[LaneIndexPayloadType, IndexPayloadType]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[
            Indexed[Index[LaneIndexPayloadType], IndexValue[Index[IndexPayloadType]]],
            Single,
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[Levels],
        ],
    ],
) -> GroupResult[
    IndexedSingleResult[LaneIndexPayloadType, IndexPayloadType],
    Grouped[MemberIndexType, KeyIndexType],
    Unpack[Levels],
]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[
            Indexed[Index[LaneIndexPayloadType], IndexValue[Index[IndexPayloadType]]],
            Definite,
        ],
    ],
) -> IndexedDefiniteResult[LaneIndexPayloadType, IndexPayloadType]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[
            Indexed[Index[LaneIndexPayloadType], IndexValue[Index[IndexPayloadType]]],
            Definite,
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[Levels],
        ],
    ],
) -> GroupResult[
    IndexedDefiniteResult[LaneIndexPayloadType, IndexPayloadType],
    Grouped[MemberIndexType, KeyIndexType],
    Unpack[Levels],
]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[
            Indexed[Index[LaneIndexPayloadType], ReturnValue[PayloadType]],
            Multiple[OrderType],
        ],
    ],
) -> IndexedResult[LaneIndexPayloadType, PayloadType]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[
            Indexed[Index[LaneIndexPayloadType], ReturnValue[PayloadType]],
            Multiple[OrderType],
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[Levels],
        ],
    ],
) -> GroupResult[
    IndexedResult[LaneIndexPayloadType, PayloadType],
    Grouped[MemberIndexType, KeyIndexType],
    Unpack[Levels],
]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[Indexed[Index[LaneIndexPayloadType], ReturnValue[PayloadType]], Single],
    ],
) -> IndexedSingleResult[LaneIndexPayloadType, PayloadType]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[
            Indexed[Index[LaneIndexPayloadType], ReturnValue[PayloadType]],
            Single,
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[Levels],
        ],
    ],
) -> GroupResult[
    IndexedSingleResult[LaneIndexPayloadType, PayloadType],
    Grouped[MemberIndexType, KeyIndexType],
    Unpack[Levels],
]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[
            Indexed[Index[LaneIndexPayloadType], ReturnValue[PayloadType]], Definite
        ],
    ],
) -> IndexedDefiniteResult[LaneIndexPayloadType, PayloadType]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[
            Indexed[Index[LaneIndexPayloadType], ReturnValue[PayloadType]],
            Definite,
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[Levels],
        ],
    ],
) -> GroupResult[
    IndexedDefiniteResult[LaneIndexPayloadType, PayloadType],
    Grouped[MemberIndexType, KeyIndexType],
    Unpack[Levels],
]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[Bare[IndexValue[Index[IndexPayloadType]]], Multiple[OrderType]],
    ],
) -> BareResult[IndexPayloadType]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[
            Bare[IndexValue[Index[IndexPayloadType]]],
            Multiple[OrderType],
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[Levels],
        ],
    ],
) -> GroupResult[
    BareResult[IndexPayloadType], Grouped[MemberIndexType, KeyIndexType], Unpack[Levels]
]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[Bare[IndexValue[Index[IndexPayloadType]]], Single],
    ],
) -> BareSingleResult[IndexPayloadType]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[
            Bare[IndexValue[Index[IndexPayloadType]]],
            Single,
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[Levels],
        ],
    ],
) -> GroupResult[
    BareSingleResult[IndexPayloadType],
    Grouped[MemberIndexType, KeyIndexType],
    Unpack[Levels],
]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[Bare[IndexValue[Index[IndexPayloadType]]], Definite],
    ],
) -> BareDefiniteResult[IndexPayloadType]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[
            Bare[IndexValue[Index[IndexPayloadType]]],
            Definite,
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[Levels],
        ],
    ],
) -> GroupResult[
    BareDefiniteResult[IndexPayloadType],
    Grouped[MemberIndexType, KeyIndexType],
    Unpack[Levels],
]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[Bare[ReturnValue[PayloadType]], Multiple[OrderType]],
    ],
) -> BareResult[PayloadType]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[
            Bare[ReturnValue[PayloadType]],
            Multiple[OrderType],
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[Levels],
        ],
    ],
) -> GroupResult[
    BareResult[PayloadType], Grouped[MemberIndexType, KeyIndexType], Unpack[Levels]
]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[Bare[ReturnValue[PayloadType]], Single],
    ],
) -> BareSingleResult[PayloadType]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[
            Bare[ReturnValue[PayloadType]],
            Single,
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[Levels],
        ],
    ],
) -> GroupResult[
    BareSingleResult[PayloadType],
    Grouped[MemberIndexType, KeyIndexType],
    Unpack[Levels],
]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[Bare[ReturnValue[PayloadType]], Definite],
    ],
) -> BareDefiniteResult[PayloadType]: ...


@overload
def query_edges(
    graphrecord: GraphRecord,
    query: Callable[
        [EdgesOperand],
        Operand[
            Bare[ReturnValue[PayloadType]],
            Definite,
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[Levels],
        ],
    ],
) -> GroupResult[
    BareDefiniteResult[PayloadType],
    Grouped[MemberIndexType, KeyIndexType],
    Unpack[Levels],
]: ...


def query_edges(
    graphrecord: GraphRecord,
    query: Callable[[EdgesOperand], Operand[Any, Any, Unpack[Tuple[Any, ...]]]],
) -> object:
    group_depth = 0

    def adapter(operand: PyOperand) -> PyOperand:
        nonlocal group_depth
        returned = query(Operand._from_py_operand(operand))._operand
        group_depth = returned.group_depth

        return returned

    terminal = graphrecord._graphrecord.query_edges(adapter)

    if group_depth == 0:
        return terminal

    return GroupResult._from_terminal(terminal, group_depth)
