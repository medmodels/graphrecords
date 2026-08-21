# ruff: noqa: D100, D101, D102, D103, D105, D107
from __future__ import annotations

from enum import Enum, auto
from typing import (
    Any,
    ClassVar,
    Generic,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    TypeAlias,
    Union,
    cast,
    overload,
)

from typing_extensions import TypeVar, TypeVarTuple, Unpack

from graphrecords._graphrecords.graphrecord import PyEdgeIndex
from graphrecords._graphrecords.querying import (
    ArgumentMissingError as ArgumentMissingError,
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
    MissingGroupBucketError as MissingGroupBucketError,
)
from graphrecords._graphrecords.querying import (
    MissingTraversedAttributeError as MissingTraversedAttributeError,
)
from graphrecords._graphrecords.querying import (
    ModuloByZeroError as ModuloByZeroError,
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
    PyEdgeEndpointRole,
    PyExpression,
    PyFailureKind,
    PyGroupedResult,
    PyResultView,
    PySeries,
    PyValueTarget,
)
from graphrecords._graphrecords.querying import (
    QueryError as QueryError,
)
from graphrecords._graphrecords.querying import (
    RaisedFailuresError as RaisedFailuresError,
)
from graphrecords._graphrecords.querying import (
    ResultConsumedError as ResultConsumedError,
)
from graphrecords._graphrecords.querying import (
    StringLengthOverflowError as StringLengthOverflowError,
)
from graphrecords._graphrecords.querying import (
    StringPaddingOverflowError as StringPaddingOverflowError,
)
from graphrecords._graphrecords.querying import (
    UncoveredIndicesError as UncoveredIndicesError,
)
from graphrecords._graphrecords.querying import (
    UnresolvedBucketFailuresError as UnresolvedBucketFailuresError,
)
from graphrecords._graphrecords.querying import (
    UnresolvedGroupKeyFailuresError as UnresolvedGroupKeyFailuresError,
)
from graphrecords._graphrecords.querying import (
    UnresolvedIndexError as UnresolvedIndexError,
)
from graphrecords._graphrecords.querying import (
    UnsupportedValueRoleError as UnsupportedValueRoleError,
)
from graphrecords._graphrecords.querying import (
    edges as py_edges,
)
from graphrecords._graphrecords.querying import (
    groups as py_groups,
)
from graphrecords._graphrecords.querying import (
    nodes as py_nodes,
)
from graphrecords.types import (
    AttributeName as Attribute,
)
from graphrecords.types import EdgeDirection
from graphrecords.types import (
    EdgeIndex as EdgeIndexPayload,
)
from graphrecords.types import (
    GroupIndex as GroupIndexPayload,
)
from graphrecords.types import (
    NodeIndex as NodeIndexPayload,
)
from graphrecords.types import (
    Value as ValuePayload,
)


class EdgeEndpointRole(Enum):
    Source = auto()
    Target = auto()

    @staticmethod
    def _from_py_edge_endpoint_role(py_role: PyEdgeEndpointRole) -> EdgeEndpointRole:
        if py_role == PyEdgeEndpointRole.Source:
            return EdgeEndpointRole.Source
        if py_role == PyEdgeEndpointRole.Target:
            return EdgeEndpointRole.Target
        msg = "Should never be reached"
        raise NotImplementedError(msg)

    def _into_py_edge_endpoint_role(self) -> PyEdgeEndpointRole:
        if self == EdgeEndpointRole.Source:
            return PyEdgeEndpointRole.Source
        if self == EdgeEndpointRole.Target:
            return PyEdgeEndpointRole.Target
        msg = "Should never be reached"
        raise NotImplementedError(msg)


class FailureKind:
    _py_failure_kind: PyFailureKind

    @classmethod
    def _from_py_failure_kind(cls, py_kind: PyFailureKind) -> FailureKind:
        kind = cls.__new__(cls)
        kind._py_failure_kind = py_kind

        return kind

    @property
    def name(self) -> str:
        return self._py_failure_kind.name

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FailureKind):
            return NotImplemented

        return self._py_failure_kind == other._py_failure_kind

    def __hash__(self) -> int:
        return hash(self._py_failure_kind)

    def __repr__(self) -> str:
        return f"FailureKind.{self.name}"


ScalarValue: TypeAlias = ValuePayload
_BooleanValue: TypeAlias = bool
IndexPayload: TypeAlias = Union[
    ValuePayload,
    EdgeIndexPayload,
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


class GroupIndex(Index[GroupIndexPayload]): ...


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
    GroupIndex,
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


class NodeReference(ReturnValue[NodeIndexPayload]): ...


class EdgeReference(ReturnValue[EdgeIndexPayload]): ...


class GroupReference(ReturnValue[GroupIndexPayload]): ...


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


class Binding: ...


class Unbound(Binding): ...


class Bound(Binding): ...


BindingType = TypeVar("BindingType", bound=Binding, default=Any)
S = TypeVar("S", bound=Shape, covariant=True, default=Any)
C = TypeVar("C", bound=Container, default=Any)
IndexType = TypeVar("IndexType", bound=Index[IndexPayload])
ValueIndexType = TypeVar("ValueIndexType", bound=Index[IndexPayload])
ContainerType = TypeVar("ContainerType", bound=Container)
PopulationContainerType = TypeVar("PopulationContainerType", bound=Container)
MemberIndexType = TypeVar("MemberIndexType", bound=Index[IndexPayload], covariant=True)
KeyIndexType = TypeVar("KeyIndexType", bound=Index[IndexPayload], covariant=True)
PopulationIndexType = TypeVar("PopulationIndexType", bound=Index[IndexPayload])
PopulationOrderType = TypeVar("PopulationOrderType")
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
ElementType = TypeVar("ElementType", covariant=True, default=Any)
LeafType = TypeVar("LeafType")
Levels = TypeVarTuple("Levels", default=Unpack[Tuple[()]])
InnerLevels = TypeVarTuple("InnerLevels")
OuterLevels = TypeVarTuple("OuterLevels")
TemplateValueType = TypeVar("TemplateValueType", bound=Value)
InheritedValueType = TypeVar("InheritedValueType", bound=ReturnValue[object])
TransitionValueType = TypeVar("TransitionValueType", bound=Value)
EntityType = TypeVar("EntityType", NodeIndex, EdgeIndex)
IntegerIndexType = TypeVar("IntegerIndexType", EdgeIndex, Positional)
BareValueType = TypeVar("BareValueType", bound=ReturnValue[object])
ReferenceType = TypeVar("ReferenceType", NodeReference, EdgeReference, GroupReference)
EntityReferenceType = TypeVar("EntityReferenceType", NodeReference, EdgeReference)
RetentionType = TypeVar("RetentionType", bound=Retention, default=Any)
ArgumentBindingType = TypeVar("ArgumentBindingType", bound=Binding)
ArgumentOrderType = TypeVar("ArgumentOrderType")
ReplacementType = TypeVar("ReplacementType", covariant=True)
ReplaceableValueType = TypeVar("ReplaceableValueType", bound=ReturnValue[object])
ScalarValueType = TypeVar("ScalarValueType", bound="ScalarValue")
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
    IndexValue[GroupIndex],
    IndexValue[ValueIndex],
    IndexValue[AttributeNameIndex],
)
StringArgumentValueType = TypeVar(
    "StringArgumentValueType",
    Scalar,
    AttributeName,
    IndexValue[NodeIndex],
    IndexValue[GroupIndex],
    IndexValue[ValueIndex],
    IndexValue[AttributeNameIndex],
)
OldStringValueType = TypeVar(
    "OldStringValueType",
    Scalar,
    AttributeName,
    IndexValue[NodeIndex],
    IndexValue[GroupIndex],
    IndexValue[ValueIndex],
    IndexValue[AttributeNameIndex],
)
NewStringValueType = TypeVar(
    "NewStringValueType",
    Scalar,
    AttributeName,
    IndexValue[NodeIndex],
    IndexValue[GroupIndex],
    IndexValue[ValueIndex],
    IndexValue[AttributeNameIndex],
)
IntegerValueType = TypeVar(
    "IntegerValueType",
    Scalar,
    AttributeName,
    IndexValue[NodeIndex],
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
    NodeReference,
    EdgeReference,
    GroupReference,
    IndexValue[NodeIndex],
    IndexValue[EdgeIndex],
    IndexValue[GroupIndex],
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
    IndexValue[GroupIndex],
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
    IndexValue[GroupIndex],
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
    IndexValue[GroupIndex],
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
    IndexValue[GroupIndex],
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
    IndexValue[GroupIndex],
    IndexValue[Positional],
    IndexValue[AttributeNameIndex],
    IndexValue[BoolIndex],
)
AttributeNameTransitionValueType = TypeVar(
    "AttributeNameTransitionValueType",
    Scalar,
    IndexValue[NodeIndex],
    IndexValue[GroupIndex],
    IndexValue[Positional],
    IndexValue[ValueIndex],
    IndexValue[AttributeNameIndex],
)
NodeIndexTransitionValueType = TypeVar(
    "NodeIndexTransitionValueType",
    Scalar,
    AttributeName,
    IndexValue[Positional],
    IndexValue[ValueIndex],
    IndexValue[AttributeNameIndex],
)
GroupIndexTransitionValueType = TypeVar(
    "GroupIndexTransitionValueType",
    Scalar,
    AttributeName,
    IndexValue[Positional],
    IndexValue[ValueIndex],
    IndexValue[AttributeNameIndex],
)
AttributeNameIndexTransitionValueType = TypeVar(
    "AttributeNameIndexTransitionValueType",
    Scalar,
    AttributeName,
    IndexValue[NodeIndex],
    IndexValue[GroupIndex],
    IndexValue[Positional],
    IndexValue[ValueIndex],
)
PositionalTransitionValueType = TypeVar(
    "PositionalTransitionValueType",
    Scalar,
    AttributeName,
    IndexValue[NodeIndex],
    IndexValue[GroupIndex],
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
    GroupIndex: ClassVar[ValueTarget[IndexValue[GroupIndex]]]
    PositionalIndex: ClassVar[ValueTarget[IndexValue[Positional]]]
    BoolIndex: ClassVar[ValueTarget[IndexValue[BoolIndex]]]
    Mask: ClassVar[ValueTarget[Mask]]
    FailureKind: ClassVar[ValueTarget[FailureKindValue]]
    FailureKindIndex: ClassVar[ValueTarget[IndexValue[FailureKindIndex]]]

    _py_value_target: PyValueTarget

    @classmethod
    def _from_py_value_target(cls, py_target: PyValueTarget) -> ValueTarget[Any]:
        target = cls.__new__(cls)
        target._py_value_target = py_target

        return target


ValueTarget.Value = ValueTarget._from_py_value_target(PyValueTarget.Value)
ValueTarget.ValueIndex = ValueTarget._from_py_value_target(PyValueTarget.ValueIndex)
ValueTarget.AttributeName = ValueTarget._from_py_value_target(
    PyValueTarget.AttributeName
)
ValueTarget.AttributeNameIndex = ValueTarget._from_py_value_target(
    PyValueTarget.AttributeNameIndex
)
ValueTarget.NodeIndex = ValueTarget._from_py_value_target(PyValueTarget.NodeIndex)
ValueTarget.GroupIndex = ValueTarget._from_py_value_target(PyValueTarget.GroupIndex)
ValueTarget.PositionalIndex = ValueTarget._from_py_value_target(
    PyValueTarget.PositionalIndex
)
ValueTarget.BoolIndex = ValueTarget._from_py_value_target(PyValueTarget.BoolIndex)
ValueTarget.Mask = ValueTarget._from_py_value_target(PyValueTarget.Mask)
ValueTarget.FailureKind = ValueTarget._from_py_value_target(PyValueTarget.FailureKind)
ValueTarget.FailureKindIndex = ValueTarget._from_py_value_target(
    PyValueTarget.FailureKindIndex
)


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

    _py_cast_target: PyCastTarget

    @classmethod
    def _from_py_cast_target(cls, py_target: PyCastTarget) -> CastTarget[Any]:
        target = cls.__new__(cls)
        target._py_cast_target = py_target

        return target


CastTarget.Bool = CastTarget._from_py_cast_target(PyCastTarget.Bool)
CastTarget.DateTime = CastTarget._from_py_cast_target(PyCastTarget.DateTime)
CastTarget.Duration = CastTarget._from_py_cast_target(PyCastTarget.Duration)
CastTarget.Float = CastTarget._from_py_cast_target(PyCastTarget.Float)
CastTarget.Int = CastTarget._from_py_cast_target(PyCastTarget.Int)
CastTarget.String = CastTarget._from_py_cast_target(PyCastTarget.String)


class Policy: ...


class Drop(Policy): ...


class Raise(Policy):
    @staticmethod
    def when(condition: BareMaskArgument) -> _RaiseWhen:
        return _RaiseWhen(condition)


class _RaiseWhen(Policy):
    def __init__(self, condition: BareMaskArgument) -> None:
        self._condition = condition


class Replace(Policy, Generic[ReplacementType]):
    def __init__(self, replacement: ReplacementType) -> None:
        self._replacement = replacement


class Argument(Generic[S, RetentionType]):
    _py_argument: PyArgument

    @classmethod
    def _from_py_argument(cls, py_argument: PyArgument) -> Argument[Any, Any]:
        argument = cls.__new__(cls)
        argument._py_argument = py_argument

        return argument


class Expression(Generic[BindingType, S, C, Unpack[Levels]]):
    _py_carrier: Union[PyExpression, PySeries]

    @classmethod
    def _from_py_expression(
        cls, py_expression: PyExpression
    ) -> Expression[BindingType, S, C, Unpack[Levels]]:
        expression = cls.__new__(cls)
        expression._py_carrier = py_expression

        return expression

    def _rebuild(self, py_carrier: Union[PyExpression, PySeries]) -> Any:  # noqa: ANN401
        wrapper = type(self)
        rebuilt = wrapper.__new__(wrapper)
        rebuilt._py_carrier = py_carrier

        return rebuilt

    @property
    def _py_expression(self) -> PyExpression:
        if isinstance(self._py_carrier, PyExpression):
            return self._py_carrier

        msg = "the expression must be free"
        raise TypeError(msg)

    @property
    def _py_series(self) -> PySeries:
        if isinstance(self._py_carrier, PySeries):
            return self._py_carrier

        msg = "the expression must be bound to a record"
        raise TypeError(msg)

    @overload
    @staticmethod
    def _to_argument(
        value: Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
    ) -> Union[PyExpression, PySeries]: ...

    @overload
    @staticmethod
    def _to_argument(value: Argument[Any, Any]) -> PyArgument: ...

    @overload
    @staticmethod
    def _to_argument(value: FailureKind) -> PyFailureKind: ...

    @overload
    @staticmethod
    def _to_argument(value: EdgeEndpointRole) -> PyEdgeEndpointRole: ...

    @overload
    @staticmethod
    def _to_argument(value: EdgeIndexPayload) -> PyEdgeIndex: ...

    @overload
    @staticmethod
    def _to_argument(value: ScalarValueType) -> ScalarValueType: ...

    @staticmethod
    def _to_argument(
        value: Union[
            ScalarValue,
            EdgeIndexPayload,
            FailureKind,
            EdgeEndpointRole,
            Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
    ) -> Union[
        ScalarValue,
        PyExpression,
        PySeries,
        PyArgument,
        PyFailureKind,
        PyEdgeIndex,
        PyEdgeEndpointRole,
    ]:
        if isinstance(value, Expression):
            return value._py_carrier

        if isinstance(value, Argument):
            return value._py_argument

        if isinstance(value, FailureKind):
            return value._py_failure_kind

        if isinstance(value, EdgeEndpointRole):
            return value._into_py_edge_endpoint_role()

        if isinstance(value, EdgeIndexPayload):
            return value._py_edge_index

        return value

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._py_carrier!r})"

    @overload
    def evaluate(
        self: Expression[
            Bound, Indexed[Index[IndexPayloadType], Unit], Multiple[OrderType]
        ],
    ) -> MembershipResult[IndexPayloadType]: ...

    @overload
    def evaluate(
        self: Expression[
            Bound,
            Indexed[Index[IndexPayloadType], Unit],
            Multiple[OrderType],
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[InnerLevels],
        ],
    ) -> GroupedResult[
        MembershipResult[IndexPayloadType],
        MemberIndexType,
        KeyIndexType,
        Unpack[InnerLevels],
    ]: ...

    @overload
    def evaluate(
        self: Expression[Bound, Indexed[Index[IndexPayloadType], Unit], Single],
    ) -> MembershipSingleResult[IndexPayloadType]: ...

    @overload
    def evaluate(
        self: Expression[
            Bound,
            Indexed[Index[IndexPayloadType], Unit],
            Single,
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[InnerLevels],
        ],
    ) -> GroupedResult[
        MembershipSingleResult[IndexPayloadType],
        MemberIndexType,
        KeyIndexType,
        Unpack[InnerLevels],
    ]: ...

    @overload
    def evaluate(
        self: Expression[Bound, Indexed[Index[IndexPayloadType], Unit], Definite],
    ) -> MembershipDefiniteResult[IndexPayloadType]: ...

    @overload
    def evaluate(
        self: Expression[
            Bound,
            Indexed[Index[IndexPayloadType], Unit],
            Definite,
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[InnerLevels],
        ],
    ) -> GroupedResult[
        MembershipDefiniteResult[IndexPayloadType],
        MemberIndexType,
        KeyIndexType,
        Unpack[InnerLevels],
    ]: ...

    @overload
    def evaluate(
        self: Expression[
            Bound,
            Indexed[Index[LaneIndexPayloadType], IndexValue[Index[IndexPayloadType]]],
            Multiple[OrderType],
        ],
    ) -> IndexedResult[LaneIndexPayloadType, IndexPayloadType]: ...

    @overload
    def evaluate(
        self: Expression[
            Bound,
            Indexed[Index[LaneIndexPayloadType], IndexValue[Index[IndexPayloadType]]],
            Multiple[OrderType],
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[InnerLevels],
        ],
    ) -> GroupedResult[
        IndexedResult[LaneIndexPayloadType, IndexPayloadType],
        MemberIndexType,
        KeyIndexType,
        Unpack[InnerLevels],
    ]: ...

    @overload
    def evaluate(
        self: Expression[
            Bound,
            Indexed[Index[LaneIndexPayloadType], IndexValue[Index[IndexPayloadType]]],
            Single,
        ],
    ) -> IndexedSingleResult[LaneIndexPayloadType, IndexPayloadType]: ...

    @overload
    def evaluate(
        self: Expression[
            Bound,
            Indexed[Index[LaneIndexPayloadType], IndexValue[Index[IndexPayloadType]]],
            Single,
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[InnerLevels],
        ],
    ) -> GroupedResult[
        IndexedSingleResult[LaneIndexPayloadType, IndexPayloadType],
        MemberIndexType,
        KeyIndexType,
        Unpack[InnerLevels],
    ]: ...

    @overload
    def evaluate(
        self: Expression[
            Bound,
            Indexed[Index[LaneIndexPayloadType], IndexValue[Index[IndexPayloadType]]],
            Definite,
        ],
    ) -> IndexedDefiniteResult[LaneIndexPayloadType, IndexPayloadType]: ...

    @overload
    def evaluate(
        self: Expression[
            Bound,
            Indexed[Index[LaneIndexPayloadType], IndexValue[Index[IndexPayloadType]]],
            Definite,
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[InnerLevels],
        ],
    ) -> GroupedResult[
        IndexedDefiniteResult[LaneIndexPayloadType, IndexPayloadType],
        MemberIndexType,
        KeyIndexType,
        Unpack[InnerLevels],
    ]: ...

    @overload
    def evaluate(
        self: Expression[
            Bound,
            Indexed[Index[LaneIndexPayloadType], ReturnValue[PayloadType]],
            Multiple[OrderType],
        ],
    ) -> IndexedResult[LaneIndexPayloadType, PayloadType]: ...

    @overload
    def evaluate(
        self: Expression[
            Bound,
            Indexed[Index[LaneIndexPayloadType], ReturnValue[PayloadType]],
            Multiple[OrderType],
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[InnerLevels],
        ],
    ) -> GroupedResult[
        IndexedResult[LaneIndexPayloadType, PayloadType],
        MemberIndexType,
        KeyIndexType,
        Unpack[InnerLevels],
    ]: ...

    @overload
    def evaluate(
        self: Expression[
            Bound,
            Indexed[Index[LaneIndexPayloadType], ReturnValue[PayloadType]],
            Single,
        ],
    ) -> IndexedSingleResult[LaneIndexPayloadType, PayloadType]: ...

    @overload
    def evaluate(
        self: Expression[
            Bound,
            Indexed[Index[LaneIndexPayloadType], ReturnValue[PayloadType]],
            Single,
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[InnerLevels],
        ],
    ) -> GroupedResult[
        IndexedSingleResult[LaneIndexPayloadType, PayloadType],
        MemberIndexType,
        KeyIndexType,
        Unpack[InnerLevels],
    ]: ...

    @overload
    def evaluate(
        self: Expression[
            Bound,
            Indexed[Index[LaneIndexPayloadType], ReturnValue[PayloadType]],
            Definite,
        ],
    ) -> IndexedDefiniteResult[LaneIndexPayloadType, PayloadType]: ...

    @overload
    def evaluate(
        self: Expression[
            Bound,
            Indexed[Index[LaneIndexPayloadType], ReturnValue[PayloadType]],
            Definite,
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[InnerLevels],
        ],
    ) -> GroupedResult[
        IndexedDefiniteResult[LaneIndexPayloadType, PayloadType],
        MemberIndexType,
        KeyIndexType,
        Unpack[InnerLevels],
    ]: ...

    @overload
    def evaluate(
        self: Expression[
            Bound, Bare[IndexValue[Index[IndexPayloadType]]], Multiple[OrderType]
        ],
    ) -> BareResult[IndexPayloadType]: ...

    @overload
    def evaluate(
        self: Expression[
            Bound,
            Bare[IndexValue[Index[IndexPayloadType]]],
            Multiple[OrderType],
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[InnerLevels],
        ],
    ) -> GroupedResult[
        BareResult[IndexPayloadType], MemberIndexType, KeyIndexType, Unpack[InnerLevels]
    ]: ...

    @overload
    def evaluate(
        self: Expression[Bound, Bare[IndexValue[Index[IndexPayloadType]]], Single],
    ) -> BareSingleResult[IndexPayloadType]: ...

    @overload
    def evaluate(
        self: Expression[
            Bound,
            Bare[IndexValue[Index[IndexPayloadType]]],
            Single,
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[InnerLevels],
        ],
    ) -> GroupedResult[
        BareSingleResult[IndexPayloadType],
        MemberIndexType,
        KeyIndexType,
        Unpack[InnerLevels],
    ]: ...

    @overload
    def evaluate(
        self: Expression[Bound, Bare[IndexValue[Index[IndexPayloadType]]], Definite],
    ) -> BareDefiniteResult[IndexPayloadType]: ...

    @overload
    def evaluate(
        self: Expression[
            Bound,
            Bare[IndexValue[Index[IndexPayloadType]]],
            Definite,
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[InnerLevels],
        ],
    ) -> GroupedResult[
        BareDefiniteResult[IndexPayloadType],
        MemberIndexType,
        KeyIndexType,
        Unpack[InnerLevels],
    ]: ...

    @overload
    def evaluate(
        self: Expression[Bound, Bare[ReturnValue[PayloadType]], Multiple[OrderType]],
    ) -> BareResult[PayloadType]: ...

    @overload
    def evaluate(
        self: Expression[
            Bound,
            Bare[ReturnValue[PayloadType]],
            Multiple[OrderType],
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[InnerLevels],
        ],
    ) -> GroupedResult[
        BareResult[PayloadType], MemberIndexType, KeyIndexType, Unpack[InnerLevels]
    ]: ...

    @overload
    def evaluate(
        self: Expression[Bound, Bare[ReturnValue[PayloadType]], Single],
    ) -> BareSingleResult[PayloadType]: ...

    @overload
    def evaluate(
        self: Expression[
            Bound,
            Bare[ReturnValue[PayloadType]],
            Single,
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[InnerLevels],
        ],
    ) -> GroupedResult[
        BareSingleResult[PayloadType],
        MemberIndexType,
        KeyIndexType,
        Unpack[InnerLevels],
    ]: ...

    @overload
    def evaluate(
        self: Expression[Bound, Bare[ReturnValue[PayloadType]], Definite],
    ) -> BareDefiniteResult[PayloadType]: ...

    @overload
    def evaluate(
        self: Expression[
            Bound,
            Bare[ReturnValue[PayloadType]],
            Definite,
            Grouped[MemberIndexType, KeyIndexType],
            Unpack[InnerLevels],
        ],
    ) -> GroupedResult[
        BareDefiniteResult[PayloadType],
        MemberIndexType,
        KeyIndexType,
        Unpack[InnerLevels],
    ]: ...

    def evaluate(self) -> object:
        terminal = self._py_series.evaluate()

        if isinstance(terminal, PyResultView):
            return ResultView._from_py_result_view(terminal)

        if isinstance(terminal, PyGroupedResult):
            return GroupedResult._from_py_grouped_result(terminal)

        return _Result._from_py_payload(terminal)

    def explain(self) -> str:
        return self._py_carrier.explain()

    def explain_unoptimized(
        self: Expression[Bound, S, C, Unpack[Levels]],
    ) -> str:
        return self._py_series.explain_unoptimized()

    @overload
    def on_missing(
        self: Expression[BindingType, Indexed[IndexType, V], Multiple[OrderType]],
        policy: Drop,
    ) -> Argument[Indexed[IndexType, V], Dropping]: ...

    @overload
    def on_missing(
        self: Expression[BindingType, Bare[BareValueType], Single], policy: Drop
    ) -> Argument[Bare[BareValueType], Dropping]: ...

    @overload
    def on_missing(
        self: Expression[BindingType, Indexed[IndexType, V], Multiple[OrderType]],
        policy: Union[
            Replace[
                Expression[
                    ArgumentBindingType,
                    Indexed[IndexType, V],
                    Multiple[ArgumentOrderType],
                ]
            ],
            Replace[IndexedExpressionArgument[IndexType, V, ArgumentOrderType]],
            Replace[IndexedDroppingArgument[IndexType, V]],
        ],
    ) -> Argument[Indexed[IndexType, V], Preserving]: ...

    @overload
    def on_missing(
        self: Expression[BindingType, Bare[BareValueType], Single],
        policy: BareReplacement[BareValueType],
    ) -> Argument[Bare[BareValueType], Preserving]: ...

    @overload
    def on_missing(
        self: Expression[BindingType, Indexed[IndexType, Scalar], Multiple[OrderType]],
        policy: Replace[ScalarValue],
    ) -> Argument[Indexed[IndexType, Scalar], Preserving]: ...

    @overload
    def on_missing(
        self: Expression[BindingType, Bare[Scalar], Single],
        policy: Replace[ScalarValue],
    ) -> Argument[Bare[Scalar], Preserving]: ...

    @overload
    def on_missing(
        self: Expression[BindingType, Indexed[IndexType, Mask], Multiple[OrderType]],
        policy: Replace[_BooleanValue],
    ) -> Argument[Indexed[IndexType, Mask], Preserving]: ...

    @overload
    def on_missing(
        self: Expression[BindingType, Bare[Mask], Single],
        policy: Replace[_BooleanValue],
    ) -> Argument[Bare[Mask], Preserving]: ...

    @overload
    def on_missing(
        self: Expression[
            BindingType, Indexed[IndexType, AttributeName], Multiple[OrderType]
        ],
        policy: Replace[Attribute],
    ) -> Argument[Indexed[IndexType, AttributeName], Preserving]: ...

    @overload
    def on_missing(
        self: Expression[BindingType, Bare[AttributeName], Single],
        policy: Replace[Attribute],
    ) -> Argument[Bare[AttributeName], Preserving]: ...

    @overload
    def on_missing(
        self: Expression[
            BindingType, Indexed[IndexType, FailureKindValue], Multiple[OrderType]
        ],
        policy: Replace[FailureKind],
    ) -> Argument[Indexed[IndexType, FailureKindValue], Preserving]: ...

    @overload
    def on_missing(
        self: Expression[BindingType, Bare[FailureKindValue], Single],
        policy: Replace[FailureKind],
    ) -> Argument[Bare[FailureKindValue], Preserving]: ...

    @overload
    def on_missing(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[FailureKindIndex]],
            Multiple[OrderType],
        ],
        policy: Replace[FailureKind],
    ) -> Argument[Indexed[IndexType, IndexValue[FailureKindIndex]], Preserving]: ...

    @overload
    def on_missing(
        self: Expression[BindingType, Bare[IndexValue[FailureKindIndex]], Single],
        policy: Replace[FailureKind],
    ) -> Argument[Bare[IndexValue[FailureKindIndex]], Preserving]: ...

    @overload
    def on_missing(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[EndpointRole]],
            Multiple[OrderType],
        ],
        policy: Replace[EdgeEndpointRole],
    ) -> Argument[Indexed[IndexType, IndexValue[EndpointRole]], Preserving]: ...

    @overload
    def on_missing(
        self: Expression[BindingType, Bare[IndexValue[EndpointRole]], Single],
        policy: Replace[EdgeEndpointRole],
    ) -> Argument[Bare[IndexValue[EndpointRole]], Preserving]: ...

    @overload
    def on_missing(
        self: Expression[
            BindingType, Indexed[IndexType, IndexValue[NodeIndex]], Multiple[OrderType]
        ],
        policy: Replace[NodeIndexPayload],
    ) -> Argument[Indexed[IndexType, IndexValue[NodeIndex]], Preserving]: ...

    @overload
    def on_missing(
        self: Expression[BindingType, Bare[IndexValue[NodeIndex]], Single],
        policy: Replace[NodeIndexPayload],
    ) -> Argument[Bare[IndexValue[NodeIndex]], Preserving]: ...

    @overload
    def on_missing(
        self: Expression[
            BindingType, Indexed[IndexType, IndexValue[GroupIndex]], Multiple[OrderType]
        ],
        policy: Replace[GroupIndexPayload],
    ) -> Argument[Indexed[IndexType, IndexValue[GroupIndex]], Preserving]: ...

    @overload
    def on_missing(
        self: Expression[BindingType, Bare[IndexValue[GroupIndex]], Single],
        policy: Replace[GroupIndexPayload],
    ) -> Argument[Bare[IndexValue[GroupIndex]], Preserving]: ...

    @overload
    def on_missing(
        self: Expression[
            BindingType, Indexed[IndexType, IndexValue[EdgeIndex]], Multiple[OrderType]
        ],
        policy: Replace[EdgeIndexPayload],
    ) -> Argument[Indexed[IndexType, IndexValue[EdgeIndex]], Preserving]: ...

    @overload
    def on_missing(
        self: Expression[BindingType, Bare[IndexValue[EdgeIndex]], Single],
        policy: Replace[EdgeIndexPayload],
    ) -> Argument[Bare[IndexValue[EdgeIndex]], Preserving]: ...

    @overload
    def on_missing(
        self: Expression[
            BindingType, Indexed[IndexType, IndexValue[ValueIndex]], Multiple[OrderType]
        ],
        policy: Replace[ScalarValue],
    ) -> Argument[Indexed[IndexType, IndexValue[ValueIndex]], Preserving]: ...

    @overload
    def on_missing(
        self: Expression[BindingType, Bare[IndexValue[ValueIndex]], Single],
        policy: Replace[ScalarValue],
    ) -> Argument[Bare[IndexValue[ValueIndex]], Preserving]: ...

    @overload
    def on_missing(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[AttributeNameIndex]],
            Multiple[OrderType],
        ],
        policy: Replace[Attribute],
    ) -> Argument[Indexed[IndexType, IndexValue[AttributeNameIndex]], Preserving]: ...

    @overload
    def on_missing(
        self: Expression[BindingType, Bare[IndexValue[AttributeNameIndex]], Single],
        policy: Replace[Attribute],
    ) -> Argument[Bare[IndexValue[AttributeNameIndex]], Preserving]: ...

    @overload
    def on_missing(
        self: Expression[
            BindingType, Indexed[IndexType, IndexValue[BoolIndex]], Multiple[OrderType]
        ],
        policy: Replace[_BooleanValue],
    ) -> Argument[Indexed[IndexType, IndexValue[BoolIndex]], Preserving]: ...

    @overload
    def on_missing(
        self: Expression[BindingType, Bare[IndexValue[BoolIndex]], Single],
        policy: Replace[_BooleanValue],
    ) -> Argument[Bare[IndexValue[BoolIndex]], Preserving]: ...

    @overload
    def on_missing(
        self: Expression[
            BindingType, Indexed[IndexType, IndexValue[Positional]], Multiple[OrderType]
        ],
        policy: Replace[int],
    ) -> Argument[Indexed[IndexType, IndexValue[Positional]], Preserving]: ...

    @overload
    def on_missing(
        self: Expression[BindingType, Bare[IndexValue[Positional]], Single],
        policy: Replace[int],
    ) -> Argument[Bare[IndexValue[Positional]], Preserving]: ...

    def on_missing(
        self,
        policy: Union[
            Drop,
            Replace[
                Union[
                    ScalarValue,
                    EdgeIndexPayload,
                    FailureKind,
                    EdgeEndpointRole,
                    Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
                    Argument[Any, Any],
                ]
            ],
        ],
    ) -> Argument[Any, Any]:
        resolved = (
            self._py_carrier.on_missing_replace(
                Expression._to_argument(policy._replacement)
            )
            if isinstance(policy, Replace)
            else self._py_carrier.on_missing_drop()
        )

        return Argument._from_py_argument(resolved)

    def cache(self) -> Expression[BindingType, S, C, Unpack[Levels]]:
        return self._rebuild(self._py_carrier.cache())

    @overload
    def filter(
        self: Expression[BindingType, Indexed[IndexType, V], Definite, Unpack[Levels]],
        mask: MaskArgument[IndexType, ArgumentOrderType],
    ) -> Expression[BindingType, Indexed[IndexType, V], Single, Unpack[Levels]]: ...

    @overload
    def filter(
        self: Expression[
            BindingType, Indexed[IndexType, V], DroppedContainerType, Unpack[Levels]
        ],
        mask: MaskArgument[IndexType, ArgumentOrderType],
    ) -> Expression[
        BindingType, Indexed[IndexType, V], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def filter(
        self: Expression[BindingType, Bare[BareValueType], Definite, Unpack[Levels]],
        mask: BareMaskArgument,
    ) -> Expression[BindingType, Bare[BareValueType], Single, Unpack[Levels]]: ...

    @overload
    def filter(
        self: Expression[
            BindingType, Bare[BareValueType], DroppedContainerType, Unpack[Levels]
        ],
        mask: BareMaskArgument,
    ) -> Expression[
        BindingType, Bare[BareValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    def filter(
        self,
        mask: Union[
            _BooleanValue,
            Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
    ) -> Any:
        return self._rebuild(self._py_carrier.filter(Expression._to_argument(mask)))

    @overload
    def and_(
        self: Expression[
            BindingType, Indexed[IndexType, Mask], Definite, Unpack[Levels]
        ],
        other: IndexedDroppingArgument[IndexType, Mask],
    ) -> Expression[BindingType, Indexed[IndexType, Mask], Single, Unpack[Levels]]: ...

    @overload
    def and_(
        self: Expression[
            BindingType, Indexed[IndexType, Mask], DroppedContainerType, Unpack[Levels]
        ],
        other: IndexedDroppingArgument[IndexType, Mask],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def and_(
        self: Expression[BindingType, Bare[Mask], Definite, Unpack[Levels]],
        other: BareDroppingArgument[Mask],
    ) -> Expression[BindingType, Bare[Mask], Single, Unpack[Levels]]: ...

    @overload
    def and_(
        self: Expression[BindingType, Bare[Mask], DroppedContainerType, Unpack[Levels]],
        other: BareDroppingArgument[Mask],
    ) -> Expression[BindingType, Bare[Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def and_(
        self: Expression[
            BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
        ],
        other: Union[
            _BooleanValue, IndexedExpressionArgument[IndexType, Mask, ArgumentOrderType]
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def and_(
        self: Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]],
        other: Union[_BooleanValue, BareExpressionArgument[Mask]],
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def and_(
        self,
        other: Union[
            _BooleanValue,
            Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
    ) -> Any:
        return self._rebuild(self._py_carrier.and_(Expression._to_argument(other)))

    @overload
    def or_(
        self: Expression[
            BindingType, Indexed[IndexType, Mask], Definite, Unpack[Levels]
        ],
        other: IndexedDroppingArgument[IndexType, Mask],
    ) -> Expression[BindingType, Indexed[IndexType, Mask], Single, Unpack[Levels]]: ...

    @overload
    def or_(
        self: Expression[
            BindingType, Indexed[IndexType, Mask], DroppedContainerType, Unpack[Levels]
        ],
        other: IndexedDroppingArgument[IndexType, Mask],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def or_(
        self: Expression[BindingType, Bare[Mask], Definite, Unpack[Levels]],
        other: BareDroppingArgument[Mask],
    ) -> Expression[BindingType, Bare[Mask], Single, Unpack[Levels]]: ...

    @overload
    def or_(
        self: Expression[BindingType, Bare[Mask], DroppedContainerType, Unpack[Levels]],
        other: BareDroppingArgument[Mask],
    ) -> Expression[BindingType, Bare[Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def or_(
        self: Expression[
            BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
        ],
        other: Union[
            _BooleanValue, IndexedExpressionArgument[IndexType, Mask, ArgumentOrderType]
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def or_(
        self: Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]],
        other: Union[_BooleanValue, BareExpressionArgument[Mask]],
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def or_(
        self,
        other: Union[
            _BooleanValue,
            Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
    ) -> Any:
        return self._rebuild(self._py_carrier.or_(Expression._to_argument(other)))

    @overload
    def xor(
        self: Expression[
            BindingType, Indexed[IndexType, Mask], Definite, Unpack[Levels]
        ],
        other: IndexedDroppingArgument[IndexType, Mask],
    ) -> Expression[BindingType, Indexed[IndexType, Mask], Single, Unpack[Levels]]: ...

    @overload
    def xor(
        self: Expression[
            BindingType, Indexed[IndexType, Mask], DroppedContainerType, Unpack[Levels]
        ],
        other: IndexedDroppingArgument[IndexType, Mask],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def xor(
        self: Expression[BindingType, Bare[Mask], Definite, Unpack[Levels]],
        other: BareDroppingArgument[Mask],
    ) -> Expression[BindingType, Bare[Mask], Single, Unpack[Levels]]: ...

    @overload
    def xor(
        self: Expression[BindingType, Bare[Mask], DroppedContainerType, Unpack[Levels]],
        other: BareDroppingArgument[Mask],
    ) -> Expression[BindingType, Bare[Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def xor(
        self: Expression[
            BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
        ],
        other: Union[
            _BooleanValue, IndexedExpressionArgument[IndexType, Mask, ArgumentOrderType]
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def xor(
        self: Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]],
        other: Union[_BooleanValue, BareExpressionArgument[Mask]],
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def xor(
        self,
        other: Union[
            _BooleanValue,
            Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
    ) -> Any:
        return self._rebuild(self._py_carrier.xor(Expression._to_argument(other)))

    @overload
    def not_(
        self: Expression[
            BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def not_(
        self: Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]],
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def not_(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.not_())

    @overload
    def first(
        self: Expression[
            BindingType, Indexed[IndexType, V], Multiple[Ordered], Unpack[Levels]
        ],
    ) -> Expression[BindingType, Indexed[IndexType, V], Single, Unpack[Levels]]: ...

    @overload
    def first(
        self: Expression[
            BindingType, Bare[BareValueType], Multiple[Ordered], Unpack[Levels]
        ],
    ) -> Expression[BindingType, Bare[BareValueType], Single, Unpack[Levels]]: ...

    def first(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.first())

    @overload
    def last(
        self: Expression[
            BindingType, Indexed[IndexType, V], Multiple[Ordered], Unpack[Levels]
        ],
    ) -> Expression[BindingType, Indexed[IndexType, V], Single, Unpack[Levels]]: ...

    @overload
    def last(
        self: Expression[
            BindingType, Bare[BareValueType], Multiple[Ordered], Unpack[Levels]
        ],
    ) -> Expression[BindingType, Bare[BareValueType], Single, Unpack[Levels]]: ...

    def last(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.last())

    @overload
    def reverse_order(
        self: Expression[
            BindingType, Indexed[IndexType, V], Multiple[Ordered], Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, V], Multiple[Ordered], Unpack[Levels]
    ]: ...

    @overload
    def reverse_order(
        self: Expression[
            BindingType, Bare[BareValueType], Multiple[Ordered], Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Bare[BareValueType], Multiple[Ordered], Unpack[Levels]
    ]: ...

    def reverse_order(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.reverse_order())

    @overload
    def shuffle(
        self: Expression[
            BindingType, Indexed[IndexType, V], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, V], Multiple[Ordered], Unpack[Levels]
    ]: ...

    @overload
    def shuffle(
        self: Expression[
            BindingType, Bare[BareValueType], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Bare[BareValueType], Multiple[Ordered], Unpack[Levels]
    ]: ...

    def shuffle(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.shuffle())

    @overload
    def unorder(
        self: Expression[
            BindingType, Indexed[IndexType, V], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, V], Multiple[Unordered], Unpack[Levels]
    ]: ...

    @overload
    def unorder(
        self: Expression[
            BindingType, Bare[BareValueType], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Bare[BareValueType], Multiple[Unordered], Unpack[Levels]
    ]: ...

    def unorder(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.unorder())

    @overload
    def sort(
        self: Expression[
            BindingType,
            Indexed[IndexType, OrderableValueType],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, OrderableValueType],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def sort(
        self: Expression[
            BindingType, Bare[OrderableValueType], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Bare[OrderableValueType], Multiple[Ordered], Unpack[Levels]
    ]: ...

    def sort(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.sort())

    def sort_by(
        self: Expression[
            BindingType, Indexed[IndexType, V], Multiple[OrderType], Unpack[Levels]
        ],
        key: IndexedAnyScalarArgument[IndexType, SortKeyValueType, ArgumentOrderType],
    ) -> Expression[
        BindingType, Indexed[IndexType, V], Multiple[Ordered], Unpack[Levels]
    ]:
        return self._rebuild(self._py_carrier.sort_by(Expression._to_argument(key)))

    @overload
    def drop_duplicates(
        self: Expression[
            BindingType,
            Indexed[IndexType, EquivalentValueType],
            Multiple[Ordered],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, EquivalentValueType],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def drop_duplicates(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[ValueIndexType]],
            Multiple[Ordered],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[ValueIndexType]],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    def drop_duplicates(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.drop_duplicates())

    @overload
    def is_duplicated(
        self: Expression[
            BindingType,
            Indexed[IndexType, EquivalentValueType],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], Multiple[OrderType], Unpack[Levels]
    ]: ...

    @overload
    def is_duplicated(
        self: Expression[
            BindingType, Bare[EquivalentValueType], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Expression[BindingType, Bare[Mask], Multiple[OrderType], Unpack[Levels]]: ...

    @overload
    def is_duplicated(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[ValueIndexType]],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], Multiple[OrderType], Unpack[Levels]
    ]: ...

    @overload
    def is_duplicated(
        self: Expression[
            BindingType,
            Bare[IndexValue[ValueIndexType]],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Expression[BindingType, Bare[Mask], Multiple[OrderType], Unpack[Levels]]: ...

    def is_duplicated(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.is_duplicated())

    @overload
    def unique(
        self: Expression[
            BindingType, Bare[EquivalentValueType], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Bare[EquivalentValueType], Multiple[OrderType], Unpack[Levels]
    ]: ...

    @overload
    def unique(
        self: Expression[
            BindingType,
            Bare[IndexValue[ValueIndexType]],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Bare[IndexValue[ValueIndexType]],
        Multiple[OrderType],
        Unpack[Levels],
    ]: ...

    def unique(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.unique())

    @overload
    def take(
        self: Expression[
            BindingType, Indexed[IndexType, V], Multiple[Ordered], Unpack[Levels]
        ],
        elements: int,
    ) -> Expression[
        BindingType, Indexed[IndexType, V], Multiple[Ordered], Unpack[Levels]
    ]: ...

    @overload
    def take(
        self: Expression[
            BindingType, Bare[BareValueType], Multiple[Ordered], Unpack[Levels]
        ],
        elements: int,
    ) -> Expression[
        BindingType, Bare[BareValueType], Multiple[Ordered], Unpack[Levels]
    ]: ...

    def take(self, elements: int) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.take(elements))

    @overload
    def is_bool(
        self: Expression[
            BindingType,
            Indexed[IndexType, ScalarInspectableValueType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def is_bool(
        self: Expression[
            BindingType, Bare[ScalarInspectableValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def is_bool(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.is_bool())

    @overload
    def is_datetime(
        self: Expression[
            BindingType,
            Indexed[IndexType, ScalarInspectableValueType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def is_datetime(
        self: Expression[
            BindingType, Bare[ScalarInspectableValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def is_datetime(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.is_datetime())

    @overload
    def is_duration(
        self: Expression[
            BindingType,
            Indexed[IndexType, ScalarInspectableValueType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def is_duration(
        self: Expression[
            BindingType, Bare[ScalarInspectableValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def is_duration(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.is_duration())

    @overload
    def is_float(
        self: Expression[
            BindingType,
            Indexed[IndexType, ScalarInspectableValueType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def is_float(
        self: Expression[
            BindingType, Bare[ScalarInspectableValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def is_float(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.is_float())

    @overload
    def is_null(
        self: Expression[
            BindingType,
            Indexed[IndexType, ScalarInspectableValueType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def is_null(
        self: Expression[
            BindingType, Bare[ScalarInspectableValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def is_null(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.is_null())

    @overload
    def is_int(
        self: Expression[
            BindingType,
            Indexed[IndexType, InspectableValueType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def is_int(
        self: Expression[
            BindingType, Bare[InspectableValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def is_int(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.is_int())

    @overload
    def is_string(
        self: Expression[
            BindingType,
            Indexed[IndexType, InspectableValueType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def is_string(
        self: Expression[
            BindingType, Bare[InspectableValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def is_string(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.is_string())

    @overload
    def abs(
        self: Expression[
            BindingType,
            Indexed[IndexType, NumericValueType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, NumericValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def abs(
        self: Expression[
            BindingType, Bare[NumericValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Bare[NumericValueType], ContainerType, Unpack[Levels]
    ]: ...

    def abs(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.abs())

    @overload
    def neg(
        self: Expression[
            BindingType,
            Indexed[IndexType, NumericValueType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, NumericValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def neg(
        self: Expression[
            BindingType, Bare[NumericValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Bare[NumericValueType], ContainerType, Unpack[Levels]
    ]: ...

    def neg(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.neg())

    @overload
    def sign(
        self: Expression[
            BindingType,
            Indexed[IndexType, NumericValueType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, NumericValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def sign(
        self: Expression[
            BindingType, Bare[NumericValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Bare[NumericValueType], ContainerType, Unpack[Levels]
    ]: ...

    def sign(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.sign())

    @overload
    def ceil(
        self: Expression[
            BindingType,
            Indexed[IndexType, RealNumericValueType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, RealNumericValueType],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def ceil(
        self: Expression[
            BindingType, Bare[RealNumericValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Bare[RealNumericValueType], ContainerType, Unpack[Levels]
    ]: ...

    def ceil(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.ceil())

    @overload
    def cbrt(
        self: Expression[
            BindingType,
            Indexed[IndexType, RealNumericValueType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, RealNumericValueType],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def cbrt(
        self: Expression[
            BindingType, Bare[RealNumericValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Bare[RealNumericValueType], ContainerType, Unpack[Levels]
    ]: ...

    def cbrt(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.cbrt())

    @overload
    def exp(
        self: Expression[
            BindingType,
            Indexed[IndexType, RealNumericValueType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, RealNumericValueType],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def exp(
        self: Expression[
            BindingType, Bare[RealNumericValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Bare[RealNumericValueType], ContainerType, Unpack[Levels]
    ]: ...

    def exp(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.exp())

    @overload
    def floor(
        self: Expression[
            BindingType,
            Indexed[IndexType, RealNumericValueType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, RealNumericValueType],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def floor(
        self: Expression[
            BindingType, Bare[RealNumericValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Bare[RealNumericValueType], ContainerType, Unpack[Levels]
    ]: ...

    def floor(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.floor())

    @overload
    def log(
        self: Expression[
            BindingType,
            Indexed[IndexType, RealNumericValueType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, RealNumericValueType],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def log(
        self: Expression[
            BindingType, Bare[RealNumericValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Bare[RealNumericValueType], ContainerType, Unpack[Levels]
    ]: ...

    def log(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.log())

    @overload
    def round(
        self: Expression[
            BindingType,
            Indexed[IndexType, RealNumericValueType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, RealNumericValueType],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def round(
        self: Expression[
            BindingType, Bare[RealNumericValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Bare[RealNumericValueType], ContainerType, Unpack[Levels]
    ]: ...

    def round(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.round())

    @overload
    def sqrt(
        self: Expression[
            BindingType,
            Indexed[IndexType, RealNumericValueType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, RealNumericValueType],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def sqrt(
        self: Expression[
            BindingType, Bare[RealNumericValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Bare[RealNumericValueType], ContainerType, Unpack[Levels]
    ]: ...

    def sqrt(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.sqrt())

    @overload
    def trim(
        self: Expression[
            BindingType,
            Indexed[IndexType, StringValueType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def trim(
        self: Expression[
            BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    def trim(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.trim())

    @overload
    def trim_start(
        self: Expression[
            BindingType,
            Indexed[IndexType, StringValueType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def trim_start(
        self: Expression[
            BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    def trim_start(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.trim_start())

    @overload
    def trim_end(
        self: Expression[
            BindingType,
            Indexed[IndexType, StringValueType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def trim_end(
        self: Expression[
            BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    def trim_end(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.trim_end())

    @overload
    def lowercase(
        self: Expression[
            BindingType,
            Indexed[IndexType, StringValueType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def lowercase(
        self: Expression[
            BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    def lowercase(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.lowercase())

    @overload
    def uppercase(
        self: Expression[
            BindingType,
            Indexed[IndexType, StringValueType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def uppercase(
        self: Expression[
            BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    def uppercase(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.uppercase())

    @overload
    def reverse(
        self: Expression[
            BindingType,
            Indexed[IndexType, StringValueType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def reverse(
        self: Expression[
            BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    def reverse(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.reverse())

    @overload
    def length(
        self: Expression[
            BindingType,
            Indexed[IndexType, StringValueType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def length(
        self: Expression[
            BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[BindingType, Bare[Scalar], ContainerType, Unpack[Levels]]: ...

    def length(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.length())

    @overload
    def slice(
        self: Expression[
            BindingType,
            Indexed[IndexType, StringValueType],
            ContainerType,
            Unpack[Levels],
        ],
        start: int,
        end: int,
    ) -> Expression[
        BindingType, Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def slice(
        self: Expression[
            BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
        ],
        start: int,
        end: int,
    ) -> Expression[
        BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    def slice(self, start: int, end: int) -> Any:
        return self._rebuild(self._py_carrier.slice(start, end))

    @overload
    def starts_with(
        self: Expression[
            BindingType, Indexed[IndexType, StringValueType], Definite, Unpack[Levels]
        ],
        argument: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Expression[BindingType, Indexed[IndexType, Mask], Single, Unpack[Levels]]: ...

    @overload
    def starts_with(
        self: Expression[
            BindingType,
            Indexed[IndexType, StringValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        argument: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def starts_with(
        self: Expression[BindingType, Bare[StringValueType], Definite, Unpack[Levels]],
        argument: BareDroppingArgument[StringArgumentValueType],
    ) -> Expression[BindingType, Bare[Mask], Single, Unpack[Levels]]: ...

    @overload
    def starts_with(
        self: Expression[
            BindingType, Bare[StringValueType], DroppedContainerType, Unpack[Levels]
        ],
        argument: BareDroppingArgument[StringArgumentValueType],
    ) -> Expression[BindingType, Bare[Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def starts_with(
        self: Expression[
            BindingType,
            Indexed[IndexType, StringValueType],
            ContainerType,
            Unpack[Levels],
        ],
        argument: IndexedExpressionArgument[
            IndexType, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def starts_with(
        self: Expression[
            BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
        ],
        argument: BareExpressionArgument[StringArgumentValueType],
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def starts_with(
        self: Expression[
            BindingType,
            Indexed[IndexType, StringValueType],
            ContainerType,
            Unpack[Levels],
        ],
        argument: ScalarValue,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def starts_with(
        self: Expression[
            BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
        ],
        argument: ScalarValue,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def starts_with(
        self,
        argument: Union[
            ScalarValue,
            Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
    ) -> Any:
        return self._rebuild(
            self._py_carrier.starts_with(Expression._to_argument(argument))
        )

    @overload
    def ends_with(
        self: Expression[
            BindingType, Indexed[IndexType, StringValueType], Definite, Unpack[Levels]
        ],
        argument: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Expression[BindingType, Indexed[IndexType, Mask], Single, Unpack[Levels]]: ...

    @overload
    def ends_with(
        self: Expression[
            BindingType,
            Indexed[IndexType, StringValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        argument: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def ends_with(
        self: Expression[BindingType, Bare[StringValueType], Definite, Unpack[Levels]],
        argument: BareDroppingArgument[StringArgumentValueType],
    ) -> Expression[BindingType, Bare[Mask], Single, Unpack[Levels]]: ...

    @overload
    def ends_with(
        self: Expression[
            BindingType, Bare[StringValueType], DroppedContainerType, Unpack[Levels]
        ],
        argument: BareDroppingArgument[StringArgumentValueType],
    ) -> Expression[BindingType, Bare[Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def ends_with(
        self: Expression[
            BindingType,
            Indexed[IndexType, StringValueType],
            ContainerType,
            Unpack[Levels],
        ],
        argument: IndexedExpressionArgument[
            IndexType, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def ends_with(
        self: Expression[
            BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
        ],
        argument: BareExpressionArgument[StringArgumentValueType],
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def ends_with(
        self: Expression[
            BindingType,
            Indexed[IndexType, StringValueType],
            ContainerType,
            Unpack[Levels],
        ],
        argument: ScalarValue,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def ends_with(
        self: Expression[
            BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
        ],
        argument: ScalarValue,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def ends_with(
        self,
        argument: Union[
            ScalarValue,
            Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
    ) -> Any:
        return self._rebuild(
            self._py_carrier.ends_with(Expression._to_argument(argument))
        )

    @overload
    def contains(
        self: Expression[
            BindingType, Indexed[IndexType, StringValueType], Definite, Unpack[Levels]
        ],
        argument: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Expression[BindingType, Indexed[IndexType, Mask], Single, Unpack[Levels]]: ...

    @overload
    def contains(
        self: Expression[
            BindingType,
            Indexed[IndexType, StringValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        argument: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def contains(
        self: Expression[BindingType, Bare[StringValueType], Definite, Unpack[Levels]],
        argument: BareDroppingArgument[StringArgumentValueType],
    ) -> Expression[BindingType, Bare[Mask], Single, Unpack[Levels]]: ...

    @overload
    def contains(
        self: Expression[
            BindingType, Bare[StringValueType], DroppedContainerType, Unpack[Levels]
        ],
        argument: BareDroppingArgument[StringArgumentValueType],
    ) -> Expression[BindingType, Bare[Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def contains(
        self: Expression[
            BindingType,
            Indexed[IndexType, StringValueType],
            ContainerType,
            Unpack[Levels],
        ],
        argument: IndexedExpressionArgument[
            IndexType, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def contains(
        self: Expression[
            BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
        ],
        argument: BareExpressionArgument[StringArgumentValueType],
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def contains(
        self: Expression[
            BindingType,
            Indexed[IndexType, StringValueType],
            ContainerType,
            Unpack[Levels],
        ],
        argument: ScalarValue,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def contains(
        self: Expression[
            BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
        ],
        argument: ScalarValue,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def contains(
        self,
        argument: Union[
            ScalarValue,
            Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
    ) -> Any:
        return self._rebuild(
            self._py_carrier.contains(Expression._to_argument(argument))
        )

    @overload
    def matches(
        self: Expression[
            BindingType, Indexed[IndexType, StringValueType], Definite, Unpack[Levels]
        ],
        pattern: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Expression[BindingType, Indexed[IndexType, Mask], Single, Unpack[Levels]]: ...

    @overload
    def matches(
        self: Expression[
            BindingType,
            Indexed[IndexType, StringValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        pattern: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def matches(
        self: Expression[BindingType, Bare[StringValueType], Definite, Unpack[Levels]],
        pattern: BareDroppingArgument[StringArgumentValueType],
    ) -> Expression[BindingType, Bare[Mask], Single, Unpack[Levels]]: ...

    @overload
    def matches(
        self: Expression[
            BindingType, Bare[StringValueType], DroppedContainerType, Unpack[Levels]
        ],
        pattern: BareDroppingArgument[StringArgumentValueType],
    ) -> Expression[BindingType, Bare[Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def matches(
        self: Expression[
            BindingType,
            Indexed[IndexType, StringValueType],
            ContainerType,
            Unpack[Levels],
        ],
        pattern: IndexedExpressionArgument[
            IndexType, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def matches(
        self: Expression[
            BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
        ],
        pattern: BareExpressionArgument[StringArgumentValueType],
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def matches(
        self: Expression[
            BindingType,
            Indexed[IndexType, StringValueType],
            ContainerType,
            Unpack[Levels],
        ],
        pattern: ScalarValue,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def matches(
        self: Expression[
            BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
        ],
        pattern: ScalarValue,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def matches(
        self,
        pattern: Union[
            ScalarValue,
            Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
    ) -> Any:
        return self._rebuild(self._py_carrier.matches(Expression._to_argument(pattern)))

    @overload
    def strip_prefix(
        self: Expression[
            BindingType, Indexed[IndexType, StringValueType], Definite, Unpack[Levels]
        ],
        prefix: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Expression[
        BindingType, Indexed[IndexType, StringValueType], Single, Unpack[Levels]
    ]: ...

    @overload
    def strip_prefix(
        self: Expression[
            BindingType,
            Indexed[IndexType, StringValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        prefix: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, StringValueType],
        DroppedContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def strip_prefix(
        self: Expression[BindingType, Bare[StringValueType], Definite, Unpack[Levels]],
        prefix: BareDroppingArgument[StringArgumentValueType],
    ) -> Expression[BindingType, Bare[StringValueType], Single, Unpack[Levels]]: ...

    @overload
    def strip_prefix(
        self: Expression[
            BindingType, Bare[StringValueType], DroppedContainerType, Unpack[Levels]
        ],
        prefix: BareDroppingArgument[StringArgumentValueType],
    ) -> Expression[
        BindingType, Bare[StringValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def strip_prefix(
        self: Expression[
            BindingType,
            Indexed[IndexType, StringValueType],
            ContainerType,
            Unpack[Levels],
        ],
        prefix: IndexedExpressionArgument[
            IndexType, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def strip_prefix(
        self: Expression[
            BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
        ],
        prefix: BareExpressionArgument[StringArgumentValueType],
    ) -> Expression[
        BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def strip_prefix(
        self: Expression[
            BindingType,
            Indexed[IndexType, StringValueType],
            ContainerType,
            Unpack[Levels],
        ],
        prefix: ScalarValue,
    ) -> Expression[
        BindingType, Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def strip_prefix(
        self: Expression[
            BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
        ],
        prefix: ScalarValue,
    ) -> Expression[
        BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    def strip_prefix(
        self,
        prefix: Union[
            ScalarValue,
            Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
    ) -> Any:
        return self._rebuild(
            self._py_carrier.strip_prefix(Expression._to_argument(prefix))
        )

    @overload
    def strip_suffix(
        self: Expression[
            BindingType, Indexed[IndexType, StringValueType], Definite, Unpack[Levels]
        ],
        suffix: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Expression[
        BindingType, Indexed[IndexType, StringValueType], Single, Unpack[Levels]
    ]: ...

    @overload
    def strip_suffix(
        self: Expression[
            BindingType,
            Indexed[IndexType, StringValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        suffix: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, StringValueType],
        DroppedContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def strip_suffix(
        self: Expression[BindingType, Bare[StringValueType], Definite, Unpack[Levels]],
        suffix: BareDroppingArgument[StringArgumentValueType],
    ) -> Expression[BindingType, Bare[StringValueType], Single, Unpack[Levels]]: ...

    @overload
    def strip_suffix(
        self: Expression[
            BindingType, Bare[StringValueType], DroppedContainerType, Unpack[Levels]
        ],
        suffix: BareDroppingArgument[StringArgumentValueType],
    ) -> Expression[
        BindingType, Bare[StringValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def strip_suffix(
        self: Expression[
            BindingType,
            Indexed[IndexType, StringValueType],
            ContainerType,
            Unpack[Levels],
        ],
        suffix: IndexedExpressionArgument[
            IndexType, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def strip_suffix(
        self: Expression[
            BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
        ],
        suffix: BareExpressionArgument[StringArgumentValueType],
    ) -> Expression[
        BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def strip_suffix(
        self: Expression[
            BindingType,
            Indexed[IndexType, StringValueType],
            ContainerType,
            Unpack[Levels],
        ],
        suffix: ScalarValue,
    ) -> Expression[
        BindingType, Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def strip_suffix(
        self: Expression[
            BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
        ],
        suffix: ScalarValue,
    ) -> Expression[
        BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    def strip_suffix(
        self,
        suffix: Union[
            ScalarValue,
            Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
    ) -> Any:
        return self._rebuild(
            self._py_carrier.strip_suffix(Expression._to_argument(suffix))
        )

    @overload
    def replace(
        self: Expression[
            BindingType,
            Indexed[IndexType, StringValueType],
            ContainerType,
            Unpack[Levels],
        ],
        old: IndexedStringArgument[IndexType, OldStringValueType, ArgumentOrderType],
        new: IndexedStringArgument[IndexType, NewStringValueType, ArgumentOrderType],
    ) -> Expression[
        BindingType, Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def replace(
        self: Expression[
            BindingType, Indexed[IndexType, StringValueType], Definite, Unpack[Levels]
        ],
        old: IndexedDroppingArgument[IndexType, OldStringValueType],
        new: IndexedAnyStringArgument[IndexType, NewStringValueType, ArgumentOrderType],
    ) -> Expression[
        BindingType, Indexed[IndexType, StringValueType], Single, Unpack[Levels]
    ]: ...

    @overload
    def replace(
        self: Expression[
            BindingType,
            Indexed[IndexType, StringValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        old: IndexedDroppingArgument[IndexType, OldStringValueType],
        new: IndexedAnyStringArgument[IndexType, NewStringValueType, ArgumentOrderType],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, StringValueType],
        DroppedContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def replace(
        self: Expression[
            BindingType, Indexed[IndexType, StringValueType], Definite, Unpack[Levels]
        ],
        old: IndexedStringArgument[IndexType, OldStringValueType, ArgumentOrderType],
        new: IndexedDroppingArgument[IndexType, NewStringValueType],
    ) -> Expression[
        BindingType, Indexed[IndexType, StringValueType], Single, Unpack[Levels]
    ]: ...

    @overload
    def replace(
        self: Expression[
            BindingType,
            Indexed[IndexType, StringValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        old: IndexedStringArgument[IndexType, OldStringValueType, ArgumentOrderType],
        new: IndexedDroppingArgument[IndexType, NewStringValueType],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, StringValueType],
        DroppedContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def replace(
        self: Expression[
            BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
        ],
        old: BareStringArgument[OldStringValueType],
        new: BareStringArgument[NewStringValueType],
    ) -> Expression[
        BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def replace(
        self: Expression[BindingType, Bare[StringValueType], Definite, Unpack[Levels]],
        old: BareDroppingArgument[OldStringValueType],
        new: BareAnyStringArgument[NewStringValueType],
    ) -> Expression[BindingType, Bare[StringValueType], Single, Unpack[Levels]]: ...

    @overload
    def replace(
        self: Expression[
            BindingType, Bare[StringValueType], DroppedContainerType, Unpack[Levels]
        ],
        old: BareDroppingArgument[OldStringValueType],
        new: BareAnyStringArgument[NewStringValueType],
    ) -> Expression[
        BindingType, Bare[StringValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def replace(
        self: Expression[BindingType, Bare[StringValueType], Definite, Unpack[Levels]],
        old: BareStringArgument[OldStringValueType],
        new: BareDroppingArgument[NewStringValueType],
    ) -> Expression[BindingType, Bare[StringValueType], Single, Unpack[Levels]]: ...

    @overload
    def replace(
        self: Expression[
            BindingType, Bare[StringValueType], DroppedContainerType, Unpack[Levels]
        ],
        old: BareStringArgument[OldStringValueType],
        new: BareDroppingArgument[NewStringValueType],
    ) -> Expression[
        BindingType, Bare[StringValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    def replace(
        self,
        old: Union[
            ScalarValue,
            Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
        new: Union[
            ScalarValue,
            Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
    ) -> Any:
        return self._rebuild(
            self._py_carrier.replace(
                Expression._to_argument(old), Expression._to_argument(new)
            )
        )

    @overload
    def replace_all(
        self: Expression[
            BindingType,
            Indexed[IndexType, StringValueType],
            ContainerType,
            Unpack[Levels],
        ],
        old: IndexedStringArgument[IndexType, OldStringValueType, ArgumentOrderType],
        new: IndexedStringArgument[IndexType, NewStringValueType, ArgumentOrderType],
    ) -> Expression[
        BindingType, Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def replace_all(
        self: Expression[
            BindingType, Indexed[IndexType, StringValueType], Definite, Unpack[Levels]
        ],
        old: IndexedDroppingArgument[IndexType, OldStringValueType],
        new: IndexedAnyStringArgument[IndexType, NewStringValueType, ArgumentOrderType],
    ) -> Expression[
        BindingType, Indexed[IndexType, StringValueType], Single, Unpack[Levels]
    ]: ...

    @overload
    def replace_all(
        self: Expression[
            BindingType,
            Indexed[IndexType, StringValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        old: IndexedDroppingArgument[IndexType, OldStringValueType],
        new: IndexedAnyStringArgument[IndexType, NewStringValueType, ArgumentOrderType],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, StringValueType],
        DroppedContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def replace_all(
        self: Expression[
            BindingType, Indexed[IndexType, StringValueType], Definite, Unpack[Levels]
        ],
        old: IndexedStringArgument[IndexType, OldStringValueType, ArgumentOrderType],
        new: IndexedDroppingArgument[IndexType, NewStringValueType],
    ) -> Expression[
        BindingType, Indexed[IndexType, StringValueType], Single, Unpack[Levels]
    ]: ...

    @overload
    def replace_all(
        self: Expression[
            BindingType,
            Indexed[IndexType, StringValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        old: IndexedStringArgument[IndexType, OldStringValueType, ArgumentOrderType],
        new: IndexedDroppingArgument[IndexType, NewStringValueType],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, StringValueType],
        DroppedContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def replace_all(
        self: Expression[
            BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
        ],
        old: BareStringArgument[OldStringValueType],
        new: BareStringArgument[NewStringValueType],
    ) -> Expression[
        BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def replace_all(
        self: Expression[BindingType, Bare[StringValueType], Definite, Unpack[Levels]],
        old: BareDroppingArgument[OldStringValueType],
        new: BareAnyStringArgument[NewStringValueType],
    ) -> Expression[BindingType, Bare[StringValueType], Single, Unpack[Levels]]: ...

    @overload
    def replace_all(
        self: Expression[
            BindingType, Bare[StringValueType], DroppedContainerType, Unpack[Levels]
        ],
        old: BareDroppingArgument[OldStringValueType],
        new: BareAnyStringArgument[NewStringValueType],
    ) -> Expression[
        BindingType, Bare[StringValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def replace_all(
        self: Expression[BindingType, Bare[StringValueType], Definite, Unpack[Levels]],
        old: BareStringArgument[OldStringValueType],
        new: BareDroppingArgument[NewStringValueType],
    ) -> Expression[BindingType, Bare[StringValueType], Single, Unpack[Levels]]: ...

    @overload
    def replace_all(
        self: Expression[
            BindingType, Bare[StringValueType], DroppedContainerType, Unpack[Levels]
        ],
        old: BareStringArgument[OldStringValueType],
        new: BareDroppingArgument[NewStringValueType],
    ) -> Expression[
        BindingType, Bare[StringValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    def replace_all(
        self,
        old: Union[
            ScalarValue,
            Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
        new: Union[
            ScalarValue,
            Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
    ) -> Any:
        return self._rebuild(
            self._py_carrier.replace_all(
                Expression._to_argument(old), Expression._to_argument(new)
            )
        )

    @overload
    def pad_start(
        self: Expression[
            BindingType,
            Indexed[IndexType, StringValueType],
            ContainerType,
            Unpack[Levels],
        ],
        width: int,
        character: IndexedStringArgument[
            IndexType, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def pad_start(
        self: Expression[
            BindingType, Indexed[IndexType, StringValueType], Definite, Unpack[Levels]
        ],
        width: int,
        character: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Expression[
        BindingType, Indexed[IndexType, StringValueType], Single, Unpack[Levels]
    ]: ...

    @overload
    def pad_start(
        self: Expression[
            BindingType,
            Indexed[IndexType, StringValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        width: int,
        character: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, StringValueType],
        DroppedContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def pad_start(
        self: Expression[
            BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
        ],
        width: int,
        character: BareStringArgument[StringArgumentValueType],
    ) -> Expression[
        BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def pad_start(
        self: Expression[BindingType, Bare[StringValueType], Definite, Unpack[Levels]],
        width: int,
        character: BareDroppingArgument[StringArgumentValueType],
    ) -> Expression[BindingType, Bare[StringValueType], Single, Unpack[Levels]]: ...

    @overload
    def pad_start(
        self: Expression[
            BindingType, Bare[StringValueType], DroppedContainerType, Unpack[Levels]
        ],
        width: int,
        character: BareDroppingArgument[StringArgumentValueType],
    ) -> Expression[
        BindingType, Bare[StringValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    def pad_start(
        self,
        width: int,
        character: Union[
            ScalarValue,
            Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
    ) -> Any:
        return self._rebuild(
            self._py_carrier.pad_start(width, Expression._to_argument(character))
        )

    @overload
    def pad_end(
        self: Expression[
            BindingType,
            Indexed[IndexType, StringValueType],
            ContainerType,
            Unpack[Levels],
        ],
        width: int,
        character: IndexedStringArgument[
            IndexType, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def pad_end(
        self: Expression[
            BindingType, Indexed[IndexType, StringValueType], Definite, Unpack[Levels]
        ],
        width: int,
        character: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Expression[
        BindingType, Indexed[IndexType, StringValueType], Single, Unpack[Levels]
    ]: ...

    @overload
    def pad_end(
        self: Expression[
            BindingType,
            Indexed[IndexType, StringValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        width: int,
        character: IndexedDroppingArgument[IndexType, StringArgumentValueType],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, StringValueType],
        DroppedContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def pad_end(
        self: Expression[
            BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
        ],
        width: int,
        character: BareStringArgument[StringArgumentValueType],
    ) -> Expression[
        BindingType, Bare[StringValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def pad_end(
        self: Expression[BindingType, Bare[StringValueType], Definite, Unpack[Levels]],
        width: int,
        character: BareDroppingArgument[StringArgumentValueType],
    ) -> Expression[BindingType, Bare[StringValueType], Single, Unpack[Levels]]: ...

    @overload
    def pad_end(
        self: Expression[
            BindingType, Bare[StringValueType], DroppedContainerType, Unpack[Levels]
        ],
        width: int,
        character: BareDroppingArgument[StringArgumentValueType],
    ) -> Expression[
        BindingType, Bare[StringValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    def pad_end(
        self,
        width: int,
        character: Union[
            ScalarValue,
            Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
    ) -> Any:
        return self._rebuild(
            self._py_carrier.pad_end(width, Expression._to_argument(character))
        )

    @overload
    def split(
        self: Expression[
            BindingType,
            Indexed[NodeIndex, StringValueType],
            Multiple[OrderType],
            Unpack[Levels],
        ],
        delimiter: IndexedAnyStringArgument[
            NodeIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[NodeIndex, Positional, Tuple[NodeIndexPayload, Optional[int]]],
            StringValueType,
        ],
        Multiple[OrderType],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Expression[
            BindingType,
            Indexed[EdgeIndex, StringValueType],
            Multiple[OrderType],
            Unpack[Levels],
        ],
        delimiter: IndexedAnyStringArgument[
            EdgeIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[EdgeIndex, Positional, Tuple[EdgeIndexPayload, Optional[int]]],
            StringValueType,
        ],
        Multiple[OrderType],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Expression[
            BindingType,
            Indexed[GroupIndex, StringValueType],
            Multiple[OrderType],
            Unpack[Levels],
        ],
        delimiter: IndexedAnyStringArgument[
            GroupIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[GroupIndex, Positional, Tuple[GroupIndexPayload, Optional[int]]],
            StringValueType,
        ],
        Multiple[OrderType],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Expression[
            BindingType,
            Indexed[Positional, StringValueType],
            Multiple[OrderType],
            Unpack[Levels],
        ],
        delimiter: IndexedAnyStringArgument[
            Positional, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[Positional, Positional, Tuple[int, Optional[int]]],
            StringValueType,
        ],
        Multiple[OrderType],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Expression[
            BindingType,
            Indexed[EndpointRole, StringValueType],
            Multiple[OrderType],
            Unpack[Levels],
        ],
        delimiter: IndexedAnyStringArgument[
            EndpointRole, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[EndpointRole, Positional, Tuple[EdgeEndpointRole, Optional[int]]],
            StringValueType,
        ],
        Multiple[OrderType],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Expression[
            BindingType,
            Indexed[ValueIndex, StringValueType],
            Multiple[OrderType],
            Unpack[Levels],
        ],
        delimiter: IndexedAnyStringArgument[
            ValueIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[ValueIndex, Positional, Tuple[ScalarValue, Optional[int]]],
            StringValueType,
        ],
        Multiple[OrderType],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Expression[
            BindingType,
            Indexed[AttributeNameIndex, StringValueType],
            Multiple[OrderType],
            Unpack[Levels],
        ],
        delimiter: IndexedAnyStringArgument[
            AttributeNameIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[AttributeNameIndex, Positional, Tuple[Attribute, Optional[int]]],
            StringValueType,
        ],
        Multiple[OrderType],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Expression[
            BindingType,
            Indexed[BoolIndex, StringValueType],
            Multiple[OrderType],
            Unpack[Levels],
        ],
        delimiter: IndexedAnyStringArgument[
            BoolIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[BoolIndex, Positional, Tuple[bool, Optional[int]]],
            StringValueType,
        ],
        Multiple[OrderType],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Expression[
            BindingType,
            Indexed[FailureKindIndex, StringValueType],
            Multiple[OrderType],
            Unpack[Levels],
        ],
        delimiter: IndexedAnyStringArgument[
            FailureKindIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[FailureKindIndex, Positional, Tuple[FailureKind, Optional[int]]],
            StringValueType,
        ],
        Multiple[OrderType],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Expression[
            BindingType,
            Indexed[Expanded[K, ChildType, ParentPayloadType], StringValueType],
            Multiple[OrderType],
            Unpack[Levels],
        ],
        delimiter: IndexedAnyStringArgument[
            Expanded[K, ChildType, ParentPayloadType],
            StringArgumentValueType,
            ArgumentOrderType,
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType, Indexed[NodeIndex, StringValueType], Single, Unpack[Levels]
        ],
        delimiter: IndexedAnyStringArgument[
            NodeIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[NodeIndex, Positional, Tuple[NodeIndexPayload, Optional[int]]],
            StringValueType,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Expression[
            BindingType, Indexed[EdgeIndex, StringValueType], Single, Unpack[Levels]
        ],
        delimiter: IndexedAnyStringArgument[
            EdgeIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[EdgeIndex, Positional, Tuple[EdgeIndexPayload, Optional[int]]],
            StringValueType,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Expression[
            BindingType, Indexed[GroupIndex, StringValueType], Single, Unpack[Levels]
        ],
        delimiter: IndexedAnyStringArgument[
            GroupIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[GroupIndex, Positional, Tuple[GroupIndexPayload, Optional[int]]],
            StringValueType,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Expression[
            BindingType, Indexed[Positional, StringValueType], Single, Unpack[Levels]
        ],
        delimiter: IndexedAnyStringArgument[
            Positional, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[Positional, Positional, Tuple[int, Optional[int]]],
            StringValueType,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Expression[
            BindingType, Indexed[EndpointRole, StringValueType], Single, Unpack[Levels]
        ],
        delimiter: IndexedAnyStringArgument[
            EndpointRole, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[EndpointRole, Positional, Tuple[EdgeEndpointRole, Optional[int]]],
            StringValueType,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Expression[
            BindingType, Indexed[ValueIndex, StringValueType], Single, Unpack[Levels]
        ],
        delimiter: IndexedAnyStringArgument[
            ValueIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[ValueIndex, Positional, Tuple[ScalarValue, Optional[int]]],
            StringValueType,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Expression[
            BindingType,
            Indexed[AttributeNameIndex, StringValueType],
            Single,
            Unpack[Levels],
        ],
        delimiter: IndexedAnyStringArgument[
            AttributeNameIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[AttributeNameIndex, Positional, Tuple[Attribute, Optional[int]]],
            StringValueType,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Expression[
            BindingType, Indexed[BoolIndex, StringValueType], Single, Unpack[Levels]
        ],
        delimiter: IndexedAnyStringArgument[
            BoolIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[BoolIndex, Positional, Tuple[bool, Optional[int]]],
            StringValueType,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Expression[
            BindingType,
            Indexed[FailureKindIndex, StringValueType],
            Single,
            Unpack[Levels],
        ],
        delimiter: IndexedAnyStringArgument[
            FailureKindIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[FailureKindIndex, Positional, Tuple[FailureKind, Optional[int]]],
            StringValueType,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Expression[
            BindingType,
            Indexed[Expanded[K, ChildType, ParentPayloadType], StringValueType],
            Single,
            Unpack[Levels],
        ],
        delimiter: IndexedAnyStringArgument[
            Expanded[K, ChildType, ParentPayloadType],
            StringArgumentValueType,
            ArgumentOrderType,
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType, Indexed[NodeIndex, StringValueType], Definite, Unpack[Levels]
        ],
        delimiter: IndexedAnyStringArgument[
            NodeIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[NodeIndex, Positional, Tuple[NodeIndexPayload, Optional[int]]],
            StringValueType,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Expression[
            BindingType, Indexed[EdgeIndex, StringValueType], Definite, Unpack[Levels]
        ],
        delimiter: IndexedAnyStringArgument[
            EdgeIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[EdgeIndex, Positional, Tuple[EdgeIndexPayload, Optional[int]]],
            StringValueType,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Expression[
            BindingType, Indexed[GroupIndex, StringValueType], Definite, Unpack[Levels]
        ],
        delimiter: IndexedAnyStringArgument[
            GroupIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[GroupIndex, Positional, Tuple[GroupIndexPayload, Optional[int]]],
            StringValueType,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Expression[
            BindingType, Indexed[Positional, StringValueType], Definite, Unpack[Levels]
        ],
        delimiter: IndexedAnyStringArgument[
            Positional, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[Positional, Positional, Tuple[int, Optional[int]]], StringValueType
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Expression[
            BindingType,
            Indexed[EndpointRole, StringValueType],
            Definite,
            Unpack[Levels],
        ],
        delimiter: IndexedAnyStringArgument[
            EndpointRole, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[EndpointRole, Positional, Tuple[EdgeEndpointRole, Optional[int]]],
            StringValueType,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Expression[
            BindingType, Indexed[ValueIndex, StringValueType], Definite, Unpack[Levels]
        ],
        delimiter: IndexedAnyStringArgument[
            ValueIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[ValueIndex, Positional, Tuple[ScalarValue, Optional[int]]],
            StringValueType,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Expression[
            BindingType,
            Indexed[AttributeNameIndex, StringValueType],
            Definite,
            Unpack[Levels],
        ],
        delimiter: IndexedAnyStringArgument[
            AttributeNameIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[AttributeNameIndex, Positional, Tuple[Attribute, Optional[int]]],
            StringValueType,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Expression[
            BindingType, Indexed[BoolIndex, StringValueType], Definite, Unpack[Levels]
        ],
        delimiter: IndexedAnyStringArgument[
            BoolIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[BoolIndex, Positional, Tuple[bool, Optional[int]]], StringValueType
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Expression[
            BindingType,
            Indexed[FailureKindIndex, StringValueType],
            Definite,
            Unpack[Levels],
        ],
        delimiter: IndexedAnyStringArgument[
            FailureKindIndex, StringArgumentValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[FailureKindIndex, Positional, Tuple[FailureKind, Optional[int]]],
            StringValueType,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def split(
        self: Expression[
            BindingType,
            Indexed[Expanded[K, ChildType, ParentPayloadType], StringValueType],
            Definite,
            Unpack[Levels],
        ],
        delimiter: IndexedAnyStringArgument[
            Expanded[K, ChildType, ParentPayloadType],
            StringArgumentValueType,
            ArgumentOrderType,
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType, Bare[StringValueType], Multiple[OrderType], Unpack[Levels]
        ],
        delimiter: BareAnyStringArgument[StringArgumentValueType],
    ) -> Expression[
        BindingType, Bare[StringValueType], Multiple[OrderType], Unpack[Levels]
    ]: ...

    @overload
    def split(
        self: Expression[BindingType, Bare[StringValueType], Single, Unpack[Levels]],
        delimiter: BareAnyStringArgument[StringArgumentValueType],
    ) -> Expression[
        BindingType, Bare[StringValueType], Multiple[Ordered], Unpack[Levels]
    ]: ...

    @overload
    def split(
        self: Expression[BindingType, Bare[StringValueType], Definite, Unpack[Levels]],
        delimiter: BareAnyStringArgument[StringArgumentValueType],
    ) -> Expression[
        BindingType, Bare[StringValueType], Multiple[Ordered], Unpack[Levels]
    ]: ...

    def split(
        self,
        delimiter: Union[
            ScalarValue,
            Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
    ) -> Any:
        return self._rebuild(self._py_carrier.split(Expression._to_argument(delimiter)))

    @overload
    def attribute(
        self: Expression[
            BindingType, Indexed[EntityType, Unit], ContainerType, Unpack[Levels]
        ],
        attribute: Attribute,
    ) -> Expression[
        BindingType, Indexed[EntityType, Scalar], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def attribute(
        self: Expression[
            BindingType,
            Indexed[IndexType, EntityReferenceType],
            ContainerType,
            Unpack[Levels],
        ],
        attribute: Attribute,
    ) -> Expression[
        BindingType, Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]
    ]: ...

    def attribute(self, attribute: Attribute) -> Any:
        return self._rebuild(self._py_carrier.attribute(attribute))

    @overload
    def attributes(
        self: Expression[
            BindingType, Indexed[NodeIndex, Unit], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType, Indexed[EdgeIndex, Unit], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[NodeIndex, EntityReferenceType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[EdgeIndex, EntityReferenceType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[GroupIndex, EntityReferenceType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[
                GroupIndex,
                AttributeNameIndex,
                Tuple[GroupIndexPayload, Optional[Attribute]],
            ],
            AttributeName,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def attributes(
        self: Expression[
            BindingType,
            Indexed[Positional, EntityReferenceType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[Positional, AttributeNameIndex, Tuple[int, Optional[Attribute]]],
            AttributeName,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def attributes(
        self: Expression[
            BindingType,
            Indexed[EndpointRole, EntityReferenceType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[ValueIndex, EntityReferenceType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[AttributeNameIndex, EntityReferenceType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[BoolIndex, EntityReferenceType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[BoolIndex, AttributeNameIndex, Tuple[bool, Optional[Attribute]]],
            AttributeName,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def attributes(
        self: Expression[
            BindingType,
            Indexed[FailureKindIndex, EntityReferenceType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[Expanded[K, ChildType, ParentPayloadType], EntityReferenceType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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

    def attributes(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.attributes())

    @overload
    def resolve(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[NodeIndex]],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, NodeReference], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def resolve(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[EdgeIndex]],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, EdgeReference], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def resolve(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[GroupIndex]],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, GroupReference], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def resolve(
        self: Expression[
            BindingType, Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Bare[NodeReference], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def resolve(
        self: Expression[
            BindingType, Bare[IndexValue[EdgeIndex]], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Bare[EdgeReference], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def resolve(
        self: Expression[
            BindingType, Bare[IndexValue[GroupIndex]], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Bare[GroupReference], ContainerType, Unpack[Levels]
    ]: ...

    def resolve(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.resolve())

    @overload
    def select(
        self: Expression[
            BindingType,
            Indexed[IndexType, NodeReference],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Indexed[NodeIndex, Unit], Multiple[Unordered], Unpack[Levels]
    ]: ...

    @overload
    def select(
        self: Expression[
            BindingType,
            Indexed[IndexType, EdgeReference],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Indexed[EdgeIndex, Unit], Multiple[Unordered], Unpack[Levels]
    ]: ...

    @overload
    def select(
        self: Expression[
            BindingType,
            Indexed[IndexType, GroupReference],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Indexed[GroupIndex, Unit], Multiple[Unordered], Unpack[Levels]
    ]: ...

    @overload
    def select(
        self: Expression[
            BindingType, Bare[NodeReference], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Indexed[NodeIndex, Unit], Multiple[Unordered], Unpack[Levels]
    ]: ...

    @overload
    def select(
        self: Expression[
            BindingType, Bare[EdgeReference], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Indexed[EdgeIndex, Unit], Multiple[Unordered], Unpack[Levels]
    ]: ...

    @overload
    def select(
        self: Expression[
            BindingType, Bare[GroupReference], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Indexed[GroupIndex, Unit], Multiple[Unordered], Unpack[Levels]
    ]: ...

    @overload
    def select(
        self: Expression[
            BindingType, Indexed[IndexType, NodeReference], Single, Unpack[Levels]
        ],
    ) -> Expression[BindingType, Indexed[NodeIndex, Unit], Single, Unpack[Levels]]: ...

    @overload
    def select(
        self: Expression[
            BindingType, Indexed[IndexType, NodeReference], Definite, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Indexed[NodeIndex, Unit], Definite, Unpack[Levels]
    ]: ...

    @overload
    def select(
        self: Expression[
            BindingType, Indexed[IndexType, EdgeReference], Single, Unpack[Levels]
        ],
    ) -> Expression[BindingType, Indexed[EdgeIndex, Unit], Single, Unpack[Levels]]: ...

    @overload
    def select(
        self: Expression[
            BindingType, Indexed[IndexType, GroupReference], Single, Unpack[Levels]
        ],
    ) -> Expression[BindingType, Indexed[GroupIndex, Unit], Single, Unpack[Levels]]: ...

    @overload
    def select(
        self: Expression[
            BindingType, Indexed[IndexType, EdgeReference], Definite, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Indexed[EdgeIndex, Unit], Definite, Unpack[Levels]
    ]: ...

    @overload
    def select(
        self: Expression[
            BindingType, Indexed[IndexType, GroupReference], Definite, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Indexed[GroupIndex, Unit], Definite, Unpack[Levels]
    ]: ...

    @overload
    def select(
        self: Expression[BindingType, Bare[NodeReference], Single, Unpack[Levels]],
    ) -> Expression[BindingType, Indexed[NodeIndex, Unit], Single, Unpack[Levels]]: ...

    @overload
    def select(
        self: Expression[BindingType, Bare[NodeReference], Definite, Unpack[Levels]],
    ) -> Expression[
        BindingType, Indexed[NodeIndex, Unit], Definite, Unpack[Levels]
    ]: ...

    @overload
    def select(
        self: Expression[BindingType, Bare[EdgeReference], Single, Unpack[Levels]],
    ) -> Expression[BindingType, Indexed[EdgeIndex, Unit], Single, Unpack[Levels]]: ...

    @overload
    def select(
        self: Expression[BindingType, Bare[GroupReference], Single, Unpack[Levels]],
    ) -> Expression[BindingType, Indexed[GroupIndex, Unit], Single, Unpack[Levels]]: ...

    @overload
    def select(
        self: Expression[BindingType, Bare[EdgeReference], Definite, Unpack[Levels]],
    ) -> Expression[
        BindingType, Indexed[EdgeIndex, Unit], Definite, Unpack[Levels]
    ]: ...

    @overload
    def select(
        self: Expression[BindingType, Bare[GroupReference], Definite, Unpack[Levels]],
    ) -> Expression[
        BindingType, Indexed[GroupIndex, Unit], Definite, Unpack[Levels]
    ]: ...

    def select(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.select())

    @overload
    def parent_index(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[Expanded[K, ChildType, ParentPayloadType]]],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, IndexValue[K]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def parent_index(
        self: Expression[
            BindingType,
            Bare[IndexValue[Expanded[K, ChildType, ParentPayloadType]]],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Bare[IndexValue[K]], ContainerType, Unpack[Levels]
    ]: ...

    def parent_index(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.parent_index())

    @overload
    def child_index(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[Expanded[K, ChildType, ParentPayloadType]]],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[ChildType]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def child_index(
        self: Expression[
            BindingType,
            Bare[IndexValue[Expanded[K, ChildType, ParentPayloadType]]],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Bare[IndexValue[ChildType]], ContainerType, Unpack[Levels]
    ]: ...

    def child_index(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.child_index())

    @overload
    def has_attribute(
        self: Expression[
            BindingType, Indexed[EntityType, Unit], ContainerType, Unpack[Levels]
        ],
        attribute: Attribute,
    ) -> Expression[
        BindingType, Indexed[EntityType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def has_attribute(
        self: Expression[
            BindingType,
            Indexed[IndexType, EntityReferenceType],
            ContainerType,
            Unpack[Levels],
        ],
        attribute: Attribute,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    def has_attribute(self, attribute: Attribute) -> Any:
        return self._rebuild(self._py_carrier.has_attribute(attribute))

    @overload
    def in_group(
        self: Expression[
            BindingType, Indexed[EntityType, Unit], ContainerType, Unpack[Levels]
        ],
        group_index: GroupIndexPayload,
    ) -> Expression[
        BindingType, Indexed[EntityType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def in_group(
        self: Expression[
            BindingType,
            Indexed[IndexType, EntityReferenceType],
            ContainerType,
            Unpack[Levels],
        ],
        group_index: GroupIndexPayload,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    def in_group(self, group_index: GroupIndexPayload) -> Any:
        return self._rebuild(self._py_carrier.in_group(group_index))

    @overload
    def add(
        self: Expression[
            BindingType,
            Indexed[IndexType, ArithmeticValueType],
            Definite,
            Unpack[Levels],
        ],
        argument: IndexedDroppingArgument[IndexType, ArithmeticValueType],
    ) -> Expression[
        BindingType, Indexed[IndexType, ArithmeticValueType], Single, Unpack[Levels]
    ]: ...

    @overload
    def add(
        self: Expression[
            BindingType,
            Indexed[IndexType, ArithmeticValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        argument: IndexedDroppingArgument[IndexType, ArithmeticValueType],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, ArithmeticValueType],
        DroppedContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def add(
        self: Expression[
            BindingType, Bare[ArithmeticValueType], Definite, Unpack[Levels]
        ],
        argument: BareDroppingArgument[ArithmeticValueType],
    ) -> Expression[BindingType, Bare[ArithmeticValueType], Single, Unpack[Levels]]: ...

    @overload
    def add(
        self: Expression[
            BindingType, Bare[ArithmeticValueType], DroppedContainerType, Unpack[Levels]
        ],
        argument: BareDroppingArgument[ArithmeticValueType],
    ) -> Expression[
        BindingType, Bare[ArithmeticValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def add(
        self: Expression[
            BindingType,
            Indexed[IndexType, ArithmeticValueType],
            ContainerType,
            Unpack[Levels],
        ],
        argument: IndexedExpressionArgument[
            IndexType, ArithmeticValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, ArithmeticValueType],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def add(
        self: Expression[
            BindingType, Bare[ArithmeticValueType], ContainerType, Unpack[Levels]
        ],
        argument: BareExpressionArgument[ArithmeticValueType],
    ) -> Expression[
        BindingType, Bare[ArithmeticValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def add(
        self: Expression[
            BindingType, Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]
        ],
        argument: ScalarValue,
    ) -> Expression[
        BindingType, Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def add(
        self: Expression[BindingType, Bare[Scalar], ContainerType, Unpack[Levels]],
        argument: ScalarValue,
    ) -> Expression[BindingType, Bare[Scalar], ContainerType, Unpack[Levels]]: ...

    @overload
    def add(
        self: Expression[
            BindingType,
            Indexed[IndexType, AttributeName],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Indexed[IndexType, AttributeName], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def add(
        self: Expression[
            BindingType, Bare[AttributeName], ContainerType, Unpack[Levels]
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Bare[AttributeName], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def add(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[NodeIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[NodeIndex]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def add(
        self: Expression[
            BindingType, Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def add(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[ValueIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: ScalarValue,
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[ValueIndex]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def add(
        self: Expression[
            BindingType, Bare[IndexValue[ValueIndex]], ContainerType, Unpack[Levels]
        ],
        argument: ScalarValue,
    ) -> Expression[
        BindingType, Bare[IndexValue[ValueIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def add(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[AttributeNameIndex]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def add(
        self: Expression[
            BindingType,
            Bare[IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Bare[IndexValue[AttributeNameIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def add(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[Positional]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: int,
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[Positional]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def add(
        self: Expression[
            BindingType, Bare[IndexValue[Positional]], ContainerType, Unpack[Levels]
        ],
        argument: int,
    ) -> Expression[
        BindingType, Bare[IndexValue[Positional]], ContainerType, Unpack[Levels]
    ]: ...

    def add(
        self,
        argument: Union[
            ScalarValue,
            Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
    ) -> Any:
        return self._rebuild(self._py_carrier.add(Expression._to_argument(argument)))

    @overload
    def subtract(
        self: Expression[
            BindingType,
            Indexed[IndexType, ArithmeticValueType],
            Definite,
            Unpack[Levels],
        ],
        argument: IndexedDroppingArgument[IndexType, ArithmeticValueType],
    ) -> Expression[
        BindingType, Indexed[IndexType, ArithmeticValueType], Single, Unpack[Levels]
    ]: ...

    @overload
    def subtract(
        self: Expression[
            BindingType,
            Indexed[IndexType, ArithmeticValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        argument: IndexedDroppingArgument[IndexType, ArithmeticValueType],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, ArithmeticValueType],
        DroppedContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def subtract(
        self: Expression[
            BindingType, Bare[ArithmeticValueType], Definite, Unpack[Levels]
        ],
        argument: BareDroppingArgument[ArithmeticValueType],
    ) -> Expression[BindingType, Bare[ArithmeticValueType], Single, Unpack[Levels]]: ...

    @overload
    def subtract(
        self: Expression[
            BindingType, Bare[ArithmeticValueType], DroppedContainerType, Unpack[Levels]
        ],
        argument: BareDroppingArgument[ArithmeticValueType],
    ) -> Expression[
        BindingType, Bare[ArithmeticValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def subtract(
        self: Expression[
            BindingType,
            Indexed[IndexType, ArithmeticValueType],
            ContainerType,
            Unpack[Levels],
        ],
        argument: IndexedExpressionArgument[
            IndexType, ArithmeticValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, ArithmeticValueType],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def subtract(
        self: Expression[
            BindingType, Bare[ArithmeticValueType], ContainerType, Unpack[Levels]
        ],
        argument: BareExpressionArgument[ArithmeticValueType],
    ) -> Expression[
        BindingType, Bare[ArithmeticValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def subtract(
        self: Expression[
            BindingType, Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]
        ],
        argument: ScalarValue,
    ) -> Expression[
        BindingType, Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def subtract(
        self: Expression[BindingType, Bare[Scalar], ContainerType, Unpack[Levels]],
        argument: ScalarValue,
    ) -> Expression[BindingType, Bare[Scalar], ContainerType, Unpack[Levels]]: ...

    @overload
    def subtract(
        self: Expression[
            BindingType,
            Indexed[IndexType, AttributeName],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Indexed[IndexType, AttributeName], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def subtract(
        self: Expression[
            BindingType, Bare[AttributeName], ContainerType, Unpack[Levels]
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Bare[AttributeName], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def subtract(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[NodeIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[NodeIndex]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def subtract(
        self: Expression[
            BindingType, Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def subtract(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[ValueIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: ScalarValue,
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[ValueIndex]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def subtract(
        self: Expression[
            BindingType, Bare[IndexValue[ValueIndex]], ContainerType, Unpack[Levels]
        ],
        argument: ScalarValue,
    ) -> Expression[
        BindingType, Bare[IndexValue[ValueIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def subtract(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[AttributeNameIndex]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def subtract(
        self: Expression[
            BindingType,
            Bare[IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Bare[IndexValue[AttributeNameIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def subtract(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[Positional]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: int,
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[Positional]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def subtract(
        self: Expression[
            BindingType, Bare[IndexValue[Positional]], ContainerType, Unpack[Levels]
        ],
        argument: int,
    ) -> Expression[
        BindingType, Bare[IndexValue[Positional]], ContainerType, Unpack[Levels]
    ]: ...

    def subtract(
        self,
        argument: Union[
            ScalarValue,
            Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
    ) -> Any:
        return self._rebuild(
            self._py_carrier.subtract(Expression._to_argument(argument))
        )

    @overload
    def multiply(
        self: Expression[
            BindingType,
            Indexed[IndexType, ArithmeticValueType],
            Definite,
            Unpack[Levels],
        ],
        argument: IndexedDroppingArgument[IndexType, ArithmeticValueType],
    ) -> Expression[
        BindingType, Indexed[IndexType, ArithmeticValueType], Single, Unpack[Levels]
    ]: ...

    @overload
    def multiply(
        self: Expression[
            BindingType,
            Indexed[IndexType, ArithmeticValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        argument: IndexedDroppingArgument[IndexType, ArithmeticValueType],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, ArithmeticValueType],
        DroppedContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def multiply(
        self: Expression[
            BindingType, Bare[ArithmeticValueType], Definite, Unpack[Levels]
        ],
        argument: BareDroppingArgument[ArithmeticValueType],
    ) -> Expression[BindingType, Bare[ArithmeticValueType], Single, Unpack[Levels]]: ...

    @overload
    def multiply(
        self: Expression[
            BindingType, Bare[ArithmeticValueType], DroppedContainerType, Unpack[Levels]
        ],
        argument: BareDroppingArgument[ArithmeticValueType],
    ) -> Expression[
        BindingType, Bare[ArithmeticValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def multiply(
        self: Expression[
            BindingType,
            Indexed[IndexType, ArithmeticValueType],
            ContainerType,
            Unpack[Levels],
        ],
        argument: IndexedExpressionArgument[
            IndexType, ArithmeticValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, ArithmeticValueType],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def multiply(
        self: Expression[
            BindingType, Bare[ArithmeticValueType], ContainerType, Unpack[Levels]
        ],
        argument: BareExpressionArgument[ArithmeticValueType],
    ) -> Expression[
        BindingType, Bare[ArithmeticValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def multiply(
        self: Expression[
            BindingType, Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]
        ],
        argument: ScalarValue,
    ) -> Expression[
        BindingType, Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def multiply(
        self: Expression[BindingType, Bare[Scalar], ContainerType, Unpack[Levels]],
        argument: ScalarValue,
    ) -> Expression[BindingType, Bare[Scalar], ContainerType, Unpack[Levels]]: ...

    @overload
    def multiply(
        self: Expression[
            BindingType,
            Indexed[IndexType, AttributeName],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Indexed[IndexType, AttributeName], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def multiply(
        self: Expression[
            BindingType, Bare[AttributeName], ContainerType, Unpack[Levels]
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Bare[AttributeName], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def multiply(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[NodeIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[NodeIndex]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def multiply(
        self: Expression[
            BindingType, Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def multiply(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[ValueIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: ScalarValue,
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[ValueIndex]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def multiply(
        self: Expression[
            BindingType, Bare[IndexValue[ValueIndex]], ContainerType, Unpack[Levels]
        ],
        argument: ScalarValue,
    ) -> Expression[
        BindingType, Bare[IndexValue[ValueIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def multiply(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[AttributeNameIndex]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def multiply(
        self: Expression[
            BindingType,
            Bare[IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Bare[IndexValue[AttributeNameIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def multiply(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[Positional]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: int,
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[Positional]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def multiply(
        self: Expression[
            BindingType, Bare[IndexValue[Positional]], ContainerType, Unpack[Levels]
        ],
        argument: int,
    ) -> Expression[
        BindingType, Bare[IndexValue[Positional]], ContainerType, Unpack[Levels]
    ]: ...

    def multiply(
        self,
        argument: Union[
            ScalarValue,
            Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
    ) -> Any:
        return self._rebuild(
            self._py_carrier.multiply(Expression._to_argument(argument))
        )

    @overload
    def power(
        self: Expression[
            BindingType,
            Indexed[IndexType, ArithmeticValueType],
            Definite,
            Unpack[Levels],
        ],
        argument: IndexedDroppingArgument[IndexType, ArithmeticValueType],
    ) -> Expression[
        BindingType, Indexed[IndexType, ArithmeticValueType], Single, Unpack[Levels]
    ]: ...

    @overload
    def power(
        self: Expression[
            BindingType,
            Indexed[IndexType, ArithmeticValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        argument: IndexedDroppingArgument[IndexType, ArithmeticValueType],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, ArithmeticValueType],
        DroppedContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def power(
        self: Expression[
            BindingType, Bare[ArithmeticValueType], Definite, Unpack[Levels]
        ],
        argument: BareDroppingArgument[ArithmeticValueType],
    ) -> Expression[BindingType, Bare[ArithmeticValueType], Single, Unpack[Levels]]: ...

    @overload
    def power(
        self: Expression[
            BindingType, Bare[ArithmeticValueType], DroppedContainerType, Unpack[Levels]
        ],
        argument: BareDroppingArgument[ArithmeticValueType],
    ) -> Expression[
        BindingType, Bare[ArithmeticValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def power(
        self: Expression[
            BindingType,
            Indexed[IndexType, ArithmeticValueType],
            ContainerType,
            Unpack[Levels],
        ],
        argument: IndexedExpressionArgument[
            IndexType, ArithmeticValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, ArithmeticValueType],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def power(
        self: Expression[
            BindingType, Bare[ArithmeticValueType], ContainerType, Unpack[Levels]
        ],
        argument: BareExpressionArgument[ArithmeticValueType],
    ) -> Expression[
        BindingType, Bare[ArithmeticValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def power(
        self: Expression[
            BindingType, Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]
        ],
        argument: ScalarValue,
    ) -> Expression[
        BindingType, Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def power(
        self: Expression[BindingType, Bare[Scalar], ContainerType, Unpack[Levels]],
        argument: ScalarValue,
    ) -> Expression[BindingType, Bare[Scalar], ContainerType, Unpack[Levels]]: ...

    @overload
    def power(
        self: Expression[
            BindingType,
            Indexed[IndexType, AttributeName],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Indexed[IndexType, AttributeName], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def power(
        self: Expression[
            BindingType, Bare[AttributeName], ContainerType, Unpack[Levels]
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Bare[AttributeName], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def power(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[NodeIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[NodeIndex]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def power(
        self: Expression[
            BindingType, Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def power(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[ValueIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: ScalarValue,
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[ValueIndex]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def power(
        self: Expression[
            BindingType, Bare[IndexValue[ValueIndex]], ContainerType, Unpack[Levels]
        ],
        argument: ScalarValue,
    ) -> Expression[
        BindingType, Bare[IndexValue[ValueIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def power(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[AttributeNameIndex]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def power(
        self: Expression[
            BindingType,
            Bare[IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Bare[IndexValue[AttributeNameIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def power(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[Positional]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: int,
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[Positional]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def power(
        self: Expression[
            BindingType, Bare[IndexValue[Positional]], ContainerType, Unpack[Levels]
        ],
        argument: int,
    ) -> Expression[
        BindingType, Bare[IndexValue[Positional]], ContainerType, Unpack[Levels]
    ]: ...

    def power(
        self,
        argument: Union[
            ScalarValue,
            Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
    ) -> Any:
        return self._rebuild(self._py_carrier.power(Expression._to_argument(argument)))

    @overload
    def modulo(
        self: Expression[
            BindingType,
            Indexed[IndexType, ArithmeticValueType],
            Definite,
            Unpack[Levels],
        ],
        argument: IndexedDroppingArgument[IndexType, ArithmeticValueType],
    ) -> Expression[
        BindingType, Indexed[IndexType, ArithmeticValueType], Single, Unpack[Levels]
    ]: ...

    @overload
    def modulo(
        self: Expression[
            BindingType,
            Indexed[IndexType, ArithmeticValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        argument: IndexedDroppingArgument[IndexType, ArithmeticValueType],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, ArithmeticValueType],
        DroppedContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def modulo(
        self: Expression[
            BindingType, Bare[ArithmeticValueType], Definite, Unpack[Levels]
        ],
        argument: BareDroppingArgument[ArithmeticValueType],
    ) -> Expression[BindingType, Bare[ArithmeticValueType], Single, Unpack[Levels]]: ...

    @overload
    def modulo(
        self: Expression[
            BindingType, Bare[ArithmeticValueType], DroppedContainerType, Unpack[Levels]
        ],
        argument: BareDroppingArgument[ArithmeticValueType],
    ) -> Expression[
        BindingType, Bare[ArithmeticValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def modulo(
        self: Expression[
            BindingType,
            Indexed[IndexType, ArithmeticValueType],
            ContainerType,
            Unpack[Levels],
        ],
        argument: IndexedExpressionArgument[
            IndexType, ArithmeticValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, ArithmeticValueType],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def modulo(
        self: Expression[
            BindingType, Bare[ArithmeticValueType], ContainerType, Unpack[Levels]
        ],
        argument: BareExpressionArgument[ArithmeticValueType],
    ) -> Expression[
        BindingType, Bare[ArithmeticValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def modulo(
        self: Expression[
            BindingType, Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]
        ],
        argument: ScalarValue,
    ) -> Expression[
        BindingType, Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def modulo(
        self: Expression[BindingType, Bare[Scalar], ContainerType, Unpack[Levels]],
        argument: ScalarValue,
    ) -> Expression[BindingType, Bare[Scalar], ContainerType, Unpack[Levels]]: ...

    @overload
    def modulo(
        self: Expression[
            BindingType,
            Indexed[IndexType, AttributeName],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Indexed[IndexType, AttributeName], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def modulo(
        self: Expression[
            BindingType, Bare[AttributeName], ContainerType, Unpack[Levels]
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Bare[AttributeName], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def modulo(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[NodeIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[NodeIndex]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def modulo(
        self: Expression[
            BindingType, Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def modulo(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[ValueIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: ScalarValue,
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[ValueIndex]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def modulo(
        self: Expression[
            BindingType, Bare[IndexValue[ValueIndex]], ContainerType, Unpack[Levels]
        ],
        argument: ScalarValue,
    ) -> Expression[
        BindingType, Bare[IndexValue[ValueIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def modulo(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[AttributeNameIndex]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def modulo(
        self: Expression[
            BindingType,
            Bare[IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Bare[IndexValue[AttributeNameIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def modulo(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[Positional]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: int,
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[Positional]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def modulo(
        self: Expression[
            BindingType, Bare[IndexValue[Positional]], ContainerType, Unpack[Levels]
        ],
        argument: int,
    ) -> Expression[
        BindingType, Bare[IndexValue[Positional]], ContainerType, Unpack[Levels]
    ]: ...

    def modulo(
        self,
        argument: Union[
            ScalarValue,
            Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
    ) -> Any:
        return self._rebuild(self._py_carrier.modulo(Expression._to_argument(argument)))

    @overload
    def divide(
        self: Expression[
            BindingType,
            Indexed[IndexType, RealNumericValueType],
            Definite,
            Unpack[Levels],
        ],
        argument: IndexedDroppingArgument[IndexType, RealNumericValueType],
    ) -> Expression[
        BindingType, Indexed[IndexType, RealNumericValueType], Single, Unpack[Levels]
    ]: ...

    @overload
    def divide(
        self: Expression[
            BindingType,
            Indexed[IndexType, RealNumericValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        argument: IndexedDroppingArgument[IndexType, RealNumericValueType],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, RealNumericValueType],
        DroppedContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def divide(
        self: Expression[
            BindingType, Bare[RealNumericValueType], Definite, Unpack[Levels]
        ],
        argument: BareDroppingArgument[RealNumericValueType],
    ) -> Expression[
        BindingType, Bare[RealNumericValueType], Single, Unpack[Levels]
    ]: ...

    @overload
    def divide(
        self: Expression[
            BindingType,
            Bare[RealNumericValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        argument: BareDroppingArgument[RealNumericValueType],
    ) -> Expression[
        BindingType, Bare[RealNumericValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def divide(
        self: Expression[
            BindingType,
            Indexed[IndexType, RealNumericValueType],
            ContainerType,
            Unpack[Levels],
        ],
        argument: IndexedExpressionArgument[
            IndexType, RealNumericValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, RealNumericValueType],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def divide(
        self: Expression[
            BindingType, Bare[RealNumericValueType], ContainerType, Unpack[Levels]
        ],
        argument: BareExpressionArgument[RealNumericValueType],
    ) -> Expression[
        BindingType, Bare[RealNumericValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def divide(
        self: Expression[
            BindingType,
            Indexed[IndexType, RealNumericValueType],
            ContainerType,
            Unpack[Levels],
        ],
        argument: ScalarValue,
    ) -> Expression[
        BindingType,
        Indexed[IndexType, RealNumericValueType],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def divide(
        self: Expression[
            BindingType, Bare[RealNumericValueType], ContainerType, Unpack[Levels]
        ],
        argument: ScalarValue,
    ) -> Expression[
        BindingType, Bare[RealNumericValueType], ContainerType, Unpack[Levels]
    ]: ...

    def divide(
        self,
        argument: Union[
            ScalarValue,
            Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
    ) -> Any:
        return self._rebuild(self._py_carrier.divide(Expression._to_argument(argument)))

    @overload
    def clip(
        self: Expression[
            BindingType,
            Indexed[IndexType, ScalarClipValueType],
            Definite,
            Unpack[Levels],
        ],
        lower: IndexedDroppingArgument[IndexType, ScalarClipValueType],
        upper: IndexedAnyScalarArgument[
            IndexType, ScalarClipValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, ScalarClipValueType], Single, Unpack[Levels]
    ]: ...

    @overload
    def clip(
        self: Expression[
            BindingType,
            Indexed[IndexType, ScalarClipValueType],
            Definite,
            Unpack[Levels],
        ],
        lower: IndexedScalarArgument[IndexType, ScalarClipValueType, ArgumentOrderType],
        upper: IndexedDroppingArgument[IndexType, ScalarClipValueType],
    ) -> Expression[
        BindingType, Indexed[IndexType, ScalarClipValueType], Single, Unpack[Levels]
    ]: ...

    @overload
    def clip(
        self: Expression[
            BindingType,
            Indexed[IndexType, ScalarClipValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        lower: IndexedDroppingArgument[IndexType, ScalarClipValueType],
        upper: IndexedAnyScalarArgument[
            IndexType, ScalarClipValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, ScalarClipValueType],
        DroppedContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def clip(
        self: Expression[
            BindingType,
            Indexed[IndexType, ScalarClipValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        lower: IndexedScalarArgument[IndexType, ScalarClipValueType, ArgumentOrderType],
        upper: IndexedDroppingArgument[IndexType, ScalarClipValueType],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, ScalarClipValueType],
        DroppedContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def clip(
        self: Expression[
            BindingType, Bare[ScalarClipValueType], Definite, Unpack[Levels]
        ],
        lower: BareDroppingArgument[ScalarClipValueType],
        upper: BareAnyScalarArgument[ScalarClipValueType],
    ) -> Expression[BindingType, Bare[ScalarClipValueType], Single, Unpack[Levels]]: ...

    @overload
    def clip(
        self: Expression[
            BindingType, Bare[ScalarClipValueType], Definite, Unpack[Levels]
        ],
        lower: BareScalarArgument[ScalarClipValueType],
        upper: BareDroppingArgument[ScalarClipValueType],
    ) -> Expression[BindingType, Bare[ScalarClipValueType], Single, Unpack[Levels]]: ...

    @overload
    def clip(
        self: Expression[
            BindingType, Bare[ScalarClipValueType], DroppedContainerType, Unpack[Levels]
        ],
        lower: BareDroppingArgument[ScalarClipValueType],
        upper: BareAnyScalarArgument[ScalarClipValueType],
    ) -> Expression[
        BindingType, Bare[ScalarClipValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def clip(
        self: Expression[
            BindingType, Bare[ScalarClipValueType], DroppedContainerType, Unpack[Levels]
        ],
        lower: BareScalarArgument[ScalarClipValueType],
        upper: BareDroppingArgument[ScalarClipValueType],
    ) -> Expression[
        BindingType, Bare[ScalarClipValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def clip(
        self: Expression[
            BindingType,
            Indexed[IndexType, AttributeClipValueType],
            Definite,
            Unpack[Levels],
        ],
        lower: IndexedDroppingArgument[IndexType, AttributeClipValueType],
        upper: IndexedAnyAttributeArgument[
            IndexType, AttributeClipValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, AttributeClipValueType], Single, Unpack[Levels]
    ]: ...

    @overload
    def clip(
        self: Expression[
            BindingType,
            Indexed[IndexType, AttributeClipValueType],
            Definite,
            Unpack[Levels],
        ],
        lower: IndexedAttributeArgument[
            IndexType, AttributeClipValueType, ArgumentOrderType
        ],
        upper: IndexedDroppingArgument[IndexType, AttributeClipValueType],
    ) -> Expression[
        BindingType, Indexed[IndexType, AttributeClipValueType], Single, Unpack[Levels]
    ]: ...

    @overload
    def clip(
        self: Expression[
            BindingType,
            Indexed[IndexType, AttributeClipValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        lower: IndexedDroppingArgument[IndexType, AttributeClipValueType],
        upper: IndexedAnyAttributeArgument[
            IndexType, AttributeClipValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, AttributeClipValueType],
        DroppedContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def clip(
        self: Expression[
            BindingType,
            Indexed[IndexType, AttributeClipValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        lower: IndexedAttributeArgument[
            IndexType, AttributeClipValueType, ArgumentOrderType
        ],
        upper: IndexedDroppingArgument[IndexType, AttributeClipValueType],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, AttributeClipValueType],
        DroppedContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def clip(
        self: Expression[
            BindingType, Bare[AttributeClipValueType], Definite, Unpack[Levels]
        ],
        lower: BareDroppingArgument[AttributeClipValueType],
        upper: BareAnyAttributeArgument[AttributeClipValueType],
    ) -> Expression[
        BindingType, Bare[AttributeClipValueType], Single, Unpack[Levels]
    ]: ...

    @overload
    def clip(
        self: Expression[
            BindingType, Bare[AttributeClipValueType], Definite, Unpack[Levels]
        ],
        lower: BareAttributeArgument[AttributeClipValueType],
        upper: BareDroppingArgument[AttributeClipValueType],
    ) -> Expression[
        BindingType, Bare[AttributeClipValueType], Single, Unpack[Levels]
    ]: ...

    @overload
    def clip(
        self: Expression[
            BindingType,
            Bare[AttributeClipValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        lower: BareDroppingArgument[AttributeClipValueType],
        upper: BareAnyAttributeArgument[AttributeClipValueType],
    ) -> Expression[
        BindingType, Bare[AttributeClipValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def clip(
        self: Expression[
            BindingType,
            Bare[AttributeClipValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        lower: BareAttributeArgument[AttributeClipValueType],
        upper: BareDroppingArgument[AttributeClipValueType],
    ) -> Expression[
        BindingType, Bare[AttributeClipValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def clip(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[Positional]],
            Definite,
            Unpack[Levels],
        ],
        lower: IndexedDroppingArgument[IndexType, IndexValue[Positional]],
        upper: IndexedAnyIntegerArgument[
            IndexType, IndexValue[Positional], ArgumentOrderType
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, IndexValue[Positional]], Single, Unpack[Levels]
    ]: ...

    @overload
    def clip(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[Positional]],
            Definite,
            Unpack[Levels],
        ],
        lower: IndexedIntegerArgument[
            IndexType, IndexValue[Positional], ArgumentOrderType
        ],
        upper: IndexedDroppingArgument[IndexType, IndexValue[Positional]],
    ) -> Expression[
        BindingType, Indexed[IndexType, IndexValue[Positional]], Single, Unpack[Levels]
    ]: ...

    @overload
    def clip(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[Positional]],
            DroppedContainerType,
            Unpack[Levels],
        ],
        lower: IndexedDroppingArgument[IndexType, IndexValue[Positional]],
        upper: IndexedAnyIntegerArgument[
            IndexType, IndexValue[Positional], ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[Positional]],
        DroppedContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def clip(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[Positional]],
            DroppedContainerType,
            Unpack[Levels],
        ],
        lower: IndexedIntegerArgument[
            IndexType, IndexValue[Positional], ArgumentOrderType
        ],
        upper: IndexedDroppingArgument[IndexType, IndexValue[Positional]],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[Positional]],
        DroppedContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def clip(
        self: Expression[
            BindingType, Bare[IndexValue[Positional]], Definite, Unpack[Levels]
        ],
        lower: BareDroppingArgument[IndexValue[Positional]],
        upper: BareAnyIntegerArgument[IndexValue[Positional]],
    ) -> Expression[
        BindingType, Bare[IndexValue[Positional]], Single, Unpack[Levels]
    ]: ...

    @overload
    def clip(
        self: Expression[
            BindingType, Bare[IndexValue[Positional]], Definite, Unpack[Levels]
        ],
        lower: BareIntegerArgument[IndexValue[Positional]],
        upper: BareDroppingArgument[IndexValue[Positional]],
    ) -> Expression[
        BindingType, Bare[IndexValue[Positional]], Single, Unpack[Levels]
    ]: ...

    @overload
    def clip(
        self: Expression[
            BindingType,
            Bare[IndexValue[Positional]],
            DroppedContainerType,
            Unpack[Levels],
        ],
        lower: BareDroppingArgument[IndexValue[Positional]],
        upper: BareAnyIntegerArgument[IndexValue[Positional]],
    ) -> Expression[
        BindingType, Bare[IndexValue[Positional]], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def clip(
        self: Expression[
            BindingType,
            Bare[IndexValue[Positional]],
            DroppedContainerType,
            Unpack[Levels],
        ],
        lower: BareIntegerArgument[IndexValue[Positional]],
        upper: BareDroppingArgument[IndexValue[Positional]],
    ) -> Expression[
        BindingType, Bare[IndexValue[Positional]], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def clip(
        self: Expression[
            BindingType,
            Indexed[IndexType, ScalarClipValueType],
            ContainerType,
            Unpack[Levels],
        ],
        lower: IndexedScalarArgument[IndexType, ScalarClipValueType, ArgumentOrderType],
        upper: IndexedScalarArgument[IndexType, ScalarClipValueType, ArgumentOrderType],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, ScalarClipValueType],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def clip(
        self: Expression[
            BindingType, Bare[ScalarClipValueType], ContainerType, Unpack[Levels]
        ],
        lower: BareScalarArgument[ScalarClipValueType],
        upper: BareScalarArgument[ScalarClipValueType],
    ) -> Expression[
        BindingType, Bare[ScalarClipValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def clip(
        self: Expression[
            BindingType,
            Indexed[IndexType, AttributeClipValueType],
            ContainerType,
            Unpack[Levels],
        ],
        lower: IndexedAttributeArgument[
            IndexType, AttributeClipValueType, ArgumentOrderType
        ],
        upper: IndexedAttributeArgument[
            IndexType, AttributeClipValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, AttributeClipValueType],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def clip(
        self: Expression[
            BindingType, Bare[AttributeClipValueType], ContainerType, Unpack[Levels]
        ],
        lower: BareAttributeArgument[AttributeClipValueType],
        upper: BareAttributeArgument[AttributeClipValueType],
    ) -> Expression[
        BindingType, Bare[AttributeClipValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def clip(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[Positional]],
            ContainerType,
            Unpack[Levels],
        ],
        lower: IndexedIntegerArgument[
            IndexType, IndexValue[Positional], ArgumentOrderType
        ],
        upper: IndexedIntegerArgument[
            IndexType, IndexValue[Positional], ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[Positional]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def clip(
        self: Expression[
            BindingType, Bare[IndexValue[Positional]], ContainerType, Unpack[Levels]
        ],
        lower: BareIntegerArgument[IndexValue[Positional]],
        upper: BareIntegerArgument[IndexValue[Positional]],
    ) -> Expression[
        BindingType, Bare[IndexValue[Positional]], ContainerType, Unpack[Levels]
    ]: ...

    def clip(
        self,
        lower: Union[
            ScalarValue,
            Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
        upper: Union[
            ScalarValue,
            Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
    ) -> Any:
        return self._rebuild(
            self._py_carrier.clip(
                Expression._to_argument(lower), Expression._to_argument(upper)
            )
        )

    @overload
    def cast(
        self: Expression[
            BindingType,
            Indexed[IndexType, CastableValueType],
            ContainerType,
            Unpack[Levels],
        ],
        target: CastTarget[CastableValueType],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, CastableValueType],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def cast(
        self: Expression[
            BindingType, Bare[CastableValueType], ContainerType, Unpack[Levels]
        ],
        target: CastTarget[CastableValueType],
    ) -> Expression[
        BindingType, Bare[CastableValueType], ContainerType, Unpack[Levels]
    ]: ...

    def cast(self, target: CastTarget[Any]) -> Any:
        return self._rebuild(self._py_carrier.cast(target._py_cast_target))

    @overload
    def __eq__(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[ValueIndexType]],
            Definite,
            Unpack[Levels],
        ],
        argument: IndexedDroppingArgument[IndexType, IndexValue[ValueIndexType]],
    ) -> Expression[BindingType, Indexed[IndexType, Mask], Single, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[ValueIndexType]],
            DroppedContainerType,
            Unpack[Levels],
        ],
        argument: IndexedDroppingArgument[IndexType, IndexValue[ValueIndexType]],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def __eq__(
        self: Expression[
            BindingType, Bare[IndexValue[ValueIndexType]], Definite, Unpack[Levels]
        ],
        argument: BareDroppingArgument[IndexValue[ValueIndexType]],
    ) -> Expression[BindingType, Bare[Mask], Single, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Expression[
            BindingType,
            Bare[IndexValue[ValueIndexType]],
            DroppedContainerType,
            Unpack[Levels],
        ],
        argument: BareDroppingArgument[IndexValue[ValueIndexType]],
    ) -> Expression[BindingType, Bare[Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[ValueIndexType]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: IndexedExpressionArgument[
            IndexType, IndexValue[ValueIndexType], ArgumentOrderType
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def __eq__(
        self: Expression[
            BindingType, Bare[IndexValue[ValueIndexType]], ContainerType, Unpack[Levels]
        ],
        argument: BareExpressionArgument[IndexValue[ValueIndexType]],
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Expression[
            BindingType,
            Indexed[IndexType, EquatableValueType],
            Definite,
            Unpack[Levels],
        ],
        argument: IndexedDroppingArgument[IndexType, EquatableValueType],
    ) -> Expression[BindingType, Indexed[IndexType, Mask], Single, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Expression[
            BindingType,
            Indexed[IndexType, EquatableValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        argument: IndexedDroppingArgument[IndexType, EquatableValueType],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def __eq__(
        self: Expression[
            BindingType, Bare[EquatableValueType], Definite, Unpack[Levels]
        ],
        argument: BareDroppingArgument[EquatableValueType],
    ) -> Expression[BindingType, Bare[Mask], Single, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Expression[
            BindingType, Bare[EquatableValueType], DroppedContainerType, Unpack[Levels]
        ],
        argument: BareDroppingArgument[EquatableValueType],
    ) -> Expression[BindingType, Bare[Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Expression[
            BindingType,
            Indexed[IndexType, EquatableValueType],
            ContainerType,
            Unpack[Levels],
        ],
        argument: IndexedExpressionArgument[
            IndexType, EquatableValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def __eq__(
        self: Expression[
            BindingType, Bare[EquatableValueType], ContainerType, Unpack[Levels]
        ],
        argument: BareExpressionArgument[EquatableValueType],
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Expression[
            BindingType, Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]
        ],
        argument: ScalarValue,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def __eq__(
        self: Expression[BindingType, Bare[Scalar], ContainerType, Unpack[Levels]],
        argument: ScalarValue,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Expression[
            BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
        ],
        argument: _BooleanValue,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def __eq__(
        self: Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]],
        argument: _BooleanValue,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Expression[
            BindingType,
            Indexed[IndexType, AttributeName],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def __eq__(
        self: Expression[
            BindingType, Bare[AttributeName], ContainerType, Unpack[Levels]
        ],
        argument: Attribute,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Expression[
            BindingType,
            Indexed[IndexType, FailureKindValue],
            ContainerType,
            Unpack[Levels],
        ],
        argument: FailureKind,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def __eq__(
        self: Expression[
            BindingType, Bare[FailureKindValue], ContainerType, Unpack[Levels]
        ],
        argument: FailureKind,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[FailureKindIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: FailureKind,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def __eq__(
        self: Expression[
            BindingType,
            Bare[IndexValue[FailureKindIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: FailureKind,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[EndpointRole]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: EdgeEndpointRole,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def __eq__(
        self: Expression[
            BindingType,
            Bare[IndexValue[EndpointRole]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: EdgeEndpointRole,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[NodeIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def __eq__(
        self: Expression[
            BindingType, Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
        ],
        argument: Attribute,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[GroupIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def __eq__(
        self: Expression[
            BindingType, Bare[IndexValue[GroupIndex]], ContainerType, Unpack[Levels]
        ],
        argument: Attribute,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[ValueIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: ScalarValue,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def __eq__(
        self: Expression[
            BindingType, Bare[IndexValue[ValueIndex]], ContainerType, Unpack[Levels]
        ],
        argument: ScalarValue,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def __eq__(
        self: Expression[
            BindingType,
            Bare[IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[BoolIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: _BooleanValue,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def __eq__(
        self: Expression[
            BindingType, Bare[IndexValue[BoolIndex]], ContainerType, Unpack[Levels]
        ],
        argument: _BooleanValue,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[EdgeIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: EdgeIndexPayload,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def __eq__(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[Positional]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: int,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def __eq__(
        self: Expression[
            BindingType, Bare[IndexValue[EdgeIndex]], ContainerType, Unpack[Levels]
        ],
        argument: EdgeIndexPayload,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __eq__(
        self: Expression[
            BindingType, Bare[IndexValue[Positional]], ContainerType, Unpack[Levels]
        ],
        argument: int,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def __eq__(
        self,
        argument: Union[
            ScalarValue,
            EdgeIndexPayload,
            FailureKind,
            EdgeEndpointRole,
            Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
    ) -> Any:
        return self._rebuild(
            self._py_carrier.equal_to(Expression._to_argument(argument))
        )

    equal_to = __eq__

    @overload
    def __ne__(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[ValueIndexType]],
            Definite,
            Unpack[Levels],
        ],
        argument: IndexedDroppingArgument[IndexType, IndexValue[ValueIndexType]],
    ) -> Expression[BindingType, Indexed[IndexType, Mask], Single, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[ValueIndexType]],
            DroppedContainerType,
            Unpack[Levels],
        ],
        argument: IndexedDroppingArgument[IndexType, IndexValue[ValueIndexType]],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def __ne__(
        self: Expression[
            BindingType, Bare[IndexValue[ValueIndexType]], Definite, Unpack[Levels]
        ],
        argument: BareDroppingArgument[IndexValue[ValueIndexType]],
    ) -> Expression[BindingType, Bare[Mask], Single, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Expression[
            BindingType,
            Bare[IndexValue[ValueIndexType]],
            DroppedContainerType,
            Unpack[Levels],
        ],
        argument: BareDroppingArgument[IndexValue[ValueIndexType]],
    ) -> Expression[BindingType, Bare[Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[ValueIndexType]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: IndexedExpressionArgument[
            IndexType, IndexValue[ValueIndexType], ArgumentOrderType
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def __ne__(
        self: Expression[
            BindingType, Bare[IndexValue[ValueIndexType]], ContainerType, Unpack[Levels]
        ],
        argument: BareExpressionArgument[IndexValue[ValueIndexType]],
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Expression[
            BindingType,
            Indexed[IndexType, EquatableValueType],
            Definite,
            Unpack[Levels],
        ],
        argument: IndexedDroppingArgument[IndexType, EquatableValueType],
    ) -> Expression[BindingType, Indexed[IndexType, Mask], Single, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Expression[
            BindingType,
            Indexed[IndexType, EquatableValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        argument: IndexedDroppingArgument[IndexType, EquatableValueType],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def __ne__(
        self: Expression[
            BindingType, Bare[EquatableValueType], Definite, Unpack[Levels]
        ],
        argument: BareDroppingArgument[EquatableValueType],
    ) -> Expression[BindingType, Bare[Mask], Single, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Expression[
            BindingType, Bare[EquatableValueType], DroppedContainerType, Unpack[Levels]
        ],
        argument: BareDroppingArgument[EquatableValueType],
    ) -> Expression[BindingType, Bare[Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Expression[
            BindingType,
            Indexed[IndexType, EquatableValueType],
            ContainerType,
            Unpack[Levels],
        ],
        argument: IndexedExpressionArgument[
            IndexType, EquatableValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def __ne__(
        self: Expression[
            BindingType, Bare[EquatableValueType], ContainerType, Unpack[Levels]
        ],
        argument: BareExpressionArgument[EquatableValueType],
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Expression[
            BindingType, Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]
        ],
        argument: ScalarValue,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def __ne__(
        self: Expression[BindingType, Bare[Scalar], ContainerType, Unpack[Levels]],
        argument: ScalarValue,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Expression[
            BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
        ],
        argument: _BooleanValue,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def __ne__(
        self: Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]],
        argument: _BooleanValue,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Expression[
            BindingType,
            Indexed[IndexType, AttributeName],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def __ne__(
        self: Expression[
            BindingType, Bare[AttributeName], ContainerType, Unpack[Levels]
        ],
        argument: Attribute,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Expression[
            BindingType,
            Indexed[IndexType, FailureKindValue],
            ContainerType,
            Unpack[Levels],
        ],
        argument: FailureKind,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def __ne__(
        self: Expression[
            BindingType, Bare[FailureKindValue], ContainerType, Unpack[Levels]
        ],
        argument: FailureKind,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[FailureKindIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: FailureKind,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def __ne__(
        self: Expression[
            BindingType,
            Bare[IndexValue[FailureKindIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: FailureKind,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[EndpointRole]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: EdgeEndpointRole,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def __ne__(
        self: Expression[
            BindingType,
            Bare[IndexValue[EndpointRole]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: EdgeEndpointRole,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[NodeIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def __ne__(
        self: Expression[
            BindingType, Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
        ],
        argument: Attribute,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[GroupIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def __ne__(
        self: Expression[
            BindingType, Bare[IndexValue[GroupIndex]], ContainerType, Unpack[Levels]
        ],
        argument: Attribute,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[ValueIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: ScalarValue,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def __ne__(
        self: Expression[
            BindingType, Bare[IndexValue[ValueIndex]], ContainerType, Unpack[Levels]
        ],
        argument: ScalarValue,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def __ne__(
        self: Expression[
            BindingType,
            Bare[IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[BoolIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: _BooleanValue,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def __ne__(
        self: Expression[
            BindingType, Bare[IndexValue[BoolIndex]], ContainerType, Unpack[Levels]
        ],
        argument: _BooleanValue,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[EdgeIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: EdgeIndexPayload,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def __ne__(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[Positional]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: int,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def __ne__(
        self: Expression[
            BindingType, Bare[IndexValue[EdgeIndex]], ContainerType, Unpack[Levels]
        ],
        argument: EdgeIndexPayload,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def __ne__(
        self: Expression[
            BindingType, Bare[IndexValue[Positional]], ContainerType, Unpack[Levels]
        ],
        argument: int,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def __ne__(
        self,
        argument: Union[
            ScalarValue,
            EdgeIndexPayload,
            FailureKind,
            EdgeEndpointRole,
            Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
    ) -> Any:
        return self._rebuild(
            self._py_carrier.not_equal_to(Expression._to_argument(argument))
        )

    not_equal_to = __ne__

    @overload
    def greater_than(
        self: Expression[
            BindingType,
            Indexed[IndexType, OrderableValueType],
            Definite,
            Unpack[Levels],
        ],
        argument: IndexedDroppingArgument[IndexType, OrderableValueType],
    ) -> Expression[BindingType, Indexed[IndexType, Mask], Single, Unpack[Levels]]: ...

    @overload
    def greater_than(
        self: Expression[
            BindingType,
            Indexed[IndexType, OrderableValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        argument: IndexedDroppingArgument[IndexType, OrderableValueType],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def greater_than(
        self: Expression[
            BindingType, Bare[OrderableValueType], Definite, Unpack[Levels]
        ],
        argument: BareDroppingArgument[OrderableValueType],
    ) -> Expression[BindingType, Bare[Mask], Single, Unpack[Levels]]: ...

    @overload
    def greater_than(
        self: Expression[
            BindingType, Bare[OrderableValueType], DroppedContainerType, Unpack[Levels]
        ],
        argument: BareDroppingArgument[OrderableValueType],
    ) -> Expression[BindingType, Bare[Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than(
        self: Expression[
            BindingType,
            Indexed[IndexType, OrderableValueType],
            ContainerType,
            Unpack[Levels],
        ],
        argument: IndexedExpressionArgument[
            IndexType, OrderableValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def greater_than(
        self: Expression[
            BindingType, Bare[OrderableValueType], ContainerType, Unpack[Levels]
        ],
        argument: BareExpressionArgument[OrderableValueType],
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than(
        self: Expression[
            BindingType, Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]
        ],
        argument: ScalarValue,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def greater_than(
        self: Expression[BindingType, Bare[Scalar], ContainerType, Unpack[Levels]],
        argument: ScalarValue,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than(
        self: Expression[
            BindingType,
            Indexed[IndexType, AttributeName],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def greater_than(
        self: Expression[
            BindingType, Bare[AttributeName], ContainerType, Unpack[Levels]
        ],
        argument: Attribute,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[NodeIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def greater_than(
        self: Expression[
            BindingType, Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
        ],
        argument: Attribute,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[GroupIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def greater_than(
        self: Expression[
            BindingType, Bare[IndexValue[GroupIndex]], ContainerType, Unpack[Levels]
        ],
        argument: Attribute,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[ValueIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: ScalarValue,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def greater_than(
        self: Expression[
            BindingType, Bare[IndexValue[ValueIndex]], ContainerType, Unpack[Levels]
        ],
        argument: ScalarValue,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def greater_than(
        self: Expression[
            BindingType,
            Bare[IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[BoolIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: _BooleanValue,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def greater_than(
        self: Expression[
            BindingType, Bare[IndexValue[BoolIndex]], ContainerType, Unpack[Levels]
        ],
        argument: _BooleanValue,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[Positional]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: int,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def greater_than(
        self: Expression[
            BindingType, Bare[IndexValue[Positional]], ContainerType, Unpack[Levels]
        ],
        argument: int,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def greater_than(
        self,
        argument: Union[
            ScalarValue,
            Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
    ) -> Any:
        return self._rebuild(
            self._py_carrier.greater_than(Expression._to_argument(argument))
        )

    @overload
    def greater_than_or_equal_to(
        self: Expression[
            BindingType,
            Indexed[IndexType, OrderableValueType],
            Definite,
            Unpack[Levels],
        ],
        argument: IndexedDroppingArgument[IndexType, OrderableValueType],
    ) -> Expression[BindingType, Indexed[IndexType, Mask], Single, Unpack[Levels]]: ...

    @overload
    def greater_than_or_equal_to(
        self: Expression[
            BindingType,
            Indexed[IndexType, OrderableValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        argument: IndexedDroppingArgument[IndexType, OrderableValueType],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def greater_than_or_equal_to(
        self: Expression[
            BindingType, Bare[OrderableValueType], Definite, Unpack[Levels]
        ],
        argument: BareDroppingArgument[OrderableValueType],
    ) -> Expression[BindingType, Bare[Mask], Single, Unpack[Levels]]: ...

    @overload
    def greater_than_or_equal_to(
        self: Expression[
            BindingType, Bare[OrderableValueType], DroppedContainerType, Unpack[Levels]
        ],
        argument: BareDroppingArgument[OrderableValueType],
    ) -> Expression[BindingType, Bare[Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than_or_equal_to(
        self: Expression[
            BindingType,
            Indexed[IndexType, OrderableValueType],
            ContainerType,
            Unpack[Levels],
        ],
        argument: IndexedExpressionArgument[
            IndexType, OrderableValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def greater_than_or_equal_to(
        self: Expression[
            BindingType, Bare[OrderableValueType], ContainerType, Unpack[Levels]
        ],
        argument: BareExpressionArgument[OrderableValueType],
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than_or_equal_to(
        self: Expression[
            BindingType, Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]
        ],
        argument: ScalarValue,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def greater_than_or_equal_to(
        self: Expression[BindingType, Bare[Scalar], ContainerType, Unpack[Levels]],
        argument: ScalarValue,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than_or_equal_to(
        self: Expression[
            BindingType,
            Indexed[IndexType, AttributeName],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def greater_than_or_equal_to(
        self: Expression[
            BindingType, Bare[AttributeName], ContainerType, Unpack[Levels]
        ],
        argument: Attribute,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than_or_equal_to(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[NodeIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def greater_than_or_equal_to(
        self: Expression[
            BindingType, Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
        ],
        argument: Attribute,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than_or_equal_to(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[GroupIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def greater_than_or_equal_to(
        self: Expression[
            BindingType, Bare[IndexValue[GroupIndex]], ContainerType, Unpack[Levels]
        ],
        argument: Attribute,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than_or_equal_to(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[ValueIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: ScalarValue,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def greater_than_or_equal_to(
        self: Expression[
            BindingType, Bare[IndexValue[ValueIndex]], ContainerType, Unpack[Levels]
        ],
        argument: ScalarValue,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than_or_equal_to(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def greater_than_or_equal_to(
        self: Expression[
            BindingType,
            Bare[IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than_or_equal_to(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[BoolIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: _BooleanValue,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def greater_than_or_equal_to(
        self: Expression[
            BindingType, Bare[IndexValue[BoolIndex]], ContainerType, Unpack[Levels]
        ],
        argument: _BooleanValue,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def greater_than_or_equal_to(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[Positional]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: int,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def greater_than_or_equal_to(
        self: Expression[
            BindingType, Bare[IndexValue[Positional]], ContainerType, Unpack[Levels]
        ],
        argument: int,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def greater_than_or_equal_to(
        self,
        argument: Union[
            ScalarValue,
            Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
    ) -> Any:
        return self._rebuild(
            self._py_carrier.greater_than_or_equal_to(Expression._to_argument(argument))
        )

    @overload
    def less_than(
        self: Expression[
            BindingType,
            Indexed[IndexType, OrderableValueType],
            Definite,
            Unpack[Levels],
        ],
        argument: IndexedDroppingArgument[IndexType, OrderableValueType],
    ) -> Expression[BindingType, Indexed[IndexType, Mask], Single, Unpack[Levels]]: ...

    @overload
    def less_than(
        self: Expression[
            BindingType,
            Indexed[IndexType, OrderableValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        argument: IndexedDroppingArgument[IndexType, OrderableValueType],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def less_than(
        self: Expression[
            BindingType, Bare[OrderableValueType], Definite, Unpack[Levels]
        ],
        argument: BareDroppingArgument[OrderableValueType],
    ) -> Expression[BindingType, Bare[Mask], Single, Unpack[Levels]]: ...

    @overload
    def less_than(
        self: Expression[
            BindingType, Bare[OrderableValueType], DroppedContainerType, Unpack[Levels]
        ],
        argument: BareDroppingArgument[OrderableValueType],
    ) -> Expression[BindingType, Bare[Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def less_than(
        self: Expression[
            BindingType,
            Indexed[IndexType, OrderableValueType],
            ContainerType,
            Unpack[Levels],
        ],
        argument: IndexedExpressionArgument[
            IndexType, OrderableValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def less_than(
        self: Expression[
            BindingType, Bare[OrderableValueType], ContainerType, Unpack[Levels]
        ],
        argument: BareExpressionArgument[OrderableValueType],
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than(
        self: Expression[
            BindingType, Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]
        ],
        argument: ScalarValue,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def less_than(
        self: Expression[BindingType, Bare[Scalar], ContainerType, Unpack[Levels]],
        argument: ScalarValue,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than(
        self: Expression[
            BindingType,
            Indexed[IndexType, AttributeName],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def less_than(
        self: Expression[
            BindingType, Bare[AttributeName], ContainerType, Unpack[Levels]
        ],
        argument: Attribute,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[NodeIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def less_than(
        self: Expression[
            BindingType, Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
        ],
        argument: Attribute,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[GroupIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def less_than(
        self: Expression[
            BindingType, Bare[IndexValue[GroupIndex]], ContainerType, Unpack[Levels]
        ],
        argument: Attribute,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[ValueIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: ScalarValue,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def less_than(
        self: Expression[
            BindingType, Bare[IndexValue[ValueIndex]], ContainerType, Unpack[Levels]
        ],
        argument: ScalarValue,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def less_than(
        self: Expression[
            BindingType,
            Bare[IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[BoolIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: _BooleanValue,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def less_than(
        self: Expression[
            BindingType, Bare[IndexValue[BoolIndex]], ContainerType, Unpack[Levels]
        ],
        argument: _BooleanValue,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[Positional]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: int,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def less_than(
        self: Expression[
            BindingType, Bare[IndexValue[Positional]], ContainerType, Unpack[Levels]
        ],
        argument: int,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def less_than(
        self,
        argument: Union[
            ScalarValue,
            Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
    ) -> Any:
        return self._rebuild(
            self._py_carrier.less_than(Expression._to_argument(argument))
        )

    @overload
    def less_than_or_equal_to(
        self: Expression[
            BindingType,
            Indexed[IndexType, OrderableValueType],
            Definite,
            Unpack[Levels],
        ],
        argument: IndexedDroppingArgument[IndexType, OrderableValueType],
    ) -> Expression[BindingType, Indexed[IndexType, Mask], Single, Unpack[Levels]]: ...

    @overload
    def less_than_or_equal_to(
        self: Expression[
            BindingType,
            Indexed[IndexType, OrderableValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        argument: IndexedDroppingArgument[IndexType, OrderableValueType],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def less_than_or_equal_to(
        self: Expression[
            BindingType, Bare[OrderableValueType], Definite, Unpack[Levels]
        ],
        argument: BareDroppingArgument[OrderableValueType],
    ) -> Expression[BindingType, Bare[Mask], Single, Unpack[Levels]]: ...

    @overload
    def less_than_or_equal_to(
        self: Expression[
            BindingType, Bare[OrderableValueType], DroppedContainerType, Unpack[Levels]
        ],
        argument: BareDroppingArgument[OrderableValueType],
    ) -> Expression[BindingType, Bare[Mask], DroppedContainerType, Unpack[Levels]]: ...

    @overload
    def less_than_or_equal_to(
        self: Expression[
            BindingType,
            Indexed[IndexType, OrderableValueType],
            ContainerType,
            Unpack[Levels],
        ],
        argument: IndexedExpressionArgument[
            IndexType, OrderableValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def less_than_or_equal_to(
        self: Expression[
            BindingType, Bare[OrderableValueType], ContainerType, Unpack[Levels]
        ],
        argument: BareExpressionArgument[OrderableValueType],
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than_or_equal_to(
        self: Expression[
            BindingType, Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]
        ],
        argument: ScalarValue,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def less_than_or_equal_to(
        self: Expression[BindingType, Bare[Scalar], ContainerType, Unpack[Levels]],
        argument: ScalarValue,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than_or_equal_to(
        self: Expression[
            BindingType,
            Indexed[IndexType, AttributeName],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def less_than_or_equal_to(
        self: Expression[
            BindingType, Bare[AttributeName], ContainerType, Unpack[Levels]
        ],
        argument: Attribute,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than_or_equal_to(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[NodeIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def less_than_or_equal_to(
        self: Expression[
            BindingType, Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
        ],
        argument: Attribute,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than_or_equal_to(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[GroupIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def less_than_or_equal_to(
        self: Expression[
            BindingType, Bare[IndexValue[GroupIndex]], ContainerType, Unpack[Levels]
        ],
        argument: Attribute,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than_or_equal_to(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[ValueIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: ScalarValue,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def less_than_or_equal_to(
        self: Expression[
            BindingType, Bare[IndexValue[ValueIndex]], ContainerType, Unpack[Levels]
        ],
        argument: ScalarValue,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than_or_equal_to(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def less_than_or_equal_to(
        self: Expression[
            BindingType,
            Bare[IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Attribute,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than_or_equal_to(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[BoolIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: _BooleanValue,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def less_than_or_equal_to(
        self: Expression[
            BindingType, Bare[IndexValue[BoolIndex]], ContainerType, Unpack[Levels]
        ],
        argument: _BooleanValue,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def less_than_or_equal_to(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[Positional]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: int,
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def less_than_or_equal_to(
        self: Expression[
            BindingType, Bare[IndexValue[Positional]], ContainerType, Unpack[Levels]
        ],
        argument: int,
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def less_than_or_equal_to(
        self,
        argument: Union[
            ScalarValue,
            Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
    ) -> Any:
        return self._rebuild(
            self._py_carrier.less_than_or_equal_to(Expression._to_argument(argument))
        )

    @overload
    def is_in(
        self: Expression[
            BindingType,
            Indexed[IndexType, MembershipValueType],
            ContainerType,
            Unpack[Levels],
        ],
        argument: MembershipArgument[MembershipValueType],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def is_in(
        self: Expression[
            BindingType, Bare[MembershipValueType], ContainerType, Unpack[Levels]
        ],
        argument: MembershipArgument[MembershipValueType],
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def is_in(
        self: Expression[
            BindingType,
            Indexed[IndexType, ScalarMembershipValueType],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Sequence[ScalarValue],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def is_in(
        self: Expression[
            BindingType, Bare[ScalarMembershipValueType], ContainerType, Unpack[Levels]
        ],
        argument: Sequence[ScalarValue],
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def is_in(
        self: Expression[
            BindingType,
            Indexed[IndexType, BooleanMembershipValueType],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Sequence[_BooleanValue],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def is_in(
        self: Expression[
            BindingType, Bare[BooleanMembershipValueType], ContainerType, Unpack[Levels]
        ],
        argument: Sequence[_BooleanValue],
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def is_in(
        self: Expression[
            BindingType,
            Indexed[IndexType, AttributeMembershipValueType],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Sequence[Attribute],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def is_in(
        self: Expression[
            BindingType,
            Bare[AttributeMembershipValueType],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Sequence[Attribute],
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def is_in(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[EdgeIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Sequence[EdgeIndexPayload],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def is_in(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[Positional]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Sequence[int],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def is_in(
        self: Expression[
            BindingType, Bare[IndexValue[EdgeIndex]], ContainerType, Unpack[Levels]
        ],
        argument: Sequence[EdgeIndexPayload],
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def is_in(
        self: Expression[
            BindingType, Bare[IndexValue[Positional]], ContainerType, Unpack[Levels]
        ],
        argument: Sequence[int],
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def is_in(
        self: Expression[
            BindingType,
            Indexed[IndexType, FailureKindMembershipValueType],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Sequence[FailureKind],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def is_in(
        self: Expression[
            BindingType,
            Bare[FailureKindMembershipValueType],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Sequence[FailureKind],
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def is_in(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[EndpointRole]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Sequence[EdgeEndpointRole],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def is_in(
        self: Expression[
            BindingType,
            Bare[IndexValue[EndpointRole]],
            ContainerType,
            Unpack[Levels],
        ],
        argument: Sequence[EdgeEndpointRole],
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    def is_in(
        self,
        argument: Union[
            Sequence[ScalarValue],
            Sequence[EdgeIndexPayload],
            Sequence[FailureKind],
            Sequence[EdgeEndpointRole],
            Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
        ],
    ) -> Any:
        if isinstance(argument, Expression):
            return self._rebuild(
                self._py_carrier.is_in(Expression._to_argument(argument))
            )

        if isinstance(argument, (str, bytes)):
            msg = "expected a sequence of values; `str` and `bytes` are single values"
            raise TypeError(msg)

        return self._rebuild(
            self._py_carrier.is_in(
                [Expression._to_argument(value) for value in argument]
            )
        )

    @overload
    def index(
        self: Expression[
            BindingType, Indexed[IndexType, Unit], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[IndexType]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def index(
        self: Expression[
            BindingType,
            Indexed[IndexType, NodeReference],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[NodeIndex]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def index(
        self: Expression[
            BindingType,
            Indexed[IndexType, EdgeReference],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[EdgeIndex]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def index(
        self: Expression[
            BindingType,
            Indexed[IndexType, GroupReference],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[GroupIndex]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    def index(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.index())

    def discard_index(
        self: Expression[
            BindingType,
            Indexed[IndexType, BareValueType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[BindingType, Bare[BareValueType], ContainerType, Unpack[Levels]]:
        return self._rebuild(self._py_carrier.discard_index())

    def discard_value(
        self: Expression[
            BindingType, Indexed[IndexType, V], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, Unit], ContainerType, Unpack[Levels]
    ]:
        return self._rebuild(self._py_carrier.discard_value())

    @overload
    def enumerate(
        self: Expression[
            BindingType, Indexed[IndexType, V], Multiple[Ordered], Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Indexed[Positional, V], Multiple[Ordered], Unpack[Levels]
    ]: ...

    @overload
    def enumerate(
        self: Expression[
            BindingType, Bare[BareValueType], Multiple[Ordered], Unpack[Levels]
        ],
    ) -> Expression[
        BindingType,
        Indexed[Positional, BareValueType],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    def enumerate(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.enumerate())

    @overload
    def errors(
        self: Expression[
            BindingType, Indexed[IndexType, V], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, FailureValue],
        Multiple[OrderType],
        Unpack[Levels],
    ]: ...

    @overload
    def errors(
        self: Expression[BindingType, Indexed[IndexType, V], Single, Unpack[Levels]],
    ) -> Expression[
        BindingType, Indexed[IndexType, FailureValue], Single, Unpack[Levels]
    ]: ...

    @overload
    def errors(
        self: Expression[BindingType, Indexed[IndexType, V], Definite, Unpack[Levels]],
    ) -> Expression[
        BindingType, Indexed[IndexType, FailureValue], Single, Unpack[Levels]
    ]: ...

    @overload
    def errors(
        self: Expression[
            BindingType, Bare[BareValueType], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Bare[FailureValue], Multiple[OrderType], Unpack[Levels]
    ]: ...

    @overload
    def errors(
        self: Expression[BindingType, Bare[BareValueType], Single, Unpack[Levels]],
    ) -> Expression[BindingType, Bare[FailureValue], Single, Unpack[Levels]]: ...

    @overload
    def errors(
        self: Expression[BindingType, Bare[BareValueType], Definite, Unpack[Levels]],
    ) -> Expression[BindingType, Bare[FailureValue], Single, Unpack[Levels]]: ...

    def errors(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.errors())

    @overload
    def on_error(
        self: Expression[BindingType, Indexed[IndexType, V], Definite, Unpack[Levels]],
        policy: Drop,
    ) -> Expression[BindingType, Indexed[IndexType, V], Single, Unpack[Levels]]: ...

    @overload
    def on_error(
        self: Expression[
            BindingType, Indexed[IndexType, V], DroppedContainerType, Unpack[Levels]
        ],
        policy: Drop,
    ) -> Expression[
        BindingType, Indexed[IndexType, V], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def on_error(
        self: Expression[BindingType, Bare[BareValueType], Definite, Unpack[Levels]],
        policy: Drop,
    ) -> Expression[BindingType, Bare[BareValueType], Single, Unpack[Levels]]: ...

    @overload
    def on_error(
        self: Expression[
            BindingType, Bare[BareValueType], DroppedContainerType, Unpack[Levels]
        ],
        policy: Drop,
    ) -> Expression[
        BindingType, Bare[BareValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def on_error(
        self: Expression[
            BindingType, Indexed[IndexType, V], ContainerType, Unpack[Levels]
        ],
        policy: Union[Raise, _RaiseWhen],
    ) -> Expression[
        BindingType, Indexed[IndexType, V], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def on_error(
        self: Expression[
            BindingType, Bare[BareValueType], ContainerType, Unpack[Levels]
        ],
        policy: Union[Raise, _RaiseWhen],
    ) -> Expression[
        BindingType, Bare[BareValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def on_error(
        self: Expression[
            BindingType,
            Indexed[IndexType, ReplaceableValueType],
            Definite,
            Unpack[Levels],
        ],
        policy: Replace[IndexedDroppingArgument[IndexType, ReplaceableValueType]],
    ) -> Expression[
        BindingType, Indexed[IndexType, ReplaceableValueType], Single, Unpack[Levels]
    ]: ...

    @overload
    def on_error(
        self: Expression[
            BindingType,
            Indexed[IndexType, ReplaceableValueType],
            DroppedContainerType,
            Unpack[Levels],
        ],
        policy: Replace[IndexedDroppingArgument[IndexType, ReplaceableValueType]],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, ReplaceableValueType],
        DroppedContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def on_error(
        self: Expression[BindingType, Bare[BareValueType], Definite, Unpack[Levels]],
        policy: Replace[BareDroppingArgument[BareValueType]],
    ) -> Expression[BindingType, Bare[BareValueType], Single, Unpack[Levels]]: ...

    @overload
    def on_error(
        self: Expression[
            BindingType, Bare[BareValueType], DroppedContainerType, Unpack[Levels]
        ],
        policy: Replace[BareDroppingArgument[BareValueType]],
    ) -> Expression[
        BindingType, Bare[BareValueType], DroppedContainerType, Unpack[Levels]
    ]: ...

    @overload
    def on_error(
        self: Expression[
            BindingType,
            Indexed[IndexType, ReplaceableValueType],
            ContainerType,
            Unpack[Levels],
        ],
        policy: Replace[
            IndexedExpressionArgument[
                IndexType, ReplaceableValueType, ArgumentOrderType
            ]
        ],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, ReplaceableValueType],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def on_error(
        self: Expression[
            BindingType, Bare[BareValueType], ContainerType, Unpack[Levels]
        ],
        policy: Replace[BareExpressionArgument[BareValueType]],
    ) -> Expression[
        BindingType, Bare[BareValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def on_error(
        self: Expression[
            BindingType, Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]
        ],
        policy: Replace[ScalarValue],
    ) -> Expression[
        BindingType, Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def on_error(
        self: Expression[BindingType, Bare[Scalar], ContainerType, Unpack[Levels]],
        policy: Replace[ScalarValue],
    ) -> Expression[BindingType, Bare[Scalar], ContainerType, Unpack[Levels]]: ...

    @overload
    def on_error(
        self: Expression[
            BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
        ],
        policy: Replace[_BooleanValue],
    ) -> Expression[
        BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def on_error(
        self: Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]],
        policy: Replace[_BooleanValue],
    ) -> Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]]: ...

    @overload
    def on_error(
        self: Expression[
            BindingType,
            Indexed[IndexType, AttributeName],
            ContainerType,
            Unpack[Levels],
        ],
        policy: Replace[Attribute],
    ) -> Expression[
        BindingType, Indexed[IndexType, AttributeName], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def on_error(
        self: Expression[
            BindingType, Bare[AttributeName], ContainerType, Unpack[Levels]
        ],
        policy: Replace[Attribute],
    ) -> Expression[
        BindingType, Bare[AttributeName], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def on_error(
        self: Expression[
            BindingType,
            Indexed[IndexType, FailureKindValue],
            ContainerType,
            Unpack[Levels],
        ],
        policy: Replace[FailureKind],
    ) -> Expression[
        BindingType, Indexed[IndexType, FailureKindValue], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def on_error(
        self: Expression[
            BindingType, Bare[FailureKindValue], ContainerType, Unpack[Levels]
        ],
        policy: Replace[FailureKind],
    ) -> Expression[
        BindingType, Bare[FailureKindValue], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def on_error(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[FailureKindIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        policy: Replace[FailureKind],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[FailureKindIndex]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def on_error(
        self: Expression[
            BindingType,
            Bare[IndexValue[FailureKindIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        policy: Replace[FailureKind],
    ) -> Expression[
        BindingType, Bare[IndexValue[FailureKindIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def on_error(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[EndpointRole]],
            ContainerType,
            Unpack[Levels],
        ],
        policy: Replace[EdgeEndpointRole],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[EndpointRole]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def on_error(
        self: Expression[
            BindingType,
            Bare[IndexValue[EndpointRole]],
            ContainerType,
            Unpack[Levels],
        ],
        policy: Replace[EdgeEndpointRole],
    ) -> Expression[
        BindingType, Bare[IndexValue[EndpointRole]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def on_error(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[NodeIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        policy: Replace[NodeIndexPayload],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[NodeIndex]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def on_error(
        self: Expression[
            BindingType, Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
        ],
        policy: Replace[NodeIndexPayload],
    ) -> Expression[
        BindingType, Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def on_error(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[GroupIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        policy: Replace[GroupIndexPayload],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[GroupIndex]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def on_error(
        self: Expression[
            BindingType, Bare[IndexValue[GroupIndex]], ContainerType, Unpack[Levels]
        ],
        policy: Replace[GroupIndexPayload],
    ) -> Expression[
        BindingType, Bare[IndexValue[GroupIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def on_error(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[EdgeIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        policy: Replace[EdgeIndexPayload],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[EdgeIndex]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def on_error(
        self: Expression[
            BindingType, Bare[IndexValue[EdgeIndex]], ContainerType, Unpack[Levels]
        ],
        policy: Replace[EdgeIndexPayload],
    ) -> Expression[
        BindingType, Bare[IndexValue[EdgeIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def on_error(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[ValueIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        policy: Replace[ScalarValue],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[ValueIndex]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def on_error(
        self: Expression[
            BindingType, Bare[IndexValue[ValueIndex]], ContainerType, Unpack[Levels]
        ],
        policy: Replace[ScalarValue],
    ) -> Expression[
        BindingType, Bare[IndexValue[ValueIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def on_error(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        policy: Replace[Attribute],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[AttributeNameIndex]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def on_error(
        self: Expression[
            BindingType,
            Bare[IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        policy: Replace[Attribute],
    ) -> Expression[
        BindingType, Bare[IndexValue[AttributeNameIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def on_error(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[BoolIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        policy: Replace[_BooleanValue],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[BoolIndex]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def on_error(
        self: Expression[
            BindingType, Bare[IndexValue[BoolIndex]], ContainerType, Unpack[Levels]
        ],
        policy: Replace[_BooleanValue],
    ) -> Expression[
        BindingType, Bare[IndexValue[BoolIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def on_error(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[Positional]],
            ContainerType,
            Unpack[Levels],
        ],
        policy: Replace[int],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[Positional]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def on_error(
        self: Expression[
            BindingType, Bare[IndexValue[Positional]], ContainerType, Unpack[Levels]
        ],
        policy: Replace[int],
    ) -> Expression[
        BindingType, Bare[IndexValue[Positional]], ContainerType, Unpack[Levels]
    ]: ...

    def on_error(
        self,
        policy: Union[
            Drop,
            Raise,
            _RaiseWhen,
            Replace[
                Union[
                    ScalarValue,
                    EdgeIndexPayload,
                    FailureKind,
                    EdgeEndpointRole,
                    Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
                    Argument[Any, Any],
                ]
            ],
        ],
    ) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        if isinstance(policy, Drop):
            return self._rebuild(self._py_carrier.on_error_drop())

        if isinstance(policy, _RaiseWhen):
            return self._rebuild(
                self._py_carrier.on_error_raise_when(
                    Expression._to_argument(policy._condition)
                )
            )

        if isinstance(policy, Replace):
            return self._rebuild(
                self._py_carrier.on_error_replace(
                    Expression._to_argument(policy._replacement)
                )
            )

        return self._rebuild(self._py_carrier.on_error_raise())

    @overload
    def kind(
        self: Expression[
            BindingType, Indexed[IndexType, FailureValue], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, FailureKindValue], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def kind(
        self: Expression[
            BindingType, Bare[FailureValue], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Bare[FailureKindValue], ContainerType, Unpack[Levels]
    ]: ...

    def kind(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.kind())

    @overload
    def name(
        self: Expression[
            BindingType,
            Indexed[IndexType, FailureKindValue],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def name(
        self: Expression[
            BindingType, Bare[FailureKindValue], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[BindingType, Bare[Scalar], ContainerType, Unpack[Levels]]: ...

    def name(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.name())

    @overload
    def count(
        self: Expression[
            BindingType, Indexed[IndexType, V], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[BindingType, Bare[Scalar], Definite, Unpack[Levels]]: ...

    @overload
    def count(
        self: Expression[
            BindingType, Bare[BareValueType], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[BindingType, Bare[Scalar], Definite, Unpack[Levels]]: ...

    def count(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.count())

    @overload
    def sum(
        self: Expression[
            BindingType, Indexed[IndexType, Scalar], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Expression[BindingType, Bare[Scalar], Single, Unpack[Levels]]: ...

    @overload
    def sum(
        self: Expression[
            BindingType, Bare[Scalar], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Expression[BindingType, Bare[Scalar], Single, Unpack[Levels]]: ...

    @overload
    def sum(
        self: Expression[
            BindingType,
            Indexed[IndexType, AttributeName],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Expression[BindingType, Bare[AttributeName], Single, Unpack[Levels]]: ...

    @overload
    def sum(
        self: Expression[
            BindingType, Bare[AttributeName], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Expression[BindingType, Bare[AttributeName], Single, Unpack[Levels]]: ...

    @overload
    def sum(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[NodeIndex]],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Bare[IndexValue[NodeIndex]], Single, Unpack[Levels]
    ]: ...

    @overload
    def sum(
        self: Expression[
            BindingType,
            Bare[IndexValue[NodeIndex]],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Bare[IndexValue[NodeIndex]], Single, Unpack[Levels]
    ]: ...

    @overload
    def sum(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[AttributeNameIndex]],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Bare[IndexValue[AttributeNameIndex]], Single, Unpack[Levels]
    ]: ...

    @overload
    def sum(
        self: Expression[
            BindingType,
            Bare[IndexValue[AttributeNameIndex]],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Bare[IndexValue[AttributeNameIndex]], Single, Unpack[Levels]
    ]: ...

    @overload
    def sum(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[ValueIndex]],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Bare[IndexValue[ValueIndex]], Single, Unpack[Levels]
    ]: ...

    @overload
    def sum(
        self: Expression[
            BindingType,
            Bare[IndexValue[ValueIndex]],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Bare[IndexValue[ValueIndex]], Single, Unpack[Levels]
    ]: ...

    @overload
    def sum(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[Positional]],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Bare[IndexValue[Positional]], Single, Unpack[Levels]
    ]: ...

    @overload
    def sum(
        self: Expression[
            BindingType,
            Bare[IndexValue[Positional]],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Bare[IndexValue[Positional]], Single, Unpack[Levels]
    ]: ...

    def sum(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.sum())

    @overload
    def mean(
        self: Expression[
            BindingType,
            Indexed[IndexType, RealNumericValueType],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Bare[RealNumericValueType], Single, Unpack[Levels]
    ]: ...

    @overload
    def mean(
        self: Expression[
            BindingType, Bare[RealNumericValueType], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Bare[RealNumericValueType], Single, Unpack[Levels]
    ]: ...

    def mean(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.mean())

    @overload
    def std(
        self: Expression[
            BindingType,
            Indexed[IndexType, RealNumericValueType],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Expression[BindingType, Bare[Scalar], Single, Unpack[Levels]]: ...

    @overload
    def std(
        self: Expression[
            BindingType, Bare[RealNumericValueType], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Expression[BindingType, Bare[Scalar], Single, Unpack[Levels]]: ...

    def std(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.std())

    @overload
    def var(
        self: Expression[
            BindingType,
            Indexed[IndexType, RealNumericValueType],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Expression[BindingType, Bare[Scalar], Single, Unpack[Levels]]: ...

    @overload
    def var(
        self: Expression[
            BindingType, Bare[RealNumericValueType], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Expression[BindingType, Bare[Scalar], Single, Unpack[Levels]]: ...

    def var(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.var())

    @overload
    def all(
        self: Expression[
            BindingType, Indexed[IndexType, Mask], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Expression[BindingType, Bare[Mask], Definite, Unpack[Levels]]: ...

    @overload
    def all(
        self: Expression[BindingType, Bare[Mask], Multiple[OrderType], Unpack[Levels]],
    ) -> Expression[BindingType, Bare[Mask], Definite, Unpack[Levels]]: ...

    def all(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.all())

    @overload
    def any(
        self: Expression[
            BindingType, Indexed[IndexType, Mask], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Expression[BindingType, Bare[Mask], Definite, Unpack[Levels]]: ...

    @overload
    def any(
        self: Expression[BindingType, Bare[Mask], Multiple[OrderType], Unpack[Levels]],
    ) -> Expression[BindingType, Bare[Mask], Definite, Unpack[Levels]]: ...

    def any(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.any())

    @overload
    def max(
        self: Expression[
            BindingType,
            Indexed[IndexType, OrderableValueType],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Expression[BindingType, Bare[OrderableValueType], Single, Unpack[Levels]]: ...

    @overload
    def max(
        self: Expression[
            BindingType, Bare[OrderableValueType], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Expression[BindingType, Bare[OrderableValueType], Single, Unpack[Levels]]: ...

    def max(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.max())

    @overload
    def min(
        self: Expression[
            BindingType,
            Indexed[IndexType, OrderableValueType],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Expression[BindingType, Bare[OrderableValueType], Single, Unpack[Levels]]: ...

    @overload
    def min(
        self: Expression[
            BindingType, Bare[OrderableValueType], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Expression[BindingType, Bare[OrderableValueType], Single, Unpack[Levels]]: ...

    def min(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.min())

    @overload
    def median(
        self: Expression[
            BindingType,
            Indexed[IndexType, MedianValueType],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Expression[BindingType, Bare[MedianValueType], Single, Unpack[Levels]]: ...

    @overload
    def median(
        self: Expression[
            BindingType, Bare[MedianValueType], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Expression[BindingType, Bare[MedianValueType], Single, Unpack[Levels]]: ...

    def median(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.median())

    @overload
    def mode(
        self: Expression[
            BindingType,
            Indexed[IndexType, ModeValueType],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Bare[ModeValueType], Multiple[OrderType], Unpack[Levels]
    ]: ...

    @overload
    def mode(
        self: Expression[
            BindingType, Bare[ModeValueType], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Bare[ModeValueType], Multiple[OrderType], Unpack[Levels]
    ]: ...

    @overload
    def mode(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[ValueIndexType]],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Bare[IndexValue[ValueIndexType]],
        Multiple[OrderType],
        Unpack[Levels],
    ]: ...

    @overload
    def mode(
        self: Expression[
            BindingType,
            Bare[IndexValue[ValueIndexType]],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Bare[IndexValue[ValueIndexType]],
        Multiple[OrderType],
        Unpack[Levels],
    ]: ...

    def mode(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.mode())

    @overload
    def product(
        self: Expression[
            BindingType,
            Indexed[IndexType, MultipliableValueType],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Bare[MultipliableValueType], Single, Unpack[Levels]
    ]: ...

    @overload
    def product(
        self: Expression[
            BindingType,
            Bare[MultipliableValueType],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Bare[MultipliableValueType], Single, Unpack[Levels]
    ]: ...

    def product(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.product())

    @overload
    def n_unique(
        self: Expression[
            BindingType,
            Indexed[IndexType, EquivalentValueType],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Expression[BindingType, Bare[Scalar], Definite, Unpack[Levels]]: ...

    @overload
    def n_unique(
        self: Expression[
            BindingType, Bare[EquivalentValueType], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Expression[BindingType, Bare[Scalar], Definite, Unpack[Levels]]: ...

    @overload
    def n_unique(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[ValueIndexType]],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Expression[BindingType, Bare[Scalar], Definite, Unpack[Levels]]: ...

    @overload
    def n_unique(
        self: Expression[
            BindingType,
            Bare[IndexValue[ValueIndexType]],
            Multiple[OrderType],
            Unpack[Levels],
        ],
    ) -> Expression[BindingType, Bare[Scalar], Definite, Unpack[Levels]]: ...

    def n_unique(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.n_unique())

    @overload
    def random(
        self: Expression[
            BindingType, Indexed[IndexType, V], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Expression[BindingType, Indexed[IndexType, V], Single, Unpack[Levels]]: ...

    @overload
    def random(
        self: Expression[
            BindingType, Bare[BareValueType], Multiple[OrderType], Unpack[Levels]
        ],
    ) -> Expression[BindingType, Bare[BareValueType], Single, Unpack[Levels]]: ...

    def random(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.random())

    @overload
    def edges(
        self: Union[
            Expression[
                BindingType, Indexed[NodeIndex, Unit], ContainerType, Unpack[Levels]
            ],
            Expression[
                BindingType,
                Indexed[IndexType, NodeReference],
                ContainerType,
                Unpack[Levels],
            ],
        ],
        direction: Optional[EdgeDirection] = None,
    ) -> Expression[
        BindingType, Indexed[EdgeIndex, Unit], Multiple[Unordered], Unpack[Levels]
    ]: ...

    @overload
    def edges(
        self: Union[
            Expression[
                BindingType, Indexed[GroupIndex, Unit], ContainerType, Unpack[Levels]
            ],
            Expression[
                BindingType,
                Indexed[IndexType, GroupReference],
                ContainerType,
                Unpack[Levels],
            ],
        ],
    ) -> Expression[
        BindingType, Indexed[EdgeIndex, Unit], Multiple[Unordered], Unpack[Levels]
    ]: ...

    def edges(self, direction: Optional[EdgeDirection] = None) -> Any:
        followed = None if direction is None else direction._into_py_edge_direction()

        return self._rebuild(self._py_carrier.edges(followed))

    def neighbors(
        self: Union[
            Expression[
                BindingType, Indexed[NodeIndex, Unit], ContainerType, Unpack[Levels]
            ],
            Expression[
                BindingType,
                Indexed[IndexType, NodeReference],
                ContainerType,
                Unpack[Levels],
            ],
        ],
        direction: EdgeDirection = EdgeDirection.Both,
    ) -> Expression[
        BindingType, Indexed[NodeIndex, Unit], Multiple[Unordered], Unpack[Levels]
    ]:
        return self._rebuild(
            self._py_carrier.neighbors(direction._into_py_edge_direction())
        )

    @overload
    def via_edges(
        self: Expression[
            BindingType, Indexed[NodeIndex, Unit], ContainerType, Unpack[Levels]
        ],
        direction: Optional[EdgeDirection] = None,
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[NodeIndex, NodeReference],
            ContainerType,
            Unpack[Levels],
        ],
        direction: Optional[EdgeDirection] = None,
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[EdgeIndex, NodeReference],
            ContainerType,
            Unpack[Levels],
        ],
        direction: Optional[EdgeDirection] = None,
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[GroupIndex, NodeReference],
            ContainerType,
            Unpack[Levels],
        ],
        direction: Optional[EdgeDirection] = None,
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[
                GroupIndex,
                EdgeIndex,
                Tuple[GroupIndexPayload, Optional[EdgeIndexPayload]],
            ],
            EdgeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_edges(
        self: Expression[
            BindingType,
            Indexed[Positional, NodeReference],
            ContainerType,
            Unpack[Levels],
        ],
        direction: Optional[EdgeDirection] = None,
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[Positional, EdgeIndex, Tuple[int, Optional[EdgeIndexPayload]]],
            EdgeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_edges(
        self: Expression[
            BindingType,
            Indexed[EndpointRole, NodeReference],
            ContainerType,
            Unpack[Levels],
        ],
        direction: Optional[EdgeDirection] = None,
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[ValueIndex, NodeReference],
            ContainerType,
            Unpack[Levels],
        ],
        direction: Optional[EdgeDirection] = None,
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[AttributeNameIndex, NodeReference],
            ContainerType,
            Unpack[Levels],
        ],
        direction: Optional[EdgeDirection] = None,
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[BoolIndex, NodeReference],
            ContainerType,
            Unpack[Levels],
        ],
        direction: Optional[EdgeDirection] = None,
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[BoolIndex, EdgeIndex, Tuple[bool, Optional[EdgeIndexPayload]]],
            EdgeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_edges(
        self: Expression[
            BindingType,
            Indexed[FailureKindIndex, NodeReference],
            ContainerType,
            Unpack[Levels],
        ],
        direction: Optional[EdgeDirection] = None,
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[Expanded[K, ChildType, ParentPayloadType], NodeReference],
            ContainerType,
            Unpack[Levels],
        ],
        direction: Optional[EdgeDirection] = None,
    ) -> Expression[
        BindingType,
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

    @overload
    def via_edges(
        self: Expression[
            BindingType, Indexed[GroupIndex, Unit], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[
                GroupIndex,
                EdgeIndex,
                Tuple[GroupIndexPayload, Optional[EdgeIndexPayload]],
            ],
            EdgeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_edges(
        self: Expression[
            BindingType,
            Indexed[NodeIndex, GroupReference],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[EdgeIndex, GroupReference],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[GroupIndex, GroupReference],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[
                GroupIndex,
                EdgeIndex,
                Tuple[GroupIndexPayload, Optional[EdgeIndexPayload]],
            ],
            EdgeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_edges(
        self: Expression[
            BindingType,
            Indexed[Positional, GroupReference],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[Positional, EdgeIndex, Tuple[int, Optional[EdgeIndexPayload]]],
            EdgeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_edges(
        self: Expression[
            BindingType,
            Indexed[EndpointRole, GroupReference],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[ValueIndex, GroupReference],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[AttributeNameIndex, GroupReference],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[BoolIndex, GroupReference],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[BoolIndex, EdgeIndex, Tuple[bool, Optional[EdgeIndexPayload]]],
            EdgeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_edges(
        self: Expression[
            BindingType,
            Indexed[FailureKindIndex, GroupReference],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[Expanded[K, ChildType, ParentPayloadType], GroupReference],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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

    def via_edges(self, direction: Optional[EdgeDirection] = None) -> Any:
        followed = None if direction is None else direction._into_py_edge_direction()

        return self._rebuild(self._py_carrier.via_edges(followed))

    @overload
    def via_neighbors(
        self: Expression[
            BindingType, Indexed[NodeIndex, Unit], ContainerType, Unpack[Levels]
        ],
        direction: EdgeDirection = EdgeDirection.Both,
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[NodeIndex, NodeReference],
            ContainerType,
            Unpack[Levels],
        ],
        direction: EdgeDirection = EdgeDirection.Both,
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[EdgeIndex, NodeReference],
            ContainerType,
            Unpack[Levels],
        ],
        direction: EdgeDirection = EdgeDirection.Both,
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[GroupIndex, NodeReference],
            ContainerType,
            Unpack[Levels],
        ],
        direction: EdgeDirection = EdgeDirection.Both,
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[
                GroupIndex,
                NodeIndex,
                Tuple[GroupIndexPayload, Optional[NodeIndexPayload]],
            ],
            NodeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_neighbors(
        self: Expression[
            BindingType,
            Indexed[Positional, NodeReference],
            ContainerType,
            Unpack[Levels],
        ],
        direction: EdgeDirection = EdgeDirection.Both,
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[Positional, NodeIndex, Tuple[int, Optional[NodeIndexPayload]]],
            NodeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_neighbors(
        self: Expression[
            BindingType,
            Indexed[EndpointRole, NodeReference],
            ContainerType,
            Unpack[Levels],
        ],
        direction: EdgeDirection = EdgeDirection.Both,
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[ValueIndex, NodeReference],
            ContainerType,
            Unpack[Levels],
        ],
        direction: EdgeDirection = EdgeDirection.Both,
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[AttributeNameIndex, NodeReference],
            ContainerType,
            Unpack[Levels],
        ],
        direction: EdgeDirection = EdgeDirection.Both,
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[BoolIndex, NodeReference],
            ContainerType,
            Unpack[Levels],
        ],
        direction: EdgeDirection = EdgeDirection.Both,
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[BoolIndex, NodeIndex, Tuple[bool, Optional[NodeIndexPayload]]],
            NodeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_neighbors(
        self: Expression[
            BindingType,
            Indexed[FailureKindIndex, NodeReference],
            ContainerType,
            Unpack[Levels],
        ],
        direction: EdgeDirection = EdgeDirection.Both,
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[Expanded[K, ChildType, ParentPayloadType], NodeReference],
            ContainerType,
            Unpack[Levels],
        ],
        direction: EdgeDirection = EdgeDirection.Both,
    ) -> Expression[
        BindingType,
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

    def via_neighbors(self, direction: EdgeDirection = EdgeDirection.Both) -> Any:
        return self._rebuild(
            self._py_carrier.via_neighbors(direction._into_py_edge_direction())
        )

    def nodes(
        self: Union[
            Expression[
                BindingType, Indexed[EdgeIndex, Unit], ContainerType, Unpack[Levels]
            ],
            Expression[
                BindingType,
                Indexed[IndexType, EdgeReference],
                ContainerType,
                Unpack[Levels],
            ],
            Expression[
                BindingType, Indexed[GroupIndex, Unit], ContainerType, Unpack[Levels]
            ],
            Expression[
                BindingType,
                Indexed[IndexType, GroupReference],
                ContainerType,
                Unpack[Levels],
            ],
        ],
    ) -> Expression[
        BindingType, Indexed[NodeIndex, Unit], Multiple[Unordered], Unpack[Levels]
    ]:
        return self._rebuild(self._py_carrier.nodes())

    @overload
    def via_nodes(
        self: Expression[
            BindingType, Indexed[EdgeIndex, Unit], Multiple[Unordered], Unpack[Levels]
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[NodeIndex, EdgeReference],
            Multiple[Unordered],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[EdgeIndex, EdgeReference],
            Multiple[Unordered],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[GroupIndex, EdgeReference],
            Multiple[Unordered],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[
                GroupIndex,
                EndpointRole,
                Tuple[GroupIndexPayload, Optional[EdgeEndpointRole]],
            ],
            NodeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Expression[
            BindingType,
            Indexed[Positional, EdgeReference],
            Multiple[Unordered],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[Positional, EndpointRole, Tuple[int, Optional[EdgeEndpointRole]]],
            NodeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Expression[
            BindingType,
            Indexed[EndpointRole, EdgeReference],
            Multiple[Unordered],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[ValueIndex, EdgeReference],
            Multiple[Unordered],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[AttributeNameIndex, EdgeReference],
            Multiple[Unordered],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[BoolIndex, EdgeReference],
            Multiple[Unordered],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[BoolIndex, EndpointRole, Tuple[bool, Optional[EdgeEndpointRole]]],
            NodeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Expression[
            BindingType,
            Indexed[FailureKindIndex, EdgeReference],
            Multiple[Unordered],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[Expanded[K, ChildType, ParentPayloadType], EdgeReference],
            Multiple[Unordered],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType, Indexed[EdgeIndex, Unit], Multiple[Ordered], Unpack[Levels]
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[NodeIndex, EdgeReference],
            Multiple[Ordered],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[EdgeIndex, EdgeReference],
            Multiple[Ordered],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[GroupIndex, EdgeReference],
            Multiple[Ordered],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[
                GroupIndex,
                EndpointRole,
                Tuple[GroupIndexPayload, Optional[EdgeEndpointRole]],
            ],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Expression[
            BindingType,
            Indexed[Positional, EdgeReference],
            Multiple[Ordered],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[Positional, EndpointRole, Tuple[int, Optional[EdgeEndpointRole]]],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Expression[
            BindingType,
            Indexed[EndpointRole, EdgeReference],
            Multiple[Ordered],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[ValueIndex, EdgeReference],
            Multiple[Ordered],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[AttributeNameIndex, EdgeReference],
            Multiple[Ordered],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[BoolIndex, EdgeReference],
            Multiple[Ordered],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[BoolIndex, EndpointRole, Tuple[bool, Optional[EdgeEndpointRole]]],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Expression[
            BindingType,
            Indexed[FailureKindIndex, EdgeReference],
            Multiple[Ordered],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[Expanded[K, ChildType, ParentPayloadType], EdgeReference],
            Multiple[Ordered],
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[BindingType, Indexed[EdgeIndex, Unit], Single, Unpack[Levels]],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType, Indexed[NodeIndex, EdgeReference], Single, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType, Indexed[EdgeIndex, EdgeReference], Single, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType, Indexed[GroupIndex, EdgeReference], Single, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[
                GroupIndex,
                EndpointRole,
                Tuple[GroupIndexPayload, Optional[EdgeEndpointRole]],
            ],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Expression[
            BindingType, Indexed[Positional, EdgeReference], Single, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[Positional, EndpointRole, Tuple[int, Optional[EdgeEndpointRole]]],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Expression[
            BindingType, Indexed[EndpointRole, EdgeReference], Single, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType, Indexed[ValueIndex, EdgeReference], Single, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[AttributeNameIndex, EdgeReference],
            Single,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType, Indexed[BoolIndex, EdgeReference], Single, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[BoolIndex, EndpointRole, Tuple[bool, Optional[EdgeEndpointRole]]],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Expression[
            BindingType,
            Indexed[FailureKindIndex, EdgeReference],
            Single,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[Expanded[K, ChildType, ParentPayloadType], EdgeReference],
            Single,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType, Indexed[EdgeIndex, Unit], Definite, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType, Indexed[NodeIndex, EdgeReference], Definite, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType, Indexed[EdgeIndex, EdgeReference], Definite, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType, Indexed[GroupIndex, EdgeReference], Definite, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[
                GroupIndex,
                EndpointRole,
                Tuple[GroupIndexPayload, Optional[EdgeEndpointRole]],
            ],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Expression[
            BindingType, Indexed[Positional, EdgeReference], Definite, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[Positional, EndpointRole, Tuple[int, Optional[EdgeEndpointRole]]],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Expression[
            BindingType, Indexed[EndpointRole, EdgeReference], Definite, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType, Indexed[ValueIndex, EdgeReference], Definite, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[AttributeNameIndex, EdgeReference],
            Definite,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType, Indexed[BoolIndex, EdgeReference], Definite, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[BoolIndex, EndpointRole, Tuple[bool, Optional[EdgeEndpointRole]]],
            NodeReference,
        ],
        Multiple[Ordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Expression[
            BindingType,
            Indexed[FailureKindIndex, EdgeReference],
            Definite,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType,
            Indexed[Expanded[K, ChildType, ParentPayloadType], EdgeReference],
            Definite,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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
        self: Expression[
            BindingType, Indexed[GroupIndex, Unit], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[
                GroupIndex,
                NodeIndex,
                Tuple[GroupIndexPayload, Optional[NodeIndexPayload]],
            ],
            NodeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Expression[
            BindingType,
            Indexed[NodeIndex, GroupReference],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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
    def via_nodes(
        self: Expression[
            BindingType,
            Indexed[EdgeIndex, GroupReference],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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
    def via_nodes(
        self: Expression[
            BindingType,
            Indexed[GroupIndex, GroupReference],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[
                GroupIndex,
                NodeIndex,
                Tuple[GroupIndexPayload, Optional[NodeIndexPayload]],
            ],
            NodeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Expression[
            BindingType,
            Indexed[Positional, GroupReference],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[Positional, NodeIndex, Tuple[int, Optional[NodeIndexPayload]]],
            NodeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Expression[
            BindingType,
            Indexed[EndpointRole, GroupReference],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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
    def via_nodes(
        self: Expression[
            BindingType,
            Indexed[ValueIndex, GroupReference],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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
    def via_nodes(
        self: Expression[
            BindingType,
            Indexed[AttributeNameIndex, GroupReference],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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
    def via_nodes(
        self: Expression[
            BindingType,
            Indexed[BoolIndex, GroupReference],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[BoolIndex, NodeIndex, Tuple[bool, Optional[NodeIndexPayload]]],
            NodeReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_nodes(
        self: Expression[
            BindingType,
            Indexed[FailureKindIndex, GroupReference],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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
    def via_nodes(
        self: Expression[
            BindingType,
            Indexed[Expanded[K, ChildType, ParentPayloadType], GroupReference],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
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

    def via_nodes(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.via_nodes())

    def groups(
        self: Union[
            Expression[
                BindingType, Indexed[EntityType, Unit], ContainerType, Unpack[Levels]
            ],
            Expression[
                BindingType,
                Indexed[IndexType, EntityReferenceType],
                ContainerType,
                Unpack[Levels],
            ],
        ],
    ) -> Expression[
        BindingType, Indexed[GroupIndex, Unit], Multiple[Unordered], Unpack[Levels]
    ]:
        return self._rebuild(self._py_carrier.groups())

    @overload
    def via_groups(
        self: Expression[
            BindingType, Indexed[NodeIndex, Unit], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[
                NodeIndex,
                GroupIndex,
                Tuple[NodeIndexPayload, Optional[GroupIndexPayload]],
            ],
            GroupReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_groups(
        self: Expression[
            BindingType, Indexed[EdgeIndex, Unit], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[
                EdgeIndex,
                GroupIndex,
                Tuple[EdgeIndexPayload, Optional[GroupIndexPayload]],
            ],
            GroupReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_groups(
        self: Expression[
            BindingType,
            Indexed[NodeIndex, EntityReferenceType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[
                NodeIndex,
                GroupIndex,
                Tuple[NodeIndexPayload, Optional[GroupIndexPayload]],
            ],
            GroupReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_groups(
        self: Expression[
            BindingType,
            Indexed[EdgeIndex, EntityReferenceType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[
                EdgeIndex,
                GroupIndex,
                Tuple[EdgeIndexPayload, Optional[GroupIndexPayload]],
            ],
            GroupReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_groups(
        self: Expression[
            BindingType,
            Indexed[GroupIndex, EntityReferenceType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[
                GroupIndex,
                GroupIndex,
                Tuple[GroupIndexPayload, Optional[GroupIndexPayload]],
            ],
            GroupReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_groups(
        self: Expression[
            BindingType,
            Indexed[Positional, EntityReferenceType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[Positional, GroupIndex, Tuple[int, Optional[GroupIndexPayload]]],
            GroupReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_groups(
        self: Expression[
            BindingType,
            Indexed[EndpointRole, EntityReferenceType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[
                EndpointRole,
                GroupIndex,
                Tuple[EdgeEndpointRole, Optional[GroupIndexPayload]],
            ],
            GroupReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_groups(
        self: Expression[
            BindingType,
            Indexed[ValueIndex, EntityReferenceType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[
                ValueIndex, GroupIndex, Tuple[ScalarValue, Optional[GroupIndexPayload]]
            ],
            GroupReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_groups(
        self: Expression[
            BindingType,
            Indexed[AttributeNameIndex, EntityReferenceType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[
                AttributeNameIndex,
                GroupIndex,
                Tuple[Attribute, Optional[GroupIndexPayload]],
            ],
            GroupReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_groups(
        self: Expression[
            BindingType,
            Indexed[BoolIndex, EntityReferenceType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[BoolIndex, GroupIndex, Tuple[bool, Optional[GroupIndexPayload]]],
            GroupReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_groups(
        self: Expression[
            BindingType,
            Indexed[FailureKindIndex, EntityReferenceType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[
                FailureKindIndex,
                GroupIndex,
                Tuple[FailureKind, Optional[GroupIndexPayload]],
            ],
            GroupReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    @overload
    def via_groups(
        self: Expression[
            BindingType,
            Indexed[Expanded[K, ChildType, ParentPayloadType], EntityReferenceType],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType,
        Indexed[
            Expanded[
                Expanded[K, ChildType, ParentPayloadType],
                GroupIndex,
                Tuple[ParentPayloadType, Optional[GroupIndexPayload]],
            ],
            GroupReference,
        ],
        Multiple[Unordered],
        Unpack[Levels],
    ]: ...

    def via_groups(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.via_groups())

    @overload
    def node_count(
        self: Expression[
            BindingType, Indexed[GroupIndex, Unit], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Indexed[GroupIndex, Scalar], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def node_count(
        self: Expression[
            BindingType,
            Indexed[IndexType, GroupReference],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]
    ]: ...

    def node_count(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.node_count())

    @overload
    def edge_count(
        self: Expression[
            BindingType, Indexed[GroupIndex, Unit], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Indexed[GroupIndex, Scalar], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def edge_count(
        self: Expression[
            BindingType,
            Indexed[IndexType, GroupReference],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]
    ]: ...

    def edge_count(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.edge_count())

    @overload
    def source_node(
        self: Union[
            Expression[
                BindingType,
                Indexed[EdgeIndex, Unit],
                Multiple[OrderType],
                Unpack[Levels],
            ],
            Expression[
                BindingType,
                Indexed[IndexType, EdgeReference],
                Multiple[OrderType],
                Unpack[Levels],
            ],
        ],
    ) -> Expression[
        BindingType, Indexed[NodeIndex, Unit], Multiple[Unordered], Unpack[Levels]
    ]: ...

    @overload
    def source_node(
        self: Union[
            Expression[BindingType, Indexed[EdgeIndex, Unit], Single, Unpack[Levels]],
            Expression[
                BindingType, Indexed[IndexType, EdgeReference], Single, Unpack[Levels]
            ],
        ],
    ) -> Expression[BindingType, Indexed[NodeIndex, Unit], Single, Unpack[Levels]]: ...

    @overload
    def source_node(
        self: Union[
            Expression[BindingType, Indexed[EdgeIndex, Unit], Definite, Unpack[Levels]],
            Expression[
                BindingType, Indexed[IndexType, EdgeReference], Definite, Unpack[Levels]
            ],
        ],
    ) -> Expression[
        BindingType, Indexed[NodeIndex, Unit], Definite, Unpack[Levels]
    ]: ...

    def source_node(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.source_node())

    @overload
    def target_node(
        self: Union[
            Expression[
                BindingType,
                Indexed[EdgeIndex, Unit],
                Multiple[OrderType],
                Unpack[Levels],
            ],
            Expression[
                BindingType,
                Indexed[IndexType, EdgeReference],
                Multiple[OrderType],
                Unpack[Levels],
            ],
        ],
    ) -> Expression[
        BindingType, Indexed[NodeIndex, Unit], Multiple[Unordered], Unpack[Levels]
    ]: ...

    @overload
    def target_node(
        self: Union[
            Expression[BindingType, Indexed[EdgeIndex, Unit], Single, Unpack[Levels]],
            Expression[
                BindingType, Indexed[IndexType, EdgeReference], Single, Unpack[Levels]
            ],
        ],
    ) -> Expression[BindingType, Indexed[NodeIndex, Unit], Single, Unpack[Levels]]: ...

    @overload
    def target_node(
        self: Union[
            Expression[BindingType, Indexed[EdgeIndex, Unit], Definite, Unpack[Levels]],
            Expression[
                BindingType, Indexed[IndexType, EdgeReference], Definite, Unpack[Levels]
            ],
        ],
    ) -> Expression[
        BindingType, Indexed[NodeIndex, Unit], Definite, Unpack[Levels]
    ]: ...

    def target_node(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.target_node())

    @overload
    def via_source_node(
        self: Expression[
            BindingType, Indexed[EdgeIndex, Unit], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Indexed[EdgeIndex, NodeReference], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def via_source_node(
        self: Expression[
            BindingType,
            Indexed[IndexType, EdgeReference],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, NodeReference], ContainerType, Unpack[Levels]
    ]: ...

    def via_source_node(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.via_source_node())

    @overload
    def via_target_node(
        self: Expression[
            BindingType, Indexed[EdgeIndex, Unit], ContainerType, Unpack[Levels]
        ],
    ) -> Expression[
        BindingType, Indexed[EdgeIndex, NodeReference], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def via_target_node(
        self: Expression[
            BindingType,
            Indexed[IndexType, EdgeReference],
            ContainerType,
            Unpack[Levels],
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, NodeReference], ContainerType, Unpack[Levels]
    ]: ...

    def via_target_node(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.via_target_node())

    @overload
    def group_by(
        self: Expression[
            BindingType, Indexed[IndexType, V], ContainerType, Unpack[Levels]
        ],
        key: Union[ScalarValue, GroupingArgument[IndexType, Scalar, ArgumentOrderType]],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, V],
        ContainerType,
        Unpack[Levels],
        Grouped[IndexType, ValueIndex],
    ]: ...

    @overload
    def group_by(
        self: Expression[
            BindingType, Indexed[IndexType, V], ContainerType, Unpack[Levels]
        ],
        key: GroupingArgument[IndexType, Mask, ArgumentOrderType],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, V],
        ContainerType,
        Unpack[Levels],
        Grouped[IndexType, BoolIndex],
    ]: ...

    @overload
    def group_by(
        self: Expression[
            BindingType, Indexed[IndexType, V], ContainerType, Unpack[Levels]
        ],
        key: GroupingArgument[IndexType, AttributeName, ArgumentOrderType],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, V],
        ContainerType,
        Unpack[Levels],
        Grouped[IndexType, AttributeNameIndex],
    ]: ...

    @overload
    def group_by(
        self: Expression[
            BindingType, Indexed[IndexType, V], ContainerType, Unpack[Levels]
        ],
        key: GroupingArgument[IndexType, FailureKindValue, ArgumentOrderType],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, V],
        ContainerType,
        Unpack[Levels],
        Grouped[IndexType, FailureKindIndex],
    ]: ...

    @overload
    def group_by(
        self: Expression[
            BindingType, Indexed[IndexType, V], ContainerType, Unpack[Levels]
        ],
        key: GroupingArgument[IndexType, IndexValue[K], ArgumentOrderType],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, V],
        ContainerType,
        Unpack[Levels],
        Grouped[IndexType, K],
    ]: ...

    @overload
    def group_by(
        self: Expression[
            BindingType, Indexed[IndexType, V], ContainerType, Unpack[Levels]
        ],
        key: GroupingArgument[IndexType, NodeReference, ArgumentOrderType],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, V],
        ContainerType,
        Unpack[Levels],
        Grouped[IndexType, NodeIndex],
    ]: ...

    @overload
    def group_by(
        self: Expression[
            BindingType, Indexed[IndexType, V], ContainerType, Unpack[Levels]
        ],
        key: GroupingArgument[IndexType, EdgeReference, ArgumentOrderType],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, V],
        ContainerType,
        Unpack[Levels],
        Grouped[IndexType, EdgeIndex],
    ]: ...

    @overload
    def group_by(
        self: Expression[
            BindingType, Indexed[IndexType, V], ContainerType, Unpack[Levels]
        ],
        key: GroupingArgument[IndexType, GroupReference, ArgumentOrderType],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, V],
        ContainerType,
        Unpack[Levels],
        Grouped[IndexType, GroupIndex],
    ]: ...

    def group_by(
        self,
        key: Union[
            ScalarValue,
            Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
    ) -> Any:
        return self._rebuild(self._py_carrier.group_by(Expression._to_argument(key)))

    def having(
        self: Expression[
            BindingType,
            S,
            C,
            Unpack[OuterLevels],
            Grouped[MemberIndexType, KeyIndexType],
        ],
        predicate: MaskArgument[KeyIndexType, ArgumentOrderType],
    ) -> Expression[
        BindingType, S, C, Unpack[OuterLevels], Grouped[MemberIndexType, KeyIndexType]
    ]:
        return self._rebuild(
            self._py_carrier.having(Expression._to_argument(predicate))
        )

    def keys(
        self: Expression[
            BindingType,
            S,
            C,
            Unpack[OuterLevels],
            Grouped[MemberIndexType, KeyIndexType],
        ],
    ) -> Expression[
        BindingType,
        Indexed[KeyIndexType, Unit],
        Multiple[Unordered],
        Unpack[OuterLevels],
    ]:
        return self._rebuild(self._py_carrier.keys())

    @overload
    def ungroup(
        self: Expression[
            BindingType,
            Indexed[IndexType, V],
            ContainerType,
            Unpack[OuterLevels],
            Grouped[MemberIndexType, KeyIndexType],
        ],
    ) -> Expression[
        BindingType, Indexed[IndexType, V], Multiple[Unordered], Unpack[OuterLevels]
    ]: ...

    @overload
    def ungroup(
        self: Expression[
            BindingType,
            Bare[BareValueType],
            ContainerType,
            Unpack[OuterLevels],
            Grouped[MemberIndexType, KeyIndexType],
        ],
    ) -> Expression[
        BindingType, Bare[BareValueType], Multiple[Unordered], Unpack[OuterLevels]
    ]: ...

    def ungroup(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.ungroup())

    @overload
    def ungroup_keyed(
        self: Expression[
            BindingType,
            Indexed[IndexType, V],
            Single,
            Unpack[OuterLevels],
            Grouped[MemberIndexType, KeyIndexType],
        ],
    ) -> Expression[
        BindingType, Indexed[KeyIndexType, V], Multiple[Unordered], Unpack[OuterLevels]
    ]: ...

    @overload
    def ungroup_keyed(
        self: Expression[
            BindingType,
            Indexed[IndexType, V],
            Definite,
            Unpack[OuterLevels],
            Grouped[MemberIndexType, KeyIndexType],
        ],
    ) -> Expression[
        BindingType, Indexed[KeyIndexType, V], Multiple[Unordered], Unpack[OuterLevels]
    ]: ...

    @overload
    def ungroup_keyed(
        self: Expression[
            BindingType,
            Bare[BareValueType],
            Single,
            Unpack[OuterLevels],
            Grouped[MemberIndexType, KeyIndexType],
        ],
    ) -> Expression[
        BindingType,
        Indexed[KeyIndexType, BareValueType],
        Multiple[Unordered],
        Unpack[OuterLevels],
    ]: ...

    @overload
    def ungroup_keyed(
        self: Expression[
            BindingType,
            Bare[BareValueType],
            Definite,
            Unpack[OuterLevels],
            Grouped[MemberIndexType, KeyIndexType],
        ],
    ) -> Expression[
        BindingType,
        Indexed[KeyIndexType, BareValueType],
        Multiple[Unordered],
        Unpack[OuterLevels],
    ]: ...

    def ungroup_keyed(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.ungroup_keyed())

    @overload
    def broadcast(
        self: Expression[
            BindingType,
            Indexed[IndexType, V],
            Single,
            Unpack[OuterLevels],
            Grouped[MemberIndexType, KeyIndexType],
        ],
    ) -> Expression[
        BindingType,
        Indexed[MemberIndexType, V],
        Multiple[Unordered],
        Unpack[OuterLevels],
    ]: ...

    @overload
    def broadcast(
        self: Expression[
            BindingType,
            Indexed[IndexType, V],
            Definite,
            Unpack[OuterLevels],
            Grouped[MemberIndexType, KeyIndexType],
        ],
    ) -> Expression[
        BindingType,
        Indexed[MemberIndexType, V],
        Multiple[Unordered],
        Unpack[OuterLevels],
    ]: ...

    @overload
    def broadcast(
        self: Expression[
            BindingType,
            Bare[BareValueType],
            Single,
            Unpack[OuterLevels],
            Grouped[MemberIndexType, KeyIndexType],
        ],
    ) -> Expression[
        BindingType,
        Indexed[MemberIndexType, BareValueType],
        Multiple[Unordered],
        Unpack[OuterLevels],
    ]: ...

    @overload
    def broadcast(
        self: Expression[
            BindingType,
            Bare[BareValueType],
            Definite,
            Unpack[OuterLevels],
            Grouped[MemberIndexType, KeyIndexType],
        ],
    ) -> Expression[
        BindingType,
        Indexed[MemberIndexType, BareValueType],
        Multiple[Unordered],
        Unpack[OuterLevels],
    ]: ...

    def broadcast(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.broadcast())

    @overload
    def broadcast_via(
        self: Union[
            Expression[
                BindingType,
                Indexed[IndexType, V],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, ValueIndex],
            ],
            Expression[
                BindingType,
                Indexed[IndexType, V],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, ValueIndex],
            ],
        ],
        via: Expression[
            Unbound, Indexed[PopulationIndexType, Scalar], PopulationContainerType
        ],
    ) -> Expression[
        BindingType,
        Indexed[PopulationIndexType, V],
        PopulationContainerType,
        Unpack[OuterLevels],
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Expression[
                BindingType,
                Indexed[IndexType, V],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, ValueIndex],
            ],
            Expression[
                BindingType,
                Indexed[IndexType, V],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, ValueIndex],
            ],
        ],
        via: Expression[
            Bound, Indexed[PopulationIndexType, Scalar], Multiple[PopulationOrderType]
        ],
    ) -> Expression[
        BindingType,
        Indexed[PopulationIndexType, V],
        Multiple[PopulationOrderType],
        Unpack[OuterLevels],
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Expression[
                BindingType,
                Bare[BareValueType],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, ValueIndex],
            ],
            Expression[
                BindingType,
                Bare[BareValueType],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, ValueIndex],
            ],
        ],
        via: Expression[
            Unbound, Indexed[PopulationIndexType, Scalar], PopulationContainerType
        ],
    ) -> Expression[
        BindingType,
        Indexed[PopulationIndexType, BareValueType],
        PopulationContainerType,
        Unpack[OuterLevels],
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Expression[
                BindingType,
                Bare[BareValueType],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, ValueIndex],
            ],
            Expression[
                BindingType,
                Bare[BareValueType],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, ValueIndex],
            ],
        ],
        via: Expression[
            Bound, Indexed[PopulationIndexType, Scalar], Multiple[PopulationOrderType]
        ],
    ) -> Expression[
        BindingType,
        Indexed[PopulationIndexType, BareValueType],
        Multiple[PopulationOrderType],
        Unpack[OuterLevels],
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Expression[
                BindingType,
                Indexed[IndexType, V],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, BoolIndex],
            ],
            Expression[
                BindingType,
                Indexed[IndexType, V],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, BoolIndex],
            ],
        ],
        via: Expression[
            Unbound, Indexed[PopulationIndexType, Mask], PopulationContainerType
        ],
    ) -> Expression[
        BindingType,
        Indexed[PopulationIndexType, V],
        PopulationContainerType,
        Unpack[OuterLevels],
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Expression[
                BindingType,
                Indexed[IndexType, V],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, BoolIndex],
            ],
            Expression[
                BindingType,
                Indexed[IndexType, V],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, BoolIndex],
            ],
        ],
        via: Expression[
            Bound, Indexed[PopulationIndexType, Mask], Multiple[PopulationOrderType]
        ],
    ) -> Expression[
        BindingType,
        Indexed[PopulationIndexType, V],
        Multiple[PopulationOrderType],
        Unpack[OuterLevels],
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Expression[
                BindingType,
                Bare[BareValueType],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, BoolIndex],
            ],
            Expression[
                BindingType,
                Bare[BareValueType],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, BoolIndex],
            ],
        ],
        via: Expression[
            Unbound, Indexed[PopulationIndexType, Mask], PopulationContainerType
        ],
    ) -> Expression[
        BindingType,
        Indexed[PopulationIndexType, BareValueType],
        PopulationContainerType,
        Unpack[OuterLevels],
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Expression[
                BindingType,
                Bare[BareValueType],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, BoolIndex],
            ],
            Expression[
                BindingType,
                Bare[BareValueType],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, BoolIndex],
            ],
        ],
        via: Expression[
            Bound, Indexed[PopulationIndexType, Mask], Multiple[PopulationOrderType]
        ],
    ) -> Expression[
        BindingType,
        Indexed[PopulationIndexType, BareValueType],
        Multiple[PopulationOrderType],
        Unpack[OuterLevels],
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Expression[
                BindingType,
                Indexed[IndexType, V],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, AttributeNameIndex],
            ],
            Expression[
                BindingType,
                Indexed[IndexType, V],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, AttributeNameIndex],
            ],
        ],
        via: Expression[
            Unbound,
            Indexed[PopulationIndexType, AttributeName],
            PopulationContainerType,
        ],
    ) -> Expression[
        BindingType,
        Indexed[PopulationIndexType, V],
        PopulationContainerType,
        Unpack[OuterLevels],
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Expression[
                BindingType,
                Indexed[IndexType, V],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, AttributeNameIndex],
            ],
            Expression[
                BindingType,
                Indexed[IndexType, V],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, AttributeNameIndex],
            ],
        ],
        via: Expression[
            Bound,
            Indexed[PopulationIndexType, AttributeName],
            Multiple[PopulationOrderType],
        ],
    ) -> Expression[
        BindingType,
        Indexed[PopulationIndexType, V],
        Multiple[PopulationOrderType],
        Unpack[OuterLevels],
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Expression[
                BindingType,
                Bare[BareValueType],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, AttributeNameIndex],
            ],
            Expression[
                BindingType,
                Bare[BareValueType],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, AttributeNameIndex],
            ],
        ],
        via: Expression[
            Unbound,
            Indexed[PopulationIndexType, AttributeName],
            PopulationContainerType,
        ],
    ) -> Expression[
        BindingType,
        Indexed[PopulationIndexType, BareValueType],
        PopulationContainerType,
        Unpack[OuterLevels],
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Expression[
                BindingType,
                Bare[BareValueType],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, AttributeNameIndex],
            ],
            Expression[
                BindingType,
                Bare[BareValueType],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, AttributeNameIndex],
            ],
        ],
        via: Expression[
            Bound,
            Indexed[PopulationIndexType, AttributeName],
            Multiple[PopulationOrderType],
        ],
    ) -> Expression[
        BindingType,
        Indexed[PopulationIndexType, BareValueType],
        Multiple[PopulationOrderType],
        Unpack[OuterLevels],
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Expression[
                BindingType,
                Indexed[IndexType, V],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, FailureKindIndex],
            ],
            Expression[
                BindingType,
                Indexed[IndexType, V],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, FailureKindIndex],
            ],
        ],
        via: Expression[
            Unbound,
            Indexed[PopulationIndexType, FailureKindValue],
            PopulationContainerType,
        ],
    ) -> Expression[
        BindingType,
        Indexed[PopulationIndexType, V],
        PopulationContainerType,
        Unpack[OuterLevels],
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Expression[
                BindingType,
                Indexed[IndexType, V],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, FailureKindIndex],
            ],
            Expression[
                BindingType,
                Indexed[IndexType, V],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, FailureKindIndex],
            ],
        ],
        via: Expression[
            Bound,
            Indexed[PopulationIndexType, FailureKindValue],
            Multiple[PopulationOrderType],
        ],
    ) -> Expression[
        BindingType,
        Indexed[PopulationIndexType, V],
        Multiple[PopulationOrderType],
        Unpack[OuterLevels],
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Expression[
                BindingType,
                Bare[BareValueType],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, FailureKindIndex],
            ],
            Expression[
                BindingType,
                Bare[BareValueType],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, FailureKindIndex],
            ],
        ],
        via: Expression[
            Unbound,
            Indexed[PopulationIndexType, FailureKindValue],
            PopulationContainerType,
        ],
    ) -> Expression[
        BindingType,
        Indexed[PopulationIndexType, BareValueType],
        PopulationContainerType,
        Unpack[OuterLevels],
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Expression[
                BindingType,
                Bare[BareValueType],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, FailureKindIndex],
            ],
            Expression[
                BindingType,
                Bare[BareValueType],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, FailureKindIndex],
            ],
        ],
        via: Expression[
            Bound,
            Indexed[PopulationIndexType, FailureKindValue],
            Multiple[PopulationOrderType],
        ],
    ) -> Expression[
        BindingType,
        Indexed[PopulationIndexType, BareValueType],
        Multiple[PopulationOrderType],
        Unpack[OuterLevels],
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Expression[
                BindingType,
                Indexed[IndexType, V],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, KeyIndexType],
            ],
            Expression[
                BindingType,
                Indexed[IndexType, V],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, KeyIndexType],
            ],
        ],
        via: Expression[
            Unbound,
            Indexed[PopulationIndexType, IndexValue[KeyIndexType]],
            PopulationContainerType,
        ],
    ) -> Expression[
        BindingType,
        Indexed[PopulationIndexType, V],
        PopulationContainerType,
        Unpack[OuterLevels],
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Expression[
                BindingType,
                Indexed[IndexType, V],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, KeyIndexType],
            ],
            Expression[
                BindingType,
                Indexed[IndexType, V],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, KeyIndexType],
            ],
        ],
        via: Expression[
            Bound,
            Indexed[PopulationIndexType, IndexValue[KeyIndexType]],
            Multiple[PopulationOrderType],
        ],
    ) -> Expression[
        BindingType,
        Indexed[PopulationIndexType, V],
        Multiple[PopulationOrderType],
        Unpack[OuterLevels],
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Expression[
                BindingType,
                Bare[BareValueType],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, KeyIndexType],
            ],
            Expression[
                BindingType,
                Bare[BareValueType],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, KeyIndexType],
            ],
        ],
        via: Expression[
            Unbound,
            Indexed[PopulationIndexType, IndexValue[KeyIndexType]],
            PopulationContainerType,
        ],
    ) -> Expression[
        BindingType,
        Indexed[PopulationIndexType, BareValueType],
        PopulationContainerType,
        Unpack[OuterLevels],
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Expression[
                BindingType,
                Bare[BareValueType],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, KeyIndexType],
            ],
            Expression[
                BindingType,
                Bare[BareValueType],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, KeyIndexType],
            ],
        ],
        via: Expression[
            Bound,
            Indexed[PopulationIndexType, IndexValue[KeyIndexType]],
            Multiple[PopulationOrderType],
        ],
    ) -> Expression[
        BindingType,
        Indexed[PopulationIndexType, BareValueType],
        Multiple[PopulationOrderType],
        Unpack[OuterLevels],
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Expression[
                BindingType,
                Indexed[IndexType, V],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, NodeIndex],
            ],
            Expression[
                BindingType,
                Indexed[IndexType, V],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, NodeIndex],
            ],
        ],
        via: Expression[
            Unbound,
            Indexed[PopulationIndexType, NodeReference],
            PopulationContainerType,
        ],
    ) -> Expression[
        BindingType,
        Indexed[PopulationIndexType, V],
        PopulationContainerType,
        Unpack[OuterLevels],
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Expression[
                BindingType,
                Indexed[IndexType, V],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, NodeIndex],
            ],
            Expression[
                BindingType,
                Indexed[IndexType, V],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, NodeIndex],
            ],
        ],
        via: Expression[
            Bound,
            Indexed[PopulationIndexType, NodeReference],
            Multiple[PopulationOrderType],
        ],
    ) -> Expression[
        BindingType,
        Indexed[PopulationIndexType, V],
        Multiple[PopulationOrderType],
        Unpack[OuterLevels],
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Expression[
                BindingType,
                Bare[BareValueType],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, NodeIndex],
            ],
            Expression[
                BindingType,
                Bare[BareValueType],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, NodeIndex],
            ],
        ],
        via: Expression[
            Unbound,
            Indexed[PopulationIndexType, NodeReference],
            PopulationContainerType,
        ],
    ) -> Expression[
        BindingType,
        Indexed[PopulationIndexType, BareValueType],
        PopulationContainerType,
        Unpack[OuterLevels],
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Expression[
                BindingType,
                Bare[BareValueType],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, NodeIndex],
            ],
            Expression[
                BindingType,
                Bare[BareValueType],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, NodeIndex],
            ],
        ],
        via: Expression[
            Bound,
            Indexed[PopulationIndexType, NodeReference],
            Multiple[PopulationOrderType],
        ],
    ) -> Expression[
        BindingType,
        Indexed[PopulationIndexType, BareValueType],
        Multiple[PopulationOrderType],
        Unpack[OuterLevels],
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Expression[
                BindingType,
                Indexed[IndexType, V],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, EdgeIndex],
            ],
            Expression[
                BindingType,
                Indexed[IndexType, V],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, EdgeIndex],
            ],
        ],
        via: Expression[
            Unbound,
            Indexed[PopulationIndexType, EdgeReference],
            PopulationContainerType,
        ],
    ) -> Expression[
        BindingType,
        Indexed[PopulationIndexType, V],
        PopulationContainerType,
        Unpack[OuterLevels],
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Expression[
                BindingType,
                Indexed[IndexType, V],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, EdgeIndex],
            ],
            Expression[
                BindingType,
                Indexed[IndexType, V],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, EdgeIndex],
            ],
        ],
        via: Expression[
            Bound,
            Indexed[PopulationIndexType, EdgeReference],
            Multiple[PopulationOrderType],
        ],
    ) -> Expression[
        BindingType,
        Indexed[PopulationIndexType, V],
        Multiple[PopulationOrderType],
        Unpack[OuterLevels],
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Expression[
                BindingType,
                Bare[BareValueType],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, EdgeIndex],
            ],
            Expression[
                BindingType,
                Bare[BareValueType],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, EdgeIndex],
            ],
        ],
        via: Expression[
            Unbound,
            Indexed[PopulationIndexType, EdgeReference],
            PopulationContainerType,
        ],
    ) -> Expression[
        BindingType,
        Indexed[PopulationIndexType, BareValueType],
        PopulationContainerType,
        Unpack[OuterLevels],
    ]: ...

    @overload
    def broadcast_via(
        self: Union[
            Expression[
                BindingType,
                Bare[BareValueType],
                Single,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, EdgeIndex],
            ],
            Expression[
                BindingType,
                Bare[BareValueType],
                Definite,
                Unpack[OuterLevels],
                Grouped[MemberIndexType, EdgeIndex],
            ],
        ],
        via: Expression[
            Bound,
            Indexed[PopulationIndexType, EdgeReference],
            Multiple[PopulationOrderType],
        ],
    ) -> Expression[
        BindingType,
        Indexed[PopulationIndexType, BareValueType],
        Multiple[PopulationOrderType],
        Unpack[OuterLevels],
    ]: ...

    def broadcast_via(
        self,
        via: Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
    ) -> Any:
        return self._rebuild(self._py_carrier.broadcast_via(via._py_carrier))

    @overload
    def bucket_errors(
        self: Expression[
            BindingType,
            Indexed[IndexType, V],
            ContainerType,
            Unpack[OuterLevels],
            Grouped[MemberIndexType, KeyIndexType],
        ],
    ) -> Expression[
        BindingType,
        Indexed[KeyIndexType, FailureValue],
        Multiple[Unordered],
        Unpack[OuterLevels],
    ]: ...

    @overload
    def bucket_errors(
        self: Expression[
            BindingType,
            Bare[BareValueType],
            ContainerType,
            Unpack[OuterLevels],
            Grouped[MemberIndexType, KeyIndexType],
        ],
    ) -> Expression[
        BindingType,
        Indexed[KeyIndexType, FailureValue],
        Multiple[Unordered],
        Unpack[OuterLevels],
    ]: ...

    def bucket_errors(self) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        return self._rebuild(self._py_carrier.bucket_errors())

    def key_errors(
        self: Expression[
            BindingType,
            S,
            C,
            Unpack[OuterLevels],
            Grouped[MemberIndexType, KeyIndexType],
        ],
    ) -> Expression[
        BindingType,
        Indexed[MemberIndexType, FailureValue],
        Multiple[Unordered],
        Unpack[OuterLevels],
    ]:
        return self._rebuild(self._py_carrier.key_errors())

    @overload
    def on_bucket_error(
        self: Expression[
            BindingType,
            Indexed[IndexType, V],
            ContainerType,
            Unpack[OuterLevels],
            Grouped[MemberIndexType, KeyIndexType],
        ],
        policy: Union[Drop, Raise],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, V],
        ContainerType,
        Unpack[OuterLevels],
        Grouped[MemberIndexType, KeyIndexType],
    ]: ...

    @overload
    def on_bucket_error(
        self: Expression[
            BindingType,
            Bare[BareValueType],
            ContainerType,
            Unpack[OuterLevels],
            Grouped[MemberIndexType, KeyIndexType],
        ],
        policy: Union[Drop, Raise],
    ) -> Expression[
        BindingType,
        Bare[BareValueType],
        ContainerType,
        Unpack[OuterLevels],
        Grouped[MemberIndexType, KeyIndexType],
    ]: ...

    def on_bucket_error(
        self, policy: Union[Drop, Raise]
    ) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        if isinstance(policy, Drop):
            return self._rebuild(self._py_carrier.on_bucket_error_drop())

        return self._rebuild(self._py_carrier.on_bucket_error_raise())

    @overload
    def on_key_error(
        self: Expression[
            BindingType,
            Indexed[IndexType, V],
            ContainerType,
            Unpack[OuterLevels],
            Grouped[MemberIndexType, KeyIndexType],
        ],
        policy: Union[Drop, Raise],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, V],
        ContainerType,
        Unpack[OuterLevels],
        Grouped[MemberIndexType, KeyIndexType],
    ]: ...

    @overload
    def on_key_error(
        self: Expression[
            BindingType,
            Bare[BareValueType],
            ContainerType,
            Unpack[OuterLevels],
            Grouped[MemberIndexType, KeyIndexType],
        ],
        policy: Union[Drop, Raise],
    ) -> Expression[
        BindingType,
        Bare[BareValueType],
        ContainerType,
        Unpack[OuterLevels],
        Grouped[MemberIndexType, KeyIndexType],
    ]: ...

    def on_key_error(
        self, policy: Union[Drop, Raise]
    ) -> Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        if isinstance(policy, Drop):
            return self._rebuild(self._py_carrier.on_key_error_drop())

        return self._rebuild(self._py_carrier.on_key_error_raise())

    @overload
    def transition(
        self: Expression[
            BindingType, Indexed[IndexType, Scalar], ContainerType, Unpack[Levels]
        ],
        target: ValueTarget[ScalarTransitionValueType],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, ScalarTransitionValueType],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def transition(
        self: Expression[BindingType, Bare[Scalar], ContainerType, Unpack[Levels]],
        target: ValueTarget[ScalarTransitionValueType],
    ) -> Expression[
        BindingType, Bare[ScalarTransitionValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def transition(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[ValueIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        target: ValueTarget[ValueIndexTransitionValueType],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, ValueIndexTransitionValueType],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def transition(
        self: Expression[
            BindingType, Bare[IndexValue[ValueIndex]], ContainerType, Unpack[Levels]
        ],
        target: ValueTarget[ValueIndexTransitionValueType],
    ) -> Expression[
        BindingType, Bare[ValueIndexTransitionValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def transition(
        self: Expression[
            BindingType,
            Indexed[IndexType, AttributeName],
            ContainerType,
            Unpack[Levels],
        ],
        target: ValueTarget[AttributeNameTransitionValueType],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, AttributeNameTransitionValueType],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def transition(
        self: Expression[
            BindingType, Bare[AttributeName], ContainerType, Unpack[Levels]
        ],
        target: ValueTarget[AttributeNameTransitionValueType],
    ) -> Expression[
        BindingType,
        Bare[AttributeNameTransitionValueType],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def transition(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[NodeIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        target: ValueTarget[NodeIndexTransitionValueType],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, NodeIndexTransitionValueType],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def transition(
        self: Expression[
            BindingType, Bare[IndexValue[NodeIndex]], ContainerType, Unpack[Levels]
        ],
        target: ValueTarget[NodeIndexTransitionValueType],
    ) -> Expression[
        BindingType, Bare[NodeIndexTransitionValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def transition(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        target: ValueTarget[AttributeNameIndexTransitionValueType],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, AttributeNameIndexTransitionValueType],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def transition(
        self: Expression[
            BindingType,
            Bare[IndexValue[AttributeNameIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        target: ValueTarget[AttributeNameIndexTransitionValueType],
    ) -> Expression[
        BindingType,
        Bare[AttributeNameIndexTransitionValueType],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def transition(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[GroupIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        target: ValueTarget[GroupIndexTransitionValueType],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, GroupIndexTransitionValueType],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def transition(
        self: Expression[
            BindingType, Bare[IndexValue[GroupIndex]], ContainerType, Unpack[Levels]
        ],
        target: ValueTarget[GroupIndexTransitionValueType],
    ) -> Expression[
        BindingType, Bare[GroupIndexTransitionValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def transition(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[Positional]],
            ContainerType,
            Unpack[Levels],
        ],
        target: ValueTarget[PositionalTransitionValueType],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, PositionalTransitionValueType],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def transition(
        self: Expression[
            BindingType, Bare[IndexValue[Positional]], ContainerType, Unpack[Levels]
        ],
        target: ValueTarget[PositionalTransitionValueType],
    ) -> Expression[
        BindingType, Bare[PositionalTransitionValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def transition(
        self: Expression[
            BindingType, Indexed[IndexType, Mask], ContainerType, Unpack[Levels]
        ],
        target: ValueTarget[MaskTransitionValueType],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, MaskTransitionValueType],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def transition(
        self: Expression[BindingType, Bare[Mask], ContainerType, Unpack[Levels]],
        target: ValueTarget[MaskTransitionValueType],
    ) -> Expression[
        BindingType, Bare[MaskTransitionValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def transition(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[BoolIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        target: ValueTarget[BoolIndexTransitionValueType],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, BoolIndexTransitionValueType],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def transition(
        self: Expression[
            BindingType, Bare[IndexValue[BoolIndex]], ContainerType, Unpack[Levels]
        ],
        target: ValueTarget[BoolIndexTransitionValueType],
    ) -> Expression[
        BindingType, Bare[BoolIndexTransitionValueType], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def transition(
        self: Expression[
            BindingType,
            Indexed[IndexType, FailureKindValue],
            ContainerType,
            Unpack[Levels],
        ],
        target: ValueTarget[IndexValue[FailureKindIndex]],
    ) -> Expression[
        BindingType,
        Indexed[IndexType, IndexValue[FailureKindIndex]],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def transition(
        self: Expression[
            BindingType, Bare[FailureKindValue], ContainerType, Unpack[Levels]
        ],
        target: ValueTarget[IndexValue[FailureKindIndex]],
    ) -> Expression[
        BindingType, Bare[IndexValue[FailureKindIndex]], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def transition(
        self: Expression[
            BindingType,
            Indexed[IndexType, IndexValue[FailureKindIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        target: ValueTarget[FailureKindValue],
    ) -> Expression[
        BindingType, Indexed[IndexType, FailureKindValue], ContainerType, Unpack[Levels]
    ]: ...

    @overload
    def transition(
        self: Expression[
            BindingType,
            Bare[IndexValue[FailureKindIndex]],
            ContainerType,
            Unpack[Levels],
        ],
        target: ValueTarget[FailureKindValue],
    ) -> Expression[
        BindingType, Bare[FailureKindValue], ContainerType, Unpack[Levels]
    ]: ...

    def transition(self, target: ValueTarget[Any]) -> Any:
        return self._rebuild(self._py_carrier.transition(target._py_value_target))

    @overload
    def inherit(
        self: Expression[
            BindingType,
            Indexed[
                Expanded[IndexType, ChildType, ParentPayloadType], TemplateValueType
            ],
            ContainerType,
            Unpack[Levels],
        ],
        values: ScalarValue,
    ) -> Expression[
        BindingType,
        Indexed[Expanded[IndexType, ChildType, ParentPayloadType], Scalar],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def inherit(
        self: Expression[
            BindingType,
            Indexed[
                Expanded[IndexType, ChildType, ParentPayloadType], TemplateValueType
            ],
            ContainerType,
            Unpack[Levels],
        ],
        values: IndexedExpressionArgument[
            IndexType, InheritedValueType, ArgumentOrderType
        ],
    ) -> Expression[
        BindingType,
        Indexed[Expanded[IndexType, ChildType, ParentPayloadType], InheritedValueType],
        ContainerType,
        Unpack[Levels],
    ]: ...

    @overload
    def inherit(
        self: Expression[
            BindingType,
            Indexed[
                Expanded[IndexType, ChildType, ParentPayloadType], TemplateValueType
            ],
            Definite,
            Unpack[Levels],
        ],
        values: IndexedDroppingArgument[IndexType, InheritedValueType],
    ) -> Expression[
        BindingType,
        Indexed[Expanded[IndexType, ChildType, ParentPayloadType], InheritedValueType],
        Single,
        Unpack[Levels],
    ]: ...

    @overload
    def inherit(
        self: Expression[
            BindingType,
            Indexed[
                Expanded[IndexType, ChildType, ParentPayloadType], TemplateValueType
            ],
            DroppedContainerType,
            Unpack[Levels],
        ],
        values: IndexedDroppingArgument[IndexType, InheritedValueType],
    ) -> Expression[
        BindingType,
        Indexed[Expanded[IndexType, ChildType, ParentPayloadType], InheritedValueType],
        DroppedContainerType,
        Unpack[Levels],
    ]: ...

    def inherit(
        self,
        values: Union[
            ScalarValue,
            Expression[Any, Any, Any, Unpack[Tuple[Any, ...]]],
            Argument[Any, Any],
        ],
    ) -> Any:
        return self._rebuild(self._py_carrier.inherit(Expression._to_argument(values)))

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


class Series(Expression[Bound, S, C, Unpack[Levels]]):
    @classmethod
    def _from_py_series(cls, py_series: PySeries) -> Series[S, C, Unpack[Levels]]:
        series = cls.__new__(cls)
        series._py_carrier = py_series

        return series


IndexedExpressionArgument: TypeAlias = Union[
    Expression[Unbound, Indexed[IndexType, V], Multiple[ArgumentOrderType]],
    Expression[Unbound, Bare[V], Single],
    Expression[Unbound, Bare[V], Definite],
    Expression[Bound, Indexed[IndexType, V], Multiple[ArgumentOrderType]],
    Expression[Bound, Bare[V], Single],
    Expression[Bound, Bare[V], Definite],
    Argument[Indexed[IndexType, V], Preserving],
    Argument[Bare[V], Preserving],
]
BareExpressionArgument: TypeAlias = Union[
    Expression[Unbound, Bare[V], Single],
    Expression[Unbound, Bare[V], Definite],
    Expression[Bound, Bare[V], Single],
    Expression[Bound, Bare[V], Definite],
    Argument[Bare[V], Preserving],
]
BareReplacement: TypeAlias = Union[
    Replace[Expression[Unbound, Bare[V], Single]],
    Replace[Expression[Unbound, Bare[V], Definite]],
    Replace[Expression[Bound, Bare[V], Single]],
    Replace[Expression[Bound, Bare[V], Definite]],
    Replace[Argument[Bare[V], Preserving]],
    Replace[Argument[Bare[V], Dropping]],
]
MaskArgument: TypeAlias = Union[
    bool,
    Expression[Unbound, Indexed[IndexType, Mask], Multiple[ArgumentOrderType]],
    Expression[Unbound, Bare[Mask], Single],
    Expression[Unbound, Bare[Mask], Definite],
    Expression[Bound, Indexed[IndexType, Mask], Multiple[ArgumentOrderType]],
    Expression[Bound, Bare[Mask], Single],
    Expression[Bound, Bare[Mask], Definite],
    Argument[Indexed[IndexType, Mask], Preserving],
    Argument[Indexed[IndexType, Mask], Dropping],
    Argument[Bare[Mask], Preserving],
    Argument[Bare[Mask], Dropping],
]
BareMaskArgument: TypeAlias = Union[
    bool,
    Expression[Unbound, Bare[Mask], Single],
    Expression[Unbound, Bare[Mask], Definite],
    Expression[Bound, Bare[Mask], Single],
    Expression[Bound, Bare[Mask], Definite],
    Argument[Bare[Mask], Preserving],
    Argument[Bare[Mask], Dropping],
]
IndexedDroppingArgument: TypeAlias = Union[
    Argument[Indexed[IndexType, V], Dropping],
    Argument[Bare[V], Dropping],
]
BareDroppingArgument: TypeAlias = Argument[Bare[V], Dropping]
GroupingArgument: TypeAlias = Union[
    Expression[Unbound, Indexed[IndexType, V], Multiple[ArgumentOrderType]],
    Expression[Unbound, Bare[V], Single],
    Expression[Unbound, Bare[V], Definite],
    Expression[Bound, Indexed[IndexType, V], Multiple[ArgumentOrderType]],
    Expression[Bound, Bare[V], Single],
    Expression[Bound, Bare[V], Definite],
    Argument[Indexed[IndexType, V], Preserving],
    Argument[Indexed[IndexType, V], Dropping],
]
MembershipArgument: TypeAlias = Union[
    Expression[Unbound, Indexed[Any, V], Any],
    Expression[Unbound, Bare[V], Any],
    Expression[Bound, Indexed[Any, V], Any],
    Expression[Bound, Bare[V], Any],
]
IndexedStringArgument: TypeAlias = Union[
    str,
    IndexedExpressionArgument[IndexType, StringArgumentValueType, ArgumentOrderType],
]
BareStringArgument: TypeAlias = Union[
    str, BareExpressionArgument[StringArgumentValueType]
]
IndexedAnyStringArgument: TypeAlias = Union[
    IndexedStringArgument[IndexType, StringArgumentValueType, ArgumentOrderType],
    IndexedDroppingArgument[IndexType, StringArgumentValueType],
]
BareAnyStringArgument: TypeAlias = Union[
    BareStringArgument[StringArgumentValueType],
    BareDroppingArgument[StringArgumentValueType],
]
IndexedIntegerArgument: TypeAlias = Union[
    int, IndexedExpressionArgument[IndexType, IntegerValueType, ArgumentOrderType]
]
BareIntegerArgument: TypeAlias = Union[int, BareExpressionArgument[IntegerValueType]]
IndexedAnyIntegerArgument: TypeAlias = Union[
    IndexedIntegerArgument[IndexType, IntegerValueType, ArgumentOrderType],
    IndexedDroppingArgument[IndexType, IntegerValueType],
]
BareAnyIntegerArgument: TypeAlias = Union[
    BareIntegerArgument[IntegerValueType], BareDroppingArgument[IntegerValueType]
]
IndexedScalarArgument: TypeAlias = Union[
    ScalarValue, IndexedExpressionArgument[IndexType, V, ArgumentOrderType]
]
BareScalarArgument: TypeAlias = Union[ScalarValue, BareExpressionArgument[V]]
BareAnyScalarArgument: TypeAlias = Union[BareScalarArgument[V], BareDroppingArgument[V]]
IndexedAnyScalarArgument: TypeAlias = Union[
    IndexedScalarArgument[IndexType, V, ArgumentOrderType],
    IndexedDroppingArgument[IndexType, V],
]
IndexedAttributeArgument: TypeAlias = Union[
    Attribute, IndexedExpressionArgument[IndexType, V, ArgumentOrderType]
]
BareAttributeArgument: TypeAlias = Union[Attribute, BareExpressionArgument[V]]
IndexedAnyAttributeArgument: TypeAlias = Union[
    IndexedAttributeArgument[IndexType, V, ArgumentOrderType],
    IndexedDroppingArgument[IndexType, V],
]
BareAnyAttributeArgument: TypeAlias = Union[
    BareAttributeArgument[V], BareDroppingArgument[V]
]


AttributesExpression: TypeAlias = Expression[
    Unbound, Indexed[IndexType, AttributeName], Multiple[OrderType]
]
BareAttributesExpression: TypeAlias = Expression[
    Unbound, Bare[AttributeName], Multiple[OrderType]
]
AttributeExpression: TypeAlias = Expression[
    Unbound, Indexed[IndexType, AttributeName], Single
]
BareAttributeExpression: TypeAlias = Expression[Unbound, Bare[AttributeName], Single]
DefiniteAttributeExpression: TypeAlias = Expression[
    Unbound, Indexed[IndexType, AttributeName], Definite
]
DefiniteBareAttributeExpression: TypeAlias = Expression[
    Unbound, Bare[AttributeName], Definite
]

BoolMaskExpression: TypeAlias = Expression[
    Unbound, Indexed[IndexType, Mask], Multiple[OrderType]
]
BareBoolMaskExpression: TypeAlias = Expression[Unbound, Bare[Mask], Multiple[OrderType]]
BoolExpression: TypeAlias = Expression[Unbound, Indexed[IndexType, Mask], Single]
BareBoolExpression: TypeAlias = Expression[Unbound, Bare[Mask], Single]
DefiniteBoolExpression: TypeAlias = Expression[
    Unbound, Indexed[IndexType, Mask], Definite
]
DefiniteBareBoolExpression: TypeAlias = Expression[Unbound, Bare[Mask], Definite]

ElementsExpression: TypeAlias = Expression[
    Unbound, Indexed[IndexType, Unit], Multiple[OrderType]
]
ElementExpression: TypeAlias = Expression[Unbound, Indexed[IndexType, Unit], Single]
DefiniteElementExpression: TypeAlias = Expression[
    Unbound, Indexed[IndexType, Unit], Definite
]

FailuresExpression: TypeAlias = Expression[
    Unbound, Indexed[IndexType, FailureValue], Multiple[OrderType]
]
FailureKindsExpression: TypeAlias = Expression[
    Unbound, Indexed[IndexType, FailureKindValue], Multiple[OrderType]
]
BareFailuresExpression: TypeAlias = Expression[
    Unbound, Bare[FailureValue], Multiple[OrderType]
]
BareFailureKindsExpression: TypeAlias = Expression[
    Unbound, Bare[FailureKindValue], Multiple[OrderType]
]
FailureExpression: TypeAlias = Expression[
    Unbound, Indexed[IndexType, FailureValue], Single
]
FailureKindExpression: TypeAlias = Expression[
    Unbound, Indexed[IndexType, FailureKindValue], Single
]
BareFailureExpression: TypeAlias = Expression[Unbound, Bare[FailureValue], Single]
BareFailureKindExpression: TypeAlias = Expression[
    Unbound, Bare[FailureKindValue], Single
]
DefiniteFailureExpression: TypeAlias = Expression[
    Unbound, Indexed[IndexType, FailureValue], Definite
]
DefiniteFailureKindExpression: TypeAlias = Expression[
    Unbound, Indexed[IndexType, FailureKindValue], Definite
]
DefiniteBareFailureExpression: TypeAlias = Expression[
    Unbound, Bare[FailureValue], Definite
]
DefiniteBareFailureKindExpression: TypeAlias = Expression[
    Unbound, Bare[FailureKindValue], Definite
]

IndicesExpression: TypeAlias = Expression[
    Unbound, Indexed[IndexType, IndexValue[IndexType]], Multiple[OrderType]
]
BareIndicesExpression: TypeAlias = Expression[
    Unbound, Bare[IndexValue[IndexType]], Multiple[OrderType]
]
IndexExpression: TypeAlias = Expression[
    Unbound, Indexed[IndexType, IndexValue[IndexType]], Single
]
BareIndexExpression: TypeAlias = Expression[
    Unbound, Bare[IndexValue[IndexType]], Single
]
DefiniteIndexExpression: TypeAlias = Expression[
    Unbound, Indexed[IndexType, IndexValue[IndexType]], Definite
]
DefiniteBareIndexExpression: TypeAlias = Expression[
    Unbound, Bare[IndexValue[IndexType]], Definite
]

ReferencesExpression: TypeAlias = Expression[
    Unbound, Indexed[IndexType, ReferenceType], Multiple[OrderType]
]
BareReferencesExpression: TypeAlias = Expression[
    Unbound, Bare[ReferenceType], Multiple[OrderType]
]
ReferenceExpression: TypeAlias = Expression[
    Unbound, Indexed[IndexType, ReferenceType], Single
]
BareReferenceExpression: TypeAlias = Expression[Unbound, Bare[ReferenceType], Single]
DefiniteReferenceExpression: TypeAlias = Expression[
    Unbound, Indexed[IndexType, ReferenceType], Definite
]
DefiniteBareReferenceExpression: TypeAlias = Expression[
    Unbound, Bare[ReferenceType], Definite
]
ReferenceIndicesExpression: TypeAlias = Expression[
    Unbound, Indexed[IndexType, IndexValue[EntityType]], Multiple[OrderType]
]
ReferenceIndexExpression: TypeAlias = Expression[
    Unbound, Indexed[IndexType, IndexValue[EntityType]], Single
]
DefiniteReferenceIndexExpression: TypeAlias = Expression[
    Unbound, Indexed[IndexType, IndexValue[EntityType]], Definite
]

ValuesExpression: TypeAlias = Expression[
    Unbound, Indexed[IndexType, Scalar], Multiple[OrderType]
]
BareValuesExpression: TypeAlias = Expression[Unbound, Bare[Scalar], Multiple[OrderType]]
ValueExpression: TypeAlias = Expression[Unbound, Indexed[IndexType, Scalar], Single]
BareValueExpression: TypeAlias = Expression[Unbound, Bare[Scalar], Single]
DefiniteValueExpression: TypeAlias = Expression[
    Unbound, Indexed[IndexType, Scalar], Definite
]
DefiniteBareValueExpression: TypeAlias = Expression[Unbound, Bare[Scalar], Definite]

NodeAttributesExpression: TypeAlias = AttributesExpression[NodeIndex, Unordered]
OrderedNodeAttributesExpression: TypeAlias = AttributesExpression[NodeIndex, Ordered]
NodeAttributeExpression: TypeAlias = AttributeExpression[NodeIndex]
DefiniteNodeAttributeExpression: TypeAlias = DefiniteAttributeExpression[NodeIndex]
EdgeAttributesExpression: TypeAlias = AttributesExpression[EdgeIndex, Unordered]
OrderedEdgeAttributesExpression: TypeAlias = AttributesExpression[EdgeIndex, Ordered]
EdgeAttributeExpression: TypeAlias = AttributeExpression[EdgeIndex]
DefiniteEdgeAttributeExpression: TypeAlias = DefiniteAttributeExpression[EdgeIndex]

NodeAttributesTreeExpression: TypeAlias = Expression[
    Unbound,
    Indexed[
        Expanded[
            NodeIndex, AttributeNameIndex, Tuple[NodeIndexPayload, Optional[Attribute]]
        ],
        AttributeName,
    ],
    Multiple[Unordered],
]
EdgeAttributesTreeExpression: TypeAlias = Expression[
    Unbound,
    Indexed[
        Expanded[
            EdgeIndex, AttributeNameIndex, Tuple[EdgeIndexPayload, Optional[Attribute]]
        ],
        AttributeName,
    ],
    Multiple[Unordered],
]

NodesExpression: TypeAlias = ElementsExpression[NodeIndex, Unordered]
OrderedNodesExpression: TypeAlias = ElementsExpression[NodeIndex, Ordered]
NodeExpression: TypeAlias = ElementExpression[NodeIndex]
DefiniteNodeExpression: TypeAlias = DefiniteElementExpression[NodeIndex]
EdgesExpression: TypeAlias = ElementsExpression[EdgeIndex, Unordered]
OrderedEdgesExpression: TypeAlias = ElementsExpression[EdgeIndex, Ordered]
EdgeExpression: TypeAlias = ElementExpression[EdgeIndex]
DefiniteEdgeExpression: TypeAlias = DefiniteElementExpression[EdgeIndex]

NodeIndicesExpression: TypeAlias = IndicesExpression[NodeIndex, Unordered]
OrderedNodeIndicesExpression: TypeAlias = IndicesExpression[NodeIndex, Ordered]
NodeIndexExpression: TypeAlias = IndexExpression[NodeIndex]
DefiniteNodeIndexExpression: TypeAlias = DefiniteIndexExpression[NodeIndex]
BareNodeIndicesExpression: TypeAlias = BareIndicesExpression[NodeIndex, Unordered]
OrderedBareNodeIndicesExpression: TypeAlias = BareIndicesExpression[NodeIndex, Ordered]
BareNodeIndexExpression: TypeAlias = BareIndexExpression[NodeIndex]
DefiniteBareNodeIndexExpression: TypeAlias = DefiniteBareIndexExpression[NodeIndex]
EdgeIndicesExpression: TypeAlias = IndicesExpression[EdgeIndex, Unordered]
OrderedEdgeIndicesExpression: TypeAlias = IndicesExpression[EdgeIndex, Ordered]
EdgeIndexExpression: TypeAlias = IndexExpression[EdgeIndex]
DefiniteEdgeIndexExpression: TypeAlias = DefiniteIndexExpression[EdgeIndex]
BareEdgeIndicesExpression: TypeAlias = BareIndicesExpression[EdgeIndex, Unordered]
OrderedBareEdgeIndicesExpression: TypeAlias = BareIndicesExpression[EdgeIndex, Ordered]
BareEdgeIndexExpression: TypeAlias = BareIndexExpression[EdgeIndex]
DefiniteBareEdgeIndexExpression: TypeAlias = DefiniteBareIndexExpression[EdgeIndex]

NodeValuesExpression: TypeAlias = ValuesExpression[NodeIndex, Unordered]
OrderedNodeValuesExpression: TypeAlias = ValuesExpression[NodeIndex, Ordered]
NodeValueExpression: TypeAlias = ValueExpression[NodeIndex]
DefiniteNodeValueExpression: TypeAlias = DefiniteValueExpression[NodeIndex]
EdgeValuesExpression: TypeAlias = ValuesExpression[EdgeIndex, Unordered]
OrderedEdgeValuesExpression: TypeAlias = ValuesExpression[EdgeIndex, Ordered]
EdgeValueExpression: TypeAlias = ValueExpression[EdgeIndex]
DefiniteEdgeValueExpression: TypeAlias = DefiniteValueExpression[EdgeIndex]


GroupsExpression: TypeAlias = ElementsExpression[GroupIndex, Unordered]
OrderedGroupsExpression: TypeAlias = ElementsExpression[GroupIndex, Ordered]
GroupExpression: TypeAlias = ElementExpression[GroupIndex]
DefiniteGroupExpression: TypeAlias = DefiniteElementExpression[GroupIndex]

GroupIndicesExpression: TypeAlias = IndicesExpression[GroupIndex, Unordered]
OrderedGroupIndicesExpression: TypeAlias = IndicesExpression[GroupIndex, Ordered]
GroupIndexExpression: TypeAlias = IndexExpression[GroupIndex]
DefiniteGroupIndexExpression: TypeAlias = DefiniteIndexExpression[GroupIndex]
BareGroupIndicesExpression: TypeAlias = BareIndicesExpression[GroupIndex, Unordered]
OrderedBareGroupIndicesExpression: TypeAlias = BareIndicesExpression[
    GroupIndex, Ordered
]
BareGroupIndexExpression: TypeAlias = BareIndexExpression[GroupIndex]
DefiniteBareGroupIndexExpression: TypeAlias = DefiniteBareIndexExpression[GroupIndex]

GroupValuesExpression: TypeAlias = ValuesExpression[GroupIndex, Unordered]
OrderedGroupValuesExpression: TypeAlias = ValuesExpression[GroupIndex, Ordered]
GroupValueExpression: TypeAlias = ValueExpression[GroupIndex]
DefiniteGroupValueExpression: TypeAlias = DefiniteValueExpression[GroupIndex]

AttributesSeries: TypeAlias = Expression[
    Bound, Indexed[IndexType, AttributeName], Multiple[OrderType]
]
BareAttributesSeries: TypeAlias = Expression[
    Bound, Bare[AttributeName], Multiple[OrderType]
]
AttributeSeries: TypeAlias = Expression[
    Bound, Indexed[IndexType, AttributeName], Single
]
BareAttributeSeries: TypeAlias = Expression[Bound, Bare[AttributeName], Single]
DefiniteAttributeSeries: TypeAlias = Expression[
    Bound, Indexed[IndexType, AttributeName], Definite
]
DefiniteBareAttributeSeries: TypeAlias = Expression[
    Bound, Bare[AttributeName], Definite
]

BoolMaskSeries: TypeAlias = Expression[
    Bound, Indexed[IndexType, Mask], Multiple[OrderType]
]
BareBoolMaskSeries: TypeAlias = Expression[Bound, Bare[Mask], Multiple[OrderType]]
BoolSeries: TypeAlias = Expression[Bound, Indexed[IndexType, Mask], Single]
BareBoolSeries: TypeAlias = Expression[Bound, Bare[Mask], Single]
DefiniteBoolSeries: TypeAlias = Expression[Bound, Indexed[IndexType, Mask], Definite]
DefiniteBareBoolSeries: TypeAlias = Expression[Bound, Bare[Mask], Definite]

ElementsSeries: TypeAlias = Expression[
    Bound, Indexed[IndexType, Unit], Multiple[OrderType]
]
ElementSeries: TypeAlias = Expression[Bound, Indexed[IndexType, Unit], Single]
DefiniteElementSeries: TypeAlias = Expression[Bound, Indexed[IndexType, Unit], Definite]

FailuresSeries: TypeAlias = Expression[
    Bound, Indexed[IndexType, FailureValue], Multiple[OrderType]
]
FailureKindsSeries: TypeAlias = Expression[
    Bound, Indexed[IndexType, FailureKindValue], Multiple[OrderType]
]
BareFailuresSeries: TypeAlias = Expression[
    Bound, Bare[FailureValue], Multiple[OrderType]
]
BareFailureKindsSeries: TypeAlias = Expression[
    Bound, Bare[FailureKindValue], Multiple[OrderType]
]
FailureSeries: TypeAlias = Expression[Bound, Indexed[IndexType, FailureValue], Single]
FailureKindSeries: TypeAlias = Expression[
    Bound, Indexed[IndexType, FailureKindValue], Single
]
BareFailureSeries: TypeAlias = Expression[Bound, Bare[FailureValue], Single]
BareFailureKindSeries: TypeAlias = Expression[Bound, Bare[FailureKindValue], Single]
DefiniteFailureSeries: TypeAlias = Expression[
    Bound, Indexed[IndexType, FailureValue], Definite
]
DefiniteFailureKindSeries: TypeAlias = Expression[
    Bound, Indexed[IndexType, FailureKindValue], Definite
]
DefiniteBareFailureSeries: TypeAlias = Expression[Bound, Bare[FailureValue], Definite]
DefiniteBareFailureKindSeries: TypeAlias = Expression[
    Bound, Bare[FailureKindValue], Definite
]

IndicesSeries: TypeAlias = Expression[
    Bound, Indexed[IndexType, IndexValue[IndexType]], Multiple[OrderType]
]
BareIndicesSeries: TypeAlias = Expression[
    Bound, Bare[IndexValue[IndexType]], Multiple[OrderType]
]
IndexSeries: TypeAlias = Expression[
    Bound, Indexed[IndexType, IndexValue[IndexType]], Single
]
BareIndexSeries: TypeAlias = Expression[Bound, Bare[IndexValue[IndexType]], Single]
DefiniteIndexSeries: TypeAlias = Expression[
    Bound, Indexed[IndexType, IndexValue[IndexType]], Definite
]
DefiniteBareIndexSeries: TypeAlias = Expression[
    Bound, Bare[IndexValue[IndexType]], Definite
]

ReferencesSeries: TypeAlias = Expression[
    Bound, Indexed[IndexType, ReferenceType], Multiple[OrderType]
]
BareReferencesSeries: TypeAlias = Expression[
    Bound, Bare[ReferenceType], Multiple[OrderType]
]
ReferenceSeries: TypeAlias = Expression[
    Bound, Indexed[IndexType, ReferenceType], Single
]
BareReferenceSeries: TypeAlias = Expression[Bound, Bare[ReferenceType], Single]
DefiniteReferenceSeries: TypeAlias = Expression[
    Bound, Indexed[IndexType, ReferenceType], Definite
]
DefiniteBareReferenceSeries: TypeAlias = Expression[
    Bound, Bare[ReferenceType], Definite
]
ReferenceIndicesSeries: TypeAlias = Expression[
    Bound, Indexed[IndexType, IndexValue[EntityType]], Multiple[OrderType]
]
ReferenceIndexSeries: TypeAlias = Expression[
    Bound, Indexed[IndexType, IndexValue[EntityType]], Single
]
DefiniteReferenceIndexSeries: TypeAlias = Expression[
    Bound, Indexed[IndexType, IndexValue[EntityType]], Definite
]

ValuesSeries: TypeAlias = Expression[
    Bound, Indexed[IndexType, Scalar], Multiple[OrderType]
]
BareValuesSeries: TypeAlias = Expression[Bound, Bare[Scalar], Multiple[OrderType]]
ValueSeries: TypeAlias = Expression[Bound, Indexed[IndexType, Scalar], Single]
BareValueSeries: TypeAlias = Expression[Bound, Bare[Scalar], Single]
DefiniteValueSeries: TypeAlias = Expression[Bound, Indexed[IndexType, Scalar], Definite]
DefiniteBareValueSeries: TypeAlias = Expression[Bound, Bare[Scalar], Definite]

NodeAttributesSeries: TypeAlias = AttributesSeries[NodeIndex, Unordered]
OrderedNodeAttributesSeries: TypeAlias = AttributesSeries[NodeIndex, Ordered]
NodeAttributeSeries: TypeAlias = AttributeSeries[NodeIndex]
DefiniteNodeAttributeSeries: TypeAlias = DefiniteAttributeSeries[NodeIndex]
EdgeAttributesSeries: TypeAlias = AttributesSeries[EdgeIndex, Unordered]
OrderedEdgeAttributesSeries: TypeAlias = AttributesSeries[EdgeIndex, Ordered]
EdgeAttributeSeries: TypeAlias = AttributeSeries[EdgeIndex]
DefiniteEdgeAttributeSeries: TypeAlias = DefiniteAttributeSeries[EdgeIndex]

NodeAttributesTreeSeries: TypeAlias = Expression[
    Bound,
    Indexed[
        Expanded[
            NodeIndex, AttributeNameIndex, Tuple[NodeIndexPayload, Optional[Attribute]]
        ],
        AttributeName,
    ],
    Multiple[Unordered],
]
EdgeAttributesTreeSeries: TypeAlias = Expression[
    Bound,
    Indexed[
        Expanded[
            EdgeIndex, AttributeNameIndex, Tuple[EdgeIndexPayload, Optional[Attribute]]
        ],
        AttributeName,
    ],
    Multiple[Unordered],
]

NodesSeries: TypeAlias = ElementsSeries[NodeIndex, Unordered]
OrderedNodesSeries: TypeAlias = ElementsSeries[NodeIndex, Ordered]
NodeSeries: TypeAlias = ElementSeries[NodeIndex]
DefiniteNodeSeries: TypeAlias = DefiniteElementSeries[NodeIndex]
EdgesSeries: TypeAlias = ElementsSeries[EdgeIndex, Unordered]
OrderedEdgesSeries: TypeAlias = ElementsSeries[EdgeIndex, Ordered]
EdgeSeries: TypeAlias = ElementSeries[EdgeIndex]
DefiniteEdgeSeries: TypeAlias = DefiniteElementSeries[EdgeIndex]

NodeIndicesSeries: TypeAlias = IndicesSeries[NodeIndex, Unordered]
OrderedNodeIndicesSeries: TypeAlias = IndicesSeries[NodeIndex, Ordered]
NodeIndexSeries: TypeAlias = IndexSeries[NodeIndex]
DefiniteNodeIndexSeries: TypeAlias = DefiniteIndexSeries[NodeIndex]
BareNodeIndicesSeries: TypeAlias = BareIndicesSeries[NodeIndex, Unordered]
OrderedBareNodeIndicesSeries: TypeAlias = BareIndicesSeries[NodeIndex, Ordered]
BareNodeIndexSeries: TypeAlias = BareIndexSeries[NodeIndex]
DefiniteBareNodeIndexSeries: TypeAlias = DefiniteBareIndexSeries[NodeIndex]
EdgeIndicesSeries: TypeAlias = IndicesSeries[EdgeIndex, Unordered]
OrderedEdgeIndicesSeries: TypeAlias = IndicesSeries[EdgeIndex, Ordered]
EdgeIndexSeries: TypeAlias = IndexSeries[EdgeIndex]
DefiniteEdgeIndexSeries: TypeAlias = DefiniteIndexSeries[EdgeIndex]
BareEdgeIndicesSeries: TypeAlias = BareIndicesSeries[EdgeIndex, Unordered]
OrderedBareEdgeIndicesSeries: TypeAlias = BareIndicesSeries[EdgeIndex, Ordered]
BareEdgeIndexSeries: TypeAlias = BareIndexSeries[EdgeIndex]
DefiniteBareEdgeIndexSeries: TypeAlias = DefiniteBareIndexSeries[EdgeIndex]

NodeValuesSeries: TypeAlias = ValuesSeries[NodeIndex, Unordered]
OrderedNodeValuesSeries: TypeAlias = ValuesSeries[NodeIndex, Ordered]
NodeValueSeries: TypeAlias = ValueSeries[NodeIndex]
DefiniteNodeValueSeries: TypeAlias = DefiniteValueSeries[NodeIndex]
EdgeValuesSeries: TypeAlias = ValuesSeries[EdgeIndex, Unordered]
OrderedEdgeValuesSeries: TypeAlias = ValuesSeries[EdgeIndex, Ordered]
EdgeValueSeries: TypeAlias = ValueSeries[EdgeIndex]
DefiniteEdgeValueSeries: TypeAlias = DefiniteValueSeries[EdgeIndex]


GroupsSeries: TypeAlias = ElementsSeries[GroupIndex, Unordered]
OrderedGroupsSeries: TypeAlias = ElementsSeries[GroupIndex, Ordered]
GroupSeries: TypeAlias = ElementSeries[GroupIndex]
DefiniteGroupSeries: TypeAlias = DefiniteElementSeries[GroupIndex]

GroupIndicesSeries: TypeAlias = IndicesSeries[GroupIndex, Unordered]
OrderedGroupIndicesSeries: TypeAlias = IndicesSeries[GroupIndex, Ordered]
GroupIndexSeries: TypeAlias = IndexSeries[GroupIndex]
DefiniteGroupIndexSeries: TypeAlias = DefiniteIndexSeries[GroupIndex]
BareGroupIndicesSeries: TypeAlias = BareIndicesSeries[GroupIndex, Unordered]
OrderedBareGroupIndicesSeries: TypeAlias = BareIndicesSeries[GroupIndex, Ordered]
BareGroupIndexSeries: TypeAlias = BareIndexSeries[GroupIndex]
DefiniteBareGroupIndexSeries: TypeAlias = DefiniteBareIndexSeries[GroupIndex]

GroupValuesSeries: TypeAlias = ValuesSeries[GroupIndex, Unordered]
OrderedGroupValuesSeries: TypeAlias = ValuesSeries[GroupIndex, Ordered]
GroupValueSeries: TypeAlias = ValueSeries[GroupIndex]
DefiniteGroupValueSeries: TypeAlias = DefiniteValueSeries[GroupIndex]


class _Result:
    @staticmethod
    def _from_py_payload(payload: object) -> object:
        if isinstance(payload, PyEdgeIndex):
            return EdgeIndexPayload._from_py_edge_index(payload)

        if isinstance(payload, PyFailureKind):
            return FailureKind._from_py_failure_kind(payload)

        if isinstance(payload, PyEdgeEndpointRole):
            return EdgeEndpointRole._from_py_edge_endpoint_role(payload)

        if isinstance(payload, tuple):
            return tuple(_Result._from_py_payload(item) for item in payload)

        return payload

    @staticmethod
    def _to_py_payload(payload: object) -> object:
        if isinstance(payload, EdgeIndexPayload):
            return payload._py_edge_index

        if isinstance(payload, FailureKind):
            return payload._py_failure_kind

        if isinstance(payload, EdgeEndpointRole):
            return payload._into_py_edge_endpoint_role()

        if isinstance(payload, tuple):
            return tuple(_Result._to_py_payload(item) for item in payload)

        return payload


class ResultView(_Result, Generic[ElementType]):
    _py_result_view: PyResultView
    _consumed: bool

    @classmethod
    def _from_py_result_view(cls, py_view: PyResultView) -> ResultView[Any]:
        view = cls.__new__(cls)
        view._py_result_view = py_view
        view._consumed = False

        return view

    def __iter__(self) -> Iterator[ElementType]:
        if self._consumed:
            msg = (
                "the result view is consumed; call `evaluate()` again or collect it "
                "into a list first"
            )
            raise ResultConsumedError(msg)
        self._consumed = True

        return _ResultCursor(iter(self._py_result_view))

    def __repr__(self) -> str:
        return "ResultView()"


class _ResultCursor(Generic[ElementType]):
    _py_result_view: PyResultView

    def __init__(self, py_view: PyResultView) -> None:
        self._py_result_view = py_view

    def __iter__(self) -> _ResultCursor[ElementType]:
        return self

    def __next__(self) -> ElementType:
        return cast("ElementType", _Result._from_py_payload(next(self._py_result_view)))


class GroupedResult(
    _Result, Generic[LeafType, MemberIndexType, KeyIndexType, Unpack[Levels]]
):
    _py_grouped_result: PyGroupedResult

    @classmethod
    def _from_py_grouped_result(
        cls, py_result: PyGroupedResult
    ) -> GroupedResult[Any, Any, Any, Unpack[Tuple[Any, ...]]]:
        result = cls.__new__(cls)
        result._py_grouped_result = py_result

        return result

    @overload
    def __getitem__(
        self: GroupedResult[LeafType, MemberIndexType, Index[KeyPayloadType]],
        key: KeyPayloadType,
    ) -> Union[LeafType, QueryError]: ...

    @overload
    def __getitem__(
        self: GroupedResult[
            LeafType,
            MemberIndexType,
            Index[KeyPayloadType],
            Grouped[InnerMemberIndexType, InnerKeyIndexType],
            Unpack[InnerLevels],
        ],
        key: KeyPayloadType,
    ) -> Union[
        GroupedResult[
            LeafType,
            InnerMemberIndexType,
            InnerKeyIndexType,
            Unpack[InnerLevels],
        ],
        QueryError,
    ]: ...

    def __getitem__(self, key: object) -> object:
        payload = self._py_grouped_result[_Result._to_py_payload(key)]

        if isinstance(payload, PyResultView):
            return ResultView._from_py_result_view(payload)

        if isinstance(payload, PyGroupedResult):
            return GroupedResult._from_py_grouped_result(payload)

        return _Result._from_py_payload(payload)

    def __len__(self) -> int:
        return len(self._py_grouped_result)

    def __contains__(self, key: object) -> bool:
        return _Result._to_py_payload(key) in self._py_grouped_result

    def __iter__(
        self: GroupedResult[
            LeafType, MemberIndexType, Index[KeyPayloadType], Unpack[Levels]
        ],
    ) -> Iterator[KeyPayloadType]:
        return iter(self.keys())

    def keys(
        self: GroupedResult[
            LeafType, MemberIndexType, Index[KeyPayloadType], Unpack[Levels]
        ],
    ) -> Sequence[KeyPayloadType]:
        return cast(
            "Sequence[KeyPayloadType]",
            [_Result._from_py_payload(key) for key in self._py_grouped_result],
        )

    @property
    def key_failures(
        self: GroupedResult[
            LeafType, Index[MemberPayloadType], KeyIndexType, Unpack[Levels]
        ],
    ) -> List[Tuple[MemberPayloadType, QueryError]]:
        return cast(
            "List[Tuple[MemberPayloadType, QueryError]]",
            [
                (_Result._from_py_payload(member), failure)
                for member, failure in self._py_grouped_result.key_failures
            ],
        )

    def __repr__(self) -> str:
        keys = [_Result._from_py_payload(key) for key in self._py_grouped_result]

        return f"GroupedResult(keys={keys!r})"


MembershipResult: TypeAlias = ResultView[Union[IndexPayloadType, QueryError]]
MembershipSingleResult: TypeAlias = Optional[Union[IndexPayloadType, QueryError]]
MembershipDefiniteResult: TypeAlias = Union[IndexPayloadType, QueryError]
IndexedResult: TypeAlias = ResultView[
    Tuple[IndexPayloadType, Union[PayloadType, QueryError]]
]
IndexedSingleResult: TypeAlias = Optional[
    Tuple[IndexPayloadType, Union[PayloadType, QueryError]]
]
IndexedDefiniteResult: TypeAlias = Tuple[
    IndexPayloadType, Union[PayloadType, QueryError]
]
BareResult: TypeAlias = ResultView[Union[PayloadType, QueryError]]
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


def nodes() -> NodesExpression:
    return Expression._from_py_expression(py_nodes())


def edges() -> EdgesExpression:
    return Expression._from_py_expression(py_edges())


def groups() -> GroupsExpression:
    return Expression._from_py_expression(py_groups())
