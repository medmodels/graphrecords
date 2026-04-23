"""Type aliases and type checking functions for the graphrecords library."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Literal,
    Mapping,
    Sequence,
    Tuple,
    TypeAlias,
    TypedDict,
    Union,
)

import pandas as pd
import polars as pl

from graphrecords._graphrecords.handle import (
    AttributeHandle as _AttributeHandle,
)
from graphrecords._graphrecords.handle import (
    GroupHandle as _GroupHandle,
)
from graphrecords._graphrecords.handle import (
    NodeHandle as _NodeHandle,
)

if TYPE_CHECKING:
    from typing_extensions import TypeIs

    from graphrecords._graphrecords.graphrecord import PyGraphRecord
    from graphrecords._graphrecords.plugins import PyPreSetSchemaContext
    from graphrecords._graphrecords.schema import PyAttributeType
    from graphrecords.schema import AttributeType


#: A type alias for attributes of a GraphRecord.
GraphRecordAttribute: TypeAlias = Union[str, int]

#: A type alias for a list of GraphRecord attributes.
GraphRecordAttributeInputList: TypeAlias = Union[
    List[str], List[int], List[GraphRecordAttribute]
]

#: A type alias for the value of a GraphRecord attribute.
GraphRecordValue: TypeAlias = Union[str, int, float, bool, datetime, timedelta, None]

#: A type alias for a node index.
NodeIndex: TypeAlias = GraphRecordAttribute

#: A type alias for a list of node indices.
NodeIndexInputList: TypeAlias = GraphRecordAttributeInputList

#: A type alias for a node handle.
NodeHandle: TypeAlias = _NodeHandle

#: A type alias for a group handle.
GroupHandle: TypeAlias = _GroupHandle

#: A type alias for an attribute handle.
AttributeHandle: TypeAlias = _AttributeHandle

#: A type alias for a value that identifies a node — either a name or a handle.
NodeLookup: TypeAlias = Union[NodeIndex, NodeHandle]

#: A type alias for a list of node lookups — homogeneous name or handle lists
#: plus the fully-mixed List[NodeLookup] option for covariant reuse.
NodeLookupInputList: TypeAlias = Union[
    List[str], List[int], List[NodeIndex], List[NodeHandle], List[NodeLookup]
]

#: A type alias for a value that identifies a group — either a name or a handle.
GroupLookup: TypeAlias = "Union[Group, GroupHandle]"

#: A type alias for a list of group lookups — homogeneous name or handle lists
#: plus the fully-mixed List[GroupLookup] option for covariant reuse.
GroupLookupInputList: TypeAlias = (
    "Union[List[str], List[int], List[Group], List[GroupHandle], List[GroupLookup]]"
)

#: A type alias for a value that identifies an attribute — either a name or a handle.
AttributeLookup: TypeAlias = Union[GraphRecordAttribute, AttributeHandle]

#: A type alias for a list of attribute lookups.
AttributeLookupInputList: TypeAlias = Union[
    List[str],
    List[int],
    List[GraphRecordAttribute],
    List[AttributeHandle],
    List[AttributeLookup],
]

#: A type alias for an edge index.
EdgeIndex: TypeAlias = int

#: A type alias for a list of edge indices.
EdgeIndexInputList: TypeAlias = List[EdgeIndex]

#: A type alias for a group.
Group: TypeAlias = GraphRecordAttribute

#: A type alias for a plugin name.
PluginName: TypeAlias = GraphRecordAttribute

#: A type alias for a list of groups.
GroupInputList: TypeAlias = GraphRecordAttributeInputList

#: A type alias for attributes.
Attributes: TypeAlias = Dict[GraphRecordAttribute, GraphRecordValue]

#: A type alias for input attributes.
AttributesInput: TypeAlias = Union[
    Mapping[GraphRecordAttribute, GraphRecordValue],
    Mapping[str, GraphRecordValue],
    Mapping[int, GraphRecordValue],
]

#: A type alias for a node tuple.
NodeTuple: TypeAlias = Union[
    Tuple[str, AttributesInput],
    Tuple[int, AttributesInput],
    Tuple[NodeIndex, AttributesInput],
]

#: A type alias for an edge tuple. Source and target may be either a node name
#: (str/int/NodeIndex) or a NodeHandle.
EdgeTuple: TypeAlias = Tuple[NodeLookup, NodeLookup, AttributesInput]

#: A type alias for input to a Polars DataFrame for nodes.
PolarsNodeDataFrameInput: TypeAlias = Tuple[pl.DataFrame, str]

#: A type alias for input to a Polars DataFrame for edges.
PolarsEdgeDataFrameInput: TypeAlias = Tuple[pl.DataFrame, str, str]

#: A type alias for input to a Pandas DataFrame for nodes.
PandasNodeDataFrameInput: TypeAlias = Tuple[pd.DataFrame, str]

#: A type alias for input to a Pandas DataFrame for edges.
PandasEdgeDataFrameInput: TypeAlias = Tuple[pd.DataFrame, str, str]

#: A type alias for batch input to nodes (everything except a single tuple).
NodeInputBatch: TypeAlias = Union[
    Sequence[NodeTuple],
    PandasNodeDataFrameInput,
    List[PandasNodeDataFrameInput],
    PolarsNodeDataFrameInput,
    List[PolarsNodeDataFrameInput],
]

#: A type alias for input to a node.
NodeInput: TypeAlias = Union[NodeTuple, NodeInputBatch]

#: A type alias for batch input to edges (everything except a single tuple).
EdgeInputBatch: TypeAlias = Union[
    Sequence[EdgeTuple],
    PandasEdgeDataFrameInput,
    List[PandasEdgeDataFrameInput],
    PolarsEdgeDataFrameInput,
    List[PolarsEdgeDataFrameInput],
]

#: A type alias for input to an edge.
EdgeInput: TypeAlias = Union[EdgeTuple, EdgeInputBatch]


class GroupInfo(TypedDict):
    """A dictionary containing lists of node and edge indices for a group."""

    nodes: List[NodeIndex]
    edges: List[EdgeIndex]


class PyCategoricalAttributeOverview(TypedDict):
    """Dictionary for a categorical attribute overview."""

    attribute_type: Literal[PyAttributeType.Categorical]
    distinct_values: List[GraphRecordValue]


class PyContinuousAttributeOverview(TypedDict):
    """Dictionary for a continuous attribute overview."""

    attribute_type: Literal[PyAttributeType.Continuous]
    min: GraphRecordValue
    mean: GraphRecordValue
    max: GraphRecordValue


class PyTemporalAttributeOverview(TypedDict):
    """Dictionary for a temporal attribute overview."""

    attribute_type: Literal[PyAttributeType.Temporal]
    min: GraphRecordValue
    max: GraphRecordValue


class PyUnstructuredAttributeOverview(TypedDict):
    """Dictionary for an unstructured attribute overview."""

    attribute_type: Literal[PyAttributeType.Unstructured]
    distinct_count: int


class CategoricalAttributeOverview(TypedDict):
    """Dictionary for a categorical attribute overview."""

    attribute_type: Literal[AttributeType.Categorical]
    distinct_values: List[GraphRecordValue]


class ContinuousAttributeOverview(TypedDict):
    """Dictionary for a continuous attribute overview."""

    attribute_type: Literal[AttributeType.Continuous]
    min: GraphRecordValue
    mean: GraphRecordValue
    max: GraphRecordValue


class TemporalAttributeOverview(TypedDict):
    """Dictionary for a temporal attribute overview."""

    attribute_type: Literal[AttributeType.Temporal]
    min: GraphRecordValue
    max: GraphRecordValue


class UnstructuredAttributeOverview(TypedDict):
    """Dictionary for an unstructured attribute overview."""

    attribute_type: Literal[AttributeType.Unstructured]
    distinct_count: int


class PolarsDataFramesGroupExport(TypedDict):
    """Dictionary for Polars DataFrames export for a group."""

    nodes: pl.DataFrame
    edges: pl.DataFrame


class PolarsDataFramesExport(TypedDict):
    """Dictionary for Polars DataFrame export."""

    ungrouped: PolarsDataFramesGroupExport
    groups: Dict[Group, PolarsDataFramesGroupExport]


class PandasDataFramesGroupExport(TypedDict):
    """Dictionary for Pandas DataFrames export for a group."""

    nodes: pd.DataFrame
    edges: pd.DataFrame


class PandasDataFramesExport(TypedDict):
    """Dictionary for Pandas DataFrame export."""

    ungrouped: PandasDataFramesGroupExport
    groups: Dict[Group, PandasDataFramesGroupExport]


class _PyPlugin(ABC):  # pyright: ignore[reportUnusedClass]
    @abstractmethod
    def initialize(self, graphrecord: PyGraphRecord) -> None: ...
    @abstractmethod
    def pre_set_schema(
        self, graphrecord: PyGraphRecord, context: PyPreSetSchemaContext
    ) -> PyPreSetSchemaContext: ...
    @abstractmethod
    def post_set_schema(self, graphrecord: PyGraphRecord) -> None: ...


class _PyConnector(ABC):  # pyright: ignore[reportUnusedClass]
    @abstractmethod
    def initialize(self, graphrecord: PyGraphRecord) -> None: ...
    @abstractmethod
    def disconnect(self, graphrecord: PyGraphRecord) -> None: ...
    @abstractmethod
    def ingest(self, graphrecord: PyGraphRecord, data: Any) -> None: ...  # noqa: ANN401
    @abstractmethod
    def export(self, graphrecord: PyGraphRecord) -> Any: ...  # noqa: ANN401


def is_graphrecord_attribute(value: object) -> TypeIs[GraphRecordAttribute]:
    """Check if a value is a GraphRecord attribute.

    Args:
        value (object): The value to check.

    Returns:
        TypeIs[GraphRecordAttribute]: True if the value is a GraphRecord attribute,
            otherwise False.
    """
    return isinstance(value, (str, int)) and not isinstance(value, bool)


def is_graphrecord_value(value: object) -> TypeIs[GraphRecordValue]:
    """Check if a value is a valid GraphRecord value.

    Args:
        value (object): The value to check.

    Returns:
        TypeIs[GraphRecordValue]: True if the value is a valid GraphRecord value,
            otherwise False.
    """
    return isinstance(value, (str, int, float, bool, datetime)) or value is None


def is_node_index(value: object) -> TypeIs[NodeIndex]:
    """Check if a value is a valid node index.

    Args:
        value (object): The value to check.

    Returns:
        TypeIs[NodeIndex]: True if the value is a valid node index, otherwise False.
    """
    return is_graphrecord_attribute(value)


def is_node_index_list(value: object) -> TypeIs[NodeIndexInputList]:
    """Check if a value is a valid list of node indices.

    Args:
        value (object): The value to check.

    Returns:
        TypeIs[NodeIndexInputList]: True if the value is a valid list of node indices,
            otherwise False.
    """
    return isinstance(value, list) and all(is_node_index(input) for input in value)


def is_edge_index(value: object) -> TypeIs[EdgeIndex]:
    """Check if a value is a valid edge index.

    Args:
        value (object): The value to check.

    Returns:
        TypeIs[EdgeIndex]: True if the value is a valid edge index, otherwise False.
    """
    return isinstance(value, int)


def is_edge_index_list(value: object) -> TypeIs[EdgeIndexInputList]:
    """Check if a value is a valid list of edge indices.

    Args:
        value (object): The value to check.

    Returns:
        TypeIs[EdgeIndexInputList]: True if the value is a valid list of edge indices,
            otherwise False.
    """
    return isinstance(value, list) and all(is_edge_index(input) for input in value)


def is_group(value: object) -> TypeIs[Group]:
    """Check if a value is a valid group.

    Args:
        value (object): The value to check.

    Returns:
        TypeIs[Group]: True if the value is a valid group, otherwise False.
    """
    return is_graphrecord_attribute(value)


def is_attributes(value: object) -> TypeIs[Attributes]:
    """Check if a value is a valid attributes dictionary.

    Args:
        value (object): The value to check.

    Returns:
        TypeIs[Attributes]: True if the value is a valid attributes dictionary,
            otherwise False.
    """
    return isinstance(value, dict)


def is_node_tuple(value: object) -> TypeIs[NodeTuple]:
    """Check if a value is a valid node tuple.

    Args:
        value (object): The value to check.

    Returns:
        TypeIs[NodeTuple]: True if the value is a valid node tuple, otherwise False.
    """
    return (
        isinstance(value, tuple)
        and len(value) == 2
        and is_graphrecord_attribute(value[0])
        and is_attributes(value[1])
    )


def is_node_tuple_list(value: object) -> TypeIs[List[NodeTuple]]:
    """Check if a value is a list of valid node tuples.

    Args:
        value (object): The value to check.

    Returns:
        TypeIs[List[NodeTuple]]: True if the value is a list of valid node tuples,
            otherwise False.
    """
    return isinstance(value, list) and all(is_node_tuple(input) for input in value)


def is_node_lookup(value: object) -> TypeIs[NodeLookup]:
    """Check if a value is a valid node lookup (name or handle).

    Args:
        value (object): The value to check.

    Returns:
        TypeIs[NodeLookup]: True if the value is a NodeIndex name or a NodeHandle,
            otherwise False.
    """
    return is_node_index(value) or isinstance(value, _NodeHandle)


def is_edge_tuple(value: object) -> TypeIs[EdgeTuple]:
    """Check if a value is a valid edge tuple.

    Args:
        value (object): The value to check.

    Returns:
        TypeIs[EdgeTuple]: True if the value is a valid edge tuple, otherwise False.
    """
    return (
        isinstance(value, tuple)
        and len(value) == 3
        and is_node_lookup(value[0])
        and is_node_lookup(value[1])
        and is_attributes(value[2])
    )


def is_edge_tuple_list(value: object) -> TypeIs[List[EdgeTuple]]:
    """Check if a value is a list of valid edge tuples.

    Args:
        value (object): The value to check.

    Returns:
        TypeIs[List[EdgeTuple]]: True if the value is a list of valid edge tuples,
            otherwise False.
    """
    return isinstance(value, list) and all(is_edge_tuple(input) for input in value)


def is_polars_node_dataframe_input(
    value: object,
) -> TypeIs[PolarsNodeDataFrameInput]:
    """Check if a value is a valid Polars DataFrame input for nodes.

    Args:
        value (object): The value to check.

    Returns:
        TypeIs[PolarsNodeDataFrameInput]: True if the value is a valid Polars DataFrame
            input for nodes, otherwise False.
    """
    return (
        isinstance(value, tuple)
        and len(value) == 2
        and isinstance(value[0], pl.DataFrame)
        and isinstance(value[1], str)
    )


def is_polars_node_dataframe_input_list(
    value: object,
) -> TypeIs[List[PolarsNodeDataFrameInput]]:
    """Check if a value is a list of valid Polars DataFrame inputs for nodes.

    Args:
        value (object): The value to check.

    Returns:
        TypeIs[List[PolarsNodeDataFrameInput]]: True if the value is a list of valid
            Polars DataFrame inputs for nodes, otherwise False.
    """
    return isinstance(value, list) and all(
        is_polars_node_dataframe_input(input) for input in value
    )


def is_polars_edge_dataframe_input(
    value: object,
) -> TypeIs[PolarsEdgeDataFrameInput]:
    """Check if a value is a valid Polars DataFrame input for edges.

    Args:
        value (object): The value to check.

    Returns:
        TypeIs[PolarsEdgeDataFrameInput]: True if the value is a valid Polars DataFrame
            input for edges, otherwise False.
    """
    return (
        isinstance(value, tuple)
        and len(value) == 3
        and isinstance(value[0], pl.DataFrame)
        and isinstance(value[1], str)
        and isinstance(value[2], str)
    )


def is_polars_edge_dataframe_input_list(
    value: object,
) -> TypeIs[List[PolarsEdgeDataFrameInput]]:
    """Check if a value is a list of valid Polars DataFrame inputs for edges.

    Args:
        value (object): The value to check.

    Returns:
        TypeIs[List[PolarsEdgeDataFrameInput]]: True if the value is a list of valid
            Polars DataFrame inputs for edges, otherwise False.
    """
    return isinstance(value, list) and all(
        is_polars_edge_dataframe_input(input) for input in value
    )


def is_pandas_node_dataframe_input(
    value: object,
) -> TypeIs[PandasNodeDataFrameInput]:
    """Check if a value is a valid Pandas DataFrame input for nodes.

    Args:
        value (object): The value to check.

    Returns:
        TypeIs[PandasNodeDataFrameInput]: True if the value is a valid Pandas DataFrame
            input for nodes, otherwise False.
    """
    return (
        isinstance(value, tuple)
        and len(value) == 2
        and isinstance(value[0], pd.DataFrame)
        and isinstance(value[1], str)
    )


def is_pandas_node_dataframe_input_list(
    value: object,
) -> TypeIs[List[PandasNodeDataFrameInput]]:
    """Check if a value is a list of valid Pandas DataFrame inputs for nodes.

    Args:
        value (object): The value to check.

    Returns:
        TypeIs[List[PandasNodeDataFrameInput]]: True if the value is a list of valid
            Pandas DataFrame inputs for nodes, otherwise False.
    """
    return isinstance(value, list) and all(
        is_pandas_node_dataframe_input(input) for input in value
    )


def is_pandas_edge_dataframe_input(
    value: object,
) -> TypeIs[PandasEdgeDataFrameInput]:
    """Check if a value is a valid Pandas DataFrame input for edges.

    Args:
        value (object): The value to check.

    Returns:
        TypeIs[PandasEdgeDataFrameInput]: True if the value is a valid Pandas DataFrame
            input for edges, otherwise False.
    """
    return (
        isinstance(value, tuple)
        and len(value) == 3
        and isinstance(value[0], pd.DataFrame)
        and isinstance(value[1], str)
        and isinstance(value[2], str)
    )


def is_pandas_edge_dataframe_input_list(
    value: object,
) -> TypeIs[List[PandasEdgeDataFrameInput]]:
    """Check if a value is a list of valid Pandas DataFrame inputs for edges.

    Args:
        value (object): The value to check.

    Returns:
        TypeIs[List[PandasEdgeDataFrameInput]]: True if the value is a list of valid
            Pandas DataFrame inputs for edges, otherwise False.
    """
    return isinstance(value, list) and all(
        is_pandas_edge_dataframe_input(input) for input in value
    )
