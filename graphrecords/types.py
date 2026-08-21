"""Type aliases and value types for the graphrecords library."""

from __future__ import annotations

from datetime import datetime, timedelta
from enum import Enum, auto
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Tuple,
    TypeAlias,
    Union,
)

from graphrecords._graphrecords.graphrecord import PyEdgeDirection

if TYPE_CHECKING:
    import polars as pl

    from graphrecords._graphrecords.graphrecord import PyEdgeIndex
    from graphrecords.graphrecord import ArrowStream, EdgeCollector, NodeCollector
    from graphrecords.querying import (
        BareEdgeIndexExpression,
        BareEdgeIndexSeries,
        BareEdgeIndicesExpression,
        BareEdgeIndicesSeries,
        BareGroupIndexExpression,
        BareGroupIndexSeries,
        BareGroupIndicesExpression,
        BareGroupIndicesSeries,
        BareNodeIndexExpression,
        BareNodeIndexSeries,
        BareNodeIndicesExpression,
        BareNodeIndicesSeries,
        BoolExpression,
        BoolMaskExpression,
        BoolMaskSeries,
        BoolSeries,
        DefiniteBareEdgeIndexExpression,
        DefiniteBareEdgeIndexSeries,
        DefiniteBareGroupIndexExpression,
        DefiniteBareGroupIndexSeries,
        DefiniteBareNodeIndexExpression,
        DefiniteBareNodeIndexSeries,
        DefiniteBoolExpression,
        DefiniteBoolSeries,
        DefiniteEdgeExpression,
        DefiniteEdgeIndexExpression,
        DefiniteEdgeIndexSeries,
        DefiniteEdgeSeries,
        DefiniteGroupExpression,
        DefiniteGroupIndexExpression,
        DefiniteGroupIndexSeries,
        DefiniteGroupSeries,
        DefiniteNodeExpression,
        DefiniteNodeIndexExpression,
        DefiniteNodeIndexSeries,
        DefiniteNodeSeries,
        EdgeExpression,
        EdgeIndexExpression,
        EdgeIndexSeries,
        EdgeIndicesExpression,
        EdgeIndicesSeries,
        EdgeSeries,
        EdgesExpression,
        EdgesSeries,
        Expression,
        GroupExpression,
        GroupIndexExpression,
        GroupIndexSeries,
        GroupIndicesExpression,
        GroupIndicesSeries,
        GroupSeries,
        GroupsExpression,
        GroupsSeries,
        Index,
        Indexed,
        IndexedDroppingArgument,
        IndexPayload,
        IndexValue,
        Mask,
        Multiple,
        NodeExpression,
        NodeIndexExpression,
        NodeIndexSeries,
        NodeIndicesExpression,
        NodeIndicesSeries,
        NodeSeries,
        NodesExpression,
        NodesSeries,
        Ordered,
        OrderedBareEdgeIndicesExpression,
        OrderedBareEdgeIndicesSeries,
        OrderedBareGroupIndicesExpression,
        OrderedBareGroupIndicesSeries,
        OrderedBareNodeIndicesExpression,
        OrderedBareNodeIndicesSeries,
        OrderedEdgeIndicesExpression,
        OrderedEdgeIndicesSeries,
        OrderedEdgesExpression,
        OrderedEdgesSeries,
        OrderedGroupIndicesExpression,
        OrderedGroupIndicesSeries,
        OrderedGroupsExpression,
        OrderedGroupsSeries,
        OrderedNodeIndicesExpression,
        OrderedNodeIndicesSeries,
        OrderedNodesExpression,
        OrderedNodesSeries,
        Unit,
        Unordered,
    )
    from graphrecords.querying import EdgeIndex as EdgeIndexDomain
    from graphrecords.querying import GroupIndex as GroupIndexDomain
    from graphrecords.querying import NodeIndex as NodeIndexDomain

#: A type alias for an identifier.
Identifier: TypeAlias = Union[str, int]

#: A type alias for a node index.
NodeIndex: TypeAlias = Identifier

#: A type alias for a group index.
GroupIndex: TypeAlias = Identifier

#: A type alias for an attribute name.
AttributeName: TypeAlias = Identifier

#: A type alias for a plugin name.
PluginName: TypeAlias = Identifier

#: A type alias for an attribute value.
Value: TypeAlias = Union[
    str,
    int,
    float,
    bool,
    datetime,
    timedelta,
    None,
]

#: A type alias for the attributes of a node or an edge.
Attributes: TypeAlias = Dict[AttributeName, Value]


class EdgeDirection(Enum):
    """Enumeration of the directions along which edges of a node are followed."""

    Incoming = auto()
    Outgoing = auto()
    Both = auto()

    def _into_py_edge_direction(self) -> PyEdgeDirection:
        """Converts an EdgeDirection to a PyEdgeDirection.

        Returns:
            PyEdgeDirection: The converted PyEdgeDirection.
        """
        if self == EdgeDirection.Incoming:
            return PyEdgeDirection.Incoming
        if self == EdgeDirection.Outgoing:
            return PyEdgeDirection.Outgoing
        if self == EdgeDirection.Both:
            return PyEdgeDirection.Both
        msg = "Should never be reached"
        raise NotImplementedError(msg)

    def __repr__(self) -> str:
        """Returns the string representation of the edge direction.

        Returns:
            str: The string representation of the edge direction.
        """
        return f"EdgeDirection.{self.name}"

    def __str__(self) -> str:
        """Returns a user-friendly string representation of the edge direction.

        Returns:
            str: The user-friendly string representation of the edge direction.
        """
        return self.name


class EdgeIndex:
    """The index of an edge, handed out by the GraphRecord that created it."""

    _py_edge_index: PyEdgeIndex

    @classmethod
    def _from_py_edge_index(cls, py_edge_index: PyEdgeIndex) -> EdgeIndex:
        """Creates an EdgeIndex from a PyEdgeIndex.

        Args:
            py_edge_index (PyEdgeIndex): The PyEdgeIndex to convert.

        Returns:
            EdgeIndex: The converted EdgeIndex.
        """
        edge_index = cls.__new__(cls)
        edge_index._py_edge_index = py_edge_index
        return edge_index

    def __eq__(self, value: object) -> bool:
        """Checks whether the EdgeIndex is equal to another one.

        Args:
            value (object): The value to compare.

        Returns:
            bool: True if both address the same edge, otherwise False.
        """
        if not isinstance(value, EdgeIndex):
            return NotImplemented

        return self._py_edge_index == value._py_edge_index

    def __hash__(self) -> int:
        """Returns the hash of the EdgeIndex.

        Returns:
            int: The hash of the EdgeIndex.
        """
        return hash(self._py_edge_index)

    def __reduce__(
        self,
    ) -> Tuple[Callable[[PyEdgeIndex], EdgeIndex], Tuple[PyEdgeIndex]]:
        """Reduces the EdgeIndex to what pickle needs to restore it.

        Returns:
            Tuple[Callable[[PyEdgeIndex], EdgeIndex], Tuple[PyEdgeIndex]]: The
                callable that restores the EdgeIndex and its arguments.
        """
        return self._from_py_edge_index, (self._py_edge_index,)

    def __repr__(self) -> str:
        """Returns the string representation of the EdgeIndex.

        Returns:
            str: The string representation of the EdgeIndex.
        """
        return repr(self._py_edge_index)

    def __str__(self) -> str:
        """Returns a user-friendly string representation of the EdgeIndex.

        Returns:
            str: The user-friendly string representation of the EdgeIndex.
        """
        return str(self._py_edge_index)


#: A type alias for everything a GraphRecord accepts as a source of nodes.
NodeSource: TypeAlias = Union[
    Iterable[Tuple[NodeIndex, Attributes]],
    "Tuple[pl.DataFrame, str]",
    "Tuple[ArrowStream, str]",
    "NodeCollector",
]

#: A type alias for everything a GraphRecord accepts as a source of edges.
EdgeSource: TypeAlias = Union[
    Iterable[Tuple[NodeIndex, NodeIndex, Attributes]],
    "Tuple[pl.DataFrame, str, str]",
    "Tuple[ArrowStream, str, str]",
    "EdgeCollector",
]

#: A type alias for a query selecting exactly one node.
SingleNodeQuery: TypeAlias = Union[
    "NodeExpression",
    "DefiniteNodeExpression",
    "NodeSeries",
    "DefiniteNodeSeries",
    "BoolExpression[NodeIndexDomain]",
    "DefiniteBoolExpression[NodeIndexDomain]",
    "BoolSeries[NodeIndexDomain]",
    "DefiniteBoolSeries[NodeIndexDomain]",
    "NodeIndexExpression",
    "DefiniteNodeIndexExpression",
    "BareNodeIndexExpression",
    "DefiniteBareNodeIndexExpression",
    "NodeIndexSeries",
    "DefiniteNodeIndexSeries",
    "BareNodeIndexSeries",
    "DefiniteBareNodeIndexSeries",
]

#: A type alias for a query selecting exactly one edge.
SingleEdgeQuery: TypeAlias = Union[
    "EdgeExpression",
    "DefiniteEdgeExpression",
    "EdgeSeries",
    "DefiniteEdgeSeries",
    "BoolExpression[EdgeIndexDomain]",
    "DefiniteBoolExpression[EdgeIndexDomain]",
    "BoolSeries[EdgeIndexDomain]",
    "DefiniteBoolSeries[EdgeIndexDomain]",
    "EdgeIndexExpression",
    "DefiniteEdgeIndexExpression",
    "BareEdgeIndexExpression",
    "DefiniteBareEdgeIndexExpression",
    "EdgeIndexSeries",
    "DefiniteEdgeIndexSeries",
    "BareEdgeIndexSeries",
    "DefiniteBareEdgeIndexSeries",
]

#: A type alias for a query selecting exactly one group.
SingleGroupQuery: TypeAlias = Union[
    "GroupExpression",
    "DefiniteGroupExpression",
    "GroupSeries",
    "DefiniteGroupSeries",
    "BoolExpression[GroupIndexDomain]",
    "DefiniteBoolExpression[GroupIndexDomain]",
    "BoolSeries[GroupIndexDomain]",
    "DefiniteBoolSeries[GroupIndexDomain]",
    "GroupIndexExpression",
    "DefiniteGroupIndexExpression",
    "BareGroupIndexExpression",
    "DefiniteBareGroupIndexExpression",
    "GroupIndexSeries",
    "DefiniteGroupIndexSeries",
    "BareGroupIndexSeries",
    "DefiniteBareGroupIndexSeries",
]

#: A type alias for a selection of nodes.
MultipleNodeSelection: TypeAlias = Union[
    NodeIndex,
    Iterable[NodeIndex],
    "NodesExpression",
    "OrderedNodesExpression",
    "NodesSeries",
    "OrderedNodesSeries",
    "BoolMaskExpression[NodeIndexDomain, Unordered]",
    "BoolMaskExpression[NodeIndexDomain, Ordered]",
    "BoolMaskSeries[NodeIndexDomain, Unordered]",
    "BoolMaskSeries[NodeIndexDomain, Ordered]",
    "NodeIndicesExpression",
    "OrderedNodeIndicesExpression",
    "BareNodeIndicesExpression",
    "OrderedBareNodeIndicesExpression",
    "NodeIndicesSeries",
    "OrderedNodeIndicesSeries",
    "BareNodeIndicesSeries",
    "OrderedBareNodeIndicesSeries",
    "Expression[Any, Indexed[Index[IndexPayload], IndexValue[NodeIndexDomain]], Multiple[Any]]",
    "IndexedDroppingArgument[NodeIndexDomain, Unit]",
    "IndexedDroppingArgument[NodeIndexDomain, Mask]",
    "IndexedDroppingArgument[NodeIndexDomain, IndexValue[NodeIndexDomain]]",
    SingleNodeQuery,
]

#: A type alias for a selection of edges.
MultipleEdgeSelection: TypeAlias = Union[
    EdgeIndex,
    Iterable[EdgeIndex],
    "EdgesExpression",
    "OrderedEdgesExpression",
    "EdgesSeries",
    "OrderedEdgesSeries",
    "BoolMaskExpression[EdgeIndexDomain, Unordered]",
    "BoolMaskExpression[EdgeIndexDomain, Ordered]",
    "BoolMaskSeries[EdgeIndexDomain, Unordered]",
    "BoolMaskSeries[EdgeIndexDomain, Ordered]",
    "EdgeIndicesExpression",
    "OrderedEdgeIndicesExpression",
    "BareEdgeIndicesExpression",
    "OrderedBareEdgeIndicesExpression",
    "EdgeIndicesSeries",
    "OrderedEdgeIndicesSeries",
    "BareEdgeIndicesSeries",
    "OrderedBareEdgeIndicesSeries",
    "Expression[Any, Indexed[Index[IndexPayload], IndexValue[EdgeIndexDomain]], Multiple[Any]]",
    "IndexedDroppingArgument[EdgeIndexDomain, Unit]",
    "IndexedDroppingArgument[EdgeIndexDomain, Mask]",
    "IndexedDroppingArgument[EdgeIndexDomain, IndexValue[EdgeIndexDomain]]",
    SingleEdgeQuery,
]

#: A type alias for a selection of groups.
MultipleGroupSelection: TypeAlias = Union[
    GroupIndex,
    Iterable[GroupIndex],
    "GroupsExpression",
    "OrderedGroupsExpression",
    "GroupsSeries",
    "OrderedGroupsSeries",
    "BoolMaskExpression[GroupIndexDomain, Unordered]",
    "BoolMaskExpression[GroupIndexDomain, Ordered]",
    "BoolMaskSeries[GroupIndexDomain, Unordered]",
    "BoolMaskSeries[GroupIndexDomain, Ordered]",
    "GroupIndicesExpression",
    "OrderedGroupIndicesExpression",
    "BareGroupIndicesExpression",
    "OrderedBareGroupIndicesExpression",
    "GroupIndicesSeries",
    "OrderedGroupIndicesSeries",
    "BareGroupIndicesSeries",
    "OrderedBareGroupIndicesSeries",
    "Expression[Any, Indexed[Index[IndexPayload], IndexValue[GroupIndexDomain]], Multiple[Any]]",
    "IndexedDroppingArgument[GroupIndexDomain, Unit]",
    "IndexedDroppingArgument[GroupIndexDomain, Mask]",
    "IndexedDroppingArgument[GroupIndexDomain, IndexValue[GroupIndexDomain]]",
    SingleGroupQuery,
]

#: A type alias for a selection of exactly one node.
SingleNodeSelection: TypeAlias = Union[
    NodeIndex,
    SingleNodeQuery,
]

#: A type alias for a selection of exactly one group.
SingleGroupSelection: TypeAlias = Union[
    GroupIndex,
    SingleGroupQuery,
]
