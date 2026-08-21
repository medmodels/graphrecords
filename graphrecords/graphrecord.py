"""GraphRecord class for managing data records using nodes, edges and groups.

The `GraphRecord` class is the core component of the `graphrecords` package. A
GraphRecord is an immutable value: every verb leaves the receiver untouched and
returns the new GraphRecord that carries the change.
"""

from __future__ import annotations

from enum import Enum, auto
from typing import (
    TYPE_CHECKING,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Protocol,
    Tuple,
    TypeVar,
    Union,
    cast,
)

from typing_extensions import TypedDict

from graphrecords._graphrecords.graphrecord import PyGraphRecord, PyOnConflict
from graphrecords.overview import DEFAULT_TRUNCATE_DETAILS, GroupOverview, Overview
from graphrecords.querying import Argument, Expression, Series
from graphrecords.schema import Schema
from graphrecords.types import EdgeDirection, EdgeIndex

if TYPE_CHECKING:
    import os
    from typing import Callable

    import pandas as pd
    import polars as pl
    from typing_extensions import Unpack

    from graphrecords._graphrecords.graphrecord import (
        PyEdgeIndex,
        PyEdgeView,
        PyGroupView,
        PyNodeView,
        PyRecordBatch,
    )
    from graphrecords._graphrecords.querying import PyArgument, PyExpression, PySeries
    from graphrecords.plugins import Plugin, _PluginBridge
    from graphrecords.querying import (
        C,
        EdgesSeries,
        GroupsSeries,
        Levels,
        NodesSeries,
        S,
        Unbound,
    )
    from graphrecords.types import (
        AttributeName,
        Attributes,
        EdgeSource,
        GroupIndex,
        MultipleEdgeSelection,
        MultipleGroupSelection,
        MultipleNodeSelection,
        NodeIndex,
        NodeSource,
        PluginName,
        SingleGroupSelection,
        SingleNodeSelection,
        Value,
    )


class OnConflict(Enum):
    """Enumeration of how a merge resolves attributes both GraphRecords define."""

    Raise = auto()
    KeepSelf = auto()
    KeepOther = auto()

    @staticmethod
    def _from_py_on_conflict(py_on_conflict: PyOnConflict) -> OnConflict:
        """Converts a PyOnConflict to an OnConflict.

        Args:
            py_on_conflict (PyOnConflict): The PyOnConflict to convert.

        Returns:
            OnConflict: The converted OnConflict.
        """
        if py_on_conflict == PyOnConflict.Raise:
            return OnConflict.Raise
        if py_on_conflict == PyOnConflict.KeepSelf:
            return OnConflict.KeepSelf
        if py_on_conflict == PyOnConflict.KeepOther:
            return OnConflict.KeepOther
        msg = "Should never be reached"
        raise NotImplementedError(msg)

    def _into_py_on_conflict(self) -> PyOnConflict:
        """Converts an OnConflict to a PyOnConflict.

        Returns:
            PyOnConflict: The converted PyOnConflict.
        """
        if self == OnConflict.Raise:
            return PyOnConflict.Raise
        if self == OnConflict.KeepSelf:
            return PyOnConflict.KeepSelf
        if self == OnConflict.KeepOther:
            return PyOnConflict.KeepOther
        msg = "Should never be reached"
        raise NotImplementedError(msg)

    def __repr__(self) -> str:
        """Returns the string representation of the conflict policy.

        Returns:
            str: The string representation of the conflict policy.
        """
        return f"OnConflict.{self.name}"

    def __str__(self) -> str:
        """Returns a user-friendly string representation of the conflict policy.

        Returns:
            str: The user-friendly string representation of the conflict policy.
        """
        return self.name


class NodeCollector(Protocol):
    """Protocol for objects that hand out the nodes to add to a GraphRecord."""

    def collect_nodes(self) -> Iterable[Tuple[NodeIndex, Attributes]]:
        """Collects the nodes to add.

        Returns:
            Iterable[Tuple[NodeIndex, Attributes]]: The index and attributes of each
                node.
        """
        ...


class EdgeCollector(Protocol):
    """Protocol for objects that hand out the edges to add to a GraphRecord."""

    def collect_edges(self) -> Iterable[Tuple[NodeIndex, NodeIndex, Attributes]]:
        """Collects the edges to add.

        Returns:
            Iterable[Tuple[NodeIndex, NodeIndex, Attributes]]: The source node index,
                target node index and attributes of each edge.
        """
        ...


class ArrowStream(Protocol):
    """Protocol for objects that export their data as an Arrow stream."""

    def __arrow_c_stream__(self, requested_schema: Optional[object] = None) -> object:
        """Exports the data as an Arrow C stream capsule.

        Args:
            requested_schema (Optional[object]): The schema the consumer asks for.

        Returns:
            object: A PyCapsule holding the Arrow C stream.
        """
        ...


T = TypeVar("T", covariant=True)


class Writer(Protocol[T]):
    """Protocol for objects that write out a GraphRecord."""

    def write(self, record: GraphRecord) -> T:
        """Writes out the GraphRecord.

        Args:
            record (GraphRecord): The GraphRecord to write out.

        Returns:
            T: The result of writing out the GraphRecord.
        """
        ...


class RecordBatch:
    """A table of exported rows, readable through the Arrow PyCapsule interface."""

    _py_record_batch: PyRecordBatch

    @classmethod
    def _from_py_record_batch(cls, py_record_batch: PyRecordBatch) -> RecordBatch:
        """Creates a RecordBatch from a PyRecordBatch.

        Args:
            py_record_batch (PyRecordBatch): The PyRecordBatch to convert.

        Returns:
            RecordBatch: The converted RecordBatch.
        """
        record_batch = cls.__new__(cls)
        record_batch._py_record_batch = py_record_batch
        return record_batch

    def __arrow_c_array__(
        self, requested_schema: Optional[object] = None
    ) -> Tuple[object, object]:
        """Exports the table as an Arrow C array capsule.

        Args:
            requested_schema (Optional[object]): The schema the consumer asks for.

        Returns:
            Tuple[object, object]: The schema capsule and the array capsule.
        """
        return self._py_record_batch.__arrow_c_array__(requested_schema)

    def __arrow_c_stream__(self, requested_schema: Optional[object] = None) -> object:
        """Exports the table as an Arrow C stream capsule.

        Args:
            requested_schema (Optional[object]): The schema the consumer asks for.

        Returns:
            object: A PyCapsule holding the Arrow C stream.
        """
        return self._py_record_batch.__arrow_c_stream__(requested_schema)

    def __len__(self) -> int:
        """Counts the rows of the table.

        Returns:
            int: The number of rows.
        """
        return len(self._py_record_batch)


class Tables(TypedDict, Generic[T]):
    """The node and edge tables of one partition of an export."""

    nodes: T
    edges: T


class Export(TypedDict, Generic[T]):
    """A GraphRecord exported to tables, partitioned by group."""

    ungrouped: Tables[T]
    groups: Dict[GroupIndex, Tables[T]]


class RonFile:
    """Writer that writes a GraphRecord to a RON file."""

    _path: Union[str, os.PathLike[str]]

    def __init__(self, path: Union[str, os.PathLike[str]]) -> None:
        """Initializes a writer for a RON file.

        Args:
            path (Union[str, os.PathLike[str]]): The path of the file to write.
        """
        self._path = path

    def write(self, record: GraphRecord) -> None:
        """Writes the GraphRecord to the RON file.

        Args:
            record (GraphRecord): The GraphRecord to write.
        """
        record.to_ron(self._path)


class PolarsFrames:
    """Writer that exports a GraphRecord to Polars DataFrames."""

    def write(self, record: GraphRecord) -> Export[pl.DataFrame]:
        """Exports the GraphRecord to Polars DataFrames, partitioned by group.

        Args:
            record (GraphRecord): The GraphRecord to export.

        Returns:
            Export[pl.DataFrame]: The node and edge tables of every group and of
                the ungrouped part.
        """
        return record.to_polars()


class ArrowTables:
    """Writer that exports a GraphRecord to Arrow record batches."""

    def write(self, record: GraphRecord) -> Export[RecordBatch]:
        """Exports the GraphRecord to Arrow record batches, partitioned by group.

        Args:
            record (GraphRecord): The GraphRecord to export.

        Returns:
            Export[RecordBatch]: The node and edge tables of every group and of
                the ungrouped part.
        """
        return record.to_arrow()


class _WriterBridge(Generic[T]):
    """Adapts a Writer to the write hook a GraphRecord calls it through.

    A GraphRecord hands the write hook the record as the binding holds it, so the
    bridge wraps it into the GraphRecord the writer is written against.
    """

    _writer: Writer[T]

    def __init__(self, writer: Writer[T]) -> None:
        """Initializes a bridge around a writer.

        Args:
            writer (Writer[T]): The writer to adapt.
        """
        self._writer = writer

    def write(self, py_record: PyGraphRecord) -> T:
        """Writes out the record a GraphRecord passes to the hook.

        Args:
            py_record (PyGraphRecord): The record to write out.

        Returns:
            T: What the writer handed back.
        """
        return self._writer.write(GraphRecord._from_py_graphrecord(py_record))


class NodeView:
    """A read-only view of a single node of a GraphRecord."""

    _py_node_view: PyNodeView

    @classmethod
    def _from_py_node_view(cls, py_node_view: PyNodeView) -> NodeView:
        """Creates a NodeView from a PyNodeView.

        Args:
            py_node_view (PyNodeView): The PyNodeView to convert.

        Returns:
            NodeView: The converted NodeView.
        """
        node_view = cls.__new__(cls)
        node_view._py_node_view = py_node_view
        return node_view

    def index(self) -> NodeIndex:
        """Returns the index of the node.

        Returns:
            NodeIndex: The index of the node.
        """
        return self._py_node_view.index()

    def attribute(self, attribute_name: AttributeName) -> Value:
        """Reads a single attribute of the node.

        Args:
            attribute_name (AttributeName): The name of the attribute to read.

        Returns:
            Value: The value of the attribute.
        """
        return self._py_node_view.attribute(attribute_name)

    def attributes(self) -> Attributes:
        """Reads all attributes of the node.

        Returns:
            Attributes: The attributes of the node.
        """
        return self._py_node_view.attributes()

    def groups(self) -> List[GroupIndex]:
        """Lists the groups the node belongs to.

        Returns:
            List[GroupIndex]: The index of every group holding the node.
        """
        return self._py_node_view.groups()

    def edges(self, direction: EdgeDirection = EdgeDirection.Both) -> List[EdgeIndex]:
        """Lists the edges attached to the node.

        Args:
            direction (EdgeDirection): The direction to follow. Defaults to
                EdgeDirection.Both.

        Returns:
            List[EdgeIndex]: The index of every attached edge.
        """
        return [
            EdgeIndex._from_py_edge_index(edge_index)
            for edge_index in self._py_node_view.edges(
                direction._into_py_edge_direction()
            )
        ]

    def neighbors(
        self, direction: EdgeDirection = EdgeDirection.Both
    ) -> List[NodeIndex]:
        """Lists the nodes the node is connected to.

        Args:
            direction (EdgeDirection): The direction to follow. Defaults to
                EdgeDirection.Both.

        Returns:
            List[NodeIndex]: The index of every neighboring node.
        """
        return self._py_node_view.neighbors(direction._into_py_edge_direction())

    def degree(self, direction: EdgeDirection = EdgeDirection.Both) -> int:
        """Counts the edges attached to the node.

        Args:
            direction (EdgeDirection): The direction to follow. Defaults to
                EdgeDirection.Both.

        Returns:
            int: The number of attached edges.
        """
        return self._py_node_view.degree(direction._into_py_edge_direction())

    def edges_to(
        self,
        target: SingleNodeSelection,
        direction: EdgeDirection = EdgeDirection.Outgoing,
    ) -> List[EdgeIndex]:
        """Lists the edges between the node and another one.

        Args:
            target (SingleNodeSelection): The node at the other end.
            direction (EdgeDirection): The direction to follow. Defaults to
                EdgeDirection.Outgoing.

        Returns:
            List[EdgeIndex]: The index of every edge between both nodes.
        """
        return [
            EdgeIndex._from_py_edge_index(edge_index)
            for edge_index in self._py_node_view.edges_to(
                GraphRecord._unwrap_single_selection(target),
                direction._into_py_edge_direction(),
            )
        ]

    def __repr__(self) -> str:
        """Returns the string representation of the NodeView.

        Returns:
            str: The string representation of the NodeView.
        """
        return repr(self._py_node_view)


class EdgeView:
    """A read-only view of a single edge of a GraphRecord."""

    _py_edge_view: PyEdgeView

    @classmethod
    def _from_py_edge_view(cls, py_edge_view: PyEdgeView) -> EdgeView:
        """Creates an EdgeView from a PyEdgeView.

        Args:
            py_edge_view (PyEdgeView): The PyEdgeView to convert.

        Returns:
            EdgeView: The converted EdgeView.
        """
        edge_view = cls.__new__(cls)
        edge_view._py_edge_view = py_edge_view
        return edge_view

    def index(self) -> EdgeIndex:
        """Returns the index of the edge.

        Returns:
            EdgeIndex: The index of the edge.
        """
        return EdgeIndex._from_py_edge_index(self._py_edge_view.index())

    def source(self) -> NodeIndex:
        """Returns the node the edge starts at.

        Returns:
            NodeIndex: The index of the source node.
        """
        return self._py_edge_view.source()

    def target(self) -> NodeIndex:
        """Returns the node the edge ends at.

        Returns:
            NodeIndex: The index of the target node.
        """
        return self._py_edge_view.target()

    def attribute(self, attribute_name: AttributeName) -> Value:
        """Reads a single attribute of the edge.

        Args:
            attribute_name (AttributeName): The name of the attribute to read.

        Returns:
            Value: The value of the attribute.
        """
        return self._py_edge_view.attribute(attribute_name)

    def attributes(self) -> Attributes:
        """Reads all attributes of the edge.

        Returns:
            Attributes: The attributes of the edge.
        """
        return self._py_edge_view.attributes()

    def groups(self) -> List[GroupIndex]:
        """Lists the groups the edge belongs to.

        Returns:
            List[GroupIndex]: The index of every group holding the edge.
        """
        return self._py_edge_view.groups()

    def __repr__(self) -> str:
        """Returns the string representation of the EdgeView.

        Returns:
            str: The string representation of the EdgeView.
        """
        return repr(self._py_edge_view)


class GroupView:
    """A read-only view of a single group of a GraphRecord."""

    _py_group_view: PyGroupView

    @classmethod
    def _from_py_group_view(cls, py_group_view: PyGroupView) -> GroupView:
        """Creates a GroupView from a PyGroupView.

        Args:
            py_group_view (PyGroupView): The PyGroupView to convert.

        Returns:
            GroupView: The converted GroupView.
        """
        group_view = cls.__new__(cls)
        group_view._py_group_view = py_group_view
        return group_view

    def index(self) -> GroupIndex:
        """Returns the index of the group.

        Returns:
            GroupIndex: The index of the group.
        """
        return self._py_group_view.index()

    def nodes(self) -> List[NodeIndex]:
        """Lists the nodes of the group.

        Returns:
            List[NodeIndex]: The index of every node in the group.
        """
        return self._py_group_view.nodes()

    def edges(self) -> List[EdgeIndex]:
        """Lists the edges of the group.

        Returns:
            List[EdgeIndex]: The index of every edge in the group.
        """
        return [
            EdgeIndex._from_py_edge_index(edge_index)
            for edge_index in self._py_group_view.edges()
        ]

    def node_count(self) -> int:
        """Counts the nodes of the group.

        Returns:
            int: The number of nodes.
        """
        return self._py_group_view.node_count()

    def edge_count(self) -> int:
        """Counts the edges of the group.

        Returns:
            int: The number of edges.
        """
        return self._py_group_view.edge_count()

    def __repr__(self) -> str:
        """Returns the string representation of the GroupView.

        Returns:
            str: The string representation of the GroupView.
        """
        return repr(self._py_group_view)


class GraphRecord:
    """An immutable record of nodes, edges and the groups they belong to."""

    _py_graphrecord: PyGraphRecord

    def __init__(self) -> None:
        """Initializes an empty GraphRecord."""
        self._py_graphrecord = PyGraphRecord()

    @classmethod
    def _from_py_graphrecord(cls, py_graphrecord: PyGraphRecord) -> GraphRecord:
        """Creates a GraphRecord from a PyGraphRecord.

        Args:
            py_graphrecord (PyGraphRecord): The PyGraphRecord to convert.

        Returns:
            GraphRecord: The converted GraphRecord.
        """
        graphrecord = cls.__new__(cls)
        graphrecord._py_graphrecord = py_graphrecord
        return graphrecord

    @classmethod
    def with_schema(cls, schema: Schema) -> GraphRecord:
        """Creates an empty GraphRecord that validates against the given schema.

        Args:
            schema (Schema): The schema the GraphRecord validates against.

        Returns:
            GraphRecord: An empty GraphRecord carrying the schema.
        """
        return cls._from_py_graphrecord(PyGraphRecord.with_schema(schema._py_schema))

    @classmethod
    def from_ron(cls, path: Union[str, os.PathLike[str]]) -> GraphRecord:
        """Reads a GraphRecord from a RON file.

        Args:
            path (Union[str, os.PathLike[str]]): The path of the file to read.

        Returns:
            GraphRecord: The GraphRecord stored in the file.
        """
        return cls._from_py_graphrecord(PyGraphRecord.from_ron(path))

    @staticmethod
    def _unwrap_selection(
        selection: Union[MultipleNodeSelection, MultipleGroupSelection],
    ) -> Union[
        PyArgument,
        PyExpression,
        PySeries,
        str,
        int,
        Iterable[Union[str, int]],
    ]:
        """Unwraps a selection of nodes or groups for the binding.

        Args:
            selection (Union[MultipleNodeSelection, MultipleGroupSelection]): The
                selection to unwrap.

        Returns:
            Union[PyArgument, PyExpression, PySeries, str, int, Iterable[Union[str,
                int]]]: The unwrapped selection.
        """
        if isinstance(selection, Expression):
            return selection._py_carrier
        if isinstance(selection, Argument):
            return selection._py_argument

        return selection

    @staticmethod
    def _unwrap_single_selection(
        selection: Union[SingleNodeSelection, SingleGroupSelection],
    ) -> Union[PyExpression, PySeries, str, int]:
        """Unwraps a selection of exactly one node or group for the binding.

        Args:
            selection (Union[SingleNodeSelection, SingleGroupSelection]): The
                selection to unwrap.

        Returns:
            Union[PyExpression, PySeries, str, int]: The unwrapped selection.
        """
        if isinstance(selection, Expression):
            return selection._py_carrier

        return selection

    @staticmethod
    def _unwrap_edge_selection(
        edge_indices: MultipleEdgeSelection,
    ) -> Union[PyArgument, PyExpression, PySeries, PyEdgeIndex, List[PyEdgeIndex]]:
        """Unwraps a selection of edges for the binding.

        Args:
            edge_indices (MultipleEdgeSelection): The selection to unwrap.

        Returns:
            Union[PyArgument, PyExpression, PySeries, PyEdgeIndex, List[PyEdgeIndex]]:
                The unwrapped selection.
        """
        if isinstance(edge_indices, Expression):
            return edge_indices._py_carrier
        if isinstance(edge_indices, Argument):
            return edge_indices._py_argument
        if isinstance(edge_indices, EdgeIndex):
            return edge_indices._py_edge_index

        return [edge_index._py_edge_index for edge_index in edge_indices]

    @property
    def plugins(self) -> List[PluginName]:
        """The names of the plugins attached to the GraphRecord.

        Returns:
            List[PluginName]: The name of every attached plugin.
        """
        return self._py_graphrecord.plugins

    @property
    def plugin_entries(self) -> Dict[PluginName, Plugin]:
        """The plugins attached to the GraphRecord, by the name they are attached under.

        Returns:
            Dict[PluginName, Plugin]: Every attached plugin, by its name.
        """
        return {
            name: cast("_PluginBridge", bridge)._plugin
            for name, bridge in self._py_graphrecord.plugin_entries.items()
        }

    def add_plugin(self, name: PluginName, plugin: Plugin) -> GraphRecord:
        """Attaches a plugin under the given name.

        Args:
            name (PluginName): The name to attach the plugin under.
            plugin (Plugin): The plugin to attach.

        Returns:
            GraphRecord: A GraphRecord with the plugin attached.
        """
        return self._from_py_graphrecord(
            self._py_graphrecord.add_plugin(name, plugin._bridge())
        )

    def remove_plugin(self, name: PluginName) -> GraphRecord:
        """Detaches the plugin attached under the given name.

        Args:
            name (PluginName): The name the plugin is attached under.

        Returns:
            GraphRecord: A GraphRecord without that plugin.
        """
        return self._from_py_graphrecord(self._py_graphrecord.remove_plugin(name))

    def add_nodes(self, source: NodeSource) -> GraphRecord:
        """Adds the nodes of the given source.

        Args:
            source (NodeSource): The nodes to add.

        Returns:
            GraphRecord: A GraphRecord containing the added nodes.
        """
        return self._from_py_graphrecord(self._py_graphrecord.add_nodes(source))

    def add_node(
        self, node_index: SingleNodeSelection, attributes: Attributes
    ) -> GraphRecord:
        """Adds a single node with the given attributes.

        Args:
            node_index (SingleNodeSelection): The index of the node to add.
            attributes (Attributes): The attributes of the node.

        Returns:
            GraphRecord: A GraphRecord containing the added node.
        """
        return self._from_py_graphrecord(
            self._py_graphrecord.add_node(
                self._unwrap_single_selection(node_index), attributes
            )
        )

    def add_nodes_in_group(
        self, source: NodeSource, group_index: SingleGroupSelection
    ) -> GraphRecord:
        """Adds the nodes of the given source to a group.

        Args:
            source (NodeSource): The nodes to add.
            group_index (SingleGroupSelection): The group the nodes are added in.

        Returns:
            GraphRecord: A GraphRecord containing the added nodes.
        """
        return self._from_py_graphrecord(
            self._py_graphrecord.add_nodes_in_group(
                source, self._unwrap_single_selection(group_index)
            )
        )

    def add_node_in_group(
        self,
        node_index: SingleNodeSelection,
        attributes: Attributes,
        group_index: SingleGroupSelection,
    ) -> GraphRecord:
        """Adds a single node with the given attributes to a group.

        Args:
            node_index (SingleNodeSelection): The index of the node to add.
            attributes (Attributes): The attributes of the node.
            group_index (SingleGroupSelection): The group the node is added in.

        Returns:
            GraphRecord: A GraphRecord containing the added node.
        """
        return self._from_py_graphrecord(
            self._py_graphrecord.add_node_in_group(
                self._unwrap_single_selection(node_index),
                attributes,
                self._unwrap_single_selection(group_index),
            )
        )

    def add_edges(self, source: EdgeSource) -> GraphRecord:
        """Adds the edges of the given source.

        Args:
            source (EdgeSource): The edges to add.

        Returns:
            GraphRecord: A GraphRecord containing the added edges.
        """
        return self._from_py_graphrecord(self._py_graphrecord.add_edges(source))

    def add_edge(
        self,
        source_node_index: SingleNodeSelection,
        target_node_index: SingleNodeSelection,
        attributes: Attributes,
    ) -> GraphRecord:
        """Adds a single edge with the given attributes.

        Args:
            source_node_index (SingleNodeSelection): The node the edge starts at.
            target_node_index (SingleNodeSelection): The node the edge ends at.
            attributes (Attributes): The attributes of the edge.

        Returns:
            GraphRecord: A GraphRecord containing the added edge.
        """
        return self._from_py_graphrecord(
            self._py_graphrecord.add_edge(
                self._unwrap_single_selection(source_node_index),
                self._unwrap_single_selection(target_node_index),
                attributes,
            )
        )

    def add_edges_in_group(
        self, source: EdgeSource, group_index: SingleGroupSelection
    ) -> GraphRecord:
        """Adds the edges of the given source to a group.

        Args:
            source (EdgeSource): The edges to add.
            group_index (SingleGroupSelection): The group the edges are added in.

        Returns:
            GraphRecord: A GraphRecord containing the added edges.
        """
        return self._from_py_graphrecord(
            self._py_graphrecord.add_edges_in_group(
                source, self._unwrap_single_selection(group_index)
            )
        )

    def add_edge_in_group(
        self,
        source_node_index: SingleNodeSelection,
        target_node_index: SingleNodeSelection,
        attributes: Attributes,
        group_index: SingleGroupSelection,
    ) -> GraphRecord:
        """Adds a single edge with the given attributes to a group.

        Args:
            source_node_index (SingleNodeSelection): The node the edge starts at.
            target_node_index (SingleNodeSelection): The node the edge ends at.
            attributes (Attributes): The attributes of the edge.
            group_index (SingleGroupSelection): The group the edge is added in.

        Returns:
            GraphRecord: A GraphRecord containing the added edge.
        """
        return self._from_py_graphrecord(
            self._py_graphrecord.add_edge_in_group(
                self._unwrap_single_selection(source_node_index),
                self._unwrap_single_selection(target_node_index),
                attributes,
                self._unwrap_single_selection(group_index),
            )
        )

    def remove_nodes(self, node_indices: MultipleNodeSelection) -> GraphRecord:
        """Removes the selected nodes and the edges attached to them.

        Args:
            node_indices (MultipleNodeSelection): The nodes to remove.

        Returns:
            GraphRecord: A GraphRecord without those nodes.
        """
        return self._from_py_graphrecord(
            self._py_graphrecord.remove_nodes(self._unwrap_selection(node_indices))
        )

    def remove_edges(self, edge_indices: MultipleEdgeSelection) -> GraphRecord:
        """Removes the selected edges.

        Args:
            edge_indices (MultipleEdgeSelection): The edges to remove.

        Returns:
            GraphRecord: A GraphRecord without those edges.
        """
        return self._from_py_graphrecord(
            self._py_graphrecord.remove_edges(self._unwrap_edge_selection(edge_indices))
        )

    def keep_nodes(self, node_indices: MultipleNodeSelection) -> GraphRecord:
        """Keeps only the selected nodes and the edges between them.

        Args:
            node_indices (MultipleNodeSelection): The nodes to keep.

        Returns:
            GraphRecord: A GraphRecord holding only those nodes.
        """
        return self._from_py_graphrecord(
            self._py_graphrecord.keep_nodes(self._unwrap_selection(node_indices))
        )

    def keep_edges(self, edge_indices: MultipleEdgeSelection) -> GraphRecord:
        """Keeps only the selected edges.

        Args:
            edge_indices (MultipleEdgeSelection): The edges to keep.

        Returns:
            GraphRecord: A GraphRecord holding only those edges.
        """
        return self._from_py_graphrecord(
            self._py_graphrecord.keep_edges(self._unwrap_edge_selection(edge_indices))
        )

    def keep_groups(self, group_indices: MultipleGroupSelection) -> GraphRecord:
        """Keeps only the selected groups and their members.

        Args:
            group_indices (MultipleGroupSelection): The groups to keep.

        Returns:
            GraphRecord: A GraphRecord holding only those groups.
        """
        return self._from_py_graphrecord(
            self._py_graphrecord.keep_groups(self._unwrap_selection(group_indices))
        )

    def intersect(self, other: GraphRecord) -> GraphRecord:
        """Keeps what this GraphRecord and the other one have in common.

        Args:
            other (GraphRecord): The GraphRecord to intersect with.

        Returns:
            GraphRecord: A GraphRecord holding the shared nodes, edges and groups.
        """
        return self._from_py_graphrecord(
            self._py_graphrecord.intersect(other._py_graphrecord)
        )

    def difference(self, other: GraphRecord) -> GraphRecord:
        """Removes everything the other GraphRecord also holds.

        Args:
            other (GraphRecord): The GraphRecord to subtract.

        Returns:
            GraphRecord: A GraphRecord holding what only this one held.
        """
        return self._from_py_graphrecord(
            self._py_graphrecord.difference(other._py_graphrecord)
        )

    def merge(
        self, other: GraphRecord, on_conflict: OnConflict = OnConflict.Raise
    ) -> GraphRecord:
        """Merges the other GraphRecord into this one.

        Args:
            other (GraphRecord): The GraphRecord to merge in.
            on_conflict (OnConflict): How attributes both GraphRecords define are
                resolved. Defaults to OnConflict.Raise.

        Returns:
            GraphRecord: A GraphRecord holding both.
        """
        return self._from_py_graphrecord(
            self._py_graphrecord.merge(
                other._py_graphrecord, on_conflict._into_py_on_conflict()
            )
        )

    def set_node_attributes(
        self, node_indices: MultipleNodeSelection, attributes: Attributes
    ) -> GraphRecord:
        """Sets the given attributes on the selected nodes, keeping the others.

        Args:
            node_indices (MultipleNodeSelection): The nodes to set the attributes on.
            attributes (Attributes): The attributes to set.

        Returns:
            GraphRecord: A GraphRecord with the attributes set.
        """
        return self._from_py_graphrecord(
            self._py_graphrecord.set_node_attributes(
                self._unwrap_selection(node_indices), attributes
            )
        )

    def replace_node_attributes(
        self, node_indices: MultipleNodeSelection, attributes: Attributes
    ) -> GraphRecord:
        """Replaces all attributes of the selected nodes with the given ones.

        Args:
            node_indices (MultipleNodeSelection): The nodes to replace the attributes
                of.
            attributes (Attributes): The attributes the nodes end up with.

        Returns:
            GraphRecord: A GraphRecord with the attributes replaced.
        """
        return self._from_py_graphrecord(
            self._py_graphrecord.replace_node_attributes(
                self._unwrap_selection(node_indices), attributes
            )
        )

    def remove_node_attributes(
        self,
        node_indices: MultipleNodeSelection,
        attribute_names: Iterable[AttributeName],
    ) -> GraphRecord:
        """Removes the named attributes from the selected nodes.

        Args:
            node_indices (MultipleNodeSelection): The nodes to remove the attributes
                from.
            attribute_names (Iterable[AttributeName]): The names of the attributes to
                remove.

        Returns:
            GraphRecord: A GraphRecord without those attributes.
        """
        return self._from_py_graphrecord(
            self._py_graphrecord.remove_node_attributes(
                self._unwrap_selection(node_indices), attribute_names
            )
        )

    def set_edge_attributes(
        self, edge_indices: MultipleEdgeSelection, attributes: Attributes
    ) -> GraphRecord:
        """Sets the given attributes on the selected edges, keeping the others.

        Args:
            edge_indices (MultipleEdgeSelection): The edges to set the attributes on.
            attributes (Attributes): The attributes to set.

        Returns:
            GraphRecord: A GraphRecord with the attributes set.
        """
        return self._from_py_graphrecord(
            self._py_graphrecord.set_edge_attributes(
                self._unwrap_edge_selection(edge_indices), attributes
            )
        )

    def replace_edge_attributes(
        self, edge_indices: MultipleEdgeSelection, attributes: Attributes
    ) -> GraphRecord:
        """Replaces all attributes of the selected edges with the given ones.

        Args:
            edge_indices (MultipleEdgeSelection): The edges to replace the attributes
                of.
            attributes (Attributes): The attributes the edges end up with.

        Returns:
            GraphRecord: A GraphRecord with the attributes replaced.
        """
        return self._from_py_graphrecord(
            self._py_graphrecord.replace_edge_attributes(
                self._unwrap_edge_selection(edge_indices), attributes
            )
        )

    def remove_edge_attributes(
        self,
        edge_indices: MultipleEdgeSelection,
        attribute_names: Iterable[AttributeName],
    ) -> GraphRecord:
        """Removes the named attributes from the selected edges.

        Args:
            edge_indices (MultipleEdgeSelection): The edges to remove the attributes
                from.
            attribute_names (Iterable[AttributeName]): The names of the attributes to
                remove.

        Returns:
            GraphRecord: A GraphRecord without those attributes.
        """
        return self._from_py_graphrecord(
            self._py_graphrecord.remove_edge_attributes(
                self._unwrap_edge_selection(edge_indices), attribute_names
            )
        )

    def add_group(self, group_index: SingleGroupSelection) -> GraphRecord:
        """Adds an empty group.

        Args:
            group_index (SingleGroupSelection): The index of the group to add.

        Returns:
            GraphRecord: A GraphRecord containing the added group.
        """
        return self._from_py_graphrecord(
            self._py_graphrecord.add_group(self._unwrap_single_selection(group_index))
        )

    def remove_groups(self, group_indices: MultipleGroupSelection) -> GraphRecord:
        """Removes the selected groups, keeping their members ungrouped.

        Args:
            group_indices (MultipleGroupSelection): The groups to remove.

        Returns:
            GraphRecord: A GraphRecord without those groups.
        """
        return self._from_py_graphrecord(
            self._py_graphrecord.remove_groups(self._unwrap_selection(group_indices))
        )

    def add_nodes_to_group(
        self, node_indices: MultipleNodeSelection, group_index: SingleGroupSelection
    ) -> GraphRecord:
        """Adds the selected nodes to a group.

        Args:
            node_indices (MultipleNodeSelection): The nodes to add.
            group_index (SingleGroupSelection): The group the nodes join.

        Returns:
            GraphRecord: A GraphRecord with those nodes in the group.
        """
        return self._from_py_graphrecord(
            self._py_graphrecord.add_nodes_to_group(
                self._unwrap_selection(node_indices),
                self._unwrap_single_selection(group_index),
            )
        )

    def remove_nodes_from_group(
        self, node_indices: MultipleNodeSelection, group_index: SingleGroupSelection
    ) -> GraphRecord:
        """Removes the selected nodes from a group, keeping the nodes themselves.

        Args:
            node_indices (MultipleNodeSelection): The nodes to remove.
            group_index (SingleGroupSelection): The group the nodes leave.

        Returns:
            GraphRecord: A GraphRecord without those nodes in the group.
        """
        return self._from_py_graphrecord(
            self._py_graphrecord.remove_nodes_from_group(
                self._unwrap_selection(node_indices),
                self._unwrap_single_selection(group_index),
            )
        )

    def add_edges_to_group(
        self, edge_indices: MultipleEdgeSelection, group_index: SingleGroupSelection
    ) -> GraphRecord:
        """Adds the selected edges to a group.

        Args:
            edge_indices (MultipleEdgeSelection): The edges to add.
            group_index (SingleGroupSelection): The group the edges join.

        Returns:
            GraphRecord: A GraphRecord with those edges in the group.
        """
        return self._from_py_graphrecord(
            self._py_graphrecord.add_edges_to_group(
                self._unwrap_edge_selection(edge_indices),
                self._unwrap_single_selection(group_index),
            )
        )

    def remove_edges_from_group(
        self, edge_indices: MultipleEdgeSelection, group_index: SingleGroupSelection
    ) -> GraphRecord:
        """Removes the selected edges from a group, keeping the edges themselves.

        Args:
            edge_indices (MultipleEdgeSelection): The edges to remove.
            group_index (SingleGroupSelection): The group the edges leave.

        Returns:
            GraphRecord: A GraphRecord without those edges in the group.
        """
        return self._from_py_graphrecord(
            self._py_graphrecord.remove_edges_from_group(
                self._unwrap_edge_selection(edge_indices),
                self._unwrap_single_selection(group_index),
            )
        )

    @property
    def schema(self) -> Schema:
        """The schema the GraphRecord validates against.

        Returns:
            Schema: The schema of the GraphRecord.
        """
        return Schema._from_py_schema(self._py_graphrecord.schema)

    def set_schema(self, schema: Schema) -> GraphRecord:
        """Validates the GraphRecord against the given schema and adopts it.

        Args:
            schema (Schema): The schema to adopt.

        Returns:
            GraphRecord: A GraphRecord carrying the schema.
        """
        return self._from_py_graphrecord(
            self._py_graphrecord.set_schema(schema._py_schema)
        )

    def freeze_schema(self) -> GraphRecord:
        """Stops the schema from growing with the data written to the GraphRecord.

        Returns:
            GraphRecord: A GraphRecord with a frozen schema.
        """
        return self._from_py_graphrecord(self._py_graphrecord.freeze_schema())

    def unfreeze_schema(self) -> GraphRecord:
        """Lets the schema grow with the data written to the GraphRecord again.

        Returns:
            GraphRecord: A GraphRecord with an unfrozen schema.
        """
        return self._from_py_graphrecord(self._py_graphrecord.unfreeze_schema())

    def clear(self) -> GraphRecord:
        """Removes all nodes, edges and groups, keeping the schema and the plugins.

        Returns:
            GraphRecord: An empty GraphRecord.
        """
        return self._from_py_graphrecord(self._py_graphrecord.clear())

    def compact(self) -> GraphRecord:
        """Reclaims the space that removed nodes and edges still occupy.

        Returns:
            GraphRecord: A GraphRecord holding the same contents, compacted.
        """
        return self._from_py_graphrecord(self._py_graphrecord.compact())

    def node_count(self) -> int:
        """Counts the nodes of the GraphRecord.

        Returns:
            int: The number of nodes.
        """
        return self._py_graphrecord.node_count()

    def edge_count(self) -> int:
        """Counts the edges of the GraphRecord.

        Returns:
            int: The number of edges.
        """
        return self._py_graphrecord.edge_count()

    def group_count(self) -> int:
        """Counts the groups of the GraphRecord.

        Returns:
            int: The number of groups.
        """
        return self._py_graphrecord.group_count()

    def contains_node(self, node_index: NodeIndex) -> bool:
        """Checks whether the GraphRecord holds a node with the given index.

        Args:
            node_index (NodeIndex): The index to look for.

        Returns:
            bool: True if the node exists, otherwise False.
        """
        return self._py_graphrecord.contains_node(node_index)

    def contains_edge(self, edge_index: EdgeIndex) -> bool:
        """Checks whether the GraphRecord holds an edge with the given index.

        Args:
            edge_index (EdgeIndex): The index to look for.

        Returns:
            bool: True if the edge exists, otherwise False.
        """
        return self._py_graphrecord.contains_edge(edge_index._py_edge_index)

    def contains_group(self, group_index: GroupIndex) -> bool:
        """Checks whether the GraphRecord holds a group with the given index.

        Args:
            group_index (GroupIndex): The index to look for.

        Returns:
            bool: True if the group exists, otherwise False.
        """
        return self._py_graphrecord.contains_group(group_index)

    def node_indices(self) -> List[NodeIndex]:
        """Lists the indices of all nodes.

        Returns:
            List[NodeIndex]: The index of every node.
        """
        return self._py_graphrecord.node_indices()

    def edge_indices(self) -> List[EdgeIndex]:
        """Lists the indices of all edges.

        Returns:
            List[EdgeIndex]: The index of every edge.
        """
        return [
            EdgeIndex._from_py_edge_index(edge_index)
            for edge_index in self._py_graphrecord.edge_indices()
        ]

    def group_indices(self) -> List[GroupIndex]:
        """Lists the indices of all groups.

        Returns:
            List[GroupIndex]: The index of every group.
        """
        return self._py_graphrecord.group_indices()

    def nodes(self) -> NodesSeries:
        """Starts a query over the nodes of the GraphRecord.

        Returns:
            NodesSeries: A series of all nodes, bound to this GraphRecord.
        """
        return Series._from_py_series(self._py_graphrecord.nodes())

    def edges(self) -> EdgesSeries:
        """Starts a query over the edges of the GraphRecord.

        Returns:
            EdgesSeries: A series of all edges, bound to this GraphRecord.
        """
        return Series._from_py_series(self._py_graphrecord.edges())

    def groups(self) -> GroupsSeries:
        """Starts a query over the groups of the GraphRecord.

        Returns:
            GroupsSeries: A series of all groups, bound to this GraphRecord.
        """
        return Series._from_py_series(self._py_graphrecord.groups())

    def query(
        self, expression: Expression[Unbound, S, C, Unpack[Levels]]
    ) -> Series[S, C, Unpack[Levels]]:
        """Binds an expression to the GraphRecord.

        Args:
            expression (Expression[Unbound, S, C, Unpack[Levels]]): The expression to
                bind.

        Returns:
            Series[S, C, Unpack[Levels]]: The expression, bound to this GraphRecord.
        """
        return cast(
            "Series[S, C, Unpack[Levels]]",
            Series._from_py_series(
                self._py_graphrecord.query(expression._py_expression)
            ),
        )

    def node(self, node_index: NodeIndex) -> NodeView:
        """Views a single node.

        Args:
            node_index (NodeIndex): The index of the node to view.

        Returns:
            NodeView: A view of the node as this GraphRecord holds it.
        """
        return NodeView._from_py_node_view(self._py_graphrecord.node(node_index))

    def edge(self, edge_index: EdgeIndex) -> EdgeView:
        """Views a single edge.

        Args:
            edge_index (EdgeIndex): The index of the edge to view.

        Returns:
            EdgeView: A view of the edge as this GraphRecord holds it.
        """
        return EdgeView._from_py_edge_view(
            self._py_graphrecord.edge(edge_index._py_edge_index)
        )

    def group(self, group_index: GroupIndex) -> GroupView:
        """Views a single group.

        Args:
            group_index (GroupIndex): The index of the group to view.

        Returns:
            GroupView: A view of the group as this GraphRecord holds it.
        """
        return GroupView._from_py_group_view(self._py_graphrecord.group(group_index))

    def export(self, writer: Writer[T]) -> T:
        """Exports the GraphRecord through a writer.

        Args:
            writer (Writer[T]): The writer to export through.

        Returns:
            T: What the writer handed back.
        """
        return self._py_graphrecord.export(_WriterBridge(writer))

    def to_polars(self) -> Export[pl.DataFrame]:
        """Exports the GraphRecord to Polars DataFrames, partitioned by group.

        Returns:
            Export[pl.DataFrame]: The node and edge tables of every group and of
                the ungrouped part.
        """
        return cast("Export[pl.DataFrame]", self._py_graphrecord.to_polars())

    @staticmethod
    def _arrow_tables(tables: Tables[PyRecordBatch]) -> Tables[RecordBatch]:
        """Wraps the tables of one partition of an Arrow export.

        Args:
            tables (Tables[PyRecordBatch]): The node and edge tables to wrap.

        Returns:
            Tables[RecordBatch]: The wrapped tables.
        """
        return {
            "nodes": RecordBatch._from_py_record_batch(tables["nodes"]),
            "edges": RecordBatch._from_py_record_batch(tables["edges"]),
        }

    def to_arrow(self) -> Export[RecordBatch]:
        """Exports the GraphRecord to Arrow record batches, partitioned by group.

        Returns:
            Export[RecordBatch]: The node and edge tables of every group and of
                the ungrouped part.
        """
        export = cast("Export[PyRecordBatch]", self._py_graphrecord.to_arrow())

        return {
            "ungrouped": self._arrow_tables(export["ungrouped"]),
            "groups": {
                group_index: self._arrow_tables(tables)
                for group_index, tables in export["groups"].items()
            },
        }

    @staticmethod
    def _pandas_tables(tables: Tables[pl.DataFrame]) -> Tables[pd.DataFrame]:
        """Converts the tables of one partition to Pandas DataFrames.

        Args:
            tables (Tables[pl.DataFrame]): The node and edge tables to convert.

        Returns:
            Tables[pd.DataFrame]: The same tables as Pandas DataFrames.
        """
        return {
            "nodes": tables["nodes"].to_pandas(),
            "edges": tables["edges"].to_pandas(),
        }

    def to_pandas(self) -> Export[pd.DataFrame]:
        """Exports the GraphRecord to Pandas DataFrames, partitioned by group.

        Returns:
            Export[pd.DataFrame]: The node and edge tables of every group and of
                the ungrouped part.
        """
        export = self.to_polars()

        return {
            "ungrouped": self._pandas_tables(export["ungrouped"]),
            "groups": {
                group_index: self._pandas_tables(tables)
                for group_index, tables in export["groups"].items()
            },
        }

    def to_ron(self, path: Union[str, os.PathLike[str]]) -> None:
        """Writes the GraphRecord to a RON file.

        Args:
            path (Union[str, os.PathLike[str]]): The path of the file to write.
        """
        self._py_graphrecord.to_ron(path)

    def overview(
        self, truncate_details: Optional[int] = DEFAULT_TRUNCATE_DETAILS
    ) -> Overview:
        """Summarizes the contents of the GraphRecord.

        Args:
            truncate_details (Optional[int]): The width detail columns are truncated
                to. No truncation if None. Defaults to DEFAULT_TRUNCATE_DETAILS.

        Returns:
            Overview: The summary of every group and of the ungrouped part.
        """
        return Overview._from_py_overview(
            self._py_graphrecord.overview(truncate_details)
        )

    def group_overview(
        self,
        group_index: GroupIndex,
        truncate_details: Optional[int] = DEFAULT_TRUNCATE_DETAILS,
    ) -> GroupOverview:
        """Summarizes the contents of a single group.

        Args:
            group_index (GroupIndex): The group to summarize.
            truncate_details (Optional[int]): The width detail columns are truncated
                to. No truncation if None. Defaults to DEFAULT_TRUNCATE_DETAILS.

        Returns:
            GroupOverview: The summary of the group.
        """
        return GroupOverview._from_py_group_overview(
            self._py_graphrecord.group_overview(group_index, truncate_details)
        )

    def __eq__(self, other: object) -> bool:
        """Compares the GraphRecord with another one by its contents.

        Args:
            other (object): The object to compare against.

        Returns:
            bool: True if both hold the same nodes, edges and groups, otherwise
                False.
        """
        if not isinstance(other, GraphRecord):
            return NotImplemented

        return self._py_graphrecord == other._py_graphrecord

    def __copy__(self) -> GraphRecord:
        """Copies the GraphRecord.

        Returns:
            GraphRecord: A GraphRecord holding the same contents.
        """
        return self._from_py_graphrecord(self._py_graphrecord.__copy__())

    def __deepcopy__(self, memo: Optional[Dict[int, object]] = None) -> GraphRecord:
        """Deep copies the GraphRecord.

        Args:
            memo (Optional[Dict[int, object]]): The objects copied so far.

        Returns:
            GraphRecord: A GraphRecord holding the same contents.
        """
        return self._from_py_graphrecord(self._py_graphrecord.__deepcopy__(memo))

    def __reduce__(
        self,
    ) -> Tuple[Callable[[PyGraphRecord], GraphRecord], Tuple[PyGraphRecord]]:
        """Reduces the GraphRecord to what pickle needs to restore it.

        Returns:
            Tuple[Callable[[PyGraphRecord], GraphRecord], Tuple[PyGraphRecord]]: The
                callable that restores the GraphRecord and its arguments.
        """
        return self._from_py_graphrecord, (self._py_graphrecord,)

    def __repr__(self) -> str:
        """Returns the string representation of the GraphRecord.

        Returns:
            str: The overview of the GraphRecord.
        """
        return repr(self.overview())
