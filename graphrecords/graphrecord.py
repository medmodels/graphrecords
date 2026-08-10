"""GraphRecord class for managing medical records using nodes and edges.

The `GraphRecord` class is the core component of the `graphrecords` package, providing
methods to create, manage, and query medical records represented through node and
edge data structures. It allows for the dynamic addition and removal of nodes and
edges, with the capability to attach, remove, and query attributes on both.

The class supports instantiation from various data formats, enhancing flexibility and
interoperability. Additionally, it offers mechanisms to group nodes and edges for
simplified management and efficient querying.
"""

from __future__ import annotations

from enum import Enum, auto
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    TypeVar,
    Union,
    overload,
)

import polars as pl

from graphrecords._graphrecords.graphrecord import PyGraphRecord
from graphrecords.builder import GraphRecordBuilder
from graphrecords.indexers import EdgeIndexer, NodeIndexer
from graphrecords.overview import (
    DEFAULT_TRUNCATE_DETAILS,
    GroupOverview,
    Overview,
)
from graphrecords.plugins import Plugin, _PluginBridge
from graphrecords.querying import QueryError
from graphrecords.querying import query_edges as _query_edges
from graphrecords.querying import query_nodes as _query_nodes
from graphrecords.schema import Schema
from graphrecords.types import (
    Attributes,
    EdgeIndex,
    EdgeIndexInputList,
    EdgeInput,
    EdgeTuple,
    Group,
    GroupInfo,
    GroupInputList,
    NodeIndex,
    NodeIndexInputList,
    NodeInput,
    NodeTuple,
    PandasDataFramesExport,
    PandasDataFramesGroupExport,
    PandasEdgeDataFrameInput,
    PandasNodeDataFrameInput,
    PluginName,
    PolarsDataFramesExport,
    PolarsDataFramesGroupExport,
    PolarsEdgeDataFrameInput,
    PolarsNodeDataFrameInput,
    is_edge_tuple,
    is_node_tuple,
    is_pandas_edge_dataframe_input,
    is_pandas_edge_dataframe_input_list,
    is_pandas_node_dataframe_input,
    is_pandas_node_dataframe_input_list,
    is_polars_edge_dataframe_input,
    is_polars_edge_dataframe_input_list,
    is_polars_node_dataframe_input,
    is_polars_node_dataframe_input_list,
)

if TYPE_CHECKING:
    from graphrecords.connectors import ConnectedGraphRecord, Connector
    from graphrecords.querying import (
        EdgeQuery,
        EdgesQuery,
        NodeQuery,
        NodesQuery,
    )

    ConnectorType = TypeVar("ConnectorType", bound=Connector)


def process_nodes_dataframe(
    nodes: PandasNodeDataFrameInput,
) -> PolarsNodeDataFrameInput:
    """Converts a PandasNodeDataFrameInput to a PolarsNodeDataFrameInput.

    Args:
        nodes (PandasNodeDataFrameInput): A tuple of the Pandas DataFrame and index
            index column name.

    Returns:
        PolarsNodeDataFrameInput: A tuple of the Polars DataFrame and index column name.
    """
    nodes_polars = pl.from_pandas(nodes[0])
    return nodes_polars, nodes[1]


def process_edges_dataframe(
    edges: PandasEdgeDataFrameInput,
) -> PolarsEdgeDataFrameInput:
    """Converts a PandasEdgeDataFrameInput to a PolarsEdgeDataFrameInput.

    Args:
        edges (PandasEdgeDataFrameInput): A tuple of the Pandas DataFrame,
            source index, and target index column names.

    Returns:
        PolarsEdgeDataFrameInput: A tuple of the Polars DataFrame, source index, and
            target index column names.
    """
    edges_polars = pl.from_pandas(edges[0])
    return edges_polars, edges[1], edges[2]


class EdgesDirection(Enum):
    """Enum for specifying the direction of edges."""

    OUTGOING = auto()
    INCOMING = auto()
    UNDIRECTED = auto()


class GraphRecord:
    """A class to manage medical records with node and edge data structures.

    Provides methods to create instances from different data formats, manage node and
    edge attributes, and perform operations like adding or removing nodes and edges.
    """

    _graphrecord: PyGraphRecord

    def __init__(self) -> None:
        """Initializes a GraphRecord instance."""
        self._graphrecord = PyGraphRecord()

    @classmethod
    def _from_py_graphrecord(cls, graphrecord: PyGraphRecord) -> GraphRecord:
        """Creates a GraphRecord instance from a PyGraphRecord object.

        Args:
            graphrecord (PyGraphRecord): The underlying PyGraphRecord object.

        Returns:
            GraphRecord: A new GraphRecord instance.
        """
        new_graphrecord = cls.__new__(cls)
        new_graphrecord._graphrecord = graphrecord
        return new_graphrecord

    @staticmethod
    def builder() -> GraphRecordBuilder:
        """Creates a GraphRecordBuilder instance to build a GraphRecord.

        Returns:
            GraphRecordBuilder: A new builder instance.
        """
        return GraphRecordBuilder()

    @classmethod
    def with_schema(cls, schema: Schema) -> GraphRecord:
        """Creates a GraphRecord instance with the specified schema.

        Args:
            schema (Schema): The schema to apply to the GraphRecord.

        Returns:
            GraphRecord: A new instance with the provided schema.
        """
        graphrecord = cls.__new__(cls)
        graphrecord._graphrecord = PyGraphRecord.with_schema(schema._schema)
        return graphrecord

    @classmethod
    def with_plugins(cls, plugins: Dict[PluginName, Plugin]) -> GraphRecord:
        """Creates a GraphRecord instance with the specified plugins.

        Args:
            plugins (Dict[PluginName, Plugin]): A dictionary mapping plugin names to
                plugin instances.

        Returns:
            GraphRecord: A new instance with the provided plugins.
        """
        graphrecord = cls.__new__(cls)

        graphrecord._graphrecord = PyGraphRecord.with_plugins(
            {
                plugin_name: _PluginBridge(plugin)
                for plugin_name, plugin in plugins.items()
            }
        )

        return graphrecord

    @classmethod
    def from_tuples(
        cls,
        nodes: Sequence[NodeTuple],
        edges: Optional[Sequence[EdgeTuple]] = None,
        schema: Optional[Schema] = None,
    ) -> GraphRecord:
        """Creates a GraphRecord instance from lists of node and edge tuples.

        Nodes and edges are specified as lists of tuples. Each node tuple contains a
        node index and attributes. Each edge tuple includes indices of the source and
        target nodes and edge attributes.

        Args:
            nodes (Sequence[NodeTuple]): Sequence of node tuples.
            edges (Optional[Sequence[EdgeTuple]]): Sequence of edge tuples.
            schema (Optional[Schema]): Schema to apply.

        Returns:
            GraphRecord: A new instance created from the provided tuples.
        """
        graphrecord = cls.__new__(cls)
        graphrecord._graphrecord = PyGraphRecord.from_tuples(
            nodes, edges, schema._schema if schema is not None else None
        )
        return graphrecord

    @classmethod
    def from_pandas(
        cls,
        nodes: Union[PandasNodeDataFrameInput, List[PandasNodeDataFrameInput]],
        edges: Optional[
            Union[PandasEdgeDataFrameInput, List[PandasEdgeDataFrameInput]]
        ] = None,
        schema: Optional[Schema] = None,
    ) -> GraphRecord:
        """Creates a GraphRecord from Pandas DataFrames of nodes and optionally edges.

        Accepts a tuple or a list of tuples for nodes and edges. Each node tuple
        consists of a Pandas DataFrame and an index column. Edge tuples include
        a DataFrame and index columns for source and target nodes.

        Args:
            nodes (Union[PolarsNodeDataFrameInput, List[PolarsNodeDataFrameInput]]):
                Node DataFrame(s).
            edges (Optional[Union[PolarsEdgeDataFrameInput, List[PolarsEdgeDataFrameInput]]]):
                Edge DataFrame(s), optional.
            schema (Optional[Schema]): Schema to apply.

        Returns:
            GraphRecord: A new instance from the provided DataFrames.
        """  # noqa: W505
        py_schema = schema._schema if schema is not None else None

        if edges is None:
            graphrecord = cls.__new__(cls)
            graphrecord._graphrecord = PyGraphRecord.from_nodes_dataframes(
                [process_nodes_dataframe(nodes_df) for nodes_df in nodes]
                if isinstance(nodes, list)
                else [process_nodes_dataframe(nodes)],
                py_schema,
            )
            return graphrecord

        graphrecord = cls.__new__(cls)
        graphrecord._graphrecord = PyGraphRecord.from_dataframes(
            (
                [process_nodes_dataframe(nodes_df) for nodes_df in nodes]
                if isinstance(nodes, list)
                else [process_nodes_dataframe(nodes)]
            ),
            (
                [process_edges_dataframe(edges_df) for edges_df in edges]
                if isinstance(edges, list)
                else [process_edges_dataframe(edges)]
            ),
            py_schema,
        )
        return graphrecord

    @classmethod
    def from_polars(
        cls,
        nodes: Union[PolarsNodeDataFrameInput, List[PolarsNodeDataFrameInput]],
        edges: Optional[
            Union[PolarsEdgeDataFrameInput, List[PolarsEdgeDataFrameInput]]
        ] = None,
        schema: Optional[Schema] = None,
    ) -> GraphRecord:
        """Creates a GraphRecord from Polars DataFrames of nodes and optionally edges.

        Accepts a tuple or a list of tuples for nodes and edges. Each node tuple
        consists of a Polars DataFrame and an index column. Edge tuples include
        a DataFrame and index columns for source and target nodes.

        Args:
            nodes (Union[PolarsNodeDataFrameInput, List[PolarsNodeDataFrameInput]]):
                Node data.
            edges (Optional[Union[PolarsEdgeDataFrameInput, List[PolarsEdgeDataFrameInput]]]):
                Edge data, optional.
            schema (Optional[Schema]): Schema to apply.

        Returns:
            GraphRecord: A new instance from the provided Polars DataFrames.
        """  # noqa: W505
        py_schema = schema._schema if schema is not None else None

        if edges is None:
            graphrecord = cls.__new__(cls)
            graphrecord._graphrecord = PyGraphRecord.from_nodes_dataframes(
                nodes if isinstance(nodes, list) else [nodes],
                py_schema,
            )
            return graphrecord

        graphrecord = cls.__new__(cls)
        graphrecord._graphrecord = PyGraphRecord.from_dataframes(
            nodes if isinstance(nodes, list) else [nodes],
            edges if isinstance(edges, list) else [edges],
            py_schema,
        )
        return graphrecord

    @classmethod
    def from_ron(cls, path: str) -> GraphRecord:
        """Creates a GraphRecord instance from a RON file.

        Reads node and edge data from a RON file specified by the path and creates a new
        GraphRecord instance using this data.

        Args:
            path (str): Path to the RON file.

        Returns:
            GraphRecord: A new instance created from the RON file.
        """
        graphrecord = cls.__new__(cls)
        graphrecord._graphrecord = PyGraphRecord.from_ron(path)
        return graphrecord

    @staticmethod
    def with_connector(connector: ConnectorType) -> ConnectedGraphRecord[ConnectorType]:
        """Creates a ConnectedGraphRecord with the specified connector.

        Initializes a new GraphRecord and calls the connector's initialize method.

        Args:
            connector (ConnectorType): The connector to attach.

        Returns:
            ConnectedGraphRecord[ConnectorType]: A new connected instance.
        """
        from graphrecords.connectors import ConnectedGraphRecord

        return ConnectedGraphRecord(connector)

    def to_ron(self, path: str) -> None:
        """Writes the GraphRecord instance to a RON file.

        Serializes the GraphRecord instance to a RON file at the specified path.

        Args:
            path (str): Path where the RON file will be written.
        """
        self._graphrecord.to_ron(path)

    def to_pandas(self) -> PandasDataFramesExport:
        """Exports the GraphRecord instance to Pandas DataFrames.

        Returns:
            PandasDataFramesExport: A dictionary containing 'ungrouped' and
                'groups' DataFrames.
        """
        export = self._graphrecord.to_dataframes()

        def _convert_group_export(
            group_export: PolarsDataFramesGroupExport,
        ) -> PandasDataFramesGroupExport:
            return {
                "nodes": group_export["nodes"].to_pandas(),
                "edges": group_export["edges"].to_pandas(),
            }

        return {
            "ungrouped": _convert_group_export(export["ungrouped"]),
            "groups": {
                group: _convert_group_export(group_export)
                for group, group_export in export["groups"].items()
            },
        }

    def to_polars(self) -> PolarsDataFramesExport:
        """Exports the GraphRecord instance to Polars DataFrames.

        Returns:
            PolarsDataFramesExport: A dictionary containing 'ungrouped' and
                'groups' DataFrames.
        """
        return self._graphrecord.to_dataframes()

    def add_plugin(self, name: PluginName, plugin: Plugin) -> None:
        """Adds a plugin to the GraphRecord instance.

        Args:
            name (PluginName): The name of the plugin.
            plugin (Plugin): The plugin instance to add.
        """
        self._graphrecord.add_plugin(name, _PluginBridge(plugin))

    def remove_plugin(self, name: PluginName) -> None:
        """Removes a plugin from the GraphRecord instance.

        Args:
            name (PluginName): The name of the plugin to remove.
        """
        self._graphrecord.remove_plugin(name)

    @property
    def plugins(self) -> List[PluginName]:
        """Lists the plugins registered in the GraphRecord instance.

        Returns a list of all plugin names currently registered with the GraphRecord
        instance.

        Returns:
            List[PluginName]: A list of plugin names.
        """
        return self._graphrecord.plugins

    def get_schema(self) -> Schema:
        """Returns a copy of the GraphRecord's schema.

        Returns:
            Schema: The schema of the GraphRecord.
        """
        return Schema._from_py_schema(self._graphrecord.get_schema())

    def set_schema(self, schema: Schema, *, bypass_plugins: bool = False) -> None:
        """Sets the schema of the GraphRecord instance.

        Args:
            schema (Schema): The new schema to apply.
            bypass_plugins (bool): If True, plugin hooks are not called.
                Defaults to False.
        """
        self._graphrecord.set_schema(schema._schema, bypass_plugins)

    def freeze_schema(self, *, bypass_plugins: bool = False) -> None:
        """Freezes the schema. No changes are automatically inferred.

        Args:
            bypass_plugins (bool): If True, plugin hooks are not called.
                Defaults to False.
        """
        self._graphrecord.freeze_schema(bypass_plugins)

    def unfreeze_schema(self, *, bypass_plugins: bool = False) -> None:
        """Unfreezes the schema. Changes are automatically inferred.

        Args:
            bypass_plugins (bool): If True, plugin hooks are not called.
                Defaults to False.
        """
        self._graphrecord.unfreeze_schema(bypass_plugins)

    @property
    def nodes(self) -> List[NodeIndex]:
        """Lists the node indices in the GraphRecord instance.

        Returns a list of all node indices currently managed by the
        GraphRecord instance.

        Returns:
            List[NodeIndex]: A list of node indices.
        """
        return self._graphrecord.nodes

    @property
    def node(self) -> NodeIndexer:
        """Provides access to node attributes within the GraphRecord via an indexer.

        Facilitates querying, accessing, manipulating, and setting node attributes using
        various indexing methods. Supports conditions and ranges for more
        complex queries.

        Returns:
            NodeIndexer: An object for manipulating and querying node attributes.
        """
        return NodeIndexer(self)  # pragma: no cover

    @property
    def edges(self) -> List[EdgeIndex]:
        """Lists the edge indices in the GraphRecord instance.

        Returns a list of all edge indices currently managed by the
        GraphRecord instance.

        Returns:
            List[EdgeIndex]: A list of edge indices.
        """
        return self._graphrecord.edges

    @property
    def edge(self) -> EdgeIndexer:
        """Provides access to edge attributes within the GraphRecord via an indexer.

        Facilitates querying, accessing, manipulating, and setting edge attributes using
        various indexing methods. Supports conditions and ranges for more
        complex queries.

        Returns:
            EdgeIndexer: An object for manipulating and querying edge attributes.
        """
        return EdgeIndexer(self)  # pragma: no cover

    @property
    def groups(self) -> List[Group]:
        """Lists the groups in the GraphRecord instance.

        Returns a list of all groups currently defined within the GraphRecord instance.

        Returns:
            List[Group]: A list of groups.
        """
        return self._graphrecord.groups

    @overload
    def group(self, group: Group) -> GroupInfo: ...

    @overload
    def group(self, group: GroupInputList) -> Dict[Group, GroupInfo]: ...

    def group(
        self, group: Union[Group, GroupInputList]
    ) -> Union[GroupInfo, Dict[Group, GroupInfo]]:
        """Returns the node and edge indices associated with the specified group/s.

        If a single group is specified, returns a list of node and edge indices
        for that group.
        If multiple groups are specified, returns a dictionary with each group name
        mapping to its list of node and edge indices indices.

        Args:
            group (Union[Group, List[Group]]): One or more group names.

        Returns:
            Union[GroupInfo, Dict[Group, GroupInfo]]: Node and edge indices for
                the specified group(s).
        """
        if isinstance(group, list):
            nodes_in_group = self._graphrecord.nodes_in_group(group)
            edges_in_group = self._graphrecord.edges_in_group(group)

            return {
                group: {"nodes": nodes_in_group[group], "edges": edges_in_group[group]}
                for group in group
            }

        nodes_in_group = self._graphrecord.nodes_in_group([group])
        edges_in_group = self._graphrecord.edges_in_group([group])

        return {"nodes": nodes_in_group[group], "edges": edges_in_group[group]}

    @overload
    def outgoing_edges(self, node: Union[NodeIndex, NodeQuery]) -> List[EdgeIndex]: ...

    @overload
    def outgoing_edges(
        self, node: Union[NodeIndexInputList, NodesQuery]
    ) -> Dict[NodeIndex, List[EdgeIndex]]: ...

    def outgoing_edges(
        self,
        node: Union[NodeIndex, NodeIndexInputList, NodeQuery, NodesQuery],
    ) -> Union[List[EdgeIndex], Dict[NodeIndex, List[EdgeIndex]]]:
        """Lists the outgoing edges of the specified node(s) in the GraphRecord.

        If a single node index is provided, returns a list of its outgoing edge indices.
        If multiple nodes are specified, returns a dictionary mapping each node index to
        its list of outgoing edge indices.

        Args:
            node (Union[NodeIndex, NodeIndexInputList, NodeQuery, NodesQuery]):
                One or more node indices or a node query.

        Returns:
            Union[List[EdgeIndex], Dict[NodeIndex, List[EdgeIndex]]]: Outgoing
                edge indices for each specified node.
        """
        if isinstance(node, Callable):
            query_result = self._query_node_indices(node)

            if isinstance(query_result, list):
                return self._graphrecord.outgoing_edges(query_result)
            if query_result is not None:
                return self._graphrecord.outgoing_edges([query_result])[query_result]

            return []

        indices = self._graphrecord.outgoing_edges(
            node if isinstance(node, list) else [node]
        )

        if isinstance(node, list):
            return indices

        return indices[node]

    @overload
    def incoming_edges(self, node: Union[NodeIndex, NodeQuery]) -> List[EdgeIndex]: ...

    @overload
    def incoming_edges(
        self, node: Union[NodeIndexInputList, NodesQuery]
    ) -> Dict[NodeIndex, List[EdgeIndex]]: ...

    def incoming_edges(
        self,
        node: Union[NodeIndex, NodeIndexInputList, NodeQuery, NodesQuery],
    ) -> Union[List[EdgeIndex], Dict[NodeIndex, List[EdgeIndex]]]:
        """Lists the incoming edges of the specified node(s) in the GraphRecord.

        If a single node index is provided, returns a list of its incoming edge indices.
        If multiple nodes are specified, returns a dictionary mapping each node index to
        its list of incoming edge indices.

        Args:
            node (Union[NodeIndex, NodeIndexInputList, NodeQuery, NodesQuery]):
                One or more node indices or a node query.

        Returns:
            Union[List[EdgeIndex], Dict[NodeIndex, List[EdgeIndex]]]: Incoming
                edge indices for each specified node.
        """
        if isinstance(node, Callable):
            query_result = self._query_node_indices(node)

            if isinstance(query_result, list):
                return self._graphrecord.incoming_edges(query_result)
            if query_result is not None:
                return self._graphrecord.incoming_edges([query_result])[query_result]

            return []

        indices = self._graphrecord.incoming_edges(
            node if isinstance(node, list) else [node]
        )

        if isinstance(node, list):
            return indices

        return indices[node]

    @overload
    def edge_endpoints(
        self, edge: Union[EdgeIndex, EdgeQuery]
    ) -> tuple[NodeIndex, NodeIndex]: ...

    @overload
    def edge_endpoints(
        self, edge: Union[EdgeIndexInputList, EdgesQuery]
    ) -> Dict[EdgeIndex, tuple[NodeIndex, NodeIndex]]: ...

    def edge_endpoints(
        self,
        edge: Union[EdgeIndex, EdgeIndexInputList, EdgeQuery, EdgesQuery],
    ) -> Union[
        tuple[NodeIndex, NodeIndex], Dict[EdgeIndex, tuple[NodeIndex, NodeIndex]]
    ]:
        """Retrieves the source and target nodes of the specified edge(s).

        If a single edge index is provided, returns a tuple of
        node indices (source, target). If multiple edges are specified, returns
        a dictionary mapping each edge index to its tuple of node indices.

        Args:
            edge (Union[EdgeIndex, EdgeIndexInputList, EdgeQuery, EdgesQuery]):
                One or more edge indices or an edge query.

        Returns:
            Union[tuple[NodeIndex, NodeIndex],
                Dict[EdgeIndex, tuple[NodeIndex, NodeIndex]]]:
                Tuple of node indices or a dictionary mapping each edge to its
                node indices.

        Raises:
            IndexError: If the query returned no results.
        """
        if isinstance(edge, Callable):
            query_result = self._query_edge_indices(edge)

            if isinstance(query_result, list):
                return self._graphrecord.edge_endpoints(query_result)
            if query_result is not None:
                return self._graphrecord.edge_endpoints([query_result])[query_result]

            msg = "The query returned no results"
            raise IndexError(msg)

        endpoints = self._graphrecord.edge_endpoints(
            edge if isinstance(edge, list) else [edge]
        )

        if isinstance(edge, list):
            return endpoints

        return endpoints[edge]

    def edges_connecting(
        self,
        source_node: Union[NodeIndex, NodeIndexInputList, NodeQuery, NodesQuery],
        target_node: Union[NodeIndex, NodeIndexInputList, NodeQuery, NodesQuery],
        directed: EdgesDirection = EdgesDirection.OUTGOING,
    ) -> List[EdgeIndex]:
        """Retrieves the edges connecting the specified source and target nodes.

        If a NodeOperation is provided for either the source or target nodes, it is
        first evaluated to obtain the corresponding node indices. The method then
        returns a list of edge indices that connect the specified source and
        target nodes.

        Args:
            source_node (Union[NodeIndex, NodeIndexInputList, NodeQuery, NodesQuery]):
                The index or indices of the source node(s), or a node query to
                select source nodes.
            target_node (Union[NodeIndex, NodeIndexInputList, NodeQuery, NodesQuery]):
                The index or indices of the target node(s), or a node query to
                select target nodes.
            directed (EdgesDirection, optional): The direction to traverse edges.
                Defaults to EdgesDirection.OUTGOING.

        Returns:
            List[EdgeIndex]: A list of edge indices connecting the specified source and
                target nodes.
        """
        if isinstance(source_node, Callable):
            query_result = self._query_node_indices(source_node)

            if query_result is None:
                return []

            source_node = query_result

        if isinstance(target_node, Callable):
            query_result = self._query_node_indices(target_node)

            if query_result is None:
                return []

            target_node = query_result

        source_node_indices = (
            source_node if isinstance(source_node, list) else [source_node]
        )
        target_node_indices = (
            target_node if isinstance(target_node, list) else [target_node]
        )

        if directed == EdgesDirection.OUTGOING:
            return self._graphrecord.edges_connecting(
                source_node_indices, target_node_indices
            )
        if directed == EdgesDirection.INCOMING:
            return self._graphrecord.edges_connecting(
                target_node_indices, source_node_indices
            )
        return self._graphrecord.edges_connecting_undirected(
            source_node_indices, target_node_indices
        )

    @overload
    def remove_nodes(
        self,
        nodes: Union[NodeIndex, NodeQuery],
        *,
        bypass_plugins: bool = False,
    ) -> Attributes: ...

    @overload
    def remove_nodes(
        self,
        nodes: Union[NodeIndexInputList, NodesQuery],
        *,
        bypass_plugins: bool = False,
    ) -> Dict[NodeIndex, Attributes]: ...

    def remove_nodes(
        self,
        nodes: Union[NodeIndex, NodeIndexInputList, NodeQuery, NodesQuery],
        *,
        bypass_plugins: bool = False,
    ) -> Union[Attributes, Dict[NodeIndex, Attributes]]:
        """Removes nodes from the GraphRecord and returns their attributes.

        If a single node index is provided, returns the attributes of the removed node.
        If multiple node indices are specified, returns a dictionary mapping each node
        index to its attributes.

        Args:
            nodes (Union[NodeIndex, NodeIndexInputList, NodeQuery, NodesQuery]):
                One or more node indices or a node query.
            bypass_plugins (bool): If True, plugin hooks are not called.
                Defaults to False.

        Returns:
            Union[Attributes, Dict[NodeIndex, Attributes]]: Attributes of the
                removed node(s).
        """
        if isinstance(nodes, Callable):
            query_result = self._query_node_indices(nodes)

            if isinstance(query_result, list):
                return self._graphrecord.remove_nodes(query_result, bypass_plugins)
            if query_result is not None:
                return self._graphrecord.remove_nodes([query_result], bypass_plugins)[
                    query_result
                ]

            return {}

        attributes = self._graphrecord.remove_nodes(
            nodes if isinstance(nodes, list) else [nodes], bypass_plugins
        )

        if isinstance(nodes, list):
            return attributes

        return attributes[nodes]

    def add_nodes(
        self,
        nodes: NodeInput,
        group: Optional[Union[Group, GroupInputList]] = None,
        *,
        bypass_plugins: bool = False,
    ) -> None:
        """Adds nodes to the GraphRecord from different data formats.

        Accepts a node tuple (single node added), a list of tuples, DataFrame(s), or
        PolarsNodeDataFrameInput(s) to add nodes. If a DataFrame or list of DataFrames
        is used, the add_nodes_pandas method is called. If PolarsNodeDataFrameInput(s)
        are provided, each tuple must include a DataFrame and the index column. If a
        group or list of groups is specified, the nodes are added to the group(s).

        Args:
            nodes (NodeInput): Data representing nodes in various formats.
            group (Optional[Union[Group, GroupInputList]]): The name of the group or
                list of groups to add the nodes to. If not specified, the nodes are
                added to the GraphRecord without a group.
            bypass_plugins (bool): If True, plugin hooks are not called.
                Defaults to False.
        """
        if is_pandas_node_dataframe_input(nodes) or is_pandas_node_dataframe_input_list(
            nodes
        ):
            return self.add_nodes_pandas(nodes, group, bypass_plugins=bypass_plugins)

        if is_polars_node_dataframe_input(nodes) or is_polars_node_dataframe_input_list(
            nodes
        ):
            return self.add_nodes_polars(nodes, group, bypass_plugins=bypass_plugins)

        if is_node_tuple(nodes):
            nodes = [nodes]

        if group is None:
            self._graphrecord.add_nodes(nodes, bypass_plugins)
        elif isinstance(group, list):
            self._graphrecord.add_nodes_with_groups(nodes, group, bypass_plugins)
        else:
            self._graphrecord.add_nodes_with_group(nodes, group, bypass_plugins)

        return None

    def add_nodes_pandas(
        self,
        nodes: Union[PandasNodeDataFrameInput, List[PandasNodeDataFrameInput]],
        group: Optional[Union[Group, GroupInputList]] = None,
        *,
        bypass_plugins: bool = False,
    ) -> None:
        """Adds nodes to the GraphRecord instance from one or more Pandas DataFrames.

        This method accepts either a single tuple or a list of tuples, where each tuple
        consists of a Pandas DataFrame and an index column string. If a group or list of
        groups is specified, the nodes are added to the group(s).

        Args:
            nodes (Union[PandasNodeDataFrameInput, List[PandasNodeDataFrameInput]]):
                A tuple or list of tuples, each with a DataFrame and index column.
            group (Optional[Union[Group, GroupInputList]]): The name of the group or
                list of groups to add the nodes to. If not specified, the nodes are
                added to the GraphRecord without a group.
            bypass_plugins (bool): If True, plugin hooks are not called.
                Defaults to False.
        """
        self.add_nodes_polars(
            [process_nodes_dataframe(nodes_df) for nodes_df in nodes]
            if isinstance(nodes, list)
            else [process_nodes_dataframe(nodes)],
            group,
            bypass_plugins=bypass_plugins,
        )

    def add_nodes_polars(
        self,
        nodes: Union[PolarsNodeDataFrameInput, List[PolarsNodeDataFrameInput]],
        group: Optional[Union[Group, GroupInputList]] = None,
        *,
        bypass_plugins: bool = False,
    ) -> None:
        """Adds nodes to the GraphRecord instance from one or more Polars DataFrames.

        This method accepts either a single tuple or a list of tuples, where each tuple
        consists of a Polars DataFrame and an index column string. If a group or list of
        groups is specified, the nodes are added to the group(s).

        Args:
            nodes (Union[PolarsNodeDataFrameInput, List[PolarsNodeDataFrameInput]]):
                A tuple or list of tuples, each with a DataFrame and index column.
            group (Optional[Union[Group, GroupInputList]]): The name of the group or
                list of groups to add the nodes to. If not specified, the nodes are
                added to the GraphRecord without a group.
            bypass_plugins (bool): If True, plugin hooks are not called.
                Defaults to False.
        """
        if not isinstance(nodes, list):
            nodes = [nodes]

        if group is None:
            self._graphrecord.add_nodes_dataframes(nodes, bypass_plugins)
        elif isinstance(group, list):
            self._graphrecord.add_nodes_dataframes_with_groups(
                nodes, group, bypass_plugins
            )
        else:
            self._graphrecord.add_nodes_dataframes_with_group(
                nodes, group, bypass_plugins
            )

    @overload
    def remove_edges(
        self,
        edges: Union[EdgeIndex, EdgeQuery],
        *,
        bypass_plugins: bool = False,
    ) -> Attributes: ...

    @overload
    def remove_edges(
        self,
        edges: Union[EdgeIndexInputList, EdgesQuery],
        *,
        bypass_plugins: bool = False,
    ) -> Dict[EdgeIndex, Attributes]: ...

    def remove_edges(
        self,
        edges: Union[EdgeIndex, EdgeIndexInputList, EdgeQuery, EdgesQuery],
        *,
        bypass_plugins: bool = False,
    ) -> Union[Attributes, Dict[EdgeIndex, Attributes]]:
        """Removes edges from the GraphRecord and returns their attributes.

        If a single edge index is provided, returns the attributes of the removed edge.
        If multiple edge indices are specified, returns a dictionary mapping each edge
        index to its attributes.

        Args:
            edges (Union[EdgeIndex, EdgeIndexInputList, EdgeQuery, EdgesQuery]):
                One or more edge indices or an edge query.
            bypass_plugins (bool): If True, plugin hooks are not called.
                Defaults to False.

        Returns:
            Union[Attributes, Dict[EdgeIndex, Attributes]]: Attributes of the
                removed edge(s).
        """
        if isinstance(edges, Callable):
            query_result = self._query_edge_indices(edges)

            if isinstance(query_result, list):
                return self._graphrecord.remove_edges(query_result, bypass_plugins)
            if query_result is not None:
                return self._graphrecord.remove_edges([query_result], bypass_plugins)[
                    query_result
                ]

            return {}

        attributes = self._graphrecord.remove_edges(
            edges if isinstance(edges, list) else [edges], bypass_plugins
        )

        if isinstance(edges, list):
            return attributes

        return attributes[edges]

    def add_edges(
        self,
        edges: EdgeInput,
        group: Optional[Union[Group, GroupInputList]] = None,
        *,
        bypass_plugins: bool = False,
    ) -> List[EdgeIndex]:
        """Adds edges to the GraphRecord instance from various data formats.

        Accepts edge tuple, lists of tuples, DataFrame(s), or EdgeDataFrameInput(s) to
        add edges. Each tuple must have indices for source and target nodes and a
        dictionary of attributes. If a DataFrame or list of DataFrames is used, the
        add_edges_dataframe method is invoked. If PolarsEdgeDataFrameInput(s) are
        provided, each tuple must include a DataFrame and index columns for source and
        target nodes. If a group or list of groups is specified, the edges are added to
        the group(s).

        Args:
            edges (EdgeInput): Data representing edges in several formats.
            group (Optional[Union[Group, GroupInputList]]): The name of the group or
                list of groups to add the edges to. If not specified, the edges are
                added to the GraphRecord without a group.
            bypass_plugins (bool): If True, plugin hooks are not called.
                Defaults to False.

        Returns:
            List[EdgeIndex]: A list of edge indices that were added.
        """
        if is_pandas_edge_dataframe_input(edges) or is_pandas_edge_dataframe_input_list(
            edges
        ):
            return self.add_edges_pandas(edges, group, bypass_plugins=bypass_plugins)
        if is_polars_edge_dataframe_input(edges) or is_polars_edge_dataframe_input_list(
            edges
        ):
            return self.add_edges_polars(edges, group, bypass_plugins=bypass_plugins)
        if is_edge_tuple(edges):
            edges = [edges]

        if group is None:
            return self._graphrecord.add_edges(edges, bypass_plugins)
        if isinstance(group, list):
            return self._graphrecord.add_edges_with_groups(edges, group, bypass_plugins)

        return self._graphrecord.add_edges_with_group(edges, group, bypass_plugins)

    def add_edges_pandas(
        self,
        edges: Union[PandasEdgeDataFrameInput, List[PandasEdgeDataFrameInput]],
        group: Optional[Union[Group, GroupInputList]] = None,
        *,
        bypass_plugins: bool = False,
    ) -> List[EdgeIndex]:
        """Adds edges to the GraphRecord from one or more Pandas DataFrames.

        This method accepts either a single PandasEdgeDataFrameInput tuple or a list of
        such tuples, each including a DataFrame and index columns for the source and
        target nodes. If a group or list of groups is specified, the edges are added to
        the group(s).

        Args:
            edges (Union[PandasEdgeDataFrameInput, List[PandasEdgeDataFrameInput]]):
                A tuple or list of tuples, each including a DataFrame and index columns
                for source and target nodes.
            group (Optional[Union[Group, GroupInputList]]): The name of the group or
                list of groups to add the edges to. If not specified, the edges are
                added to the GraphRecord without a group.
            bypass_plugins (bool): If True, plugin hooks are not called.
                Defaults to False.

        Returns:
            List[EdgeIndex]: A list of the edge indices added.
        """
        return self.add_edges_polars(
            [process_edges_dataframe(edges_df) for edges_df in edges]
            if isinstance(edges, list)
            else [process_edges_dataframe(edges)],
            group,
            bypass_plugins=bypass_plugins,
        )

    def add_edges_polars(
        self,
        edges: Union[PolarsEdgeDataFrameInput, List[PolarsEdgeDataFrameInput]],
        group: Optional[Union[Group, GroupInputList]] = None,
        *,
        bypass_plugins: bool = False,
    ) -> List[EdgeIndex]:
        """Adds edges to the GraphRecord from one or more Polars DataFrames.

        This method accepts either a single PolarsEdgeDataFrameInput tuple or a list of
        such tuples, each including a DataFrame and index columns for the source and
        target nodes. If a group or list of groups is specified, the edges are added to
        the group(s).

        Args:
            edges (Union[PolarsEdgeDataFrameInput, List[PolarsEdgeDataFrameInput]]):
                A tuple or list of tuples, each including a DataFrame and index columns
                for source and target nodes.
            group (Optional[Union[Group, GroupInputList]]): The name of the group or
                list of groups to add the edges to. If not specified, the edges are
                added to the GraphRecord without a group.
            bypass_plugins (bool): If True, plugin hooks are not called.
                Defaults to False.

        Returns:
            List[EdgeIndex]: A list of the edge indices added.
        """
        if not isinstance(edges, list):
            edges = [edges]

        if group is None:
            return self._graphrecord.add_edges_dataframes(edges, bypass_plugins)
        if isinstance(group, list):
            return self._graphrecord.add_edges_dataframes_with_groups(
                edges, group, bypass_plugins
            )
        return self._graphrecord.add_edges_dataframes_with_group(
            edges, group, bypass_plugins
        )

    def add_group(
        self,
        group: Group,
        nodes: Optional[
            Union[NodeIndex, NodeIndexInputList, NodeQuery, NodesQuery]
        ] = None,
        edges: Optional[
            Union[EdgeIndex, EdgeIndexInputList, EdgeQuery, EdgesQuery]
        ] = None,
        *,
        bypass_plugins: bool = False,
    ) -> None:
        """Adds a group to the GraphRecord, optionally with node and edge indices.

        If node indices are specified, they are added to the group. If no nodes are
        specified, the group is created without any nodes.

        Args:
            group (Group): The name of the group to add.
            nodes (Optional[Union[NodeIndex, NodeIndexInputList, NodeQuery, NodesQuery]]):
                One or more node indices or a node query to add
                to the group, optional.
            edges (Optional[Union[EdgeIndex, EdgeIndexInputList, EdgeQuery, EdgesQuery]]):
                One or more edge indices or an edge query to add
                to the group, optional.
            bypass_plugins (bool): If True, plugin hooks are not called.
                Defaults to False.
        """  # noqa: W505
        if isinstance(nodes, Callable):
            nodes = self._query_node_indices(nodes)

        if isinstance(edges, Callable):
            edges = self._query_edge_indices(edges)

        if nodes is not None and not isinstance(nodes, list):
            nodes = [nodes]

        if edges is not None and not isinstance(edges, list):
            edges = [edges]

        self._graphrecord.add_group(group, nodes, edges, bypass_plugins)

    def remove_groups(
        self,
        groups: Union[Group, GroupInputList],
        *,
        bypass_plugins: bool = False,
    ) -> None:
        """Removes one or more groups from the GraphRecord instance.

        Args:
            groups (Union[Group, GroupInputList]): One or more group names to remove.
            bypass_plugins (bool): If True, plugin hooks are not called.
                Defaults to False.
        """
        if not isinstance(groups, list):
            groups = [groups]

        self._graphrecord.remove_groups(groups, bypass_plugins)

    def add_nodes_to_group(
        self,
        group: Union[Group, GroupInputList],
        nodes: Union[NodeIndex, NodeIndexInputList, NodeQuery, NodesQuery],
        *,
        bypass_plugins: bool = False,
    ) -> None:
        """Adds one or more nodes to a specified group or groups in the GraphRecord.

        Args:
            group (Union[Group, GroupInputList]): The name of the group or list of
                groups to add nodes to.
            nodes (Union[NodeIndex, NodeIndexInputList, NodeQuery, NodesQuery]):
                One or more node indices or a node query to add to the group.
            bypass_plugins (bool): If True, plugin hooks are not called.
                Defaults to False.
        """
        if isinstance(nodes, Callable):
            query_result = self._query_node_indices(nodes)
            if query_result is None:
                return
            nodes = list(
                query_result if isinstance(query_result, list) else [query_result]
            )
        elif not isinstance(nodes, list):
            nodes = [nodes]

        if isinstance(group, list):
            self._graphrecord.add_nodes_to_groups(nodes, group, bypass_plugins)
        else:
            self._graphrecord.add_nodes_to_group(group, nodes, bypass_plugins)

    def add_edges_to_group(
        self,
        group: Union[Group, GroupInputList],
        edges: Union[EdgeIndex, EdgeIndexInputList, EdgeQuery, EdgesQuery],
        *,
        bypass_plugins: bool = False,
    ) -> None:
        """Adds one or more edges to a specified group or groups in the GraphRecord.

        Args:
            group (Union[Group, GroupInputList]): The name of the group or list of
                groups to add edges to.
            edges (Union[EdgeIndex, EdgeIndexInputList, EdgeQuery, EdgesQuery]):
                One or more edge indices or an edge query to add to the group.
            bypass_plugins (bool): If True, plugin hooks are not called.
                Defaults to False.
        """
        if isinstance(edges, Callable):
            query_result = self._query_edge_indices(edges)
            if query_result is None:
                return
            edges = list(
                query_result if isinstance(query_result, list) else [query_result]
            )
        elif not isinstance(edges, list):
            edges = [edges]

        if isinstance(group, list):
            self._graphrecord.add_edges_to_groups(edges, group, bypass_plugins)
        else:
            self._graphrecord.add_edges_to_group(group, edges, bypass_plugins)

    def remove_nodes_from_group(
        self,
        group: Union[Group, GroupInputList],
        nodes: Union[NodeIndex, NodeIndexInputList, NodeQuery, NodesQuery],
        *,
        bypass_plugins: bool = False,
    ) -> None:
        """Removes nodes from a specified group or groups in the GraphRecord.

        Args:
            group (Union[Group, GroupInputList]): The name of the group or list of
                groups from which to remove nodes.
            nodes (Union[NodeIndex, NodeIndexInputList, NodeQuery, NodesQuery]):
                One or more node indices or a node query to remove from the group.
            bypass_plugins (bool): If True, plugin hooks are not called.
                Defaults to False.
        """
        if isinstance(nodes, Callable):
            query_result = self._query_node_indices(nodes)
            if query_result is None:
                return
            nodes = list(
                query_result if isinstance(query_result, list) else [query_result]
            )
        elif not isinstance(nodes, list):
            nodes = [nodes]

        if isinstance(group, list):
            self._graphrecord.remove_nodes_from_groups(nodes, group, bypass_plugins)
        else:
            self._graphrecord.remove_nodes_from_group(group, nodes, bypass_plugins)

    def remove_edges_from_group(
        self,
        group: Union[Group, GroupInputList],
        edges: Union[EdgeIndex, EdgeIndexInputList, EdgeQuery, EdgesQuery],
        *,
        bypass_plugins: bool = False,
    ) -> None:
        """Removes edges from a specified group or groups in the GraphRecord.

        Args:
            group (Union[Group, GroupInputList]): The name of the group or list of
                groups from which to remove edges.
            edges (Union[EdgeIndex, EdgeIndexInputList, EdgeQuery, EdgesQuery]):
                One or more edge indices or an edge query to remove from the group.
            bypass_plugins (bool): If True, plugin hooks are not called.
                Defaults to False.
        """
        if isinstance(edges, Callable):
            query_result = self._query_edge_indices(edges)
            if query_result is None:
                return
            edges = list(
                query_result if isinstance(query_result, list) else [query_result]
            )
        elif not isinstance(edges, list):
            edges = [edges]

        if isinstance(group, list):
            self._graphrecord.remove_edges_from_groups(edges, group, bypass_plugins)
        else:
            self._graphrecord.remove_edges_from_group(group, edges, bypass_plugins)

    @overload
    def nodes_in_group(self, group: Group) -> List[NodeIndex]: ...

    @overload
    def nodes_in_group(self, group: GroupInputList) -> Dict[Group, List[NodeIndex]]: ...

    def nodes_in_group(
        self, group: Union[Group, GroupInputList]
    ) -> Union[List[NodeIndex], Dict[Group, List[NodeIndex]]]:
        """Retrieves the node indices associated with the specified group/s.

        If a single group is specified, returns a list of node indices for that group.
        If multiple groups are specified, returns a dictionary mapping each group name
        to its list of node indices.

        Args:
            group (GroupInputList): One or more group names.

        Returns:
            Union[List[NodeIndex], Dict[Group, List[NodeIndex]]]: Node indices
                associated with the specified group(s).
        """
        nodes = self._graphrecord.nodes_in_group(
            group if isinstance(group, list) else [group]
        )

        if isinstance(group, list):
            return nodes

        return nodes[group]

    def ungrouped_nodes(self) -> List[NodeIndex]:
        """Retrieves the node indices that are not associated with any group.

        Returns:
            List[NodeIndex]: Node indices that are ungrouped.
        """
        return self._graphrecord.ungrouped_nodes()

    @overload
    def edges_in_group(self, group: Group) -> List[EdgeIndex]: ...

    @overload
    def edges_in_group(self, group: GroupInputList) -> Dict[Group, List[EdgeIndex]]: ...

    def edges_in_group(
        self, group: Union[Group, GroupInputList]
    ) -> Union[List[EdgeIndex], Dict[Group, List[EdgeIndex]]]:
        """Retrieves the edge indices associated with the specified group(s).

        If a single group is specified, returns a list of edge indices for that group.
        If multiple groups are specified, returns a dictionary mapping each group name
        to its list of edge indices.

        Args:
            group (GroupInputList): One or more group names.

        Returns:
            Union[List[EdgeIndex], Dict[Group, List[EdgeIndex]]]: Edge indices
                associated with the specified group(s).
        """
        edges = self._graphrecord.edges_in_group(
            group if isinstance(group, list) else [group]
        )

        if isinstance(group, list):
            return edges

        return edges[group]

    def ungrouped_edges(self) -> List[EdgeIndex]:
        """Retrieves the edge indices that are not associated with any group.

        Returns:
            List[EdgeIndex]: Edge indices that are ungrouped.
        """
        return self._graphrecord.ungrouped_edges()

    @overload
    def groups_of_node(self, node: Union[NodeIndex, NodeQuery]) -> List[Group]: ...

    @overload
    def groups_of_node(
        self, node: Union[NodeIndexInputList, NodesQuery]
    ) -> Dict[NodeIndex, List[Group]]: ...

    def groups_of_node(
        self,
        node: Union[NodeIndex, NodeIndexInputList, NodeQuery, NodesQuery],
    ) -> Union[List[Group], Dict[NodeIndex, List[Group]]]:
        """Retrieves the groups associated with specified nodes in the GraphRecord.

        If a single node index is provided, returns a list of groups for that node.
        If multiple nodes are specified, returns a dictionary mapping each node index to
        its list of groups.

        Args:
            node (Union[NodeIndex, NodeIndexInputList, NodeQuery, NodesQuery]):
                One or more node indices or a node query.

        Returns:
            Union[List[Group], Dict[NodeIndex, List[Group]]]: Groups associated with
                each node.
        """
        if isinstance(node, Callable):
            query_result = self._query_node_indices(node)

            if isinstance(query_result, list):
                return self._graphrecord.groups_of_node(query_result)
            if query_result is not None:
                return self._graphrecord.groups_of_node([query_result])[query_result]

            return []

        groups = self._graphrecord.groups_of_node(
            node if isinstance(node, list) else [node]
        )

        if isinstance(node, list):
            return groups

        return groups[node]

    @overload
    def groups_of_edge(self, edge: Union[EdgeIndex, EdgeQuery]) -> List[Group]: ...

    @overload
    def groups_of_edge(
        self, edge: Union[EdgeIndexInputList, EdgesQuery]
    ) -> Dict[EdgeIndex, List[Group]]: ...

    def groups_of_edge(
        self,
        edge: Union[EdgeIndex, EdgeIndexInputList, EdgeQuery, EdgesQuery],
    ) -> Union[List[Group], Dict[EdgeIndex, List[Group]]]:
        """Retrieves the groups associated with specified edges in the GraphRecord.

        If a single edge index is provided, returns a list of groups for that edge.
        If multiple edges are specified, returns a dictionary mapping each edge index to
        its list of groups.

        Args:
            edge (Union[EdgeIndex, EdgeIndexInputList, EdgeQuery, EdgesQuery]):
                One or more edge indices or an edge query.

        Returns:
            Union[List[Group], Dict[EdgeIndex, List[Group]]]: Groups associated with
                each edge.
        """
        if isinstance(edge, Callable):
            query_result = self._query_edge_indices(edge)

            if isinstance(query_result, list):
                return self._graphrecord.groups_of_edge(query_result)
            if query_result is not None:
                return self._graphrecord.groups_of_edge([query_result])[query_result]

            return []

        groups = self._graphrecord.groups_of_edge(
            edge if isinstance(edge, list) else [edge]
        )

        if isinstance(edge, list):
            return groups

        return groups[edge]

    def node_count(self) -> int:
        """Returns the total number of nodes currently managed by the GraphRecord.

        Returns:
            int: The total number of nodes.
        """
        return self._graphrecord.node_count()

    def edge_count(self) -> int:
        """Returns the total number of edges currently managed by the GraphRecord.

        Returns:
            int: The total number of edges.
        """
        return self._graphrecord.edge_count()

    def group_count(self) -> int:
        """Returns the total number of groups currently defined within the GraphRecord.

        Returns:
            int: The total number of groups.
        """
        return self._graphrecord.group_count()

    def contains_node(self, node: NodeIndex) -> bool:
        """Checks whether a specific node exists in the GraphRecord.

        Args:
            node (NodeIndex): The index of the node to check.

        Returns:
            bool: True if the node exists, False otherwise.
        """
        return self._graphrecord.contains_node(node)

    def contains_edge(self, edge: EdgeIndex) -> bool:
        """Checks whether a specific edge exists in the GraphRecord.

        Args:
            edge (EdgeIndex): The index of the edge to check.

        Returns:
            bool: True if the edge exists, False otherwise.
        """
        return self._graphrecord.contains_edge(edge)

    def contains_group(self, group: Group) -> bool:
        """Checks whether a specific group exists in the GraphRecord.

        Args:
            group (Group): The name of the group to check.

        Returns:
            bool: True if the group exists, False otherwise.
        """
        return self._graphrecord.contains_group(group)

    @overload
    def neighbors(
        self,
        node: Union[NodeIndex, NodeQuery],
        directed: EdgesDirection = EdgesDirection.OUTGOING,
    ) -> List[NodeIndex]: ...

    @overload
    def neighbors(
        self,
        node: Union[NodeIndexInputList, NodesQuery],
        directed: EdgesDirection = EdgesDirection.OUTGOING,
    ) -> Dict[NodeIndex, List[NodeIndex]]: ...

    def neighbors(
        self,
        node: Union[NodeIndex, NodeIndexInputList, NodeQuery, NodesQuery],
        directed: EdgesDirection = EdgesDirection.OUTGOING,
    ) -> Union[List[NodeIndex], Dict[NodeIndex, List[NodeIndex]]]:
        """Retrieves the neighbors of the specified node(s) in the GraphRecord.

        If a single node index is provided, returns a list of its neighboring
        node indices. If multiple nodes are specified, returns a dictionary mapping
        each node index to its list of neighboring nodes.

        Args:
            node (Union[NodeIndex, NodeIndexInputList, NodeQuery, NodesQuery]):
                One or more node indices or a query that returns node indices.
            directed (EdgesDirection, optional): The direction to traverse edges.
                Defaults to EdgesDirection.OUTGOING.

        Returns:
            Union[List[NodeIndex], Dict[NodeIndex, List[NodeIndex]]]: Neighboring nodes.
        """
        if isinstance(node, Callable):
            query_result = self._query_node_indices(node)

            if query_result is None:
                return []

            node = query_result

        node_indices = node if isinstance(node, list) else [node]

        if directed == EdgesDirection.OUTGOING:
            neighbors = self._graphrecord.outgoing_neighbors(node_indices)
        elif directed == EdgesDirection.INCOMING:
            neighbors = self._graphrecord.incoming_neighbors(node_indices)
        else:
            neighbors = self._graphrecord.neighbors(node_indices)

        if isinstance(node, list):
            return neighbors

        return neighbors[node]

    def clear(self, *, bypass_plugins: bool = False) -> None:
        """Clears all data from the GraphRecord instance.

        Removes all nodes, edges, and groups, effectively resetting the instance.

        Args:
            bypass_plugins (bool): If True, plugin hooks are not called.
                Defaults to False.
        """
        self._graphrecord.clear(bypass_plugins)

    query_nodes = _query_nodes
    query_edges = _query_edges

    @overload
    def _query_node_indices(self, query: NodeQuery) -> Optional[NodeIndex]: ...

    @overload
    def _query_node_indices(self, query: NodesQuery) -> List[NodeIndex]: ...

    def _query_node_indices(self, query: Any) -> object:
        result: Union[
            List[Union[NodeIndex, QueryError]], NodeIndex, QueryError, None
        ] = self.query_nodes(query)

        if isinstance(result, QueryError):
            raise result

        if not isinstance(result, list):
            return result

        for index in result:
            if isinstance(index, QueryError):
                raise index

        return result

    @overload
    def _query_edge_indices(self, query: EdgeQuery) -> Optional[EdgeIndex]: ...

    @overload
    def _query_edge_indices(self, query: EdgesQuery) -> List[EdgeIndex]: ...

    def _query_edge_indices(self, query: Any) -> object:
        result: Union[
            List[Union[EdgeIndex, QueryError]], EdgeIndex, QueryError, None
        ] = self.query_edges(query)

        if isinstance(result, QueryError):
            raise result

        if not isinstance(result, list):
            return result

        for index in result:
            if isinstance(index, QueryError):
                raise index

        return result

    def clone(self) -> GraphRecord:
        """Clones the GraphRecord instance.

        Returns:
            GraphRecord: A clone of the GraphRecord instance.
        """
        graphrecord = GraphRecord.__new__(GraphRecord)
        graphrecord._graphrecord = self._graphrecord.clone()

        return graphrecord

    def overview(
        self, truncate_details: Optional[int] = DEFAULT_TRUNCATE_DETAILS
    ) -> Overview:
        """Generates an overview of the GraphRecord instance.

        Args:
            truncate_details (int, optional): The maximum number of detail characters
                to include in the overview. No truncation if None.
                Defaults to DEFAULT_TRUNCATE_DETAILS.

        Returns:
            Overview: An overview of the GraphRecord instance.
        """
        return Overview._from_py_overview(
            self._graphrecord.overview(truncate_details)
        )  # pragma: no cover

    def group_overview(
        self, group: Group, truncate_details: Optional[int] = DEFAULT_TRUNCATE_DETAILS
    ) -> GroupOverview:
        """Generates an overview of a specific group in the GraphRecord instance.

        Args:
            group (Group): The name of the group to generate an overview for.
            truncate_details (int, optional): The maximum number of detail characters
                to include in the overview. No truncation if None.
                Defaults to DEFAULT_TRUNCATE_DETAILS.

        Returns:
            GroupOverview: An overview of the specified group.
        """
        return GroupOverview._from_py_group_overview(  # pragma: no cover
            self._graphrecord.group_overview(group, truncate_details)
        )

    def __repr__(self) -> str:
        """Returns a string representation of the GraphRecord instance.

        Returns:
            str: A string representation of the GraphRecord instance.
        """
        return self.overview().__repr__()  # pragma: no cover
