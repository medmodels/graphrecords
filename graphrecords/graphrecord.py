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
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Union,
    overload,
)

import polars as pl

from graphrecords._graphrecords import PyEdgeOperand, PyGraphRecord, PyNodeOperand
from graphrecords.builder import GraphRecordBuilder
from graphrecords.indexers import EdgeIndexer, NodeIndexer
from graphrecords.overview import (
    DEFAULT_TRUNCATE_DETAILS,
    GroupOverview,
    Overview,
)
from graphrecords.plugins import Plugin, _PluginBridge
from graphrecords.querying import (
    EdgeAttributesTreeGroupOperand,
    EdgeAttributesTreeGroupQueryResult,
    EdgeAttributesTreeOperand,
    EdgeAttributesTreeQueryResult,
    EdgeIndexGroupOperand,
    EdgeIndexGroupQueryResult,
    EdgeIndexOperand,
    EdgeIndexQuery,
    EdgeIndexQueryResult,
    EdgeIndicesGroupOperand,
    EdgeIndicesGroupQueryResult,
    EdgeIndicesOperand,
    EdgeIndicesQuery,
    EdgeIndicesQueryResult,
    EdgeMultipleAttributesWithIndexGroupOperand,
    EdgeMultipleAttributesWithIndexGroupQueryResult,
    EdgeMultipleAttributesWithIndexOperand,
    EdgeMultipleAttributesWithIndexQueryResult,
    EdgeMultipleAttributesWithoutIndexOperand,
    EdgeMultipleAttributesWithoutIndexQueryResult,
    EdgeMultipleValuesWithIndexGroupOperand,
    EdgeMultipleValuesWithIndexGroupQueryResult,
    EdgeMultipleValuesWithIndexOperand,
    EdgeMultipleValuesWithIndexQueryResult,
    EdgeMultipleValuesWithoutIndexOperand,
    EdgeMultipleValuesWithoutIndexQueryResult,
    EdgeOperand,
    EdgeQuery,
    EdgeSingleAttributeWithIndexGroupOperand,
    EdgeSingleAttributeWithIndexGroupQueryResult,
    EdgeSingleAttributeWithIndexOperand,
    EdgeSingleAttributeWithIndexQueryResult,
    EdgeSingleAttributeWithoutIndexGroupOperand,
    EdgeSingleAttributeWithoutIndexGroupQueryResult,
    EdgeSingleAttributeWithoutIndexOperand,
    EdgeSingleAttributeWithoutIndexQueryResult,
    EdgeSingleValueWithIndexGroupOperand,
    EdgeSingleValueWithIndexGroupQueryResult,
    EdgeSingleValueWithIndexOperand,
    EdgeSingleValueWithIndexQueryResult,
    EdgeSingleValueWithoutIndexGroupOperand,
    EdgeSingleValueWithoutIndexGroupQueryResult,
    EdgeSingleValueWithoutIndexOperand,
    EdgeSingleValueWithoutIndexQueryResult,
    NodeAttributesTreeGroupOperand,
    NodeAttributesTreeGroupQueryResult,
    NodeAttributesTreeOperand,
    NodeAttributesTreeQueryResult,
    NodeIndexGroupOperand,
    NodeIndexGroupQueryResult,
    NodeIndexOperand,
    NodeIndexQuery,
    NodeIndexQueryResult,
    NodeIndicesGroupOperand,
    NodeIndicesGroupQueryResult,
    NodeIndicesOperand,
    NodeIndicesQuery,
    NodeIndicesQueryResult,
    NodeMultipleAttributesWithIndexGroupOperand,
    NodeMultipleAttributesWithIndexGroupQueryResult,
    NodeMultipleAttributesWithIndexOperand,
    NodeMultipleAttributesWithIndexQueryResult,
    NodeMultipleAttributesWithoutIndexOperand,
    NodeMultipleAttributesWithoutIndexQueryResult,
    NodeMultipleValuesWithIndexGroupOperand,
    NodeMultipleValuesWithIndexGroupQueryResult,
    NodeMultipleValuesWithIndexOperand,
    NodeMultipleValuesWithIndexQueryResult,
    NodeMultipleValuesWithoutIndexOperand,
    NodeMultipleValuesWithoutIndexQueryResult,
    NodeOperand,
    NodeQuery,
    NodeSingleAttributeWithIndexGroupOperand,
    NodeSingleAttributeWithIndexGroupQueryResult,
    NodeSingleAttributeWithIndexOperand,
    NodeSingleAttributeWithIndexQueryResult,
    NodeSingleAttributeWithoutIndexGroupOperand,
    NodeSingleAttributeWithoutIndexGroupQueryResult,
    NodeSingleAttributeWithoutIndexOperand,
    NodeSingleAttributeWithoutIndexQueryResult,
    NodeSingleValueWithIndexGroupOperand,
    NodeSingleValueWithIndexGroupQueryResult,
    NodeSingleValueWithIndexOperand,
    NodeSingleValueWithIndexQueryResult,
    NodeSingleValueWithoutIndexGroupOperand,
    NodeSingleValueWithoutIndexGroupQueryResult,
    NodeSingleValueWithoutIndexOperand,
    NodeSingleValueWithoutIndexQueryResult,
    PyQueryReturnOperand,
    QueryResult,
    QueryReturnOperand,
)
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


def _convert_queryreturnoperand_to_pyqueryreturnoperand(
    operand: QueryReturnOperand,
) -> PyQueryReturnOperand:
    if isinstance(
        operand,
        (
            NodeAttributesTreeOperand,
            NodeAttributesTreeGroupOperand,
            EdgeAttributesTreeOperand,
            EdgeAttributesTreeGroupOperand,
        ),
    ):
        return operand._attributes_tree_operand
    if isinstance(
        operand,
        (
            NodeMultipleAttributesWithIndexOperand,
            NodeMultipleAttributesWithIndexGroupOperand,
            NodeMultipleAttributesWithoutIndexOperand,
            EdgeMultipleAttributesWithIndexOperand,
            EdgeMultipleAttributesWithIndexGroupOperand,
            EdgeMultipleAttributesWithoutIndexOperand,
        ),
    ):
        return operand._multiple_attributes_operand
    if isinstance(
        operand,
        (
            NodeSingleAttributeWithIndexOperand,
            NodeSingleAttributeWithIndexGroupOperand,
            NodeSingleAttributeWithoutIndexOperand,
            NodeSingleAttributeWithoutIndexGroupOperand,
            EdgeSingleAttributeWithIndexOperand,
            EdgeSingleAttributeWithIndexGroupOperand,
            EdgeSingleAttributeWithoutIndexOperand,
            EdgeSingleAttributeWithoutIndexGroupOperand,
        ),
    ):
        return operand._single_attribute_operand
    if isinstance(operand, (EdgeIndicesOperand, EdgeIndicesGroupOperand)):
        return operand._edge_indices_operand
    if isinstance(operand, (EdgeIndexOperand, EdgeIndexGroupOperand)):
        return operand._edge_index_operand
    if isinstance(operand, (NodeIndicesOperand, NodeIndicesGroupOperand)):
        return operand._node_indices_operand
    if isinstance(operand, (NodeIndexOperand, NodeIndexGroupOperand)):
        return operand._node_index_operand
    if isinstance(
        operand,
        (
            NodeMultipleValuesWithIndexOperand,
            NodeMultipleValuesWithIndexGroupOperand,
            NodeMultipleValuesWithoutIndexOperand,
            EdgeMultipleValuesWithIndexOperand,
            EdgeMultipleValuesWithIndexGroupOperand,
            EdgeMultipleValuesWithoutIndexOperand,
        ),
    ):
        return operand._multiple_values_operand
    if isinstance(operand, Sequence):
        return [
            _convert_queryreturnoperand_to_pyqueryreturnoperand(operand)
            for operand in operand
        ]

    return operand._single_value_operand


class EdgesDirected(Enum):
    """Enum for specifying whether edges are considered directed or undirected."""

    DIRECTED = auto()
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
    def with_plugins(cls, plugins: List[Plugin]) -> GraphRecord:  # noqa: D102
        graphrecord = cls.__new__(cls)

        graphrecord._graphrecord = PyGraphRecord.with_plugins(
            [_PluginBridge(plugin) for plugin in plugins]
        )

        return graphrecord

    @classmethod
    def from_tuples(
        cls,
        nodes: Sequence[NodeTuple],
        edges: Optional[Sequence[EdgeTuple]] = None,
    ) -> GraphRecord:
        """Creates a GraphRecord instance from lists of node and edge tuples.

        Nodes and edges are specified as lists of tuples. Each node tuple contains a
        node index and attributes. Each edge tuple includes indices of the source and
        target nodes and edge attributes.

        Args:
            nodes (Sequence[NodeTuple]): Sequence of node tuples.
            edges (Optional[Sequence[EdgeTuple]]): Sequence of edge tuples.

        Returns:
            GraphRecord: A new instance created from the provided tuples.
        """
        graphrecord = cls.__new__(cls)
        graphrecord._graphrecord = PyGraphRecord.from_tuples(nodes, edges)
        return graphrecord

    @classmethod
    def from_pandas(
        cls,
        nodes: Union[PandasNodeDataFrameInput, List[PandasNodeDataFrameInput]],
        edges: Optional[
            Union[PandasEdgeDataFrameInput, List[PandasEdgeDataFrameInput]]
        ] = None,
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

        Returns:
            GraphRecord: A new instance from the provided DataFrames.
        """  # noqa: W505
        if edges is None:
            graphrecord = cls.__new__(cls)
            graphrecord._graphrecord = PyGraphRecord.from_nodes_dataframes(
                [process_nodes_dataframe(nodes_df) for nodes_df in nodes]
                if isinstance(nodes, list)
                else [process_nodes_dataframe(nodes)]
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
        )
        return graphrecord

    @classmethod
    def from_polars(
        cls,
        nodes: Union[PolarsNodeDataFrameInput, List[PolarsNodeDataFrameInput]],
        edges: Optional[
            Union[PolarsEdgeDataFrameInput, List[PolarsEdgeDataFrameInput]]
        ] = None,
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

        Returns:
            GraphRecord: A new instance from the provided Polars DataFrames.
        """  # noqa: W505
        if edges is None:
            graphrecord = cls.__new__(cls)
            graphrecord._graphrecord = PyGraphRecord.from_nodes_dataframes(
                nodes if isinstance(nodes, list) else [nodes]
            )
            return graphrecord

        graphrecord = cls.__new__(cls)
        graphrecord._graphrecord = PyGraphRecord.from_dataframes(
            nodes if isinstance(nodes, list) else [nodes],
            edges if isinstance(edges, list) else [edges],
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

    def get_schema(self) -> Schema:
        """Returns a copy of the GraphRecord's schema.

        Returns:
            Schema: The schema of the GraphRecord.
        """
        return Schema._from_py_schema(self._graphrecord.get_schema())

    def set_schema(self, schema: Schema) -> None:
        """Sets the schema of the GraphRecord instance.

        Args:
            schema (Schema): The new schema to apply.
        """
        self._graphrecord.set_schema(schema._schema)

    def freeze_schema(self) -> None:
        """Freezes the schema. No changes are automatically inferred."""
        self._graphrecord.freeze_schema()

    def unfreeze_schema(self) -> None:
        """Unfreezes the schema. Changes are automatically inferred."""
        self._graphrecord.unfreeze_schema()

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
    def outgoing_edges(
        self, node: Union[NodeIndex, NodeIndexQuery]
    ) -> List[EdgeIndex]: ...

    @overload
    def outgoing_edges(
        self, node: Union[NodeIndexInputList, NodeIndicesQuery]
    ) -> Dict[NodeIndex, List[EdgeIndex]]: ...

    def outgoing_edges(
        self,
        node: Union[NodeIndex, NodeIndexInputList, NodeIndexQuery, NodeIndicesQuery],
    ) -> Union[List[EdgeIndex], Dict[NodeIndex, List[EdgeIndex]]]:
        """Lists the outgoing edges of the specified node(s) in the GraphRecord.

        If a single node index is provided, returns a list of its outgoing edge indices.
        If multiple nodes are specified, returns a dictionary mapping each node index to
        its list of outgoing edge indices.

        Args:
            node (Union[NodeIndex, NodeIndexInputList, NodeIndexQuery, NodeIndicesQuery]):
                One or more node indices or a node query.

        Returns:
            Union[List[EdgeIndex], Dict[NodeIndex, List[EdgeIndex]]]: Outgoing
                edge indices for each specified node.
        """  # noqa: W505
        if isinstance(node, Callable):
            query_result = self.query_nodes(node)

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
    def incoming_edges(
        self, node: Union[NodeIndex, NodeIndexQuery]
    ) -> List[EdgeIndex]: ...

    @overload
    def incoming_edges(
        self, node: Union[NodeIndexInputList, NodeIndicesQuery]
    ) -> Dict[NodeIndex, List[EdgeIndex]]: ...

    def incoming_edges(
        self,
        node: Union[NodeIndex, NodeIndexInputList, NodeIndexQuery, NodeIndicesQuery],
    ) -> Union[List[EdgeIndex], Dict[NodeIndex, List[EdgeIndex]]]:
        """Lists the incoming edges of the specified node(s) in the GraphRecord.

        If a single node index is provided, returns a list of its incoming edge indices.
        If multiple nodes are specified, returns a dictionary mapping each node index to
        its list of incoming edge indices.

        Args:
            node (Union[NodeIndex, NodeIndexInputList, NodeIndexQuery, NodeIndicesQuery]):
                One or more node indices or a node query.

        Returns:
            Union[List[EdgeIndex], Dict[NodeIndex, List[EdgeIndex]]]: Incoming
                edge indices for each specified node.
        """  # noqa: W505
        if isinstance(node, Callable):
            query_result = self.query_nodes(node)

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
        self, edge: Union[EdgeIndex, EdgeIndexQuery]
    ) -> tuple[NodeIndex, NodeIndex]: ...

    @overload
    def edge_endpoints(
        self, edge: Union[EdgeIndexInputList, EdgeIndicesQuery]
    ) -> Dict[EdgeIndex, tuple[NodeIndex, NodeIndex]]: ...

    def edge_endpoints(
        self,
        edge: Union[EdgeIndex, EdgeIndexInputList, EdgeIndexQuery, EdgeIndicesQuery],
    ) -> Union[
        tuple[NodeIndex, NodeIndex], Dict[EdgeIndex, tuple[NodeIndex, NodeIndex]]
    ]:
        """Retrieves the source and target nodes of the specified edge(s).

        If a single edge index is provided, returns a tuple of
        node indices (source, target). If multiple edges are specified, returns
        a dictionary mapping each edge index to its tuple of node indices.

        Args:
            edge (Union[EdgeIndex, EdgeIndexInputList, EdgeIndexQuery, EdgeIndicesQuery]):
                One or more edge indices or an edge query.

        Returns:
            Union[tuple[NodeIndex, NodeIndex],
                Dict[EdgeIndex, tuple[NodeIndex, NodeIndex]]]:
                Tuple of node indices or a dictionary mapping each edge to its
                node indices.

        Raises:
            IndexError: If the query returned no results.
        """  # noqa: W505
        if isinstance(edge, Callable):
            query_result = self.query_edges(edge)

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
        source_node: Union[
            NodeIndex, NodeIndexInputList, NodeIndexQuery, NodeIndicesQuery
        ],
        target_node: Union[
            NodeIndex, NodeIndexInputList, NodeIndexQuery, NodeIndicesQuery
        ],
        directed: EdgesDirected = EdgesDirected.DIRECTED,
    ) -> List[EdgeIndex]:
        """Retrieves the edges connecting the specified source and target nodes.

        If a NodeOperation is provided for either the source or target nodes, it is
        first evaluated to obtain the corresponding node indices. The method then
        returns a list of edge indices that connect the specified source and
        target nodes.

        Args:
            source_node (Union[NodeIndex, NodeIndexInputList, NodeIndexQuery, NodeIndicesQuery]):
                The index or indices of the source node(s), or a node query to
                select source nodes.
            target_node (Union[NodeIndex, NodeIndexInputList, NodeIndexQuery, NodeIndicesQuery]):
                The index or indices of the target node(s), or a node query to
                select target nodes.
            directed (EdgesDirected, optional): Whether to consider edges as directed.
                Defaults to EdgesDirected.DIRECTED.

        Returns:
            List[EdgeIndex]: A list of edge indices connecting the specified source and
                target nodes.
        """  # noqa: W505
        if isinstance(source_node, Callable):
            query_result = self.query_nodes(source_node)

            if query_result is None:
                return []

            source_node = query_result

        if isinstance(target_node, Callable):
            query_result = self.query_nodes(target_node)

            if query_result is None:
                return []

            target_node = query_result

        if directed == EdgesDirected.DIRECTED:
            return self._graphrecord.edges_connecting(
                (source_node if isinstance(source_node, list) else [source_node]),
                (target_node if isinstance(target_node, list) else [target_node]),
            )
        return self._graphrecord.edges_connecting_undirected(
            (source_node if isinstance(source_node, list) else [source_node]),
            (target_node if isinstance(target_node, list) else [target_node]),
        )

    @overload
    def remove_nodes(self, nodes: Union[NodeIndex, NodeIndexQuery]) -> Attributes: ...

    @overload
    def remove_nodes(
        self, nodes: Union[NodeIndexInputList, NodeIndicesQuery]
    ) -> Dict[NodeIndex, Attributes]: ...

    def remove_nodes(
        self,
        nodes: Union[NodeIndex, NodeIndexInputList, NodeIndexQuery, NodeIndicesQuery],
    ) -> Union[Attributes, Dict[NodeIndex, Attributes]]:
        """Removes nodes from the GraphRecord and returns their attributes.

        If a single node index is provided, returns the attributes of the removed node.
        If multiple node indices are specified, returns a dictionary mapping each node
        index to its attributes.

        Args:
            nodes (Union[NodeIndex, NodeIndexInputList, NodeIndexQuery, NodeIndicesQuery]):
                One or more node indices or a node query.

        Returns:
            Union[Attributes, Dict[NodeIndex, Attributes]]: Attributes of the
                removed node(s).
        """  # noqa: W505
        if isinstance(nodes, Callable):
            query_result = self.query_nodes(nodes)

            if isinstance(query_result, list):
                return self._graphrecord.remove_nodes(query_result)
            if query_result is not None:
                return self._graphrecord.remove_nodes([query_result])[query_result]

            return {}

        attributes = self._graphrecord.remove_nodes(
            nodes if isinstance(nodes, list) else [nodes]
        )

        if isinstance(nodes, list):
            return attributes

        return attributes[nodes]

    def add_nodes(
        self,
        nodes: NodeInput,
        group: Optional[Group] = None,
    ) -> None:
        """Adds nodes to the GraphRecord from different data formats.

        Accepts a node tuple (single node added), a list of tuples, DataFrame(s), or
        PolarsNodeDataFrameInput(s) to add nodes. If a DataFrame or list of DataFrames
        is used, the add_nodes_pandas method is called. If PolarsNodeDataFrameInput(s)
        are provided, each tuple must include a DataFrame and the index column. If a
        group is specified, the nodes are added to the group.

        Args:
            nodes (NodeInput): Data representing nodes in various formats.
            group (Optional[Group]): The name of the group to add the nodes to. If not
                specified, the nodes are added to the GraphRecord without a group.
        """
        if is_pandas_node_dataframe_input(nodes) or is_pandas_node_dataframe_input_list(
            nodes
        ):
            return self.add_nodes_pandas(nodes, group)

        if is_polars_node_dataframe_input(nodes) or is_polars_node_dataframe_input_list(
            nodes
        ):
            return self.add_nodes_polars(nodes, group)

        if is_node_tuple(nodes):
            nodes = [nodes]

        if group is None:
            self._graphrecord.add_nodes(nodes)
        else:
            self._graphrecord.add_nodes_with_group(nodes, group)

        return None

    def add_nodes_pandas(
        self,
        nodes: Union[PandasNodeDataFrameInput, List[PandasNodeDataFrameInput]],
        group: Optional[Group] = None,
    ) -> None:
        """Adds nodes to the GraphRecord instance from one or more Pandas DataFrames.

        This method accepts either a single tuple or a list of tuples, where each tuple
        consists of a Pandas DataFrame and an index column string. If a group is
        specified, the nodes are added to the group.

        Args:
            nodes (Union[PandasNodeDataFrameInput, List[PandasNodeDataFrameInput]]):
                A tuple or list of tuples, each with a DataFrame and index column.
            group (Optional[Group]): The name of the group to add the nodes to. If not
                specified, the nodes are added to the GraphRecord without a group.
        """
        self.add_nodes_polars(
            [process_nodes_dataframe(nodes_df) for nodes_df in nodes]
            if isinstance(nodes, list)
            else [process_nodes_dataframe(nodes)],
            group,
        )

    def add_nodes_polars(
        self,
        nodes: Union[PolarsNodeDataFrameInput, List[PolarsNodeDataFrameInput]],
        group: Optional[Group] = None,
    ) -> None:
        """Adds nodes to the GraphRecord instance from one or more Polars DataFrames.

        This method accepts either a single tuple or a list of tuples, where each tuple
        consists of a Polars DataFrame and an index column string. If a group is
        specified, the nodes are added to the group.

        Args:
            nodes (Union[PolarsNodeDataFrameInput, List[PolarsNodeDataFrameInput]]):
                A tuple or list of tuples, each with a DataFrame and index column.
            group (Optional[Group]): The name of the group to add the nodes to. If not
                specified, the nodes are added to the GraphRecord without a group.
        """
        if not isinstance(nodes, list):
            nodes = [nodes]

        if group is None:
            self._graphrecord.add_nodes_dataframes(nodes)
        else:
            self._graphrecord.add_nodes_dataframes_with_group(nodes, group)

    @overload
    def remove_edges(self, edges: Union[EdgeIndex, EdgeIndexQuery]) -> Attributes: ...

    @overload
    def remove_edges(
        self, edges: Union[EdgeIndexInputList, EdgeIndicesQuery]
    ) -> Dict[EdgeIndex, Attributes]: ...

    def remove_edges(
        self,
        edges: Union[EdgeIndex, EdgeIndexInputList, EdgeIndexQuery, EdgeIndicesQuery],
    ) -> Union[Attributes, Dict[EdgeIndex, Attributes]]:
        """Removes edges from the GraphRecord and returns their attributes.

        If a single edge index is provided, returns the attributes of the removed edge.
        If multiple edge indices are specified, returns a dictionary mapping each edge
        index to its attributes.

        Args:
            edges (Union[EdgeIndex, EdgeIndexInputList, EdgeIndexQuery, EdgeIndicesQuery]):
                One or more edge indices or an edge query.

        Returns:
            Union[Attributes, Dict[EdgeIndex, Attributes]]: Attributes of the
                removed edge(s).
        """  # noqa: W505
        if isinstance(edges, Callable):
            query_result = self.query_edges(edges)

            if isinstance(query_result, list):
                return self._graphrecord.remove_edges(query_result)
            if query_result is not None:
                return self._graphrecord.remove_edges([query_result])[query_result]

            return {}

        attributes = self._graphrecord.remove_edges(
            edges if isinstance(edges, list) else [edges]
        )

        if isinstance(edges, list):
            return attributes

        return attributes[edges]

    def add_edges(
        self,
        edges: EdgeInput,
        group: Optional[Group] = None,
    ) -> List[EdgeIndex]:
        """Adds edges to the GraphRecord instance from various data formats.

        Accepts edge tuple, lists of tuples, DataFrame(s), or EdgeDataFrameInput(s) to
        add edges. Each tuple must have indices for source and target nodes and a
        dictionary of attributes. If a DataFrame or list of DataFrames is used, the
        add_edges_dataframe method is invoked. If PolarsEdgeDataFrameInput(s) are
        provided, each tuple must include a DataFrame and index columns for source and
        target nodes. If a group is specified, the edges are added to the group.

        Args:
            edges (EdgeInput): Data representing edges in several formats.
            group (Optional[Group]): The name of the group to add the edges to. If not
                specified, the edges are added to the GraphRecord without a group.

        Returns:
            List[EdgeIndex]: A list of edge indices that were added.
        """
        if is_pandas_edge_dataframe_input(edges) or is_pandas_edge_dataframe_input_list(
            edges
        ):
            return self.add_edges_pandas(edges, group)
        if is_polars_edge_dataframe_input(edges) or is_polars_edge_dataframe_input_list(
            edges
        ):
            return self.add_edges_polars(edges, group)
        if is_edge_tuple(edges):
            edges = [edges]

        if group is None:
            return self._graphrecord.add_edges(edges)

        return self._graphrecord.add_edges_with_group(edges, group)

    def add_edges_pandas(
        self,
        edges: Union[PandasEdgeDataFrameInput, List[PandasEdgeDataFrameInput]],
        group: Optional[Group] = None,
    ) -> List[EdgeIndex]:
        """Adds edges to the GraphRecord from one or more Pandas DataFrames.

        This method accepts either a single PandasEdgeDataFrameInput tuple or a list of
        such tuples, each including a DataFrame and index columns for the source and
        target nodes. If a group is specified, the edges are added to the group.

        Args:
            edges (Union[PandasEdgeDataFrameInput, List[PandasEdgeDataFrameInput]]):
                A tuple or list of tuples, each including a DataFrame and index columns
                for source and target nodes.
            group (Optional[Group]): The name of the group to add the edges to. If not
                specified, the edges are added to the GraphRecord without a group.

        Returns:
            List[EdgeIndex]: A list of the edge indices added.
        """
        return self.add_edges_polars(
            [process_edges_dataframe(edges_df) for edges_df in edges]
            if isinstance(edges, list)
            else [process_edges_dataframe(edges)],
            group,
        )

    def add_edges_polars(
        self,
        edges: Union[PolarsEdgeDataFrameInput, List[PolarsEdgeDataFrameInput]],
        group: Optional[Group] = None,
    ) -> List[EdgeIndex]:
        """Adds edges to the GraphRecord from one or more Polars DataFrames.

        This method accepts either a single PolarsEdgeDataFrameInput tuple or a list of
        such tuples, each including a DataFrame and index columns for the source and
        target nodes. If a group is specified, the edges are added to the group.

        Args:
            edges (Union[PolarsEdgeDataFrameInput, List[PolarsEdgeDataFrameInput]]):
                A tuple or list of tuples, each including a DataFrame and index columns
                for source and target nodes.
            group (Optional[Group]): The name of the group to add the edges to. If not
                specified, the edges are added to the GraphRecord without a group.

        Returns:
            List[EdgeIndex]: A list of the edge indices added.
        """
        if not isinstance(edges, list):
            edges = [edges]

        if group is None:
            return self._graphrecord.add_edges_dataframes(edges)
        return self._graphrecord.add_edges_dataframes_with_group(edges, group)

    def add_group(
        self,
        group: Group,
        nodes: Optional[
            Union[NodeIndex, NodeIndexInputList, NodeIndexQuery, NodeIndicesQuery]
        ] = None,
        edges: Optional[
            Union[EdgeIndex, EdgeIndexInputList, EdgeIndexQuery, EdgeIndicesQuery]
        ] = None,
    ) -> None:
        """Adds a group to the GraphRecord, optionally with node and edge indices.

        If node indices are specified, they are added to the group. If no nodes are
        specified, the group is created without any nodes.

        Args:
            group (Group): The name of the group to add.
            nodes (Optional[Union[NodeIndex, NodeIndexInputList, NodeIndexQuery, NodeIndicesQuery]]):
                One or more node indices or a node query to add
                to the group, optional.
            edges (Optional[Union[EdgeIndex, EdgeIndexInputList, EdgeIndexQuery, EdgeIndicesQuery]]):
                One or more edge indices or an edge query to add
                to the group, optional.
        """  # noqa: W505
        if isinstance(nodes, Callable):
            nodes = self.query_nodes(nodes)

        if isinstance(edges, Callable):
            edges = self.query_edges(edges)

        if nodes is not None and not isinstance(nodes, list):
            nodes = [nodes]

        if edges is not None and not isinstance(edges, list):
            edges = [edges]

        self._graphrecord.add_group(group, nodes, edges)

    def remove_groups(self, groups: Union[Group, GroupInputList]) -> None:
        """Removes one or more groups from the GraphRecord instance.

        Args:
            groups (Union[Group, GroupInputList]): One or more group names to remove.
        """
        if not isinstance(groups, list):
            groups = [groups]

        self._graphrecord.remove_groups(groups)

    def add_nodes_to_group(
        self,
        group: Group,
        nodes: Union[NodeIndex, NodeIndexInputList, NodeIndexQuery, NodeIndicesQuery],
    ) -> None:
        """Adds one or more nodes to a specified group in the GraphRecord.

        Args:
            group (Group): The name of the group to add nodes to.
            nodes (Union[NodeIndex, NodeIndexInputList, NodeIndexQuery, NodeIndicesQuery]):
                One or more node indices or a node query to add to the group.
        """  # noqa: W505
        if isinstance(nodes, Callable):
            query_result = self.query_nodes(nodes)
            if query_result is None:
                return
            nodes = list(
                query_result if isinstance(query_result, list) else [query_result]
            )
        elif not isinstance(nodes, list):
            nodes = [nodes]

        self._graphrecord.add_nodes_to_group(group, nodes)

    def add_edges_to_group(
        self,
        group: Group,
        edges: Union[EdgeIndex, EdgeIndexInputList, EdgeIndexQuery, EdgeIndicesQuery],
    ) -> None:
        """Adds one or more edges to a specified group in the GraphRecord.

        Args:
            group (Group): The name of the group to add edges to.
            edges (Union[EdgeIndex, EdgeIndexInputList, EdgeIndexQuery, EdgeIndicesQuery]):
                One or more edge indices or an edge query to add to the group.
        """  # noqa: W505
        if isinstance(edges, Callable):
            query_result = self.query_edges(edges)
            if query_result is None:
                return
            edges = list(
                query_result if isinstance(query_result, list) else [query_result]
            )
        elif not isinstance(edges, list):
            edges = [edges]

        self._graphrecord.add_edges_to_group(group, edges)

    def remove_nodes_from_group(
        self,
        group: Group,
        nodes: Union[NodeIndex, NodeIndexInputList, NodeIndexQuery, NodeIndicesQuery],
    ) -> None:
        """Removes one or more nodes from a specified group in the GraphRecord.

        Args:
            group (Group): The name of the group from which to remove nodes.
            nodes (Union[NodeIndex, NodeIndexInputList, NodeIndexQuery, NodeIndicesQuery]):
                One or more node indices or a node query to remove from the group.
        """  # noqa: W505
        if isinstance(nodes, Callable):
            query_result = self.query_nodes(nodes)
            if query_result is None:
                return
            nodes = list(
                query_result if isinstance(query_result, list) else [query_result]
            )
        elif not isinstance(nodes, list):
            nodes = [nodes]

        self._graphrecord.remove_nodes_from_group(group, nodes)

    def remove_edges_from_group(
        self,
        group: Group,
        edges: Union[EdgeIndex, EdgeIndexInputList, EdgeIndexQuery, EdgeIndicesQuery],
    ) -> None:
        """Removes one or more edges from a specified group in the GraphRecord.

        Args:
            group (Group): The name of the group from which to remove edges.
            edges (Union[EdgeIndex, EdgeIndexInputList, EdgeIndexQuery, EdgeIndicesQuery]):
                One or more edge indices or an edge query to remove from the group.
        """  # noqa: W505
        if isinstance(edges, Callable):
            query_result = self.query_edges(edges)
            if query_result is None:
                return
            edges = list(
                query_result if isinstance(query_result, list) else [query_result]
            )
        elif not isinstance(edges, list):
            edges = [edges]

        self._graphrecord.remove_edges_from_group(group, edges)

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
    def groups_of_node(self, node: Union[NodeIndex, NodeIndexQuery]) -> List[Group]: ...

    @overload
    def groups_of_node(
        self, node: Union[NodeIndexInputList, NodeIndicesQuery]
    ) -> Dict[NodeIndex, List[Group]]: ...

    def groups_of_node(
        self,
        node: Union[NodeIndex, NodeIndexInputList, NodeIndexQuery, NodeIndicesQuery],
    ) -> Union[List[Group], Dict[NodeIndex, List[Group]]]:
        """Retrieves the groups associated with the specified node(s) in the GraphRecord.

        If a single node index is provided, returns a list of groups for that node.
        If multiple nodes are specified, returns a dictionary mapping each node index to
        its list of groups.

        Args:
            node (Union[NodeIndex, NodeIndexInputList, NodeIndexQuery, NodeIndicesQuery]):
                One or more node indices or a node query.

        Returns:
            Union[List[Group], Dict[NodeIndex, List[Group]]]: Groups associated with
                each node.
        """  # noqa: W505
        if isinstance(node, Callable):
            query_result = self.query_nodes(node)

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
    def groups_of_edge(self, edge: Union[EdgeIndex, EdgeIndexQuery]) -> List[Group]: ...

    @overload
    def groups_of_edge(
        self, edge: Union[EdgeIndexInputList, EdgeIndicesQuery]
    ) -> Dict[EdgeIndex, List[Group]]: ...

    def groups_of_edge(
        self,
        edge: Union[EdgeIndex, EdgeIndexInputList, EdgeIndexQuery, EdgeIndicesQuery],
    ) -> Union[List[Group], Dict[EdgeIndex, List[Group]]]:
        """Retrieves the groups associated with the specified edge(s) in the GraphRecord.

        If a single edge index is provided, returns a list of groups for that edge.
        If multiple edges are specified, returns a dictionary mapping each edge index to
        its list of groups.

        Args:
            edge (Union[EdgeIndex, EdgeIndexInputList, EdgeIndexQuery, EdgeIndicesQuery]):
                One or more edge indices or an edge query.

        Returns:
            Union[List[Group], Dict[EdgeIndex, List[Group]]]: Groups associated with
                each edge.
        """  # noqa: W505
        if isinstance(edge, Callable):
            query_result = self.query_edges(edge)

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
        node: Union[NodeIndex, NodeIndexQuery],
        directed: EdgesDirected = EdgesDirected.DIRECTED,
    ) -> List[NodeIndex]: ...

    @overload
    def neighbors(
        self,
        node: Union[NodeIndexInputList, NodeIndicesQuery],
        directed: EdgesDirected = EdgesDirected.DIRECTED,
    ) -> Dict[NodeIndex, List[NodeIndex]]: ...

    def neighbors(
        self,
        node: Union[NodeIndex, NodeIndexInputList, NodeIndexQuery, NodeIndicesQuery],
        directed: EdgesDirected = EdgesDirected.DIRECTED,
    ) -> Union[List[NodeIndex], Dict[NodeIndex, List[NodeIndex]]]:
        """Retrieves the neighbors of the specified node(s) in the GraphRecord.

        If a single node index is provided, returns a list of its neighboring
        node indices. If multiple nodes are specified, returns a dictionary mapping
        each node index to its list of neighboring nodes.

        Args:
            node (Union[NodeIndex, NodeIndexInputList, NodeIndexQuery, NodeIndicesQuery]):
                One or more node indices or a query that returns node indices.
            directed (EdgesDirected, optional): Whether to consider edges as directed.
                Defaults to EdgesDirected.DIRECTED.

        Returns:
            Union[List[NodeIndex], Dict[NodeIndex, List[NodeIndex]]]: Neighboring nodes.
        """  # noqa: W505
        if isinstance(node, Callable):
            query_result = self.query_nodes(node)

            if query_result is None:
                return []

            node = query_result

        if directed == EdgesDirected.DIRECTED:
            neighbors = self._graphrecord.neighbors(
                node if isinstance(node, list) else [node]
            )
        else:
            neighbors = self._graphrecord.neighbors_undirected(
                node if isinstance(node, list) else [node]
            )

        if isinstance(node, list):
            return neighbors

        return neighbors[node]

    def clear(self) -> None:
        """Clears all data from the GraphRecord instance.

        Removes all nodes, edges, and groups, effectively resetting the instance.
        """
        self._graphrecord.clear()

    @overload
    def query_nodes(
        self, query: Callable[[NodeOperand], NodeAttributesTreeOperand]
    ) -> NodeAttributesTreeQueryResult: ...
    @overload
    def query_nodes(
        self, query: Callable[[NodeOperand], NodeAttributesTreeGroupOperand]
    ) -> NodeAttributesTreeGroupQueryResult: ...
    @overload
    def query_nodes(
        self, query: Callable[[NodeOperand], EdgeAttributesTreeOperand]
    ) -> EdgeAttributesTreeQueryResult: ...
    @overload
    def query_nodes(
        self, query: Callable[[NodeOperand], EdgeAttributesTreeGroupOperand]
    ) -> EdgeAttributesTreeGroupQueryResult: ...
    @overload
    def query_nodes(
        self, query: Callable[[NodeOperand], NodeMultipleAttributesWithIndexOperand]
    ) -> NodeMultipleAttributesWithIndexQueryResult: ...
    @overload
    def query_nodes(
        self,
        query: Callable[[NodeOperand], NodeMultipleAttributesWithIndexGroupOperand],
    ) -> NodeMultipleAttributesWithIndexGroupQueryResult: ...
    @overload
    def query_nodes(
        self, query: Callable[[NodeOperand], NodeMultipleAttributesWithoutIndexOperand]
    ) -> NodeMultipleAttributesWithoutIndexQueryResult: ...
    @overload
    def query_nodes(
        self, query: Callable[[NodeOperand], EdgeMultipleAttributesWithIndexOperand]
    ) -> EdgeMultipleAttributesWithIndexQueryResult: ...
    @overload
    def query_nodes(
        self,
        query: Callable[[NodeOperand], EdgeMultipleAttributesWithIndexGroupOperand],
    ) -> EdgeMultipleAttributesWithIndexGroupQueryResult: ...
    @overload
    def query_nodes(
        self, query: Callable[[NodeOperand], EdgeMultipleAttributesWithoutIndexOperand]
    ) -> EdgeMultipleAttributesWithoutIndexQueryResult: ...
    @overload
    def query_nodes(
        self, query: Callable[[NodeOperand], NodeSingleAttributeWithIndexOperand]
    ) -> NodeSingleAttributeWithIndexQueryResult: ...
    @overload
    def query_nodes(
        self, query: Callable[[NodeOperand], NodeSingleAttributeWithIndexGroupOperand]
    ) -> NodeSingleAttributeWithIndexGroupQueryResult: ...
    @overload
    def query_nodes(
        self, query: Callable[[NodeOperand], NodeSingleAttributeWithoutIndexOperand]
    ) -> NodeSingleAttributeWithoutIndexQueryResult: ...
    @overload
    def query_nodes(
        self,
        query: Callable[[NodeOperand], NodeSingleAttributeWithoutIndexGroupOperand],
    ) -> NodeSingleAttributeWithoutIndexGroupQueryResult: ...
    @overload
    def query_nodes(
        self, query: Callable[[NodeOperand], EdgeSingleAttributeWithIndexOperand]
    ) -> EdgeSingleAttributeWithIndexQueryResult: ...
    @overload
    def query_nodes(
        self, query: Callable[[NodeOperand], EdgeSingleAttributeWithIndexGroupOperand]
    ) -> EdgeSingleAttributeWithIndexGroupQueryResult: ...
    @overload
    def query_nodes(
        self, query: Callable[[NodeOperand], EdgeSingleAttributeWithoutIndexOperand]
    ) -> EdgeSingleAttributeWithoutIndexQueryResult: ...
    @overload
    def query_nodes(
        self,
        query: Callable[[NodeOperand], EdgeSingleAttributeWithoutIndexGroupOperand],
    ) -> EdgeSingleAttributeWithoutIndexGroupQueryResult: ...
    @overload
    def query_nodes(
        self, query: Callable[[NodeOperand], EdgeIndicesOperand]
    ) -> EdgeIndicesQueryResult: ...
    @overload
    def query_nodes(
        self, query: Callable[[NodeOperand], EdgeIndicesGroupOperand]
    ) -> EdgeIndicesGroupQueryResult: ...
    @overload
    def query_nodes(
        self, query: Callable[[NodeOperand], EdgeIndexOperand]
    ) -> EdgeIndexQueryResult: ...
    @overload
    def query_nodes(
        self, query: Callable[[NodeOperand], EdgeIndexGroupOperand]
    ) -> EdgeIndexGroupQueryResult: ...
    @overload
    def query_nodes(
        self, query: Callable[[NodeOperand], NodeIndicesOperand]
    ) -> NodeIndicesQueryResult: ...
    @overload
    def query_nodes(
        self, query: Callable[[NodeOperand], NodeIndicesGroupOperand]
    ) -> NodeIndicesGroupQueryResult: ...
    @overload
    def query_nodes(
        self, query: Callable[[NodeOperand], NodeIndexOperand]
    ) -> NodeIndexQueryResult: ...
    @overload
    def query_nodes(
        self, query: Callable[[NodeOperand], NodeIndexGroupOperand]
    ) -> NodeIndexGroupQueryResult: ...
    @overload
    def query_nodes(
        self, query: Callable[[NodeOperand], NodeMultipleValuesWithIndexOperand]
    ) -> NodeMultipleValuesWithIndexQueryResult: ...
    @overload
    def query_nodes(
        self, query: Callable[[NodeOperand], NodeMultipleValuesWithIndexGroupOperand]
    ) -> NodeMultipleValuesWithIndexGroupQueryResult: ...
    @overload
    def query_nodes(
        self, query: Callable[[NodeOperand], NodeMultipleValuesWithoutIndexOperand]
    ) -> NodeMultipleValuesWithoutIndexQueryResult: ...
    @overload
    def query_nodes(
        self, query: Callable[[NodeOperand], EdgeMultipleValuesWithIndexOperand]
    ) -> EdgeMultipleValuesWithIndexQueryResult: ...
    @overload
    def query_nodes(
        self, query: Callable[[NodeOperand], EdgeMultipleValuesWithIndexGroupOperand]
    ) -> EdgeMultipleValuesWithIndexGroupQueryResult: ...
    @overload
    def query_nodes(
        self, query: Callable[[NodeOperand], EdgeMultipleValuesWithoutIndexOperand]
    ) -> EdgeMultipleValuesWithoutIndexQueryResult: ...
    @overload
    def query_nodes(
        self, query: Callable[[NodeOperand], NodeSingleValueWithIndexOperand]
    ) -> NodeSingleValueWithIndexQueryResult: ...
    @overload
    def query_nodes(
        self, query: Callable[[NodeOperand], NodeSingleValueWithIndexGroupOperand]
    ) -> NodeSingleValueWithIndexGroupQueryResult: ...
    @overload
    def query_nodes(
        self, query: Callable[[NodeOperand], NodeSingleValueWithoutIndexOperand]
    ) -> NodeSingleValueWithoutIndexQueryResult: ...
    @overload
    def query_nodes(
        self, query: Callable[[NodeOperand], NodeSingleValueWithoutIndexGroupOperand]
    ) -> NodeSingleValueWithoutIndexGroupQueryResult: ...
    @overload
    def query_nodes(
        self, query: Callable[[NodeOperand], EdgeSingleValueWithIndexOperand]
    ) -> EdgeSingleValueWithIndexQueryResult: ...
    @overload
    def query_nodes(
        self, query: Callable[[NodeOperand], EdgeSingleValueWithIndexGroupOperand]
    ) -> EdgeSingleValueWithIndexGroupQueryResult: ...
    @overload
    def query_nodes(
        self, query: Callable[[NodeOperand], EdgeSingleValueWithoutIndexOperand]
    ) -> EdgeSingleValueWithoutIndexQueryResult: ...
    @overload
    def query_nodes(
        self, query: Callable[[NodeOperand], EdgeSingleValueWithoutIndexGroupOperand]
    ) -> EdgeSingleValueWithoutIndexGroupQueryResult: ...
    @overload
    def query_nodes(
        self, query: Callable[[NodeOperand], Sequence[QueryReturnOperand]]
    ) -> List[QueryResult]: ...

    def query_nodes(self, query: NodeQuery) -> QueryResult:
        """Retrieves information on the nodes from the GraphRecord given the query.

        Args:
            query (NodeQuery): A query to define the information to be retrieved.
                The query should be a callable that takes a NodeOperand and returns
                a QueryReturnOperand.

        Returns:
            QueryResult: The result of the query, which can be a list of node indices
                or a dictionary of node attributes, among others.
        """

        def _query(node: PyNodeOperand) -> PyQueryReturnOperand:
            result = query(NodeOperand._from_py_node_operand(node))

            return _convert_queryreturnoperand_to_pyqueryreturnoperand(result)

        return self._graphrecord.query_nodes(_query)

    @overload
    def query_edges(
        self, query: Callable[[EdgeOperand], NodeAttributesTreeOperand]
    ) -> NodeAttributesTreeQueryResult: ...
    @overload
    def query_edges(
        self, query: Callable[[EdgeOperand], NodeAttributesTreeGroupOperand]
    ) -> NodeAttributesTreeGroupQueryResult: ...
    @overload
    def query_edges(
        self, query: Callable[[EdgeOperand], EdgeAttributesTreeOperand]
    ) -> EdgeAttributesTreeQueryResult: ...
    @overload
    def query_edges(
        self, query: Callable[[EdgeOperand], EdgeAttributesTreeGroupOperand]
    ) -> EdgeAttributesTreeGroupQueryResult: ...
    @overload
    def query_edges(
        self, query: Callable[[EdgeOperand], NodeMultipleAttributesWithIndexOperand]
    ) -> NodeMultipleAttributesWithIndexQueryResult: ...
    @overload
    def query_edges(
        self,
        query: Callable[[EdgeOperand], NodeMultipleAttributesWithIndexGroupOperand],
    ) -> NodeMultipleAttributesWithIndexGroupQueryResult: ...
    @overload
    def query_edges(
        self, query: Callable[[EdgeOperand], NodeMultipleAttributesWithoutIndexOperand]
    ) -> NodeMultipleAttributesWithoutIndexQueryResult: ...
    @overload
    def query_edges(
        self, query: Callable[[EdgeOperand], EdgeMultipleAttributesWithIndexOperand]
    ) -> EdgeMultipleAttributesWithIndexQueryResult: ...
    @overload
    def query_edges(
        self,
        query: Callable[[EdgeOperand], EdgeMultipleAttributesWithIndexGroupOperand],
    ) -> EdgeMultipleAttributesWithIndexGroupQueryResult: ...
    @overload
    def query_edges(
        self, query: Callable[[EdgeOperand], EdgeMultipleAttributesWithoutIndexOperand]
    ) -> EdgeMultipleAttributesWithoutIndexQueryResult: ...
    @overload
    def query_edges(
        self, query: Callable[[EdgeOperand], NodeSingleAttributeWithIndexOperand]
    ) -> NodeSingleAttributeWithIndexQueryResult: ...
    @overload
    def query_edges(
        self, query: Callable[[EdgeOperand], NodeSingleAttributeWithIndexGroupOperand]
    ) -> NodeSingleAttributeWithIndexGroupQueryResult: ...
    @overload
    def query_edges(
        self, query: Callable[[EdgeOperand], NodeSingleAttributeWithoutIndexOperand]
    ) -> NodeSingleAttributeWithoutIndexQueryResult: ...
    @overload
    def query_edges(
        self,
        query: Callable[[EdgeOperand], NodeSingleAttributeWithoutIndexGroupOperand],
    ) -> NodeSingleAttributeWithoutIndexGroupQueryResult: ...
    @overload
    def query_edges(
        self, query: Callable[[EdgeOperand], EdgeSingleAttributeWithIndexOperand]
    ) -> EdgeSingleAttributeWithIndexQueryResult: ...
    @overload
    def query_edges(
        self, query: Callable[[EdgeOperand], EdgeSingleAttributeWithIndexGroupOperand]
    ) -> EdgeSingleAttributeWithIndexGroupQueryResult: ...
    @overload
    def query_edges(
        self, query: Callable[[EdgeOperand], EdgeSingleAttributeWithoutIndexOperand]
    ) -> EdgeSingleAttributeWithoutIndexQueryResult: ...
    @overload
    def query_edges(
        self,
        query: Callable[[EdgeOperand], EdgeSingleAttributeWithoutIndexGroupOperand],
    ) -> EdgeSingleAttributeWithoutIndexGroupQueryResult: ...
    @overload
    def query_edges(
        self, query: Callable[[EdgeOperand], EdgeIndicesOperand]
    ) -> EdgeIndicesQueryResult: ...
    @overload
    def query_edges(
        self, query: Callable[[EdgeOperand], EdgeIndicesGroupOperand]
    ) -> EdgeIndicesGroupQueryResult: ...
    @overload
    def query_edges(
        self, query: Callable[[EdgeOperand], EdgeIndexOperand]
    ) -> EdgeIndexQueryResult: ...
    @overload
    def query_edges(
        self, query: Callable[[EdgeOperand], EdgeIndexGroupOperand]
    ) -> EdgeIndexGroupQueryResult: ...
    @overload
    def query_edges(
        self, query: Callable[[EdgeOperand], NodeIndicesOperand]
    ) -> NodeIndicesQueryResult: ...
    @overload
    def query_edges(
        self, query: Callable[[EdgeOperand], NodeIndicesGroupOperand]
    ) -> NodeIndicesGroupQueryResult: ...
    @overload
    def query_edges(
        self, query: Callable[[EdgeOperand], NodeIndexOperand]
    ) -> NodeIndexQueryResult: ...
    @overload
    def query_edges(
        self, query: Callable[[EdgeOperand], NodeIndexGroupOperand]
    ) -> NodeIndexGroupQueryResult: ...
    @overload
    def query_edges(
        self, query: Callable[[EdgeOperand], NodeMultipleValuesWithIndexOperand]
    ) -> NodeMultipleValuesWithIndexQueryResult: ...
    @overload
    def query_edges(
        self, query: Callable[[EdgeOperand], NodeMultipleValuesWithIndexGroupOperand]
    ) -> NodeMultipleValuesWithIndexGroupQueryResult: ...
    @overload
    def query_edges(
        self, query: Callable[[EdgeOperand], NodeMultipleValuesWithoutIndexOperand]
    ) -> NodeMultipleValuesWithoutIndexQueryResult: ...
    @overload
    def query_edges(
        self, query: Callable[[EdgeOperand], EdgeMultipleValuesWithIndexOperand]
    ) -> EdgeMultipleValuesWithIndexQueryResult: ...
    @overload
    def query_edges(
        self, query: Callable[[EdgeOperand], EdgeMultipleValuesWithIndexGroupOperand]
    ) -> EdgeMultipleValuesWithIndexGroupQueryResult: ...
    @overload
    def query_edges(
        self, query: Callable[[EdgeOperand], EdgeMultipleValuesWithoutIndexOperand]
    ) -> EdgeMultipleValuesWithoutIndexQueryResult: ...
    @overload
    def query_edges(
        self, query: Callable[[EdgeOperand], NodeSingleValueWithIndexOperand]
    ) -> NodeSingleValueWithIndexQueryResult: ...
    @overload
    def query_edges(
        self, query: Callable[[EdgeOperand], NodeSingleValueWithIndexGroupOperand]
    ) -> NodeSingleValueWithIndexGroupQueryResult: ...
    @overload
    def query_edges(
        self, query: Callable[[EdgeOperand], NodeSingleValueWithoutIndexOperand]
    ) -> NodeSingleValueWithoutIndexQueryResult: ...
    @overload
    def query_edges(
        self, query: Callable[[EdgeOperand], NodeSingleValueWithoutIndexGroupOperand]
    ) -> NodeSingleValueWithoutIndexGroupQueryResult: ...
    @overload
    def query_edges(
        self, query: Callable[[EdgeOperand], EdgeSingleValueWithIndexOperand]
    ) -> EdgeSingleValueWithIndexQueryResult: ...
    @overload
    def query_edges(
        self, query: Callable[[EdgeOperand], EdgeSingleValueWithIndexGroupOperand]
    ) -> EdgeSingleValueWithIndexGroupQueryResult: ...
    @overload
    def query_edges(
        self, query: Callable[[EdgeOperand], EdgeSingleValueWithoutIndexOperand]
    ) -> EdgeSingleValueWithoutIndexQueryResult: ...
    @overload
    def query_edges(
        self, query: Callable[[EdgeOperand], EdgeSingleValueWithoutIndexGroupOperand]
    ) -> EdgeSingleValueWithoutIndexGroupQueryResult: ...
    @overload
    def query_edges(
        self, query: Callable[[EdgeOperand], Sequence[QueryReturnOperand]]
    ) -> List[QueryResult]: ...

    def query_edges(self, query: EdgeQuery) -> QueryResult:
        """Retrieves information on the edges from the GraphRecord given the query.

        Args:
            query (EdgeQuery): A query to define the information to be retrieved.
                The query should be a callable that takes an EdgeOperand and returns
                a QueryReturnOperand.

        Returns:
            QueryResult: The result of the query, which can be a list of edge indices or
                a dictionary of edge attributes, among others.
        """

        def _query(edge: PyEdgeOperand) -> PyQueryReturnOperand:
            result = query(EdgeOperand._from_py_edge_operand(edge))

            return _convert_queryreturnoperand_to_pyqueryreturnoperand(result)

        return self._graphrecord.query_edges(_query)

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
