from typing import Any, Callable, Dict, List, Optional, Sequence

from graphrecords._graphrecords.overview import PyGroupOverview, PyOverview
from graphrecords._graphrecords.querying import PyEdgeOperand, PyNodeOperand
from graphrecords._graphrecords.schema import PySchema
from graphrecords.querying import PyQueryReturnOperand, QueryResult
from graphrecords.types import (
    AttributeHandle,
    AttributeLookup,
    Attributes,
    AttributesInput,
    EdgeIndex,
    EdgeIndexInputList,
    EdgeTuple,
    GraphRecordAttribute,
    GraphRecordValue,
    Group,
    GroupHandle,
    GroupLookup,
    GroupLookupInputList,
    NodeHandle,
    NodeIndex,
    NodeLookup,
    NodeLookupInputList,
    NodeTuple,
    PluginName,
    PolarsDataFramesExport,
    PolarsEdgeDataFrameInput,
    PolarsNodeDataFrameInput,
    _PyConnector,
    _PyPlugin,
)

class PyGraphRecord:
    nodes: List[NodeIndex]
    edges: List[EdgeIndex]
    groups: List[Group]
    plugins: List[PluginName]

    def __init__(self) -> None: ...
    def _to_bytes(self) -> bytes: ...
    @staticmethod
    def _from_bytes(data: bytes) -> PyGraphRecord: ...
    @staticmethod
    def with_schema(schema: PySchema) -> PyGraphRecord: ...
    @staticmethod
    def with_plugins(plugins: Dict[PluginName, _PyPlugin]) -> PyGraphRecord: ...
    @staticmethod
    def from_tuples(
        nodes: Sequence[NodeTuple],
        edges: Optional[Sequence[EdgeTuple]] = None,
        schema: Optional[PySchema] = None,
    ) -> PyGraphRecord: ...
    @staticmethod
    def from_dataframes(
        nodes_dataframes: List[PolarsNodeDataFrameInput],
        edges_dataframes: List[PolarsEdgeDataFrameInput],
        schema: Optional[PySchema] = None,
    ) -> PyGraphRecord: ...
    @staticmethod
    def from_nodes_dataframes(
        nodes_dataframes: List[PolarsNodeDataFrameInput],
        schema: Optional[PySchema] = None,
    ) -> PyGraphRecord: ...
    @staticmethod
    def from_ron(path: str) -> PyGraphRecord: ...
    @staticmethod
    def with_connector(connector: _PyConnector) -> PyGraphRecord: ...
    def to_ron(self, path: str) -> None: ...
    def to_dataframes(self) -> PolarsDataFramesExport: ...
    def disconnect(self) -> PyGraphRecord: ...
    def ingest(self, data: Any) -> None: ...  # noqa: ANN401
    def export(self) -> Any: ...  # noqa: ANN401
    def add_plugin(self, name: PluginName, plugin: _PyPlugin) -> None: ...
    def remove_plugin(self, name: PluginName) -> None: ...
    def get_schema(self) -> PySchema: ...
    def set_schema(self, schema: PySchema, bypass_plugins: bool = False) -> None: ...
    def freeze_schema(self, bypass_plugins: bool = False) -> None: ...
    def unfreeze_schema(self, bypass_plugins: bool = False) -> None: ...
    def node(self, node_index: NodeLookupInputList) -> Dict[NodeLookup, Attributes]: ...
    def edge(self, edge_index: EdgeIndexInputList) -> Dict[EdgeIndex, Attributes]: ...
    def outgoing_edges(
        self, node_index: NodeLookupInputList
    ) -> Dict[NodeLookup, List[EdgeIndex]]: ...
    def incoming_edges(
        self, node_index: NodeLookupInputList
    ) -> Dict[NodeLookup, List[EdgeIndex]]: ...
    def edge_endpoints(
        self, edge_index: EdgeIndexInputList
    ) -> Dict[EdgeIndex, tuple[NodeIndex, NodeIndex]]: ...
    def edge_endpoint_handles(
        self, edge_index: EdgeIndexInputList
    ) -> Dict[EdgeIndex, tuple[NodeHandle, NodeHandle]]: ...
    def edges_connecting(
        self,
        source_node_indices: NodeLookupInputList,
        target_node_indices: NodeLookupInputList,
    ) -> List[EdgeIndex]: ...
    def edges_connecting_undirected(
        self,
        source_node_indices: NodeLookupInputList,
        target_node_indices: NodeLookupInputList,
    ) -> List[EdgeIndex]: ...
    def remove_nodes(
        self, node_index: NodeLookupInputList, bypass_plugins: bool = False
    ) -> Dict[NodeLookup, Attributes]: ...
    def replace_node_attributes(
        self, node_index: NodeLookupInputList, attributes: AttributesInput
    ) -> None: ...
    def update_node_attribute(
        self,
        node_index: NodeLookupInputList,
        attribute: AttributeLookup,
        value: GraphRecordValue,
    ) -> None: ...
    def remove_node_attribute(
        self, node_index: NodeLookupInputList, attribute: AttributeLookup
    ) -> None: ...
    def add_node(
        self,
        node_index: NodeIndex,
        attributes: AttributesInput,
        bypass_plugins: bool = False,
    ) -> NodeHandle: ...
    def add_node_with_group(
        self,
        node_index: NodeIndex,
        attributes: AttributesInput,
        group: GroupLookup,
        bypass_plugins: bool = False,
    ) -> NodeHandle: ...
    def add_edge(
        self,
        source_node_index: NodeLookup,
        target_node_index: NodeLookup,
        attributes: AttributesInput,
        bypass_plugins: bool = False,
    ) -> EdgeIndex: ...
    def add_edge_with_group(
        self,
        source_node_index: NodeLookup,
        target_node_index: NodeLookup,
        attributes: AttributesInput,
        group: GroupLookup,
        bypass_plugins: bool = False,
    ) -> EdgeIndex: ...
    def add_nodes(
        self, nodes: Sequence[NodeTuple], bypass_plugins: bool = False
    ) -> List[NodeHandle]: ...
    def add_nodes_with_group(
        self,
        nodes: Sequence[NodeTuple],
        group: GroupLookup,
        bypass_plugins: bool = False,
    ) -> List[NodeHandle]: ...
    def add_nodes_with_groups(
        self,
        nodes: Sequence[NodeTuple],
        groups: GroupLookupInputList,
        bypass_plugins: bool = False,
    ) -> List[NodeHandle]: ...
    def add_nodes_dataframes(
        self,
        nodes_dataframe: List[PolarsNodeDataFrameInput],
        bypass_plugins: bool = False,
    ) -> List[NodeHandle]: ...
    def add_nodes_dataframes_with_group(
        self,
        nodes_dataframe: List[PolarsNodeDataFrameInput],
        group: GroupLookup,
        bypass_plugins: bool = False,
    ) -> List[NodeHandle]: ...
    def add_nodes_dataframes_with_groups(
        self,
        nodes_dataframe: List[PolarsNodeDataFrameInput],
        groups: GroupLookupInputList,
        bypass_plugins: bool = False,
    ) -> List[NodeHandle]: ...
    def remove_edges(
        self, edge_index: EdgeIndexInputList, bypass_plugins: bool = False
    ) -> Dict[EdgeIndex, Attributes]: ...
    def replace_edge_attributes(
        self, edge_index: EdgeIndexInputList, attributes: AttributesInput
    ) -> None: ...
    def update_edge_attribute(
        self,
        edge_index: EdgeIndexInputList,
        attribute: AttributeLookup,
        value: GraphRecordValue,
    ) -> None: ...
    def remove_edge_attribute(
        self, edge_index: EdgeIndexInputList, attribute: AttributeLookup
    ) -> None: ...
    def add_edges(
        self, edges: Sequence[EdgeTuple], bypass_plugins: bool = False
    ) -> List[EdgeIndex]: ...
    def add_edges_with_group(
        self,
        edges: Sequence[EdgeTuple],
        group: GroupLookup,
        bypass_plugins: bool = False,
    ) -> List[EdgeIndex]: ...
    def add_edges_with_groups(
        self,
        edges: Sequence[EdgeTuple],
        groups: GroupLookupInputList,
        bypass_plugins: bool = False,
    ) -> List[EdgeIndex]: ...
    def add_edges_dataframes(
        self,
        edges_dataframe: List[PolarsEdgeDataFrameInput],
        bypass_plugins: bool = False,
    ) -> List[EdgeIndex]: ...
    def add_edges_dataframes_with_group(
        self,
        edges_dataframe: List[PolarsEdgeDataFrameInput],
        group: GroupLookup,
        bypass_plugins: bool = False,
    ) -> List[EdgeIndex]: ...
    def add_edges_dataframes_with_groups(
        self,
        edges_dataframe: List[PolarsEdgeDataFrameInput],
        groups: GroupLookupInputList,
        bypass_plugins: bool = False,
    ) -> List[EdgeIndex]: ...
    def add_group(
        self,
        group: Group,
        node_indices_to_add: Optional[NodeLookupInputList],
        edge_indices_to_add: Optional[EdgeIndexInputList],
        bypass_plugins: bool = False,
    ) -> None: ...
    def remove_groups(
        self, group: GroupLookupInputList, bypass_plugins: bool = False
    ) -> None: ...
    def add_nodes_to_group(
        self,
        group: GroupLookup,
        node_index: NodeLookupInputList,
        bypass_plugins: bool = False,
    ) -> None: ...
    def add_node_to_groups(
        self,
        node_index: NodeLookup,
        groups: GroupLookupInputList,
        bypass_plugins: bool = False,
    ) -> None: ...
    def add_nodes_to_groups(
        self,
        node_indices: NodeLookupInputList,
        groups: GroupLookupInputList,
        bypass_plugins: bool = False,
    ) -> None: ...
    def add_edges_to_group(
        self,
        group: GroupLookup,
        edge_index: EdgeIndexInputList,
        bypass_plugins: bool = False,
    ) -> None: ...
    def add_edge_to_groups(
        self,
        edge_index: EdgeIndex,
        groups: GroupLookupInputList,
        bypass_plugins: bool = False,
    ) -> None: ...
    def add_edges_to_groups(
        self,
        edge_indices: EdgeIndexInputList,
        groups: GroupLookupInputList,
        bypass_plugins: bool = False,
    ) -> None: ...
    def remove_nodes_from_group(
        self,
        group: GroupLookup,
        node_index: NodeLookupInputList,
        bypass_plugins: bool = False,
    ) -> None: ...
    def remove_node_from_groups(
        self,
        node_index: NodeLookup,
        groups: GroupLookupInputList,
        bypass_plugins: bool = False,
    ) -> None: ...
    def remove_nodes_from_groups(
        self,
        node_indices: NodeLookupInputList,
        groups: GroupLookupInputList,
        bypass_plugins: bool = False,
    ) -> None: ...
    def remove_edges_from_group(
        self,
        group: GroupLookup,
        edge_index: EdgeIndexInputList,
        bypass_plugins: bool = False,
    ) -> None: ...
    def remove_edge_from_groups(
        self,
        edge_index: EdgeIndex,
        groups: GroupLookupInputList,
        bypass_plugins: bool = False,
    ) -> None: ...
    def remove_edges_from_groups(
        self,
        edge_indices: EdgeIndexInputList,
        groups: GroupLookupInputList,
        bypass_plugins: bool = False,
    ) -> None: ...
    def add_node_with_groups(
        self,
        node_index: NodeIndex,
        attributes: AttributesInput,
        groups: GroupLookupInputList,
        bypass_plugins: bool = False,
    ) -> NodeHandle: ...
    def add_edge_with_groups(
        self,
        source_node_index: NodeLookup,
        target_node_index: NodeLookup,
        attributes: AttributesInput,
        groups: GroupLookupInputList,
        bypass_plugins: bool = False,
    ) -> EdgeIndex: ...
    def nodes_in_group(
        self, group: GroupLookupInputList
    ) -> Dict[GroupLookup, List[NodeIndex]]: ...
    def ungrouped_nodes(self) -> List[NodeIndex]: ...
    def edges_in_group(
        self, group: GroupLookupInputList
    ) -> Dict[GroupLookup, List[EdgeIndex]]: ...
    def ungrouped_edges(self) -> List[EdgeIndex]: ...
    def groups_of_node(
        self, node_index: NodeLookupInputList
    ) -> Dict[NodeLookup, List[Group]]: ...
    def groups_of_edge(
        self, edge_index: EdgeIndexInputList
    ) -> Dict[EdgeIndex, List[Group]]: ...
    def node_handles(self) -> List[NodeHandle]: ...
    def group_handles(self) -> List[GroupHandle]: ...
    def node_handles_in_group(
        self, group: GroupLookupInputList
    ) -> Dict[GroupLookup, List[NodeHandle]]: ...
    def ungrouped_node_handles(self) -> List[NodeHandle]: ...
    def group_handles_of_node(
        self, node_index: NodeLookupInputList
    ) -> Dict[NodeLookup, List[GroupHandle]]: ...
    def group_handles_of_edge(
        self, edge_index: EdgeIndexInputList
    ) -> Dict[EdgeIndex, List[GroupHandle]]: ...
    def node_count(self) -> int: ...
    def edge_count(self) -> int: ...
    def group_count(self) -> int: ...
    def contains_node(self, node_index: NodeLookup) -> bool: ...
    def contains_edge(self, edge_index: EdgeIndex) -> bool: ...
    def contains_group(self, group: GroupLookup) -> bool: ...
    def outgoing_neighbors(
        self, node_indices: NodeLookupInputList
    ) -> Dict[NodeLookup, List[NodeIndex]]: ...
    def incoming_neighbors(
        self, node_indices: NodeLookupInputList
    ) -> Dict[NodeLookup, List[NodeIndex]]: ...
    def neighbors(
        self, node_indices: NodeLookupInputList
    ) -> Dict[NodeLookup, List[NodeIndex]]: ...
    def outgoing_neighbor_handles(
        self, node_indices: NodeLookupInputList
    ) -> Dict[NodeLookup, List[NodeHandle]]: ...
    def incoming_neighbor_handles(
        self, node_indices: NodeLookupInputList
    ) -> Dict[NodeLookup, List[NodeHandle]]: ...
    def neighbor_handles(
        self, node_indices: NodeLookupInputList
    ) -> Dict[NodeLookup, List[NodeHandle]]: ...
    def node_handle(self, node_index: NodeIndex) -> Optional[NodeHandle]: ...
    def group_handle(self, group: Group) -> Optional[GroupHandle]: ...
    def attribute_handle(
        self, name: GraphRecordAttribute
    ) -> Optional[AttributeHandle]: ...
    def resolve_node_handle(self, handle: NodeHandle) -> NodeIndex: ...
    def resolve_group_handle(self, handle: GroupHandle) -> Group: ...
    def resolve_attribute_handle(
        self, handle: AttributeHandle
    ) -> GraphRecordAttribute: ...
    def clear(self, bypass_plugins: bool = False) -> None: ...
    def query_nodes(
        self, query: Callable[[PyNodeOperand], PyQueryReturnOperand]
    ) -> QueryResult: ...
    def query_edges(
        self, query: Callable[[PyEdgeOperand], PyQueryReturnOperand]
    ) -> QueryResult: ...
    def clone(self) -> PyGraphRecord: ...
    def overview(self, truncate_details: Optional[int]) -> PyOverview: ...
    def group_overview(
        self, group: GroupLookup, truncate_details: Optional[int]
    ) -> PyGroupOverview: ...
