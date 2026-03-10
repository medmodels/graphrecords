"""Builder class for constructing GraphRecord instances."""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List, Optional, Tuple, Union

import graphrecords as gr
from graphrecords.types import (
    EdgeTuple,
    Group,
    GroupInfo,
    NodeIndex,
    NodeTuple,
    PandasEdgeDataFrameInput,
    PandasNodeDataFrameInput,
    PluginName,
    PolarsEdgeDataFrameInput,
    PolarsNodeDataFrameInput,
)

if TYPE_CHECKING:
    from graphrecords.plugins import Plugin
    from graphrecords.schema import Schema

NodeInputBuilder = Union[
    NodeTuple,
    List[NodeTuple],
    PandasNodeDataFrameInput,
    List[PandasNodeDataFrameInput],
    PolarsNodeDataFrameInput,
    List[PolarsNodeDataFrameInput],
]

EdgeInputBuilder = Union[
    EdgeTuple,
    List[EdgeTuple],
    PandasEdgeDataFrameInput,
    List[PandasEdgeDataFrameInput],
    PolarsEdgeDataFrameInput,
    List[PolarsEdgeDataFrameInput],
]

StoredNode = Tuple[NodeInputBuilder, Optional[Group], bool]
StoredEdge = Tuple[EdgeInputBuilder, Optional[Group], bool]
StoredGroup = Tuple[GroupInfo, bool]


class GraphRecordBuilder:
    """A builder class for constructing GraphRecord instances.

    Allows for adding nodes, edges, and groups incrementally, and optionally
    specifying a schema.
    """

    _nodes: List[StoredNode]
    _edges: List[StoredEdge]
    _groups: Dict[Group, StoredGroup]
    _schema: Optional[Tuple[Schema, bool]]
    _plugins: Dict[PluginName, Plugin]

    def __init__(self) -> None:
        """Initializes a new GraphRecordBuilder instance."""
        self._nodes = []
        self._edges = []
        self._groups = {}
        self._schema = None
        self._plugins = {}

    def add_nodes(
        self,
        nodes: NodeInputBuilder,
        *,
        group: Optional[Group] = None,
        bypass_plugins: bool = False,
    ) -> GraphRecordBuilder:
        """Adds nodes to the builder.

        Args:
            nodes (NodeInput): Nodes to add.
            group (Optional[Group], optional): Group to associate with the nodes.
            bypass_plugins (bool): If True, plugin hooks are not called.
                Defaults to False.

        Returns:
            GraphRecordBuilder: The current instance of the builder.
        """
        self._nodes.append((nodes, group, bypass_plugins))
        return self

    def add_edges(
        self,
        edges: EdgeInputBuilder,
        *,
        group: Optional[Group] = None,
        bypass_plugins: bool = False,
    ) -> GraphRecordBuilder:
        """Adds edges to the builder.

        Args:
            edges (EdgeInput): Edges to add.
            group (Optional[Group], optional): Group to associate with the edges.
            bypass_plugins (bool): If True, plugin hooks are not called.
                Defaults to False.

        Returns:
            GraphRecordBuilder: The current instance of the builder.
        """
        self._edges.append((edges, group, bypass_plugins))
        return self

    def add_group(
        self,
        group: Group,
        *,
        nodes: Optional[List[NodeIndex]] = None,
        bypass_plugins: bool = False,
    ) -> GraphRecordBuilder:
        """Adds a group to the builder with an optional list of nodes.

        Args:
            group (Group): The name of the group to add.
            nodes (List[NodeIndex], optional): Node indices to add to the group.
            bypass_plugins (bool): If True, plugin hooks are not called.
                Defaults to False.

        Returns:
            GraphRecordBuilder: The current instance of the builder.
        """
        if nodes is None:
            nodes = []
        self._groups[group] = ({"nodes": nodes, "edges": []}, bypass_plugins)
        return self

    def with_schema(
        self, schema: Schema, *, bypass_plugins: bool = False
    ) -> GraphRecordBuilder:
        """Specifies a schema for the GraphRecord.

        Args:
            schema (Schema): The schema to apply.
            bypass_plugins (bool): If True, plugin hooks are not called.
                Defaults to False.

        Returns:
            GraphRecordBuilder: The current instance of the builder.
        """
        self._schema = (schema, bypass_plugins)
        return self

    def with_plugins(self, plugins: Dict[PluginName, Plugin]) -> GraphRecordBuilder:
        """Specifies plugins for the GraphRecord.

        Args:
            plugins (Dict[PluginName, Plugin]): A dictionary mapping plugin names
                to plugin instances.

        Returns:
            GraphRecordBuilder: The current instance of the builder.
        """
        self._plugins.update(plugins)
        return self

    def add_plugin(self, name: PluginName, plugin: Plugin) -> GraphRecordBuilder:
        """Adds a plugin to the builder.

        Args:
            name (PluginName): The name of the plugin.
            plugin (Plugin): The plugin instance to add.

        Returns:
            GraphRecordBuilder: The current instance of the builder.
        """
        self._plugins[name] = plugin
        return self

    def _build_group(
        self,
        graphrecord: gr.GraphRecord,
        group_name: Group,
        group_info: GroupInfo,
        *,
        bypass_plugins: bool,
    ) -> None:
        nodes = group_info["nodes"]

        if not graphrecord.contains_group(group_name):
            graphrecord.add_group(group_name, nodes, bypass_plugins=bypass_plugins)
            return

        existing_nodes = graphrecord.nodes_in_group(group_name)
        missing_nodes = [node for node in nodes if node not in existing_nodes]
        if missing_nodes:
            graphrecord.add_nodes_to_group(
                group_name, missing_nodes, bypass_plugins=bypass_plugins
            )

    def build(self) -> gr.GraphRecord:
        """Constructs a GraphRecord instance from the builder's configuration.

        Returns:
            GraphRecord: The constructed GraphRecord instance.
        """
        if self._plugins:
            graphrecord = gr.GraphRecord.with_plugins(self._plugins)
        else:
            graphrecord = gr.GraphRecord()

        for nodes, group, bypass_plugins in self._nodes:
            graphrecord.add_nodes(nodes, group, bypass_plugins=bypass_plugins)

        for edges, group, bypass_plugins in self._edges:
            graphrecord.add_edges(edges, group, bypass_plugins=bypass_plugins)

        for group_name, (group_info, bypass_plugins) in self._groups.items():
            self._build_group(
                graphrecord,
                group_name,
                group_info,
                bypass_plugins=bypass_plugins,
            )

        if self._schema is not None:
            schema, bypass_plugins = self._schema
            graphrecord.set_schema(schema, bypass_plugins=bypass_plugins)

        return graphrecord
