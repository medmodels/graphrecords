"""Plugin system for hooking into GraphRecord mutation operations."""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional, Tuple

from graphrecords.types import PyPlugin

if TYPE_CHECKING:
    from graphrecords._graphrecords import (
        PyGraphRecord,
        PyPostAddEdgeContext,
        PyPostAddEdgesContext,
        PyPostAddEdgesDataframesContext,
        PyPostAddEdgesDataframesWithGroupContext,
        PyPostAddEdgesWithGroupContext,
        PyPostAddEdgeToGroupContext,
        PyPostAddEdgeWithGroupContext,
        PyPostAddGroupContext,
        PyPostAddNodeContext,
        PyPostAddNodesContext,
        PyPostAddNodesDataframesContext,
        PyPostAddNodesDataframesWithGroupContext,
        PyPostAddNodesWithGroupContext,
        PyPostAddNodeToGroupContext,
        PyPostAddNodeWithGroupContext,
        PyPostRemoveEdgeContext,
        PyPostRemoveEdgeFromGroupContext,
        PyPostRemoveGroupContext,
        PyPostRemoveNodeContext,
        PyPostRemoveNodeFromGroupContext,
        PyPreAddEdgeContext,
        PyPreAddEdgesContext,
        PyPreAddEdgesDataframesContext,
        PyPreAddEdgesDataframesWithGroupContext,
        PyPreAddEdgesWithGroupContext,
        PyPreAddEdgeToGroupContext,
        PyPreAddEdgeWithGroupContext,
        PyPreAddGroupContext,
        PyPreAddNodeContext,
        PyPreAddNodesContext,
        PyPreAddNodesDataframesContext,
        PyPreAddNodesDataframesWithGroupContext,
        PyPreAddNodesWithGroupContext,
        PyPreAddNodeToGroupContext,
        PyPreAddNodeWithGroupContext,
        PyPreRemoveEdgeContext,
        PyPreRemoveEdgeFromGroupContext,
        PyPreRemoveGroupContext,
        PyPreRemoveNodeContext,
        PyPreRemoveNodeFromGroupContext,
        PyPreSetSchemaContext,
    )
    from graphrecords.graphrecord import GraphRecord
    from graphrecords.schema import Schema
    from graphrecords.types import (
        Attributes,
        EdgeIndex,
        Group,
        NodeIndex,
        PolarsEdgeDataFrameInput,
        PolarsNodeDataFrameInput,
    )


class _PluginBridge(PyPlugin):  # pyright: ignore[reportUnusedClass]
    _plugin: Plugin

    def __init__(self, plugin: Plugin) -> None:
        self._plugin = plugin

    def _graphrecord(self, graphrecord: PyGraphRecord) -> GraphRecord:
        from graphrecords.graphrecord import GraphRecord

        return GraphRecord._from_py_graphrecord(graphrecord)

    def initialize(self, graphrecord: PyGraphRecord) -> None:
        self._plugin.initialize(self._graphrecord(graphrecord))

    def pre_set_schema(
        self, graphrecord: PyGraphRecord, context: PyPreSetSchemaContext
    ) -> PyPreSetSchemaContext:
        return self._plugin.pre_set_schema(
            self._graphrecord(graphrecord),
            PreSetSchemaContext._from_py_pre_set_schema_context(context),
        )._py_pre_set_schema_context

    def post_set_schema(self, graphrecord: PyGraphRecord) -> None:
        self._plugin.post_set_schema(self._graphrecord(graphrecord))

    def pre_freeze_schema(self, graphrecord: PyGraphRecord) -> None:
        self._plugin.pre_freeze_schema(self._graphrecord(graphrecord))

    def post_freeze_schema(self, graphrecord: PyGraphRecord) -> None:
        self._plugin.post_freeze_schema(self._graphrecord(graphrecord))

    def pre_unfreeze_schema(self, graphrecord: PyGraphRecord) -> None:
        self._plugin.pre_unfreeze_schema(self._graphrecord(graphrecord))

    def post_unfreeze_schema(self, graphrecord: PyGraphRecord) -> None:
        self._plugin.post_unfreeze_schema(self._graphrecord(graphrecord))

    def pre_add_node(
        self, graphrecord: PyGraphRecord, context: PyPreAddNodeContext
    ) -> PyPreAddNodeContext:
        return self._plugin.pre_add_node(
            self._graphrecord(graphrecord),
            PreAddNodeContext._from_py_context(context),
        )._py_context

    def post_add_node(
        self, graphrecord: PyGraphRecord, context: PyPostAddNodeContext
    ) -> None:
        self._plugin.post_add_node(
            self._graphrecord(graphrecord),
            PostAddNodeContext._from_py_context(context),
        )

    def pre_add_node_with_group(
        self, graphrecord: PyGraphRecord, context: PyPreAddNodeWithGroupContext
    ) -> PyPreAddNodeWithGroupContext:
        return self._plugin.pre_add_node_with_group(
            self._graphrecord(graphrecord),
            PreAddNodeWithGroupContext._from_py_context(context),
        )._py_context

    def post_add_node_with_group(
        self, graphrecord: PyGraphRecord, context: PyPostAddNodeWithGroupContext
    ) -> None:
        self._plugin.post_add_node_with_group(
            self._graphrecord(graphrecord),
            PostAddNodeWithGroupContext._from_py_context(context),
        )

    def pre_remove_node(
        self, graphrecord: PyGraphRecord, context: PyPreRemoveNodeContext
    ) -> PyPreRemoveNodeContext:
        return self._plugin.pre_remove_node(
            self._graphrecord(graphrecord),
            PreRemoveNodeContext._from_py_context(context),
        )._py_context

    def post_remove_node(
        self, graphrecord: PyGraphRecord, context: PyPostRemoveNodeContext
    ) -> None:
        self._plugin.post_remove_node(
            self._graphrecord(graphrecord),
            PostRemoveNodeContext._from_py_context(context),
        )

    def pre_add_nodes(
        self, graphrecord: PyGraphRecord, context: PyPreAddNodesContext
    ) -> PyPreAddNodesContext:
        return self._plugin.pre_add_nodes(
            self._graphrecord(graphrecord),
            PreAddNodesContext._from_py_context(context),
        )._py_context

    def post_add_nodes(
        self, graphrecord: PyGraphRecord, context: PyPostAddNodesContext
    ) -> None:
        self._plugin.post_add_nodes(
            self._graphrecord(graphrecord),
            PostAddNodesContext._from_py_context(context),
        )

    def pre_add_nodes_with_group(
        self, graphrecord: PyGraphRecord, context: PyPreAddNodesWithGroupContext
    ) -> PyPreAddNodesWithGroupContext:
        return self._plugin.pre_add_nodes_with_group(
            self._graphrecord(graphrecord),
            PreAddNodesWithGroupContext._from_py_context(context),
        )._py_context

    def post_add_nodes_with_group(
        self, graphrecord: PyGraphRecord, context: PyPostAddNodesWithGroupContext
    ) -> None:
        self._plugin.post_add_nodes_with_group(
            self._graphrecord(graphrecord),
            PostAddNodesWithGroupContext._from_py_context(context),
        )

    def pre_add_nodes_dataframes(
        self, graphrecord: PyGraphRecord, context: PyPreAddNodesDataframesContext
    ) -> PyPreAddNodesDataframesContext:
        return self._plugin.pre_add_nodes_dataframes(
            self._graphrecord(graphrecord),
            PreAddNodesDataframesContext._from_py_context(context),
        )._py_context

    def post_add_nodes_dataframes(
        self, graphrecord: PyGraphRecord, context: PyPostAddNodesDataframesContext
    ) -> None:
        self._plugin.post_add_nodes_dataframes(
            self._graphrecord(graphrecord),
            PostAddNodesDataframesContext._from_py_context(context),
        )

    def pre_add_nodes_dataframes_with_group(
        self,
        graphrecord: PyGraphRecord,
        context: PyPreAddNodesDataframesWithGroupContext,
    ) -> PyPreAddNodesDataframesWithGroupContext:
        return self._plugin.pre_add_nodes_dataframes_with_group(
            self._graphrecord(graphrecord),
            PreAddNodesDataframesWithGroupContext._from_py_context(context),
        )._py_context

    def post_add_nodes_dataframes_with_group(
        self,
        graphrecord: PyGraphRecord,
        context: PyPostAddNodesDataframesWithGroupContext,
    ) -> None:
        self._plugin.post_add_nodes_dataframes_with_group(
            self._graphrecord(graphrecord),
            PostAddNodesDataframesWithGroupContext._from_py_context(context),
        )

    def pre_add_edge(
        self, graphrecord: PyGraphRecord, context: PyPreAddEdgeContext
    ) -> PyPreAddEdgeContext:
        return self._plugin.pre_add_edge(
            self._graphrecord(graphrecord),
            PreAddEdgeContext._from_py_context(context),
        )._py_context

    def post_add_edge(
        self, graphrecord: PyGraphRecord, context: PyPostAddEdgeContext
    ) -> None:
        self._plugin.post_add_edge(
            self._graphrecord(graphrecord),
            PostAddEdgeContext._from_py_context(context),
        )

    def pre_add_edge_with_group(
        self, graphrecord: PyGraphRecord, context: PyPreAddEdgeWithGroupContext
    ) -> PyPreAddEdgeWithGroupContext:
        return self._plugin.pre_add_edge_with_group(
            self._graphrecord(graphrecord),
            PreAddEdgeWithGroupContext._from_py_context(context),
        )._py_context

    def post_add_edge_with_group(
        self, graphrecord: PyGraphRecord, context: PyPostAddEdgeWithGroupContext
    ) -> None:
        self._plugin.post_add_edge_with_group(
            self._graphrecord(graphrecord),
            PostAddEdgeWithGroupContext._from_py_context(context),
        )

    def pre_remove_edge(
        self, graphrecord: PyGraphRecord, context: PyPreRemoveEdgeContext
    ) -> PyPreRemoveEdgeContext:
        return self._plugin.pre_remove_edge(
            self._graphrecord(graphrecord),
            PreRemoveEdgeContext._from_py_context(context),
        )._py_context

    def post_remove_edge(
        self, graphrecord: PyGraphRecord, context: PyPostRemoveEdgeContext
    ) -> None:
        self._plugin.post_remove_edge(
            self._graphrecord(graphrecord),
            PostRemoveEdgeContext._from_py_context(context),
        )

    def pre_add_edges(
        self, graphrecord: PyGraphRecord, context: PyPreAddEdgesContext
    ) -> PyPreAddEdgesContext:
        return self._plugin.pre_add_edges(
            self._graphrecord(graphrecord),
            PreAddEdgesContext._from_py_context(context),
        )._py_context

    def post_add_edges(
        self, graphrecord: PyGraphRecord, context: PyPostAddEdgesContext
    ) -> None:
        self._plugin.post_add_edges(
            self._graphrecord(graphrecord),
            PostAddEdgesContext._from_py_context(context),
        )

    def pre_add_edges_with_group(
        self, graphrecord: PyGraphRecord, context: PyPreAddEdgesWithGroupContext
    ) -> PyPreAddEdgesWithGroupContext:
        return self._plugin.pre_add_edges_with_group(
            self._graphrecord(graphrecord),
            PreAddEdgesWithGroupContext._from_py_context(context),
        )._py_context

    def post_add_edges_with_group(
        self, graphrecord: PyGraphRecord, context: PyPostAddEdgesWithGroupContext
    ) -> None:
        self._plugin.post_add_edges_with_group(
            self._graphrecord(graphrecord),
            PostAddEdgesWithGroupContext._from_py_context(context),
        )

    def pre_add_edges_dataframes(
        self, graphrecord: PyGraphRecord, context: PyPreAddEdgesDataframesContext
    ) -> PyPreAddEdgesDataframesContext:
        return self._plugin.pre_add_edges_dataframes(
            self._graphrecord(graphrecord),
            PreAddEdgesDataframesContext._from_py_context(context),
        )._py_context

    def post_add_edges_dataframes(
        self, graphrecord: PyGraphRecord, context: PyPostAddEdgesDataframesContext
    ) -> None:
        self._plugin.post_add_edges_dataframes(
            self._graphrecord(graphrecord),
            PostAddEdgesDataframesContext._from_py_context(context),
        )

    def pre_add_edges_dataframes_with_group(
        self,
        graphrecord: PyGraphRecord,
        context: PyPreAddEdgesDataframesWithGroupContext,
    ) -> PyPreAddEdgesDataframesWithGroupContext:
        return self._plugin.pre_add_edges_dataframes_with_group(
            self._graphrecord(graphrecord),
            PreAddEdgesDataframesWithGroupContext._from_py_context(context),
        )._py_context

    def post_add_edges_dataframes_with_group(
        self,
        graphrecord: PyGraphRecord,
        context: PyPostAddEdgesDataframesWithGroupContext,
    ) -> None:
        self._plugin.post_add_edges_dataframes_with_group(
            self._graphrecord(graphrecord),
            PostAddEdgesDataframesWithGroupContext._from_py_context(context),
        )

    def pre_add_group(
        self, graphrecord: PyGraphRecord, context: PyPreAddGroupContext
    ) -> PyPreAddGroupContext:
        return self._plugin.pre_add_group(
            self._graphrecord(graphrecord),
            PreAddGroupContext._from_py_context(context),
        )._py_context

    def post_add_group(
        self, graphrecord: PyGraphRecord, context: PyPostAddGroupContext
    ) -> None:
        self._plugin.post_add_group(
            self._graphrecord(graphrecord),
            PostAddGroupContext._from_py_context(context),
        )

    def pre_remove_group(
        self, graphrecord: PyGraphRecord, context: PyPreRemoveGroupContext
    ) -> PyPreRemoveGroupContext:
        return self._plugin.pre_remove_group(
            self._graphrecord(graphrecord),
            PreRemoveGroupContext._from_py_context(context),
        )._py_context

    def post_remove_group(
        self, graphrecord: PyGraphRecord, context: PyPostRemoveGroupContext
    ) -> None:
        self._plugin.post_remove_group(
            self._graphrecord(graphrecord),
            PostRemoveGroupContext._from_py_context(context),
        )

    def pre_add_node_to_group(
        self, graphrecord: PyGraphRecord, context: PyPreAddNodeToGroupContext
    ) -> PyPreAddNodeToGroupContext:
        return self._plugin.pre_add_node_to_group(
            self._graphrecord(graphrecord),
            PreAddNodeToGroupContext._from_py_context(context),
        )._py_context

    def post_add_node_to_group(
        self, graphrecord: PyGraphRecord, context: PyPostAddNodeToGroupContext
    ) -> None:
        self._plugin.post_add_node_to_group(
            self._graphrecord(graphrecord),
            PostAddNodeToGroupContext._from_py_context(context),
        )

    def pre_add_edge_to_group(
        self, graphrecord: PyGraphRecord, context: PyPreAddEdgeToGroupContext
    ) -> PyPreAddEdgeToGroupContext:
        return self._plugin.pre_add_edge_to_group(
            self._graphrecord(graphrecord),
            PreAddEdgeToGroupContext._from_py_context(context),
        )._py_context

    def post_add_edge_to_group(
        self, graphrecord: PyGraphRecord, context: PyPostAddEdgeToGroupContext
    ) -> None:
        self._plugin.post_add_edge_to_group(
            self._graphrecord(graphrecord),
            PostAddEdgeToGroupContext._from_py_context(context),
        )

    def pre_remove_node_from_group(
        self, graphrecord: PyGraphRecord, context: PyPreRemoveNodeFromGroupContext
    ) -> PyPreRemoveNodeFromGroupContext:
        return self._plugin.pre_remove_node_from_group(
            self._graphrecord(graphrecord),
            PreRemoveNodeFromGroupContext._from_py_context(context),
        )._py_context

    def post_remove_node_from_group(
        self, graphrecord: PyGraphRecord, context: PyPostRemoveNodeFromGroupContext
    ) -> None:
        self._plugin.post_remove_node_from_group(
            self._graphrecord(graphrecord),
            PostRemoveNodeFromGroupContext._from_py_context(context),
        )

    def pre_remove_edge_from_group(
        self, graphrecord: PyGraphRecord, context: PyPreRemoveEdgeFromGroupContext
    ) -> PyPreRemoveEdgeFromGroupContext:
        return self._plugin.pre_remove_edge_from_group(
            self._graphrecord(graphrecord),
            PreRemoveEdgeFromGroupContext._from_py_context(context),
        )._py_context

    def post_remove_edge_from_group(
        self, graphrecord: PyGraphRecord, context: PyPostRemoveEdgeFromGroupContext
    ) -> None:
        self._plugin.post_remove_edge_from_group(
            self._graphrecord(graphrecord),
            PostRemoveEdgeFromGroupContext._from_py_context(context),
        )

    def pre_clear(self, graphrecord: PyGraphRecord) -> None:
        self._plugin.pre_clear(self._graphrecord(graphrecord))

    def post_clear(self, graphrecord: PyGraphRecord) -> None:
        self._plugin.post_clear(self._graphrecord(graphrecord))


class PreSetSchemaContext:
    """Context for the pre_set_schema hook."""

    _py_pre_set_schema_context: PyPreSetSchemaContext

    def __init__(self, schema: Schema) -> None:
        """Initializes a PreSetSchemaContext.

        Args:
            schema (Schema): The schema being set.
        """
        from graphrecords._graphrecords import PyPreSetSchemaContext

        self._py_pre_set_schema_context = PyPreSetSchemaContext(schema._schema)

    @classmethod
    def _from_py_pre_set_schema_context(
        cls, py_context: PyPreSetSchemaContext
    ) -> PreSetSchemaContext:
        context = cls.__new__(cls)
        context._py_pre_set_schema_context = py_context
        return context

    @property
    def schema(self) -> Schema:
        """The schema being set."""
        from graphrecords.schema import Schema

        return Schema._from_py_schema(self._py_pre_set_schema_context.schema)


class PreAddNodeContext:
    """Context for the pre_add_node hook."""

    _py_context: PyPreAddNodeContext

    def __init__(self, node_index: NodeIndex, attributes: Attributes) -> None:
        """Initializes a PreAddNodeContext.

        Args:
            node_index (NodeIndex): The index of the node being added.
            attributes (Attributes): The attributes of the node being added.
        """
        from graphrecords._graphrecords import PyPreAddNodeContext

        self._py_context = PyPreAddNodeContext(node_index, attributes)

    @classmethod
    def _from_py_context(cls, py_context: PyPreAddNodeContext) -> PreAddNodeContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def node_index(self) -> NodeIndex:
        """The index of the node being added."""
        return self._py_context.node_index

    @property
    def attributes(self) -> Attributes:
        """The attributes of the node being added."""
        return self._py_context.attributes


class PostAddNodeContext:
    """Context for the post_add_node hook."""

    _py_context: PyPostAddNodeContext

    def __init__(self, node_index: NodeIndex) -> None:
        """Initializes a PostAddNodeContext.

        Args:
            node_index (NodeIndex): The index of the node that was added.
        """
        from graphrecords._graphrecords import PyPostAddNodeContext

        self._py_context = PyPostAddNodeContext(node_index)

    @classmethod
    def _from_py_context(cls, py_context: PyPostAddNodeContext) -> PostAddNodeContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def node_index(self) -> NodeIndex:
        """The index of the node that was added."""
        return self._py_context.node_index


class PreAddNodeWithGroupContext:
    """Context for the pre_add_node_with_group hook."""

    _py_context: PyPreAddNodeWithGroupContext

    def __init__(
        self, node_index: NodeIndex, attributes: Attributes, group: Group
    ) -> None:
        """Initializes a PreAddNodeWithGroupContext.

        Args:
            node_index (NodeIndex): The index of the node being added.
            attributes (Attributes): The attributes of the node being added.
            group (Group): The group to add the node to.
        """
        from graphrecords._graphrecords import PyPreAddNodeWithGroupContext

        self._py_context = PyPreAddNodeWithGroupContext(node_index, attributes, group)

    @classmethod
    def _from_py_context(
        cls, py_context: PyPreAddNodeWithGroupContext
    ) -> PreAddNodeWithGroupContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def node_index(self) -> NodeIndex:
        """The index of the node being added."""
        return self._py_context.node_index

    @property
    def attributes(self) -> Attributes:
        """The attributes of the node being added."""
        return self._py_context.attributes

    @property
    def group(self) -> Group:
        """The group to add the node to."""
        return self._py_context.group


class PostAddNodeWithGroupContext:
    """Context for the post_add_node_with_group hook."""

    _py_context: PyPostAddNodeWithGroupContext

    def __init__(self, node_index: NodeIndex, group: Group) -> None:
        """Initializes a PostAddNodeWithGroupContext.

        Args:
            node_index (NodeIndex): The index of the node that was added.
            group (Group): The group the node was added to.
        """
        from graphrecords._graphrecords import PyPostAddNodeWithGroupContext

        self._py_context = PyPostAddNodeWithGroupContext(node_index, group)

    @classmethod
    def _from_py_context(
        cls, py_context: PyPostAddNodeWithGroupContext
    ) -> PostAddNodeWithGroupContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def node_index(self) -> NodeIndex:
        """The index of the node that was added."""
        return self._py_context.node_index

    @property
    def group(self) -> Group:
        """The group the node was added to."""
        return self._py_context.group


class PreRemoveNodeContext:
    """Context for the pre_remove_node hook."""

    _py_context: PyPreRemoveNodeContext

    def __init__(self, node_index: NodeIndex) -> None:
        """Initializes a PreRemoveNodeContext.

        Args:
            node_index (NodeIndex): The index of the node being removed.
        """
        from graphrecords._graphrecords import PyPreRemoveNodeContext

        self._py_context = PyPreRemoveNodeContext(node_index)

    @classmethod
    def _from_py_context(
        cls, py_context: PyPreRemoveNodeContext
    ) -> PreRemoveNodeContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def node_index(self) -> NodeIndex:
        """The index of the node being removed."""
        return self._py_context.node_index


class PostRemoveNodeContext:
    """Context for the post_remove_node hook."""

    _py_context: PyPostRemoveNodeContext

    def __init__(self, node_index: NodeIndex) -> None:
        """Initializes a PostRemoveNodeContext.

        Args:
            node_index (NodeIndex): The index of the node that was removed.
        """
        from graphrecords._graphrecords import PyPostRemoveNodeContext

        self._py_context = PyPostRemoveNodeContext(node_index)

    @classmethod
    def _from_py_context(
        cls, py_context: PyPostRemoveNodeContext
    ) -> PostRemoveNodeContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def node_index(self) -> NodeIndex:
        """The index of the node that was removed."""
        return self._py_context.node_index


class PreAddNodesContext:
    """Context for the pre_add_nodes hook."""

    _py_context: PyPreAddNodesContext

    def __init__(self, nodes: List[Tuple[NodeIndex, Attributes]]) -> None:
        """Initializes a PreAddNodesContext.

        Args:
            nodes (List[Tuple[NodeIndex, Attributes]]): The nodes being added.
        """
        from graphrecords._graphrecords import PyPreAddNodesContext

        self._py_context = PyPreAddNodesContext(nodes)

    @classmethod
    def _from_py_context(cls, py_context: PyPreAddNodesContext) -> PreAddNodesContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def nodes(self) -> List[Tuple[NodeIndex, Attributes]]:
        """The nodes being added."""
        return self._py_context.nodes


class PostAddNodesContext:
    """Context for the post_add_nodes hook."""

    _py_context: PyPostAddNodesContext

    def __init__(self, nodes: List[Tuple[NodeIndex, Attributes]]) -> None:
        """Initializes a PostAddNodesContext.

        Args:
            nodes (List[Tuple[NodeIndex, Attributes]]): The nodes that were added.
        """
        from graphrecords._graphrecords import PyPostAddNodesContext

        self._py_context = PyPostAddNodesContext(nodes)

    @classmethod
    def _from_py_context(cls, py_context: PyPostAddNodesContext) -> PostAddNodesContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def nodes(self) -> List[Tuple[NodeIndex, Attributes]]:
        """The nodes that were added."""
        return self._py_context.nodes


class PreAddNodesWithGroupContext:
    """Context for the pre_add_nodes_with_group hook."""

    _py_context: PyPreAddNodesWithGroupContext

    def __init__(self, nodes: List[Tuple[NodeIndex, Attributes]], group: Group) -> None:
        """Initializes a PreAddNodesWithGroupContext.

        Args:
            nodes (List[Tuple[NodeIndex, Attributes]]): The nodes being added.
            group (Group): The group to add the nodes to.
        """
        from graphrecords._graphrecords import PyPreAddNodesWithGroupContext

        self._py_context = PyPreAddNodesWithGroupContext(nodes, group)

    @classmethod
    def _from_py_context(
        cls, py_context: PyPreAddNodesWithGroupContext
    ) -> PreAddNodesWithGroupContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def nodes(self) -> List[Tuple[NodeIndex, Attributes]]:
        """The nodes being added."""
        return self._py_context.nodes

    @property
    def group(self) -> Group:
        """The group to add the nodes to."""
        return self._py_context.group


class PostAddNodesWithGroupContext:
    """Context for the post_add_nodes_with_group hook."""

    _py_context: PyPostAddNodesWithGroupContext

    def __init__(self, nodes: List[Tuple[NodeIndex, Attributes]], group: Group) -> None:
        """Initializes a PostAddNodesWithGroupContext.

        Args:
            nodes (List[Tuple[NodeIndex, Attributes]]): The nodes that were added.
            group (Group): The group the nodes were added to.
        """
        from graphrecords._graphrecords import PyPostAddNodesWithGroupContext

        self._py_context = PyPostAddNodesWithGroupContext(nodes, group)

    @classmethod
    def _from_py_context(
        cls, py_context: PyPostAddNodesWithGroupContext
    ) -> PostAddNodesWithGroupContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def nodes(self) -> List[Tuple[NodeIndex, Attributes]]:
        """The nodes that were added."""
        return self._py_context.nodes

    @property
    def group(self) -> Group:
        """The group the nodes were added to."""
        return self._py_context.group


class PreAddNodesDataframesContext:
    """Context for the pre_add_nodes_dataframes hook."""

    _py_context: PyPreAddNodesDataframesContext

    def __init__(self, nodes_dataframes: List[PolarsNodeDataFrameInput]) -> None:
        """Initializes a PreAddNodesDataframesContext.

        Args:
            nodes_dataframes (List[PolarsNodeDataFrameInput]): The node dataframe
                inputs.
        """
        from graphrecords._graphrecords import PyPreAddNodesDataframesContext

        self._py_context = PyPreAddNodesDataframesContext(nodes_dataframes)

    @classmethod
    def _from_py_context(
        cls, py_context: PyPreAddNodesDataframesContext
    ) -> PreAddNodesDataframesContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def nodes_dataframes(self) -> List[PolarsNodeDataFrameInput]:
        """The node dataframe inputs."""
        return self._py_context.nodes_dataframes


class PostAddNodesDataframesContext:
    """Context for the post_add_nodes_dataframes hook."""

    _py_context: PyPostAddNodesDataframesContext

    def __init__(self, nodes_dataframes: List[PolarsNodeDataFrameInput]) -> None:
        """Initializes a PostAddNodesDataframesContext.

        Args:
            nodes_dataframes (List[PolarsNodeDataFrameInput]): The node dataframe
                inputs.
        """
        from graphrecords._graphrecords import PyPostAddNodesDataframesContext

        self._py_context = PyPostAddNodesDataframesContext(nodes_dataframes)

    @classmethod
    def _from_py_context(
        cls, py_context: PyPostAddNodesDataframesContext
    ) -> PostAddNodesDataframesContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def nodes_dataframes(self) -> List[PolarsNodeDataFrameInput]:
        """The node dataframe inputs."""
        return self._py_context.nodes_dataframes


class PreAddNodesDataframesWithGroupContext:
    """Context for the pre_add_nodes_dataframes_with_group hook."""

    _py_context: PyPreAddNodesDataframesWithGroupContext

    def __init__(
        self, nodes_dataframes: List[PolarsNodeDataFrameInput], group: Group
    ) -> None:
        """Initializes a PreAddNodesDataframesWithGroupContext.

        Args:
            nodes_dataframes (List[PolarsNodeDataFrameInput]): The node dataframe
                inputs.
            group (Group): The group to add the nodes to.
        """
        from graphrecords._graphrecords import (
            PyPreAddNodesDataframesWithGroupContext,
        )

        self._py_context = PyPreAddNodesDataframesWithGroupContext(
            nodes_dataframes, group
        )

    @classmethod
    def _from_py_context(
        cls, py_context: PyPreAddNodesDataframesWithGroupContext
    ) -> PreAddNodesDataframesWithGroupContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def nodes_dataframes(self) -> List[PolarsNodeDataFrameInput]:
        """The node dataframe inputs."""
        return self._py_context.nodes_dataframes

    @property
    def group(self) -> Group:
        """The group to add the nodes to."""
        return self._py_context.group


class PostAddNodesDataframesWithGroupContext:
    """Context for the post_add_nodes_dataframes_with_group hook."""

    _py_context: PyPostAddNodesDataframesWithGroupContext

    def __init__(
        self, nodes_dataframes: List[PolarsNodeDataFrameInput], group: Group
    ) -> None:
        """Initializes a PostAddNodesDataframesWithGroupContext.

        Args:
            nodes_dataframes (List[PolarsNodeDataFrameInput]): The node dataframe
                inputs.
            group (Group): The group the nodes were added to.
        """
        from graphrecords._graphrecords import (
            PyPostAddNodesDataframesWithGroupContext,
        )

        self._py_context = PyPostAddNodesDataframesWithGroupContext(
            nodes_dataframes, group
        )

    @classmethod
    def _from_py_context(
        cls, py_context: PyPostAddNodesDataframesWithGroupContext
    ) -> PostAddNodesDataframesWithGroupContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def nodes_dataframes(self) -> List[PolarsNodeDataFrameInput]:
        """The node dataframe inputs."""
        return self._py_context.nodes_dataframes

    @property
    def group(self) -> Group:
        """The group the nodes were added to."""
        return self._py_context.group


class PreAddEdgeContext:
    """Context for the pre_add_edge hook."""

    _py_context: PyPreAddEdgeContext

    def __init__(
        self,
        source_node_index: NodeIndex,
        target_node_index: NodeIndex,
        attributes: Attributes,
    ) -> None:
        """Initializes a PreAddEdgeContext.

        Args:
            source_node_index (NodeIndex): The index of the source node.
            target_node_index (NodeIndex): The index of the target node.
            attributes (Attributes): The attributes of the edge being added.
        """
        from graphrecords._graphrecords import PyPreAddEdgeContext

        self._py_context = PyPreAddEdgeContext(
            source_node_index, target_node_index, attributes
        )

    @classmethod
    def _from_py_context(cls, py_context: PyPreAddEdgeContext) -> PreAddEdgeContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def source_node_index(self) -> NodeIndex:
        """The index of the source node."""
        return self._py_context.source_node_index

    @property
    def target_node_index(self) -> NodeIndex:
        """The index of the target node."""
        return self._py_context.target_node_index

    @property
    def attributes(self) -> Attributes:
        """The attributes of the edge being added."""
        return self._py_context.attributes


class PostAddEdgeContext:
    """Context for the post_add_edge hook."""

    _py_context: PyPostAddEdgeContext

    def __init__(self, edge_index: EdgeIndex) -> None:
        """Initializes a PostAddEdgeContext.

        Args:
            edge_index (EdgeIndex): The index of the edge that was added.
        """
        from graphrecords._graphrecords import PyPostAddEdgeContext

        self._py_context = PyPostAddEdgeContext(edge_index)

    @classmethod
    def _from_py_context(cls, py_context: PyPostAddEdgeContext) -> PostAddEdgeContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def edge_index(self) -> EdgeIndex:
        """The index of the edge that was added."""
        return self._py_context.edge_index


class PreAddEdgeWithGroupContext:
    """Context for the pre_add_edge_with_group hook."""

    _py_context: PyPreAddEdgeWithGroupContext

    def __init__(
        self,
        source_node_index: NodeIndex,
        target_node_index: NodeIndex,
        attributes: Attributes,
        group: Group,
    ) -> None:
        """Initializes a PreAddEdgeWithGroupContext.

        Args:
            source_node_index (NodeIndex): The index of the source node.
            target_node_index (NodeIndex): The index of the target node.
            attributes (Attributes): The attributes of the edge being added.
            group (Group): The group to add the edge to.
        """
        from graphrecords._graphrecords import PyPreAddEdgeWithGroupContext

        self._py_context = PyPreAddEdgeWithGroupContext(
            source_node_index, target_node_index, attributes, group
        )

    @classmethod
    def _from_py_context(
        cls, py_context: PyPreAddEdgeWithGroupContext
    ) -> PreAddEdgeWithGroupContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def source_node_index(self) -> NodeIndex:
        """The index of the source node."""
        return self._py_context.source_node_index

    @property
    def target_node_index(self) -> NodeIndex:
        """The index of the target node."""
        return self._py_context.target_node_index

    @property
    def attributes(self) -> Attributes:
        """The attributes of the edge being added."""
        return self._py_context.attributes

    @property
    def group(self) -> Group:
        """The group to add the edge to."""
        return self._py_context.group


class PostAddEdgeWithGroupContext:
    """Context for the post_add_edge_with_group hook."""

    _py_context: PyPostAddEdgeWithGroupContext

    def __init__(self, edge_index: EdgeIndex) -> None:
        """Initializes a PostAddEdgeWithGroupContext.

        Args:
            edge_index (EdgeIndex): The index of the edge that was added.
        """
        from graphrecords._graphrecords import PyPostAddEdgeWithGroupContext

        self._py_context = PyPostAddEdgeWithGroupContext(edge_index)

    @classmethod
    def _from_py_context(
        cls, py_context: PyPostAddEdgeWithGroupContext
    ) -> PostAddEdgeWithGroupContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def edge_index(self) -> EdgeIndex:
        """The index of the edge that was added."""
        return self._py_context.edge_index


class PreRemoveEdgeContext:
    """Context for the pre_remove_edge hook."""

    _py_context: PyPreRemoveEdgeContext

    def __init__(self, edge_index: EdgeIndex) -> None:
        """Initializes a PreRemoveEdgeContext.

        Args:
            edge_index (EdgeIndex): The index of the edge being removed.
        """
        from graphrecords._graphrecords import PyPreRemoveEdgeContext

        self._py_context = PyPreRemoveEdgeContext(edge_index)

    @classmethod
    def _from_py_context(
        cls, py_context: PyPreRemoveEdgeContext
    ) -> PreRemoveEdgeContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def edge_index(self) -> EdgeIndex:
        """The index of the edge being removed."""
        return self._py_context.edge_index


class PostRemoveEdgeContext:
    """Context for the post_remove_edge hook."""

    _py_context: PyPostRemoveEdgeContext

    def __init__(self, edge_index: EdgeIndex) -> None:
        """Initializes a PostRemoveEdgeContext.

        Args:
            edge_index (EdgeIndex): The index of the edge that was removed.
        """
        from graphrecords._graphrecords import PyPostRemoveEdgeContext

        self._py_context = PyPostRemoveEdgeContext(edge_index)

    @classmethod
    def _from_py_context(
        cls, py_context: PyPostRemoveEdgeContext
    ) -> PostRemoveEdgeContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def edge_index(self) -> EdgeIndex:
        """The index of the edge that was removed."""
        return self._py_context.edge_index


class PreAddEdgesContext:
    """Context for the pre_add_edges hook."""

    _py_context: PyPreAddEdgesContext

    def __init__(self, edges: List[Tuple[NodeIndex, NodeIndex, Attributes]]) -> None:
        """Initializes a PreAddEdgesContext.

        Args:
            edges (List[Tuple[NodeIndex, NodeIndex, Attributes]]): The edges being
                added.
        """
        from graphrecords._graphrecords import PyPreAddEdgesContext

        self._py_context = PyPreAddEdgesContext(edges)

    @classmethod
    def _from_py_context(cls, py_context: PyPreAddEdgesContext) -> PreAddEdgesContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def edges(self) -> List[Tuple[NodeIndex, NodeIndex, Attributes]]:
        """The edges being added."""
        return self._py_context.edges


class PostAddEdgesContext:
    """Context for the post_add_edges hook."""

    _py_context: PyPostAddEdgesContext

    def __init__(self, edge_indices: List[EdgeIndex]) -> None:
        """Initializes a PostAddEdgesContext.

        Args:
            edge_indices (List[EdgeIndex]): The indices of the edges that were added.
        """
        from graphrecords._graphrecords import PyPostAddEdgesContext

        self._py_context = PyPostAddEdgesContext(edge_indices)

    @classmethod
    def _from_py_context(cls, py_context: PyPostAddEdgesContext) -> PostAddEdgesContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def edge_indices(self) -> List[EdgeIndex]:
        """The indices of the edges that were added."""
        return self._py_context.edge_indices


class PreAddEdgesWithGroupContext:
    """Context for the pre_add_edges_with_group hook."""

    _py_context: PyPreAddEdgesWithGroupContext

    def __init__(
        self, edges: List[Tuple[NodeIndex, NodeIndex, Attributes]], group: Group
    ) -> None:
        """Initializes a PreAddEdgesWithGroupContext.

        Args:
            edges (List[Tuple[NodeIndex, NodeIndex, Attributes]]): The edges being
                added.
            group (Group): The group to add the edges to.
        """
        from graphrecords._graphrecords import PyPreAddEdgesWithGroupContext

        self._py_context = PyPreAddEdgesWithGroupContext(edges, group)

    @classmethod
    def _from_py_context(
        cls, py_context: PyPreAddEdgesWithGroupContext
    ) -> PreAddEdgesWithGroupContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def edges(self) -> List[Tuple[NodeIndex, NodeIndex, Attributes]]:
        """The edges being added."""
        return self._py_context.edges

    @property
    def group(self) -> Group:
        """The group to add the edges to."""
        return self._py_context.group


class PostAddEdgesWithGroupContext:
    """Context for the post_add_edges_with_group hook."""

    _py_context: PyPostAddEdgesWithGroupContext

    def __init__(self, edge_indices: List[EdgeIndex]) -> None:
        """Initializes a PostAddEdgesWithGroupContext.

        Args:
            edge_indices (List[EdgeIndex]): The indices of the edges that were added.
        """
        from graphrecords._graphrecords import PyPostAddEdgesWithGroupContext

        self._py_context = PyPostAddEdgesWithGroupContext(edge_indices)

    @classmethod
    def _from_py_context(
        cls, py_context: PyPostAddEdgesWithGroupContext
    ) -> PostAddEdgesWithGroupContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def edge_indices(self) -> List[EdgeIndex]:
        """The indices of the edges that were added."""
        return self._py_context.edge_indices


class PreAddEdgesDataframesContext:
    """Context for the pre_add_edges_dataframes hook."""

    _py_context: PyPreAddEdgesDataframesContext

    def __init__(self, edges_dataframes: List[PolarsEdgeDataFrameInput]) -> None:
        """Initializes a PreAddEdgesDataframesContext.

        Args:
            edges_dataframes (List[PolarsEdgeDataFrameInput]): The edge dataframe
                inputs.
        """
        from graphrecords._graphrecords import PyPreAddEdgesDataframesContext

        self._py_context = PyPreAddEdgesDataframesContext(edges_dataframes)

    @classmethod
    def _from_py_context(
        cls, py_context: PyPreAddEdgesDataframesContext
    ) -> PreAddEdgesDataframesContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def edges_dataframes(self) -> List[PolarsEdgeDataFrameInput]:
        """The edge dataframe inputs."""
        return self._py_context.edges_dataframes


class PostAddEdgesDataframesContext:
    """Context for the post_add_edges_dataframes hook."""

    _py_context: PyPostAddEdgesDataframesContext

    def __init__(self, edges_dataframes: List[PolarsEdgeDataFrameInput]) -> None:
        """Initializes a PostAddEdgesDataframesContext.

        Args:
            edges_dataframes (List[PolarsEdgeDataFrameInput]): The edge dataframe
                inputs.
        """
        from graphrecords._graphrecords import PyPostAddEdgesDataframesContext

        self._py_context = PyPostAddEdgesDataframesContext(edges_dataframes)

    @classmethod
    def _from_py_context(
        cls, py_context: PyPostAddEdgesDataframesContext
    ) -> PostAddEdgesDataframesContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def edges_dataframes(self) -> List[PolarsEdgeDataFrameInput]:
        """The edge dataframe inputs."""
        return self._py_context.edges_dataframes


class PreAddEdgesDataframesWithGroupContext:
    """Context for the pre_add_edges_dataframes_with_group hook."""

    _py_context: PyPreAddEdgesDataframesWithGroupContext

    def __init__(
        self, edges_dataframes: List[PolarsEdgeDataFrameInput], group: Group
    ) -> None:
        """Initializes a PreAddEdgesDataframesWithGroupContext.

        Args:
            edges_dataframes (List[PolarsEdgeDataFrameInput]): The edge dataframe
                inputs.
            group (Group): The group to add the edges to.
        """
        from graphrecords._graphrecords import (
            PyPreAddEdgesDataframesWithGroupContext,
        )

        self._py_context = PyPreAddEdgesDataframesWithGroupContext(
            edges_dataframes, group
        )

    @classmethod
    def _from_py_context(
        cls, py_context: PyPreAddEdgesDataframesWithGroupContext
    ) -> PreAddEdgesDataframesWithGroupContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def edges_dataframes(self) -> List[PolarsEdgeDataFrameInput]:
        """The edge dataframe inputs."""
        return self._py_context.edges_dataframes

    @property
    def group(self) -> Group:
        """The group to add the edges to."""
        return self._py_context.group


class PostAddEdgesDataframesWithGroupContext:
    """Context for the post_add_edges_dataframes_with_group hook."""

    _py_context: PyPostAddEdgesDataframesWithGroupContext

    def __init__(
        self, edges_dataframes: List[PolarsEdgeDataFrameInput], group: Group
    ) -> None:
        """Initializes a PostAddEdgesDataframesWithGroupContext.

        Args:
            edges_dataframes (List[PolarsEdgeDataFrameInput]): The edge dataframe
                inputs.
            group (Group): The group the edges were added to.
        """
        from graphrecords._graphrecords import (
            PyPostAddEdgesDataframesWithGroupContext,
        )

        self._py_context = PyPostAddEdgesDataframesWithGroupContext(
            edges_dataframes, group
        )

    @classmethod
    def _from_py_context(
        cls, py_context: PyPostAddEdgesDataframesWithGroupContext
    ) -> PostAddEdgesDataframesWithGroupContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def edges_dataframes(self) -> List[PolarsEdgeDataFrameInput]:
        """The edge dataframe inputs."""
        return self._py_context.edges_dataframes

    @property
    def group(self) -> Group:
        """The group the edges were added to."""
        return self._py_context.group


class PreAddGroupContext:
    """Context for the pre_add_group hook."""

    _py_context: PyPreAddGroupContext

    def __init__(
        self,
        group: Group,
        node_indices: Optional[List[NodeIndex]],
        edge_indices: Optional[List[EdgeIndex]],
    ) -> None:
        """Initializes a PreAddGroupContext.

        Args:
            group (Group): The group being added.
            node_indices (Optional[List[NodeIndex]]): The node indices to add to
                the group.
            edge_indices (Optional[List[EdgeIndex]]): The edge indices to add to
                the group.
        """
        from graphrecords._graphrecords import PyPreAddGroupContext

        self._py_context = PyPreAddGroupContext(group, node_indices, edge_indices)

    @classmethod
    def _from_py_context(cls, py_context: PyPreAddGroupContext) -> PreAddGroupContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def group(self) -> Group:
        """The group being added."""
        return self._py_context.group

    @property
    def node_indices(self) -> Optional[List[NodeIndex]]:
        """The node indices to add to the group."""
        return self._py_context.node_indices

    @property
    def edge_indices(self) -> Optional[List[EdgeIndex]]:
        """The edge indices to add to the group."""
        return self._py_context.edge_indices


class PostAddGroupContext:
    """Context for the post_add_group hook."""

    _py_context: PyPostAddGroupContext

    def __init__(
        self,
        group: Group,
        node_indices: Optional[List[NodeIndex]],
        edge_indices: Optional[List[EdgeIndex]],
    ) -> None:
        """Initializes a PostAddGroupContext.

        Args:
            group (Group): The group that was added.
            node_indices (Optional[List[NodeIndex]]): The node indices added to
                the group.
            edge_indices (Optional[List[EdgeIndex]]): The edge indices added to
                the group.
        """
        from graphrecords._graphrecords import PyPostAddGroupContext

        self._py_context = PyPostAddGroupContext(group, node_indices, edge_indices)

    @classmethod
    def _from_py_context(cls, py_context: PyPostAddGroupContext) -> PostAddGroupContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def group(self) -> Group:
        """The group that was added."""
        return self._py_context.group

    @property
    def node_indices(self) -> Optional[List[NodeIndex]]:
        """The node indices added to the group."""
        return self._py_context.node_indices

    @property
    def edge_indices(self) -> Optional[List[EdgeIndex]]:
        """The edge indices added to the group."""
        return self._py_context.edge_indices


class PreRemoveGroupContext:
    """Context for the pre_remove_group hook."""

    _py_context: PyPreRemoveGroupContext

    def __init__(self, group: Group) -> None:
        """Initializes a PreRemoveGroupContext.

        Args:
            group (Group): The group being removed.
        """
        from graphrecords._graphrecords import PyPreRemoveGroupContext

        self._py_context = PyPreRemoveGroupContext(group)

    @classmethod
    def _from_py_context(
        cls, py_context: PyPreRemoveGroupContext
    ) -> PreRemoveGroupContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def group(self) -> Group:
        """The group being removed."""
        return self._py_context.group


class PostRemoveGroupContext:
    """Context for the post_remove_group hook."""

    _py_context: PyPostRemoveGroupContext

    def __init__(self, group: Group) -> None:
        """Initializes a PostRemoveGroupContext.

        Args:
            group (Group): The group that was removed.
        """
        from graphrecords._graphrecords import PyPostRemoveGroupContext

        self._py_context = PyPostRemoveGroupContext(group)

    @classmethod
    def _from_py_context(
        cls, py_context: PyPostRemoveGroupContext
    ) -> PostRemoveGroupContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def group(self) -> Group:
        """The group that was removed."""
        return self._py_context.group


class PreAddNodeToGroupContext:
    """Context for the pre_add_node_to_group hook."""

    _py_context: PyPreAddNodeToGroupContext

    def __init__(self, group: Group, node_index: NodeIndex) -> None:
        """Initializes a PreAddNodeToGroupContext.

        Args:
            group (Group): The group to add the node to.
            node_index (NodeIndex): The index of the node being added to the group.
        """
        from graphrecords._graphrecords import PyPreAddNodeToGroupContext

        self._py_context = PyPreAddNodeToGroupContext(group, node_index)

    @classmethod
    def _from_py_context(
        cls, py_context: PyPreAddNodeToGroupContext
    ) -> PreAddNodeToGroupContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def group(self) -> Group:
        """The group to add the node to."""
        return self._py_context.group

    @property
    def node_index(self) -> NodeIndex:
        """The index of the node being added to the group."""
        return self._py_context.node_index


class PostAddNodeToGroupContext:
    """Context for the post_add_node_to_group hook."""

    _py_context: PyPostAddNodeToGroupContext

    def __init__(self, group: Group, node_index: NodeIndex) -> None:
        """Initializes a PostAddNodeToGroupContext.

        Args:
            group (Group): The group the node was added to.
            node_index (NodeIndex): The index of the node that was added to the group.
        """
        from graphrecords._graphrecords import PyPostAddNodeToGroupContext

        self._py_context = PyPostAddNodeToGroupContext(group, node_index)

    @classmethod
    def _from_py_context(
        cls, py_context: PyPostAddNodeToGroupContext
    ) -> PostAddNodeToGroupContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def group(self) -> Group:
        """The group the node was added to."""
        return self._py_context.group

    @property
    def node_index(self) -> NodeIndex:
        """The index of the node that was added to the group."""
        return self._py_context.node_index


class PreAddEdgeToGroupContext:
    """Context for the pre_add_edge_to_group hook."""

    _py_context: PyPreAddEdgeToGroupContext

    def __init__(self, group: Group, edge_index: EdgeIndex) -> None:
        """Initializes a PreAddEdgeToGroupContext.

        Args:
            group (Group): The group to add the edge to.
            edge_index (EdgeIndex): The index of the edge being added to the group.
        """
        from graphrecords._graphrecords import PyPreAddEdgeToGroupContext

        self._py_context = PyPreAddEdgeToGroupContext(group, edge_index)

    @classmethod
    def _from_py_context(
        cls, py_context: PyPreAddEdgeToGroupContext
    ) -> PreAddEdgeToGroupContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def group(self) -> Group:
        """The group to add the edge to."""
        return self._py_context.group

    @property
    def edge_index(self) -> EdgeIndex:
        """The index of the edge being added to the group."""
        return self._py_context.edge_index


class PostAddEdgeToGroupContext:
    """Context for the post_add_edge_to_group hook."""

    _py_context: PyPostAddEdgeToGroupContext

    def __init__(self, group: Group, edge_index: EdgeIndex) -> None:
        """Initializes a PostAddEdgeToGroupContext.

        Args:
            group (Group): The group the edge was added to.
            edge_index (EdgeIndex): The index of the edge that was added to the group.
        """
        from graphrecords._graphrecords import PyPostAddEdgeToGroupContext

        self._py_context = PyPostAddEdgeToGroupContext(group, edge_index)

    @classmethod
    def _from_py_context(
        cls, py_context: PyPostAddEdgeToGroupContext
    ) -> PostAddEdgeToGroupContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def group(self) -> Group:
        """The group the edge was added to."""
        return self._py_context.group

    @property
    def edge_index(self) -> EdgeIndex:
        """The index of the edge that was added to the group."""
        return self._py_context.edge_index


class PreRemoveNodeFromGroupContext:
    """Context for the pre_remove_node_from_group hook."""

    _py_context: PyPreRemoveNodeFromGroupContext

    def __init__(self, group: Group, node_index: NodeIndex) -> None:
        """Initializes a PreRemoveNodeFromGroupContext.

        Args:
            group (Group): The group to remove the node from.
            node_index (NodeIndex): The index of the node being removed from the group.
        """
        from graphrecords._graphrecords import PyPreRemoveNodeFromGroupContext

        self._py_context = PyPreRemoveNodeFromGroupContext(group, node_index)

    @classmethod
    def _from_py_context(
        cls, py_context: PyPreRemoveNodeFromGroupContext
    ) -> PreRemoveNodeFromGroupContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def group(self) -> Group:
        """The group to remove the node from."""
        return self._py_context.group

    @property
    def node_index(self) -> NodeIndex:
        """The index of the node being removed from the group."""
        return self._py_context.node_index


class PostRemoveNodeFromGroupContext:
    """Context for the post_remove_node_from_group hook."""

    _py_context: PyPostRemoveNodeFromGroupContext

    def __init__(self, group: Group, node_index: NodeIndex) -> None:
        """Initializes a PostRemoveNodeFromGroupContext.

        Args:
            group (Group): The group the node was removed from.
            node_index (NodeIndex): The index of the node that was removed from
                the group.
        """
        from graphrecords._graphrecords import PyPostRemoveNodeFromGroupContext

        self._py_context = PyPostRemoveNodeFromGroupContext(group, node_index)

    @classmethod
    def _from_py_context(
        cls, py_context: PyPostRemoveNodeFromGroupContext
    ) -> PostRemoveNodeFromGroupContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def group(self) -> Group:
        """The group the node was removed from."""
        return self._py_context.group

    @property
    def node_index(self) -> NodeIndex:
        """The index of the node that was removed from the group."""
        return self._py_context.node_index


class PreRemoveEdgeFromGroupContext:
    """Context for the pre_remove_edge_from_group hook."""

    _py_context: PyPreRemoveEdgeFromGroupContext

    def __init__(self, group: Group, edge_index: EdgeIndex) -> None:
        """Initializes a PreRemoveEdgeFromGroupContext.

        Args:
            group (Group): The group to remove the edge from.
            edge_index (EdgeIndex): The index of the edge being removed from the group.
        """
        from graphrecords._graphrecords import PyPreRemoveEdgeFromGroupContext

        self._py_context = PyPreRemoveEdgeFromGroupContext(group, edge_index)

    @classmethod
    def _from_py_context(
        cls, py_context: PyPreRemoveEdgeFromGroupContext
    ) -> PreRemoveEdgeFromGroupContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def group(self) -> Group:
        """The group to remove the edge from."""
        return self._py_context.group

    @property
    def edge_index(self) -> EdgeIndex:
        """The index of the edge being removed from the group."""
        return self._py_context.edge_index


class PostRemoveEdgeFromGroupContext:
    """Context for the post_remove_edge_from_group hook."""

    _py_context: PyPostRemoveEdgeFromGroupContext

    def __init__(self, group: Group, edge_index: EdgeIndex) -> None:
        """Initializes a PostRemoveEdgeFromGroupContext.

        Args:
            group (Group): The group the edge was removed from.
            edge_index (EdgeIndex): The index of the edge that was removed from
                the group.
        """
        from graphrecords._graphrecords import PyPostRemoveEdgeFromGroupContext

        self._py_context = PyPostRemoveEdgeFromGroupContext(group, edge_index)

    @classmethod
    def _from_py_context(
        cls, py_context: PyPostRemoveEdgeFromGroupContext
    ) -> PostRemoveEdgeFromGroupContext:
        context = cls.__new__(cls)
        context._py_context = py_context
        return context

    @property
    def group(self) -> Group:
        """The group the edge was removed from."""
        return self._py_context.group

    @property
    def edge_index(self) -> EdgeIndex:
        """The index of the edge that was removed from the group."""
        return self._py_context.edge_index


class Plugin:
    """Base class for GraphRecord plugins.

    Subclass and override pre/post methods to hook into GraphRecord
    mutation operations. Pre-hooks can modify the context before the
    operation executes. Post-hooks run after the operation completes.
    """

    def initialize(self, graphrecord: GraphRecord) -> None:
        """Called when the plugin is registered to a GraphRecord.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
        """
        pass

    def pre_set_schema(
        self, graphrecord: GraphRecord, context: PreSetSchemaContext
    ) -> PreSetSchemaContext:
        """Called before setting the schema.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PreSetSchemaContext): The operation context.

        Returns:
            PreSetSchemaContext: The potentially modified context.
        """
        return context

    def post_set_schema(self, graphrecord: GraphRecord) -> None:
        """Called after setting the schema.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
        """
        pass

    def pre_freeze_schema(self, graphrecord: GraphRecord) -> None:
        """Called before freezing the schema.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
        """
        pass

    def post_freeze_schema(self, graphrecord: GraphRecord) -> None:
        """Called after freezing the schema.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
        """
        pass

    def pre_unfreeze_schema(self, graphrecord: GraphRecord) -> None:
        """Called before unfreezing the schema.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
        """
        pass

    def post_unfreeze_schema(self, graphrecord: GraphRecord) -> None:
        """Called after unfreezing the schema.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
        """
        pass

    def pre_add_node(
        self, graphrecord: GraphRecord, context: PreAddNodeContext
    ) -> PreAddNodeContext:
        """Called before adding a node.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PreAddNodeContext): The operation context.

        Returns:
            PreAddNodeContext: The potentially modified context.
        """
        return context

    def post_add_node(
        self, graphrecord: GraphRecord, context: PostAddNodeContext
    ) -> None:
        """Called after adding a node.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PostAddNodeContext): The operation context.
        """
        pass

    def pre_add_node_with_group(
        self, graphrecord: GraphRecord, context: PreAddNodeWithGroupContext
    ) -> PreAddNodeWithGroupContext:
        """Called before adding a node with a group.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PreAddNodeWithGroupContext): The operation context.

        Returns:
            PreAddNodeWithGroupContext: The potentially modified context.
        """
        return context

    def post_add_node_with_group(
        self, graphrecord: GraphRecord, context: PostAddNodeWithGroupContext
    ) -> None:
        """Called after adding a node with a group.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PostAddNodeWithGroupContext): The operation context.
        """
        pass

    def pre_remove_node(
        self, graphrecord: GraphRecord, context: PreRemoveNodeContext
    ) -> PreRemoveNodeContext:
        """Called before removing a node.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PreRemoveNodeContext): The operation context.

        Returns:
            PreRemoveNodeContext: The potentially modified context.
        """
        return context

    def post_remove_node(
        self, graphrecord: GraphRecord, context: PostRemoveNodeContext
    ) -> None:
        """Called after removing a node.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PostRemoveNodeContext): The operation context.
        """
        pass

    def pre_add_nodes(
        self, graphrecord: GraphRecord, context: PreAddNodesContext
    ) -> PreAddNodesContext:
        """Called before adding multiple nodes.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PreAddNodesContext): The operation context.

        Returns:
            PreAddNodesContext: The potentially modified context.
        """
        return context

    def post_add_nodes(
        self, graphrecord: GraphRecord, context: PostAddNodesContext
    ) -> None:
        """Called after adding multiple nodes.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PostAddNodesContext): The operation context.
        """
        pass

    def pre_add_nodes_with_group(
        self, graphrecord: GraphRecord, context: PreAddNodesWithGroupContext
    ) -> PreAddNodesWithGroupContext:
        """Called before adding multiple nodes with a group.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PreAddNodesWithGroupContext): The operation context.

        Returns:
            PreAddNodesWithGroupContext: The potentially modified context.
        """
        return context

    def post_add_nodes_with_group(
        self, graphrecord: GraphRecord, context: PostAddNodesWithGroupContext
    ) -> None:
        """Called after adding multiple nodes with a group.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PostAddNodesWithGroupContext): The operation context.
        """
        pass

    def pre_add_nodes_dataframes(
        self, graphrecord: GraphRecord, context: PreAddNodesDataframesContext
    ) -> PreAddNodesDataframesContext:
        """Called before adding nodes from dataframes.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PreAddNodesDataframesContext): The operation context.

        Returns:
            PreAddNodesDataframesContext: The potentially modified context.
        """
        return context

    def post_add_nodes_dataframes(
        self, graphrecord: GraphRecord, context: PostAddNodesDataframesContext
    ) -> None:
        """Called after adding nodes from dataframes.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PostAddNodesDataframesContext): The operation context.
        """
        pass

    def pre_add_nodes_dataframes_with_group(
        self,
        graphrecord: GraphRecord,
        context: PreAddNodesDataframesWithGroupContext,
    ) -> PreAddNodesDataframesWithGroupContext:
        """Called before adding nodes from dataframes with a group.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PreAddNodesDataframesWithGroupContext): The operation context.

        Returns:
            PreAddNodesDataframesWithGroupContext: The potentially modified context.
        """
        return context

    def post_add_nodes_dataframes_with_group(
        self,
        graphrecord: GraphRecord,
        context: PostAddNodesDataframesWithGroupContext,
    ) -> None:
        """Called after adding nodes from dataframes with a group.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PostAddNodesDataframesWithGroupContext): The operation context.
        """
        pass

    def pre_add_edge(
        self, graphrecord: GraphRecord, context: PreAddEdgeContext
    ) -> PreAddEdgeContext:
        """Called before adding an edge.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PreAddEdgeContext): The operation context.

        Returns:
            PreAddEdgeContext: The potentially modified context.
        """
        return context

    def post_add_edge(
        self, graphrecord: GraphRecord, context: PostAddEdgeContext
    ) -> None:
        """Called after adding an edge.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PostAddEdgeContext): The operation context.
        """
        pass

    def pre_add_edge_with_group(
        self, graphrecord: GraphRecord, context: PreAddEdgeWithGroupContext
    ) -> PreAddEdgeWithGroupContext:
        """Called before adding an edge with a group.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PreAddEdgeWithGroupContext): The operation context.

        Returns:
            PreAddEdgeWithGroupContext: The potentially modified context.
        """
        return context

    def post_add_edge_with_group(
        self, graphrecord: GraphRecord, context: PostAddEdgeWithGroupContext
    ) -> None:
        """Called after adding an edge with a group.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PostAddEdgeWithGroupContext): The operation context.
        """
        pass

    def pre_remove_edge(
        self, graphrecord: GraphRecord, context: PreRemoveEdgeContext
    ) -> PreRemoveEdgeContext:
        """Called before removing an edge.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PreRemoveEdgeContext): The operation context.

        Returns:
            PreRemoveEdgeContext: The potentially modified context.
        """
        return context

    def post_remove_edge(
        self, graphrecord: GraphRecord, context: PostRemoveEdgeContext
    ) -> None:
        """Called after removing an edge.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PostRemoveEdgeContext): The operation context.
        """
        pass

    def pre_add_edges(
        self, graphrecord: GraphRecord, context: PreAddEdgesContext
    ) -> PreAddEdgesContext:
        """Called before adding multiple edges.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PreAddEdgesContext): The operation context.

        Returns:
            PreAddEdgesContext: The potentially modified context.
        """
        return context

    def post_add_edges(
        self, graphrecord: GraphRecord, context: PostAddEdgesContext
    ) -> None:
        """Called after adding multiple edges.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PostAddEdgesContext): The operation context.
        """
        pass

    def pre_add_edges_with_group(
        self, graphrecord: GraphRecord, context: PreAddEdgesWithGroupContext
    ) -> PreAddEdgesWithGroupContext:
        """Called before adding multiple edges with a group.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PreAddEdgesWithGroupContext): The operation context.

        Returns:
            PreAddEdgesWithGroupContext: The potentially modified context.
        """
        return context

    def post_add_edges_with_group(
        self, graphrecord: GraphRecord, context: PostAddEdgesWithGroupContext
    ) -> None:
        """Called after adding multiple edges with a group.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PostAddEdgesWithGroupContext): The operation context.
        """
        pass

    def pre_add_edges_dataframes(
        self, graphrecord: GraphRecord, context: PreAddEdgesDataframesContext
    ) -> PreAddEdgesDataframesContext:
        """Called before adding edges from dataframes.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PreAddEdgesDataframesContext): The operation context.

        Returns:
            PreAddEdgesDataframesContext: The potentially modified context.
        """
        return context

    def post_add_edges_dataframes(
        self, graphrecord: GraphRecord, context: PostAddEdgesDataframesContext
    ) -> None:
        """Called after adding edges from dataframes.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PostAddEdgesDataframesContext): The operation context.
        """
        pass

    def pre_add_edges_dataframes_with_group(
        self,
        graphrecord: GraphRecord,
        context: PreAddEdgesDataframesWithGroupContext,
    ) -> PreAddEdgesDataframesWithGroupContext:
        """Called before adding edges from dataframes with a group.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PreAddEdgesDataframesWithGroupContext): The operation context.

        Returns:
            PreAddEdgesDataframesWithGroupContext: The potentially modified context.
        """
        return context

    def post_add_edges_dataframes_with_group(
        self,
        graphrecord: GraphRecord,
        context: PostAddEdgesDataframesWithGroupContext,
    ) -> None:
        """Called after adding edges from dataframes with a group.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PostAddEdgesDataframesWithGroupContext): The operation context.
        """
        pass

    def pre_add_group(
        self, graphrecord: GraphRecord, context: PreAddGroupContext
    ) -> PreAddGroupContext:
        """Called before adding a group.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PreAddGroupContext): The operation context.

        Returns:
            PreAddGroupContext: The potentially modified context.
        """
        return context

    def post_add_group(
        self, graphrecord: GraphRecord, context: PostAddGroupContext
    ) -> None:
        """Called after adding a group.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PostAddGroupContext): The operation context.
        """
        pass

    def pre_remove_group(
        self, graphrecord: GraphRecord, context: PreRemoveGroupContext
    ) -> PreRemoveGroupContext:
        """Called before removing a group.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PreRemoveGroupContext): The operation context.

        Returns:
            PreRemoveGroupContext: The potentially modified context.
        """
        return context

    def post_remove_group(
        self, graphrecord: GraphRecord, context: PostRemoveGroupContext
    ) -> None:
        """Called after removing a group.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PostRemoveGroupContext): The operation context.
        """
        pass

    def pre_add_node_to_group(
        self, graphrecord: GraphRecord, context: PreAddNodeToGroupContext
    ) -> PreAddNodeToGroupContext:
        """Called before adding a node to a group.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PreAddNodeToGroupContext): The operation context.

        Returns:
            PreAddNodeToGroupContext: The potentially modified context.
        """
        return context

    def post_add_node_to_group(
        self, graphrecord: GraphRecord, context: PostAddNodeToGroupContext
    ) -> None:
        """Called after adding a node to a group.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PostAddNodeToGroupContext): The operation context.
        """
        pass

    def pre_add_edge_to_group(
        self, graphrecord: GraphRecord, context: PreAddEdgeToGroupContext
    ) -> PreAddEdgeToGroupContext:
        """Called before adding an edge to a group.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PreAddEdgeToGroupContext): The operation context.

        Returns:
            PreAddEdgeToGroupContext: The potentially modified context.
        """
        return context

    def post_add_edge_to_group(
        self, graphrecord: GraphRecord, context: PostAddEdgeToGroupContext
    ) -> None:
        """Called after adding an edge to a group.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PostAddEdgeToGroupContext): The operation context.
        """
        pass

    def pre_remove_node_from_group(
        self, graphrecord: GraphRecord, context: PreRemoveNodeFromGroupContext
    ) -> PreRemoveNodeFromGroupContext:
        """Called before removing a node from a group.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PreRemoveNodeFromGroupContext): The operation context.

        Returns:
            PreRemoveNodeFromGroupContext: The potentially modified context.
        """
        return context

    def post_remove_node_from_group(
        self, graphrecord: GraphRecord, context: PostRemoveNodeFromGroupContext
    ) -> None:
        """Called after removing a node from a group.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PostRemoveNodeFromGroupContext): The operation context.
        """
        pass

    def pre_remove_edge_from_group(
        self, graphrecord: GraphRecord, context: PreRemoveEdgeFromGroupContext
    ) -> PreRemoveEdgeFromGroupContext:
        """Called before removing an edge from a group.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PreRemoveEdgeFromGroupContext): The operation context.

        Returns:
            PreRemoveEdgeFromGroupContext: The potentially modified context.
        """
        return context

    def post_remove_edge_from_group(
        self, graphrecord: GraphRecord, context: PostRemoveEdgeFromGroupContext
    ) -> None:
        """Called after removing an edge from a group.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (PostRemoveEdgeFromGroupContext): The operation context.
        """
        pass

    def pre_clear(self, graphrecord: GraphRecord) -> None:
        """Called before clearing the graphrecord.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
        """
        pass

    def post_clear(self, graphrecord: GraphRecord) -> None:
        """Called after clearing the graphrecord.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
        """
        pass
