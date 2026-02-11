"""Plugin system for hooking into GraphRecord mutation operations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, List, Optional, Sequence

if TYPE_CHECKING:
    from graphrecords.graphrecord import GraphRecord
    from graphrecords.schema import Schema
    from graphrecords.types import (
        Attributes,
        EdgeIndex,
        EdgeTuple,
        Group,
        NodeIndex,
        NodeTuple,
        PolarsEdgeDataFrameInput,
        PolarsNodeDataFrameInput,
    )


@dataclass
class AddNodesContext:
    """Context for the add_nodes hook."""

    nodes: Sequence[NodeTuple]
    group: Optional[Group]


@dataclass
class AddNodesPolarsContext:
    """Context for the add_nodes_polars hook."""

    nodes: List[PolarsNodeDataFrameInput]
    group: Optional[Group]


@dataclass
class AddEdgesContext:
    """Context for the add_edges hook."""

    edges: Sequence[EdgeTuple]
    group: Optional[Group]


@dataclass
class AddEdgesPolarsContext:
    """Context for the add_edges_polars hook."""

    edges: List[PolarsEdgeDataFrameInput]
    group: Optional[Group]


@dataclass
class RemoveNodesContext:
    """Context for the remove_nodes hook."""

    nodes: List[NodeIndex]


@dataclass
class RemoveEdgesContext:
    """Context for the remove_edges hook."""

    edges: List[EdgeIndex]


@dataclass
class AddGroupContext:
    """Context for the add_group hook."""

    group: Group
    nodes: Optional[List[NodeIndex]]
    edges: Optional[List[EdgeIndex]]


@dataclass
class RemoveGroupsContext:
    """Context for the remove_groups hook."""

    groups: List[Group]


@dataclass
class AddNodesToGroupContext:
    """Context for the add_nodes_to_group hook."""

    group: Group
    nodes: List[NodeIndex]


@dataclass
class AddEdgesToGroupContext:
    """Context for the add_edges_to_group hook."""

    group: Group
    edges: List[EdgeIndex]


@dataclass
class RemoveNodesFromGroupContext:
    """Context for the remove_nodes_from_group hook."""

    group: Group
    nodes: List[NodeIndex]


@dataclass
class RemoveEdgesFromGroupContext:
    """Context for the remove_edges_from_group hook."""

    group: Group
    edges: List[EdgeIndex]


@dataclass
class SetSchemaContext:
    """Context for the set_schema hook."""

    schema: Schema


class Plugin:
    """Base class for GraphRecord plugins.

    Subclass and override pre/post methods to hook into GraphRecord
    mutation operations. Pre-hooks can modify the context before the
    operation executes. Post-hooks run after the operation completes.
    """

    def pre_add_nodes(
        self, graphrecord: GraphRecord, context: AddNodesContext
    ) -> AddNodesContext:
        """Called before adding nodes.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (AddNodesContext): The operation context.

        Returns:
            AddNodesContext: The potentially modified context.
        """
        return context

    def post_add_nodes(
        self, graphrecord: GraphRecord, context: AddNodesContext
    ) -> None:
        """Called after adding nodes.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (AddNodesContext): The operation context.
        """

    def pre_add_nodes_polars(
        self, graphrecord: GraphRecord, context: AddNodesPolarsContext
    ) -> AddNodesPolarsContext:
        """Called before adding nodes via Polars.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (AddNodesPolarsContext): The operation context.

        Returns:
            AddNodesPolarsContext: The potentially modified context.
        """
        return context

    def post_add_nodes_polars(
        self, graphrecord: GraphRecord, context: AddNodesPolarsContext
    ) -> None:
        """Called after adding nodes via Polars.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (AddNodesPolarsContext): The operation context.
        """

    def pre_add_edges(
        self, graphrecord: GraphRecord, context: AddEdgesContext
    ) -> AddEdgesContext:
        """Called before adding edges.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (AddEdgesContext): The operation context.

        Returns:
            AddEdgesContext: The potentially modified context.
        """
        return context

    def post_add_edges(
        self,
        graphrecord: GraphRecord,
        context: AddEdgesContext,
        result: List[EdgeIndex],
    ) -> None:
        """Called after adding edges.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (AddEdgesContext): The operation context.
            result (List[EdgeIndex]): The edge indices that were added.
        """

    def pre_add_edges_polars(
        self, graphrecord: GraphRecord, context: AddEdgesPolarsContext
    ) -> AddEdgesPolarsContext:
        """Called before adding edges via Polars.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (AddEdgesPolarsContext): The operation context.

        Returns:
            AddEdgesPolarsContext: The potentially modified context.
        """
        return context

    def post_add_edges_polars(
        self,
        graphrecord: GraphRecord,
        context: AddEdgesPolarsContext,
        result: List[EdgeIndex],
    ) -> None:
        """Called after adding edges via Polars.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (AddEdgesPolarsContext): The operation context.
            result (List[EdgeIndex]): The edge indices that were added.
        """

    def pre_remove_nodes(
        self, graphrecord: GraphRecord, context: RemoveNodesContext
    ) -> RemoveNodesContext:
        """Called before removing nodes.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (RemoveNodesContext): The operation context.

        Returns:
            RemoveNodesContext: The potentially modified context.
        """
        return context

    def post_remove_nodes(
        self,
        graphrecord: GraphRecord,
        context: RemoveNodesContext,
        result: Dict[NodeIndex, Attributes],
    ) -> None:
        """Called after removing nodes.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (RemoveNodesContext): The operation context.
            result (Dict[NodeIndex, Attributes]): The removed nodes.
        """

    def pre_remove_edges(
        self, graphrecord: GraphRecord, context: RemoveEdgesContext
    ) -> RemoveEdgesContext:
        """Called before removing edges.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (RemoveEdgesContext): The operation context.

        Returns:
            RemoveEdgesContext: The potentially modified context.
        """
        return context

    def post_remove_edges(
        self,
        graphrecord: GraphRecord,
        context: RemoveEdgesContext,
        result: Dict[EdgeIndex, Attributes],
    ) -> None:
        """Called after removing edges.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (RemoveEdgesContext): The operation context.
            result (Dict[EdgeIndex, Attributes]): The removed edges.
        """

    def pre_add_group(
        self, graphrecord: GraphRecord, context: AddGroupContext
    ) -> AddGroupContext:
        """Called before adding a group.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (AddGroupContext): The operation context.

        Returns:
            AddGroupContext: The potentially modified context.
        """
        return context

    def post_add_group(
        self, graphrecord: GraphRecord, context: AddGroupContext
    ) -> None:
        """Called after adding a group.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (AddGroupContext): The operation context.
        """

    def pre_remove_groups(
        self, graphrecord: GraphRecord, context: RemoveGroupsContext
    ) -> RemoveGroupsContext:
        """Called before removing groups.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (RemoveGroupsContext): The operation context.

        Returns:
            RemoveGroupsContext: The potentially modified context.
        """
        return context

    def post_remove_groups(
        self, graphrecord: GraphRecord, context: RemoveGroupsContext
    ) -> None:
        """Called after removing groups.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (RemoveGroupsContext): The operation context.
        """

    def pre_add_nodes_to_group(
        self, graphrecord: GraphRecord, context: AddNodesToGroupContext
    ) -> AddNodesToGroupContext:
        """Called before adding nodes to a group.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (AddNodesToGroupContext): The operation context.

        Returns:
            AddNodesToGroupContext: The potentially modified context.
        """
        return context

    def post_add_nodes_to_group(
        self, graphrecord: GraphRecord, context: AddNodesToGroupContext
    ) -> None:
        """Called after adding nodes to a group.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (AddNodesToGroupContext): The operation context.
        """

    def pre_add_edges_to_group(
        self, graphrecord: GraphRecord, context: AddEdgesToGroupContext
    ) -> AddEdgesToGroupContext:
        """Called before adding edges to a group.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (AddEdgesToGroupContext): The operation context.

        Returns:
            AddEdgesToGroupContext: The potentially modified context.
        """
        return context

    def post_add_edges_to_group(
        self, graphrecord: GraphRecord, context: AddEdgesToGroupContext
    ) -> None:
        """Called after adding edges to a group.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (AddEdgesToGroupContext): The operation context.
        """

    def pre_remove_nodes_from_group(
        self, graphrecord: GraphRecord, context: RemoveNodesFromGroupContext
    ) -> RemoveNodesFromGroupContext:
        """Called before removing nodes from a group.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (RemoveNodesFromGroupContext): The operation context.

        Returns:
            RemoveNodesFromGroupContext: The potentially modified context.
        """
        return context

    def post_remove_nodes_from_group(
        self, graphrecord: GraphRecord, context: RemoveNodesFromGroupContext
    ) -> None:
        """Called after removing nodes from a group.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (RemoveNodesFromGroupContext): The operation context.
        """

    def pre_remove_edges_from_group(
        self, graphrecord: GraphRecord, context: RemoveEdgesFromGroupContext
    ) -> RemoveEdgesFromGroupContext:
        """Called before removing edges from a group.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (RemoveEdgesFromGroupContext): The operation context.

        Returns:
            RemoveEdgesFromGroupContext: The potentially modified context.
        """
        return context

    def post_remove_edges_from_group(
        self, graphrecord: GraphRecord, context: RemoveEdgesFromGroupContext
    ) -> None:
        """Called after removing edges from a group.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (RemoveEdgesFromGroupContext): The operation context.
        """

    def pre_clear(self, graphrecord: GraphRecord) -> None:
        """Called before clearing the GraphRecord.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
        """

    def post_clear(self, graphrecord: GraphRecord) -> None:
        """Called after clearing the GraphRecord.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
        """

    def pre_set_schema(
        self, graphrecord: GraphRecord, context: SetSchemaContext
    ) -> SetSchemaContext:
        """Called before setting the schema.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (SetSchemaContext): The operation context.

        Returns:
            SetSchemaContext: The potentially modified context.
        """
        return context

    def post_set_schema(
        self, graphrecord: GraphRecord, context: SetSchemaContext
    ) -> None:
        """Called after setting the schema.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
            context (SetSchemaContext): The operation context.
        """

    def pre_freeze_schema(self, graphrecord: GraphRecord) -> None:
        """Called before freezing the schema.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
        """

    def post_freeze_schema(self, graphrecord: GraphRecord) -> None:
        """Called after freezing the schema.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
        """

    def pre_unfreeze_schema(self, graphrecord: GraphRecord) -> None:
        """Called before unfreezing the schema.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
        """

    def post_unfreeze_schema(self, graphrecord: GraphRecord) -> None:
        """Called after unfreezing the schema.

        Args:
            graphrecord (GraphRecord): The GraphRecord instance.
        """
