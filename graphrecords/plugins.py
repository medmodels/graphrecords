"""Plugin authoring surface for the graphrecords library."""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    Final,
    FrozenSet,
    List,
    Optional,
    Tuple,
    TypeAlias,
    Union,
)

from graphrecords._graphrecords.plugins import (
    PyAddEdges,
    PyAddEdgesInGroup,
    PyAddEdgesToGroup,
    PyAddGroup,
    PyAddNodes,
    PyAddNodesInGroup,
    PyAddNodesToGroup,
    PyClear,
    PyEdgeBatch,
    PyFreezeSchema,
    PyNodeBatch,
    PyRemoveEdgeAttributes,
    PyRemoveEdges,
    PyRemoveEdgesFromGroup,
    PyRemoveGroups,
    PyRemoveNodeAttributes,
    PyRemoveNodes,
    PyRemoveNodesFromGroup,
    PyReplaceEdgeAttributes,
    PyReplaceNodeAttributes,
    PySetEdgeAttributes,
    PySetNodeAttributes,
    PySetSchema,
    PyUnfreezeSchema,
)
from graphrecords.schema import Schema
from graphrecords.types import EdgeIndex

if TYPE_CHECKING:
    from graphrecords._graphrecords.graphrecord import PyGraphRecord
    from graphrecords._graphrecords.plugins import (
        PyEdgeBatchIterator,
        PyNodeBatchIterator,
    )
    from graphrecords.graphrecord import GraphRecord
    from graphrecords.types import (
        AttributeName,
        Attributes,
        EdgeSource,
        GroupIndex,
        NodeIndex,
        NodeSource,
        Value,
    )


class NodeBatchIterator:
    """An iterator over the nodes of a batch."""

    _py_node_batch_iterator: PyNodeBatchIterator

    @classmethod
    def _from_py_node_batch_iterator(
        cls, py_node_batch_iterator: PyNodeBatchIterator
    ) -> NodeBatchIterator:
        """Creates a NodeBatchIterator from a PyNodeBatchIterator.

        Args:
            py_node_batch_iterator (PyNodeBatchIterator): The PyNodeBatchIterator to
                convert.

        Returns:
            NodeBatchIterator: The converted NodeBatchIterator.
        """
        node_batch_iterator = cls.__new__(cls)
        node_batch_iterator._py_node_batch_iterator = py_node_batch_iterator
        return node_batch_iterator

    def __iter__(self) -> NodeBatchIterator:
        """Returns the iterator itself.

        Returns:
            NodeBatchIterator: The iterator itself.
        """
        return self

    def __next__(self) -> Tuple[NodeIndex, Attributes]:
        """Returns the next node of the batch.

        Returns:
            Tuple[NodeIndex, Attributes]: The index and attributes of the node.
        """
        return next(self._py_node_batch_iterator)


class NodeBatch:
    """A batch of nodes carried by a change payload."""

    _py_node_batch: PyNodeBatch

    def __init__(self, nodes: NodeSource) -> None:
        """Initializes a batch of nodes.

        Args:
            nodes (NodeSource): The nodes of the batch.
        """
        self._py_node_batch = PyNodeBatch(nodes)

    @classmethod
    def _from_py_node_batch(cls, py_node_batch: PyNodeBatch) -> NodeBatch:
        """Creates a NodeBatch from a PyNodeBatch.

        Args:
            py_node_batch (PyNodeBatch): The PyNodeBatch to convert.

        Returns:
            NodeBatch: The converted NodeBatch.
        """
        node_batch = cls.__new__(cls)
        node_batch._py_node_batch = py_node_batch
        return node_batch

    def is_empty(self) -> bool:
        """Checks whether the batch holds no nodes.

        Returns:
            bool: True if the batch is empty, otherwise False.
        """
        return self._py_node_batch.is_empty()

    def attribute_values(
        self, attribute_name: AttributeName
    ) -> List[Tuple[NodeIndex, Value]]:
        """Collects the value of one attribute across the nodes that carry it.

        Args:
            attribute_name (AttributeName): The name of the attribute to read.

        Returns:
            List[Tuple[NodeIndex, Value]]: The index and value of every node that
                carries the attribute.
        """
        return self._py_node_batch.attribute_values(attribute_name)

    def __len__(self) -> int:
        """Counts the nodes of the batch.

        Returns:
            int: The number of nodes.
        """
        return len(self._py_node_batch)

    def __iter__(self) -> NodeBatchIterator:
        """Iterates over the nodes of the batch.

        Returns:
            NodeBatchIterator: An iterator over the nodes.
        """
        return NodeBatchIterator._from_py_node_batch_iterator(iter(self._py_node_batch))


class EdgeBatchIterator:
    """An iterator over the edges of a batch."""

    _py_edge_batch_iterator: PyEdgeBatchIterator

    @classmethod
    def _from_py_edge_batch_iterator(
        cls, py_edge_batch_iterator: PyEdgeBatchIterator
    ) -> EdgeBatchIterator:
        """Creates an EdgeBatchIterator from a PyEdgeBatchIterator.

        Args:
            py_edge_batch_iterator (PyEdgeBatchIterator): The PyEdgeBatchIterator to
                convert.

        Returns:
            EdgeBatchIterator: The converted EdgeBatchIterator.
        """
        edge_batch_iterator = cls.__new__(cls)
        edge_batch_iterator._py_edge_batch_iterator = py_edge_batch_iterator
        return edge_batch_iterator

    def __iter__(self) -> EdgeBatchIterator:
        """Returns the iterator itself.

        Returns:
            EdgeBatchIterator: The iterator itself.
        """
        return self

    def __next__(self) -> Tuple[NodeIndex, NodeIndex, Attributes]:
        """Returns the next edge of the batch.

        Returns:
            Tuple[NodeIndex, NodeIndex, Attributes]: The source node index, target
                node index and attributes of the edge.
        """
        return next(self._py_edge_batch_iterator)


class EdgeBatch:
    """A batch of edges carried by a change payload."""

    _py_edge_batch: PyEdgeBatch

    def __init__(self, edges: EdgeSource) -> None:
        """Initializes a batch of edges.

        Args:
            edges (EdgeSource): The edges of the batch.
        """
        self._py_edge_batch = PyEdgeBatch(edges)

    @classmethod
    def _from_py_edge_batch(cls, py_edge_batch: PyEdgeBatch) -> EdgeBatch:
        """Creates an EdgeBatch from a PyEdgeBatch.

        Args:
            py_edge_batch (PyEdgeBatch): The PyEdgeBatch to convert.

        Returns:
            EdgeBatch: The converted EdgeBatch.
        """
        edge_batch = cls.__new__(cls)
        edge_batch._py_edge_batch = py_edge_batch
        return edge_batch

    def is_empty(self) -> bool:
        """Checks whether the batch holds no edges.

        Returns:
            bool: True if the batch is empty, otherwise False.
        """
        return self._py_edge_batch.is_empty()

    def attribute_values(
        self, attribute_name: AttributeName
    ) -> List[Tuple[NodeIndex, NodeIndex, Value]]:
        """Collects the value of one attribute across the edges that carry it.

        Args:
            attribute_name (AttributeName): The name of the attribute to read.

        Returns:
            List[Tuple[NodeIndex, NodeIndex, Value]]: The source node index, target
                node index and value of every edge that carries the attribute.
        """
        return self._py_edge_batch.attribute_values(attribute_name)

    def __len__(self) -> int:
        """Counts the edges of the batch.

        Returns:
            int: The number of edges.
        """
        return len(self._py_edge_batch)

    def __iter__(self) -> EdgeBatchIterator:
        """Iterates over the edges of the batch.

        Returns:
            EdgeBatchIterator: An iterator over the edges.
        """
        return EdgeBatchIterator._from_py_edge_batch_iterator(iter(self._py_edge_batch))


class AddNodes:
    """The payload of a change adding nodes."""

    _py_add_nodes: PyAddNodes

    def __init__(self, batch: NodeBatch) -> None:
        """Initializes the payload of a change adding nodes.

        Args:
            batch (NodeBatch): The nodes that are added.
        """
        self._py_add_nodes = PyAddNodes(batch._py_node_batch)

    @classmethod
    def _from_py_add_nodes(cls, py_add_nodes: PyAddNodes) -> AddNodes:
        """Creates an AddNodes from a PyAddNodes.

        Args:
            py_add_nodes (PyAddNodes): The PyAddNodes to convert.

        Returns:
            AddNodes: The converted AddNodes.
        """
        add_nodes = cls.__new__(cls)
        add_nodes._py_add_nodes = py_add_nodes
        return add_nodes

    @property
    def batch(self) -> NodeBatch:
        """The nodes that are added.

        Returns:
            NodeBatch: The nodes that are added.
        """
        return NodeBatch._from_py_node_batch(self._py_add_nodes.batch)


class AddNodesInGroup:
    """The payload of a change adding nodes in a group."""

    _py_add_nodes_in_group: PyAddNodesInGroup

    def __init__(self, batch: NodeBatch, group_index: GroupIndex) -> None:
        """Initializes the payload of a change adding nodes in a group.

        Args:
            batch (NodeBatch): The nodes that are added.
            group_index (GroupIndex): The group the nodes are added in.
        """
        self._py_add_nodes_in_group = PyAddNodesInGroup(
            batch._py_node_batch, group_index
        )

    @classmethod
    def _from_py_add_nodes_in_group(
        cls, py_add_nodes_in_group: PyAddNodesInGroup
    ) -> AddNodesInGroup:
        """Creates an AddNodesInGroup from a PyAddNodesInGroup.

        Args:
            py_add_nodes_in_group (PyAddNodesInGroup): The PyAddNodesInGroup to convert.

        Returns:
            AddNodesInGroup: The converted AddNodesInGroup.
        """
        add_nodes_in_group = cls.__new__(cls)
        add_nodes_in_group._py_add_nodes_in_group = py_add_nodes_in_group
        return add_nodes_in_group

    @property
    def batch(self) -> NodeBatch:
        """The nodes that are added.

        Returns:
            NodeBatch: The nodes that are added.
        """
        return NodeBatch._from_py_node_batch(self._py_add_nodes_in_group.batch)

    @property
    def group_index(self) -> GroupIndex:
        """The group the nodes are added in.

        Returns:
            GroupIndex: The group the nodes are added in.
        """
        return self._py_add_nodes_in_group.group_index


class AddEdges:
    """The payload of a change adding edges."""

    _py_add_edges: PyAddEdges

    def __init__(self, batch: EdgeBatch) -> None:
        """Initializes the payload of a change adding edges.

        Args:
            batch (EdgeBatch): The edges that are added.
        """
        self._py_add_edges = PyAddEdges(batch._py_edge_batch)

    @classmethod
    def _from_py_add_edges(cls, py_add_edges: PyAddEdges) -> AddEdges:
        """Creates an AddEdges from a PyAddEdges.

        Args:
            py_add_edges (PyAddEdges): The PyAddEdges to convert.

        Returns:
            AddEdges: The converted AddEdges.
        """
        add_edges = cls.__new__(cls)
        add_edges._py_add_edges = py_add_edges
        return add_edges

    @property
    def batch(self) -> EdgeBatch:
        """The edges that are added.

        Returns:
            EdgeBatch: The edges that are added.
        """
        return EdgeBatch._from_py_edge_batch(self._py_add_edges.batch)


class AddEdgesInGroup:
    """The payload of a change adding edges in a group."""

    _py_add_edges_in_group: PyAddEdgesInGroup

    def __init__(self, batch: EdgeBatch, group_index: GroupIndex) -> None:
        """Initializes the payload of a change adding edges in a group.

        Args:
            batch (EdgeBatch): The edges that are added.
            group_index (GroupIndex): The group the edges are added in.
        """
        self._py_add_edges_in_group = PyAddEdgesInGroup(
            batch._py_edge_batch, group_index
        )

    @classmethod
    def _from_py_add_edges_in_group(
        cls, py_add_edges_in_group: PyAddEdgesInGroup
    ) -> AddEdgesInGroup:
        """Creates an AddEdgesInGroup from a PyAddEdgesInGroup.

        Args:
            py_add_edges_in_group (PyAddEdgesInGroup): The PyAddEdgesInGroup to convert.

        Returns:
            AddEdgesInGroup: The converted AddEdgesInGroup.
        """
        add_edges_in_group = cls.__new__(cls)
        add_edges_in_group._py_add_edges_in_group = py_add_edges_in_group
        return add_edges_in_group

    @property
    def batch(self) -> EdgeBatch:
        """The edges that are added.

        Returns:
            EdgeBatch: The edges that are added.
        """
        return EdgeBatch._from_py_edge_batch(self._py_add_edges_in_group.batch)

    @property
    def group_index(self) -> GroupIndex:
        """The group the edges are added in.

        Returns:
            GroupIndex: The group the edges are added in.
        """
        return self._py_add_edges_in_group.group_index


class RemoveNodes:
    """The payload of a change removing nodes."""

    _py_remove_nodes: PyRemoveNodes

    def __init__(self, node_indices: List[NodeIndex]) -> None:
        """Initializes the payload of a change removing nodes.

        Args:
            node_indices (List[NodeIndex]): The nodes that are removed.
        """
        self._py_remove_nodes = PyRemoveNodes(node_indices)

    @classmethod
    def _from_py_remove_nodes(cls, py_remove_nodes: PyRemoveNodes) -> RemoveNodes:
        """Creates a RemoveNodes from a PyRemoveNodes.

        Args:
            py_remove_nodes (PyRemoveNodes): The PyRemoveNodes to convert.

        Returns:
            RemoveNodes: The converted RemoveNodes.
        """
        remove_nodes = cls.__new__(cls)
        remove_nodes._py_remove_nodes = py_remove_nodes
        return remove_nodes

    @property
    def node_indices(self) -> List[NodeIndex]:
        """The nodes that are removed.

        Returns:
            List[NodeIndex]: The nodes that are removed.
        """
        return self._py_remove_nodes.node_indices


class RemoveEdges:
    """The payload of a change removing edges."""

    _py_remove_edges: PyRemoveEdges

    def __init__(self, edge_indices: List[EdgeIndex]) -> None:
        """Initializes the payload of a change removing edges.

        Args:
            edge_indices (List[EdgeIndex]): The edges that are removed.
        """
        self._py_remove_edges = PyRemoveEdges(
            [edge_index._py_edge_index for edge_index in edge_indices]
        )

    @classmethod
    def _from_py_remove_edges(cls, py_remove_edges: PyRemoveEdges) -> RemoveEdges:
        """Creates a RemoveEdges from a PyRemoveEdges.

        Args:
            py_remove_edges (PyRemoveEdges): The PyRemoveEdges to convert.

        Returns:
            RemoveEdges: The converted RemoveEdges.
        """
        remove_edges = cls.__new__(cls)
        remove_edges._py_remove_edges = py_remove_edges
        return remove_edges

    @property
    def edge_indices(self) -> List[EdgeIndex]:
        """The edges that are removed.

        Returns:
            List[EdgeIndex]: The edges that are removed.
        """
        return [
            EdgeIndex._from_py_edge_index(edge_index)
            for edge_index in self._py_remove_edges.edge_indices
        ]


class SetNodeAttributes:
    """The payload of a change setting node attributes."""

    _py_set_node_attributes: PySetNodeAttributes

    def __init__(self, node_indices: List[NodeIndex], attributes: Attributes) -> None:
        """Initializes the payload of a change setting node attributes.

        Args:
            node_indices (List[NodeIndex]): The nodes the attributes are set on.
            attributes (Attributes): The attributes that are set.
        """
        self._py_set_node_attributes = PySetNodeAttributes(node_indices, attributes)

    @classmethod
    def _from_py_set_node_attributes(
        cls, py_set_node_attributes: PySetNodeAttributes
    ) -> SetNodeAttributes:
        """Creates a SetNodeAttributes from a PySetNodeAttributes.

        Args:
            py_set_node_attributes (PySetNodeAttributes): The PySetNodeAttributes to
                convert.

        Returns:
            SetNodeAttributes: The converted SetNodeAttributes.
        """
        set_node_attributes = cls.__new__(cls)
        set_node_attributes._py_set_node_attributes = py_set_node_attributes
        return set_node_attributes

    @property
    def node_indices(self) -> List[NodeIndex]:
        """The nodes the attributes are set on.

        Returns:
            List[NodeIndex]: The nodes the attributes are set on.
        """
        return self._py_set_node_attributes.node_indices

    @property
    def attributes(self) -> Attributes:
        """The attributes that are set.

        Returns:
            Attributes: The attributes that are set.
        """
        return self._py_set_node_attributes.attributes


class ReplaceNodeAttributes:
    """The payload of a change replacing node attributes."""

    _py_replace_node_attributes: PyReplaceNodeAttributes

    def __init__(self, node_indices: List[NodeIndex], attributes: Attributes) -> None:
        """Initializes the payload of a change replacing node attributes.

        Args:
            node_indices (List[NodeIndex]): The nodes the attributes are replaced on.
            attributes (Attributes): The attributes the nodes end up with.
        """
        self._py_replace_node_attributes = PyReplaceNodeAttributes(
            node_indices, attributes
        )

    @classmethod
    def _from_py_replace_node_attributes(
        cls, py_replace_node_attributes: PyReplaceNodeAttributes
    ) -> ReplaceNodeAttributes:
        """Creates a ReplaceNodeAttributes from a PyReplaceNodeAttributes.

        Args:
            py_replace_node_attributes (PyReplaceNodeAttributes): The
                PyReplaceNodeAttributes to convert.

        Returns:
            ReplaceNodeAttributes: The converted ReplaceNodeAttributes.
        """
        replace_node_attributes = cls.__new__(cls)
        replace_node_attributes._py_replace_node_attributes = py_replace_node_attributes
        return replace_node_attributes

    @property
    def node_indices(self) -> List[NodeIndex]:
        """The nodes the attributes are replaced on.

        Returns:
            List[NodeIndex]: The nodes the attributes are replaced on.
        """
        return self._py_replace_node_attributes.node_indices

    @property
    def attributes(self) -> Attributes:
        """The attributes the nodes end up with.

        Returns:
            Attributes: The attributes the nodes end up with.
        """
        return self._py_replace_node_attributes.attributes


class RemoveNodeAttributes:
    """The payload of a change removing node attributes."""

    _py_remove_node_attributes: PyRemoveNodeAttributes

    def __init__(
        self, node_indices: List[NodeIndex], attribute_names: List[AttributeName]
    ) -> None:
        """Initializes the payload of a change removing node attributes.

        Args:
            node_indices (List[NodeIndex]): The nodes the attributes are removed from.
            attribute_names (List[AttributeName]): The names of the removed attributes.
        """
        self._py_remove_node_attributes = PyRemoveNodeAttributes(
            node_indices, attribute_names
        )

    @classmethod
    def _from_py_remove_node_attributes(
        cls, py_remove_node_attributes: PyRemoveNodeAttributes
    ) -> RemoveNodeAttributes:
        """Creates a RemoveNodeAttributes from a PyRemoveNodeAttributes.

        Args:
            py_remove_node_attributes (PyRemoveNodeAttributes): The
                PyRemoveNodeAttributes
                to convert.

        Returns:
            RemoveNodeAttributes: The converted RemoveNodeAttributes.
        """
        remove_node_attributes = cls.__new__(cls)
        remove_node_attributes._py_remove_node_attributes = py_remove_node_attributes
        return remove_node_attributes

    @property
    def node_indices(self) -> List[NodeIndex]:
        """The nodes the attributes are removed from.

        Returns:
            List[NodeIndex]: The nodes the attributes are removed from.
        """
        return self._py_remove_node_attributes.node_indices

    @property
    def attribute_names(self) -> List[AttributeName]:
        """The names of the removed attributes.

        Returns:
            List[AttributeName]: The names of the removed attributes.
        """
        return self._py_remove_node_attributes.attribute_names


class SetEdgeAttributes:
    """The payload of a change setting edge attributes."""

    _py_set_edge_attributes: PySetEdgeAttributes

    def __init__(self, edge_indices: List[EdgeIndex], attributes: Attributes) -> None:
        """Initializes the payload of a change setting edge attributes.

        Args:
            edge_indices (List[EdgeIndex]): The edges the attributes are set on.
            attributes (Attributes): The attributes that are set.
        """
        self._py_set_edge_attributes = PySetEdgeAttributes(
            [edge_index._py_edge_index for edge_index in edge_indices], attributes
        )

    @classmethod
    def _from_py_set_edge_attributes(
        cls, py_set_edge_attributes: PySetEdgeAttributes
    ) -> SetEdgeAttributes:
        """Creates a SetEdgeAttributes from a PySetEdgeAttributes.

        Args:
            py_set_edge_attributes (PySetEdgeAttributes): The PySetEdgeAttributes to
                convert.

        Returns:
            SetEdgeAttributes: The converted SetEdgeAttributes.
        """
        set_edge_attributes = cls.__new__(cls)
        set_edge_attributes._py_set_edge_attributes = py_set_edge_attributes
        return set_edge_attributes

    @property
    def edge_indices(self) -> List[EdgeIndex]:
        """The edges the attributes are set on.

        Returns:
            List[EdgeIndex]: The edges the attributes are set on.
        """
        return [
            EdgeIndex._from_py_edge_index(edge_index)
            for edge_index in self._py_set_edge_attributes.edge_indices
        ]

    @property
    def attributes(self) -> Attributes:
        """The attributes that are set.

        Returns:
            Attributes: The attributes that are set.
        """
        return self._py_set_edge_attributes.attributes


class ReplaceEdgeAttributes:
    """The payload of a change replacing edge attributes."""

    _py_replace_edge_attributes: PyReplaceEdgeAttributes

    def __init__(self, edge_indices: List[EdgeIndex], attributes: Attributes) -> None:
        """Initializes the payload of a change replacing edge attributes.

        Args:
            edge_indices (List[EdgeIndex]): The edges the attributes are replaced on.
            attributes (Attributes): The attributes the edges end up with.
        """
        self._py_replace_edge_attributes = PyReplaceEdgeAttributes(
            [edge_index._py_edge_index for edge_index in edge_indices], attributes
        )

    @classmethod
    def _from_py_replace_edge_attributes(
        cls, py_replace_edge_attributes: PyReplaceEdgeAttributes
    ) -> ReplaceEdgeAttributes:
        """Creates a ReplaceEdgeAttributes from a PyReplaceEdgeAttributes.

        Args:
            py_replace_edge_attributes (PyReplaceEdgeAttributes): The
                PyReplaceEdgeAttributes to convert.

        Returns:
            ReplaceEdgeAttributes: The converted ReplaceEdgeAttributes.
        """
        replace_edge_attributes = cls.__new__(cls)
        replace_edge_attributes._py_replace_edge_attributes = py_replace_edge_attributes
        return replace_edge_attributes

    @property
    def edge_indices(self) -> List[EdgeIndex]:
        """The edges the attributes are replaced on.

        Returns:
            List[EdgeIndex]: The edges the attributes are replaced on.
        """
        return [
            EdgeIndex._from_py_edge_index(edge_index)
            for edge_index in self._py_replace_edge_attributes.edge_indices
        ]

    @property
    def attributes(self) -> Attributes:
        """The attributes the edges end up with.

        Returns:
            Attributes: The attributes the edges end up with.
        """
        return self._py_replace_edge_attributes.attributes


class RemoveEdgeAttributes:
    """The payload of a change removing edge attributes."""

    _py_remove_edge_attributes: PyRemoveEdgeAttributes

    def __init__(
        self, edge_indices: List[EdgeIndex], attribute_names: List[AttributeName]
    ) -> None:
        """Initializes the payload of a change removing edge attributes.

        Args:
            edge_indices (List[EdgeIndex]): The edges the attributes are removed from.
            attribute_names (List[AttributeName]): The names of the removed attributes.
        """
        self._py_remove_edge_attributes = PyRemoveEdgeAttributes(
            [edge_index._py_edge_index for edge_index in edge_indices], attribute_names
        )

    @classmethod
    def _from_py_remove_edge_attributes(
        cls, py_remove_edge_attributes: PyRemoveEdgeAttributes
    ) -> RemoveEdgeAttributes:
        """Creates a RemoveEdgeAttributes from a PyRemoveEdgeAttributes.

        Args:
            py_remove_edge_attributes (PyRemoveEdgeAttributes): The
                PyRemoveEdgeAttributes
                to convert.

        Returns:
            RemoveEdgeAttributes: The converted RemoveEdgeAttributes.
        """
        remove_edge_attributes = cls.__new__(cls)
        remove_edge_attributes._py_remove_edge_attributes = py_remove_edge_attributes
        return remove_edge_attributes

    @property
    def edge_indices(self) -> List[EdgeIndex]:
        """The edges the attributes are removed from.

        Returns:
            List[EdgeIndex]: The edges the attributes are removed from.
        """
        return [
            EdgeIndex._from_py_edge_index(edge_index)
            for edge_index in self._py_remove_edge_attributes.edge_indices
        ]

    @property
    def attribute_names(self) -> List[AttributeName]:
        """The names of the removed attributes.

        Returns:
            List[AttributeName]: The names of the removed attributes.
        """
        return self._py_remove_edge_attributes.attribute_names


class AddGroup:
    """The payload of a change adding a group."""

    _py_add_group: PyAddGroup

    def __init__(self, group_index: GroupIndex) -> None:
        """Initializes the payload of a change adding a group.

        Args:
            group_index (GroupIndex): The group that is added.
        """
        self._py_add_group = PyAddGroup(group_index)

    @classmethod
    def _from_py_add_group(cls, py_add_group: PyAddGroup) -> AddGroup:
        """Creates an AddGroup from a PyAddGroup.

        Args:
            py_add_group (PyAddGroup): The PyAddGroup to convert.

        Returns:
            AddGroup: The converted AddGroup.
        """
        add_group = cls.__new__(cls)
        add_group._py_add_group = py_add_group
        return add_group

    @property
    def group_index(self) -> GroupIndex:
        """The group that is added.

        Returns:
            GroupIndex: The group that is added.
        """
        return self._py_add_group.group_index


class RemoveGroups:
    """The payload of a change removing groups."""

    _py_remove_groups: PyRemoveGroups

    def __init__(self, group_indices: List[GroupIndex]) -> None:
        """Initializes the payload of a change removing groups.

        Args:
            group_indices (List[GroupIndex]): The groups that are removed.
        """
        self._py_remove_groups = PyRemoveGroups(group_indices)

    @classmethod
    def _from_py_remove_groups(cls, py_remove_groups: PyRemoveGroups) -> RemoveGroups:
        """Creates a RemoveGroups from a PyRemoveGroups.

        Args:
            py_remove_groups (PyRemoveGroups): The PyRemoveGroups to convert.

        Returns:
            RemoveGroups: The converted RemoveGroups.
        """
        remove_groups = cls.__new__(cls)
        remove_groups._py_remove_groups = py_remove_groups
        return remove_groups

    @property
    def group_indices(self) -> List[GroupIndex]:
        """The groups that are removed.

        Returns:
            List[GroupIndex]: The groups that are removed.
        """
        return self._py_remove_groups.group_indices


class AddNodesToGroup:
    """The payload of a change adding nodes to a group."""

    _py_add_nodes_to_group: PyAddNodesToGroup

    def __init__(self, node_indices: List[NodeIndex], group_index: GroupIndex) -> None:
        """Initializes the payload of a change adding nodes to a group.

        Args:
            node_indices (List[NodeIndex]): The nodes that are added.
            group_index (GroupIndex): The group the nodes join.
        """
        self._py_add_nodes_to_group = PyAddNodesToGroup(node_indices, group_index)

    @classmethod
    def _from_py_add_nodes_to_group(
        cls, py_add_nodes_to_group: PyAddNodesToGroup
    ) -> AddNodesToGroup:
        """Creates an AddNodesToGroup from a PyAddNodesToGroup.

        Args:
            py_add_nodes_to_group (PyAddNodesToGroup): The PyAddNodesToGroup to convert.

        Returns:
            AddNodesToGroup: The converted AddNodesToGroup.
        """
        add_nodes_to_group = cls.__new__(cls)
        add_nodes_to_group._py_add_nodes_to_group = py_add_nodes_to_group
        return add_nodes_to_group

    @property
    def node_indices(self) -> List[NodeIndex]:
        """The nodes that are added.

        Returns:
            List[NodeIndex]: The nodes that are added.
        """
        return self._py_add_nodes_to_group.node_indices

    @property
    def group_index(self) -> GroupIndex:
        """The group the nodes join.

        Returns:
            GroupIndex: The group the nodes join.
        """
        return self._py_add_nodes_to_group.group_index


class RemoveNodesFromGroup:
    """The payload of a change removing nodes from a group."""

    _py_remove_nodes_from_group: PyRemoveNodesFromGroup

    def __init__(self, node_indices: List[NodeIndex], group_index: GroupIndex) -> None:
        """Initializes the payload of a change removing nodes from a group.

        Args:
            node_indices (List[NodeIndex]): The nodes that are removed.
            group_index (GroupIndex): The group the nodes leave.
        """
        self._py_remove_nodes_from_group = PyRemoveNodesFromGroup(
            node_indices, group_index
        )

    @classmethod
    def _from_py_remove_nodes_from_group(
        cls, py_remove_nodes_from_group: PyRemoveNodesFromGroup
    ) -> RemoveNodesFromGroup:
        """Creates a RemoveNodesFromGroup from a PyRemoveNodesFromGroup.

        Args:
            py_remove_nodes_from_group (PyRemoveNodesFromGroup): The
                PyRemoveNodesFromGroup
                to convert.

        Returns:
            RemoveNodesFromGroup: The converted RemoveNodesFromGroup.
        """
        remove_nodes_from_group = cls.__new__(cls)
        remove_nodes_from_group._py_remove_nodes_from_group = py_remove_nodes_from_group
        return remove_nodes_from_group

    @property
    def node_indices(self) -> List[NodeIndex]:
        """The nodes that are removed.

        Returns:
            List[NodeIndex]: The nodes that are removed.
        """
        return self._py_remove_nodes_from_group.node_indices

    @property
    def group_index(self) -> GroupIndex:
        """The group the nodes leave.

        Returns:
            GroupIndex: The group the nodes leave.
        """
        return self._py_remove_nodes_from_group.group_index


class AddEdgesToGroup:
    """The payload of a change adding edges to a group."""

    _py_add_edges_to_group: PyAddEdgesToGroup

    def __init__(self, edge_indices: List[EdgeIndex], group_index: GroupIndex) -> None:
        """Initializes the payload of a change adding edges to a group.

        Args:
            edge_indices (List[EdgeIndex]): The edges that are added.
            group_index (GroupIndex): The group the edges join.
        """
        self._py_add_edges_to_group = PyAddEdgesToGroup(
            [edge_index._py_edge_index for edge_index in edge_indices], group_index
        )

    @classmethod
    def _from_py_add_edges_to_group(
        cls, py_add_edges_to_group: PyAddEdgesToGroup
    ) -> AddEdgesToGroup:
        """Creates an AddEdgesToGroup from a PyAddEdgesToGroup.

        Args:
            py_add_edges_to_group (PyAddEdgesToGroup): The PyAddEdgesToGroup to convert.

        Returns:
            AddEdgesToGroup: The converted AddEdgesToGroup.
        """
        add_edges_to_group = cls.__new__(cls)
        add_edges_to_group._py_add_edges_to_group = py_add_edges_to_group
        return add_edges_to_group

    @property
    def edge_indices(self) -> List[EdgeIndex]:
        """The edges that are added.

        Returns:
            List[EdgeIndex]: The edges that are added.
        """
        return [
            EdgeIndex._from_py_edge_index(edge_index)
            for edge_index in self._py_add_edges_to_group.edge_indices
        ]

    @property
    def group_index(self) -> GroupIndex:
        """The group the edges join.

        Returns:
            GroupIndex: The group the edges join.
        """
        return self._py_add_edges_to_group.group_index


class RemoveEdgesFromGroup:
    """The payload of a change removing edges from a group."""

    _py_remove_edges_from_group: PyRemoveEdgesFromGroup

    def __init__(self, edge_indices: List[EdgeIndex], group_index: GroupIndex) -> None:
        """Initializes the payload of a change removing edges from a group.

        Args:
            edge_indices (List[EdgeIndex]): The edges that are removed.
            group_index (GroupIndex): The group the edges leave.
        """
        self._py_remove_edges_from_group = PyRemoveEdgesFromGroup(
            [edge_index._py_edge_index for edge_index in edge_indices], group_index
        )

    @classmethod
    def _from_py_remove_edges_from_group(
        cls, py_remove_edges_from_group: PyRemoveEdgesFromGroup
    ) -> RemoveEdgesFromGroup:
        """Creates a RemoveEdgesFromGroup from a PyRemoveEdgesFromGroup.

        Args:
            py_remove_edges_from_group (PyRemoveEdgesFromGroup): The
                PyRemoveEdgesFromGroup
                to convert.

        Returns:
            RemoveEdgesFromGroup: The converted RemoveEdgesFromGroup.
        """
        remove_edges_from_group = cls.__new__(cls)
        remove_edges_from_group._py_remove_edges_from_group = py_remove_edges_from_group
        return remove_edges_from_group

    @property
    def edge_indices(self) -> List[EdgeIndex]:
        """The edges that are removed.

        Returns:
            List[EdgeIndex]: The edges that are removed.
        """
        return [
            EdgeIndex._from_py_edge_index(edge_index)
            for edge_index in self._py_remove_edges_from_group.edge_indices
        ]

    @property
    def group_index(self) -> GroupIndex:
        """The group the edges leave.

        Returns:
            GroupIndex: The group the edges leave.
        """
        return self._py_remove_edges_from_group.group_index


class SetSchema:
    """The payload of a change setting the schema."""

    _py_set_schema: PySetSchema

    def __init__(self, schema: Schema) -> None:
        """Initializes the payload of a change setting the schema.

        Args:
            schema (Schema): The schema that is set.
        """
        self._py_set_schema = PySetSchema(schema._py_schema)

    @classmethod
    def _from_py_set_schema(cls, py_set_schema: PySetSchema) -> SetSchema:
        """Creates a SetSchema from a PySetSchema.

        Args:
            py_set_schema (PySetSchema): The PySetSchema to convert.

        Returns:
            SetSchema: The converted SetSchema.
        """
        set_schema = cls.__new__(cls)
        set_schema._py_set_schema = py_set_schema
        return set_schema

    @property
    def schema(self) -> Schema:
        """The schema that is set.

        Returns:
            Schema: The schema that is set.
        """
        return Schema._from_py_schema(self._py_set_schema.schema)


class FreezeSchema:
    """The payload of a change freezing the schema."""

    _py_freeze_schema: PyFreezeSchema

    def __init__(self) -> None:
        """Initializes the payload of a change freezing the schema."""
        self._py_freeze_schema = PyFreezeSchema()

    @classmethod
    def _from_py_freeze_schema(cls, py_freeze_schema: PyFreezeSchema) -> FreezeSchema:
        """Creates a FreezeSchema from a PyFreezeSchema.

        Args:
            py_freeze_schema (PyFreezeSchema): The PyFreezeSchema to convert.

        Returns:
            FreezeSchema: The converted FreezeSchema.
        """
        freeze_schema = cls.__new__(cls)
        freeze_schema._py_freeze_schema = py_freeze_schema
        return freeze_schema


class UnfreezeSchema:
    """The payload of a change unfreezing the schema."""

    _py_unfreeze_schema: PyUnfreezeSchema

    def __init__(self) -> None:
        """Initializes the payload of a change unfreezing the schema."""
        self._py_unfreeze_schema = PyUnfreezeSchema()

    @classmethod
    def _from_py_unfreeze_schema(
        cls, py_unfreeze_schema: PyUnfreezeSchema
    ) -> UnfreezeSchema:
        """Creates an UnfreezeSchema from a PyUnfreezeSchema.

        Args:
            py_unfreeze_schema (PyUnfreezeSchema): The PyUnfreezeSchema to convert.

        Returns:
            UnfreezeSchema: The converted UnfreezeSchema.
        """
        unfreeze_schema = cls.__new__(cls)
        unfreeze_schema._py_unfreeze_schema = py_unfreeze_schema
        return unfreeze_schema


class Clear:
    """The payload of a change clearing the GraphRecord."""

    _py_clear: PyClear

    def __init__(self) -> None:
        """Initializes the payload of a change clearing the GraphRecord."""
        self._py_clear = PyClear()

    @classmethod
    def _from_py_clear(cls, py_clear: PyClear) -> Clear:
        """Creates a Clear from a PyClear.

        Args:
            py_clear (PyClear): The PyClear to convert.

        Returns:
            Clear: The converted Clear.
        """
        clear = cls.__new__(cls)
        clear._py_clear = py_clear
        return clear


#: A type alias for a change a plugin hook may return.
Change: TypeAlias = Union[
    AddNodes,
    AddNodesInGroup,
    AddEdges,
    AddEdgesInGroup,
    RemoveNodes,
    RemoveEdges,
    SetNodeAttributes,
    ReplaceNodeAttributes,
    RemoveNodeAttributes,
    SetEdgeAttributes,
    ReplaceEdgeAttributes,
    RemoveEdgeAttributes,
    AddGroup,
    RemoveGroups,
    AddNodesToGroup,
    RemoveNodesFromGroup,
    AddEdgesToGroup,
    RemoveEdgesFromGroup,
    SetSchema,
    FreezeSchema,
    UnfreezeSchema,
    Clear,
]

#: A type alias for the changes a plugin hook may return.
Changes: TypeAlias = Union[Change, List[Change]]


class Plugin:
    """Base class for GraphRecord plugins.

    Every hook is optional: a GraphRecord calls a hook only when the plugin defines
    it. A ``pre_`` hook returns what the GraphRecord applies in place of the change
    it received: ``None`` keeps that change, a change or a list of changes replaces
    it, and an empty list drops it. ``initialize`` and ``finalize`` return changes
    the same way, but receive no change, so ``None`` means no changes. A ``post_``
    hook observes the GraphRecords before and after the applied changes together
    with the change that was applied and must return ``None``. The two GraphRecords
    bracket every change applied in the call, not only the change the hook
    received. The hooks are
    declared for type checkers only, which keeps a plugin free of the hooks it does
    not implement and keeps a GraphRecord from building payloads nobody reads.
    """

    def _bridge(self) -> _PluginBridge:
        """Wraps the plugin in the bridge a GraphRecord calls its hooks through.

        Returns:
            _PluginBridge: The bridge around this plugin.
        """
        return _PluginBridge(self)

    if TYPE_CHECKING:

        def initialize(self, record: GraphRecord) -> Optional[Changes]:
            """Handles the plugin being added to a GraphRecord.

            Args:
                record (GraphRecord): The GraphRecord the plugin is added to.

            Returns:
                Optional[Changes]: The changes to apply, or None to apply none.
            """

        def finalize(self, record: GraphRecord) -> Optional[Changes]:
            """Handles the plugin being removed from a GraphRecord.

            Args:
                record (GraphRecord): The GraphRecord the plugin is removed from.

            Returns:
                Optional[Changes]: The changes to apply, or None to apply none.
            """

        def pre_add_nodes(
            self, record: GraphRecord, addition: AddNodes
        ) -> Optional[Changes]:
            """Handles nodes being added.

            Args:
                record (GraphRecord): The GraphRecord the change is applied to.
                addition (AddNodes): The change that is applied.

            Returns:
                Optional[Changes]: The changes to apply instead, or None to apply
                    the change unchanged.
            """

        def post_add_nodes(
            self,
            previous: GraphRecord,
            candidate: GraphRecord,
            addition: AddNodes,
        ) -> None:
            """Observes the GraphRecord after nodes were added.

            Args:
                previous (GraphRecord): The GraphRecord before the applied changes.
                candidate (GraphRecord): The GraphRecord with all changes applied.
                addition (AddNodes): The change that was applied.
            """

        def pre_add_nodes_in_group(
            self, record: GraphRecord, addition: AddNodesInGroup
        ) -> Optional[Changes]:
            """Handles nodes being added in a group.

            Args:
                record (GraphRecord): The GraphRecord the change is applied to.
                addition (AddNodesInGroup): The change that is applied.

            Returns:
                Optional[Changes]: The changes to apply instead, or None to apply
                    the change unchanged.
            """

        def post_add_nodes_in_group(
            self,
            previous: GraphRecord,
            candidate: GraphRecord,
            addition: AddNodesInGroup,
        ) -> None:
            """Observes the GraphRecord after nodes were added in a group.

            Args:
                previous (GraphRecord): The GraphRecord before the applied changes.
                candidate (GraphRecord): The GraphRecord with all changes applied.
                addition (AddNodesInGroup): The change that was applied.
            """

        def pre_add_edges(
            self, record: GraphRecord, addition: AddEdges
        ) -> Optional[Changes]:
            """Handles edges being added.

            Args:
                record (GraphRecord): The GraphRecord the change is applied to.
                addition (AddEdges): The change that is applied.

            Returns:
                Optional[Changes]: The changes to apply instead, or None to apply
                    the change unchanged.
            """

        def post_add_edges(
            self,
            previous: GraphRecord,
            candidate: GraphRecord,
            addition: AddEdges,
        ) -> None:
            """Observes the GraphRecord after edges were added.

            Args:
                previous (GraphRecord): The GraphRecord before the applied changes.
                candidate (GraphRecord): The GraphRecord with all changes applied.
                addition (AddEdges): The change that was applied.
            """

        def pre_add_edges_in_group(
            self, record: GraphRecord, addition: AddEdgesInGroup
        ) -> Optional[Changes]:
            """Handles edges being added in a group.

            Args:
                record (GraphRecord): The GraphRecord the change is applied to.
                addition (AddEdgesInGroup): The change that is applied.

            Returns:
                Optional[Changes]: The changes to apply instead, or None to apply
                    the change unchanged.
            """

        def post_add_edges_in_group(
            self,
            previous: GraphRecord,
            candidate: GraphRecord,
            addition: AddEdgesInGroup,
        ) -> None:
            """Observes the GraphRecord after edges were added in a group.

            Args:
                previous (GraphRecord): The GraphRecord before the applied changes.
                candidate (GraphRecord): The GraphRecord with all changes applied.
                addition (AddEdgesInGroup): The change that was applied.
            """

        def pre_remove_nodes(
            self, record: GraphRecord, removal: RemoveNodes
        ) -> Optional[Changes]:
            """Handles nodes being removed.

            Args:
                record (GraphRecord): The GraphRecord the change is applied to.
                removal (RemoveNodes): The change that is applied.

            Returns:
                Optional[Changes]: The changes to apply instead, or None to apply
                    the change unchanged.
            """

        def post_remove_nodes(
            self,
            previous: GraphRecord,
            candidate: GraphRecord,
            removal: RemoveNodes,
        ) -> None:
            """Observes the GraphRecord after nodes were removed.

            Args:
                previous (GraphRecord): The GraphRecord before the applied changes.
                candidate (GraphRecord): The GraphRecord with all changes applied.
                removal (RemoveNodes): The change that was applied.
            """

        def pre_remove_edges(
            self, record: GraphRecord, removal: RemoveEdges
        ) -> Optional[Changes]:
            """Handles edges being removed.

            Args:
                record (GraphRecord): The GraphRecord the change is applied to.
                removal (RemoveEdges): The change that is applied.

            Returns:
                Optional[Changes]: The changes to apply instead, or None to apply
                    the change unchanged.
            """

        def post_remove_edges(
            self,
            previous: GraphRecord,
            candidate: GraphRecord,
            removal: RemoveEdges,
        ) -> None:
            """Observes the GraphRecord after edges were removed.

            Args:
                previous (GraphRecord): The GraphRecord before the applied changes.
                candidate (GraphRecord): The GraphRecord with all changes applied.
                removal (RemoveEdges): The change that was applied.
            """

        def pre_set_node_attributes(
            self, record: GraphRecord, assignment: SetNodeAttributes
        ) -> Optional[Changes]:
            """Handles node attributes being set.

            Args:
                record (GraphRecord): The GraphRecord the change is applied to.
                assignment (SetNodeAttributes): The change that is applied.

            Returns:
                Optional[Changes]: The changes to apply instead, or None to apply
                    the change unchanged.
            """

        def post_set_node_attributes(
            self,
            previous: GraphRecord,
            candidate: GraphRecord,
            assignment: SetNodeAttributes,
        ) -> None:
            """Observes the GraphRecord after node attributes were set.

            Args:
                previous (GraphRecord): The GraphRecord before the applied changes.
                candidate (GraphRecord): The GraphRecord with all changes applied.
                assignment (SetNodeAttributes): The change that was applied.
            """

        def pre_replace_node_attributes(
            self, record: GraphRecord, assignment: ReplaceNodeAttributes
        ) -> Optional[Changes]:
            """Handles node attributes being replaced.

            Args:
                record (GraphRecord): The GraphRecord the change is applied to.
                assignment (ReplaceNodeAttributes): The change that is applied.

            Returns:
                Optional[Changes]: The changes to apply instead, or None to apply
                    the change unchanged.
            """

        def post_replace_node_attributes(
            self,
            previous: GraphRecord,
            candidate: GraphRecord,
            assignment: ReplaceNodeAttributes,
        ) -> None:
            """Observes the GraphRecord after node attributes were replaced.

            Args:
                previous (GraphRecord): The GraphRecord before the applied changes.
                candidate (GraphRecord): The GraphRecord with all changes applied.
                assignment (ReplaceNodeAttributes): The change that was applied.
            """

        def pre_remove_node_attributes(
            self, record: GraphRecord, removal: RemoveNodeAttributes
        ) -> Optional[Changes]:
            """Handles node attributes being removed.

            Args:
                record (GraphRecord): The GraphRecord the change is applied to.
                removal (RemoveNodeAttributes): The change that is applied.

            Returns:
                Optional[Changes]: The changes to apply instead, or None to apply
                    the change unchanged.
            """

        def post_remove_node_attributes(
            self,
            previous: GraphRecord,
            candidate: GraphRecord,
            removal: RemoveNodeAttributes,
        ) -> None:
            """Observes the GraphRecord after node attributes were removed.

            Args:
                previous (GraphRecord): The GraphRecord before the applied changes.
                candidate (GraphRecord): The GraphRecord with all changes applied.
                removal (RemoveNodeAttributes): The change that was applied.
            """

        def pre_set_edge_attributes(
            self, record: GraphRecord, assignment: SetEdgeAttributes
        ) -> Optional[Changes]:
            """Handles edge attributes being set.

            Args:
                record (GraphRecord): The GraphRecord the change is applied to.
                assignment (SetEdgeAttributes): The change that is applied.

            Returns:
                Optional[Changes]: The changes to apply instead, or None to apply
                    the change unchanged.
            """

        def post_set_edge_attributes(
            self,
            previous: GraphRecord,
            candidate: GraphRecord,
            assignment: SetEdgeAttributes,
        ) -> None:
            """Observes the GraphRecord after edge attributes were set.

            Args:
                previous (GraphRecord): The GraphRecord before the applied changes.
                candidate (GraphRecord): The GraphRecord with all changes applied.
                assignment (SetEdgeAttributes): The change that was applied.
            """

        def pre_replace_edge_attributes(
            self, record: GraphRecord, assignment: ReplaceEdgeAttributes
        ) -> Optional[Changes]:
            """Handles edge attributes being replaced.

            Args:
                record (GraphRecord): The GraphRecord the change is applied to.
                assignment (ReplaceEdgeAttributes): The change that is applied.

            Returns:
                Optional[Changes]: The changes to apply instead, or None to apply
                    the change unchanged.
            """

        def post_replace_edge_attributes(
            self,
            previous: GraphRecord,
            candidate: GraphRecord,
            assignment: ReplaceEdgeAttributes,
        ) -> None:
            """Observes the GraphRecord after edge attributes were replaced.

            Args:
                previous (GraphRecord): The GraphRecord before the applied changes.
                candidate (GraphRecord): The GraphRecord with all changes applied.
                assignment (ReplaceEdgeAttributes): The change that was applied.
            """

        def pre_remove_edge_attributes(
            self, record: GraphRecord, removal: RemoveEdgeAttributes
        ) -> Optional[Changes]:
            """Handles edge attributes being removed.

            Args:
                record (GraphRecord): The GraphRecord the change is applied to.
                removal (RemoveEdgeAttributes): The change that is applied.

            Returns:
                Optional[Changes]: The changes to apply instead, or None to apply
                    the change unchanged.
            """

        def post_remove_edge_attributes(
            self,
            previous: GraphRecord,
            candidate: GraphRecord,
            removal: RemoveEdgeAttributes,
        ) -> None:
            """Observes the GraphRecord after edge attributes were removed.

            Args:
                previous (GraphRecord): The GraphRecord before the applied changes.
                candidate (GraphRecord): The GraphRecord with all changes applied.
                removal (RemoveEdgeAttributes): The change that was applied.
            """

        def pre_add_group(
            self, record: GraphRecord, addition: AddGroup
        ) -> Optional[Changes]:
            """Handles a group being added.

            Args:
                record (GraphRecord): The GraphRecord the change is applied to.
                addition (AddGroup): The change that is applied.

            Returns:
                Optional[Changes]: The changes to apply instead, or None to apply
                    the change unchanged.
            """

        def post_add_group(
            self,
            previous: GraphRecord,
            candidate: GraphRecord,
            addition: AddGroup,
        ) -> None:
            """Observes the GraphRecord after a group was added.

            Args:
                previous (GraphRecord): The GraphRecord before the applied changes.
                candidate (GraphRecord): The GraphRecord with all changes applied.
                addition (AddGroup): The change that was applied.
            """

        def pre_remove_groups(
            self, record: GraphRecord, removal: RemoveGroups
        ) -> Optional[Changes]:
            """Handles groups being removed.

            Args:
                record (GraphRecord): The GraphRecord the change is applied to.
                removal (RemoveGroups): The change that is applied.

            Returns:
                Optional[Changes]: The changes to apply instead, or None to apply
                    the change unchanged.
            """

        def post_remove_groups(
            self,
            previous: GraphRecord,
            candidate: GraphRecord,
            removal: RemoveGroups,
        ) -> None:
            """Observes the GraphRecord after groups were removed.

            Args:
                previous (GraphRecord): The GraphRecord before the applied changes.
                candidate (GraphRecord): The GraphRecord with all changes applied.
                removal (RemoveGroups): The change that was applied.
            """

        def pre_add_nodes_to_group(
            self, record: GraphRecord, membership: AddNodesToGroup
        ) -> Optional[Changes]:
            """Handles nodes being added to a group.

            Args:
                record (GraphRecord): The GraphRecord the change is applied to.
                membership (AddNodesToGroup): The change that is applied.

            Returns:
                Optional[Changes]: The changes to apply instead, or None to apply
                    the change unchanged.
            """

        def post_add_nodes_to_group(
            self,
            previous: GraphRecord,
            candidate: GraphRecord,
            membership: AddNodesToGroup,
        ) -> None:
            """Observes the GraphRecord after nodes were added to a group.

            Args:
                previous (GraphRecord): The GraphRecord before the applied changes.
                candidate (GraphRecord): The GraphRecord with all changes applied.
                membership (AddNodesToGroup): The change that was applied.
            """

        def pre_remove_nodes_from_group(
            self, record: GraphRecord, membership: RemoveNodesFromGroup
        ) -> Optional[Changes]:
            """Handles nodes being removed from a group.

            Args:
                record (GraphRecord): The GraphRecord the change is applied to.
                membership (RemoveNodesFromGroup): The change that is applied.

            Returns:
                Optional[Changes]: The changes to apply instead, or None to apply
                    the change unchanged.
            """

        def post_remove_nodes_from_group(
            self,
            previous: GraphRecord,
            candidate: GraphRecord,
            membership: RemoveNodesFromGroup,
        ) -> None:
            """Observes the GraphRecord after nodes were removed from a group.

            Args:
                previous (GraphRecord): The GraphRecord before the applied changes.
                candidate (GraphRecord): The GraphRecord with all changes applied.
                membership (RemoveNodesFromGroup): The change that was applied.
            """

        def pre_add_edges_to_group(
            self, record: GraphRecord, membership: AddEdgesToGroup
        ) -> Optional[Changes]:
            """Handles edges being added to a group.

            Args:
                record (GraphRecord): The GraphRecord the change is applied to.
                membership (AddEdgesToGroup): The change that is applied.

            Returns:
                Optional[Changes]: The changes to apply instead, or None to apply
                    the change unchanged.
            """

        def post_add_edges_to_group(
            self,
            previous: GraphRecord,
            candidate: GraphRecord,
            membership: AddEdgesToGroup,
        ) -> None:
            """Observes the GraphRecord after edges were added to a group.

            Args:
                previous (GraphRecord): The GraphRecord before the applied changes.
                candidate (GraphRecord): The GraphRecord with all changes applied.
                membership (AddEdgesToGroup): The change that was applied.
            """

        def pre_remove_edges_from_group(
            self, record: GraphRecord, membership: RemoveEdgesFromGroup
        ) -> Optional[Changes]:
            """Handles edges being removed from a group.

            Args:
                record (GraphRecord): The GraphRecord the change is applied to.
                membership (RemoveEdgesFromGroup): The change that is applied.

            Returns:
                Optional[Changes]: The changes to apply instead, or None to apply
                    the change unchanged.
            """

        def post_remove_edges_from_group(
            self,
            previous: GraphRecord,
            candidate: GraphRecord,
            membership: RemoveEdgesFromGroup,
        ) -> None:
            """Observes the GraphRecord after edges were removed from a group.

            Args:
                previous (GraphRecord): The GraphRecord before the applied changes.
                candidate (GraphRecord): The GraphRecord with all changes applied.
                membership (RemoveEdgesFromGroup): The change that was applied.
            """

        def pre_set_schema(
            self, record: GraphRecord, schema_change: SetSchema
        ) -> Optional[Changes]:
            """Handles the schema being set.

            Args:
                record (GraphRecord): The GraphRecord the change is applied to.
                schema_change (SetSchema): The change that is applied.

            Returns:
                Optional[Changes]: The changes to apply instead, or None to apply
                    the change unchanged.
            """

        def post_set_schema(
            self,
            previous: GraphRecord,
            candidate: GraphRecord,
            schema_change: SetSchema,
        ) -> None:
            """Observes the GraphRecord after the schema was set.

            Args:
                previous (GraphRecord): The GraphRecord before the applied changes.
                candidate (GraphRecord): The GraphRecord with all changes applied.
                schema_change (SetSchema): The change that was applied.
            """

        def pre_freeze_schema(
            self, record: GraphRecord, schema_change: FreezeSchema
        ) -> Optional[Changes]:
            """Handles the schema being frozen.

            Args:
                record (GraphRecord): The GraphRecord the change is applied to.
                schema_change (FreezeSchema): The change that is applied.

            Returns:
                Optional[Changes]: The changes to apply instead, or None to apply
                    the change unchanged.
            """

        def post_freeze_schema(
            self,
            previous: GraphRecord,
            candidate: GraphRecord,
            schema_change: FreezeSchema,
        ) -> None:
            """Observes the GraphRecord after the schema was frozen.

            Args:
                previous (GraphRecord): The GraphRecord before the applied changes.
                candidate (GraphRecord): The GraphRecord with all changes applied.
                schema_change (FreezeSchema): The change that was applied.
            """

        def pre_unfreeze_schema(
            self, record: GraphRecord, schema_change: UnfreezeSchema
        ) -> Optional[Changes]:
            """Handles the schema being unfrozen.

            Args:
                record (GraphRecord): The GraphRecord the change is applied to.
                schema_change (UnfreezeSchema): The change that is applied.

            Returns:
                Optional[Changes]: The changes to apply instead, or None to apply
                    the change unchanged.
            """

        def post_unfreeze_schema(
            self,
            previous: GraphRecord,
            candidate: GraphRecord,
            schema_change: UnfreezeSchema,
        ) -> None:
            """Observes the GraphRecord after the schema was unfrozen.

            Args:
                previous (GraphRecord): The GraphRecord before the applied changes.
                candidate (GraphRecord): The GraphRecord with all changes applied.
                schema_change (UnfreezeSchema): The change that was applied.
            """

        def pre_clear(self, record: GraphRecord, clearing: Clear) -> Optional[Changes]:
            """Handles the GraphRecord being cleared.

            Args:
                record (GraphRecord): The GraphRecord the change is applied to.
                clearing (Clear): The change that is applied.

            Returns:
                Optional[Changes]: The changes to apply instead, or None to apply
                    the change unchanged.
            """

        def post_clear(
            self,
            previous: GraphRecord,
            candidate: GraphRecord,
            clearing: Clear,
        ) -> None:
            """Observes the GraphRecord after it was cleared.

            Args:
                previous (GraphRecord): The GraphRecord before the applied changes.
                candidate (GraphRecord): The GraphRecord with all changes applied.
                clearing (Clear): The change that was applied.
            """


#: The payload converter of every pre hook a GraphRecord may call.
_PRE_HOOKS: Final[Dict[str, Callable[..., object]]] = {
    "pre_add_nodes": AddNodes._from_py_add_nodes,
    "pre_add_nodes_in_group": AddNodesInGroup._from_py_add_nodes_in_group,
    "pre_add_edges": AddEdges._from_py_add_edges,
    "pre_add_edges_in_group": AddEdgesInGroup._from_py_add_edges_in_group,
    "pre_remove_nodes": RemoveNodes._from_py_remove_nodes,
    "pre_remove_edges": RemoveEdges._from_py_remove_edges,
    "pre_set_node_attributes": SetNodeAttributes._from_py_set_node_attributes,
    "pre_replace_node_attributes": ReplaceNodeAttributes._from_py_replace_node_attributes,
    "pre_remove_node_attributes": RemoveNodeAttributes._from_py_remove_node_attributes,
    "pre_set_edge_attributes": SetEdgeAttributes._from_py_set_edge_attributes,
    "pre_replace_edge_attributes": ReplaceEdgeAttributes._from_py_replace_edge_attributes,
    "pre_remove_edge_attributes": RemoveEdgeAttributes._from_py_remove_edge_attributes,
    "pre_add_group": AddGroup._from_py_add_group,
    "pre_remove_groups": RemoveGroups._from_py_remove_groups,
    "pre_add_nodes_to_group": AddNodesToGroup._from_py_add_nodes_to_group,
    "pre_remove_nodes_from_group": RemoveNodesFromGroup._from_py_remove_nodes_from_group,
    "pre_add_edges_to_group": AddEdgesToGroup._from_py_add_edges_to_group,
    "pre_remove_edges_from_group": RemoveEdgesFromGroup._from_py_remove_edges_from_group,
    "pre_set_schema": SetSchema._from_py_set_schema,
    "pre_freeze_schema": FreezeSchema._from_py_freeze_schema,
    "pre_unfreeze_schema": UnfreezeSchema._from_py_unfreeze_schema,
    "pre_clear": Clear._from_py_clear,
}

#: The payload converter of every post hook a GraphRecord may call.
_POST_HOOKS: Final[Dict[str, Callable[..., object]]] = {
    "post_" + name.removeprefix("pre_"): convert for name, convert in _PRE_HOOKS.items()
}

#: The name of every lifecycle hook a GraphRecord may call.
_LIFECYCLE_HOOKS: Final[FrozenSet[str]] = frozenset({"initialize", "finalize"})

#: The payload attribute of every change a plugin hook may return.
_CHANGE_PAYLOADS: Final[Dict[type, str]] = {
    AddNodes: "_py_add_nodes",
    AddNodesInGroup: "_py_add_nodes_in_group",
    AddEdges: "_py_add_edges",
    AddEdgesInGroup: "_py_add_edges_in_group",
    RemoveNodes: "_py_remove_nodes",
    RemoveEdges: "_py_remove_edges",
    SetNodeAttributes: "_py_set_node_attributes",
    ReplaceNodeAttributes: "_py_replace_node_attributes",
    RemoveNodeAttributes: "_py_remove_node_attributes",
    SetEdgeAttributes: "_py_set_edge_attributes",
    ReplaceEdgeAttributes: "_py_replace_edge_attributes",
    RemoveEdgeAttributes: "_py_remove_edge_attributes",
    AddGroup: "_py_add_group",
    RemoveGroups: "_py_remove_groups",
    AddNodesToGroup: "_py_add_nodes_to_group",
    RemoveNodesFromGroup: "_py_remove_nodes_from_group",
    AddEdgesToGroup: "_py_add_edges_to_group",
    RemoveEdgesFromGroup: "_py_remove_edges_from_group",
    SetSchema: "_py_set_schema",
    FreezeSchema: "_py_freeze_schema",
    UnfreezeSchema: "_py_unfreeze_schema",
    Clear: "_py_clear",
}


class _PluginBridge:
    """Adapts a Plugin to the hooks a GraphRecord looks up on it.

    A GraphRecord looks a hook up by name and skips it when the lookup fails, so the
    bridge resolves a hook only when the wrapped plugin defines it. Resolving one
    yields a closure that converts the arguments the GraphRecord passes into the
    wrapper types the plugin is written against, and the changes the plugin returns
    back into the payloads the GraphRecord applies.
    """

    _plugin: Plugin

    def __init__(self, plugin: Plugin) -> None:
        """Initializes a bridge around a plugin.

        Args:
            plugin (Plugin): The plugin to adapt.
        """
        self._plugin = plugin

    @staticmethod
    def _record(py_record: PyGraphRecord) -> GraphRecord:
        """Converts a py_record a GraphRecord passed to a hook.

        Args:
            py_record (PyGraphRecord): The py_record to convert.

        Returns:
            GraphRecord: The converted py_record.
        """
        from graphrecords.graphrecord import GraphRecord

        return GraphRecord._from_py_graphrecord(py_record)

    @staticmethod
    def _change(returned: object) -> object:
        """Converts one change a hook returned into what a GraphRecord applies.

        Args:
            returned (object): The change the hook returned.

        Returns:
            object: The payload of the change, or what the hook returned when that is
                not a change.
        """
        payload = _CHANGE_PAYLOADS.get(type(returned))

        if payload is None:
            return returned

        return getattr(returned, payload)

    @staticmethod
    def _changes(returned: object) -> object:
        """Converts what a hook returned into what a GraphRecord applies.

        Args:
            returned (object): The value the hook returned.

        Returns:
            object: The payload of every change the hook returned.
        """
        if isinstance(returned, list):
            return [_PluginBridge._change(element) for element in returned]

        return _PluginBridge._change(returned)

    def _pre_hook(
        self, name: str, convert: Callable[..., object]
    ) -> Callable[[PyGraphRecord, object], object]:
        """Resolves a pre hook of the plugin.

        Args:
            name (str): The name of the hook.
            convert (Callable[..., object]): The converter of the hook's payload.

        Returns:
            Callable[[PyGraphRecord, object], object]: The hook, converting its
                arguments and what it returns.
        """
        hook = getattr(self._plugin, name)

        def call(py_record: PyGraphRecord, payload: object) -> object:
            return self._changes(hook(self._record(py_record), convert(payload)))

        return call

    def _post_hook(
        self, name: str, convert: Callable[..., object]
    ) -> Callable[[PyGraphRecord, PyGraphRecord, object], object]:
        """Resolves a post hook of the plugin.

        Args:
            name (str): The name of the hook.
            convert (Callable[..., object]): The converter of the hook's payload.

        Returns:
            Callable[[PyGraphRecord, PyGraphRecord, object], object]: The hook,
                converting its arguments.
        """
        hook = getattr(self._plugin, name)

        def call(
            py_previous: PyGraphRecord, py_candidate: PyGraphRecord, payload: object
        ) -> object:
            return hook(
                self._record(py_previous), self._record(py_candidate), convert(payload)
            )

        return call

    def _lifecycle_hook(self, name: str) -> Callable[[PyGraphRecord], object]:
        """Resolves a lifecycle hook of the plugin.

        Args:
            name (str): The name of the hook.

        Returns:
            Callable[[PyGraphRecord], object]: The hook, converting its argument and
                what it returns.
        """
        hook = getattr(self._plugin, name)

        def call(py_record: PyGraphRecord) -> object:
            return self._changes(hook(self._record(py_record)))

        return call

    def __getattr__(self, name: str) -> Callable[..., object]:
        """Resolves a hook a GraphRecord looks up on the bridge.

        Args:
            name (str): The name of the hook.

        Returns:
            Callable[..., object]: The hook, converting its arguments.

        Raises:
            AttributeError: If the name is not a hook, or the plugin does not
                define it.
        """
        convert = _PRE_HOOKS.get(name)
        if convert is not None:
            return self._pre_hook(name, convert)

        convert = _POST_HOOKS.get(name)
        if convert is not None:
            return self._post_hook(name, convert)

        if name in _LIFECYCLE_HOOKS:
            return self._lifecycle_hook(name)

        raise AttributeError(name)
