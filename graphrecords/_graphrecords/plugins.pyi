from typing import List, Tuple

from graphrecords._graphrecords.graphrecord import PyEdgeIndex
from graphrecords._graphrecords.schema import PySchema
from graphrecords.types import (
    AttributeName,
    Attributes,
    GroupIndex,
    NodeIndex,
    Value,
)

class PyNodeBatch:
    def __init__(self, nodes: List[Tuple[NodeIndex, Attributes]]) -> None: ...
    def __len__(self) -> int: ...
    def is_empty(self) -> bool: ...
    def __iter__(self) -> PyNodeBatchIterator: ...
    def attribute_values(
        self, attribute_name: AttributeName
    ) -> List[Tuple[NodeIndex, Value]]: ...

class PyNodeBatchIterator:
    def __iter__(self) -> PyNodeBatchIterator: ...
    def __next__(self) -> Tuple[NodeIndex, Attributes]: ...

class PyEdgeBatch:
    def __init__(
        self, edges: List[Tuple[NodeIndex, NodeIndex, Attributes]]
    ) -> None: ...
    def __len__(self) -> int: ...
    def is_empty(self) -> bool: ...
    def __iter__(self) -> PyEdgeBatchIterator: ...
    def attribute_values(
        self, attribute_name: AttributeName
    ) -> List[Tuple[NodeIndex, NodeIndex, Value]]: ...

class PyEdgeBatchIterator:
    def __iter__(self) -> PyEdgeBatchIterator: ...
    def __next__(self) -> Tuple[NodeIndex, NodeIndex, Attributes]: ...

class PyAddNodes:
    batch: PyNodeBatch
    def __init__(self, batch: PyNodeBatch) -> None: ...

class PyAddEdges:
    batch: PyEdgeBatch
    def __init__(self, batch: PyEdgeBatch) -> None: ...

class PyAddNodesInGroup:
    batch: PyNodeBatch
    group_index: GroupIndex
    def __init__(self, batch: PyNodeBatch, group_index: GroupIndex) -> None: ...

class PyAddEdgesInGroup:
    batch: PyEdgeBatch
    group_index: GroupIndex
    def __init__(self, batch: PyEdgeBatch, group_index: GroupIndex) -> None: ...

class PyRemoveNodes:
    node_indices: List[NodeIndex]
    def __init__(self, node_indices: List[NodeIndex]) -> None: ...

class PyRemoveEdges:
    edge_indices: List[PyEdgeIndex]
    def __init__(self, edge_indices: List[PyEdgeIndex]) -> None: ...

class PySetNodeAttributes:
    node_indices: List[NodeIndex]
    attributes: Attributes
    def __init__(
        self, node_indices: List[NodeIndex], attributes: Attributes
    ) -> None: ...

class PyReplaceNodeAttributes:
    node_indices: List[NodeIndex]
    attributes: Attributes
    def __init__(
        self, node_indices: List[NodeIndex], attributes: Attributes
    ) -> None: ...

class PySetEdgeAttributes:
    edge_indices: List[PyEdgeIndex]
    attributes: Attributes
    def __init__(
        self, edge_indices: List[PyEdgeIndex], attributes: Attributes
    ) -> None: ...

class PyReplaceEdgeAttributes:
    edge_indices: List[PyEdgeIndex]
    attributes: Attributes
    def __init__(
        self, edge_indices: List[PyEdgeIndex], attributes: Attributes
    ) -> None: ...

class PyRemoveNodeAttributes:
    node_indices: List[NodeIndex]
    attribute_names: List[AttributeName]
    def __init__(
        self, node_indices: List[NodeIndex], attribute_names: List[AttributeName]
    ) -> None: ...

class PyRemoveEdgeAttributes:
    edge_indices: List[PyEdgeIndex]
    attribute_names: List[AttributeName]
    def __init__(
        self, edge_indices: List[PyEdgeIndex], attribute_names: List[AttributeName]
    ) -> None: ...

class PyAddNodesToGroup:
    node_indices: List[NodeIndex]
    group_index: GroupIndex
    def __init__(
        self, node_indices: List[NodeIndex], group_index: GroupIndex
    ) -> None: ...

class PyRemoveNodesFromGroup:
    node_indices: List[NodeIndex]
    group_index: GroupIndex
    def __init__(
        self, node_indices: List[NodeIndex], group_index: GroupIndex
    ) -> None: ...

class PyAddEdgesToGroup:
    edge_indices: List[PyEdgeIndex]
    group_index: GroupIndex
    def __init__(
        self, edge_indices: List[PyEdgeIndex], group_index: GroupIndex
    ) -> None: ...

class PyRemoveEdgesFromGroup:
    edge_indices: List[PyEdgeIndex]
    group_index: GroupIndex
    def __init__(
        self, edge_indices: List[PyEdgeIndex], group_index: GroupIndex
    ) -> None: ...

class PyAddGroup:
    group_index: GroupIndex
    def __init__(self, group_index: GroupIndex) -> None: ...

class PyRemoveGroups:
    group_indices: List[GroupIndex]
    def __init__(self, group_indices: List[GroupIndex]) -> None: ...

class PySetSchema:
    schema: PySchema
    def __init__(self, schema: PySchema) -> None: ...

class PyFreezeSchema:
    def __init__(self) -> None: ...

class PyUnfreezeSchema:
    def __init__(self) -> None: ...

class PyClear:
    def __init__(self) -> None: ...
