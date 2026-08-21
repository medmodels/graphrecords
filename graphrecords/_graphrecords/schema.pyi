from typing import Callable, ClassVar, Dict, Optional, Tuple

from graphrecords._graphrecords.datatype import PyDataType
from graphrecords._graphrecords.graphrecord import PyEdgeIndex, PyGraphRecord
from graphrecords.types import AttributeName, Attributes, GroupIndex, NodeIndex

class PyAttributeType:
    Categorical: PyAttributeType
    Continuous: PyAttributeType
    Temporal: PyAttributeType
    Unstructured: PyAttributeType

    @staticmethod
    def infer(data_type: PyDataType) -> PyAttributeType: ...
    def __eq__(self, other: object) -> bool: ...
    def __hash__(self) -> int: ...

class PyAttributeDataType:
    data_type: PyDataType
    attribute_type: PyAttributeType

    def __init__(
        self,
        data_type: PyDataType,
        attribute_type: Optional[PyAttributeType] = None,
    ) -> None: ...
    @staticmethod
    def _from_bytes(data: bytes) -> PyAttributeDataType: ...
    def __reduce__(
        self,
    ) -> Tuple[Callable[[bytes], PyAttributeDataType], Tuple[bytes]]: ...

class PyGroupSchema:
    nodes: Dict[AttributeName, PyAttributeDataType]
    edges: Dict[AttributeName, PyAttributeDataType]

    def __init__(
        self,
        nodes: Dict[AttributeName, PyAttributeDataType],
        edges: Dict[AttributeName, PyAttributeDataType],
    ) -> None: ...
    def validate_node(self, node_index: NodeIndex, attributes: Attributes) -> None: ...
    def validate_edge(
        self, edge_index: PyEdgeIndex, attributes: Attributes
    ) -> None: ...
    @staticmethod
    def _from_bytes(data: bytes) -> PyGroupSchema: ...
    def __reduce__(self) -> Tuple[Callable[[bytes], PyGroupSchema], Tuple[bytes]]: ...
    def __eq__(self, other: object) -> bool: ...
    __hash__: ClassVar[None]

class PySchemaType:
    Provided: PySchemaType
    Inferred: PySchemaType

    def __eq__(self, other: object) -> bool: ...
    def __hash__(self) -> int: ...

class PySchema:
    groups: Dict[GroupIndex, PyGroupSchema]
    ungrouped: PyGroupSchema
    schema_type: PySchemaType

    def __init__(
        self,
        groups: Dict[GroupIndex, PyGroupSchema],
        ungrouped: PyGroupSchema,
        schema_type: PySchemaType = ...,
    ) -> None: ...
    @staticmethod
    def infer(graphrecord: PyGraphRecord) -> PySchema: ...
    def group(self, group_index: GroupIndex) -> PyGroupSchema: ...
    def validate_node(
        self,
        node_index: NodeIndex,
        attributes: Attributes,
        group_index: Optional[GroupIndex] = None,
    ) -> None: ...
    def validate_edge(
        self,
        edge_index: PyEdgeIndex,
        attributes: Attributes,
        group_index: Optional[GroupIndex] = None,
    ) -> None: ...
    def set_node_attribute(
        self,
        attribute_name: AttributeName,
        data_type: PyDataType,
        attribute_type: PyAttributeType,
        group_index: Optional[GroupIndex] = None,
    ) -> PySchema: ...
    def set_edge_attribute(
        self,
        attribute_name: AttributeName,
        data_type: PyDataType,
        attribute_type: PyAttributeType,
        group_index: Optional[GroupIndex] = None,
    ) -> PySchema: ...
    def update_node_attribute(
        self,
        attribute_name: AttributeName,
        data_type: PyDataType,
        attribute_type: PyAttributeType,
        group_index: Optional[GroupIndex] = None,
    ) -> PySchema: ...
    def update_edge_attribute(
        self,
        attribute_name: AttributeName,
        data_type: PyDataType,
        attribute_type: PyAttributeType,
        group_index: Optional[GroupIndex] = None,
    ) -> PySchema: ...
    def remove_node_attribute(
        self, attribute_name: AttributeName, group_index: Optional[GroupIndex] = None
    ) -> PySchema: ...
    def remove_edge_attribute(
        self, attribute_name: AttributeName, group_index: Optional[GroupIndex] = None
    ) -> PySchema: ...
    def add_group(
        self, group_index: GroupIndex, group_schema: PyGroupSchema
    ) -> PySchema: ...
    def remove_group(self, group_index: GroupIndex) -> PySchema: ...
    def freeze(self) -> PySchema: ...
    def unfreeze(self) -> PySchema: ...
    @staticmethod
    def _from_bytes(data: bytes) -> PySchema: ...
    def __reduce__(self) -> Tuple[Callable[[bytes], PySchema], Tuple[bytes]]: ...
    def __eq__(self, other: object) -> bool: ...
    __hash__: ClassVar[None]
