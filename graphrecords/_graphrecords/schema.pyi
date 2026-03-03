from enum import Enum
from typing import Dict, List, Optional

from graphrecords._graphrecords.datatype import PyDataType
from graphrecords._graphrecords.graphrecord import PyGraphRecord
from graphrecords.types import (
    Attributes,
    EdgeIndex,
    GraphRecordAttribute,
    Group,
    NodeIndex,
)

class PyAttributeType(Enum):
    Categorical = ...
    Continuous = ...
    Temporal = ...
    Unstructured = ...

    @staticmethod
    def infer(data_type: PyDataType) -> PyAttributeType: ...

class PyAttributeDataType:
    data_type: PyDataType
    attribute_type: PyAttributeType

    def __init__(
        self, data_type: PyDataType, attribute_type: PyAttributeType
    ) -> None: ...

class PyGroupSchema:
    nodes: Dict[GraphRecordAttribute, PyAttributeDataType]
    edges: Dict[GraphRecordAttribute, PyAttributeDataType]

    def __init__(
        self,
        *,
        nodes: Dict[GraphRecordAttribute, PyAttributeDataType],
        edges: Dict[GraphRecordAttribute, PyAttributeDataType],
    ) -> None: ...
    def validate_node(self, index: NodeIndex, attributes: Attributes) -> None: ...
    def validate_edge(self, index: EdgeIndex, attributes: Attributes) -> None: ...

class PySchemaType(Enum):
    Provided = ...
    Inferred = ...

class PySchema:
    groups: List[Group]
    ungrouped: PyGroupSchema
    schema_type: PySchemaType

    def __init__(
        self,
        *,
        groups: Dict[Group, PyGroupSchema],
        ungrouped: PyGroupSchema,
        schema_type: PySchemaType = ...,
    ) -> None: ...
    @staticmethod
    def infer(graphrecord: PyGraphRecord) -> PySchema: ...
    def group(self, group: Group) -> PyGroupSchema: ...
    def validate_node(
        self, index: NodeIndex, attributes: Attributes, group: Optional[Group] = None
    ) -> None: ...
    def validate_edge(
        self, index: EdgeIndex, attributes: Attributes, group: Optional[Group] = None
    ) -> None: ...
    def set_node_attribute(
        self,
        attribute: GraphRecordAttribute,
        data_type: PyDataType,
        attribute_type: PyAttributeType,
        group: Optional[Group] = None,
    ) -> None: ...
    def set_edge_attribute(
        self,
        attribute: GraphRecordAttribute,
        data_type: PyDataType,
        attribute_type: PyAttributeType,
        group: Optional[Group] = None,
    ) -> None: ...
    def update_node_attribute(
        self,
        attribute: GraphRecordAttribute,
        data_type: PyDataType,
        attribute_type: PyAttributeType,
        group: Optional[Group] = None,
    ) -> None: ...
    def update_edge_attribute(
        self,
        attribute: GraphRecordAttribute,
        data_type: PyDataType,
        attribute_type: PyAttributeType,
        group: Optional[Group] = None,
    ) -> None: ...
    def remove_node_attribute(
        self, attribute: GraphRecordAttribute, group: Optional[Group] = None
    ) -> None: ...
    def remove_edge_attribute(
        self, attribute: GraphRecordAttribute, group: Optional[Group] = None
    ) -> None: ...
    def add_group(self, group: Group, schema: PyGroupSchema) -> None: ...
    def remove_group(self, group: Group) -> None: ...
    def freeze(self) -> None: ...
    def unfreeze(self) -> None: ...
