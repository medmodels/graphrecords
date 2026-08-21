"""Schema classes describing the attributes a GraphRecord may hold."""

from __future__ import annotations

from enum import Enum, auto
from typing import TYPE_CHECKING, Dict, Optional

from graphrecords._graphrecords.schema import (
    PyAttributeDataType,
    PyAttributeType,
    PyGroupSchema,
    PySchema,
    PySchemaType,
)
from graphrecords.datatype import DataType

if TYPE_CHECKING:
    from graphrecords.graphrecord import GraphRecord
    from graphrecords.types import (
        AttributeName,
        Attributes,
        EdgeIndex,
        GroupIndex,
        NodeIndex,
    )


class AttributeType(Enum):
    """Enumeration of attribute types."""

    Categorical = auto()
    Continuous = auto()
    Temporal = auto()
    Unstructured = auto()

    @staticmethod
    def _from_py_attribute_type(py_attribute_type: PyAttributeType) -> AttributeType:
        """Converts a PyAttributeType to an AttributeType.

        Args:
            py_attribute_type (PyAttributeType): The PyAttributeType to convert.

        Returns:
            AttributeType: The converted AttributeType.
        """
        if py_attribute_type == PyAttributeType.Categorical:
            return AttributeType.Categorical
        if py_attribute_type == PyAttributeType.Continuous:
            return AttributeType.Continuous
        if py_attribute_type == PyAttributeType.Temporal:
            return AttributeType.Temporal
        if py_attribute_type == PyAttributeType.Unstructured:
            return AttributeType.Unstructured
        msg = "Should never be reached"
        raise NotImplementedError(msg)

    def _into_py_attribute_type(self) -> PyAttributeType:
        """Converts an AttributeType to a PyAttributeType.

        Returns:
            PyAttributeType: The converted PyAttributeType.
        """
        if self == AttributeType.Categorical:
            return PyAttributeType.Categorical
        if self == AttributeType.Continuous:
            return PyAttributeType.Continuous
        if self == AttributeType.Temporal:
            return PyAttributeType.Temporal
        if self == AttributeType.Unstructured:
            return PyAttributeType.Unstructured
        msg = "Should never be reached"
        raise NotImplementedError(msg)

    @staticmethod
    def infer(data_type: DataType) -> AttributeType:
        """Infers the attribute type from the data type.

        Args:
            data_type (DataType): The data type to infer the attribute type from.

        Returns:
            AttributeType: The inferred attribute type.
        """
        return AttributeType._from_py_attribute_type(
            PyAttributeType.infer(data_type._inner())
        )

    def __repr__(self) -> str:
        """Returns the string representation of the attribute type.

        Returns:
            str: The string representation of the attribute type.
        """
        return f"AttributeType.{self.name}"

    def __str__(self) -> str:
        """Returns a user-friendly string representation of the attribute type.

        Returns:
            str: The user-friendly string representation of the attribute type.
        """
        return self.name


class SchemaType(Enum):
    """Enumeration of schema types."""

    Provided = auto()
    Inferred = auto()

    @staticmethod
    def _from_py_schema_type(py_schema_type: PySchemaType) -> SchemaType:
        """Converts a PySchemaType to a SchemaType.

        Args:
            py_schema_type (PySchemaType): The PySchemaType to convert.

        Returns:
            SchemaType: The converted SchemaType.
        """
        if py_schema_type == PySchemaType.Provided:
            return SchemaType.Provided
        if py_schema_type == PySchemaType.Inferred:
            return SchemaType.Inferred
        msg = "Should never be reached"
        raise NotImplementedError(msg)

    def _into_py_schema_type(self) -> PySchemaType:
        """Converts a SchemaType to a PySchemaType.

        Returns:
            PySchemaType: The converted PySchemaType.
        """
        if self == SchemaType.Provided:
            return PySchemaType.Provided
        if self == SchemaType.Inferred:
            return PySchemaType.Inferred
        msg = "Should never be reached"
        raise NotImplementedError(msg)

    def __repr__(self) -> str:
        """Returns the string representation of the schema type.

        Returns:
            str: The string representation of the schema type.
        """
        return f"SchemaType.{self.name}"

    def __str__(self) -> str:
        """Returns a user-friendly string representation of the schema type.

        Returns:
            str: The user-friendly string representation of the schema type.
        """
        return self.name


class AttributeDataType:
    """The data type of an attribute together with its statistical type."""

    _py_attribute_data_type: PyAttributeDataType

    def __init__(
        self, data_type: DataType, attribute_type: Optional[AttributeType] = None
    ) -> None:
        """Initializes an AttributeDataType.

        Args:
            data_type (DataType): The data type of the attribute.
            attribute_type (Optional[AttributeType]): The statistical type of the
                attribute. Defaults to the type inferred from the data type.
        """
        self._py_attribute_data_type = PyAttributeDataType(
            data_type._inner(),
            None
            if attribute_type is None
            else attribute_type._into_py_attribute_type(),
        )

    @classmethod
    def _from_py_attribute_data_type(
        cls, py_attribute_data_type: PyAttributeDataType
    ) -> AttributeDataType:
        """Creates an AttributeDataType from a PyAttributeDataType.

        Args:
            py_attribute_data_type (PyAttributeDataType): The PyAttributeDataType to
                convert.

        Returns:
            AttributeDataType: The converted AttributeDataType.
        """
        attribute_data_type = cls.__new__(cls)
        attribute_data_type._py_attribute_data_type = py_attribute_data_type
        return attribute_data_type

    @property
    def data_type(self) -> DataType:
        """The data type of the attribute.

        Returns:
            DataType: The data type of the attribute.
        """
        return DataType._from_py_data_type(self._py_attribute_data_type.data_type)

    @property
    def attribute_type(self) -> AttributeType:
        """The statistical type of the attribute.

        Returns:
            AttributeType: The statistical type of the attribute.
        """
        return AttributeType._from_py_attribute_type(
            self._py_attribute_data_type.attribute_type
        )

    def __eq__(self, value: object) -> bool:
        """Checks whether the AttributeDataType is equal to another one.

        Args:
            value (object): The value to compare.

        Returns:
            bool: True if both describe the same attribute, otherwise False.
        """
        if not isinstance(value, AttributeDataType):
            return NotImplemented

        return (
            self.data_type == value.data_type
            and self.attribute_type == value.attribute_type
        )

    def __repr__(self) -> str:
        """Returns the string representation of the AttributeDataType.

        Returns:
            str: The string representation of the AttributeDataType.
        """
        return f"AttributeDataType({self.data_type!r}, {self.attribute_type!r})"


class GroupSchema:
    """The node and edge attributes of a single group."""

    _py_group_schema: PyGroupSchema

    def __init__(
        self,
        *,
        nodes: Optional[Dict[AttributeName, AttributeDataType]] = None,
        edges: Optional[Dict[AttributeName, AttributeDataType]] = None,
    ) -> None:
        """Initializes a GroupSchema.

        Args:
            nodes (Optional[Dict[AttributeName, AttributeDataType]]): The attributes
                the nodes of the group hold. Defaults to no attributes.
            edges (Optional[Dict[AttributeName, AttributeDataType]]): The attributes
                the edges of the group hold. Defaults to no attributes.
        """
        if nodes is None:
            nodes = {}
        if edges is None:
            edges = {}

        self._py_group_schema = PyGroupSchema(
            {
                attribute_name: attribute_data_type._py_attribute_data_type
                for attribute_name, attribute_data_type in nodes.items()
            },
            {
                attribute_name: attribute_data_type._py_attribute_data_type
                for attribute_name, attribute_data_type in edges.items()
            },
        )

    @classmethod
    def _from_py_group_schema(cls, py_group_schema: PyGroupSchema) -> GroupSchema:
        """Creates a GroupSchema from a PyGroupSchema.

        Args:
            py_group_schema (PyGroupSchema): The PyGroupSchema to convert.

        Returns:
            GroupSchema: The converted GroupSchema.
        """
        group_schema = cls.__new__(cls)
        group_schema._py_group_schema = py_group_schema
        return group_schema

    @property
    def nodes(self) -> Dict[AttributeName, AttributeDataType]:
        """The attributes the nodes of the group hold.

        Returns:
            Dict[AttributeName, AttributeDataType]: The data type of every node
                attribute.
        """
        return {
            attribute_name: AttributeDataType._from_py_attribute_data_type(
                attribute_data_type
            )
            for attribute_name, attribute_data_type in self._py_group_schema.nodes.items()
        }

    @property
    def edges(self) -> Dict[AttributeName, AttributeDataType]:
        """The attributes the edges of the group hold.

        Returns:
            Dict[AttributeName, AttributeDataType]: The data type of every edge
                attribute.
        """
        return {
            attribute_name: AttributeDataType._from_py_attribute_data_type(
                attribute_data_type
            )
            for attribute_name, attribute_data_type in self._py_group_schema.edges.items()
        }

    def validate_node(self, node_index: NodeIndex, attributes: Attributes) -> None:
        """Validates the attributes of a node against the group schema.

        Args:
            node_index (NodeIndex): The index of the node.
            attributes (Attributes): The attributes of the node.
        """
        self._py_group_schema.validate_node(node_index, attributes)

    def validate_edge(self, edge_index: EdgeIndex, attributes: Attributes) -> None:
        """Validates the attributes of an edge against the group schema.

        Args:
            edge_index (EdgeIndex): The index of the edge.
            attributes (Attributes): The attributes of the edge.
        """
        self._py_group_schema.validate_edge(edge_index._py_edge_index, attributes)

    def __eq__(self, value: object) -> bool:
        """Checks whether the GroupSchema is equal to another one.

        Args:
            value (object): The value to compare.

        Returns:
            bool: True if both describe the same attributes, otherwise False.
        """
        if not isinstance(value, GroupSchema):
            return NotImplemented

        return self._py_group_schema == value._py_group_schema

    def __repr__(self) -> str:
        """Returns the string representation of the GroupSchema.

        Returns:
            str: The string representation of the GroupSchema.
        """
        return repr(self._py_group_schema)


class Schema:
    """The attributes of every group of a GraphRecord, and of its ungrouped part."""

    _py_schema: PySchema

    def __init__(
        self,
        *,
        groups: Optional[Dict[GroupIndex, GroupSchema]] = None,
        ungrouped: Optional[GroupSchema] = None,
        schema_type: SchemaType = SchemaType.Provided,
    ) -> None:
        """Initializes a Schema.

        Args:
            groups (Optional[Dict[GroupIndex, GroupSchema]]): The schema of every
                group. Defaults to no groups.
            ungrouped (Optional[GroupSchema]): The schema of everything outside a
                group. Defaults to no attributes.
            schema_type (SchemaType): Whether the schema is provided or inferred.
                Defaults to SchemaType.Provided.
        """
        if groups is None:
            groups = {}
        if ungrouped is None:
            ungrouped = GroupSchema()

        self._py_schema = PySchema(
            {
                group_index: group_schema._py_group_schema
                for group_index, group_schema in groups.items()
            },
            ungrouped._py_group_schema,
            schema_type._into_py_schema_type(),
        )

    @classmethod
    def _from_py_schema(cls, py_schema: PySchema) -> Schema:
        """Creates a Schema from a PySchema.

        Args:
            py_schema (PySchema): The PySchema to convert.

        Returns:
            Schema: The converted Schema.
        """
        schema = cls.__new__(cls)
        schema._py_schema = py_schema
        return schema

    @classmethod
    def infer(cls, graphrecord: GraphRecord) -> Schema:
        """Infers the schema of a GraphRecord from the data it holds.

        Args:
            graphrecord (GraphRecord): The GraphRecord to infer the schema from.

        Returns:
            Schema: The inferred schema.
        """
        return cls._from_py_schema(PySchema.infer(graphrecord._py_graphrecord))

    @property
    def groups(self) -> Dict[GroupIndex, GroupSchema]:
        """The groups the schema describes.

        Returns:
            Dict[GroupIndex, GroupSchema]: The schema of every described group.
        """
        return {
            group_index: GroupSchema._from_py_group_schema(group_schema)
            for group_index, group_schema in self._py_schema.groups.items()
        }

    def group(self, group_index: GroupIndex) -> GroupSchema:
        """Returns the schema of a single group.

        Args:
            group_index (GroupIndex): The group to return the schema of.

        Returns:
            GroupSchema: The schema of the group.
        """
        return GroupSchema._from_py_group_schema(self._py_schema.group(group_index))

    @property
    def ungrouped(self) -> GroupSchema:
        """The schema of everything outside a group.

        Returns:
            GroupSchema: The schema of the ungrouped part.
        """
        return GroupSchema._from_py_group_schema(self._py_schema.ungrouped)

    @property
    def schema_type(self) -> SchemaType:
        """Whether the schema was provided or inferred.

        Returns:
            SchemaType: The type of the schema.
        """
        return SchemaType._from_py_schema_type(self._py_schema.schema_type)

    def validate_node(
        self,
        node_index: NodeIndex,
        attributes: Attributes,
        group_index: Optional[GroupIndex] = None,
    ) -> None:
        """Validates the attributes of a node against the schema.

        Args:
            node_index (NodeIndex): The index of the node.
            attributes (Attributes): The attributes of the node.
            group_index (Optional[GroupIndex]): The group the node belongs to.
                Defaults to the ungrouped part.
        """
        self._py_schema.validate_node(node_index, attributes, group_index)

    def validate_edge(
        self,
        edge_index: EdgeIndex,
        attributes: Attributes,
        group_index: Optional[GroupIndex] = None,
    ) -> None:
        """Validates the attributes of an edge against the schema.

        Args:
            edge_index (EdgeIndex): The index of the edge.
            attributes (Attributes): The attributes of the edge.
            group_index (Optional[GroupIndex]): The group the edge belongs to.
                Defaults to the ungrouped part.
        """
        self._py_schema.validate_edge(
            edge_index._py_edge_index, attributes, group_index
        )

    def set_node_attribute(
        self,
        attribute_name: AttributeName,
        data_type: DataType,
        attribute_type: AttributeType,
        group_index: Optional[GroupIndex] = None,
    ) -> Schema:
        """Adds a node attribute to the schema, overwriting it if it already exists.

        Args:
            attribute_name (AttributeName): The name of the attribute.
            data_type (DataType): The data type of the attribute.
            attribute_type (AttributeType): The statistical type of the attribute.
            group_index (Optional[GroupIndex]): The group to add the attribute to.
                Defaults to the ungrouped part.

        Returns:
            Schema: A Schema describing the attribute.
        """
        return self._from_py_schema(
            self._py_schema.set_node_attribute(
                attribute_name,
                data_type._inner(),
                attribute_type._into_py_attribute_type(),
                group_index,
            )
        )

    def set_edge_attribute(
        self,
        attribute_name: AttributeName,
        data_type: DataType,
        attribute_type: AttributeType,
        group_index: Optional[GroupIndex] = None,
    ) -> Schema:
        """Adds an edge attribute to the schema, overwriting it if it already exists.

        Args:
            attribute_name (AttributeName): The name of the attribute.
            data_type (DataType): The data type of the attribute.
            attribute_type (AttributeType): The statistical type of the attribute.
            group_index (Optional[GroupIndex]): The group to add the attribute to.
                Defaults to the ungrouped part.

        Returns:
            Schema: A Schema describing the attribute.
        """
        return self._from_py_schema(
            self._py_schema.set_edge_attribute(
                attribute_name,
                data_type._inner(),
                attribute_type._into_py_attribute_type(),
                group_index,
            )
        )

    def update_node_attribute(
        self,
        attribute_name: AttributeName,
        data_type: DataType,
        attribute_type: AttributeType,
        group_index: Optional[GroupIndex] = None,
    ) -> Schema:
        """Widens a node attribute of the schema to also cover the given types.

        Args:
            attribute_name (AttributeName): The name of the attribute.
            data_type (DataType): The data type to cover as well.
            attribute_type (AttributeType): The statistical type of the attribute.
            group_index (Optional[GroupIndex]): The group holding the attribute.
                Defaults to the ungrouped part.

        Returns:
            Schema: A Schema describing the widened attribute.
        """
        return self._from_py_schema(
            self._py_schema.update_node_attribute(
                attribute_name,
                data_type._inner(),
                attribute_type._into_py_attribute_type(),
                group_index,
            )
        )

    def update_edge_attribute(
        self,
        attribute_name: AttributeName,
        data_type: DataType,
        attribute_type: AttributeType,
        group_index: Optional[GroupIndex] = None,
    ) -> Schema:
        """Widens an edge attribute of the schema to also cover the given types.

        Args:
            attribute_name (AttributeName): The name of the attribute.
            data_type (DataType): The data type to cover as well.
            attribute_type (AttributeType): The statistical type of the attribute.
            group_index (Optional[GroupIndex]): The group holding the attribute.
                Defaults to the ungrouped part.

        Returns:
            Schema: A Schema describing the widened attribute.
        """
        return self._from_py_schema(
            self._py_schema.update_edge_attribute(
                attribute_name,
                data_type._inner(),
                attribute_type._into_py_attribute_type(),
                group_index,
            )
        )

    def remove_node_attribute(
        self, attribute_name: AttributeName, group_index: Optional[GroupIndex] = None
    ) -> Schema:
        """Removes a node attribute from the schema.

        Args:
            attribute_name (AttributeName): The name of the attribute to remove.
            group_index (Optional[GroupIndex]): The group holding the attribute.
                Defaults to the ungrouped part.

        Returns:
            Schema: A Schema without that attribute.
        """
        return self._from_py_schema(
            self._py_schema.remove_node_attribute(attribute_name, group_index)
        )

    def remove_edge_attribute(
        self, attribute_name: AttributeName, group_index: Optional[GroupIndex] = None
    ) -> Schema:
        """Removes an edge attribute from the schema.

        Args:
            attribute_name (AttributeName): The name of the attribute to remove.
            group_index (Optional[GroupIndex]): The group holding the attribute.
                Defaults to the ungrouped part.

        Returns:
            Schema: A Schema without that attribute.
        """
        return self._from_py_schema(
            self._py_schema.remove_edge_attribute(attribute_name, group_index)
        )

    def add_group(self, group_index: GroupIndex, group_schema: GroupSchema) -> Schema:
        """Adds the schema of a group.

        Args:
            group_index (GroupIndex): The group to describe.
            group_schema (GroupSchema): The schema of the group.

        Returns:
            Schema: A Schema describing that group.
        """
        return self._from_py_schema(
            self._py_schema.add_group(group_index, group_schema._py_group_schema)
        )

    def remove_group(self, group_index: GroupIndex) -> Schema:
        """Removes the schema of a group.

        Args:
            group_index (GroupIndex): The group to stop describing.

        Returns:
            Schema: A Schema without that group.
        """
        return self._from_py_schema(self._py_schema.remove_group(group_index))

    def freeze(self) -> Schema:
        """Stops the schema from growing with the data written to a GraphRecord.

        Returns:
            Schema: A frozen Schema.
        """
        return self._from_py_schema(self._py_schema.freeze())

    def unfreeze(self) -> Schema:
        """Lets the schema grow with the data written to a GraphRecord again.

        Returns:
            Schema: An unfrozen Schema.
        """
        return self._from_py_schema(self._py_schema.unfreeze())

    def __eq__(self, value: object) -> bool:
        """Checks whether the Schema is equal to another one.

        Args:
            value (object): The value to compare.

        Returns:
            bool: True if both describe the same groups and attributes, otherwise
                False.
        """
        if not isinstance(value, Schema):
            return NotImplemented

        return self._py_schema == value._py_schema

    def __repr__(self) -> str:
        """Returns the string representation of the Schema.

        Returns:
            str: The string representation of the Schema.
        """
        return repr(self._py_schema)
