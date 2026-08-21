"""Overview classes summarizing the contents of a GraphRecord."""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Dict,
    Final,
    List,
    Literal,
    TypeAlias,
    TypedDict,
    Union,
)

from graphrecords._graphrecords.overview import PY_DEFAULT_TRUNCATE_DETAILS
from graphrecords.datatype import DataType
from graphrecords.schema import AttributeType

if TYPE_CHECKING:
    from graphrecords._graphrecords.overview import (
        PyAttributeOverview,
        PyEdgeGroupOverview,
        PyGroupOverview,
        PyNodeGroupOverview,
        PyOverview,
    )
    from graphrecords.types import AttributeName, GroupIndex, Value

#: The number of details an overview shows before it truncates them.
DEFAULT_TRUNCATE_DETAILS: Final[int] = PY_DEFAULT_TRUNCATE_DETAILS


class CategoricalAttributeOverview(TypedDict):
    """The overview data of a categorical attribute."""

    attribute_type: Literal[AttributeType.Categorical]
    distinct_values: List[Value]


class ContinuousAttributeOverview(TypedDict):
    """The overview data of a continuous attribute."""

    attribute_type: Literal[AttributeType.Continuous]
    min: Value
    mean: Value
    max: Value


class TemporalAttributeOverview(TypedDict):
    """The overview data of a temporal attribute."""

    attribute_type: Literal[AttributeType.Temporal]
    min: Value
    max: Value


class UnstructuredAttributeOverview(TypedDict):
    """The overview data of an unstructured attribute."""

    attribute_type: Literal[AttributeType.Unstructured]
    distinct_count: int


#: A type alias for the overview data of an attribute of any type.
AttributeOverviewData: TypeAlias = Union[
    CategoricalAttributeOverview,
    ContinuousAttributeOverview,
    TemporalAttributeOverview,
    UnstructuredAttributeOverview,
]


class AttributeOverview:
    """Overview data of an attribute."""

    _py_attribute_overview: PyAttributeOverview

    @classmethod
    def _from_py_attribute_overview(
        cls, py_attribute_overview: PyAttributeOverview
    ) -> AttributeOverview:
        """Creates an AttributeOverview from a PyAttributeOverview.

        Args:
            py_attribute_overview (PyAttributeOverview): The PyAttributeOverview to
                convert.

        Returns:
            AttributeOverview: The converted AttributeOverview.
        """
        attribute_overview = cls.__new__(cls)
        attribute_overview._py_attribute_overview = py_attribute_overview
        return attribute_overview

    @property
    def data_type(self) -> DataType:
        """The data type of the attribute.

        Returns:
            DataType: The data type of the attribute.
        """
        return DataType._from_py_data_type(self._py_attribute_overview.data_type)

    @property
    def data(self) -> AttributeOverviewData:
        """The overview data of the attribute.

        Returns:
            AttributeOverviewData: The overview data, shaped by the statistical type
                of the attribute.
        """
        data = self._py_attribute_overview.data

        if "distinct_values" in data:
            return {
                "attribute_type": AttributeType.Categorical,
                "distinct_values": data["distinct_values"],
            }

        if "mean" in data:
            return {
                "attribute_type": AttributeType.Continuous,
                "min": data["min"],
                "mean": data["mean"],
                "max": data["max"],
            }

        if "min" in data:
            return {
                "attribute_type": AttributeType.Temporal,
                "min": data["min"],
                "max": data["max"],
            }

        return {
            "attribute_type": AttributeType.Unstructured,
            "distinct_count": data["distinct_count"],
        }

    def __repr__(self) -> str:
        """Returns the string representation of the AttributeOverview.

        Returns:
            str: The string representation of the AttributeOverview.
        """
        return repr(self._py_attribute_overview)


class NodeGroupOverview:
    """Overview data of the nodes of a group."""

    _py_node_group_overview: PyNodeGroupOverview

    @classmethod
    def _from_py_node_group_overview(
        cls, py_node_group_overview: PyNodeGroupOverview
    ) -> NodeGroupOverview:
        """Creates a NodeGroupOverview from a PyNodeGroupOverview.

        Args:
            py_node_group_overview (PyNodeGroupOverview): The PyNodeGroupOverview to
                convert.

        Returns:
            NodeGroupOverview: The converted NodeGroupOverview.
        """
        node_group_overview = cls.__new__(cls)
        node_group_overview._py_node_group_overview = py_node_group_overview
        return node_group_overview

    @property
    def count(self) -> int:
        """The number of nodes in the group.

        Returns:
            int: The number of nodes in the group.
        """
        return self._py_node_group_overview.count

    @property
    def attributes(self) -> Dict[AttributeName, AttributeOverview]:
        """The attribute overviews of the nodes.

        Returns:
            Dict[AttributeName, AttributeOverview]: The overview of every node
                attribute.
        """
        return {
            attribute_name: AttributeOverview._from_py_attribute_overview(
                attribute_overview
            )
            for attribute_name, attribute_overview in (
                self._py_node_group_overview.attributes.items()
            )
        }

    def __repr__(self) -> str:
        """Returns the string representation of the NodeGroupOverview.

        Returns:
            str: The string representation of the NodeGroupOverview.
        """
        return repr(self._py_node_group_overview)


class EdgeGroupOverview:
    """Overview data of the edges of a group."""

    _py_edge_group_overview: PyEdgeGroupOverview

    @classmethod
    def _from_py_edge_group_overview(
        cls, py_edge_group_overview: PyEdgeGroupOverview
    ) -> EdgeGroupOverview:
        """Creates an EdgeGroupOverview from a PyEdgeGroupOverview.

        Args:
            py_edge_group_overview (PyEdgeGroupOverview): The PyEdgeGroupOverview to
                convert.

        Returns:
            EdgeGroupOverview: The converted EdgeGroupOverview.
        """
        edge_group_overview = cls.__new__(cls)
        edge_group_overview._py_edge_group_overview = py_edge_group_overview
        return edge_group_overview

    @property
    def count(self) -> int:
        """The number of edges in the group.

        Returns:
            int: The number of edges in the group.
        """
        return self._py_edge_group_overview.count

    @property
    def attributes(self) -> Dict[AttributeName, AttributeOverview]:
        """The attribute overviews of the edges.

        Returns:
            Dict[AttributeName, AttributeOverview]: The overview of every edge
                attribute.
        """
        return {
            attribute_name: AttributeOverview._from_py_attribute_overview(
                attribute_overview
            )
            for attribute_name, attribute_overview in (
                self._py_edge_group_overview.attributes.items()
            )
        }

    def __repr__(self) -> str:
        """Returns the string representation of the EdgeGroupOverview.

        Returns:
            str: The string representation of the EdgeGroupOverview.
        """
        return repr(self._py_edge_group_overview)


class GroupOverview:
    """Overview data of the nodes and edges of a group."""

    _py_group_overview: PyGroupOverview

    @classmethod
    def _from_py_group_overview(
        cls, py_group_overview: PyGroupOverview
    ) -> GroupOverview:
        """Creates a GroupOverview from a PyGroupOverview.

        Args:
            py_group_overview (PyGroupOverview): The PyGroupOverview to convert.

        Returns:
            GroupOverview: The converted GroupOverview.
        """
        group_overview = cls.__new__(cls)
        group_overview._py_group_overview = py_group_overview
        return group_overview

    @property
    def node_overview(self) -> NodeGroupOverview:
        """The overview of the nodes of the group.

        Returns:
            NodeGroupOverview: The overview of the nodes.
        """
        return NodeGroupOverview._from_py_node_group_overview(
            self._py_group_overview.node_overview
        )

    @property
    def edge_overview(self) -> EdgeGroupOverview:
        """The overview of the edges of the group.

        Returns:
            EdgeGroupOverview: The overview of the edges.
        """
        return EdgeGroupOverview._from_py_edge_group_overview(
            self._py_group_overview.edge_overview
        )

    def __repr__(self) -> str:
        """Returns the string representation of the GroupOverview.

        Returns:
            str: The string representation of the GroupOverview.
        """
        return repr(self._py_group_overview)


class Overview:
    """Overview data of every group of a GraphRecord, and of its ungrouped part."""

    _py_overview: PyOverview

    @classmethod
    def _from_py_overview(cls, py_overview: PyOverview) -> Overview:
        """Creates an Overview from a PyOverview.

        Args:
            py_overview (PyOverview): The PyOverview to convert.

        Returns:
            Overview: The converted Overview.
        """
        overview = cls.__new__(cls)
        overview._py_overview = py_overview
        return overview

    @property
    def ungrouped_overview(self) -> GroupOverview:
        """The overview of everything outside a group.

        Returns:
            GroupOverview: The overview of the ungrouped part.
        """
        return GroupOverview._from_py_group_overview(
            self._py_overview.ungrouped_overview
        )

    @property
    def grouped_overviews(self) -> Dict[GroupIndex, GroupOverview]:
        """The overview of every group.

        Returns:
            Dict[GroupIndex, GroupOverview]: The overview of every group.
        """
        return {
            group_index: GroupOverview._from_py_group_overview(group_overview)
            for group_index, group_overview in (
                self._py_overview.grouped_overviews.items()
            )
        }

    def __repr__(self) -> str:
        """Returns the string representation of the Overview.

        Returns:
            str: The string representation of the Overview.
        """
        return repr(self._py_overview)
