from typing import Dict, List, TypedDict, Union

from graphrecords._graphrecords.datatype import PyDataType
from graphrecords._graphrecords.schema import PyAttributeType
from graphrecords.types import AttributeName, GroupIndex, Value

PY_DEFAULT_TRUNCATE_DETAILS: int

class PyCategoricalAttributeOverview(TypedDict):
    attribute_type: PyAttributeType
    distinct_values: List[Value]

class PyContinuousAttributeOverview(TypedDict):
    attribute_type: PyAttributeType
    min: Value
    mean: Value
    max: Value

class PyTemporalAttributeOverview(TypedDict):
    attribute_type: PyAttributeType
    min: Value
    max: Value

class PyUnstructuredAttributeOverview(TypedDict):
    attribute_type: PyAttributeType
    distinct_count: int

class PyAttributeOverview:
    data_type: PyDataType
    data: Union[
        PyCategoricalAttributeOverview,
        PyContinuousAttributeOverview,
        PyTemporalAttributeOverview,
        PyUnstructuredAttributeOverview,
    ]

class PyNodeGroupOverview:
    count: int
    attributes: Dict[AttributeName, PyAttributeOverview]

class PyEdgeGroupOverview:
    count: int
    attributes: Dict[AttributeName, PyAttributeOverview]

class PyGroupOverview:
    node_overview: PyNodeGroupOverview
    edge_overview: PyEdgeGroupOverview

class PyOverview:
    ungrouped_overview: PyGroupOverview
    grouped_overviews: Dict[GroupIndex, PyGroupOverview]
