from typing import Dict, Union

from typing_extensions import Final

from graphrecords._graphrecords.datatype import PyDataType
from graphrecords.types import (
    AttributeName,
    Group,
    PyCategoricalAttributeOverview,
    PyContinuousAttributeOverview,
    PyTemporalAttributeOverview,
    PyUnstructuredAttributeOverview,
)

PY_DEFAULT_TRUNCATE_DETAILS: Final[int] = ...

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
    grouped_overviews: Dict[Group, PyGroupOverview]
