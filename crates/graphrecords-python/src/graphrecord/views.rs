use super::{
    PyAttributeName, PyAttributes, PyGroupIndex, PyNodeIndex, direction::PyEdgeDirection,
    edge_index::PyEdgeIndex, errors::PyGraphRecordError, selection::ResolvedSelection,
    traits::DeepInto, value::PyValue,
};
use graphrecords_core::{
    errors::GraphRecordError,
    graphrecord::{
        AttributeName, EdgeIndex, EdgeView, GraphRecord, GroupIndex, GroupView, NodeIndex, NodeView,
    },
};
use pyo3::{Bound, PyAny, PyResult, prelude::*};

#[pyclass(frozen, module = "graphrecords._graphrecords.graphrecord")]
pub struct PyNodeView {
    graphrecord: GraphRecord,
    node_index: NodeIndex,
}

impl PyNodeView {
    pub fn new(graphrecord: GraphRecord, node_index: NodeIndex) -> PyResult<Self> {
        graphrecord
            .node(&node_index)
            .map_err(PyGraphRecordError::from)?;

        Ok(Self {
            graphrecord,
            node_index,
        })
    }

    fn view(&self) -> NodeView<'_> {
        self.graphrecord
            .node(&self.node_index)
            .expect("Node must exist.")
    }
}

#[pymethods]
impl PyNodeView {
    pub fn index(&self) -> PyNodeIndex {
        self.node_index.clone().into()
    }

    pub fn attribute(&self, attribute_name: PyAttributeName) -> PyResult<PyValue> {
        let attribute_name = AttributeName::from(attribute_name);
        let value = self.view().attribute(&attribute_name).ok_or_else(|| {
            PyGraphRecordError::from(GraphRecordError::NodeAttributeNotFound {
                node_index: self.node_index.clone(),
                attribute_name,
            })
        })?;

        Ok(value.deep_into())
    }

    pub fn attributes(&self) -> PyAttributes {
        self.view().attributes().map(DeepInto::deep_into).collect()
    }

    pub fn groups(&self) -> Vec<PyGroupIndex> {
        self.view().groups().map(DeepInto::deep_into).collect()
    }

    #[pyo3(signature = (direction=PyEdgeDirection::Both))]
    pub fn edges(&self, direction: PyEdgeDirection) -> Vec<PyEdgeIndex> {
        self.view()
            .edges(direction.into())
            .map(DeepInto::deep_into)
            .collect()
    }

    #[pyo3(signature = (direction=PyEdgeDirection::Both))]
    pub fn neighbors(&self, direction: PyEdgeDirection) -> Vec<PyNodeIndex> {
        self.view()
            .neighbors(direction.into())
            .map(DeepInto::deep_into)
            .collect()
    }

    #[pyo3(signature = (direction=PyEdgeDirection::Both))]
    pub fn degree(&self, direction: PyEdgeDirection) -> usize {
        self.view().degree(direction.into())
    }

    #[pyo3(signature = (target, direction=PyEdgeDirection::Outgoing))]
    pub fn edges_to(
        &self,
        target: &Bound<'_, PyAny>,
        direction: PyEdgeDirection,
    ) -> PyResult<Vec<PyEdgeIndex>> {
        let target = ResolvedSelection::single_node(&self.graphrecord, target)?;

        Ok(self
            .view()
            .edges_to(&target, direction.into())
            .map_err(PyGraphRecordError::from)?
            .map(DeepInto::deep_into)
            .collect())
    }

    pub fn __repr__(&self) -> String {
        format!("NodeView({})", self.node_index)
    }
}

#[pyclass(frozen, module = "graphrecords._graphrecords.graphrecord")]
pub struct PyEdgeView {
    graphrecord: GraphRecord,
    edge_index: EdgeIndex,
}

impl PyEdgeView {
    pub fn new(graphrecord: GraphRecord, edge_index: EdgeIndex) -> PyResult<Self> {
        graphrecord
            .edge(&edge_index)
            .map_err(PyGraphRecordError::from)?;

        Ok(Self {
            graphrecord,
            edge_index,
        })
    }

    fn view(&self) -> EdgeView<'_> {
        self.graphrecord
            .edge(&self.edge_index)
            .expect("Edge must exist.")
    }
}

#[pymethods]
impl PyEdgeView {
    pub fn index(&self) -> PyEdgeIndex {
        self.edge_index.into()
    }

    pub fn source(&self) -> PyNodeIndex {
        self.view().source().deep_into()
    }

    pub fn target(&self) -> PyNodeIndex {
        self.view().target().deep_into()
    }

    pub fn attribute(&self, attribute_name: PyAttributeName) -> PyResult<PyValue> {
        let attribute_name = AttributeName::from(attribute_name);
        let value = self.view().attribute(&attribute_name).ok_or_else(|| {
            PyGraphRecordError::from(GraphRecordError::EdgeAttributeNotFound {
                edge_index: self.edge_index,
                attribute_name,
            })
        })?;

        Ok(value.deep_into())
    }

    pub fn attributes(&self) -> PyAttributes {
        self.view().attributes().map(DeepInto::deep_into).collect()
    }

    pub fn groups(&self) -> Vec<PyGroupIndex> {
        self.view().groups().map(DeepInto::deep_into).collect()
    }

    pub fn __repr__(&self) -> String {
        format!("EdgeView({})", self.edge_index)
    }
}

#[pyclass(frozen, module = "graphrecords._graphrecords.graphrecord")]
pub struct PyGroupView {
    graphrecord: GraphRecord,
    group_index: GroupIndex,
}

impl PyGroupView {
    pub fn new(graphrecord: GraphRecord, group_index: GroupIndex) -> PyResult<Self> {
        graphrecord
            .group(&group_index)
            .map_err(PyGraphRecordError::from)?;

        Ok(Self {
            graphrecord,
            group_index,
        })
    }

    fn view(&self) -> GroupView<'_> {
        self.graphrecord
            .group(&self.group_index)
            .expect("Group must exist.")
    }
}

#[pymethods]
impl PyGroupView {
    pub fn index(&self) -> PyGroupIndex {
        self.group_index.clone().into()
    }

    pub fn nodes(&self) -> Vec<PyNodeIndex> {
        self.view().nodes().map(DeepInto::deep_into).collect()
    }

    pub fn edges(&self) -> Vec<PyEdgeIndex> {
        self.view().edges().map(DeepInto::deep_into).collect()
    }

    pub fn node_count(&self) -> usize {
        self.view().node_count()
    }

    pub fn edge_count(&self) -> usize {
        self.view().edge_count()
    }

    pub fn __repr__(&self) -> String {
        format!("GroupView({})", self.group_index)
    }
}
