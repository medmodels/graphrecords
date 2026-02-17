#![allow(clippy::new_without_default, clippy::significant_drop_tightening)]

pub mod attribute;
mod borrowed;
pub mod datatype;
pub mod errors;
pub mod overview;
mod plugins;
pub mod querying;
pub mod schema;
pub mod traits;
pub mod value;

use crate::{
    conversion_lut::ConversionLut,
    graphrecord::{
        overview::{PyGroupOverview, PyOverview},
        plugins::PyPlugin,
    },
};
use attribute::PyGraphRecordAttribute;
use borrowed::BorrowedGraphRecord;
use errors::PyGraphRecordError;
use graphrecords_core::{
    errors::GraphRecordError,
    graphrecord::{
        Attributes, EdgeIndex, GraphRecord, GraphRecordAttribute, GraphRecordValue, plugins::Plugin,
    },
};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use pyo3::{
    exceptions::PyRuntimeError,
    prelude::*,
    types::{PyBytes, PyDict, PyFunction},
};
use pyo3_polars::PyDataFrame;
use querying::{PyReturnOperand, edges::PyEdgeOperand, nodes::PyNodeOperand};
use schema::PySchema;
use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
    ptr::NonNull,
};
use traits::DeepInto;
use value::PyGraphRecordValue;

pub type PyAttributes = HashMap<PyGraphRecordAttribute, PyGraphRecordValue>;
pub type PyGroup = PyGraphRecordAttribute;
pub type PyNodeIndex = PyGraphRecordAttribute;
pub type PyEdgeIndex = EdgeIndex;
type Lut<T> = ConversionLut<usize, fn(&Bound<'_, PyAny>) -> PyResult<T>>;

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyGraphRecord {
    inner: PyGraphRecordInner,
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum PyGraphRecordInner {
    Owned(RwLock<GraphRecord>),
    Borrowed(BorrowedGraphRecord),
}

pub(crate) enum InnerRef<'a> {
    Owned(RwLockReadGuard<'a, GraphRecord>),
    Borrowed(RwLockReadGuard<'a, Option<NonNull<GraphRecord>>>),
}

impl Deref for InnerRef<'_> {
    type Target = GraphRecord;

    fn deref(&self) -> &GraphRecord {
        match self {
            InnerRef::Owned(guard) => guard,
            // SAFETY: The guard is only constructed after checking `is_some()` in `inner()`.
            // The pointer is valid for the duration of the `scope()` call because the scope's
            // Drop guard needs a write lock to clear it, and this read guard prevents that.
            InnerRef::Borrowed(guard) => unsafe {
                guard
                    .expect("Borrowed pointer must be Some when InnerRef is alive")
                    .as_ref()
            },
        }
    }
}

pub(crate) enum InnerRefMut<'a> {
    Owned(RwLockWriteGuard<'a, GraphRecord>),
    Borrowed(RwLockWriteGuard<'a, Option<NonNull<GraphRecord>>>),
}

impl Deref for InnerRefMut<'_> {
    type Target = GraphRecord;

    fn deref(&self) -> &GraphRecord {
        match self {
            InnerRefMut::Owned(guard) => guard,
            // SAFETY: Same as `InnerRef::Borrowed`. Pointer was checked `is_some()` in
            // `inner_mut()`, and the write guard keeps the scope's Drop from clearing it.
            InnerRefMut::Borrowed(guard) => unsafe {
                guard
                    .expect("Borrowed pointer must be Some when InnerRefMut is alive")
                    .as_ref()
            },
        }
    }
}

impl DerefMut for InnerRefMut<'_> {
    fn deref_mut(&mut self) -> &mut GraphRecord {
        match self {
            InnerRefMut::Owned(guard) => &mut *guard,
            // SAFETY: Same as above, plus: the write guard ensures exclusive access to the
            // pointer, so creating `&mut GraphRecord` is sound. The original `scope()` call
            // holds `&mut GraphRecord`, guaranteeing no other references to the pointee exist
            // outside this lock.
            InnerRefMut::Borrowed(guard) => unsafe {
                guard
                    .expect("Borrowed pointer must be Some when InnerRefMut is alive")
                    .as_mut()
            },
        }
    }
}

impl Clone for PyGraphRecord {
    fn clone(&self) -> Self {
        match &self.inner {
            PyGraphRecordInner::Owned(lock) => Self {
                inner: PyGraphRecordInner::Owned(RwLock::new(lock.read().clone())),
            },
            PyGraphRecordInner::Borrowed(_) => Self {
                inner: PyGraphRecordInner::Borrowed(BorrowedGraphRecord::dead()),
            },
        }
    }
}

impl PyGraphRecord {
    pub(crate) fn inner(&self) -> PyResult<InnerRef<'_>> {
        match &self.inner {
            PyGraphRecordInner::Owned(lock) => Ok(InnerRef::Owned(lock.read())),
            PyGraphRecordInner::Borrowed(borrowed) => {
                let guard = borrowed.read();
                if guard.is_some() {
                    Ok(InnerRef::Borrowed(guard))
                } else {
                    Err(PyRuntimeError::new_err(
                        "GraphRecord reference is no longer valid (used outside plugin callback scope)",
                    ))
                }
            }
        }
    }

    pub(crate) fn inner_mut(&self) -> PyResult<InnerRefMut<'_>> {
        match &self.inner {
            PyGraphRecordInner::Owned(lock) => Ok(InnerRefMut::Owned(lock.write())),
            PyGraphRecordInner::Borrowed(borrowed) => {
                let guard = borrowed.write();
                if guard.is_some() {
                    Ok(InnerRefMut::Borrowed(guard))
                } else {
                    Err(PyRuntimeError::new_err(
                        "GraphRecord reference is no longer valid (used outside plugin callback scope)",
                    ))
                }
            }
        }
    }
}

impl From<GraphRecord> for PyGraphRecord {
    fn from(value: GraphRecord) -> Self {
        Self {
            inner: PyGraphRecordInner::Owned(RwLock::new(value)),
        }
    }
}

impl TryFrom<PyGraphRecord> for GraphRecord {
    type Error = PyErr;

    fn try_from(value: PyGraphRecord) -> PyResult<Self> {
        match value.inner {
            PyGraphRecordInner::Owned(lock) => Ok(lock.into_inner()),
            PyGraphRecordInner::Borrowed(_) => Err(PyRuntimeError::new_err(
                "Cannot convert a borrowed PyGraphRecord into an owned GraphRecord",
            )),
        }
    }
}

#[pymethods]
impl PyGraphRecord {
    #[new]
    pub fn new() -> Self {
        GraphRecord::new().into()
    }

    pub fn _to_bytes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let bytes = bincode::serialize(&*self.inner()?)
            .map_err(|_| {
                GraphRecordError::ConversionError("Could not serialize GraphRecord".into())
            })
            .map_err(PyGraphRecordError::from)?;

        Ok(PyBytes::new(py, &bytes))
    }

    #[staticmethod]
    pub fn _from_bytes(data: &Bound<'_, PyBytes>) -> PyResult<Self> {
        let graphrecord: GraphRecord = bincode::deserialize(data.as_bytes())
            .map_err(|_| {
                GraphRecordError::ConversionError("Could not deserialize GraphRecord".into())
            })
            .map_err(PyGraphRecordError::from)?;

        Ok(graphrecord.into())
    }

    #[staticmethod]
    pub fn with_schema(schema: PySchema) -> Self {
        GraphRecord::with_schema(schema.into()).into()
    }

    #[staticmethod]
    pub fn with_plugins(plugins: Vec<Py<PyAny>>) -> Self {
        let plugins = plugins
            .into_iter()
            .map(|plugin| Box::new(PyPlugin::new(plugin)) as Box<dyn Plugin>)
            .collect();

        GraphRecord::with_plugins(plugins).into()
    }

    #[staticmethod]
    #[pyo3(signature = (nodes, edges=None))]
    pub fn from_tuples(
        nodes: Vec<(PyNodeIndex, PyAttributes)>,
        edges: Option<Vec<(PyNodeIndex, PyNodeIndex, PyAttributes)>>,
    ) -> PyResult<Self> {
        Ok(
            GraphRecord::from_tuples(nodes.deep_into(), edges.deep_into(), None)
                .map_err(PyGraphRecordError::from)?
                .into(),
        )
    }

    #[staticmethod]
    pub fn from_dataframes(
        nodes_dataframes: Vec<(PyDataFrame, String)>,
        edges_dataframes: Vec<(PyDataFrame, String, String)>,
    ) -> PyResult<Self> {
        Ok(
            GraphRecord::from_dataframes(nodes_dataframes, edges_dataframes, None)
                .map_err(PyGraphRecordError::from)?
                .into(),
        )
    }

    #[staticmethod]
    pub fn from_nodes_dataframes(nodes_dataframes: Vec<(PyDataFrame, String)>) -> PyResult<Self> {
        Ok(GraphRecord::from_nodes_dataframes(nodes_dataframes, None)
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    #[staticmethod]
    pub fn from_ron(path: &str) -> PyResult<Self> {
        Ok(GraphRecord::from_ron(path)
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    pub fn to_ron(&self, path: &str) -> PyResult<()> {
        Ok(self
            .inner()?
            .to_ron(path)
            .map_err(PyGraphRecordError::from)?)
    }

    #[allow(clippy::missing_panics_doc, reason = "infallible")]
    pub fn to_dataframes(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let export = self
            .inner()?
            .to_dataframes()
            .map_err(PyGraphRecordError::from)?;

        let outer_dict = PyDict::new(py);
        let inner_dict = PyDict::new(py);

        for (group, group_export) in export.groups {
            let group_dict = PyDict::new(py);

            let nodes_df = PyDataFrame(group_export.nodes);
            group_dict
                .set_item("nodes", nodes_df)
                .expect("Setting item must succeed");

            let edges_df = PyDataFrame(group_export.edges);
            group_dict
                .set_item("edges", edges_df)
                .expect("Setting item must succeed");

            inner_dict
                .set_item(PyGraphRecordAttribute::from(group), group_dict)
                .expect("Setting item must succeed");
        }

        outer_dict
            .set_item("groups", inner_dict)
            .expect("Setting item must succeed");

        let ungrouped_dict = PyDict::new(py);

        let nodes_df = PyDataFrame(export.ungrouped.nodes);
        ungrouped_dict
            .set_item("nodes", nodes_df)
            .expect("Setting item must succeed");

        let edges_df = PyDataFrame(export.ungrouped.edges);
        ungrouped_dict
            .set_item("edges", edges_df)
            .expect("Setting item must succeed");

        outer_dict
            .set_item("ungrouped", ungrouped_dict)
            .expect("Setting item must succeed");

        Ok(outer_dict.into())
    }

    pub fn get_schema(&self) -> PyResult<PySchema> {
        Ok(self.inner()?.get_schema().clone().into())
    }

    pub fn set_schema(&self, schema: PySchema) -> PyResult<()> {
        Ok(self
            .inner_mut()?
            .set_schema(schema.into())
            .map_err(PyGraphRecordError::from)?)
    }

    pub fn freeze_schema(&self) -> PyResult<()> {
        let _: () = self.inner_mut()?.freeze_schema();
        Ok(())
    }

    pub fn unfreeze_schema(&self) -> PyResult<()> {
        let _: () = self.inner_mut()?.unfreeze_schema();
        Ok(())
    }

    #[getter]
    pub fn nodes(&self) -> PyResult<Vec<PyNodeIndex>> {
        Ok(self
            .inner()?
            .node_indices()
            .map(|node_index| node_index.clone().into())
            .collect())
    }

    pub fn node(
        &self,
        node_index: Vec<PyNodeIndex>,
    ) -> PyResult<HashMap<PyNodeIndex, PyAttributes>> {
        let graphrecord = self.inner()?;

        node_index
            .into_iter()
            .map(|node_index| {
                let node_attributes = graphrecord
                    .node_attributes(&node_index)
                    .map_err(PyGraphRecordError::from)?;

                Ok((node_index, node_attributes.deep_into()))
            })
            .collect()
    }

    #[getter]
    pub fn edges(&self) -> PyResult<Vec<EdgeIndex>> {
        Ok(self.inner()?.edge_indices().copied().collect())
    }

    pub fn edge(&self, edge_index: Vec<EdgeIndex>) -> PyResult<HashMap<EdgeIndex, PyAttributes>> {
        let graphrecord = self.inner()?;

        edge_index
            .into_iter()
            .map(|edge_index| {
                let edge_attributes = graphrecord
                    .edge_attributes(&edge_index)
                    .map_err(PyGraphRecordError::from)?;

                Ok((edge_index, edge_attributes.deep_into()))
            })
            .collect()
    }

    #[getter]
    pub fn groups(&self) -> PyResult<Vec<PyGroup>> {
        Ok(self
            .inner()?
            .groups()
            .map(|group| group.clone().into())
            .collect())
    }

    pub fn outgoing_edges(
        &self,
        node_index: Vec<PyNodeIndex>,
    ) -> PyResult<HashMap<PyNodeIndex, Vec<EdgeIndex>>> {
        let graphrecord = self.inner()?;

        node_index
            .into_iter()
            .map(|node_index| {
                let edges = graphrecord
                    .outgoing_edges(&node_index)
                    .map_err(PyGraphRecordError::from)?
                    .copied()
                    .collect();

                Ok((node_index, edges))
            })
            .collect()
    }

    pub fn incoming_edges(
        &self,
        node_index: Vec<PyNodeIndex>,
    ) -> PyResult<HashMap<PyNodeIndex, Vec<EdgeIndex>>> {
        let graphrecord = self.inner()?;

        node_index
            .into_iter()
            .map(|node_index| {
                let edges = graphrecord
                    .incoming_edges(&node_index)
                    .map_err(PyGraphRecordError::from)?
                    .copied()
                    .collect();

                Ok((node_index, edges))
            })
            .collect()
    }

    pub fn edge_endpoints(
        &self,
        edge_index: Vec<EdgeIndex>,
    ) -> PyResult<HashMap<EdgeIndex, (PyNodeIndex, PyNodeIndex)>> {
        let graphrecord = self.inner()?;

        edge_index
            .into_iter()
            .map(|edge_index| {
                let edge_endpoints = graphrecord
                    .edge_endpoints(&edge_index)
                    .map_err(PyGraphRecordError::from)?;

                Ok((
                    edge_index,
                    (
                        edge_endpoints.0.clone().into(),
                        edge_endpoints.1.clone().into(),
                    ),
                ))
            })
            .collect()
    }

    pub fn edges_connecting(
        &self,
        source_node_indices: Vec<PyNodeIndex>,
        target_node_indices: Vec<PyNodeIndex>,
    ) -> PyResult<Vec<EdgeIndex>> {
        let source_node_indices: Vec<GraphRecordAttribute> = source_node_indices.deep_into();
        let target_node_indices: Vec<GraphRecordAttribute> = target_node_indices.deep_into();

        Ok(self
            .inner()?
            .edges_connecting(
                source_node_indices.iter().collect(),
                target_node_indices.iter().collect(),
            )
            .copied()
            .collect())
    }

    pub fn edges_connecting_undirected(
        &self,
        first_node_indices: Vec<PyNodeIndex>,
        second_node_indices: Vec<PyNodeIndex>,
    ) -> PyResult<Vec<EdgeIndex>> {
        let first_node_indices: Vec<GraphRecordAttribute> = first_node_indices.deep_into();
        let second_node_indices: Vec<GraphRecordAttribute> = second_node_indices.deep_into();

        Ok(self
            .inner()?
            .edges_connecting_undirected(
                first_node_indices.iter().collect(),
                second_node_indices.iter().collect(),
            )
            .copied()
            .collect())
    }

    pub fn remove_nodes(
        &self,
        node_indices: Vec<PyNodeIndex>,
    ) -> PyResult<HashMap<PyNodeIndex, PyAttributes>> {
        let mut graphrecord = self.inner_mut()?;

        node_indices
            .into_iter()
            .map(|node_index| {
                let attributes = graphrecord
                    .remove_node(&node_index)
                    .map_err(PyGraphRecordError::from)?;

                Ok((node_index, attributes.deep_into()))
            })
            .collect()
    }

    pub fn replace_node_attributes(
        &self,
        node_indices: Vec<PyNodeIndex>,
        attributes: PyAttributes,
    ) -> PyResult<()> {
        let mut graphrecord = self.inner_mut()?;

        let attributes: Attributes = attributes.deep_into();

        for node_index in node_indices {
            let mut current_attributes = graphrecord
                .node_attributes_mut(&node_index)
                .map_err(PyGraphRecordError::from)?;

            current_attributes
                .replace_attributes(attributes.clone())
                .map_err(PyGraphRecordError::from)?;
        }

        Ok(())
    }

    pub fn update_node_attribute(
        &self,
        node_indices: Vec<PyNodeIndex>,
        attribute: PyGraphRecordAttribute,
        value: PyGraphRecordValue,
    ) -> PyResult<()> {
        let mut graphrecord = self.inner_mut()?;

        let attribute: GraphRecordAttribute = attribute.into();
        let value: GraphRecordValue = value.into();

        for node_index in node_indices {
            let mut node_attributes = graphrecord
                .node_attributes_mut(&node_index)
                .map_err(PyGraphRecordError::from)?;

            node_attributes
                .update_attribute(&attribute, value.clone())
                .map_err(PyGraphRecordError::from)?;
        }

        Ok(())
    }

    pub fn remove_node_attribute(
        &self,
        node_indices: Vec<PyNodeIndex>,
        attribute: PyGraphRecordAttribute,
    ) -> PyResult<()> {
        let mut graphrecord = self.inner_mut()?;

        let attribute: GraphRecordAttribute = attribute.into();

        for node_index in node_indices {
            let mut node_attributes = graphrecord
                .node_attributes_mut(&node_index)
                .map_err(PyGraphRecordError::from)?;

            node_attributes
                .remove_attribute(&attribute)
                .map_err(PyGraphRecordError::from)?;
        }

        Ok(())
    }

    pub fn add_nodes(&self, nodes: Vec<(PyNodeIndex, PyAttributes)>) -> PyResult<()> {
        Ok(self
            .inner_mut()?
            .add_nodes(nodes.deep_into())
            .map_err(PyGraphRecordError::from)?)
    }

    pub fn add_nodes_with_group(
        &self,
        nodes: Vec<(PyNodeIndex, PyAttributes)>,
        group: PyGroup,
    ) -> PyResult<()> {
        Ok(self
            .inner_mut()?
            .add_nodes_with_group(nodes.deep_into(), group.into())
            .map_err(PyGraphRecordError::from)?)
    }

    pub fn add_nodes_dataframes(
        &self,
        nodes_dataframes: Vec<(PyDataFrame, String)>,
    ) -> PyResult<()> {
        Ok(self
            .inner_mut()?
            .add_nodes_dataframes(nodes_dataframes)
            .map_err(PyGraphRecordError::from)?)
    }

    pub fn add_nodes_dataframes_with_group(
        &self,
        nodes_dataframes: Vec<(PyDataFrame, String)>,
        group: PyGroup,
    ) -> PyResult<()> {
        Ok(self
            .inner_mut()?
            .add_nodes_dataframes_with_group(nodes_dataframes, group.into())
            .map_err(PyGraphRecordError::from)?)
    }

    pub fn remove_edges(
        &self,
        edge_indices: Vec<EdgeIndex>,
    ) -> PyResult<HashMap<EdgeIndex, PyAttributes>> {
        let mut graphrecord = self.inner_mut()?;

        edge_indices
            .into_iter()
            .map(|edge_index| {
                let attributes = graphrecord
                    .remove_edge(&edge_index)
                    .map_err(PyGraphRecordError::from)?;

                Ok((edge_index, attributes.deep_into()))
            })
            .collect()
    }

    pub fn replace_edge_attributes(
        &self,
        edge_indices: Vec<EdgeIndex>,
        attributes: PyAttributes,
    ) -> PyResult<()> {
        let mut graphrecord = self.inner_mut()?;

        let attributes: Attributes = attributes.deep_into();

        for edge_index in edge_indices {
            let mut current_attributes = graphrecord
                .edge_attributes_mut(&edge_index)
                .map_err(PyGraphRecordError::from)?;

            current_attributes
                .replace_attributes(attributes.clone())
                .map_err(PyGraphRecordError::from)?;
        }

        Ok(())
    }

    pub fn update_edge_attribute(
        &self,
        edge_indices: Vec<EdgeIndex>,
        attribute: PyGraphRecordAttribute,
        value: PyGraphRecordValue,
    ) -> PyResult<()> {
        let mut graphrecord = self.inner_mut()?;

        let attribute: GraphRecordAttribute = attribute.into();
        let value: GraphRecordValue = value.into();

        for edge_index in edge_indices {
            let mut edge_attributes = graphrecord
                .edge_attributes_mut(&edge_index)
                .map_err(PyGraphRecordError::from)?;

            edge_attributes
                .update_attribute(&attribute, value.clone())
                .map_err(PyGraphRecordError::from)?;
        }

        Ok(())
    }

    pub fn remove_edge_attribute(
        &self,
        edge_indices: Vec<EdgeIndex>,
        attribute: PyGraphRecordAttribute,
    ) -> PyResult<()> {
        let mut graphrecord = self.inner_mut()?;

        let attribute: GraphRecordAttribute = attribute.into();

        for edge_index in edge_indices {
            let mut edge_attributes = graphrecord
                .edge_attributes_mut(&edge_index)
                .map_err(PyGraphRecordError::from)?;

            edge_attributes
                .remove_attribute(&attribute)
                .map_err(PyGraphRecordError::from)?;
        }

        Ok(())
    }

    pub fn add_edges(
        &self,
        relations: Vec<(PyNodeIndex, PyNodeIndex, PyAttributes)>,
    ) -> PyResult<Vec<EdgeIndex>> {
        Ok(self
            .inner_mut()?
            .add_edges(relations.deep_into())
            .map_err(PyGraphRecordError::from)?)
    }

    pub fn add_edges_with_group(
        &self,
        relations: Vec<(PyNodeIndex, PyNodeIndex, PyAttributes)>,
        group: PyGroup,
    ) -> PyResult<Vec<EdgeIndex>> {
        Ok(self
            .inner_mut()?
            .add_edges_with_group(relations.deep_into(), &group)
            .map_err(PyGraphRecordError::from)?)
    }

    pub fn add_edges_dataframes(
        &self,
        edges_dataframes: Vec<(PyDataFrame, String, String)>,
    ) -> PyResult<Vec<EdgeIndex>> {
        Ok(self
            .inner_mut()?
            .add_edges_dataframes(edges_dataframes)
            .map_err(PyGraphRecordError::from)?)
    }

    pub fn add_edges_dataframes_with_group(
        &self,
        edges_dataframes: Vec<(PyDataFrame, String, String)>,
        group: PyGroup,
    ) -> PyResult<Vec<EdgeIndex>> {
        Ok(self
            .inner_mut()?
            .add_edges_dataframes_with_group(edges_dataframes, &group)
            .map_err(PyGraphRecordError::from)?)
    }

    #[pyo3(signature = (group, node_indices_to_add=None, edge_indices_to_add=None))]
    pub fn add_group(
        &self,
        group: PyGroup,
        node_indices_to_add: Option<Vec<PyNodeIndex>>,
        edge_indices_to_add: Option<Vec<EdgeIndex>>,
    ) -> PyResult<()> {
        Ok(self
            .inner_mut()?
            .add_group(
                group.into(),
                node_indices_to_add.deep_into(),
                edge_indices_to_add,
            )
            .map_err(PyGraphRecordError::from)?)
    }

    pub fn remove_groups(&self, group: Vec<PyGroup>) -> PyResult<()> {
        let mut graphrecord = self.inner_mut()?;

        group.into_iter().try_for_each(|group| {
            graphrecord
                .remove_group(&group)
                .map_err(PyGraphRecordError::from)?;

            Ok(())
        })
    }

    pub fn add_nodes_to_group(&self, group: PyGroup, node_index: Vec<PyNodeIndex>) -> PyResult<()> {
        let mut graphrecord = self.inner_mut()?;

        node_index.into_iter().try_for_each(|node_index| {
            Ok(graphrecord
                .add_node_to_group(group.clone().into(), node_index.into())
                .map_err(PyGraphRecordError::from)?)
        })
    }

    pub fn add_edges_to_group(&self, group: PyGroup, edge_index: Vec<EdgeIndex>) -> PyResult<()> {
        let mut graphrecord = self.inner_mut()?;

        edge_index.into_iter().try_for_each(|edge_index| {
            Ok(graphrecord
                .add_edge_to_group(group.clone().into(), edge_index)
                .map_err(PyGraphRecordError::from)?)
        })
    }

    pub fn remove_nodes_from_group(
        &self,
        group: PyGroup,
        node_index: Vec<PyNodeIndex>,
    ) -> PyResult<()> {
        let mut graphrecord = self.inner_mut()?;

        node_index.into_iter().try_for_each(|node_index| {
            Ok(graphrecord
                .remove_node_from_group(&group, &node_index)
                .map_err(PyGraphRecordError::from)?)
        })
    }

    pub fn remove_edges_from_group(
        &self,
        group: PyGroup,
        edge_index: Vec<EdgeIndex>,
    ) -> PyResult<()> {
        let mut graphrecord = self.inner_mut()?;

        edge_index.into_iter().try_for_each(|edge_index| {
            Ok(graphrecord
                .remove_edge_from_group(&group, &edge_index)
                .map_err(PyGraphRecordError::from)?)
        })
    }

    pub fn nodes_in_group(
        &self,
        group: Vec<PyGroup>,
    ) -> PyResult<HashMap<PyGroup, Vec<PyNodeIndex>>> {
        let graphrecord = self.inner()?;

        group
            .into_iter()
            .map(|group| {
                let nodes_attributes = graphrecord
                    .nodes_in_group(&group)
                    .map_err(PyGraphRecordError::from)?
                    .map(|node_index| node_index.clone().into())
                    .collect();

                Ok((group, nodes_attributes))
            })
            .collect()
    }

    pub fn ungrouped_nodes(&self) -> PyResult<Vec<PyNodeIndex>> {
        Ok(self
            .inner()?
            .ungrouped_nodes()
            .map(|node_index| node_index.clone().into())
            .collect())
    }

    pub fn edges_in_group(
        &self,
        group: Vec<PyGroup>,
    ) -> PyResult<HashMap<PyGroup, Vec<EdgeIndex>>> {
        let graphrecord = self.inner()?;

        group
            .into_iter()
            .map(|group| {
                let edges = graphrecord
                    .edges_in_group(&group)
                    .map_err(PyGraphRecordError::from)?
                    .copied()
                    .collect();

                Ok((group, edges))
            })
            .collect()
    }

    pub fn ungrouped_edges(&self) -> PyResult<Vec<EdgeIndex>> {
        Ok(self.inner()?.ungrouped_edges().copied().collect())
    }

    pub fn groups_of_node(
        &self,
        node_index: Vec<PyNodeIndex>,
    ) -> PyResult<HashMap<PyNodeIndex, Vec<PyGroup>>> {
        let graphrecord = self.inner()?;

        node_index
            .into_iter()
            .map(|node_index| {
                let groups = graphrecord
                    .groups_of_node(&node_index)
                    .map_err(PyGraphRecordError::from)?
                    .map(|node_index| node_index.clone().into())
                    .collect();

                Ok((node_index, groups))
            })
            .collect()
    }

    pub fn groups_of_edge(
        &self,
        edge_index: Vec<EdgeIndex>,
    ) -> PyResult<HashMap<EdgeIndex, Vec<PyGroup>>> {
        let graphrecord = self.inner()?;

        edge_index
            .into_iter()
            .map(|edge_index| {
                let groups = graphrecord
                    .groups_of_edge(&edge_index)
                    .map_err(PyGraphRecordError::from)?
                    .map(|group| group.clone().into())
                    .collect();

                Ok((edge_index, groups))
            })
            .collect()
    }

    pub fn node_count(&self) -> PyResult<usize> {
        Ok(self.inner()?.node_count())
    }

    pub fn edge_count(&self) -> PyResult<usize> {
        Ok(self.inner()?.edge_count())
    }

    pub fn group_count(&self) -> PyResult<usize> {
        Ok(self.inner()?.group_count())
    }

    pub fn contains_node(&self, node_index: PyNodeIndex) -> PyResult<bool> {
        Ok(self.inner()?.contains_node(&node_index.into()))
    }

    pub fn contains_edge(&self, edge_index: EdgeIndex) -> PyResult<bool> {
        Ok(self.inner()?.contains_edge(&edge_index))
    }

    pub fn contains_group(&self, group: PyGroup) -> PyResult<bool> {
        Ok(self.inner()?.contains_group(&group.into()))
    }

    pub fn neighbors(
        &self,
        node_indices: Vec<PyNodeIndex>,
    ) -> PyResult<HashMap<PyNodeIndex, Vec<PyNodeIndex>>> {
        let graphrecord = self.inner()?;

        node_indices
            .into_iter()
            .map(|node_index| {
                let neighbors = graphrecord
                    .neighbors_outgoing(&node_index)
                    .map_err(PyGraphRecordError::from)?
                    .map(|neighbor| neighbor.clone().into())
                    .collect();

                Ok((node_index, neighbors))
            })
            .collect()
    }

    pub fn neighbors_undirected(
        &self,
        node_indices: Vec<PyNodeIndex>,
    ) -> PyResult<HashMap<PyNodeIndex, Vec<PyNodeIndex>>> {
        let graphrecord = self.inner()?;

        node_indices
            .into_iter()
            .map(|node_index| {
                let neighbors = graphrecord
                    .neighbors_undirected(&node_index)
                    .map_err(PyGraphRecordError::from)?
                    .map(|neighbor| neighbor.clone().into())
                    .collect();

                Ok((node_index, neighbors))
            })
            .collect()
    }

    pub fn clear(&self) -> PyResult<()> {
        let _: () = self.inner_mut()?.clear();
        Ok(())
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn query_nodes(
        &self,
        py: Python<'_>,
        query: &Bound<'_, PyFunction>,
    ) -> PyResult<Py<PyAny>> {
        let graphrecord = self.inner()?;

        let result = graphrecord
            .query_nodes(|nodes| {
                let result = query
                    .call1((PyNodeOperand::from(nodes.clone()),))
                    .expect("Call should succeed");

                result
                    .extract::<PyReturnOperand>()
                    .expect("Extraction must succeed")
            })
            .evaluate()
            .map_err(PyGraphRecordError::from)?;

        Ok(result.into_pyobject(py)?.unbind())
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn query_edges(
        &self,
        py: Python<'_>,
        query: &Bound<'_, PyFunction>,
    ) -> PyResult<Py<PyAny>> {
        let graphrecord = self.inner()?;

        let result = graphrecord
            .query_edges(|edges| {
                let result = query
                    .call1((PyEdgeOperand::from(edges.clone()),))
                    .expect("Call should succeed");

                result
                    .extract::<PyReturnOperand>()
                    .expect("Extraction must succeed")
            })
            .evaluate()
            .map_err(PyGraphRecordError::from)?;

        Ok(result.into_pyobject(py)?.unbind())
    }

    #[allow(clippy::should_implement_trait)]
    pub fn clone(&self) -> Self {
        Clone::clone(self)
    }

    pub fn overview(&self, truncate_details: Option<usize>) -> PyResult<PyOverview> {
        Ok(self
            .inner()?
            .overview(truncate_details)
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    pub fn group_overview(
        &self,
        group: PyGroup,
        truncate_details: Option<usize>,
    ) -> PyResult<PyGroupOverview> {
        Ok(self
            .inner()?
            .group_overview(&group.into(), truncate_details)
            .map_err(PyGraphRecordError::from)?
            .into())
    }
}
