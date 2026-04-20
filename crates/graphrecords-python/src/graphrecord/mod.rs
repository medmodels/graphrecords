#![allow(clippy::new_without_default, clippy::significant_drop_tightening)]

pub mod attribute;
mod borrowed;
pub mod connector;
pub mod datatype;
pub mod errors;
pub mod handle;
pub mod overview;
pub mod plugins;
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
use connector::PyConnector;
use errors::PyGraphRecordError;
use graphrecords_core::{
    errors::GraphRecordError,
    graphrecord::{
        AttributeNameKind, Attributes, EdgeDataFrameInput, EdgeIndex, GraphRecord,
        GraphRecordAttribute, GraphRecordValue, Group, GroupKind, HandleLookup, NodeDataFrameInput,
        NodeIndexKind, connector::ConnectedGraphRecord, plugins::Plugin,
    },
    prelude::NodeIndex,
};
use handle::{
    PyAttributeHandle, PyAttributeInput, PyGroupHandle, PyGroupInput, PyNodeHandle, PyNodeInput,
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
pub type PyPluginName = PyGraphRecordAttribute;
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
    Connected(RwLock<ConnectedGraphRecord<PyConnector>>),
    Borrowed(BorrowedGraphRecord),
}

pub(crate) enum InnerRef<'a> {
    Owned(RwLockReadGuard<'a, GraphRecord>),
    Connected(RwLockReadGuard<'a, ConnectedGraphRecord<PyConnector>>),
    Borrowed(RwLockReadGuard<'a, Option<NonNull<GraphRecord>>>),
}

impl Deref for InnerRef<'_> {
    type Target = GraphRecord;

    fn deref(&self) -> &GraphRecord {
        match self {
            InnerRef::Owned(guard) => guard,
            InnerRef::Connected(guard) => guard,
            // SAFETY: The guard is only constructed after checking `is_some()` in `inner()`.
            // The pointer is valid for the duration of the `scope()`/`scope_mut()` call
            // because the scope's Drop guard needs a write lock to clear it, and this read
            // guard prevents that.
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
    Connected(RwLockWriteGuard<'a, ConnectedGraphRecord<PyConnector>>),
    Borrowed(RwLockWriteGuard<'a, Option<NonNull<GraphRecord>>>),
}

impl Deref for InnerRefMut<'_> {
    type Target = GraphRecord;

    fn deref(&self) -> &GraphRecord {
        match self {
            InnerRefMut::Owned(guard) => guard,
            InnerRefMut::Connected(guard) => guard,
            // SAFETY: Same as `InnerRef::Borrowed`. Pointer was checked `is_some()` in
            // `inner_mut()`, and the write guard keeps the scope's Drop from clearing it.
            // Additionally, `inner_mut()` has already verified `is_mutable()` is true.
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
            InnerRefMut::Connected(guard) => &mut *guard,
            // SAFETY: Same as above, plus: the write guard ensures exclusive access to the
            // pointer, so creating `&mut GraphRecord` is sound. The original `scope_mut()`
            // call holds `&mut GraphRecord`, guaranteeing no other references to the pointee
            // exist outside this lock. `inner_mut()` has verified `is_mutable()` is true,
            // ensuring this path is only reachable for pointers originating from `&mut`.
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
            PyGraphRecordInner::Connected(lock) => Self {
                inner: PyGraphRecordInner::Connected(RwLock::new(lock.read().clone())),
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
            PyGraphRecordInner::Connected(lock) => Ok(InnerRef::Connected(lock.read())),
            PyGraphRecordInner::Borrowed(borrowed) => {
                let guard = borrowed.read();
                if guard.is_some() {
                    Ok(InnerRef::Borrowed(guard))
                } else {
                    Err(PyRuntimeError::new_err(
                        "GraphRecord reference is no longer valid (used outside callback scope)",
                    ))
                }
            }
        }
    }

    pub(crate) fn connected(
        &self,
    ) -> PyResult<RwLockWriteGuard<'_, ConnectedGraphRecord<PyConnector>>> {
        match &self.inner {
            PyGraphRecordInner::Connected(lock) => Ok(lock.write()),
            _ => Err(PyRuntimeError::new_err(
                "GraphRecord has no connector attached",
            )),
        }
    }

    pub(crate) fn inner_mut(&self) -> PyResult<InnerRefMut<'_>> {
        match &self.inner {
            PyGraphRecordInner::Owned(lock) => Ok(InnerRefMut::Owned(lock.write())),
            PyGraphRecordInner::Connected(lock) => Ok(InnerRefMut::Connected(lock.write())),
            PyGraphRecordInner::Borrowed(borrowed) => {
                if !borrowed.is_mutable() {
                    return Err(PyRuntimeError::new_err("GraphRecord is read-only"));
                }
                let guard = borrowed.write();
                if guard.is_some() {
                    Ok(InnerRefMut::Borrowed(guard))
                } else {
                    Err(PyRuntimeError::new_err(
                        "GraphRecord reference is no longer valid (used outside callback scope)",
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

impl From<ConnectedGraphRecord<PyConnector>> for PyGraphRecord {
    fn from(value: ConnectedGraphRecord<PyConnector>) -> Self {
        Self {
            inner: PyGraphRecordInner::Connected(RwLock::new(value)),
        }
    }
}

impl TryFrom<PyGraphRecord> for GraphRecord {
    type Error = PyErr;

    fn try_from(value: PyGraphRecord) -> PyResult<Self> {
        match value.inner {
            PyGraphRecordInner::Owned(lock) => Ok(lock.into_inner()),
            PyGraphRecordInner::Connected(lock) => Ok(lock.into_inner().into()),
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
    pub fn with_plugins(plugins: HashMap<PyPluginName, Py<PyAny>>) -> PyResult<Self> {
        let plugins = plugins
            .into_iter()
            .map(|(name, plugin)| {
                (
                    name.into(),
                    Box::new(PyPlugin::new(plugin)) as Box<dyn Plugin>,
                )
            })
            .collect();

        let graphrecord = GraphRecord::with_plugins(plugins).map_err(PyGraphRecordError::from)?;

        Ok(graphrecord.into())
    }

    #[staticmethod]
    #[pyo3(signature = (nodes, edges=None, schema=None))]
    pub fn from_tuples(
        nodes: Vec<(PyNodeIndex, PyAttributes)>,
        edges: Option<Vec<(PyNodeIndex, PyNodeIndex, PyAttributes)>>,
        schema: Option<PySchema>,
    ) -> PyResult<Self> {
        Ok(
            GraphRecord::from_tuples(nodes.deep_into(), edges.deep_into(), schema.map(Into::into))
                .map_err(PyGraphRecordError::from)?
                .into(),
        )
    }

    #[staticmethod]
    #[pyo3(signature = (nodes_dataframes, edges_dataframes, schema=None))]
    pub fn from_dataframes(
        nodes_dataframes: Vec<(PyDataFrame, String)>,
        edges_dataframes: Vec<(PyDataFrame, String, String)>,
        schema: Option<PySchema>,
    ) -> PyResult<Self> {
        Ok(
            GraphRecord::from_dataframes(
                nodes_dataframes,
                edges_dataframes,
                schema.map(Into::into),
            )
            .map_err(PyGraphRecordError::from)?
            .into(),
        )
    }

    #[staticmethod]
    #[pyo3(signature = (nodes_dataframes, schema=None))]
    pub fn from_nodes_dataframes(
        nodes_dataframes: Vec<(PyDataFrame, String)>,
        schema: Option<PySchema>,
    ) -> PyResult<Self> {
        Ok(
            GraphRecord::from_nodes_dataframes(nodes_dataframes, schema.map(Into::into))
                .map_err(PyGraphRecordError::from)?
                .into(),
        )
    }

    #[staticmethod]
    pub fn from_ron(path: &str) -> PyResult<Self> {
        Ok(GraphRecord::from_ron(path)
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    #[staticmethod]
    pub fn with_connector(connector: Py<PyAny>) -> PyResult<Self> {
        let connected = ConnectedGraphRecord::new(PyConnector::new(connector))
            .map_err(PyGraphRecordError::from)?;

        Ok(connected.into())
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

    pub fn disconnect(&self) -> PyResult<Self> {
        let graphrecord = self
            .connected()?
            .clone()
            .disconnect()
            .map_err(PyGraphRecordError::from)?;

        Ok(graphrecord.into())
    }

    pub fn ingest(&self, data: Py<PyAny>) -> PyResult<()> {
        self.connected()?
            .ingest(data)
            .map_err(PyGraphRecordError::from)?;

        Ok(())
    }

    pub fn export(&self) -> PyResult<Py<PyAny>> {
        let data = self
            .connected()?
            .export()
            .map_err(PyGraphRecordError::from)?;

        Ok(data)
    }

    pub fn add_plugin(&self, name: PyPluginName, plugin: Py<PyAny>) -> PyResult<()> {
        let mut graphrecord = self.inner_mut()?;

        graphrecord
            .add_plugin(name.into(), Box::new(PyPlugin::new(plugin)))
            .map_err(PyGraphRecordError::from)?;

        Ok(())
    }

    pub fn remove_plugin(&self, name: PyPluginName) -> PyResult<()> {
        let mut graphrecord = self.inner_mut()?;

        graphrecord
            .remove_plugin(&name.into())
            .map_err(PyGraphRecordError::from)?;

        Ok(())
    }

    #[getter]
    pub fn plugins(&self) -> PyResult<Vec<PyPluginName>> {
        Ok(self
            .inner()?
            .plugin_names()
            .cloned()
            .map(std::convert::Into::into)
            .collect())
    }

    pub fn get_schema(&self) -> PyResult<PySchema> {
        Ok(self.inner()?.get_schema().clone().into())
    }

    #[pyo3(signature = (schema, bypass_plugins=false))]
    pub fn set_schema(&self, schema: PySchema, bypass_plugins: bool) -> PyResult<()> {
        let mut graphrecord = self.inner_mut()?;

        if bypass_plugins {
            Ok(graphrecord
                .set_schema_bypass_plugins(schema.into())
                .map_err(PyGraphRecordError::from)?)
        } else {
            Ok(graphrecord
                .set_schema(schema.into())
                .map_err(PyGraphRecordError::from)?)
        }
    }

    #[pyo3(signature = (bypass_plugins=false))]
    pub fn freeze_schema(&self, bypass_plugins: bool) -> PyResult<()> {
        let mut graphrecord = self.inner_mut()?;

        if bypass_plugins {
            Ok(graphrecord
                .freeze_schema_bypass_plugins()
                .map_err(PyGraphRecordError::from)?)
        } else {
            Ok(graphrecord
                .freeze_schema()
                .map_err(PyGraphRecordError::from)?)
        }
    }

    #[pyo3(signature = (bypass_plugins=false))]
    pub fn unfreeze_schema(&self, bypass_plugins: bool) -> PyResult<()> {
        let mut graphrecord = self.inner_mut()?;

        if bypass_plugins {
            Ok(graphrecord
                .unfreeze_schema_bypass_plugins()
                .map_err(PyGraphRecordError::from)?)
        } else {
            Ok(graphrecord
                .unfreeze_schema()
                .map_err(PyGraphRecordError::from)?)
        }
    }

    #[getter]
    pub fn nodes(&self) -> PyResult<Vec<PyNodeIndex>> {
        Ok(self
            .inner()?
            .node_indices()
            .map(|node_index| node_index.clone().into())
            .collect())
    }

    pub fn node(&self, nodes: Vec<PyNodeInput>) -> PyResult<HashMap<PyNodeInput, PyAttributes>> {
        let graphrecord = self.inner()?;

        nodes
            .into_iter()
            .map(|input| {
                let node_attributes = graphrecord
                    .node_attributes(&input)
                    .map_err(PyGraphRecordError::from)?
                    .to_owned_attributes();

                Ok((input, node_attributes.deep_into()))
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
                    .map_err(PyGraphRecordError::from)?
                    .to_owned_attributes();

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
        nodes: Vec<PyNodeInput>,
    ) -> PyResult<HashMap<PyNodeInput, Vec<EdgeIndex>>> {
        let graphrecord = self.inner()?;

        nodes
            .into_iter()
            .map(|input| {
                let edges = graphrecord
                    .outgoing_edges(&input)
                    .map_err(PyGraphRecordError::from)?
                    .copied()
                    .collect();

                Ok((input, edges))
            })
            .collect()
    }

    pub fn incoming_edges(
        &self,
        nodes: Vec<PyNodeInput>,
    ) -> PyResult<HashMap<PyNodeInput, Vec<EdgeIndex>>> {
        let graphrecord = self.inner()?;

        nodes
            .into_iter()
            .map(|input| {
                let edges = graphrecord
                    .incoming_edges(&input)
                    .map_err(PyGraphRecordError::from)?
                    .copied()
                    .collect();

                Ok((input, edges))
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
                let (source, target) = graphrecord
                    .edge_endpoints(&edge_index)
                    .map_err(PyGraphRecordError::from)?;

                Ok((edge_index, (source.clone().into(), target.clone().into())))
            })
            .collect()
    }

    pub fn edge_endpoint_handles(
        &self,
        edge_index: Vec<EdgeIndex>,
    ) -> PyResult<HashMap<EdgeIndex, (PyNodeHandle, PyNodeHandle)>> {
        let graphrecord = self.inner()?;

        edge_index
            .into_iter()
            .map(|edge_index| {
                let (source_handle, target_handle) = graphrecord
                    .edge_endpoint_handles(&edge_index)
                    .map_err(PyGraphRecordError::from)?;

                Ok((
                    edge_index,
                    (
                        PyNodeHandle::from(source_handle),
                        PyNodeHandle::from(target_handle),
                    ),
                ))
            })
            .collect()
    }

    pub fn edges_connecting(
        &self,
        source_nodes: Vec<PyNodeInput>,
        target_nodes: Vec<PyNodeInput>,
    ) -> PyResult<Vec<EdgeIndex>> {
        Ok(self
            .inner()?
            .edges_connecting(source_nodes.iter(), target_nodes.iter())
            .map_err(PyGraphRecordError::from)?
            .copied()
            .collect())
    }

    pub fn edges_connecting_undirected(
        &self,
        first_nodes: Vec<PyNodeInput>,
        second_nodes: Vec<PyNodeInput>,
    ) -> PyResult<Vec<EdgeIndex>> {
        Ok(self
            .inner()?
            .edges_connecting_undirected(first_nodes.iter(), second_nodes.iter())
            .map_err(PyGraphRecordError::from)?
            .copied()
            .collect())
    }

    #[pyo3(signature = (node_indices, bypass_plugins=false))]
    pub fn remove_nodes(
        &self,
        node_indices: Vec<PyNodeInput>,
        bypass_plugins: bool,
    ) -> PyResult<HashMap<PyNodeInput, PyAttributes>> {
        let mut graphrecord = self.inner_mut()?;

        if bypass_plugins {
            node_indices
                .into_iter()
                .map(|input| {
                    let attributes = graphrecord
                        .remove_node_bypass_plugins(&input)
                        .map_err(PyGraphRecordError::from)?;
                    Ok((input, attributes.deep_into()))
                })
                .collect()
        } else {
            node_indices
                .into_iter()
                .map(|input| {
                    let attributes = graphrecord
                        .remove_node(&input)
                        .map_err(PyGraphRecordError::from)?;
                    Ok((input, attributes.deep_into()))
                })
                .collect()
        }
    }

    pub fn replace_node_attributes(
        &self,
        node_indices: Vec<PyNodeInput>,
        attributes: PyAttributes,
    ) -> PyResult<()> {
        let mut graphrecord = self.inner_mut()?;

        let attributes: Attributes = attributes.deep_into();

        for input in node_indices {
            let mut current_attributes = graphrecord
                .node_attributes_mut(&input)
                .map_err(PyGraphRecordError::from)?;

            current_attributes
                .replace_attributes(attributes.clone())
                .map_err(PyGraphRecordError::from)?;
        }

        Ok(())
    }

    pub fn update_node_attribute(
        &self,
        node_indices: Vec<PyNodeInput>,
        attribute: PyAttributeInput,
        value: PyGraphRecordValue,
    ) -> PyResult<()> {
        let mut graphrecord = self.inner_mut()?;
        let value: GraphRecordValue = value.into();

        for input in node_indices {
            let mut node_attributes = graphrecord
                .node_attributes_mut(&input)
                .map_err(PyGraphRecordError::from)?;

            node_attributes
                .update_attribute(&attribute, value.clone())
                .map_err(PyGraphRecordError::from)?;
        }

        Ok(())
    }

    pub fn remove_node_attribute(
        &self,
        node_indices: Vec<PyNodeInput>,
        attribute: PyAttributeInput,
    ) -> PyResult<()> {
        let mut graphrecord = self.inner_mut()?;

        for input in node_indices {
            let mut node_attributes = graphrecord
                .node_attributes_mut(&input)
                .map_err(PyGraphRecordError::from)?;

            node_attributes
                .remove_attribute(&attribute)
                .map_err(PyGraphRecordError::from)?;
        }

        Ok(())
    }

    #[pyo3(signature = (node_index, attributes, bypass_plugins=false))]
    pub fn add_node(
        &self,
        node_index: PyNodeIndex,
        attributes: PyAttributes,
        bypass_plugins: bool,
    ) -> PyResult<PyNodeHandle> {
        let mut graphrecord = self.inner_mut()?;
        let node_index: NodeIndex = node_index.into();
        let attributes: Attributes = attributes.deep_into();

        let handle = if bypass_plugins {
            graphrecord
                .add_node_bypass_plugins(node_index, attributes)
                .map_err(PyGraphRecordError::from)?
        } else {
            graphrecord
                .add_node(node_index, attributes)
                .map_err(PyGraphRecordError::from)?
        };

        Ok(handle.into())
    }

    #[pyo3(signature = (node_index, attributes, group, bypass_plugins=false))]
    pub fn add_node_with_group(
        &self,
        node_index: PyNodeIndex,
        attributes: PyAttributes,
        group: PyGroupInput,
        bypass_plugins: bool,
    ) -> PyResult<PyNodeHandle> {
        let mut graphrecord = self.inner_mut()?;
        let node_index: NodeIndex = node_index.into();
        let attributes: Attributes = attributes.deep_into();

        let handle = if bypass_plugins {
            graphrecord
                .add_node_with_group_bypass_plugins(node_index, attributes, &group)
                .map_err(PyGraphRecordError::from)?
        } else {
            graphrecord
                .add_node_with_group(node_index, attributes, &group)
                .map_err(PyGraphRecordError::from)?
        };

        Ok(handle.into())
    }

    #[pyo3(signature = (source, target, attributes, bypass_plugins=false))]
    pub fn add_edge(
        &self,
        source: PyNodeInput,
        target: PyNodeInput,
        attributes: PyAttributes,
        bypass_plugins: bool,
    ) -> PyResult<EdgeIndex> {
        let mut graphrecord = self.inner_mut()?;
        let attributes: Attributes = attributes.deep_into();

        let edge_index = if bypass_plugins {
            graphrecord
                .add_edge_bypass_plugins(&source, &target, attributes)
                .map_err(PyGraphRecordError::from)?
        } else {
            graphrecord
                .add_edge(&source, &target, attributes)
                .map_err(PyGraphRecordError::from)?
        };

        Ok(edge_index)
    }

    #[pyo3(signature = (source, target, attributes, group, bypass_plugins=false))]
    pub fn add_edge_with_group(
        &self,
        source: PyNodeInput,
        target: PyNodeInput,
        attributes: PyAttributes,
        group: PyGroupInput,
        bypass_plugins: bool,
    ) -> PyResult<EdgeIndex> {
        let mut graphrecord = self.inner_mut()?;
        let attributes: Attributes = attributes.deep_into();

        let edge_index = if bypass_plugins {
            graphrecord
                .add_edge_with_group_bypass_plugins(&source, &target, attributes, &group)
                .map_err(PyGraphRecordError::from)?
        } else {
            graphrecord
                .add_edge_with_group(&source, &target, attributes, &group)
                .map_err(PyGraphRecordError::from)?
        };

        Ok(edge_index)
    }

    #[pyo3(signature = (nodes, bypass_plugins=false))]
    pub fn add_nodes(
        &self,
        nodes: Vec<(PyNodeIndex, PyAttributes)>,
        bypass_plugins: bool,
    ) -> PyResult<Vec<PyNodeHandle>> {
        let mut graphrecord = self.inner_mut()?;

        let handles = if bypass_plugins {
            graphrecord
                .add_nodes_bypass_plugins(nodes.deep_into())
                .map_err(PyGraphRecordError::from)?
        } else {
            graphrecord
                .add_nodes(nodes.deep_into())
                .map_err(PyGraphRecordError::from)?
        };

        Ok(handles.deep_into())
    }

    #[pyo3(signature = (nodes, group, bypass_plugins=false))]
    pub fn add_nodes_with_group(
        &self,
        nodes: Vec<(PyNodeIndex, PyAttributes)>,
        group: PyGroupInput,
        bypass_plugins: bool,
    ) -> PyResult<Vec<PyNodeHandle>> {
        let mut graphrecord = self.inner_mut()?;

        let handles = if bypass_plugins {
            graphrecord
                .add_nodes_with_group_bypass_plugins(nodes.deep_into(), &group)
                .map_err(PyGraphRecordError::from)?
        } else {
            graphrecord
                .add_nodes_with_group(nodes.deep_into(), &group)
                .map_err(PyGraphRecordError::from)?
        };

        Ok(handles.deep_into())
    }

    #[pyo3(signature = (nodes, groups, bypass_plugins=false))]
    pub fn add_nodes_with_groups(
        &self,
        nodes: Vec<(PyNodeIndex, PyAttributes)>,
        groups: Vec<PyGroupInput>,
        bypass_plugins: bool,
    ) -> PyResult<Vec<PyNodeHandle>> {
        let mut graphrecord = self.inner_mut()?;

        let handles = if bypass_plugins {
            graphrecord
                .add_nodes_with_groups_bypass_plugins(nodes.deep_into(), groups.iter())
                .map_err(PyGraphRecordError::from)?
        } else {
            graphrecord
                .add_nodes_with_groups(nodes.deep_into(), groups.iter())
                .map_err(PyGraphRecordError::from)?
        };

        Ok(handles.deep_into())
    }

    #[pyo3(signature = (node_index, attributes, groups, bypass_plugins=false))]
    pub fn add_node_with_groups(
        &self,
        node_index: PyNodeIndex,
        attributes: PyAttributes,
        groups: Vec<PyGroupInput>,
        bypass_plugins: bool,
    ) -> PyResult<PyNodeHandle> {
        let mut graphrecord = self.inner_mut()?;

        let handle = if bypass_plugins {
            graphrecord
                .add_node_with_groups_bypass_plugins(
                    node_index.into(),
                    attributes.deep_into(),
                    groups.iter(),
                )
                .map_err(PyGraphRecordError::from)?
        } else {
            graphrecord
                .add_node_with_groups(node_index.into(), attributes.deep_into(), groups.iter())
                .map_err(PyGraphRecordError::from)?
        };

        Ok(handle.into())
    }

    #[pyo3(signature = (nodes_dataframes, bypass_plugins=false))]
    pub fn add_nodes_dataframes(
        &self,
        nodes_dataframes: Vec<(PyDataFrame, String)>,
        bypass_plugins: bool,
    ) -> PyResult<Vec<PyNodeHandle>> {
        let mut graphrecord = self.inner_mut()?;

        let handles = if bypass_plugins {
            graphrecord
                .add_nodes_dataframes_bypass_plugins(nodes_dataframes)
                .map_err(PyGraphRecordError::from)?
        } else {
            graphrecord
                .add_nodes_dataframes(nodes_dataframes)
                .map_err(PyGraphRecordError::from)?
        };

        Ok(handles.deep_into())
    }

    #[pyo3(signature = (nodes_dataframes, group, bypass_plugins=false))]
    pub fn add_nodes_dataframes_with_group(
        &self,
        nodes_dataframes: Vec<(PyDataFrame, String)>,
        group: PyGroupInput,
        bypass_plugins: bool,
    ) -> PyResult<Vec<PyNodeHandle>> {
        let mut graphrecord = self.inner_mut()?;

        let handles = if bypass_plugins {
            graphrecord
                .add_nodes_dataframes_with_group_bypass_plugins(nodes_dataframes, &group)
                .map_err(PyGraphRecordError::from)?
        } else {
            graphrecord
                .add_nodes_dataframes_with_group(nodes_dataframes, &group)
                .map_err(PyGraphRecordError::from)?
        };

        Ok(handles.deep_into())
    }

    #[pyo3(signature = (nodes_dataframes, groups, bypass_plugins=false))]
    pub fn add_nodes_dataframes_with_groups(
        &self,
        nodes_dataframes: Vec<(PyDataFrame, String)>,
        groups: Vec<PyGroupInput>,
        bypass_plugins: bool,
    ) -> PyResult<Vec<PyNodeHandle>> {
        let mut graphrecord = self.inner_mut()?;
        let nodes_dataframes: Vec<NodeDataFrameInput> =
            nodes_dataframes.into_iter().map(Into::into).collect();

        let handles = if bypass_plugins {
            graphrecord
                .add_nodes_dataframes_with_groups_bypass_plugins(nodes_dataframes, groups.iter())
                .map_err(PyGraphRecordError::from)?
        } else {
            graphrecord
                .add_nodes_dataframes_with_groups(nodes_dataframes, groups.iter())
                .map_err(PyGraphRecordError::from)?
        };

        Ok(handles.deep_into())
    }

    #[pyo3(signature = (edge_indices, bypass_plugins=false))]
    pub fn remove_edges(
        &self,
        edge_indices: Vec<EdgeIndex>,
        bypass_plugins: bool,
    ) -> PyResult<HashMap<EdgeIndex, PyAttributes>> {
        let mut graphrecord = self.inner_mut()?;

        if bypass_plugins {
            edge_indices
                .into_iter()
                .map(|edge_index| {
                    let attributes = graphrecord
                        .remove_edge_bypass_plugins(&edge_index)
                        .map_err(PyGraphRecordError::from)?;
                    Ok((edge_index, attributes.deep_into()))
                })
                .collect()
        } else {
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
        attribute: PyAttributeInput,
        value: PyGraphRecordValue,
    ) -> PyResult<()> {
        let mut graphrecord = self.inner_mut()?;
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
        attribute: PyAttributeInput,
    ) -> PyResult<()> {
        let mut graphrecord = self.inner_mut()?;

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

    #[pyo3(signature = (relations, bypass_plugins=false))]
    pub fn add_edges(
        &self,
        relations: Vec<(PyNodeInput, PyNodeInput, PyAttributes)>,
        bypass_plugins: bool,
    ) -> PyResult<Vec<EdgeIndex>> {
        let mut graphrecord = self.inner_mut()?;
        let relations: Vec<(PyNodeInput, PyNodeInput, Attributes)> = relations
            .into_iter()
            .map(|(source, target, attributes)| (source, target, attributes.deep_into()))
            .collect();

        if bypass_plugins {
            Ok(graphrecord
                .add_edges_bypass_plugins(relations)
                .map_err(PyGraphRecordError::from)?)
        } else {
            Ok(graphrecord
                .add_edges(relations)
                .map_err(PyGraphRecordError::from)?)
        }
    }

    #[pyo3(signature = (relations, group, bypass_plugins=false))]
    pub fn add_edges_with_group(
        &self,
        relations: Vec<(PyNodeInput, PyNodeInput, PyAttributes)>,
        group: PyGroupInput,
        bypass_plugins: bool,
    ) -> PyResult<Vec<EdgeIndex>> {
        let mut graphrecord = self.inner_mut()?;
        let relations: Vec<(PyNodeInput, PyNodeInput, Attributes)> = relations
            .into_iter()
            .map(|(source, target, attributes)| (source, target, attributes.deep_into()))
            .collect();

        if bypass_plugins {
            Ok(graphrecord
                .add_edges_with_group_bypass_plugins(relations, &group)
                .map_err(PyGraphRecordError::from)?)
        } else {
            Ok(graphrecord
                .add_edges_with_group(relations, &group)
                .map_err(PyGraphRecordError::from)?)
        }
    }

    #[pyo3(signature = (relations, groups, bypass_plugins=false))]
    pub fn add_edges_with_groups(
        &self,
        relations: Vec<(PyNodeInput, PyNodeInput, PyAttributes)>,
        groups: Vec<PyGroupInput>,
        bypass_plugins: bool,
    ) -> PyResult<Vec<EdgeIndex>> {
        let mut graphrecord = self.inner_mut()?;
        let relations: Vec<(PyNodeInput, PyNodeInput, Attributes)> = relations
            .into_iter()
            .map(|(source, target, attributes)| (source, target, attributes.deep_into()))
            .collect();

        if bypass_plugins {
            Ok(graphrecord
                .add_edges_with_groups_bypass_plugins(relations, groups.iter())
                .map_err(PyGraphRecordError::from)?)
        } else {
            Ok(graphrecord
                .add_edges_with_groups(relations, groups.iter())
                .map_err(PyGraphRecordError::from)?)
        }
    }

    #[pyo3(signature = (source, target, attributes, groups, bypass_plugins=false))]
    pub fn add_edge_with_groups(
        &self,
        source: PyNodeInput,
        target: PyNodeInput,
        attributes: PyAttributes,
        groups: Vec<PyGroupInput>,
        bypass_plugins: bool,
    ) -> PyResult<EdgeIndex> {
        let mut graphrecord = self.inner_mut()?;

        if bypass_plugins {
            Ok(graphrecord
                .add_edge_with_groups_bypass_plugins(
                    &source,
                    &target,
                    attributes.deep_into(),
                    groups.iter(),
                )
                .map_err(PyGraphRecordError::from)?)
        } else {
            Ok(graphrecord
                .add_edge_with_groups(&source, &target, attributes.deep_into(), groups.iter())
                .map_err(PyGraphRecordError::from)?)
        }
    }

    #[pyo3(signature = (edges_dataframes, bypass_plugins=false))]
    pub fn add_edges_dataframes(
        &self,
        edges_dataframes: Vec<(PyDataFrame, String, String)>,
        bypass_plugins: bool,
    ) -> PyResult<Vec<EdgeIndex>> {
        let mut graphrecord = self.inner_mut()?;

        if bypass_plugins {
            Ok(graphrecord
                .add_edges_dataframes_bypass_plugins(edges_dataframes)
                .map_err(PyGraphRecordError::from)?)
        } else {
            Ok(graphrecord
                .add_edges_dataframes(edges_dataframes)
                .map_err(PyGraphRecordError::from)?)
        }
    }

    #[pyo3(signature = (edges_dataframes, group, bypass_plugins=false))]
    pub fn add_edges_dataframes_with_group(
        &self,
        edges_dataframes: Vec<(PyDataFrame, String, String)>,
        group: PyGroupInput,
        bypass_plugins: bool,
    ) -> PyResult<Vec<EdgeIndex>> {
        let mut graphrecord = self.inner_mut()?;

        if bypass_plugins {
            Ok(graphrecord
                .add_edges_dataframes_with_group_bypass_plugins(edges_dataframes, &group)
                .map_err(PyGraphRecordError::from)?)
        } else {
            Ok(graphrecord
                .add_edges_dataframes_with_group(edges_dataframes, &group)
                .map_err(PyGraphRecordError::from)?)
        }
    }

    #[pyo3(signature = (edges_dataframes, groups, bypass_plugins=false))]
    pub fn add_edges_dataframes_with_groups(
        &self,
        edges_dataframes: Vec<(PyDataFrame, String, String)>,
        groups: Vec<PyGroupInput>,
        bypass_plugins: bool,
    ) -> PyResult<Vec<EdgeIndex>> {
        let mut graphrecord = self.inner_mut()?;
        let edges_dataframes: Vec<EdgeDataFrameInput> =
            edges_dataframes.into_iter().map(Into::into).collect();

        if bypass_plugins {
            Ok(graphrecord
                .add_edges_dataframes_with_groups_bypass_plugins(edges_dataframes, groups.iter())
                .map_err(PyGraphRecordError::from)?)
        } else {
            Ok(graphrecord
                .add_edges_dataframes_with_groups(edges_dataframes, groups.iter())
                .map_err(PyGraphRecordError::from)?)
        }
    }

    #[pyo3(signature = (group, node_indices_to_add=None, edge_indices_to_add=None, bypass_plugins=false))]
    pub fn add_group(
        &self,
        group: PyGroup,
        node_indices_to_add: Option<Vec<PyNodeInput>>,
        edge_indices_to_add: Option<Vec<EdgeIndex>>,
        bypass_plugins: bool,
    ) -> PyResult<()> {
        let mut graphrecord = self.inner_mut()?;

        let group: Group = group.into();
        let node_refs: Option<Vec<&PyNodeInput>> = node_indices_to_add
            .as_ref()
            .map(|nodes| nodes.iter().collect());

        if bypass_plugins {
            Ok(graphrecord
                .add_group_bypass_plugins(group, node_refs, edge_indices_to_add)
                .map_err(PyGraphRecordError::from)?)
        } else {
            Ok(graphrecord
                .add_group(group, node_refs, edge_indices_to_add)
                .map_err(PyGraphRecordError::from)?)
        }
    }

    #[pyo3(signature = (groups, bypass_plugins=false))]
    pub fn remove_groups(&self, groups: Vec<PyGroupInput>, bypass_plugins: bool) -> PyResult<()> {
        let mut graphrecord = self.inner_mut()?;

        if bypass_plugins {
            groups.into_iter().try_for_each(|input| {
                graphrecord
                    .remove_group_bypass_plugins(&input)
                    .map_err(PyGraphRecordError::from)?;
                Ok(())
            })
        } else {
            groups.into_iter().try_for_each(|input| {
                graphrecord
                    .remove_group(&input)
                    .map_err(PyGraphRecordError::from)?;
                Ok(())
            })
        }
    }

    #[pyo3(signature = (group, node_indices, bypass_plugins=false))]
    pub fn add_nodes_to_group(
        &self,
        group: PyGroupInput,
        node_indices: Vec<PyNodeInput>,
        bypass_plugins: bool,
    ) -> PyResult<()> {
        let mut graphrecord = self.inner_mut()?;

        if bypass_plugins {
            node_indices.into_iter().try_for_each(|input| {
                Ok(graphrecord
                    .add_node_to_group_bypass_plugins(&group, &input)
                    .map_err(PyGraphRecordError::from)?)
            })
        } else {
            node_indices.into_iter().try_for_each(|input| {
                Ok(graphrecord
                    .add_node_to_group(&group, &input)
                    .map_err(PyGraphRecordError::from)?)
            })
        }
    }

    #[pyo3(signature = (node, groups, bypass_plugins=false))]
    pub fn add_node_to_groups(
        &self,
        node: PyNodeInput,
        groups: Vec<PyGroupInput>,
        bypass_plugins: bool,
    ) -> PyResult<()> {
        let mut graphrecord = self.inner_mut()?;

        if bypass_plugins {
            graphrecord
                .add_node_to_groups_bypass_plugins(groups.iter(), &node)
                .map_err(PyGraphRecordError::from)?;
        } else {
            graphrecord
                .add_node_to_groups(groups.iter(), &node)
                .map_err(PyGraphRecordError::from)?;
        }

        Ok(())
    }

    #[pyo3(signature = (node_indices, groups, bypass_plugins=false))]
    pub fn add_nodes_to_groups(
        &self,
        node_indices: Vec<PyNodeInput>,
        groups: Vec<PyGroupInput>,
        bypass_plugins: bool,
    ) -> PyResult<()> {
        let mut graphrecord = self.inner_mut()?;

        if bypass_plugins {
            graphrecord
                .add_nodes_to_groups_bypass_plugins(groups.iter(), node_indices.iter())
                .map_err(PyGraphRecordError::from)?;
        } else {
            graphrecord
                .add_nodes_to_groups(groups.iter(), node_indices.iter())
                .map_err(PyGraphRecordError::from)?;
        }

        Ok(())
    }

    #[pyo3(signature = (group, edge_indices, bypass_plugins=false))]
    pub fn add_edges_to_group(
        &self,
        group: PyGroupInput,
        edge_indices: Vec<EdgeIndex>,
        bypass_plugins: bool,
    ) -> PyResult<()> {
        let mut graphrecord = self.inner_mut()?;

        if bypass_plugins {
            edge_indices.into_iter().try_for_each(|edge_index| {
                Ok(graphrecord
                    .add_edge_to_group_bypass_plugins(&group, edge_index)
                    .map_err(PyGraphRecordError::from)?)
            })
        } else {
            edge_indices.into_iter().try_for_each(|edge_index| {
                Ok(graphrecord
                    .add_edge_to_group(&group, edge_index)
                    .map_err(PyGraphRecordError::from)?)
            })
        }
    }

    #[pyo3(signature = (edge_index, groups, bypass_plugins=false))]
    pub fn add_edge_to_groups(
        &self,
        edge_index: EdgeIndex,
        groups: Vec<PyGroupInput>,
        bypass_plugins: bool,
    ) -> PyResult<()> {
        let mut graphrecord = self.inner_mut()?;

        if bypass_plugins {
            graphrecord
                .add_edge_to_groups_bypass_plugins(groups.iter(), edge_index)
                .map_err(PyGraphRecordError::from)?;
        } else {
            graphrecord
                .add_edge_to_groups(groups.iter(), edge_index)
                .map_err(PyGraphRecordError::from)?;
        }

        Ok(())
    }

    #[pyo3(signature = (edge_indices, groups, bypass_plugins=false))]
    pub fn add_edges_to_groups(
        &self,
        edge_indices: Vec<EdgeIndex>,
        groups: Vec<PyGroupInput>,
        bypass_plugins: bool,
    ) -> PyResult<()> {
        let mut graphrecord = self.inner_mut()?;

        if bypass_plugins {
            graphrecord
                .add_edges_to_groups_bypass_plugins(groups.iter(), edge_indices)
                .map_err(PyGraphRecordError::from)?;
        } else {
            graphrecord
                .add_edges_to_groups(groups.iter(), edge_indices)
                .map_err(PyGraphRecordError::from)?;
        }

        Ok(())
    }

    #[pyo3(signature = (group, node_indices, bypass_plugins=false))]
    pub fn remove_nodes_from_group(
        &self,
        group: PyGroupInput,
        node_indices: Vec<PyNodeInput>,
        bypass_plugins: bool,
    ) -> PyResult<()> {
        let mut graphrecord = self.inner_mut()?;

        if bypass_plugins {
            node_indices.into_iter().try_for_each(|input| {
                Ok(graphrecord
                    .remove_node_from_group_bypass_plugins(&group, &input)
                    .map_err(PyGraphRecordError::from)?)
            })
        } else {
            node_indices.into_iter().try_for_each(|input| {
                Ok(graphrecord
                    .remove_node_from_group(&group, &input)
                    .map_err(PyGraphRecordError::from)?)
            })
        }
    }

    #[pyo3(signature = (node, groups, bypass_plugins=false))]
    pub fn remove_node_from_groups(
        &self,
        node: PyNodeInput,
        groups: Vec<PyGroupInput>,
        bypass_plugins: bool,
    ) -> PyResult<()> {
        let mut graphrecord = self.inner_mut()?;

        if bypass_plugins {
            graphrecord
                .remove_node_from_groups_bypass_plugins(groups.iter(), &node)
                .map_err(PyGraphRecordError::from)?;
        } else {
            graphrecord
                .remove_node_from_groups(groups.iter(), &node)
                .map_err(PyGraphRecordError::from)?;
        }

        Ok(())
    }

    #[pyo3(signature = (node_indices, groups, bypass_plugins=false))]
    pub fn remove_nodes_from_groups(
        &self,
        node_indices: Vec<PyNodeInput>,
        groups: Vec<PyGroupInput>,
        bypass_plugins: bool,
    ) -> PyResult<()> {
        let mut graphrecord = self.inner_mut()?;

        if bypass_plugins {
            graphrecord
                .remove_nodes_from_groups_bypass_plugins(groups.iter(), node_indices.iter())
                .map_err(PyGraphRecordError::from)?;
        } else {
            graphrecord
                .remove_nodes_from_groups(groups.iter(), node_indices.iter())
                .map_err(PyGraphRecordError::from)?;
        }

        Ok(())
    }

    #[pyo3(signature = (group, edge_indices, bypass_plugins=false))]
    pub fn remove_edges_from_group(
        &self,
        group: PyGroupInput,
        edge_indices: Vec<EdgeIndex>,
        bypass_plugins: bool,
    ) -> PyResult<()> {
        let mut graphrecord = self.inner_mut()?;

        if bypass_plugins {
            edge_indices.into_iter().try_for_each(|edge_index| {
                Ok(graphrecord
                    .remove_edge_from_group_bypass_plugins(&group, &edge_index)
                    .map_err(PyGraphRecordError::from)?)
            })
        } else {
            edge_indices.into_iter().try_for_each(|edge_index| {
                Ok(graphrecord
                    .remove_edge_from_group(&group, &edge_index)
                    .map_err(PyGraphRecordError::from)?)
            })
        }
    }

    #[pyo3(signature = (edge_index, groups, bypass_plugins=false))]
    pub fn remove_edge_from_groups(
        &self,
        edge_index: EdgeIndex,
        groups: Vec<PyGroupInput>,
        bypass_plugins: bool,
    ) -> PyResult<()> {
        let mut graphrecord = self.inner_mut()?;

        if bypass_plugins {
            graphrecord
                .remove_edge_from_groups_bypass_plugins(groups.iter(), &edge_index)
                .map_err(PyGraphRecordError::from)?;
        } else {
            graphrecord
                .remove_edge_from_groups(groups.iter(), &edge_index)
                .map_err(PyGraphRecordError::from)?;
        }

        Ok(())
    }

    #[pyo3(signature = (edge_indices, groups, bypass_plugins=false))]
    pub fn remove_edges_from_groups(
        &self,
        edge_indices: Vec<EdgeIndex>,
        groups: Vec<PyGroupInput>,
        bypass_plugins: bool,
    ) -> PyResult<()> {
        let mut graphrecord = self.inner_mut()?;

        if bypass_plugins {
            graphrecord
                .remove_edges_from_groups_bypass_plugins(groups.iter(), &edge_indices)
                .map_err(PyGraphRecordError::from)?;
        } else {
            graphrecord
                .remove_edges_from_groups(groups.iter(), &edge_indices)
                .map_err(PyGraphRecordError::from)?;
        }

        Ok(())
    }

    pub fn nodes_in_group(
        &self,
        groups: Vec<PyGroupInput>,
    ) -> PyResult<HashMap<PyGroupInput, Vec<PyNodeIndex>>> {
        let graphrecord = self.inner()?;

        groups
            .into_iter()
            .map(|input| {
                let nodes_attributes = graphrecord
                    .nodes_in_group(&input)
                    .map_err(PyGraphRecordError::from)?
                    .map(|node_index| node_index.clone().into())
                    .collect();

                Ok((input, nodes_attributes))
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
        groups: Vec<PyGroupInput>,
    ) -> PyResult<HashMap<PyGroupInput, Vec<EdgeIndex>>> {
        let graphrecord = self.inner()?;

        groups
            .into_iter()
            .map(|input| {
                let edges = graphrecord
                    .edges_in_group(&input)
                    .map_err(PyGraphRecordError::from)?
                    .copied()
                    .collect();

                Ok((input, edges))
            })
            .collect()
    }

    pub fn ungrouped_edges(&self) -> PyResult<Vec<EdgeIndex>> {
        Ok(self.inner()?.ungrouped_edges().copied().collect())
    }

    pub fn groups_of_node(
        &self,
        nodes: Vec<PyNodeInput>,
    ) -> PyResult<HashMap<PyNodeInput, Vec<PyGroup>>> {
        let graphrecord = self.inner()?;

        nodes
            .into_iter()
            .map(|input| {
                let groups = graphrecord
                    .groups_of_node(&input)
                    .map_err(PyGraphRecordError::from)?
                    .map(|group| group.clone().into())
                    .collect();

                Ok((input, groups))
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

    pub fn node_handles(&self) -> PyResult<Vec<PyNodeHandle>> {
        Ok(self
            .inner()?
            .node_handles()
            .map(PyNodeHandle::from)
            .collect())
    }

    pub fn group_handles(&self) -> PyResult<Vec<PyGroupHandle>> {
        Ok(self
            .inner()?
            .group_handles()
            .map(PyGroupHandle::from)
            .collect())
    }

    pub fn node_handles_in_group(
        &self,
        groups: Vec<PyGroupInput>,
    ) -> PyResult<HashMap<PyGroupInput, Vec<PyNodeHandle>>> {
        let graphrecord = self.inner()?;

        groups
            .into_iter()
            .map(|input| {
                let handles = graphrecord
                    .node_handles_in_group(&input)
                    .map_err(PyGraphRecordError::from)?
                    .map(PyNodeHandle::from)
                    .collect();

                Ok((input, handles))
            })
            .collect()
    }

    pub fn ungrouped_node_handles(&self) -> PyResult<Vec<PyNodeHandle>> {
        Ok(self
            .inner()?
            .ungrouped_node_handles()
            .map(PyNodeHandle::from)
            .collect())
    }

    pub fn group_handles_of_node(
        &self,
        nodes: Vec<PyNodeInput>,
    ) -> PyResult<HashMap<PyNodeInput, Vec<PyGroupHandle>>> {
        let graphrecord = self.inner()?;

        nodes
            .into_iter()
            .map(|input| {
                let handles = graphrecord
                    .group_handles_of_node(&input)
                    .map_err(PyGraphRecordError::from)?
                    .map(PyGroupHandle::from)
                    .collect();

                Ok((input, handles))
            })
            .collect()
    }

    pub fn group_handles_of_edge(
        &self,
        edge_index: Vec<EdgeIndex>,
    ) -> PyResult<HashMap<EdgeIndex, Vec<PyGroupHandle>>> {
        let graphrecord = self.inner()?;

        edge_index
            .into_iter()
            .map(|edge_index| {
                let handles = graphrecord
                    .group_handles_of_edge(&edge_index)
                    .map_err(PyGraphRecordError::from)?
                    .map(PyGroupHandle::from)
                    .collect();

                Ok((edge_index, handles))
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

    pub fn contains_node(&self, node: PyNodeInput) -> PyResult<bool> {
        Ok(self.inner()?.contains_node(&node))
    }

    pub fn contains_edge(&self, edge_index: EdgeIndex) -> PyResult<bool> {
        Ok(self.inner()?.contains_edge(&edge_index))
    }

    pub fn contains_group(&self, group: PyGroupInput) -> PyResult<bool> {
        Ok(self.inner()?.contains_group(&group))
    }

    pub fn node_handle(&self, node_index: PyNodeIndex) -> PyResult<Option<PyNodeHandle>> {
        let graphrecord = self.inner()?;
        let core_index: NodeIndex = node_index.into();

        Ok(
            <GraphRecord as HandleLookup<NodeIndexKind>>::handle_of(&graphrecord, &core_index)
                .map(PyNodeHandle::from),
        )
    }

    pub fn group_handle(&self, group: PyGroup) -> PyResult<Option<PyGroupHandle>> {
        let graphrecord = self.inner()?;
        let core_group: Group = group.into();

        Ok(
            <GraphRecord as HandleLookup<GroupKind>>::handle_of(&graphrecord, &core_group)
                .map(PyGroupHandle::from),
        )
    }

    pub fn attribute_handle(
        &self,
        name: PyGraphRecordAttribute,
    ) -> PyResult<Option<PyAttributeHandle>> {
        let graphrecord = self.inner()?;
        let core_name: GraphRecordAttribute = name.into();

        Ok(
            <GraphRecord as HandleLookup<AttributeNameKind>>::handle_of(&graphrecord, &core_name)
                .map(PyAttributeHandle::from),
        )
    }

    pub fn resolve_node_handle(&self, handle: PyNodeHandle) -> PyResult<PyNodeIndex> {
        let graphrecord = self.inner()?;
        let value = <GraphRecord as HandleLookup<NodeIndexKind>>::resolve_handle(
            &graphrecord,
            handle.into(),
        )
        .map_err(PyGraphRecordError::from)?;

        Ok(PyNodeIndex::from(value.clone()))
    }

    pub fn resolve_group_handle(&self, handle: PyGroupHandle) -> PyResult<PyGroup> {
        let graphrecord = self.inner()?;
        let value =
            <GraphRecord as HandleLookup<GroupKind>>::resolve_handle(&graphrecord, handle.into())
                .map_err(PyGraphRecordError::from)?;

        Ok(PyGroup::from(value.clone()))
    }

    pub fn resolve_attribute_handle(
        &self,
        handle: PyAttributeHandle,
    ) -> PyResult<PyGraphRecordAttribute> {
        let graphrecord = self.inner()?;
        let value = <GraphRecord as HandleLookup<AttributeNameKind>>::resolve_handle(
            &graphrecord,
            handle.into(),
        )
        .map_err(PyGraphRecordError::from)?;

        Ok(PyGraphRecordAttribute::from(value.clone()))
    }

    pub fn outgoing_neighbors(
        &self,
        node_indices: Vec<PyNodeInput>,
    ) -> PyResult<HashMap<PyNodeInput, Vec<PyNodeIndex>>> {
        let graphrecord = self.inner()?;

        node_indices
            .into_iter()
            .map(|input| {
                let neighbors = graphrecord
                    .outgoing_neighbors(&input)
                    .map_err(PyGraphRecordError::from)?
                    .map(|neighbor| neighbor.clone().into())
                    .collect();

                Ok((input, neighbors))
            })
            .collect()
    }

    pub fn incoming_neighbors(
        &self,
        node_indices: Vec<PyNodeInput>,
    ) -> PyResult<HashMap<PyNodeInput, Vec<PyNodeIndex>>> {
        let graphrecord = self.inner()?;

        node_indices
            .into_iter()
            .map(|input| {
                let neighbors = graphrecord
                    .incoming_neighbors(&input)
                    .map_err(PyGraphRecordError::from)?
                    .map(|neighbor| neighbor.clone().into())
                    .collect();

                Ok((input, neighbors))
            })
            .collect()
    }

    pub fn neighbors(
        &self,
        node_indices: Vec<PyNodeInput>,
    ) -> PyResult<HashMap<PyNodeInput, Vec<PyNodeIndex>>> {
        let graphrecord = self.inner()?;

        node_indices
            .into_iter()
            .map(|input| {
                let neighbors = graphrecord
                    .neighbors(&input)
                    .map_err(PyGraphRecordError::from)?
                    .map(|neighbor| neighbor.clone().into())
                    .collect();

                Ok((input, neighbors))
            })
            .collect()
    }

    pub fn outgoing_neighbor_handles(
        &self,
        node_indices: Vec<PyNodeInput>,
    ) -> PyResult<HashMap<PyNodeInput, Vec<PyNodeHandle>>> {
        let graphrecord = self.inner()?;

        node_indices
            .into_iter()
            .map(|input| {
                let handles = graphrecord
                    .outgoing_neighbor_handles(&input)
                    .map_err(PyGraphRecordError::from)?
                    .map(PyNodeHandle::from)
                    .collect();

                Ok((input, handles))
            })
            .collect()
    }

    pub fn incoming_neighbor_handles(
        &self,
        node_indices: Vec<PyNodeInput>,
    ) -> PyResult<HashMap<PyNodeInput, Vec<PyNodeHandle>>> {
        let graphrecord = self.inner()?;

        node_indices
            .into_iter()
            .map(|input| {
                let handles = graphrecord
                    .incoming_neighbor_handles(&input)
                    .map_err(PyGraphRecordError::from)?
                    .map(PyNodeHandle::from)
                    .collect();

                Ok((input, handles))
            })
            .collect()
    }

    pub fn neighbor_handles(
        &self,
        node_indices: Vec<PyNodeInput>,
    ) -> PyResult<HashMap<PyNodeInput, Vec<PyNodeHandle>>> {
        let graphrecord = self.inner()?;

        node_indices
            .into_iter()
            .map(|input| {
                let handles = graphrecord
                    .neighbor_handles(&input)
                    .map_err(PyGraphRecordError::from)?
                    .map(PyNodeHandle::from)
                    .collect();

                Ok((input, handles))
            })
            .collect()
    }

    #[pyo3(signature = (bypass_plugins=false))]
    pub fn clear(&self, bypass_plugins: bool) -> PyResult<()> {
        let mut graphrecord = self.inner_mut()?;

        if bypass_plugins {
            Ok(graphrecord
                .clear_bypass_plugins()
                .map_err(PyGraphRecordError::from)?)
        } else {
            Ok(graphrecord.clear().map_err(PyGraphRecordError::from)?)
        }
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
        group: PyGroupInput,
        truncate_details: Option<usize>,
    ) -> PyResult<PyGroupOverview> {
        Ok(self
            .inner()?
            .group_overview(&group, truncate_details)
            .map_err(PyGraphRecordError::from)?
            .into())
    }
}
