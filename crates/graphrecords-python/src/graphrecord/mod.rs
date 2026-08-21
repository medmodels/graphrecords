pub mod datatype;
pub mod direction;
pub mod edge_index;
pub mod errors;
mod frame;
pub mod identifier;
pub mod on_conflict;
pub mod overview;
pub mod plugins;
pub mod record_batch;
pub mod schema;
pub mod selection;
pub mod source;
pub mod traits;
pub mod value;
pub mod views;
pub mod writer;

use crate::{
    conversion_lut::{ConversionLut, TypeObjectKey},
    graphrecord::{
        overview::{PyGroupOverview, PyOverview},
        plugins::PyPlugin,
    },
    querying::{PyExpression, PySeries},
};
pub use direction::PyEdgeDirection;
pub use edge_index::PyEdgeIndex;
use errors::PyGraphRecordError;
use graphrecords_core::graphrecord::{GraphRecord, GroupIndex, NodeIndex};
use graphrecords_overview::{GroupOverviewable, Overviewable};
use graphrecords_query::{Queryable, dynamic};
use identifier::PyIdentifier;
use on_conflict::PyOnConflict;
use pyo3::{
    IntoPyObjectExt,
    exceptions::PyTypeError,
    prelude::*,
    types::{PyBytes, PyBytesMethods, PyDict, PyTuple},
};
use pyo3_polars::PyDataFrame;
use record_batch::PyRecordBatch;
use schema::PySchema;
use selection::ResolvedSelection;
use source::{PyEdgeSource, PyNodeSource};
use std::{any::Any, collections::HashMap, path::PathBuf, sync::Arc};
use traits::DeepInto;
use value::PyValue;
use views::{PyEdgeView, PyGroupView, PyNodeView};
use writer::PyWriter;

pub type PyAttributeName = PyIdentifier;
pub type PyAttributes = HashMap<PyAttributeName, PyValue>;
pub type PyGroupIndex = PyIdentifier;
pub type PyNodeIndex = PyIdentifier;
pub type PyPluginName = PyIdentifier;
type Converter<T> = fn(&Bound<'_, PyAny>) -> PyResult<T>;
type Lut<T> = ConversionLut<TypeObjectKey, Option<Converter<T>>>;

#[pyclass(frozen, eq, module = "graphrecords._graphrecords.graphrecord")]
#[repr(transparent)]
#[derive(Clone, Debug, PartialEq)]
pub struct PyGraphRecord(GraphRecord);

impl From<GraphRecord> for PyGraphRecord {
    fn from(value: GraphRecord) -> Self {
        Self(value)
    }
}

impl From<PyGraphRecord> for GraphRecord {
    fn from(value: PyGraphRecord) -> Self {
        value.0
    }
}

impl Default for PyGraphRecord {
    fn default() -> Self {
        Self::new()
    }
}

impl PyGraphRecord {
    pub const fn record(&self) -> &GraphRecord {
        &self.0
    }

    fn python_plugins(
        &self,
        py: Python<'_>,
    ) -> Result<Vec<(PyPluginName, Py<PyAny>)>, PyGraphRecordError> {
        self.0
            .plugin_entries()
            .map(|(name, plugin)| {
                let plugin = Arc::clone(plugin) as Arc<dyn Any + Send + Sync>;
                let plugin = plugin.downcast::<PyPlugin>().map_err(|_| {
                    PyGraphRecordError::Conversion(format!(
                        "Plugin `{name}` is not a python plugin"
                    ))
                })?;

                Ok((
                    PyPluginName::from(name.clone()),
                    plugin.plugin().clone_ref(py),
                ))
            })
            .collect()
    }
}

#[pymethods]
impl PyGraphRecord {
    #[new]
    pub fn new() -> Self {
        GraphRecord::new().into()
    }

    #[staticmethod]
    pub fn with_schema(schema: PySchema) -> Self {
        GraphRecord::with_schema(schema.into()).into()
    }

    #[staticmethod]
    pub fn from_ron(path: PathBuf) -> PyResult<Self> {
        Ok(GraphRecord::from_ron(path)
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    pub fn add_plugin(&self, name: PyPluginName, plugin: Py<PyAny>) -> PyResult<Self> {
        Ok(self
            .0
            .add_plugin(name, PyPlugin::new(plugin))
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    pub fn remove_plugin(&self, name: PyPluginName) -> PyResult<Self> {
        Ok(self
            .0
            .remove_plugin(name)
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    #[getter]
    pub fn plugins(&self) -> Vec<PyPluginName> {
        self.0
            .plugins()
            .map(|name| PyPluginName::from(name.clone()))
            .collect()
    }

    #[getter]
    pub fn plugin_entries(&self, py: Python<'_>) -> PyResult<HashMap<PyPluginName, Py<PyAny>>> {
        Ok(self.python_plugins(py)?.into_iter().collect())
    }

    pub fn add_nodes(&self, source: PyNodeSource) -> PyResult<Self> {
        Ok(self
            .0
            .add_nodes(source)
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    pub fn add_node(
        &self,
        node_index: &Bound<'_, PyAny>,
        attributes: PyAttributes,
    ) -> PyResult<Self> {
        let node_index = ResolvedSelection::single_node(&self.0, node_index)?;

        Ok(self
            .0
            .add_node(node_index, attributes)
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    pub fn add_nodes_in_group(
        &self,
        source: PyNodeSource,
        group_index: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let group_index = ResolvedSelection::single_group(&self.0, group_index)?;

        Ok(self
            .0
            .add_nodes_in_group(source, group_index)
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    pub fn add_node_in_group(
        &self,
        node_index: &Bound<'_, PyAny>,
        attributes: PyAttributes,
        group_index: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let node_index = ResolvedSelection::single_node(&self.0, node_index)?;
        let group_index = ResolvedSelection::single_group(&self.0, group_index)?;

        Ok(self
            .0
            .add_node_in_group(node_index, attributes, group_index)
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    pub fn add_edges(&self, source: PyEdgeSource) -> PyResult<Self> {
        Ok(self
            .0
            .add_edges(source)
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    pub fn add_edge(
        &self,
        source_node_index: &Bound<'_, PyAny>,
        target_node_index: &Bound<'_, PyAny>,
        attributes: PyAttributes,
    ) -> PyResult<Self> {
        let source_node_index = ResolvedSelection::single_node(&self.0, source_node_index)?;
        let target_node_index = ResolvedSelection::single_node(&self.0, target_node_index)?;

        Ok(self
            .0
            .add_edge(source_node_index, target_node_index, attributes)
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    pub fn add_edges_in_group(
        &self,
        source: PyEdgeSource,
        group_index: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let group_index = ResolvedSelection::single_group(&self.0, group_index)?;

        Ok(self
            .0
            .add_edges_in_group(source, group_index)
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    pub fn add_edge_in_group(
        &self,
        source_node_index: &Bound<'_, PyAny>,
        target_node_index: &Bound<'_, PyAny>,
        attributes: PyAttributes,
        group_index: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let source_node_index = ResolvedSelection::single_node(&self.0, source_node_index)?;
        let target_node_index = ResolvedSelection::single_node(&self.0, target_node_index)?;
        let group_index = ResolvedSelection::single_group(&self.0, group_index)?;

        Ok(self
            .0
            .add_edge_in_group(
                source_node_index,
                target_node_index,
                attributes,
                group_index,
            )
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    pub fn remove_nodes(&self, node_indices: &Bound<'_, PyAny>) -> PyResult<Self> {
        let node_indices = ResolvedSelection::nodes(&self.0, node_indices)?;

        Ok(self
            .0
            .remove_nodes(node_indices)
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    pub fn remove_edges(&self, edge_indices: &Bound<'_, PyAny>) -> PyResult<Self> {
        let edge_indices = ResolvedSelection::edges(&self.0, edge_indices)?;

        Ok(self
            .0
            .remove_edges(edge_indices)
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    pub fn keep_nodes(&self, node_indices: &Bound<'_, PyAny>) -> PyResult<Self> {
        let node_indices = ResolvedSelection::nodes(&self.0, node_indices)?;

        Ok(self
            .0
            .keep_nodes(node_indices)
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    pub fn keep_edges(&self, edge_indices: &Bound<'_, PyAny>) -> PyResult<Self> {
        let edge_indices = ResolvedSelection::edges(&self.0, edge_indices)?;

        Ok(self
            .0
            .keep_edges(edge_indices)
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    pub fn keep_groups(&self, group_indices: &Bound<'_, PyAny>) -> PyResult<Self> {
        let group_indices = ResolvedSelection::groups(&self.0, group_indices)?;

        Ok(self
            .0
            .keep_groups(group_indices)
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    pub fn intersect(&self, other: &Self) -> PyResult<Self> {
        Ok(self
            .0
            .intersect(&other.0)
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    pub fn difference(&self, other: &Self) -> PyResult<Self> {
        Ok(self
            .0
            .difference(&other.0)
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    #[pyo3(signature = (other, on_conflict=PyOnConflict::Raise))]
    pub fn merge(&self, other: &Self, on_conflict: PyOnConflict) -> PyResult<Self> {
        Ok(self
            .0
            .merge(&other.0, on_conflict.into())
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    pub fn set_node_attributes(
        &self,
        node_indices: &Bound<'_, PyAny>,
        attributes: PyAttributes,
    ) -> PyResult<Self> {
        let node_indices = ResolvedSelection::nodes(&self.0, node_indices)?;

        Ok(self
            .0
            .set_node_attributes(node_indices, attributes)
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    pub fn replace_node_attributes(
        &self,
        node_indices: &Bound<'_, PyAny>,
        attributes: PyAttributes,
    ) -> PyResult<Self> {
        let node_indices = ResolvedSelection::nodes(&self.0, node_indices)?;

        Ok(self
            .0
            .replace_node_attributes(node_indices, attributes)
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    pub fn remove_node_attributes(
        &self,
        node_indices: &Bound<'_, PyAny>,
        attribute_names: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let node_indices = ResolvedSelection::nodes(&self.0, node_indices)?;
        let attribute_names = ResolvedSelection::attribute_names(attribute_names)?;

        Ok(self
            .0
            .remove_node_attributes(node_indices, attribute_names)
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    pub fn set_edge_attributes(
        &self,
        edge_indices: &Bound<'_, PyAny>,
        attributes: PyAttributes,
    ) -> PyResult<Self> {
        let edge_indices = ResolvedSelection::edges(&self.0, edge_indices)?;

        Ok(self
            .0
            .set_edge_attributes(edge_indices, attributes)
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    pub fn replace_edge_attributes(
        &self,
        edge_indices: &Bound<'_, PyAny>,
        attributes: PyAttributes,
    ) -> PyResult<Self> {
        let edge_indices = ResolvedSelection::edges(&self.0, edge_indices)?;

        Ok(self
            .0
            .replace_edge_attributes(edge_indices, attributes)
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    pub fn remove_edge_attributes(
        &self,
        edge_indices: &Bound<'_, PyAny>,
        attribute_names: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let edge_indices = ResolvedSelection::edges(&self.0, edge_indices)?;
        let attribute_names = ResolvedSelection::attribute_names(attribute_names)?;

        Ok(self
            .0
            .remove_edge_attributes(edge_indices, attribute_names)
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    pub fn add_group(&self, group_index: &Bound<'_, PyAny>) -> PyResult<Self> {
        let group_index = ResolvedSelection::single_group(&self.0, group_index)?;

        Ok(self
            .0
            .add_group(group_index)
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    pub fn remove_groups(&self, group_indices: &Bound<'_, PyAny>) -> PyResult<Self> {
        let group_indices = ResolvedSelection::groups(&self.0, group_indices)?;

        Ok(self
            .0
            .remove_groups(group_indices)
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    pub fn add_nodes_to_group(
        &self,
        node_indices: &Bound<'_, PyAny>,
        group_index: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let node_indices = ResolvedSelection::nodes(&self.0, node_indices)?;
        let group_index = ResolvedSelection::single_group(&self.0, group_index)?;

        Ok(self
            .0
            .add_nodes_to_group(node_indices, group_index)
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    pub fn remove_nodes_from_group(
        &self,
        node_indices: &Bound<'_, PyAny>,
        group_index: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let node_indices = ResolvedSelection::nodes(&self.0, node_indices)?;
        let group_index = ResolvedSelection::single_group(&self.0, group_index)?;

        Ok(self
            .0
            .remove_nodes_from_group(node_indices, group_index)
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    pub fn add_edges_to_group(
        &self,
        edge_indices: &Bound<'_, PyAny>,
        group_index: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let edge_indices = ResolvedSelection::edges(&self.0, edge_indices)?;
        let group_index = ResolvedSelection::single_group(&self.0, group_index)?;

        Ok(self
            .0
            .add_edges_to_group(edge_indices, group_index)
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    pub fn remove_edges_from_group(
        &self,
        edge_indices: &Bound<'_, PyAny>,
        group_index: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let edge_indices = ResolvedSelection::edges(&self.0, edge_indices)?;
        let group_index = ResolvedSelection::single_group(&self.0, group_index)?;

        Ok(self
            .0
            .remove_edges_from_group(edge_indices, group_index)
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    #[getter]
    pub fn schema(&self) -> PySchema {
        self.0.schema().clone().into()
    }

    pub fn set_schema(&self, schema: PySchema) -> PyResult<Self> {
        Ok(self
            .0
            .set_schema(schema.into())
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    pub fn freeze_schema(&self) -> PyResult<Self> {
        Ok(self
            .0
            .freeze_schema()
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    pub fn unfreeze_schema(&self) -> PyResult<Self> {
        Ok(self
            .0
            .unfreeze_schema()
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    pub fn clear(&self) -> PyResult<Self> {
        Ok(self.0.clear().map_err(PyGraphRecordError::from)?.into())
    }

    pub fn compact(&self) -> Self {
        self.0.compact().into()
    }

    pub fn node_count(&self) -> usize {
        self.0.node_count()
    }

    pub fn edge_count(&self) -> usize {
        self.0.edge_count()
    }

    pub fn group_count(&self) -> usize {
        self.0.group_count()
    }

    pub fn contains_node(&self, node_index: PyNodeIndex) -> bool {
        self.0.contains_node(&NodeIndex::from(node_index))
    }

    pub fn contains_edge(&self, edge_index: PyEdgeIndex) -> bool {
        self.0.contains_edge(&edge_index)
    }

    pub fn contains_group(&self, group_index: PyGroupIndex) -> bool {
        self.0.contains_group(&GroupIndex::from(group_index))
    }

    pub fn node_indices(&self, py: Python<'_>) -> Vec<PyNodeIndex> {
        let node_indices: Vec<_> = py.detach(|| {
            self.0
                .node_indices()
                .map(NodeIndex::from)
                .collect::<Vec<_>>()
        });

        node_indices.deep_into()
    }

    pub fn edge_indices(&self, py: Python<'_>) -> Vec<PyEdgeIndex> {
        let edge_indices: Vec<_> = py.detach(|| self.0.edge_indices().collect::<Vec<_>>());

        edge_indices.deep_into()
    }

    pub fn group_indices(&self, py: Python<'_>) -> Vec<PyGroupIndex> {
        let group_indices: Vec<_> =
            py.detach(|| self.0.group_indices().cloned().collect::<Vec<_>>());

        group_indices.deep_into()
    }

    pub fn nodes(&self) -> PySeries {
        self.0.query(dynamic::nodes()).into()
    }

    pub fn edges(&self) -> PySeries {
        self.0.query(dynamic::edges()).into()
    }

    pub fn groups(&self) -> PySeries {
        self.0.query(dynamic::groups()).into()
    }

    pub fn query(&self, expression: &PyExpression) -> PySeries {
        self.0.query(expression.expression().clone()).into()
    }

    pub fn node(&self, node_index: PyNodeIndex) -> PyResult<PyNodeView> {
        PyNodeView::new(self.0.clone(), node_index.into())
    }

    pub fn edge(&self, edge_index: PyEdgeIndex) -> PyResult<PyEdgeView> {
        PyEdgeView::new(self.0.clone(), edge_index.into())
    }

    pub fn group(&self, group_index: PyGroupIndex) -> PyResult<PyGroupView> {
        PyGroupView::new(self.0.clone(), group_index.into())
    }

    pub fn export(&self, writer: Py<PyAny>) -> PyResult<Py<PyAny>> {
        Ok(self
            .0
            .export(PyWriter::new(writer))
            .map_err(PyGraphRecordError::from)?)
    }

    pub fn to_polars(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let export = py
            .detach(|| self.0.to_polars())
            .map_err(PyGraphRecordError::from)?;

        frame::partitioned(py, export, |py, frame| PyDataFrame(frame).into_py_any(py))
    }

    pub fn to_arrow(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let export = py
            .detach(|| self.0.to_arrow())
            .map_err(PyGraphRecordError::from)?;

        frame::partitioned(py, export, |py, table| {
            PyRecordBatch::from(table).into_py_any(py)
        })
    }

    pub fn to_ron(&self, path: PathBuf) -> PyResult<()> {
        Ok(self.0.to_ron(path).map_err(PyGraphRecordError::from)?)
    }

    #[pyo3(signature = (truncate_details=None))]
    pub fn overview(
        &self,
        py: Python<'_>,
        truncate_details: Option<usize>,
    ) -> PyResult<PyOverview> {
        let overview = py
            .detach(|| self.0.overview(truncate_details))
            .map_err(PyGraphRecordError::from)?;

        Ok(overview.into())
    }

    #[pyo3(signature = (group_index, truncate_details=None))]
    pub fn group_overview(
        &self,
        py: Python<'_>,
        group_index: PyGroupIndex,
        truncate_details: Option<usize>,
    ) -> PyResult<PyGroupOverview> {
        let group_index = GroupIndex::from(group_index);
        let overview = py
            .detach(|| self.0.group_overview(&group_index, truncate_details))
            .map_err(PyGraphRecordError::from)?;

        Ok(overview.into())
    }

    #[staticmethod]
    pub fn _from_bytes(py: Python<'_>, data: &Bound<'_, PyBytes>) -> PyResult<Self> {
        let bytes = data.as_bytes();
        let record: GraphRecord = py.detach(|| bincode::deserialize(bytes)).map_err(|_| {
            PyGraphRecordError::Conversion("Failed to deserialize GraphRecord".to_string())
        })?;

        Ok(record.into())
    }

    pub fn _to_bytes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let bytes = py.detach(|| bincode::serialize(&self.0)).map_err(|_| {
            PyGraphRecordError::Conversion("Failed to serialize GraphRecord".to_string())
        })?;

        Ok(PyBytes::new(py, &bytes))
    }

    #[staticmethod]
    pub fn _restore(
        py: Python<'_>,
        data: &Bound<'_, PyBytes>,
        plugins: Vec<(PyPluginName, Py<PyAny>)>,
    ) -> PyResult<Self> {
        let record = GraphRecord::from(Self::_from_bytes(py, data)?);
        let entries = plugins
            .into_iter()
            .map(|(name, plugin)| (name, Arc::new(PyPlugin::new(plugin)) as _));

        Ok(record
            .reattach_plugins(entries)
            .map_err(PyGraphRecordError::from)?
            .into())
    }

    pub fn __reduce__<'py>(&self, py: Python<'py>) -> PyResult<(Py<PyAny>, Bound<'py, PyTuple>)> {
        let bytes = self._to_bytes(py)?;
        let plugins = self.python_plugins(py)?;
        let constructor = py.get_type::<Self>().getattr("_restore")?.unbind();
        let arguments = (bytes, plugins).into_pyobject(py)?;

        Ok((constructor, arguments))
    }

    pub fn __hash__(&self) -> PyResult<isize> {
        Err(PyTypeError::new_err("unhashable type: 'GraphRecord'"))
    }

    pub fn __copy__(&self) -> Self {
        self.clone()
    }

    #[pyo3(signature = (memo=None))]
    pub fn __deepcopy__(&self, memo: Option<&Bound<'_, PyDict>>) -> Self {
        let _ = memo;

        self.clone()
    }

    pub fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let overview = py
            .detach(|| self.0.overview(None))
            .map_err(PyGraphRecordError::from)?;

        Ok(overview.to_string())
    }
}
