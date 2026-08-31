use super::{
    PyAttributeName, PyAttributes, PyGraphRecord, PyGroupIndex, PyNodeIndex,
    edge_index::PyEdgeIndex,
    errors::PyGraphRecordError,
    schema::PySchema,
    source::{PyEdgeSource, PyNodeSource},
    traits::DeepInto,
    value::PyValue,
};
use graphrecords_core::{
    errors::{GraphRecordError, GraphRecordResult},
    graphrecord::{
        AttributeMap, Changes, EdgeBatch, EdgeSource, GraphRecord, NodeBatch, NodeIndex,
        NodeSource, Plugin,
        changes::{
            AddEdges, AddEdgesInGroup, AddEdgesToGroup, AddGroup, AddNodes, AddNodesInGroup,
            AddNodesToGroup, Clear, FreezeSchema, RemoveEdgeAttributes, RemoveEdges,
            RemoveEdgesFromGroup, RemoveGroups, RemoveNodeAttributes, RemoveNodes,
            RemoveNodesFromGroup, ReplaceEdgeAttributes, ReplaceNodeAttributes, SetEdgeAttributes,
            SetNodeAttributes, SetSchema, UnfreezeSchema,
        },
    },
};
use pyo3::{
    Bound, IntoPyObjectExt, Py, PyAny, PyErr, PyResult, Python,
    exceptions::{PyAttributeError, PyTypeError},
    prelude::*,
    types::PyList,
};
use std::sync::Arc;

#[pyclass(frozen, module = "graphrecords._graphrecords.plugins")]
#[repr(transparent)]
#[derive(Clone)]
pub struct PyNodeBatch(Arc<Vec<(NodeIndex, AttributeMap)>>);

impl From<&NodeBatch> for PyNodeBatch {
    fn from(batch: &NodeBatch) -> Self {
        Self(Arc::new(
            batch
                .iter()
                .map(|(node_index, attributes)| (node_index.clone(), attributes.clone()))
                .collect(),
        ))
    }
}

impl From<&PyNodeBatch> for NodeBatch {
    fn from(batch: &PyNodeBatch) -> Self {
        Self::from(batch.0.as_ref().clone())
    }
}

#[pymethods]
impl PyNodeBatch {
    #[new]
    pub fn new(nodes: PyNodeSource) -> PyResult<Self> {
        let batch = nodes.collect_nodes().map_err(PyGraphRecordError::from)?;

        Ok(Self(Arc::new(batch.into_iter().collect())))
    }

    pub fn __len__(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn __iter__(&self) -> PyNodeBatchIterator {
        PyNodeBatchIterator {
            elements: Arc::clone(&self.0),
            cursor: 0,
        }
    }

    pub fn attribute_values(&self, attribute_name: PyAttributeName) -> Vec<(PyNodeIndex, PyValue)> {
        let attribute_name = attribute_name.into();

        self.0
            .iter()
            .filter_map(|(node_index, attributes)| {
                attributes
                    .get(&attribute_name)
                    .map(|value| (node_index.clone(), value.clone()).deep_into())
            })
            .collect()
    }
}

#[pyclass(module = "graphrecords._graphrecords.plugins")]
pub struct PyNodeBatchIterator {
    elements: Arc<Vec<(NodeIndex, AttributeMap)>>,
    cursor: usize,
}

#[pymethods]
impl PyNodeBatchIterator {
    pub const fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    pub fn __next__(&mut self) -> Option<(PyNodeIndex, PyAttributes)> {
        let element = self.elements.get(self.cursor)?;
        self.cursor += 1;

        Some(element.clone().deep_into())
    }
}

#[pyclass(frozen, module = "graphrecords._graphrecords.plugins")]
#[repr(transparent)]
#[derive(Clone)]
pub struct PyEdgeBatch(Arc<Vec<(NodeIndex, NodeIndex, AttributeMap)>>);

impl From<&EdgeBatch> for PyEdgeBatch {
    fn from(batch: &EdgeBatch) -> Self {
        Self(Arc::new(
            batch
                .iter()
                .map(|(source_node_index, target_node_index, attributes)| {
                    (
                        source_node_index.clone(),
                        target_node_index.clone(),
                        attributes.clone(),
                    )
                })
                .collect(),
        ))
    }
}

impl From<&PyEdgeBatch> for EdgeBatch {
    fn from(batch: &PyEdgeBatch) -> Self {
        Self::from(batch.0.as_ref().clone())
    }
}

#[pymethods]
impl PyEdgeBatch {
    #[new]
    pub fn new(edges: PyEdgeSource) -> PyResult<Self> {
        let batch = edges.collect_edges().map_err(PyGraphRecordError::from)?;

        Ok(Self(Arc::new(batch.into_iter().collect())))
    }

    pub fn __len__(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn __iter__(&self) -> PyEdgeBatchIterator {
        PyEdgeBatchIterator {
            elements: Arc::clone(&self.0),
            cursor: 0,
        }
    }

    pub fn attribute_values(
        &self,
        attribute_name: PyAttributeName,
    ) -> Vec<(PyNodeIndex, PyNodeIndex, PyValue)> {
        let attribute_name = attribute_name.into();

        self.0
            .iter()
            .filter_map(|(source_node_index, target_node_index, attributes)| {
                attributes.get(&attribute_name).map(|value| {
                    (
                        source_node_index.clone(),
                        target_node_index.clone(),
                        value.clone(),
                    )
                        .deep_into()
                })
            })
            .collect()
    }
}

#[pyclass(module = "graphrecords._graphrecords.plugins")]
pub struct PyEdgeBatchIterator {
    elements: Arc<Vec<(NodeIndex, NodeIndex, AttributeMap)>>,
    cursor: usize,
}

#[pymethods]
impl PyEdgeBatchIterator {
    pub const fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    pub fn __next__(&mut self) -> Option<(PyNodeIndex, PyNodeIndex, PyAttributes)> {
        let element = self.elements.get(self.cursor)?;
        self.cursor += 1;

        Some(element.clone().deep_into())
    }
}

macro_rules! implement_batch_payload {
    ($name:ident, $core:ident, $batch:ident, $batch_view:ident) => {
        #[pyclass(frozen, module = "graphrecords._graphrecords.plugins")]
        #[repr(transparent)]
        pub struct $name($batch_view);

        impl From<&$core> for $name {
            fn from(change: &$core) -> Self {
                Self($batch_view::from(change.batch()))
            }
        }

        impl From<&$name> for $core {
            fn from(payload: &$name) -> Self {
                Self::new((&payload.0).into())
            }
        }

        #[pymethods]
        impl $name {
            #[new]
            pub fn new($batch: &$batch_view) -> Self {
                Self($batch.clone())
            }

            #[getter]
            pub fn $batch(&self) -> $batch_view {
                self.0.clone()
            }
        }
    };
}

macro_rules! implement_grouped_batch_payload {
    ($name:ident, $core:ident, $batch:ident, $batch_view:ident) => {
        #[pyclass(frozen, module = "graphrecords._graphrecords.plugins")]
        pub struct $name {
            batch: $batch_view,
            group_index: PyGroupIndex,
        }

        impl From<&$core> for $name {
            fn from(change: &$core) -> Self {
                Self {
                    batch: $batch_view::from(change.batch()),
                    group_index: change.group_index().clone().into(),
                }
            }
        }

        impl From<&$name> for $core {
            fn from(payload: &$name) -> Self {
                Self::new((&payload.batch).into(), payload.group_index.clone().into())
            }
        }

        #[pymethods]
        impl $name {
            #[new]
            pub fn new($batch: &$batch_view, group_index: PyGroupIndex) -> Self {
                Self {
                    batch: $batch.clone(),
                    group_index,
                }
            }

            #[getter]
            pub fn $batch(&self) -> $batch_view {
                self.batch.clone()
            }

            #[getter]
            pub fn group_index(&self) -> PyGroupIndex {
                self.group_index.clone()
            }
        }
    };
}

macro_rules! implement_node_indices_payload {
    ($name:ident, $core:ident) => {
        #[pyclass(frozen, module = "graphrecords._graphrecords.plugins")]
        #[repr(transparent)]
        pub struct $name(Vec<PyNodeIndex>);

        impl From<&$core> for $name {
            fn from(change: &$core) -> Self {
                Self(change.node_indices().deep_into())
            }
        }

        impl From<&$name> for $core {
            fn from(payload: &$name) -> Self {
                Self::new(payload.0.as_slice().deep_into())
            }
        }

        #[pymethods]
        impl $name {
            #[new]
            pub const fn new(node_indices: Vec<PyNodeIndex>) -> Self {
                Self(node_indices)
            }

            #[getter]
            pub fn node_indices(&self) -> Vec<PyNodeIndex> {
                self.0.clone()
            }
        }
    };
}

macro_rules! implement_edge_indices_payload {
    ($name:ident, $core:ident) => {
        #[pyclass(frozen, module = "graphrecords._graphrecords.plugins")]
        #[repr(transparent)]
        pub struct $name(Vec<PyEdgeIndex>);

        impl From<&$core> for $name {
            fn from(change: &$core) -> Self {
                Self(change.edge_indices().deep_into())
            }
        }

        impl From<&$name> for $core {
            fn from(payload: &$name) -> Self {
                Self::new(payload.0.as_slice().deep_into())
            }
        }

        #[pymethods]
        impl $name {
            #[new]
            pub const fn new(edge_indices: Vec<PyEdgeIndex>) -> Self {
                Self(edge_indices)
            }

            #[getter]
            pub fn edge_indices(&self) -> Vec<PyEdgeIndex> {
                self.0.clone()
            }
        }
    };
}

macro_rules! implement_node_attributes_payload {
    ($name:ident, $core:ident) => {
        #[pyclass(frozen, module = "graphrecords._graphrecords.plugins")]
        pub struct $name {
            node_indices: Vec<PyNodeIndex>,
            attributes: PyAttributes,
        }

        impl From<&$core> for $name {
            fn from(change: &$core) -> Self {
                Self {
                    node_indices: change.node_indices().deep_into(),
                    attributes: change.attributes().clone().deep_into(),
                }
            }
        }

        impl From<&$name> for $core {
            fn from(payload: &$name) -> Self {
                Self::new(
                    payload.node_indices.as_slice().deep_into(),
                    payload.attributes.clone().deep_into(),
                )
            }
        }

        #[pymethods]
        impl $name {
            #[new]
            pub const fn new(node_indices: Vec<PyNodeIndex>, attributes: PyAttributes) -> Self {
                Self {
                    node_indices,
                    attributes,
                }
            }

            #[getter]
            pub fn node_indices(&self) -> Vec<PyNodeIndex> {
                self.node_indices.clone()
            }

            #[getter]
            pub fn attributes(&self) -> PyAttributes {
                self.attributes.clone()
            }
        }
    };
}

macro_rules! implement_edge_attributes_payload {
    ($name:ident, $core:ident) => {
        #[pyclass(frozen, module = "graphrecords._graphrecords.plugins")]
        pub struct $name {
            edge_indices: Vec<PyEdgeIndex>,
            attributes: PyAttributes,
        }

        impl From<&$core> for $name {
            fn from(change: &$core) -> Self {
                Self {
                    edge_indices: change.edge_indices().deep_into(),
                    attributes: change.attributes().clone().deep_into(),
                }
            }
        }

        impl From<&$name> for $core {
            fn from(payload: &$name) -> Self {
                Self::new(
                    payload.edge_indices.as_slice().deep_into(),
                    payload.attributes.clone().deep_into(),
                )
            }
        }

        #[pymethods]
        impl $name {
            #[new]
            pub const fn new(edge_indices: Vec<PyEdgeIndex>, attributes: PyAttributes) -> Self {
                Self {
                    edge_indices,
                    attributes,
                }
            }

            #[getter]
            pub fn edge_indices(&self) -> Vec<PyEdgeIndex> {
                self.edge_indices.clone()
            }

            #[getter]
            pub fn attributes(&self) -> PyAttributes {
                self.attributes.clone()
            }
        }
    };
}

macro_rules! implement_node_membership_payload {
    ($name:ident, $core:ident) => {
        #[pyclass(frozen, module = "graphrecords._graphrecords.plugins")]
        pub struct $name {
            node_indices: Vec<PyNodeIndex>,
            group_index: PyGroupIndex,
        }

        impl From<&$core> for $name {
            fn from(change: &$core) -> Self {
                Self {
                    node_indices: change.node_indices().deep_into(),
                    group_index: change.group_index().clone().into(),
                }
            }
        }

        impl From<&$name> for $core {
            fn from(payload: &$name) -> Self {
                Self::new(
                    payload.node_indices.as_slice().deep_into(),
                    payload.group_index.clone().into(),
                )
            }
        }

        #[pymethods]
        impl $name {
            #[new]
            pub const fn new(node_indices: Vec<PyNodeIndex>, group_index: PyGroupIndex) -> Self {
                Self {
                    node_indices,
                    group_index,
                }
            }

            #[getter]
            pub fn node_indices(&self) -> Vec<PyNodeIndex> {
                self.node_indices.clone()
            }

            #[getter]
            pub fn group_index(&self) -> PyGroupIndex {
                self.group_index.clone()
            }
        }
    };
}

macro_rules! implement_edge_membership_payload {
    ($name:ident, $core:ident) => {
        #[pyclass(frozen, module = "graphrecords._graphrecords.plugins")]
        pub struct $name {
            edge_indices: Vec<PyEdgeIndex>,
            group_index: PyGroupIndex,
        }

        impl From<&$core> for $name {
            fn from(change: &$core) -> Self {
                Self {
                    edge_indices: change.edge_indices().deep_into(),
                    group_index: change.group_index().clone().into(),
                }
            }
        }

        impl From<&$name> for $core {
            fn from(payload: &$name) -> Self {
                Self::new(
                    payload.edge_indices.as_slice().deep_into(),
                    payload.group_index.clone().into(),
                )
            }
        }

        #[pymethods]
        impl $name {
            #[new]
            pub const fn new(edge_indices: Vec<PyEdgeIndex>, group_index: PyGroupIndex) -> Self {
                Self {
                    edge_indices,
                    group_index,
                }
            }

            #[getter]
            pub fn edge_indices(&self) -> Vec<PyEdgeIndex> {
                self.edge_indices.clone()
            }

            #[getter]
            pub fn group_index(&self) -> PyGroupIndex {
                self.group_index.clone()
            }
        }
    };
}

macro_rules! implement_empty_payload {
    ($name:ident, $core:ident) => {
        #[pyclass(frozen, module = "graphrecords._graphrecords.plugins")]
        pub struct $name;

        impl From<&$core> for $name {
            fn from(_change: &$core) -> Self {
                Self
            }
        }

        impl From<&$name> for $core {
            fn from(_payload: &$name) -> Self {
                Self::new()
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        #[pymethods]
        impl $name {
            #[new]
            pub const fn new() -> Self {
                Self
            }
        }
    };
}

implement_batch_payload!(PyAddNodes, AddNodes, batch, PyNodeBatch);
implement_batch_payload!(PyAddEdges, AddEdges, batch, PyEdgeBatch);
implement_grouped_batch_payload!(PyAddNodesInGroup, AddNodesInGroup, batch, PyNodeBatch);
implement_grouped_batch_payload!(PyAddEdgesInGroup, AddEdgesInGroup, batch, PyEdgeBatch);
implement_node_indices_payload!(PyRemoveNodes, RemoveNodes);
implement_edge_indices_payload!(PyRemoveEdges, RemoveEdges);
implement_node_attributes_payload!(PySetNodeAttributes, SetNodeAttributes);
implement_node_attributes_payload!(PyReplaceNodeAttributes, ReplaceNodeAttributes);
implement_edge_attributes_payload!(PySetEdgeAttributes, SetEdgeAttributes);
implement_edge_attributes_payload!(PyReplaceEdgeAttributes, ReplaceEdgeAttributes);
implement_node_membership_payload!(PyAddNodesToGroup, AddNodesToGroup);
implement_node_membership_payload!(PyRemoveNodesFromGroup, RemoveNodesFromGroup);
implement_edge_membership_payload!(PyAddEdgesToGroup, AddEdgesToGroup);
implement_edge_membership_payload!(PyRemoveEdgesFromGroup, RemoveEdgesFromGroup);
implement_empty_payload!(PyFreezeSchema, FreezeSchema);
implement_empty_payload!(PyUnfreezeSchema, UnfreezeSchema);
implement_empty_payload!(PyClear, Clear);

#[pyclass(frozen, module = "graphrecords._graphrecords.plugins")]
pub struct PyRemoveNodeAttributes {
    node_indices: Vec<PyNodeIndex>,
    attribute_names: Vec<PyAttributeName>,
}

impl From<&RemoveNodeAttributes> for PyRemoveNodeAttributes {
    fn from(change: &RemoveNodeAttributes) -> Self {
        Self {
            node_indices: change.node_indices().deep_into(),
            attribute_names: change.attribute_names().deep_into(),
        }
    }
}

impl From<&PyRemoveNodeAttributes> for RemoveNodeAttributes {
    fn from(payload: &PyRemoveNodeAttributes) -> Self {
        Self::new(
            payload.node_indices.as_slice().deep_into(),
            payload.attribute_names.as_slice().deep_into(),
        )
    }
}

#[pymethods]
impl PyRemoveNodeAttributes {
    #[new]
    pub const fn new(
        node_indices: Vec<PyNodeIndex>,
        attribute_names: Vec<PyAttributeName>,
    ) -> Self {
        Self {
            node_indices,
            attribute_names,
        }
    }

    #[getter]
    pub fn node_indices(&self) -> Vec<PyNodeIndex> {
        self.node_indices.clone()
    }

    #[getter]
    pub fn attribute_names(&self) -> Vec<PyAttributeName> {
        self.attribute_names.clone()
    }
}

#[pyclass(frozen, module = "graphrecords._graphrecords.plugins")]
pub struct PyRemoveEdgeAttributes {
    edge_indices: Vec<PyEdgeIndex>,
    attribute_names: Vec<PyAttributeName>,
}

impl From<&RemoveEdgeAttributes> for PyRemoveEdgeAttributes {
    fn from(change: &RemoveEdgeAttributes) -> Self {
        Self {
            edge_indices: change.edge_indices().deep_into(),
            attribute_names: change.attribute_names().deep_into(),
        }
    }
}

impl From<&PyRemoveEdgeAttributes> for RemoveEdgeAttributes {
    fn from(payload: &PyRemoveEdgeAttributes) -> Self {
        Self::new(
            payload.edge_indices.as_slice().deep_into(),
            payload.attribute_names.as_slice().deep_into(),
        )
    }
}

#[pymethods]
impl PyRemoveEdgeAttributes {
    #[new]
    pub const fn new(
        edge_indices: Vec<PyEdgeIndex>,
        attribute_names: Vec<PyAttributeName>,
    ) -> Self {
        Self {
            edge_indices,
            attribute_names,
        }
    }

    #[getter]
    pub fn edge_indices(&self) -> Vec<PyEdgeIndex> {
        self.edge_indices.clone()
    }

    #[getter]
    pub fn attribute_names(&self) -> Vec<PyAttributeName> {
        self.attribute_names.clone()
    }
}

#[pyclass(frozen, module = "graphrecords._graphrecords.plugins")]
#[repr(transparent)]
pub struct PyAddGroup(PyGroupIndex);

impl From<&AddGroup> for PyAddGroup {
    fn from(change: &AddGroup) -> Self {
        Self(change.group_index().clone().into())
    }
}

impl From<&PyAddGroup> for AddGroup {
    fn from(payload: &PyAddGroup) -> Self {
        Self::new(payload.0.clone().into())
    }
}

#[pymethods]
impl PyAddGroup {
    #[new]
    pub const fn new(group_index: PyGroupIndex) -> Self {
        Self(group_index)
    }

    #[getter]
    pub fn group_index(&self) -> PyGroupIndex {
        self.0.clone()
    }
}

#[pyclass(frozen, module = "graphrecords._graphrecords.plugins")]
#[repr(transparent)]
pub struct PyRemoveGroups(Vec<PyGroupIndex>);

impl From<&RemoveGroups> for PyRemoveGroups {
    fn from(change: &RemoveGroups) -> Self {
        Self(change.group_indices().deep_into())
    }
}

impl From<&PyRemoveGroups> for RemoveGroups {
    fn from(payload: &PyRemoveGroups) -> Self {
        Self::new(payload.0.as_slice().deep_into())
    }
}

#[pymethods]
impl PyRemoveGroups {
    #[new]
    pub const fn new(group_indices: Vec<PyGroupIndex>) -> Self {
        Self(group_indices)
    }

    #[getter]
    pub fn group_indices(&self) -> Vec<PyGroupIndex> {
        self.0.clone()
    }
}

#[pyclass(frozen, module = "graphrecords._graphrecords.plugins")]
#[repr(transparent)]
pub struct PySetSchema(PySchema);

impl From<&SetSchema> for PySetSchema {
    fn from(change: &SetSchema) -> Self {
        Self(change.schema().clone().into())
    }
}

impl From<&PySetSchema> for SetSchema {
    fn from(payload: &PySetSchema) -> Self {
        Self::new(payload.0.clone().into())
    }
}

#[pymethods]
impl PySetSchema {
    #[new]
    pub fn new(schema: &PySchema) -> Self {
        Self(schema.clone())
    }

    #[getter]
    pub fn schema(&self) -> PySchema {
        self.0.clone()
    }
}

macro_rules! implement_change_conversion {
    ($($payload:ident => $core:ident),+ $(,)?) => {
        fn push_change(changes: &mut Changes, returned: &Bound<'_, PyAny>) -> GraphRecordResult<()> {
            $(
                if let Ok(payload) = returned.cast::<$payload>() {
                    changes.push($core::from(payload.get()));

                    return Ok(());
                }
            )+

            Err(Self::failure(PyTypeError::new_err(
                "Plugin hooks must return a change, a list of changes, or None",
            )))
        }
    };
}

#[repr(transparent)]
pub struct PyPlugin(Py<PyAny>);

impl PyPlugin {
    implement_change_conversion!(
        PyAddNodes => AddNodes,
        PyAddNodesInGroup => AddNodesInGroup,
        PyAddEdges => AddEdges,
        PyAddEdgesInGroup => AddEdgesInGroup,
        PyRemoveNodes => RemoveNodes,
        PyRemoveEdges => RemoveEdges,
        PySetNodeAttributes => SetNodeAttributes,
        PyReplaceNodeAttributes => ReplaceNodeAttributes,
        PyRemoveNodeAttributes => RemoveNodeAttributes,
        PySetEdgeAttributes => SetEdgeAttributes,
        PyReplaceEdgeAttributes => ReplaceEdgeAttributes,
        PyRemoveEdgeAttributes => RemoveEdgeAttributes,
        PyAddGroup => AddGroup,
        PyRemoveGroups => RemoveGroups,
        PyAddNodesToGroup => AddNodesToGroup,
        PyRemoveNodesFromGroup => RemoveNodesFromGroup,
        PyAddEdgesToGroup => AddEdgesToGroup,
        PyRemoveEdgesFromGroup => RemoveEdgesFromGroup,
        PySetSchema => SetSchema,
        PyFreezeSchema => FreezeSchema,
        PyUnfreezeSchema => UnfreezeSchema,
        PyClear => Clear,
    );

    pub const fn new(plugin: Py<PyAny>) -> Self {
        Self(plugin)
    }

    pub const fn plugin(&self) -> &Py<PyAny> {
        &self.0
    }

    fn failure(error: PyErr) -> GraphRecordError {
        GraphRecordError::PluginFailure {
            cause: Arc::new(error),
        }
    }

    fn method<'py>(
        &self,
        py: Python<'py>,
        name: &str,
    ) -> GraphRecordResult<Option<Bound<'py, PyAny>>> {
        match self.0.bind(py).getattr(name) {
            Ok(method) => Ok(Some(method)),
            Err(error) if error.is_instance_of::<PyAttributeError>(py) => Ok(None),
            Err(error) => Err(Self::failure(error)),
        }
    }

    fn observed(returned: &Bound<'_, PyAny>) -> GraphRecordResult<()> {
        if returned.is_none() {
            return Ok(());
        }

        Err(Self::failure(PyTypeError::new_err(
            "Plugin observer hooks must return None",
        )))
    }

    fn changes(returned: &Bound<'_, PyAny>) -> GraphRecordResult<Option<Changes>> {
        if returned.is_none() {
            return Ok(None);
        }

        let mut changes = Changes::new();

        if let Ok(elements) = returned.cast::<PyList>() {
            for element in elements {
                Self::push_change(&mut changes, &element)?;
            }
        } else {
            Self::push_change(&mut changes, returned)?;
        }

        Ok(Some(changes))
    }

    fn dispatch<F>(
        &self,
        name: &str,
        record: &GraphRecord,
        payload: F,
    ) -> GraphRecordResult<Option<Changes>>
    where
        F: for<'py> FnOnce(Python<'py>) -> PyResult<Bound<'py, PyAny>>,
    {
        Python::attach(|py| {
            let Some(method) = self.method(py, name)? else {
                return Ok(None);
            };

            let payload = payload(py).map_err(Self::failure)?;

            method
                .call1((PyGraphRecord::from(record.clone()), payload))
                .map_err(Self::failure)
                .and_then(|returned| Self::changes(&returned))
        })
    }

    fn observe(
        &self,
        name: &str,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        Python::attach(|py| {
            let Some(method) = self.method(py, name)? else {
                return Ok(());
            };

            method
                .call1((
                    PyGraphRecord::from(previous.clone()),
                    PyGraphRecord::from(candidate.clone()),
                ))
                .map_err(Self::failure)
                .and_then(|returned| Self::observed(&returned))
        })
    }

    fn announce(&self, name: &str, record: &GraphRecord) -> GraphRecordResult<Option<Changes>> {
        Python::attach(|py| {
            let Some(method) = self.method(py, name)? else {
                return Ok(None);
            };

            method
                .call1((PyGraphRecord::from(record.clone()),))
                .map_err(Self::failure)
                .and_then(|returned| Self::changes(&returned))
        })
    }
}

macro_rules! implement_change_hook {
    ($method:ident, $parameter:ident, $core:ident, $payload:ident) => {
        fn $method(&self, record: &GraphRecord, $parameter: $core) -> GraphRecordResult<Changes> {
            let changes = self.dispatch(stringify!($method), record, |py| {
                $payload::from(&$parameter).into_bound_py_any(py)
            })?;

            Ok(changes.unwrap_or_else(|| $parameter.into()))
        }
    };
}

macro_rules! implement_observer_hook {
    ($method:ident) => {
        fn $method(
            &self,
            previous: &GraphRecord,
            candidate: &GraphRecord,
        ) -> GraphRecordResult<()> {
            self.observe(stringify!($method), previous, candidate)
        }
    };
}

impl Plugin for PyPlugin {
    implement_change_hook!(on_add_nodes, addition, AddNodes, PyAddNodes);

    implement_observer_hook!(post_add_nodes);

    implement_change_hook!(
        on_add_nodes_in_group,
        addition,
        AddNodesInGroup,
        PyAddNodesInGroup
    );

    implement_observer_hook!(post_add_nodes_in_group);

    implement_change_hook!(on_add_edges, addition, AddEdges, PyAddEdges);

    implement_observer_hook!(post_add_edges);

    implement_change_hook!(
        on_add_edges_in_group,
        addition,
        AddEdgesInGroup,
        PyAddEdgesInGroup
    );

    implement_observer_hook!(post_add_edges_in_group);

    implement_change_hook!(on_remove_nodes, removal, RemoveNodes, PyRemoveNodes);

    implement_observer_hook!(post_remove_nodes);

    implement_change_hook!(on_remove_edges, removal, RemoveEdges, PyRemoveEdges);

    implement_observer_hook!(post_remove_edges);

    implement_change_hook!(
        on_set_node_attributes,
        assignment,
        SetNodeAttributes,
        PySetNodeAttributes
    );

    implement_observer_hook!(post_set_node_attributes);

    implement_change_hook!(
        on_replace_node_attributes,
        assignment,
        ReplaceNodeAttributes,
        PyReplaceNodeAttributes
    );

    implement_observer_hook!(post_replace_node_attributes);

    implement_change_hook!(
        on_remove_node_attributes,
        removal,
        RemoveNodeAttributes,
        PyRemoveNodeAttributes
    );

    implement_observer_hook!(post_remove_node_attributes);

    implement_change_hook!(
        on_set_edge_attributes,
        assignment,
        SetEdgeAttributes,
        PySetEdgeAttributes
    );

    implement_observer_hook!(post_set_edge_attributes);

    implement_change_hook!(
        on_replace_edge_attributes,
        assignment,
        ReplaceEdgeAttributes,
        PyReplaceEdgeAttributes
    );

    implement_observer_hook!(post_replace_edge_attributes);

    implement_change_hook!(
        on_remove_edge_attributes,
        removal,
        RemoveEdgeAttributes,
        PyRemoveEdgeAttributes
    );

    implement_observer_hook!(post_remove_edge_attributes);

    implement_change_hook!(on_add_group, addition, AddGroup, PyAddGroup);

    implement_observer_hook!(post_add_group);

    implement_change_hook!(on_remove_groups, removal, RemoveGroups, PyRemoveGroups);

    implement_observer_hook!(post_remove_groups);

    implement_change_hook!(
        on_add_nodes_to_group,
        membership,
        AddNodesToGroup,
        PyAddNodesToGroup
    );

    implement_observer_hook!(post_add_nodes_to_group);

    implement_change_hook!(
        on_remove_nodes_from_group,
        membership,
        RemoveNodesFromGroup,
        PyRemoveNodesFromGroup
    );

    implement_observer_hook!(post_remove_nodes_from_group);

    implement_change_hook!(
        on_add_edges_to_group,
        membership,
        AddEdgesToGroup,
        PyAddEdgesToGroup
    );

    implement_observer_hook!(post_add_edges_to_group);

    implement_change_hook!(
        on_remove_edges_from_group,
        membership,
        RemoveEdgesFromGroup,
        PyRemoveEdgesFromGroup
    );

    implement_observer_hook!(post_remove_edges_from_group);

    implement_change_hook!(on_set_schema, schema_change, SetSchema, PySetSchema);

    implement_observer_hook!(post_set_schema);

    implement_change_hook!(
        on_freeze_schema,
        schema_change,
        FreezeSchema,
        PyFreezeSchema
    );

    implement_observer_hook!(post_freeze_schema);

    implement_change_hook!(
        on_unfreeze_schema,
        schema_change,
        UnfreezeSchema,
        PyUnfreezeSchema
    );

    implement_observer_hook!(post_unfreeze_schema);

    implement_change_hook!(on_clear, clearing, Clear, PyClear);

    implement_observer_hook!(post_clear);

    fn initialize(&self, record: &GraphRecord) -> GraphRecordResult<Changes> {
        Ok(self.announce("initialize", record)?.unwrap_or_default())
    }

    fn finalize(&self, record: &GraphRecord) -> GraphRecordResult<Changes> {
        Ok(self.announce("finalize", record)?.unwrap_or_default())
    }
}
