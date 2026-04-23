use crate::{
    graphrecord::{
        handle::{PyGroupHandle, PyNodeHandle},
        traits::DeepInto,
    },
    prelude::{PyAttributes, PyGraphRecord, PyGroup, PyNodeIndex, PySchema},
};
use graphrecords_core::{
    errors::{GraphRecordError, GraphRecordResult},
    graphrecord::{
        EdgeDataFrameInput, EdgeIndex, GraphRecord, NodeDataFrameInput,
        plugins::{
            Plugin, PostAddEdgeContext, PostAddEdgeToGroupContext, PostAddEdgeToGroupsContext,
            PostAddEdgeWithGroupContext, PostAddEdgeWithGroupsContext, PostAddEdgesContext,
            PostAddEdgesDataframesContext, PostAddEdgesDataframesWithGroupContext,
            PostAddEdgesDataframesWithGroupsContext, PostAddEdgesToGroupsContext,
            PostAddEdgesWithGroupContext, PostAddEdgesWithGroupsContext, PostAddGroupContext,
            PostAddNodeContext, PostAddNodeToGroupContext, PostAddNodeToGroupsContext,
            PostAddNodeWithGroupContext, PostAddNodeWithGroupsContext, PostAddNodesContext,
            PostAddNodesDataframesContext, PostAddNodesDataframesWithGroupContext,
            PostAddNodesDataframesWithGroupsContext, PostAddNodesToGroupsContext,
            PostAddNodesWithGroupContext, PostAddNodesWithGroupsContext, PostRemoveEdgeContext,
            PostRemoveEdgeFromGroupContext, PostRemoveEdgeFromGroupsContext,
            PostRemoveEdgesFromGroupsContext, PostRemoveGroupContext, PostRemoveNodeContext,
            PostRemoveNodeFromGroupContext, PostRemoveNodeFromGroupsContext,
            PostRemoveNodesFromGroupsContext, PreAddEdgeContext, PreAddEdgeToGroupContext,
            PreAddEdgeToGroupsContext, PreAddEdgeWithGroupContext, PreAddEdgeWithGroupsContext,
            PreAddEdgesContext, PreAddEdgesDataframesContext,
            PreAddEdgesDataframesWithGroupContext, PreAddEdgesDataframesWithGroupsContext,
            PreAddEdgesToGroupsContext, PreAddEdgesWithGroupContext, PreAddEdgesWithGroupsContext,
            PreAddGroupContext, PreAddNodeContext, PreAddNodeToGroupContext,
            PreAddNodeToGroupsContext, PreAddNodeWithGroupContext, PreAddNodeWithGroupsContext,
            PreAddNodesContext, PreAddNodesDataframesContext,
            PreAddNodesDataframesWithGroupContext, PreAddNodesDataframesWithGroupsContext,
            PreAddNodesToGroupsContext, PreAddNodesWithGroupContext, PreAddNodesWithGroupsContext,
            PreRemoveEdgeContext, PreRemoveEdgeFromGroupContext, PreRemoveEdgeFromGroupsContext,
            PreRemoveEdgesFromGroupsContext, PreRemoveGroupContext, PreRemoveNodeContext,
            PreRemoveNodeFromGroupContext, PreRemoveNodeFromGroupsContext,
            PreRemoveNodesFromGroupsContext, PreSetSchemaContext,
        },
    },
};
use pyo3::{IntoPyObjectExt, Py, PyAny, Python, pyclass, pymethods, types::PyAnyMethods};
use pyo3_polars::PyDataFrame;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

macro_rules! impl_pre_hook {
    ($method:ident, $py_context_type:ident, $core_context_type:ident) => {
        fn $method(
            &self,
            graphrecord: &mut GraphRecord,
            context: $core_context_type,
        ) -> GraphRecordResult<$core_context_type> {
            Python::attach(|py| {
                PyGraphRecord::scope_mut(py, graphrecord, |py, graphrecord| {
                    let py_context = $py_context_type::bind(py, context);

                    let result = self
                        .0
                        .call_method1(py, stringify!($method), (graphrecord, py_context))
                        .map_err(|err| GraphRecordError::ConversionError(format!("{}", err)))?;

                    Ok(result
                        .extract::<$py_context_type>(py)
                        .map_err(|err| GraphRecordError::ConversionError(format!("{}", err)))?
                        .extract(py))
                })
            })
        }
    };
}

macro_rules! impl_post_hook {
    ($method:ident) => {
        fn $method(&self, graphrecord: &mut GraphRecord) -> GraphRecordResult<()> {
            Python::attach(|py| {
                PyGraphRecord::scope_mut(py, graphrecord, |py, graphrecord| {
                    self.0
                        .call_method1(py, stringify!($method), (graphrecord,))
                        .map_err(|err| GraphRecordError::ConversionError(format!("{}", err)))?;

                    Ok(())
                })
            })
        }
    };
    ($method:ident, $py_context_type:ident, $core_context_type:ident) => {
        fn $method(
            &self,
            graphrecord: &mut GraphRecord,
            context: $core_context_type,
        ) -> GraphRecordResult<()> {
            Python::attach(|py| {
                PyGraphRecord::scope_mut(py, graphrecord, |py, graphrecord| {
                    let py_context = $py_context_type::bind(py, context);

                    self.0
                        .call_method1(py, stringify!($method), (graphrecord, py_context))
                        .map_err(|err| GraphRecordError::ConversionError(format!("{}", err)))?;

                    Ok(())
                })
            })
        }
    };
}

#[derive(Debug)]
pub struct PyPlugin(Py<PyAny>);

impl Serialize for PyPlugin {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        Python::attach(|py| {
            let cloudpickle = py
                .import("cloudpickle")
                .map_err(serde::ser::Error::custom)?;

            let bytes: Vec<u8> = cloudpickle
                .call_method1("dumps", (&self.0,))
                .map_err(serde::ser::Error::custom)?
                .extract()
                .map_err(serde::ser::Error::custom)?;

            serializer.serialize_bytes(&bytes)
        })
    }
}

impl<'de> Deserialize<'de> for PyPlugin {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes: Vec<u8> = Deserialize::deserialize(deserializer)?;

        Python::attach(|py| {
            let cloudpickle = py.import("cloudpickle").map_err(serde::de::Error::custom)?;

            let obj: Py<PyAny> = cloudpickle
                .call_method1("loads", (bytes.as_slice(),))
                .map_err(serde::de::Error::custom)?
                .into();

            Ok(Self(obj))
        })
    }
}

impl PyPlugin {
    pub const fn new(py_obj: Py<PyAny>) -> Self {
        Self(py_obj)
    }
}

fn node_dataframe_inputs_to_py(inputs: Vec<NodeDataFrameInput>) -> Vec<(PyDataFrame, String)> {
    inputs
        .into_iter()
        .map(|input| (PyDataFrame(input.dataframe), input.index_column))
        .collect()
}

fn py_to_node_dataframe_inputs(inputs: Vec<(PyDataFrame, String)>) -> Vec<NodeDataFrameInput> {
    inputs
        .into_iter()
        .map(|(dataframe, index_column)| NodeDataFrameInput {
            dataframe: dataframe.0,
            index_column,
        })
        .collect()
}

fn edge_dataframe_inputs_to_py(
    inputs: Vec<EdgeDataFrameInput>,
) -> Vec<(PyDataFrame, String, String)> {
    inputs
        .into_iter()
        .map(|input| {
            (
                PyDataFrame(input.dataframe),
                input.source_index_column,
                input.target_index_column,
            )
        })
        .collect()
}

fn py_to_edge_dataframe_inputs(
    inputs: Vec<(PyDataFrame, String, String)>,
) -> Vec<EdgeDataFrameInput> {
    inputs
        .into_iter()
        .map(
            |(dataframe, source_index_column, target_index_column)| EdgeDataFrameInput {
                dataframe: dataframe.0,
                source_index_column,
                target_index_column,
            },
        )
        .collect()
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPreSetSchemaContext {
    schema: Py<PySchema>,
}

impl Clone for PyPreSetSchemaContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            schema: self.schema.clone_ref(py),
        })
    }
}

impl PyPreSetSchemaContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PreSetSchemaContext) -> Self {
        Self {
            schema: Py::new(py, PySchema::from(context.schema))
                .expect("PySchema should be creatable"),
        }
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn extract(self, py: Python<'_>) -> PreSetSchemaContext {
        let py_schema: PySchema = self
            .schema
            .extract(py)
            .expect("PySchema should be extractable");

        PreSetSchemaContext {
            schema: py_schema.into(),
        }
    }
}

#[pymethods]
impl PyPreSetSchemaContext {
    #[new]
    pub const fn new(schema: Py<PySchema>) -> Self {
        Self { schema }
    }

    #[getter]
    pub fn schema(&self, py: Python<'_>) -> Py<PySchema> {
        self.schema.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPreAddNodeContext {
    node_index: Py<PyAny>,
    attributes: Py<PyAny>,
}

impl Clone for PyPreAddNodeContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            node_index: self.node_index.clone_ref(py),
            attributes: self.attributes.clone_ref(py),
        })
    }
}

impl PyPreAddNodeContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PreAddNodeContext) -> Self {
        Self {
            node_index: PyNodeIndex::from(context.node_index)
                .into_py_any(py)
                .expect("PyNodeIndex should be creatable"),
            attributes: {
                let py_attrs: PyAttributes = context.attributes.deep_into();
                py_attrs
                    .into_py_any(py)
                    .expect("PyAttributes should be creatable")
            },
        }
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn extract(self, py: Python<'_>) -> PreAddNodeContext {
        let node_index: PyNodeIndex = self
            .node_index
            .extract(py)
            .expect("PyNodeIndex should be extractable");

        let attributes: PyAttributes = self
            .attributes
            .extract(py)
            .expect("PyAttributes should be extractable");

        PreAddNodeContext {
            node_index: node_index.into(),
            attributes: attributes.deep_into(),
        }
    }
}

#[pymethods]
impl PyPreAddNodeContext {
    #[new]
    pub const fn new(node_index: Py<PyAny>, attributes: Py<PyAny>) -> Self {
        Self {
            node_index,
            attributes,
        }
    }

    #[getter]
    pub fn node_index(&self, py: Python<'_>) -> Py<PyAny> {
        self.node_index.clone_ref(py)
    }

    #[getter]
    pub fn attributes(&self, py: Python<'_>) -> Py<PyAny> {
        self.attributes.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPostAddNodeContext {
    node_index: Py<PyAny>,
}

impl Clone for PyPostAddNodeContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            node_index: self.node_index.clone_ref(py),
        })
    }
}

impl PyPostAddNodeContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PostAddNodeContext) -> Self {
        Self {
            node_index: PyNodeIndex::from(context.node_index)
                .into_py_any(py)
                .expect("PyNodeIndex should be creatable"),
        }
    }
}

#[pymethods]
impl PyPostAddNodeContext {
    #[new]
    pub const fn new(node_index: Py<PyAny>) -> Self {
        Self { node_index }
    }

    #[getter]
    pub fn node_index(&self, py: Python<'_>) -> Py<PyAny> {
        self.node_index.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPreAddNodeWithGroupContext {
    node_index: Py<PyAny>,
    attributes: Py<PyAny>,
    group_handle: Py<PyAny>,
}

impl Clone for PyPreAddNodeWithGroupContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            node_index: self.node_index.clone_ref(py),
            attributes: self.attributes.clone_ref(py),
            group_handle: self.group_handle.clone_ref(py),
        })
    }
}

impl PyPreAddNodeWithGroupContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PreAddNodeWithGroupContext) -> Self {
        Self {
            node_index: PyNodeIndex::from(context.node_index)
                .into_py_any(py)
                .expect("PyNodeIndex should be creatable"),
            attributes: {
                let py_attrs: PyAttributes = context.attributes.deep_into();
                py_attrs
                    .into_py_any(py)
                    .expect("PyAttributes should be creatable")
            },
            group_handle: PyGroupHandle::from(context.group_handle)
                .into_py_any(py)
                .expect("PyGroup should be creatable"),
        }
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn extract(self, py: Python<'_>) -> PreAddNodeWithGroupContext {
        let node_index: PyNodeIndex = self
            .node_index
            .extract(py)
            .expect("PyNodeIndex should be extractable");

        let attributes: PyAttributes = self
            .attributes
            .extract(py)
            .expect("PyAttributes should be extractable");

        let group_handle: PyGroupHandle = self
            .group_handle
            .extract(py)
            .expect("PyGroup should be extractable");

        PreAddNodeWithGroupContext {
            node_index: node_index.into(),
            attributes: attributes.deep_into(),
            group_handle: group_handle.into(),
        }
    }
}

#[pymethods]
impl PyPreAddNodeWithGroupContext {
    #[new]
    pub const fn new(
        node_index: Py<PyAny>,
        attributes: Py<PyAny>,
        group_handle: Py<PyAny>,
    ) -> Self {
        Self {
            node_index,
            attributes,
            group_handle,
        }
    }

    #[getter]
    pub fn node_index(&self, py: Python<'_>) -> Py<PyAny> {
        self.node_index.clone_ref(py)
    }

    #[getter]
    pub fn attributes(&self, py: Python<'_>) -> Py<PyAny> {
        self.attributes.clone_ref(py)
    }

    #[getter]
    pub fn group_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handle.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPostAddNodeWithGroupContext {
    node_index: Py<PyAny>,
    group_handle: Py<PyAny>,
}

impl Clone for PyPostAddNodeWithGroupContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            node_index: self.node_index.clone_ref(py),
            group_handle: self.group_handle.clone_ref(py),
        })
    }
}

impl PyPostAddNodeWithGroupContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PostAddNodeWithGroupContext) -> Self {
        Self {
            node_index: PyNodeIndex::from(context.node_index)
                .into_py_any(py)
                .expect("PyNodeIndex should be creatable"),
            group_handle: PyGroupHandle::from(context.group_handle)
                .into_py_any(py)
                .expect("PyGroup should be creatable"),
        }
    }
}

#[pymethods]
impl PyPostAddNodeWithGroupContext {
    #[new]
    pub const fn new(node_index: Py<PyAny>, group_handle: Py<PyAny>) -> Self {
        Self {
            node_index,
            group_handle,
        }
    }

    #[getter]
    pub fn node_index(&self, py: Python<'_>) -> Py<PyAny> {
        self.node_index.clone_ref(py)
    }

    #[getter]
    pub fn group_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handle.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPreAddNodeWithGroupsContext {
    node_index: Py<PyAny>,
    attributes: Py<PyAny>,
    group_handles: Py<PyAny>,
}

impl Clone for PyPreAddNodeWithGroupsContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            node_index: self.node_index.clone_ref(py),
            attributes: self.attributes.clone_ref(py),
            group_handles: self.group_handles.clone_ref(py),
        })
    }
}

impl PyPreAddNodeWithGroupsContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PreAddNodeWithGroupsContext) -> Self {
        let group_handles: Vec<PyGroupHandle> = context.group_handles.deep_into();

        Self {
            node_index: PyNodeIndex::from(context.node_index)
                .into_py_any(py)
                .expect("PyNodeIndex should be creatable"),
            attributes: {
                let py_attrs: PyAttributes = context.attributes.deep_into();
                py_attrs
                    .into_py_any(py)
                    .expect("PyAttributes should be creatable")
            },
            group_handles: group_handles
                .into_py_any(py)
                .expect("groups should be creatable"),
        }
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn extract(self, py: Python<'_>) -> PreAddNodeWithGroupsContext {
        let node_index: PyNodeIndex = self
            .node_index
            .extract(py)
            .expect("PyNodeIndex should be extractable");

        let attributes: PyAttributes = self
            .attributes
            .extract(py)
            .expect("PyAttributes should be extractable");

        let group_handles: Vec<PyGroupHandle> = self
            .group_handles
            .extract(py)
            .expect("groups should be extractable");

        PreAddNodeWithGroupsContext {
            node_index: node_index.into(),
            attributes: attributes.deep_into(),
            group_handles: group_handles.deep_into(),
        }
    }
}

#[pymethods]
impl PyPreAddNodeWithGroupsContext {
    #[new]
    pub const fn new(
        node_index: Py<PyAny>,
        attributes: Py<PyAny>,
        group_handles: Py<PyAny>,
    ) -> Self {
        Self {
            node_index,
            attributes,
            group_handles,
        }
    }

    #[getter]
    pub fn node_index(&self, py: Python<'_>) -> Py<PyAny> {
        self.node_index.clone_ref(py)
    }

    #[getter]
    pub fn attributes(&self, py: Python<'_>) -> Py<PyAny> {
        self.attributes.clone_ref(py)
    }

    #[getter]
    pub fn group_handles(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handles.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPostAddNodeWithGroupsContext {
    node_index: Py<PyAny>,
    group_handles: Py<PyAny>,
}

impl Clone for PyPostAddNodeWithGroupsContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            node_index: self.node_index.clone_ref(py),
            group_handles: self.group_handles.clone_ref(py),
        })
    }
}

impl PyPostAddNodeWithGroupsContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PostAddNodeWithGroupsContext) -> Self {
        let group_handles: Vec<PyGroupHandle> = context.group_handles.deep_into();

        Self {
            node_index: PyNodeIndex::from(context.node_index)
                .into_py_any(py)
                .expect("PyNodeIndex should be creatable"),
            group_handles: group_handles
                .into_py_any(py)
                .expect("groups should be creatable"),
        }
    }
}

#[pymethods]
impl PyPostAddNodeWithGroupsContext {
    #[new]
    pub const fn new(node_index: Py<PyAny>, group_handles: Py<PyAny>) -> Self {
        Self {
            node_index,
            group_handles,
        }
    }

    #[getter]
    pub fn node_index(&self, py: Python<'_>) -> Py<PyAny> {
        self.node_index.clone_ref(py)
    }

    #[getter]
    pub fn group_handles(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handles.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPreRemoveNodeContext {
    node_handle: Py<PyAny>,
}

impl Clone for PyPreRemoveNodeContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            node_handle: self.node_handle.clone_ref(py),
        })
    }
}

impl PyPreRemoveNodeContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PreRemoveNodeContext) -> Self {
        Self {
            node_handle: PyNodeHandle::from(context.node_handle)
                .into_py_any(py)
                .expect("PyNodeIndex should be creatable"),
        }
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn extract(self, py: Python<'_>) -> PreRemoveNodeContext {
        let node_handle: PyNodeHandle = self
            .node_handle
            .extract(py)
            .expect("PyNodeHandle should be extractable");

        PreRemoveNodeContext {
            node_handle: node_handle.into(),
        }
    }
}

#[pymethods]
impl PyPreRemoveNodeContext {
    #[new]
    pub const fn new(node_handle: Py<PyAny>) -> Self {
        Self { node_handle }
    }

    #[getter]
    pub fn node_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.node_handle.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPostRemoveNodeContext {
    node_handle: Py<PyAny>,
}

impl Clone for PyPostRemoveNodeContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            node_handle: self.node_handle.clone_ref(py),
        })
    }
}

impl PyPostRemoveNodeContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PostRemoveNodeContext) -> Self {
        Self {
            node_handle: PyNodeHandle::from(context.node_handle)
                .into_py_any(py)
                .expect("PyNodeIndex should be creatable"),
        }
    }
}

#[pymethods]
impl PyPostRemoveNodeContext {
    #[new]
    pub const fn new(node_handle: Py<PyAny>) -> Self {
        Self { node_handle }
    }

    #[getter]
    pub fn node_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.node_handle.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPreAddNodesContext {
    nodes: Py<PyAny>,
}

impl Clone for PyPreAddNodesContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            nodes: self.nodes.clone_ref(py),
        })
    }
}

impl PyPreAddNodesContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PreAddNodesContext) -> Self {
        let nodes: Vec<(PyNodeIndex, PyAttributes)> = context.nodes.deep_into();

        Self {
            nodes: nodes.into_py_any(py).expect("nodes should be creatable"),
        }
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn extract(self, py: Python<'_>) -> PreAddNodesContext {
        let nodes: Vec<(PyNodeIndex, PyAttributes)> =
            self.nodes.extract(py).expect("nodes should be extractable");

        PreAddNodesContext {
            nodes: nodes.deep_into(),
        }
    }
}

#[pymethods]
impl PyPreAddNodesContext {
    #[new]
    pub const fn new(nodes: Py<PyAny>) -> Self {
        Self { nodes }
    }

    #[getter]
    pub fn nodes(&self, py: Python<'_>) -> Py<PyAny> {
        self.nodes.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPostAddNodesContext {
    nodes: Py<PyAny>,
}

impl Clone for PyPostAddNodesContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            nodes: self.nodes.clone_ref(py),
        })
    }
}

impl PyPostAddNodesContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PostAddNodesContext) -> Self {
        let nodes: Vec<(PyNodeIndex, PyAttributes)> = context.nodes.deep_into();

        Self {
            nodes: nodes.into_py_any(py).expect("nodes should be creatable"),
        }
    }
}

#[pymethods]
impl PyPostAddNodesContext {
    #[new]
    pub const fn new(nodes: Py<PyAny>) -> Self {
        Self { nodes }
    }

    #[getter]
    pub fn nodes(&self, py: Python<'_>) -> Py<PyAny> {
        self.nodes.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPreAddNodesWithGroupContext {
    nodes: Py<PyAny>,
    group_handle: Py<PyAny>,
}

impl Clone for PyPreAddNodesWithGroupContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            nodes: self.nodes.clone_ref(py),
            group_handle: self.group_handle.clone_ref(py),
        })
    }
}

impl PyPreAddNodesWithGroupContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PreAddNodesWithGroupContext) -> Self {
        let nodes: Vec<(PyNodeIndex, PyAttributes)> = context.nodes.deep_into();

        Self {
            nodes: nodes.into_py_any(py).expect("nodes should be creatable"),
            group_handle: PyGroupHandle::from(context.group_handle)
                .into_py_any(py)
                .expect("PyGroup should be creatable"),
        }
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn extract(self, py: Python<'_>) -> PreAddNodesWithGroupContext {
        let nodes: Vec<(PyNodeIndex, PyAttributes)> =
            self.nodes.extract(py).expect("nodes should be extractable");

        let group_handle: PyGroupHandle = self
            .group_handle
            .extract(py)
            .expect("PyGroup should be extractable");

        PreAddNodesWithGroupContext {
            nodes: nodes.deep_into(),
            group_handle: group_handle.into(),
        }
    }
}

#[pymethods]
impl PyPreAddNodesWithGroupContext {
    #[new]
    pub const fn new(nodes: Py<PyAny>, group_handle: Py<PyAny>) -> Self {
        Self {
            nodes,
            group_handle,
        }
    }

    #[getter]
    pub fn nodes(&self, py: Python<'_>) -> Py<PyAny> {
        self.nodes.clone_ref(py)
    }

    #[getter]
    pub fn group_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handle.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPostAddNodesWithGroupContext {
    nodes: Py<PyAny>,
    group_handle: Py<PyAny>,
}

impl Clone for PyPostAddNodesWithGroupContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            nodes: self.nodes.clone_ref(py),
            group_handle: self.group_handle.clone_ref(py),
        })
    }
}

impl PyPostAddNodesWithGroupContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PostAddNodesWithGroupContext) -> Self {
        let nodes: Vec<(PyNodeIndex, PyAttributes)> = context.nodes.deep_into();

        Self {
            nodes: nodes.into_py_any(py).expect("nodes should be creatable"),
            group_handle: PyGroupHandle::from(context.group_handle)
                .into_py_any(py)
                .expect("PyGroup should be creatable"),
        }
    }
}

#[pymethods]
impl PyPostAddNodesWithGroupContext {
    #[new]
    pub const fn new(nodes: Py<PyAny>, group_handle: Py<PyAny>) -> Self {
        Self {
            nodes,
            group_handle,
        }
    }

    #[getter]
    pub fn nodes(&self, py: Python<'_>) -> Py<PyAny> {
        self.nodes.clone_ref(py)
    }

    #[getter]
    pub fn group_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handle.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPreAddNodesWithGroupsContext {
    nodes: Py<PyAny>,
    group_handles: Py<PyAny>,
}

impl Clone for PyPreAddNodesWithGroupsContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            nodes: self.nodes.clone_ref(py),
            group_handles: self.group_handles.clone_ref(py),
        })
    }
}

impl PyPreAddNodesWithGroupsContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PreAddNodesWithGroupsContext) -> Self {
        let nodes: Vec<(PyNodeIndex, PyAttributes)> = context.nodes.deep_into();
        let group_handles: Vec<PyGroupHandle> = context.group_handles.deep_into();

        Self {
            nodes: nodes.into_py_any(py).expect("nodes should be creatable"),
            group_handles: group_handles
                .into_py_any(py)
                .expect("groups should be creatable"),
        }
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn extract(self, py: Python<'_>) -> PreAddNodesWithGroupsContext {
        let nodes: Vec<(PyNodeIndex, PyAttributes)> =
            self.nodes.extract(py).expect("nodes should be extractable");

        let group_handles: Vec<PyGroupHandle> = self
            .group_handles
            .extract(py)
            .expect("groups should be extractable");

        PreAddNodesWithGroupsContext {
            nodes: nodes.deep_into(),
            group_handles: group_handles.deep_into(),
        }
    }
}

#[pymethods]
impl PyPreAddNodesWithGroupsContext {
    #[new]
    pub const fn new(nodes: Py<PyAny>, group_handles: Py<PyAny>) -> Self {
        Self {
            nodes,
            group_handles,
        }
    }

    #[getter]
    pub fn nodes(&self, py: Python<'_>) -> Py<PyAny> {
        self.nodes.clone_ref(py)
    }

    #[getter]
    pub fn group_handles(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handles.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPostAddNodesWithGroupsContext {
    nodes: Py<PyAny>,
    group_handles: Py<PyAny>,
}

impl Clone for PyPostAddNodesWithGroupsContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            nodes: self.nodes.clone_ref(py),
            group_handles: self.group_handles.clone_ref(py),
        })
    }
}

impl PyPostAddNodesWithGroupsContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PostAddNodesWithGroupsContext) -> Self {
        let nodes: Vec<(PyNodeIndex, PyAttributes)> = context.nodes.deep_into();
        let group_handles: Vec<PyGroupHandle> = context.group_handles.deep_into();

        Self {
            nodes: nodes.into_py_any(py).expect("nodes should be creatable"),
            group_handles: group_handles
                .into_py_any(py)
                .expect("groups should be creatable"),
        }
    }
}

#[pymethods]
impl PyPostAddNodesWithGroupsContext {
    #[new]
    pub const fn new(nodes: Py<PyAny>, group_handles: Py<PyAny>) -> Self {
        Self {
            nodes,
            group_handles,
        }
    }

    #[getter]
    pub fn nodes(&self, py: Python<'_>) -> Py<PyAny> {
        self.nodes.clone_ref(py)
    }

    #[getter]
    pub fn group_handles(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handles.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPreAddNodesDataframesContext {
    nodes_dataframes: Py<PyAny>,
}

impl Clone for PyPreAddNodesDataframesContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            nodes_dataframes: self.nodes_dataframes.clone_ref(py),
        })
    }
}

impl PyPreAddNodesDataframesContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PreAddNodesDataframesContext) -> Self {
        let nodes_dataframes = node_dataframe_inputs_to_py(context.nodes_dataframes);

        Self {
            nodes_dataframes: nodes_dataframes
                .into_py_any(py)
                .expect("nodes_dataframes should be creatable"),
        }
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn extract(self, py: Python<'_>) -> PreAddNodesDataframesContext {
        let nodes_dataframes: Vec<(PyDataFrame, String)> = self
            .nodes_dataframes
            .extract(py)
            .expect("nodes_dataframes should be extractable");

        PreAddNodesDataframesContext {
            nodes_dataframes: py_to_node_dataframe_inputs(nodes_dataframes),
        }
    }
}

#[pymethods]
impl PyPreAddNodesDataframesContext {
    #[new]
    pub const fn new(nodes_dataframes: Py<PyAny>) -> Self {
        Self { nodes_dataframes }
    }

    #[getter]
    pub fn nodes_dataframes(&self, py: Python<'_>) -> Py<PyAny> {
        self.nodes_dataframes.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPostAddNodesDataframesContext {
    nodes_dataframes: Py<PyAny>,
}

impl Clone for PyPostAddNodesDataframesContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            nodes_dataframes: self.nodes_dataframes.clone_ref(py),
        })
    }
}

impl PyPostAddNodesDataframesContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PostAddNodesDataframesContext) -> Self {
        let nodes_dataframes = node_dataframe_inputs_to_py(context.nodes_dataframes);

        Self {
            nodes_dataframes: nodes_dataframes
                .into_py_any(py)
                .expect("nodes_dataframes should be creatable"),
        }
    }
}

#[pymethods]
impl PyPostAddNodesDataframesContext {
    #[new]
    pub const fn new(nodes_dataframes: Py<PyAny>) -> Self {
        Self { nodes_dataframes }
    }

    #[getter]
    pub fn nodes_dataframes(&self, py: Python<'_>) -> Py<PyAny> {
        self.nodes_dataframes.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPreAddNodesDataframesWithGroupContext {
    nodes_dataframes: Py<PyAny>,
    group_handle: Py<PyAny>,
}

impl Clone for PyPreAddNodesDataframesWithGroupContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            nodes_dataframes: self.nodes_dataframes.clone_ref(py),
            group_handle: self.group_handle.clone_ref(py),
        })
    }
}

impl PyPreAddNodesDataframesWithGroupContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PreAddNodesDataframesWithGroupContext) -> Self {
        let nodes_dataframes = node_dataframe_inputs_to_py(context.nodes_dataframes);

        Self {
            nodes_dataframes: nodes_dataframes
                .into_py_any(py)
                .expect("nodes_dataframes should be creatable"),
            group_handle: PyGroupHandle::from(context.group_handle)
                .into_py_any(py)
                .expect("PyGroup should be creatable"),
        }
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn extract(self, py: Python<'_>) -> PreAddNodesDataframesWithGroupContext {
        let nodes_dataframes: Vec<(PyDataFrame, String)> = self
            .nodes_dataframes
            .extract(py)
            .expect("nodes_dataframes should be extractable");

        let group_handle: PyGroupHandle = self
            .group_handle
            .extract(py)
            .expect("PyGroup should be extractable");

        PreAddNodesDataframesWithGroupContext {
            nodes_dataframes: py_to_node_dataframe_inputs(nodes_dataframes),
            group_handle: group_handle.into(),
        }
    }
}

#[pymethods]
impl PyPreAddNodesDataframesWithGroupContext {
    #[new]
    pub const fn new(nodes_dataframes: Py<PyAny>, group_handle: Py<PyAny>) -> Self {
        Self {
            nodes_dataframes,
            group_handle,
        }
    }

    #[getter]
    pub fn nodes_dataframes(&self, py: Python<'_>) -> Py<PyAny> {
        self.nodes_dataframes.clone_ref(py)
    }

    #[getter]
    pub fn group_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handle.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPostAddNodesDataframesWithGroupContext {
    nodes_dataframes: Py<PyAny>,
    group_handle: Py<PyAny>,
}

impl Clone for PyPostAddNodesDataframesWithGroupContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            nodes_dataframes: self.nodes_dataframes.clone_ref(py),
            group_handle: self.group_handle.clone_ref(py),
        })
    }
}

impl PyPostAddNodesDataframesWithGroupContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PostAddNodesDataframesWithGroupContext) -> Self {
        let nodes_dataframes = node_dataframe_inputs_to_py(context.nodes_dataframes);

        Self {
            nodes_dataframes: nodes_dataframes
                .into_py_any(py)
                .expect("nodes_dataframes should be creatable"),
            group_handle: PyGroupHandle::from(context.group_handle)
                .into_py_any(py)
                .expect("PyGroup should be creatable"),
        }
    }
}

#[pymethods]
impl PyPostAddNodesDataframesWithGroupContext {
    #[new]
    pub const fn new(nodes_dataframes: Py<PyAny>, group_handle: Py<PyAny>) -> Self {
        Self {
            nodes_dataframes,
            group_handle,
        }
    }

    #[getter]
    pub fn nodes_dataframes(&self, py: Python<'_>) -> Py<PyAny> {
        self.nodes_dataframes.clone_ref(py)
    }

    #[getter]
    pub fn group_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handle.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPreAddNodesDataframesWithGroupsContext {
    nodes_dataframes: Py<PyAny>,
    group_handles: Py<PyAny>,
}

impl Clone for PyPreAddNodesDataframesWithGroupsContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            nodes_dataframes: self.nodes_dataframes.clone_ref(py),
            group_handles: self.group_handles.clone_ref(py),
        })
    }
}

impl PyPreAddNodesDataframesWithGroupsContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PreAddNodesDataframesWithGroupsContext) -> Self {
        let nodes_dataframes = node_dataframe_inputs_to_py(context.nodes_dataframes);
        let group_handles: Vec<PyGroupHandle> = context.group_handles.deep_into();

        Self {
            nodes_dataframes: nodes_dataframes
                .into_py_any(py)
                .expect("nodes_dataframes should be creatable"),
            group_handles: group_handles
                .into_py_any(py)
                .expect("groups should be creatable"),
        }
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn extract(self, py: Python<'_>) -> PreAddNodesDataframesWithGroupsContext {
        let nodes_dataframes: Vec<(PyDataFrame, String)> = self
            .nodes_dataframes
            .extract(py)
            .expect("nodes_dataframes should be extractable");

        let group_handles: Vec<PyGroupHandle> = self
            .group_handles
            .extract(py)
            .expect("groups should be extractable");

        PreAddNodesDataframesWithGroupsContext {
            nodes_dataframes: py_to_node_dataframe_inputs(nodes_dataframes),
            group_handles: group_handles.deep_into(),
        }
    }
}

#[pymethods]
impl PyPreAddNodesDataframesWithGroupsContext {
    #[new]
    pub const fn new(nodes_dataframes: Py<PyAny>, group_handles: Py<PyAny>) -> Self {
        Self {
            nodes_dataframes,
            group_handles,
        }
    }

    #[getter]
    pub fn nodes_dataframes(&self, py: Python<'_>) -> Py<PyAny> {
        self.nodes_dataframes.clone_ref(py)
    }

    #[getter]
    pub fn group_handles(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handles.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPostAddNodesDataframesWithGroupsContext {
    nodes_dataframes: Py<PyAny>,
    group_handles: Py<PyAny>,
}

impl Clone for PyPostAddNodesDataframesWithGroupsContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            nodes_dataframes: self.nodes_dataframes.clone_ref(py),
            group_handles: self.group_handles.clone_ref(py),
        })
    }
}

impl PyPostAddNodesDataframesWithGroupsContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PostAddNodesDataframesWithGroupsContext) -> Self {
        let nodes_dataframes = node_dataframe_inputs_to_py(context.nodes_dataframes);
        let group_handles: Vec<PyGroupHandle> = context.group_handles.deep_into();

        Self {
            nodes_dataframes: nodes_dataframes
                .into_py_any(py)
                .expect("nodes_dataframes should be creatable"),
            group_handles: group_handles
                .into_py_any(py)
                .expect("groups should be creatable"),
        }
    }
}

#[pymethods]
impl PyPostAddNodesDataframesWithGroupsContext {
    #[new]
    pub const fn new(nodes_dataframes: Py<PyAny>, group_handles: Py<PyAny>) -> Self {
        Self {
            nodes_dataframes,
            group_handles,
        }
    }

    #[getter]
    pub fn nodes_dataframes(&self, py: Python<'_>) -> Py<PyAny> {
        self.nodes_dataframes.clone_ref(py)
    }

    #[getter]
    pub fn group_handles(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handles.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPreAddEdgeContext {
    source_node_handle: Py<PyAny>,
    target_node_handle: Py<PyAny>,
    attributes: Py<PyAny>,
}

impl Clone for PyPreAddEdgeContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            source_node_handle: self.source_node_handle.clone_ref(py),
            target_node_handle: self.target_node_handle.clone_ref(py),
            attributes: self.attributes.clone_ref(py),
        })
    }
}

impl PyPreAddEdgeContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PreAddEdgeContext) -> Self {
        Self {
            source_node_handle: PyNodeHandle::from(context.source_node_handle)
                .into_py_any(py)
                .expect("PyNodeHandle should be creatable"),
            target_node_handle: PyNodeHandle::from(context.target_node_handle)
                .into_py_any(py)
                .expect("PyNodeHandle should be creatable"),
            attributes: {
                let py_attrs: PyAttributes = context.attributes.deep_into();
                py_attrs
                    .into_py_any(py)
                    .expect("PyAttributes should be creatable")
            },
        }
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn extract(self, py: Python<'_>) -> PreAddEdgeContext {
        let source: PyNodeHandle = self
            .source_node_handle
            .extract(py)
            .expect("PyNodeHandle should be extractable");

        let target: PyNodeHandle = self
            .target_node_handle
            .extract(py)
            .expect("PyNodeHandle should be extractable");

        let attributes: PyAttributes = self
            .attributes
            .extract(py)
            .expect("PyAttributes should be extractable");

        PreAddEdgeContext {
            source_node_handle: source.into(),
            target_node_handle: target.into(),
            attributes: attributes.deep_into(),
        }
    }
}

#[pymethods]
impl PyPreAddEdgeContext {
    #[new]
    pub const fn new(
        source_node_handle: Py<PyAny>,
        target_node_handle: Py<PyAny>,
        attributes: Py<PyAny>,
    ) -> Self {
        Self {
            source_node_handle,
            target_node_handle,
            attributes,
        }
    }

    #[getter]
    pub fn source_node_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.source_node_handle.clone_ref(py)
    }

    #[getter]
    pub fn target_node_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.target_node_handle.clone_ref(py)
    }

    #[getter]
    pub fn attributes(&self, py: Python<'_>) -> Py<PyAny> {
        self.attributes.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPostAddEdgeContext {
    edge_index: Py<PyAny>,
}

impl Clone for PyPostAddEdgeContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            edge_index: self.edge_index.clone_ref(py),
        })
    }
}

impl PyPostAddEdgeContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PostAddEdgeContext) -> Self {
        Self {
            edge_index: context
                .edge_index
                .into_py_any(py)
                .expect("edge_index should be creatable"),
        }
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn extract(self, py: Python<'_>) -> PostAddEdgeContext {
        PostAddEdgeContext {
            edge_index: self
                .edge_index
                .extract(py)
                .expect("edge_index should be extractable"),
        }
    }
}

#[pymethods]
impl PyPostAddEdgeContext {
    #[new]
    pub const fn new(edge_index: Py<PyAny>) -> Self {
        Self { edge_index }
    }

    #[getter]
    pub fn edge_index(&self, py: Python<'_>) -> Py<PyAny> {
        self.edge_index.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPreAddEdgeWithGroupContext {
    source_node_handle: Py<PyAny>,
    target_node_handle: Py<PyAny>,
    attributes: Py<PyAny>,
    group_handle: Py<PyAny>,
}

impl Clone for PyPreAddEdgeWithGroupContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            source_node_handle: self.source_node_handle.clone_ref(py),
            target_node_handle: self.target_node_handle.clone_ref(py),
            attributes: self.attributes.clone_ref(py),
            group_handle: self.group_handle.clone_ref(py),
        })
    }
}

impl PyPreAddEdgeWithGroupContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PreAddEdgeWithGroupContext) -> Self {
        Self {
            source_node_handle: PyNodeHandle::from(context.source_node_handle)
                .into_py_any(py)
                .expect("PyNodeHandle should be creatable"),
            target_node_handle: PyNodeHandle::from(context.target_node_handle)
                .into_py_any(py)
                .expect("PyNodeHandle should be creatable"),
            attributes: {
                let py_attrs: PyAttributes = context.attributes.deep_into();
                py_attrs
                    .into_py_any(py)
                    .expect("PyAttributes should be creatable")
            },
            group_handle: PyGroupHandle::from(context.group_handle)
                .into_py_any(py)
                .expect("PyGroup should be creatable"),
        }
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn extract(self, py: Python<'_>) -> PreAddEdgeWithGroupContext {
        let source: PyNodeHandle = self
            .source_node_handle
            .extract(py)
            .expect("PyNodeHandle should be extractable");

        let target: PyNodeHandle = self
            .target_node_handle
            .extract(py)
            .expect("PyNodeHandle should be extractable");

        let attributes: PyAttributes = self
            .attributes
            .extract(py)
            .expect("PyAttributes should be extractable");

        let group_handle: PyGroupHandle = self
            .group_handle
            .extract(py)
            .expect("PyGroup should be extractable");

        PreAddEdgeWithGroupContext {
            source_node_handle: source.into(),
            target_node_handle: target.into(),
            attributes: attributes.deep_into(),
            group_handle: group_handle.into(),
        }
    }
}

#[pymethods]
impl PyPreAddEdgeWithGroupContext {
    #[new]
    pub const fn new(
        source_node_handle: Py<PyAny>,
        target_node_handle: Py<PyAny>,
        attributes: Py<PyAny>,
        group_handle: Py<PyAny>,
    ) -> Self {
        Self {
            source_node_handle,
            target_node_handle,
            attributes,
            group_handle,
        }
    }

    #[getter]
    pub fn source_node_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.source_node_handle.clone_ref(py)
    }

    #[getter]
    pub fn target_node_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.target_node_handle.clone_ref(py)
    }

    #[getter]
    pub fn attributes(&self, py: Python<'_>) -> Py<PyAny> {
        self.attributes.clone_ref(py)
    }

    #[getter]
    pub fn group_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handle.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPostAddEdgeWithGroupContext {
    edge_index: Py<PyAny>,
    group_handle: Py<PyAny>,
}

impl Clone for PyPostAddEdgeWithGroupContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            edge_index: self.edge_index.clone_ref(py),
            group_handle: self.group_handle.clone_ref(py),
        })
    }
}

impl PyPostAddEdgeWithGroupContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PostAddEdgeWithGroupContext) -> Self {
        Self {
            edge_index: context
                .edge_index
                .into_py_any(py)
                .expect("edge_index should be creatable"),
            group_handle: PyGroupHandle::from(context.group_handle)
                .into_py_any(py)
                .expect("group_handle should be creatable"),
        }
    }
}

#[pymethods]
impl PyPostAddEdgeWithGroupContext {
    #[new]
    pub const fn new(edge_index: Py<PyAny>, group_handle: Py<PyAny>) -> Self {
        Self {
            edge_index,
            group_handle,
        }
    }

    #[getter]
    pub fn edge_index(&self, py: Python<'_>) -> Py<PyAny> {
        self.edge_index.clone_ref(py)
    }

    #[getter]
    pub fn group_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handle.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPreAddEdgeWithGroupsContext {
    source_node_handle: Py<PyAny>,
    target_node_handle: Py<PyAny>,
    attributes: Py<PyAny>,
    group_handles: Py<PyAny>,
}

impl Clone for PyPreAddEdgeWithGroupsContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            source_node_handle: self.source_node_handle.clone_ref(py),
            target_node_handle: self.target_node_handle.clone_ref(py),
            attributes: self.attributes.clone_ref(py),
            group_handles: self.group_handles.clone_ref(py),
        })
    }
}

impl PyPreAddEdgeWithGroupsContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PreAddEdgeWithGroupsContext) -> Self {
        let group_handles: Vec<PyGroupHandle> = context.group_handles.deep_into();

        Self {
            source_node_handle: PyNodeHandle::from(context.source_node_handle)
                .into_py_any(py)
                .expect("PyNodeHandle should be creatable"),
            target_node_handle: PyNodeHandle::from(context.target_node_handle)
                .into_py_any(py)
                .expect("PyNodeHandle should be creatable"),
            attributes: {
                let py_attrs: PyAttributes = context.attributes.deep_into();
                py_attrs
                    .into_py_any(py)
                    .expect("PyAttributes should be creatable")
            },
            group_handles: group_handles
                .into_py_any(py)
                .expect("groups should be creatable"),
        }
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn extract(self, py: Python<'_>) -> PreAddEdgeWithGroupsContext {
        let source: PyNodeHandle = self
            .source_node_handle
            .extract(py)
            .expect("PyNodeHandle should be extractable");

        let target: PyNodeHandle = self
            .target_node_handle
            .extract(py)
            .expect("PyNodeHandle should be extractable");

        let attributes: PyAttributes = self
            .attributes
            .extract(py)
            .expect("PyAttributes should be extractable");

        let group_handles: Vec<PyGroupHandle> = self
            .group_handles
            .extract(py)
            .expect("groups should be extractable");

        PreAddEdgeWithGroupsContext {
            source_node_handle: source.into(),
            target_node_handle: target.into(),
            attributes: attributes.deep_into(),
            group_handles: group_handles.deep_into(),
        }
    }
}

#[pymethods]
impl PyPreAddEdgeWithGroupsContext {
    #[new]
    pub const fn new(
        source_node_handle: Py<PyAny>,
        target_node_handle: Py<PyAny>,
        attributes: Py<PyAny>,
        group_handles: Py<PyAny>,
    ) -> Self {
        Self {
            source_node_handle,
            target_node_handle,
            attributes,
            group_handles,
        }
    }

    #[getter]
    pub fn source_node_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.source_node_handle.clone_ref(py)
    }

    #[getter]
    pub fn target_node_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.target_node_handle.clone_ref(py)
    }

    #[getter]
    pub fn attributes(&self, py: Python<'_>) -> Py<PyAny> {
        self.attributes.clone_ref(py)
    }

    #[getter]
    pub fn group_handles(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handles.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPostAddEdgeWithGroupsContext {
    edge_index: Py<PyAny>,
    group_handles: Py<PyAny>,
}

impl Clone for PyPostAddEdgeWithGroupsContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            edge_index: self.edge_index.clone_ref(py),
            group_handles: self.group_handles.clone_ref(py),
        })
    }
}

impl PyPostAddEdgeWithGroupsContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PostAddEdgeWithGroupsContext) -> Self {
        let group_handles: Vec<PyGroupHandle> = context.group_handles.deep_into();

        Self {
            edge_index: context
                .edge_index
                .into_py_any(py)
                .expect("edge_index should be creatable"),
            group_handles: group_handles
                .into_py_any(py)
                .expect("groups should be creatable"),
        }
    }
}

#[pymethods]
impl PyPostAddEdgeWithGroupsContext {
    #[new]
    pub const fn new(edge_index: Py<PyAny>, group_handles: Py<PyAny>) -> Self {
        Self {
            edge_index,
            group_handles,
        }
    }

    #[getter]
    pub fn edge_index(&self, py: Python<'_>) -> Py<PyAny> {
        self.edge_index.clone_ref(py)
    }

    #[getter]
    pub fn group_handles(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handles.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPreRemoveEdgeContext {
    edge_index: Py<PyAny>,
}

impl Clone for PyPreRemoveEdgeContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            edge_index: self.edge_index.clone_ref(py),
        })
    }
}

impl PyPreRemoveEdgeContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PreRemoveEdgeContext) -> Self {
        Self {
            edge_index: context
                .edge_index
                .into_py_any(py)
                .expect("edge_index should be creatable"),
        }
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn extract(self, py: Python<'_>) -> PreRemoveEdgeContext {
        PreRemoveEdgeContext {
            edge_index: self
                .edge_index
                .extract(py)
                .expect("edge_index should be extractable"),
        }
    }
}

#[pymethods]
impl PyPreRemoveEdgeContext {
    #[new]
    pub const fn new(edge_index: Py<PyAny>) -> Self {
        Self { edge_index }
    }

    #[getter]
    pub fn edge_index(&self, py: Python<'_>) -> Py<PyAny> {
        self.edge_index.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPostRemoveEdgeContext {
    edge_index: Py<PyAny>,
}

impl Clone for PyPostRemoveEdgeContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            edge_index: self.edge_index.clone_ref(py),
        })
    }
}

impl PyPostRemoveEdgeContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PostRemoveEdgeContext) -> Self {
        Self {
            edge_index: context
                .edge_index
                .into_py_any(py)
                .expect("edge_index should be creatable"),
        }
    }
}

#[pymethods]
impl PyPostRemoveEdgeContext {
    #[new]
    pub const fn new(edge_index: Py<PyAny>) -> Self {
        Self { edge_index }
    }

    #[getter]
    pub fn edge_index(&self, py: Python<'_>) -> Py<PyAny> {
        self.edge_index.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPreAddEdgesContext {
    edges: Py<PyAny>,
}

impl Clone for PyPreAddEdgesContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            edges: self.edges.clone_ref(py),
        })
    }
}

impl PyPreAddEdgesContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PreAddEdgesContext) -> Self {
        let edges: Vec<(PyNodeHandle, PyNodeHandle, PyAttributes)> = context.edges.deep_into();

        Self {
            edges: edges.into_py_any(py).expect("edges should be creatable"),
        }
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn extract(self, py: Python<'_>) -> PreAddEdgesContext {
        let edges: Vec<(PyNodeHandle, PyNodeHandle, PyAttributes)> =
            self.edges.extract(py).expect("edges should be extractable");

        PreAddEdgesContext {
            edges: edges.deep_into(),
        }
    }
}

#[pymethods]
impl PyPreAddEdgesContext {
    #[new]
    pub const fn new(edges: Py<PyAny>) -> Self {
        Self { edges }
    }

    #[getter]
    pub fn edges(&self, py: Python<'_>) -> Py<PyAny> {
        self.edges.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPostAddEdgesContext {
    edge_indices: Py<PyAny>,
}

impl Clone for PyPostAddEdgesContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            edge_indices: self.edge_indices.clone_ref(py),
        })
    }
}

impl PyPostAddEdgesContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PostAddEdgesContext) -> Self {
        Self {
            edge_indices: context
                .edge_indices
                .into_py_any(py)
                .expect("edge_indices should be creatable"),
        }
    }
}

#[pymethods]
impl PyPostAddEdgesContext {
    #[new]
    pub const fn new(edge_indices: Py<PyAny>) -> Self {
        Self { edge_indices }
    }

    #[getter]
    pub fn edge_indices(&self, py: Python<'_>) -> Py<PyAny> {
        self.edge_indices.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPreAddEdgesWithGroupContext {
    edges: Py<PyAny>,
    group_handle: Py<PyAny>,
}

impl Clone for PyPreAddEdgesWithGroupContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            edges: self.edges.clone_ref(py),
            group_handle: self.group_handle.clone_ref(py),
        })
    }
}

impl PyPreAddEdgesWithGroupContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PreAddEdgesWithGroupContext) -> Self {
        let edges: Vec<(PyNodeHandle, PyNodeHandle, PyAttributes)> = context.edges.deep_into();

        Self {
            edges: edges.into_py_any(py).expect("edges should be creatable"),
            group_handle: PyGroupHandle::from(context.group_handle)
                .into_py_any(py)
                .expect("PyGroup should be creatable"),
        }
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn extract(self, py: Python<'_>) -> PreAddEdgesWithGroupContext {
        let edges: Vec<(PyNodeHandle, PyNodeHandle, PyAttributes)> =
            self.edges.extract(py).expect("edges should be extractable");

        let group_handle: PyGroupHandle = self
            .group_handle
            .extract(py)
            .expect("PyGroup should be extractable");

        PreAddEdgesWithGroupContext {
            edges: edges.deep_into(),
            group_handle: group_handle.into(),
        }
    }
}

#[pymethods]
impl PyPreAddEdgesWithGroupContext {
    #[new]
    pub const fn new(edges: Py<PyAny>, group_handle: Py<PyAny>) -> Self {
        Self {
            edges,
            group_handle,
        }
    }

    #[getter]
    pub fn edges(&self, py: Python<'_>) -> Py<PyAny> {
        self.edges.clone_ref(py)
    }

    #[getter]
    pub fn group_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handle.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPostAddEdgesWithGroupContext {
    edge_indices: Py<PyAny>,
    group_handle: Py<PyAny>,
}

impl Clone for PyPostAddEdgesWithGroupContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            edge_indices: self.edge_indices.clone_ref(py),
            group_handle: self.group_handle.clone_ref(py),
        })
    }
}

impl PyPostAddEdgesWithGroupContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PostAddEdgesWithGroupContext) -> Self {
        Self {
            edge_indices: context
                .edge_indices
                .into_py_any(py)
                .expect("edge_indices should be creatable"),
            group_handle: PyGroupHandle::from(context.group_handle)
                .into_py_any(py)
                .expect("group_handle should be creatable"),
        }
    }
}

#[pymethods]
impl PyPostAddEdgesWithGroupContext {
    #[new]
    pub const fn new(edge_indices: Py<PyAny>, group_handle: Py<PyAny>) -> Self {
        Self {
            edge_indices,
            group_handle,
        }
    }

    #[getter]
    pub fn edge_indices(&self, py: Python<'_>) -> Py<PyAny> {
        self.edge_indices.clone_ref(py)
    }

    #[getter]
    pub fn group_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handle.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPreAddEdgesWithGroupsContext {
    edges: Py<PyAny>,
    group_handles: Py<PyAny>,
}

impl Clone for PyPreAddEdgesWithGroupsContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            edges: self.edges.clone_ref(py),
            group_handles: self.group_handles.clone_ref(py),
        })
    }
}

impl PyPreAddEdgesWithGroupsContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PreAddEdgesWithGroupsContext) -> Self {
        let edges: Vec<(PyNodeHandle, PyNodeHandle, PyAttributes)> = context.edges.deep_into();
        let group_handles: Vec<PyGroupHandle> = context.group_handles.deep_into();

        Self {
            edges: edges.into_py_any(py).expect("edges should be creatable"),
            group_handles: group_handles
                .into_py_any(py)
                .expect("groups should be creatable"),
        }
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn extract(self, py: Python<'_>) -> PreAddEdgesWithGroupsContext {
        let edges: Vec<(PyNodeHandle, PyNodeHandle, PyAttributes)> =
            self.edges.extract(py).expect("edges should be extractable");

        let group_handles: Vec<PyGroupHandle> = self
            .group_handles
            .extract(py)
            .expect("groups should be extractable");

        PreAddEdgesWithGroupsContext {
            edges: edges.deep_into(),
            group_handles: group_handles.deep_into(),
        }
    }
}

#[pymethods]
impl PyPreAddEdgesWithGroupsContext {
    #[new]
    pub const fn new(edges: Py<PyAny>, group_handles: Py<PyAny>) -> Self {
        Self {
            edges,
            group_handles,
        }
    }

    #[getter]
    pub fn edges(&self, py: Python<'_>) -> Py<PyAny> {
        self.edges.clone_ref(py)
    }

    #[getter]
    pub fn group_handles(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handles.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPostAddEdgesWithGroupsContext {
    edge_indices: Py<PyAny>,
    group_handles: Py<PyAny>,
}

impl Clone for PyPostAddEdgesWithGroupsContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            edge_indices: self.edge_indices.clone_ref(py),
            group_handles: self.group_handles.clone_ref(py),
        })
    }
}

impl PyPostAddEdgesWithGroupsContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PostAddEdgesWithGroupsContext) -> Self {
        let group_handles: Vec<PyGroupHandle> = context.group_handles.deep_into();

        Self {
            edge_indices: context
                .edge_indices
                .into_py_any(py)
                .expect("edge_indices should be creatable"),
            group_handles: group_handles
                .into_py_any(py)
                .expect("groups should be creatable"),
        }
    }
}

#[pymethods]
impl PyPostAddEdgesWithGroupsContext {
    #[new]
    pub const fn new(edge_indices: Py<PyAny>, group_handles: Py<PyAny>) -> Self {
        Self {
            edge_indices,
            group_handles,
        }
    }

    #[getter]
    pub fn edge_indices(&self, py: Python<'_>) -> Py<PyAny> {
        self.edge_indices.clone_ref(py)
    }

    #[getter]
    pub fn group_handles(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handles.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPreAddEdgesDataframesContext {
    edges_dataframes: Py<PyAny>,
}

impl Clone for PyPreAddEdgesDataframesContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            edges_dataframes: self.edges_dataframes.clone_ref(py),
        })
    }
}

impl PyPreAddEdgesDataframesContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PreAddEdgesDataframesContext) -> Self {
        let edges_dataframes = edge_dataframe_inputs_to_py(context.edges_dataframes);

        Self {
            edges_dataframes: edges_dataframes
                .into_py_any(py)
                .expect("edges_dataframes should be creatable"),
        }
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn extract(self, py: Python<'_>) -> PreAddEdgesDataframesContext {
        let edges_dataframes: Vec<(PyDataFrame, String, String)> = self
            .edges_dataframes
            .extract(py)
            .expect("edges_dataframes should be extractable");

        PreAddEdgesDataframesContext {
            edges_dataframes: py_to_edge_dataframe_inputs(edges_dataframes),
        }
    }
}

#[pymethods]
impl PyPreAddEdgesDataframesContext {
    #[new]
    pub const fn new(edges_dataframes: Py<PyAny>) -> Self {
        Self { edges_dataframes }
    }

    #[getter]
    pub fn edges_dataframes(&self, py: Python<'_>) -> Py<PyAny> {
        self.edges_dataframes.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPostAddEdgesDataframesContext {
    edges_dataframes: Py<PyAny>,
}

impl Clone for PyPostAddEdgesDataframesContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            edges_dataframes: self.edges_dataframes.clone_ref(py),
        })
    }
}

impl PyPostAddEdgesDataframesContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PostAddEdgesDataframesContext) -> Self {
        let edges_dataframes = edge_dataframe_inputs_to_py(context.edges_dataframes);

        Self {
            edges_dataframes: edges_dataframes
                .into_py_any(py)
                .expect("edges_dataframes should be creatable"),
        }
    }
}

#[pymethods]
impl PyPostAddEdgesDataframesContext {
    #[new]
    pub const fn new(edges_dataframes: Py<PyAny>) -> Self {
        Self { edges_dataframes }
    }

    #[getter]
    pub fn edges_dataframes(&self, py: Python<'_>) -> Py<PyAny> {
        self.edges_dataframes.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPreAddEdgesDataframesWithGroupContext {
    edges_dataframes: Py<PyAny>,
    group_handle: Py<PyAny>,
}

impl Clone for PyPreAddEdgesDataframesWithGroupContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            edges_dataframes: self.edges_dataframes.clone_ref(py),
            group_handle: self.group_handle.clone_ref(py),
        })
    }
}

impl PyPreAddEdgesDataframesWithGroupContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PreAddEdgesDataframesWithGroupContext) -> Self {
        let edges_dataframes = edge_dataframe_inputs_to_py(context.edges_dataframes);

        Self {
            edges_dataframes: edges_dataframes
                .into_py_any(py)
                .expect("edges_dataframes should be creatable"),
            group_handle: PyGroupHandle::from(context.group_handle)
                .into_py_any(py)
                .expect("PyGroup should be creatable"),
        }
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn extract(self, py: Python<'_>) -> PreAddEdgesDataframesWithGroupContext {
        let edges_dataframes: Vec<(PyDataFrame, String, String)> = self
            .edges_dataframes
            .extract(py)
            .expect("edges_dataframes should be extractable");

        let group_handle: PyGroupHandle = self
            .group_handle
            .extract(py)
            .expect("PyGroup should be extractable");

        PreAddEdgesDataframesWithGroupContext {
            edges_dataframes: py_to_edge_dataframe_inputs(edges_dataframes),
            group_handle: group_handle.into(),
        }
    }
}

#[pymethods]
impl PyPreAddEdgesDataframesWithGroupContext {
    #[new]
    pub const fn new(edges_dataframes: Py<PyAny>, group_handle: Py<PyAny>) -> Self {
        Self {
            edges_dataframes,
            group_handle,
        }
    }

    #[getter]
    pub fn edges_dataframes(&self, py: Python<'_>) -> Py<PyAny> {
        self.edges_dataframes.clone_ref(py)
    }

    #[getter]
    pub fn group_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handle.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPostAddEdgesDataframesWithGroupContext {
    edges_dataframes: Py<PyAny>,
    group_handle: Py<PyAny>,
}

impl Clone for PyPostAddEdgesDataframesWithGroupContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            edges_dataframes: self.edges_dataframes.clone_ref(py),
            group_handle: self.group_handle.clone_ref(py),
        })
    }
}

impl PyPostAddEdgesDataframesWithGroupContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PostAddEdgesDataframesWithGroupContext) -> Self {
        let edges_dataframes = edge_dataframe_inputs_to_py(context.edges_dataframes);

        Self {
            edges_dataframes: edges_dataframes
                .into_py_any(py)
                .expect("edges_dataframes should be creatable"),
            group_handle: PyGroupHandle::from(context.group_handle)
                .into_py_any(py)
                .expect("PyGroup should be creatable"),
        }
    }
}

#[pymethods]
impl PyPostAddEdgesDataframesWithGroupContext {
    #[new]
    pub const fn new(edges_dataframes: Py<PyAny>, group_handle: Py<PyAny>) -> Self {
        Self {
            edges_dataframes,
            group_handle,
        }
    }

    #[getter]
    pub fn edges_dataframes(&self, py: Python<'_>) -> Py<PyAny> {
        self.edges_dataframes.clone_ref(py)
    }

    #[getter]
    pub fn group_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handle.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPreAddEdgesDataframesWithGroupsContext {
    edges_dataframes: Py<PyAny>,
    group_handles: Py<PyAny>,
}

impl Clone for PyPreAddEdgesDataframesWithGroupsContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            edges_dataframes: self.edges_dataframes.clone_ref(py),
            group_handles: self.group_handles.clone_ref(py),
        })
    }
}

impl PyPreAddEdgesDataframesWithGroupsContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PreAddEdgesDataframesWithGroupsContext) -> Self {
        let edges_dataframes = edge_dataframe_inputs_to_py(context.edges_dataframes);
        let group_handles: Vec<PyGroupHandle> = context.group_handles.deep_into();

        Self {
            edges_dataframes: edges_dataframes
                .into_py_any(py)
                .expect("edges_dataframes should be creatable"),
            group_handles: group_handles
                .into_py_any(py)
                .expect("groups should be creatable"),
        }
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn extract(self, py: Python<'_>) -> PreAddEdgesDataframesWithGroupsContext {
        let edges_dataframes: Vec<(PyDataFrame, String, String)> = self
            .edges_dataframes
            .extract(py)
            .expect("edges_dataframes should be extractable");

        let group_handles: Vec<PyGroupHandle> = self
            .group_handles
            .extract(py)
            .expect("groups should be extractable");

        PreAddEdgesDataframesWithGroupsContext {
            edges_dataframes: py_to_edge_dataframe_inputs(edges_dataframes),
            group_handles: group_handles.deep_into(),
        }
    }
}

#[pymethods]
impl PyPreAddEdgesDataframesWithGroupsContext {
    #[new]
    pub const fn new(edges_dataframes: Py<PyAny>, group_handles: Py<PyAny>) -> Self {
        Self {
            edges_dataframes,
            group_handles,
        }
    }

    #[getter]
    pub fn edges_dataframes(&self, py: Python<'_>) -> Py<PyAny> {
        self.edges_dataframes.clone_ref(py)
    }

    #[getter]
    pub fn group_handles(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handles.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPostAddEdgesDataframesWithGroupsContext {
    edges_dataframes: Py<PyAny>,
    group_handles: Py<PyAny>,
}

impl Clone for PyPostAddEdgesDataframesWithGroupsContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            edges_dataframes: self.edges_dataframes.clone_ref(py),
            group_handles: self.group_handles.clone_ref(py),
        })
    }
}

impl PyPostAddEdgesDataframesWithGroupsContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PostAddEdgesDataframesWithGroupsContext) -> Self {
        let edges_dataframes = edge_dataframe_inputs_to_py(context.edges_dataframes);
        let group_handles: Vec<PyGroupHandle> = context.group_handles.deep_into();

        Self {
            edges_dataframes: edges_dataframes
                .into_py_any(py)
                .expect("edges_dataframes should be creatable"),
            group_handles: group_handles
                .into_py_any(py)
                .expect("groups should be creatable"),
        }
    }
}

#[pymethods]
impl PyPostAddEdgesDataframesWithGroupsContext {
    #[new]
    pub const fn new(edges_dataframes: Py<PyAny>, group_handles: Py<PyAny>) -> Self {
        Self {
            edges_dataframes,
            group_handles,
        }
    }

    #[getter]
    pub fn edges_dataframes(&self, py: Python<'_>) -> Py<PyAny> {
        self.edges_dataframes.clone_ref(py)
    }

    #[getter]
    pub fn group_handles(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handles.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPreAddGroupContext {
    group: Py<PyAny>,
    node_handles: Py<PyAny>,
    edge_indices: Py<PyAny>,
}

impl Clone for PyPreAddGroupContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            group: self.group.clone_ref(py),
            node_handles: self.node_handles.clone_ref(py),
            edge_indices: self.edge_indices.clone_ref(py),
        })
    }
}

impl PyPreAddGroupContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PreAddGroupContext) -> Self {
        let node_handles: Option<Vec<PyNodeHandle>> = context.node_handles.deep_into();

        Self {
            group: PyGroup::from(context.group)
                .into_py_any(py)
                .expect("PyGroup should be creatable"),
            node_handles: node_handles
                .into_py_any(py)
                .expect("node_handles should be creatable"),
            edge_indices: context
                .edge_indices
                .into_py_any(py)
                .expect("edge_indices should be creatable"),
        }
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn extract(self, py: Python<'_>) -> PreAddGroupContext {
        let group: PyGroup = self
            .group
            .extract(py)
            .expect("PyGroup should be extractable");

        let node_handles: Option<Vec<PyNodeHandle>> = self
            .node_handles
            .extract(py)
            .expect("node_handles should be extractable");

        let edge_indices: Option<Vec<EdgeIndex>> = self
            .edge_indices
            .extract(py)
            .expect("edge_indices should be extractable");

        PreAddGroupContext {
            group: group.into(),
            node_handles: node_handles.deep_into(),
            edge_indices,
        }
    }
}

#[pymethods]
impl PyPreAddGroupContext {
    #[new]
    pub const fn new(group: Py<PyAny>, node_handles: Py<PyAny>, edge_indices: Py<PyAny>) -> Self {
        Self {
            group,
            node_handles,
            edge_indices,
        }
    }

    #[getter]
    pub fn group(&self, py: Python<'_>) -> Py<PyAny> {
        self.group.clone_ref(py)
    }

    #[getter]
    pub fn node_handles(&self, py: Python<'_>) -> Py<PyAny> {
        self.node_handles.clone_ref(py)
    }

    #[getter]
    pub fn edge_indices(&self, py: Python<'_>) -> Py<PyAny> {
        self.edge_indices.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPostAddGroupContext {
    group: Py<PyAny>,
    node_handles: Py<PyAny>,
    edge_indices: Py<PyAny>,
}

impl Clone for PyPostAddGroupContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            group: self.group.clone_ref(py),
            node_handles: self.node_handles.clone_ref(py),
            edge_indices: self.edge_indices.clone_ref(py),
        })
    }
}

impl PyPostAddGroupContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PostAddGroupContext) -> Self {
        let node_handles: Option<Vec<PyNodeHandle>> = context.node_handles.deep_into();

        Self {
            group: PyGroup::from(context.group)
                .into_py_any(py)
                .expect("PyGroup should be creatable"),
            node_handles: node_handles
                .into_py_any(py)
                .expect("node_handles should be creatable"),
            edge_indices: context
                .edge_indices
                .into_py_any(py)
                .expect("edge_indices should be creatable"),
        }
    }
}

#[pymethods]
impl PyPostAddGroupContext {
    #[new]
    pub const fn new(group: Py<PyAny>, node_handles: Py<PyAny>, edge_indices: Py<PyAny>) -> Self {
        Self {
            group,
            node_handles,
            edge_indices,
        }
    }

    #[getter]
    pub fn group(&self, py: Python<'_>) -> Py<PyAny> {
        self.group.clone_ref(py)
    }

    #[getter]
    pub fn node_handles(&self, py: Python<'_>) -> Py<PyAny> {
        self.node_handles.clone_ref(py)
    }

    #[getter]
    pub fn edge_indices(&self, py: Python<'_>) -> Py<PyAny> {
        self.edge_indices.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPreRemoveGroupContext {
    group_handle: Py<PyAny>,
}

impl Clone for PyPreRemoveGroupContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            group_handle: self.group_handle.clone_ref(py),
        })
    }
}

impl PyPreRemoveGroupContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PreRemoveGroupContext) -> Self {
        Self {
            group_handle: PyGroupHandle::from(context.group_handle)
                .into_py_any(py)
                .expect("PyGroup should be creatable"),
        }
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn extract(self, py: Python<'_>) -> PreRemoveGroupContext {
        let group_handle: PyGroupHandle = self
            .group_handle
            .extract(py)
            .expect("PyGroup should be extractable");

        PreRemoveGroupContext {
            group_handle: group_handle.into(),
        }
    }
}

#[pymethods]
impl PyPreRemoveGroupContext {
    #[new]
    pub const fn new(group_handle: Py<PyAny>) -> Self {
        Self { group_handle }
    }

    #[getter]
    pub fn group_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handle.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPostRemoveGroupContext {
    group_handle: Py<PyAny>,
}

impl Clone for PyPostRemoveGroupContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            group_handle: self.group_handle.clone_ref(py),
        })
    }
}

impl PyPostRemoveGroupContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PostRemoveGroupContext) -> Self {
        Self {
            group_handle: PyGroupHandle::from(context.group_handle)
                .into_py_any(py)
                .expect("PyGroupHandle should be creatable"),
        }
    }
}

#[pymethods]
impl PyPostRemoveGroupContext {
    #[new]
    pub const fn new(group_handle: Py<PyAny>) -> Self {
        Self { group_handle }
    }

    #[getter]
    pub fn group_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handle.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPreAddNodeToGroupContext {
    group_handle: Py<PyAny>,
    node_handle: Py<PyAny>,
}

impl Clone for PyPreAddNodeToGroupContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            group_handle: self.group_handle.clone_ref(py),
            node_handle: self.node_handle.clone_ref(py),
        })
    }
}

impl PyPreAddNodeToGroupContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PreAddNodeToGroupContext) -> Self {
        Self {
            group_handle: PyGroupHandle::from(context.group_handle)
                .into_py_any(py)
                .expect("PyGroup should be creatable"),
            node_handle: PyNodeHandle::from(context.node_handle)
                .into_py_any(py)
                .expect("PyNodeIndex should be creatable"),
        }
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn extract(self, py: Python<'_>) -> PreAddNodeToGroupContext {
        let group_handle: PyGroupHandle = self
            .group_handle
            .extract(py)
            .expect("PyGroup should be extractable");

        let node_handle: PyNodeHandle = self
            .node_handle
            .extract(py)
            .expect("PyNodeIndex should be extractable");

        PreAddNodeToGroupContext {
            group_handle: group_handle.into(),
            node_handle: node_handle.into(),
        }
    }
}

#[pymethods]
impl PyPreAddNodeToGroupContext {
    #[new]
    pub const fn new(group_handle: Py<PyAny>, node_handle: Py<PyAny>) -> Self {
        Self {
            group_handle,
            node_handle,
        }
    }

    #[getter]
    pub fn group_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handle.clone_ref(py)
    }

    #[getter]
    pub fn node_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.node_handle.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPostAddNodeToGroupContext {
    group_handle: Py<PyAny>,
    node_handle: Py<PyAny>,
}

impl Clone for PyPostAddNodeToGroupContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            group_handle: self.group_handle.clone_ref(py),
            node_handle: self.node_handle.clone_ref(py),
        })
    }
}

impl PyPostAddNodeToGroupContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PostAddNodeToGroupContext) -> Self {
        Self {
            group_handle: PyGroupHandle::from(context.group_handle)
                .into_py_any(py)
                .expect("PyGroup should be creatable"),
            node_handle: PyNodeHandle::from(context.node_handle)
                .into_py_any(py)
                .expect("PyNodeIndex should be creatable"),
        }
    }
}

#[pymethods]
impl PyPostAddNodeToGroupContext {
    #[new]
    pub const fn new(group_handle: Py<PyAny>, node_handle: Py<PyAny>) -> Self {
        Self {
            group_handle,
            node_handle,
        }
    }

    #[getter]
    pub fn group_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handle.clone_ref(py)
    }

    #[getter]
    pub fn node_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.node_handle.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPreAddNodeToGroupsContext {
    group_handles: Py<PyAny>,
    node_handle: Py<PyAny>,
}

impl Clone for PyPreAddNodeToGroupsContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            group_handles: self.group_handles.clone_ref(py),
            node_handle: self.node_handle.clone_ref(py),
        })
    }
}

impl PyPreAddNodeToGroupsContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PreAddNodeToGroupsContext) -> Self {
        let group_handles: Vec<PyGroupHandle> = context.group_handles.deep_into();

        Self {
            group_handles: group_handles
                .into_py_any(py)
                .expect("groups should be creatable"),
            node_handle: PyNodeHandle::from(context.node_handle)
                .into_py_any(py)
                .expect("PyNodeIndex should be creatable"),
        }
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn extract(self, py: Python<'_>) -> PreAddNodeToGroupsContext {
        let group_handles: Vec<PyGroupHandle> = self
            .group_handles
            .extract(py)
            .expect("groups should be extractable");

        let node_handle: PyNodeHandle = self
            .node_handle
            .extract(py)
            .expect("PyNodeIndex should be extractable");

        PreAddNodeToGroupsContext {
            group_handles: group_handles.deep_into(),
            node_handle: node_handle.into(),
        }
    }
}

#[pymethods]
impl PyPreAddNodeToGroupsContext {
    #[new]
    pub const fn new(group_handles: Py<PyAny>, node_handle: Py<PyAny>) -> Self {
        Self {
            group_handles,
            node_handle,
        }
    }

    #[getter]
    pub fn group_handles(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handles.clone_ref(py)
    }

    #[getter]
    pub fn node_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.node_handle.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPostAddNodeToGroupsContext {
    group_handles: Py<PyAny>,
    node_handle: Py<PyAny>,
}

impl Clone for PyPostAddNodeToGroupsContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            group_handles: self.group_handles.clone_ref(py),
            node_handle: self.node_handle.clone_ref(py),
        })
    }
}

impl PyPostAddNodeToGroupsContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PostAddNodeToGroupsContext) -> Self {
        let group_handles: Vec<PyGroupHandle> = context.group_handles.deep_into();

        Self {
            group_handles: group_handles
                .into_py_any(py)
                .expect("groups should be creatable"),
            node_handle: PyNodeHandle::from(context.node_handle)
                .into_py_any(py)
                .expect("PyNodeIndex should be creatable"),
        }
    }
}

#[pymethods]
impl PyPostAddNodeToGroupsContext {
    #[new]
    pub const fn new(group_handles: Py<PyAny>, node_handle: Py<PyAny>) -> Self {
        Self {
            group_handles,
            node_handle,
        }
    }

    #[getter]
    pub fn group_handles(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handles.clone_ref(py)
    }

    #[getter]
    pub fn node_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.node_handle.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPreAddNodesToGroupsContext {
    group_handles: Py<PyAny>,
    node_handles: Py<PyAny>,
}

impl Clone for PyPreAddNodesToGroupsContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            group_handles: self.group_handles.clone_ref(py),
            node_handles: self.node_handles.clone_ref(py),
        })
    }
}

impl PyPreAddNodesToGroupsContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PreAddNodesToGroupsContext) -> Self {
        let group_handles: Vec<PyGroupHandle> = context.group_handles.deep_into();
        let node_handles: Vec<PyNodeHandle> = context.node_handles.deep_into();

        Self {
            group_handles: group_handles
                .into_py_any(py)
                .expect("groups should be creatable"),
            node_handles: node_handles
                .into_py_any(py)
                .expect("node_indices should be creatable"),
        }
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn extract(self, py: Python<'_>) -> PreAddNodesToGroupsContext {
        let group_handles: Vec<PyGroupHandle> = self
            .group_handles
            .extract(py)
            .expect("groups should be extractable");

        let node_handles: Vec<PyNodeHandle> = self
            .node_handles
            .extract(py)
            .expect("node_indices should be extractable");

        PreAddNodesToGroupsContext {
            group_handles: group_handles.deep_into(),
            node_handles: node_handles.deep_into(),
        }
    }
}

#[pymethods]
impl PyPreAddNodesToGroupsContext {
    #[new]
    pub const fn new(group_handles: Py<PyAny>, node_handles: Py<PyAny>) -> Self {
        Self {
            group_handles,
            node_handles,
        }
    }

    #[getter]
    pub fn group_handles(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handles.clone_ref(py)
    }

    #[getter]
    pub fn node_handles(&self, py: Python<'_>) -> Py<PyAny> {
        self.node_handles.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPostAddNodesToGroupsContext {
    group_handles: Py<PyAny>,
    node_handles: Py<PyAny>,
}

impl Clone for PyPostAddNodesToGroupsContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            group_handles: self.group_handles.clone_ref(py),
            node_handles: self.node_handles.clone_ref(py),
        })
    }
}

impl PyPostAddNodesToGroupsContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PostAddNodesToGroupsContext) -> Self {
        let group_handles: Vec<PyGroupHandle> = context.group_handles.deep_into();
        let node_handles: Vec<PyNodeHandle> = context.node_handles.deep_into();

        Self {
            group_handles: group_handles
                .into_py_any(py)
                .expect("groups should be creatable"),
            node_handles: node_handles
                .into_py_any(py)
                .expect("node_indices should be creatable"),
        }
    }
}

#[pymethods]
impl PyPostAddNodesToGroupsContext {
    #[new]
    pub const fn new(group_handles: Py<PyAny>, node_handles: Py<PyAny>) -> Self {
        Self {
            group_handles,
            node_handles,
        }
    }

    #[getter]
    pub fn group_handles(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handles.clone_ref(py)
    }

    #[getter]
    pub fn node_handles(&self, py: Python<'_>) -> Py<PyAny> {
        self.node_handles.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPreAddEdgeToGroupContext {
    group_handle: Py<PyAny>,
    edge_index: Py<PyAny>,
}

impl Clone for PyPreAddEdgeToGroupContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            group_handle: self.group_handle.clone_ref(py),
            edge_index: self.edge_index.clone_ref(py),
        })
    }
}

impl PyPreAddEdgeToGroupContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PreAddEdgeToGroupContext) -> Self {
        Self {
            group_handle: PyGroupHandle::from(context.group_handle)
                .into_py_any(py)
                .expect("PyGroup should be creatable"),
            edge_index: context
                .edge_index
                .into_py_any(py)
                .expect("edge_index should be creatable"),
        }
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn extract(self, py: Python<'_>) -> PreAddEdgeToGroupContext {
        let group_handle: PyGroupHandle = self
            .group_handle
            .extract(py)
            .expect("PyGroup should be extractable");

        PreAddEdgeToGroupContext {
            group_handle: group_handle.into(),
            edge_index: self
                .edge_index
                .extract(py)
                .expect("edge_index should be extractable"),
        }
    }
}

#[pymethods]
impl PyPreAddEdgeToGroupContext {
    #[new]
    pub const fn new(group_handle: Py<PyAny>, edge_index: Py<PyAny>) -> Self {
        Self {
            group_handle,
            edge_index,
        }
    }

    #[getter]
    pub fn group_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handle.clone_ref(py)
    }

    #[getter]
    pub fn edge_index(&self, py: Python<'_>) -> Py<PyAny> {
        self.edge_index.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPostAddEdgeToGroupContext {
    group_handle: Py<PyAny>,
    edge_index: Py<PyAny>,
}

impl Clone for PyPostAddEdgeToGroupContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            group_handle: self.group_handle.clone_ref(py),
            edge_index: self.edge_index.clone_ref(py),
        })
    }
}

impl PyPostAddEdgeToGroupContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PostAddEdgeToGroupContext) -> Self {
        Self {
            group_handle: PyGroupHandle::from(context.group_handle)
                .into_py_any(py)
                .expect("PyGroup should be creatable"),
            edge_index: context
                .edge_index
                .into_py_any(py)
                .expect("edge_index should be creatable"),
        }
    }
}

#[pymethods]
impl PyPostAddEdgeToGroupContext {
    #[new]
    pub const fn new(group_handle: Py<PyAny>, edge_index: Py<PyAny>) -> Self {
        Self {
            group_handle,
            edge_index,
        }
    }

    #[getter]
    pub fn group_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handle.clone_ref(py)
    }

    #[getter]
    pub fn edge_index(&self, py: Python<'_>) -> Py<PyAny> {
        self.edge_index.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPreAddEdgeToGroupsContext {
    group_handles: Py<PyAny>,
    edge_index: Py<PyAny>,
}

impl Clone for PyPreAddEdgeToGroupsContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            group_handles: self.group_handles.clone_ref(py),
            edge_index: self.edge_index.clone_ref(py),
        })
    }
}

impl PyPreAddEdgeToGroupsContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PreAddEdgeToGroupsContext) -> Self {
        let group_handles: Vec<PyGroupHandle> = context.group_handles.deep_into();

        Self {
            group_handles: group_handles
                .into_py_any(py)
                .expect("groups should be creatable"),
            edge_index: context
                .edge_index
                .into_py_any(py)
                .expect("edge_index should be creatable"),
        }
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn extract(self, py: Python<'_>) -> PreAddEdgeToGroupsContext {
        let group_handles: Vec<PyGroupHandle> = self
            .group_handles
            .extract(py)
            .expect("groups should be extractable");

        PreAddEdgeToGroupsContext {
            group_handles: group_handles.deep_into(),
            edge_index: self
                .edge_index
                .extract(py)
                .expect("edge_index should be extractable"),
        }
    }
}

#[pymethods]
impl PyPreAddEdgeToGroupsContext {
    #[new]
    pub const fn new(group_handles: Py<PyAny>, edge_index: Py<PyAny>) -> Self {
        Self {
            group_handles,
            edge_index,
        }
    }

    #[getter]
    pub fn group_handles(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handles.clone_ref(py)
    }

    #[getter]
    pub fn edge_index(&self, py: Python<'_>) -> Py<PyAny> {
        self.edge_index.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPostAddEdgeToGroupsContext {
    group_handles: Py<PyAny>,
    edge_index: Py<PyAny>,
}

impl Clone for PyPostAddEdgeToGroupsContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            group_handles: self.group_handles.clone_ref(py),
            edge_index: self.edge_index.clone_ref(py),
        })
    }
}

impl PyPostAddEdgeToGroupsContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PostAddEdgeToGroupsContext) -> Self {
        let group_handles: Vec<PyGroupHandle> = context.group_handles.deep_into();

        Self {
            group_handles: group_handles
                .into_py_any(py)
                .expect("groups should be creatable"),
            edge_index: context
                .edge_index
                .into_py_any(py)
                .expect("edge_index should be creatable"),
        }
    }
}

#[pymethods]
impl PyPostAddEdgeToGroupsContext {
    #[new]
    pub const fn new(group_handles: Py<PyAny>, edge_index: Py<PyAny>) -> Self {
        Self {
            group_handles,
            edge_index,
        }
    }

    #[getter]
    pub fn group_handles(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handles.clone_ref(py)
    }

    #[getter]
    pub fn edge_index(&self, py: Python<'_>) -> Py<PyAny> {
        self.edge_index.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPreAddEdgesToGroupsContext {
    group_handles: Py<PyAny>,
    edge_indices: Py<PyAny>,
}

impl Clone for PyPreAddEdgesToGroupsContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            group_handles: self.group_handles.clone_ref(py),
            edge_indices: self.edge_indices.clone_ref(py),
        })
    }
}

impl PyPreAddEdgesToGroupsContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PreAddEdgesToGroupsContext) -> Self {
        let group_handles: Vec<PyGroupHandle> = context.group_handles.deep_into();

        Self {
            group_handles: group_handles
                .into_py_any(py)
                .expect("groups should be creatable"),
            edge_indices: context
                .edge_indices
                .into_py_any(py)
                .expect("edge_indices should be creatable"),
        }
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn extract(self, py: Python<'_>) -> PreAddEdgesToGroupsContext {
        let group_handles: Vec<PyGroupHandle> = self
            .group_handles
            .extract(py)
            .expect("groups should be extractable");

        PreAddEdgesToGroupsContext {
            group_handles: group_handles.deep_into(),
            edge_indices: self
                .edge_indices
                .extract(py)
                .expect("edge_indices should be extractable"),
        }
    }
}

#[pymethods]
impl PyPreAddEdgesToGroupsContext {
    #[new]
    pub const fn new(group_handles: Py<PyAny>, edge_indices: Py<PyAny>) -> Self {
        Self {
            group_handles,
            edge_indices,
        }
    }

    #[getter]
    pub fn group_handles(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handles.clone_ref(py)
    }

    #[getter]
    pub fn edge_indices(&self, py: Python<'_>) -> Py<PyAny> {
        self.edge_indices.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPostAddEdgesToGroupsContext {
    group_handles: Py<PyAny>,
    edge_indices: Py<PyAny>,
}

impl Clone for PyPostAddEdgesToGroupsContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            group_handles: self.group_handles.clone_ref(py),
            edge_indices: self.edge_indices.clone_ref(py),
        })
    }
}

impl PyPostAddEdgesToGroupsContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PostAddEdgesToGroupsContext) -> Self {
        let group_handles: Vec<PyGroupHandle> = context.group_handles.deep_into();

        Self {
            group_handles: group_handles
                .into_py_any(py)
                .expect("groups should be creatable"),
            edge_indices: context
                .edge_indices
                .into_py_any(py)
                .expect("edge_indices should be creatable"),
        }
    }
}

#[pymethods]
impl PyPostAddEdgesToGroupsContext {
    #[new]
    pub const fn new(group_handles: Py<PyAny>, edge_indices: Py<PyAny>) -> Self {
        Self {
            group_handles,
            edge_indices,
        }
    }

    #[getter]
    pub fn group_handles(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handles.clone_ref(py)
    }

    #[getter]
    pub fn edge_indices(&self, py: Python<'_>) -> Py<PyAny> {
        self.edge_indices.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPreRemoveNodeFromGroupContext {
    group_handle: Py<PyAny>,
    node_handle: Py<PyAny>,
}

impl Clone for PyPreRemoveNodeFromGroupContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            group_handle: self.group_handle.clone_ref(py),
            node_handle: self.node_handle.clone_ref(py),
        })
    }
}

impl PyPreRemoveNodeFromGroupContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PreRemoveNodeFromGroupContext) -> Self {
        Self {
            group_handle: PyGroupHandle::from(context.group_handle)
                .into_py_any(py)
                .expect("PyGroup should be creatable"),
            node_handle: PyNodeHandle::from(context.node_handle)
                .into_py_any(py)
                .expect("PyNodeIndex should be creatable"),
        }
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn extract(self, py: Python<'_>) -> PreRemoveNodeFromGroupContext {
        let group_handle: PyGroupHandle = self
            .group_handle
            .extract(py)
            .expect("PyGroup should be extractable");

        let node_handle: PyNodeHandle = self
            .node_handle
            .extract(py)
            .expect("PyNodeIndex should be extractable");

        PreRemoveNodeFromGroupContext {
            group_handle: group_handle.into(),
            node_handle: node_handle.into(),
        }
    }
}

#[pymethods]
impl PyPreRemoveNodeFromGroupContext {
    #[new]
    pub const fn new(group_handle: Py<PyAny>, node_handle: Py<PyAny>) -> Self {
        Self {
            group_handle,
            node_handle,
        }
    }

    #[getter]
    pub fn group_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handle.clone_ref(py)
    }

    #[getter]
    pub fn node_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.node_handle.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPostRemoveNodeFromGroupContext {
    group_handle: Py<PyAny>,
    node_handle: Py<PyAny>,
}

impl Clone for PyPostRemoveNodeFromGroupContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            group_handle: self.group_handle.clone_ref(py),
            node_handle: self.node_handle.clone_ref(py),
        })
    }
}

impl PyPostRemoveNodeFromGroupContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PostRemoveNodeFromGroupContext) -> Self {
        Self {
            group_handle: PyGroupHandle::from(context.group_handle)
                .into_py_any(py)
                .expect("PyGroup should be creatable"),
            node_handle: PyNodeHandle::from(context.node_handle)
                .into_py_any(py)
                .expect("PyNodeIndex should be creatable"),
        }
    }
}

#[pymethods]
impl PyPostRemoveNodeFromGroupContext {
    #[new]
    pub const fn new(group_handle: Py<PyAny>, node_handle: Py<PyAny>) -> Self {
        Self {
            group_handle,
            node_handle,
        }
    }

    #[getter]
    pub fn group_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handle.clone_ref(py)
    }

    #[getter]
    pub fn node_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.node_handle.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPreRemoveNodeFromGroupsContext {
    group_handles: Py<PyAny>,
    node_handle: Py<PyAny>,
}

impl Clone for PyPreRemoveNodeFromGroupsContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            group_handles: self.group_handles.clone_ref(py),
            node_handle: self.node_handle.clone_ref(py),
        })
    }
}

impl PyPreRemoveNodeFromGroupsContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PreRemoveNodeFromGroupsContext) -> Self {
        let group_handles: Vec<PyGroupHandle> = context.group_handles.deep_into();

        Self {
            group_handles: group_handles
                .into_py_any(py)
                .expect("groups should be creatable"),
            node_handle: PyNodeHandle::from(context.node_handle)
                .into_py_any(py)
                .expect("PyNodeIndex should be creatable"),
        }
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn extract(self, py: Python<'_>) -> PreRemoveNodeFromGroupsContext {
        let group_handles: Vec<PyGroupHandle> = self
            .group_handles
            .extract(py)
            .expect("groups should be extractable");

        let node_handle: PyNodeHandle = self
            .node_handle
            .extract(py)
            .expect("PyNodeIndex should be extractable");

        PreRemoveNodeFromGroupsContext {
            group_handles: group_handles.deep_into(),
            node_handle: node_handle.into(),
        }
    }
}

#[pymethods]
impl PyPreRemoveNodeFromGroupsContext {
    #[new]
    pub const fn new(group_handles: Py<PyAny>, node_handle: Py<PyAny>) -> Self {
        Self {
            group_handles,
            node_handle,
        }
    }

    #[getter]
    pub fn group_handles(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handles.clone_ref(py)
    }

    #[getter]
    pub fn node_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.node_handle.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPostRemoveNodeFromGroupsContext {
    group_handles: Py<PyAny>,
    node_handle: Py<PyAny>,
}

impl Clone for PyPostRemoveNodeFromGroupsContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            group_handles: self.group_handles.clone_ref(py),
            node_handle: self.node_handle.clone_ref(py),
        })
    }
}

impl PyPostRemoveNodeFromGroupsContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PostRemoveNodeFromGroupsContext) -> Self {
        let group_handles: Vec<PyGroupHandle> = context.group_handles.deep_into();

        Self {
            group_handles: group_handles
                .into_py_any(py)
                .expect("groups should be creatable"),
            node_handle: PyNodeHandle::from(context.node_handle)
                .into_py_any(py)
                .expect("PyNodeIndex should be creatable"),
        }
    }
}

#[pymethods]
impl PyPostRemoveNodeFromGroupsContext {
    #[new]
    pub const fn new(group_handles: Py<PyAny>, node_handle: Py<PyAny>) -> Self {
        Self {
            group_handles,
            node_handle,
        }
    }

    #[getter]
    pub fn group_handles(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handles.clone_ref(py)
    }

    #[getter]
    pub fn node_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.node_handle.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPreRemoveNodesFromGroupsContext {
    group_handles: Py<PyAny>,
    node_handles: Py<PyAny>,
}

impl Clone for PyPreRemoveNodesFromGroupsContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            group_handles: self.group_handles.clone_ref(py),
            node_handles: self.node_handles.clone_ref(py),
        })
    }
}

impl PyPreRemoveNodesFromGroupsContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PreRemoveNodesFromGroupsContext) -> Self {
        let group_handles: Vec<PyGroupHandle> = context.group_handles.deep_into();
        let node_handles: Vec<PyNodeHandle> = context.node_handles.deep_into();

        Self {
            group_handles: group_handles
                .into_py_any(py)
                .expect("groups should be creatable"),
            node_handles: node_handles
                .into_py_any(py)
                .expect("node_indices should be creatable"),
        }
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn extract(self, py: Python<'_>) -> PreRemoveNodesFromGroupsContext {
        let group_handles: Vec<PyGroupHandle> = self
            .group_handles
            .extract(py)
            .expect("groups should be extractable");

        let node_handles: Vec<PyNodeHandle> = self
            .node_handles
            .extract(py)
            .expect("node_indices should be extractable");

        PreRemoveNodesFromGroupsContext {
            group_handles: group_handles.deep_into(),
            node_handles: node_handles.deep_into(),
        }
    }
}

#[pymethods]
impl PyPreRemoveNodesFromGroupsContext {
    #[new]
    pub const fn new(group_handles: Py<PyAny>, node_handles: Py<PyAny>) -> Self {
        Self {
            group_handles,
            node_handles,
        }
    }

    #[getter]
    pub fn group_handles(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handles.clone_ref(py)
    }

    #[getter]
    pub fn node_handles(&self, py: Python<'_>) -> Py<PyAny> {
        self.node_handles.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPostRemoveNodesFromGroupsContext {
    group_handles: Py<PyAny>,
    node_handles: Py<PyAny>,
}

impl Clone for PyPostRemoveNodesFromGroupsContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            group_handles: self.group_handles.clone_ref(py),
            node_handles: self.node_handles.clone_ref(py),
        })
    }
}

impl PyPostRemoveNodesFromGroupsContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PostRemoveNodesFromGroupsContext) -> Self {
        let group_handles: Vec<PyGroupHandle> = context.group_handles.deep_into();
        let node_handles: Vec<PyNodeHandle> = context.node_handles.deep_into();

        Self {
            group_handles: group_handles
                .into_py_any(py)
                .expect("groups should be creatable"),
            node_handles: node_handles
                .into_py_any(py)
                .expect("node_indices should be creatable"),
        }
    }
}

#[pymethods]
impl PyPostRemoveNodesFromGroupsContext {
    #[new]
    pub const fn new(group_handles: Py<PyAny>, node_handles: Py<PyAny>) -> Self {
        Self {
            group_handles,
            node_handles,
        }
    }

    #[getter]
    pub fn group_handles(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handles.clone_ref(py)
    }

    #[getter]
    pub fn node_handles(&self, py: Python<'_>) -> Py<PyAny> {
        self.node_handles.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPreRemoveEdgeFromGroupContext {
    group_handle: Py<PyAny>,
    edge_index: Py<PyAny>,
}

impl Clone for PyPreRemoveEdgeFromGroupContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            group_handle: self.group_handle.clone_ref(py),
            edge_index: self.edge_index.clone_ref(py),
        })
    }
}

impl PyPreRemoveEdgeFromGroupContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PreRemoveEdgeFromGroupContext) -> Self {
        Self {
            group_handle: PyGroupHandle::from(context.group_handle)
                .into_py_any(py)
                .expect("PyGroup should be creatable"),
            edge_index: context
                .edge_index
                .into_py_any(py)
                .expect("edge_index should be creatable"),
        }
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn extract(self, py: Python<'_>) -> PreRemoveEdgeFromGroupContext {
        let group_handle: PyGroupHandle = self
            .group_handle
            .extract(py)
            .expect("PyGroup should be extractable");

        PreRemoveEdgeFromGroupContext {
            group_handle: group_handle.into(),
            edge_index: self
                .edge_index
                .extract(py)
                .expect("edge_index should be extractable"),
        }
    }
}

#[pymethods]
impl PyPreRemoveEdgeFromGroupContext {
    #[new]
    pub const fn new(group_handle: Py<PyAny>, edge_index: Py<PyAny>) -> Self {
        Self {
            group_handle,
            edge_index,
        }
    }

    #[getter]
    pub fn group_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handle.clone_ref(py)
    }

    #[getter]
    pub fn edge_index(&self, py: Python<'_>) -> Py<PyAny> {
        self.edge_index.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPostRemoveEdgeFromGroupContext {
    group_handle: Py<PyAny>,
    edge_index: Py<PyAny>,
}

impl Clone for PyPostRemoveEdgeFromGroupContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            group_handle: self.group_handle.clone_ref(py),
            edge_index: self.edge_index.clone_ref(py),
        })
    }
}

impl PyPostRemoveEdgeFromGroupContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PostRemoveEdgeFromGroupContext) -> Self {
        Self {
            group_handle: PyGroupHandle::from(context.group_handle)
                .into_py_any(py)
                .expect("PyGroup should be creatable"),
            edge_index: context
                .edge_index
                .into_py_any(py)
                .expect("edge_index should be creatable"),
        }
    }
}

#[pymethods]
impl PyPostRemoveEdgeFromGroupContext {
    #[new]
    pub const fn new(group_handle: Py<PyAny>, edge_index: Py<PyAny>) -> Self {
        Self {
            group_handle,
            edge_index,
        }
    }

    #[getter]
    pub fn group_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handle.clone_ref(py)
    }

    #[getter]
    pub fn edge_index(&self, py: Python<'_>) -> Py<PyAny> {
        self.edge_index.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPreRemoveEdgeFromGroupsContext {
    group_handles: Py<PyAny>,
    edge_index: Py<PyAny>,
}

impl Clone for PyPreRemoveEdgeFromGroupsContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            group_handles: self.group_handles.clone_ref(py),
            edge_index: self.edge_index.clone_ref(py),
        })
    }
}

impl PyPreRemoveEdgeFromGroupsContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PreRemoveEdgeFromGroupsContext) -> Self {
        let group_handles: Vec<PyGroupHandle> = context.group_handles.deep_into();

        Self {
            group_handles: group_handles
                .into_py_any(py)
                .expect("groups should be creatable"),
            edge_index: context
                .edge_index
                .into_py_any(py)
                .expect("edge_index should be creatable"),
        }
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn extract(self, py: Python<'_>) -> PreRemoveEdgeFromGroupsContext {
        let group_handles: Vec<PyGroupHandle> = self
            .group_handles
            .extract(py)
            .expect("groups should be extractable");

        PreRemoveEdgeFromGroupsContext {
            group_handles: group_handles.deep_into(),
            edge_index: self
                .edge_index
                .extract(py)
                .expect("edge_index should be extractable"),
        }
    }
}

#[pymethods]
impl PyPreRemoveEdgeFromGroupsContext {
    #[new]
    pub const fn new(group_handles: Py<PyAny>, edge_index: Py<PyAny>) -> Self {
        Self {
            group_handles,
            edge_index,
        }
    }

    #[getter]
    pub fn group_handles(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handles.clone_ref(py)
    }

    #[getter]
    pub fn edge_index(&self, py: Python<'_>) -> Py<PyAny> {
        self.edge_index.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPostRemoveEdgeFromGroupsContext {
    group_handles: Py<PyAny>,
    edge_index: Py<PyAny>,
}

impl Clone for PyPostRemoveEdgeFromGroupsContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            group_handles: self.group_handles.clone_ref(py),
            edge_index: self.edge_index.clone_ref(py),
        })
    }
}

impl PyPostRemoveEdgeFromGroupsContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PostRemoveEdgeFromGroupsContext) -> Self {
        let group_handles: Vec<PyGroupHandle> = context.group_handles.deep_into();

        Self {
            group_handles: group_handles
                .into_py_any(py)
                .expect("groups should be creatable"),
            edge_index: context
                .edge_index
                .into_py_any(py)
                .expect("edge_index should be creatable"),
        }
    }
}

#[pymethods]
impl PyPostRemoveEdgeFromGroupsContext {
    #[new]
    pub const fn new(group_handles: Py<PyAny>, edge_index: Py<PyAny>) -> Self {
        Self {
            group_handles,
            edge_index,
        }
    }

    #[getter]
    pub fn group_handles(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handles.clone_ref(py)
    }

    #[getter]
    pub fn edge_index(&self, py: Python<'_>) -> Py<PyAny> {
        self.edge_index.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPreRemoveEdgesFromGroupsContext {
    group_handles: Py<PyAny>,
    edge_indices: Py<PyAny>,
}

impl Clone for PyPreRemoveEdgesFromGroupsContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            group_handles: self.group_handles.clone_ref(py),
            edge_indices: self.edge_indices.clone_ref(py),
        })
    }
}

impl PyPreRemoveEdgesFromGroupsContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PreRemoveEdgesFromGroupsContext) -> Self {
        let group_handles: Vec<PyGroupHandle> = context.group_handles.deep_into();

        Self {
            group_handles: group_handles
                .into_py_any(py)
                .expect("groups should be creatable"),
            edge_indices: context
                .edge_indices
                .into_py_any(py)
                .expect("edge_indices should be creatable"),
        }
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn extract(self, py: Python<'_>) -> PreRemoveEdgesFromGroupsContext {
        let group_handles: Vec<PyGroupHandle> = self
            .group_handles
            .extract(py)
            .expect("groups should be extractable");

        PreRemoveEdgesFromGroupsContext {
            group_handles: group_handles.deep_into(),
            edge_indices: self
                .edge_indices
                .extract(py)
                .expect("edge_indices should be extractable"),
        }
    }
}

#[pymethods]
impl PyPreRemoveEdgesFromGroupsContext {
    #[new]
    pub const fn new(group_handles: Py<PyAny>, edge_indices: Py<PyAny>) -> Self {
        Self {
            group_handles,
            edge_indices,
        }
    }

    #[getter]
    pub fn group_handles(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handles.clone_ref(py)
    }

    #[getter]
    pub fn edge_indices(&self, py: Python<'_>) -> Py<PyAny> {
        self.edge_indices.clone_ref(py)
    }
}

#[pyclass(frozen)]
#[derive(Debug)]
pub struct PyPostRemoveEdgesFromGroupsContext {
    group_handles: Py<PyAny>,
    edge_indices: Py<PyAny>,
}

impl Clone for PyPostRemoveEdgesFromGroupsContext {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            group_handles: self.group_handles.clone_ref(py),
            edge_indices: self.edge_indices.clone_ref(py),
        })
    }
}

impl PyPostRemoveEdgesFromGroupsContext {
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn bind(py: Python<'_>, context: PostRemoveEdgesFromGroupsContext) -> Self {
        let group_handles: Vec<PyGroupHandle> = context.group_handles.deep_into();

        Self {
            group_handles: group_handles
                .into_py_any(py)
                .expect("groups should be creatable"),
            edge_indices: context
                .edge_indices
                .into_py_any(py)
                .expect("edge_indices should be creatable"),
        }
    }
}

#[pymethods]
impl PyPostRemoveEdgesFromGroupsContext {
    #[new]
    pub const fn new(group_handles: Py<PyAny>, edge_indices: Py<PyAny>) -> Self {
        Self {
            group_handles,
            edge_indices,
        }
    }

    #[getter]
    pub fn group_handles(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_handles.clone_ref(py)
    }

    #[getter]
    pub fn edge_indices(&self, py: Python<'_>) -> Py<PyAny> {
        self.edge_indices.clone_ref(py)
    }
}

#[typetag::serde]
impl Plugin for PyPlugin {
    fn clone_box(&self) -> Box<dyn Plugin> {
        Python::attach(|py| Box::new(Self(self.0.clone_ref(py))))
    }

    fn initialize(&self, graphrecord: &mut GraphRecord) -> GraphRecordResult<()> {
        Python::attach(|py| {
            PyGraphRecord::scope_mut(py, graphrecord, |py, graphrecord| {
                self.0
                    .call_method1(py, "initialize", (graphrecord,))
                    .map_err(|err| GraphRecordError::ConversionError(format!("{err}")))?;

                Ok(())
            })
        })
    }

    fn finalize(&self, graphrecord: &mut GraphRecord) -> GraphRecordResult<()> {
        Python::attach(|py| {
            PyGraphRecord::scope_mut(py, graphrecord, |py, graphrecord| {
                self.0
                    .call_method1(py, "finalize", (graphrecord,))
                    .map_err(|err| GraphRecordError::ConversionError(format!("{err}")))?;

                Ok(())
            })
        })
    }

    impl_pre_hook!(pre_set_schema, PyPreSetSchemaContext, PreSetSchemaContext);
    impl_post_hook!(post_set_schema);
    impl_post_hook!(pre_freeze_schema);
    impl_post_hook!(post_freeze_schema);
    impl_post_hook!(pre_unfreeze_schema);
    impl_post_hook!(post_unfreeze_schema);
    impl_pre_hook!(pre_add_node, PyPreAddNodeContext, PreAddNodeContext);
    impl_post_hook!(post_add_node, PyPostAddNodeContext, PostAddNodeContext);
    impl_pre_hook!(
        pre_add_node_with_group,
        PyPreAddNodeWithGroupContext,
        PreAddNodeWithGroupContext
    );
    impl_post_hook!(
        post_add_node_with_group,
        PyPostAddNodeWithGroupContext,
        PostAddNodeWithGroupContext
    );
    impl_pre_hook!(
        pre_add_node_with_groups,
        PyPreAddNodeWithGroupsContext,
        PreAddNodeWithGroupsContext
    );
    impl_post_hook!(
        post_add_node_with_groups,
        PyPostAddNodeWithGroupsContext,
        PostAddNodeWithGroupsContext
    );
    impl_pre_hook!(
        pre_remove_node,
        PyPreRemoveNodeContext,
        PreRemoveNodeContext
    );
    impl_post_hook!(
        post_remove_node,
        PyPostRemoveNodeContext,
        PostRemoveNodeContext
    );
    impl_pre_hook!(pre_add_nodes, PyPreAddNodesContext, PreAddNodesContext);
    impl_post_hook!(post_add_nodes, PyPostAddNodesContext, PostAddNodesContext);
    impl_pre_hook!(
        pre_add_nodes_with_group,
        PyPreAddNodesWithGroupContext,
        PreAddNodesWithGroupContext
    );
    impl_post_hook!(
        post_add_nodes_with_group,
        PyPostAddNodesWithGroupContext,
        PostAddNodesWithGroupContext
    );
    impl_pre_hook!(
        pre_add_nodes_with_groups,
        PyPreAddNodesWithGroupsContext,
        PreAddNodesWithGroupsContext
    );
    impl_post_hook!(
        post_add_nodes_with_groups,
        PyPostAddNodesWithGroupsContext,
        PostAddNodesWithGroupsContext
    );
    impl_pre_hook!(
        pre_add_nodes_dataframes,
        PyPreAddNodesDataframesContext,
        PreAddNodesDataframesContext
    );
    impl_post_hook!(
        post_add_nodes_dataframes,
        PyPostAddNodesDataframesContext,
        PostAddNodesDataframesContext
    );
    impl_pre_hook!(
        pre_add_nodes_dataframes_with_group,
        PyPreAddNodesDataframesWithGroupContext,
        PreAddNodesDataframesWithGroupContext
    );
    impl_post_hook!(
        post_add_nodes_dataframes_with_group,
        PyPostAddNodesDataframesWithGroupContext,
        PostAddNodesDataframesWithGroupContext
    );
    impl_pre_hook!(
        pre_add_nodes_dataframes_with_groups,
        PyPreAddNodesDataframesWithGroupsContext,
        PreAddNodesDataframesWithGroupsContext
    );
    impl_post_hook!(
        post_add_nodes_dataframes_with_groups,
        PyPostAddNodesDataframesWithGroupsContext,
        PostAddNodesDataframesWithGroupsContext
    );
    impl_pre_hook!(pre_add_edge, PyPreAddEdgeContext, PreAddEdgeContext);
    impl_post_hook!(post_add_edge, PyPostAddEdgeContext, PostAddEdgeContext);
    impl_pre_hook!(
        pre_add_edge_with_group,
        PyPreAddEdgeWithGroupContext,
        PreAddEdgeWithGroupContext
    );
    impl_post_hook!(
        post_add_edge_with_group,
        PyPostAddEdgeWithGroupContext,
        PostAddEdgeWithGroupContext
    );
    impl_pre_hook!(
        pre_add_edge_with_groups,
        PyPreAddEdgeWithGroupsContext,
        PreAddEdgeWithGroupsContext
    );
    impl_post_hook!(
        post_add_edge_with_groups,
        PyPostAddEdgeWithGroupsContext,
        PostAddEdgeWithGroupsContext
    );
    impl_pre_hook!(
        pre_remove_edge,
        PyPreRemoveEdgeContext,
        PreRemoveEdgeContext
    );
    impl_post_hook!(
        post_remove_edge,
        PyPostRemoveEdgeContext,
        PostRemoveEdgeContext
    );
    impl_pre_hook!(pre_add_edges, PyPreAddEdgesContext, PreAddEdgesContext);
    impl_post_hook!(post_add_edges, PyPostAddEdgesContext, PostAddEdgesContext);
    impl_pre_hook!(
        pre_add_edges_with_group,
        PyPreAddEdgesWithGroupContext,
        PreAddEdgesWithGroupContext
    );
    impl_post_hook!(
        post_add_edges_with_group,
        PyPostAddEdgesWithGroupContext,
        PostAddEdgesWithGroupContext
    );
    impl_pre_hook!(
        pre_add_edges_with_groups,
        PyPreAddEdgesWithGroupsContext,
        PreAddEdgesWithGroupsContext
    );
    impl_post_hook!(
        post_add_edges_with_groups,
        PyPostAddEdgesWithGroupsContext,
        PostAddEdgesWithGroupsContext
    );
    impl_pre_hook!(
        pre_add_edges_dataframes,
        PyPreAddEdgesDataframesContext,
        PreAddEdgesDataframesContext
    );
    impl_post_hook!(
        post_add_edges_dataframes,
        PyPostAddEdgesDataframesContext,
        PostAddEdgesDataframesContext
    );
    impl_pre_hook!(
        pre_add_edges_dataframes_with_group,
        PyPreAddEdgesDataframesWithGroupContext,
        PreAddEdgesDataframesWithGroupContext
    );
    impl_post_hook!(
        post_add_edges_dataframes_with_group,
        PyPostAddEdgesDataframesWithGroupContext,
        PostAddEdgesDataframesWithGroupContext
    );
    impl_pre_hook!(
        pre_add_edges_dataframes_with_groups,
        PyPreAddEdgesDataframesWithGroupsContext,
        PreAddEdgesDataframesWithGroupsContext
    );
    impl_post_hook!(
        post_add_edges_dataframes_with_groups,
        PyPostAddEdgesDataframesWithGroupsContext,
        PostAddEdgesDataframesWithGroupsContext
    );
    impl_pre_hook!(pre_add_group, PyPreAddGroupContext, PreAddGroupContext);
    impl_post_hook!(post_add_group, PyPostAddGroupContext, PostAddGroupContext);
    impl_pre_hook!(
        pre_remove_group,
        PyPreRemoveGroupContext,
        PreRemoveGroupContext
    );
    impl_post_hook!(
        post_remove_group,
        PyPostRemoveGroupContext,
        PostRemoveGroupContext
    );
    impl_pre_hook!(
        pre_add_node_to_group,
        PyPreAddNodeToGroupContext,
        PreAddNodeToGroupContext
    );
    impl_post_hook!(
        post_add_node_to_group,
        PyPostAddNodeToGroupContext,
        PostAddNodeToGroupContext
    );
    impl_pre_hook!(
        pre_add_node_to_groups,
        PyPreAddNodeToGroupsContext,
        PreAddNodeToGroupsContext
    );
    impl_post_hook!(
        post_add_node_to_groups,
        PyPostAddNodeToGroupsContext,
        PostAddNodeToGroupsContext
    );
    impl_pre_hook!(
        pre_add_nodes_to_groups,
        PyPreAddNodesToGroupsContext,
        PreAddNodesToGroupsContext
    );
    impl_post_hook!(
        post_add_nodes_to_groups,
        PyPostAddNodesToGroupsContext,
        PostAddNodesToGroupsContext
    );
    impl_pre_hook!(
        pre_add_edge_to_group,
        PyPreAddEdgeToGroupContext,
        PreAddEdgeToGroupContext
    );
    impl_post_hook!(
        post_add_edge_to_group,
        PyPostAddEdgeToGroupContext,
        PostAddEdgeToGroupContext
    );
    impl_pre_hook!(
        pre_add_edge_to_groups,
        PyPreAddEdgeToGroupsContext,
        PreAddEdgeToGroupsContext
    );
    impl_post_hook!(
        post_add_edge_to_groups,
        PyPostAddEdgeToGroupsContext,
        PostAddEdgeToGroupsContext
    );
    impl_pre_hook!(
        pre_add_edges_to_groups,
        PyPreAddEdgesToGroupsContext,
        PreAddEdgesToGroupsContext
    );
    impl_post_hook!(
        post_add_edges_to_groups,
        PyPostAddEdgesToGroupsContext,
        PostAddEdgesToGroupsContext
    );
    impl_pre_hook!(
        pre_remove_node_from_group,
        PyPreRemoveNodeFromGroupContext,
        PreRemoveNodeFromGroupContext
    );
    impl_post_hook!(
        post_remove_node_from_group,
        PyPostRemoveNodeFromGroupContext,
        PostRemoveNodeFromGroupContext
    );
    impl_pre_hook!(
        pre_remove_node_from_groups,
        PyPreRemoveNodeFromGroupsContext,
        PreRemoveNodeFromGroupsContext
    );
    impl_post_hook!(
        post_remove_node_from_groups,
        PyPostRemoveNodeFromGroupsContext,
        PostRemoveNodeFromGroupsContext
    );
    impl_pre_hook!(
        pre_remove_nodes_from_groups,
        PyPreRemoveNodesFromGroupsContext,
        PreRemoveNodesFromGroupsContext
    );
    impl_post_hook!(
        post_remove_nodes_from_groups,
        PyPostRemoveNodesFromGroupsContext,
        PostRemoveNodesFromGroupsContext
    );
    impl_pre_hook!(
        pre_remove_edge_from_group,
        PyPreRemoveEdgeFromGroupContext,
        PreRemoveEdgeFromGroupContext
    );
    impl_post_hook!(
        post_remove_edge_from_group,
        PyPostRemoveEdgeFromGroupContext,
        PostRemoveEdgeFromGroupContext
    );
    impl_pre_hook!(
        pre_remove_edge_from_groups,
        PyPreRemoveEdgeFromGroupsContext,
        PreRemoveEdgeFromGroupsContext
    );
    impl_post_hook!(
        post_remove_edge_from_groups,
        PyPostRemoveEdgeFromGroupsContext,
        PostRemoveEdgeFromGroupsContext
    );
    impl_pre_hook!(
        pre_remove_edges_from_groups,
        PyPreRemoveEdgesFromGroupsContext,
        PreRemoveEdgesFromGroupsContext
    );
    impl_post_hook!(
        post_remove_edges_from_groups,
        PyPostRemoveEdgesFromGroupsContext,
        PostRemoveEdgesFromGroupsContext
    );
    impl_post_hook!(pre_clear);
    impl_post_hook!(post_clear);
}
