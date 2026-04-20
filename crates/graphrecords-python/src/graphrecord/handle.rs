use graphrecords_core::errors::GraphRecordResult;
use graphrecords_core::graphrecord::{
    AsAttributeName, AsLookup, AttributeHandle, AttributeNameKind, GraphRecord,
    GraphRecordAttribute, Group, GroupHandle, GroupKind, Handle, NodeIndex, NodeHandle,
    NodeIndexKind,
};
use pyo3::prelude::*;
use pyo3::{Borrowed, FromPyObject, IntoPyObject, IntoPyObjectExt, PyAny, PyErr, PyResult, Python};

use crate::graphrecord::attribute::PyGraphRecordAttribute;
use crate::graphrecord::traits::DeepFrom;

#[pyclass(name = "NodeHandle", frozen, eq, hash)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct PyNodeHandle(pub NodeHandle);

impl From<NodeHandle> for PyNodeHandle {
    fn from(handle: NodeHandle) -> Self {
        Self(handle)
    }
}

impl From<PyNodeHandle> for NodeHandle {
    fn from(handle: PyNodeHandle) -> Self {
        handle.0
    }
}

#[pyclass(name = "GroupHandle", frozen, eq, hash)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct PyGroupHandle(pub GroupHandle);

impl From<GroupHandle> for PyGroupHandle {
    fn from(handle: GroupHandle) -> Self {
        Self(handle)
    }
}

impl From<PyGroupHandle> for GroupHandle {
    fn from(handle: PyGroupHandle) -> Self {
        handle.0
    }
}

#[pyclass(name = "AttributeHandle", frozen, eq, hash)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct PyAttributeHandle(pub AttributeHandle);

impl From<AttributeHandle> for PyAttributeHandle {
    fn from(handle: AttributeHandle) -> Self {
        Self(handle)
    }
}

impl From<PyAttributeHandle> for AttributeHandle {
    fn from(handle: PyAttributeHandle) -> Self {
        handle.0
    }
}

impl DeepFrom<NodeHandle> for PyNodeHandle {
    fn deep_from(value: NodeHandle) -> Self {
        Self::from(value)
    }
}

impl DeepFrom<PyNodeHandle> for NodeHandle {
    fn deep_from(value: PyNodeHandle) -> Self {
        Self::from(value)
    }
}

impl DeepFrom<GroupHandle> for PyGroupHandle {
    fn deep_from(value: GroupHandle) -> Self {
        Self::from(value)
    }
}

impl DeepFrom<PyGroupHandle> for GroupHandle {
    fn deep_from(value: PyGroupHandle) -> Self {
        Self::from(value)
    }
}

impl DeepFrom<AttributeHandle> for PyAttributeHandle {
    fn deep_from(value: AttributeHandle) -> Self {
        Self::from(value)
    }
}

impl DeepFrom<PyAttributeHandle> for AttributeHandle {
    fn deep_from(value: PyAttributeHandle) -> Self {
        Self::from(value)
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum PyNodeInput {
    Handle(PyNodeHandle),
    Name(PyGraphRecordAttribute),
}

impl FromPyObject<'_, '_> for PyNodeInput {
    type Error = PyErr;

    fn extract(ob: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        if let Ok(handle) = ob.extract::<PyNodeHandle>() {
            return Ok(Self::Handle(handle));
        }

        ob.extract::<PyGraphRecordAttribute>().map(Self::Name)
    }
}

impl<'py> IntoPyObject<'py> for PyNodeInput {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            Self::Handle(handle) => handle.into_bound_py_any(py),
            Self::Name(name) => name.into_bound_py_any(py),
        }
    }
}

impl AsLookup<NodeIndexKind> for PyNodeInput {
    fn resolve(&self, graph: &GraphRecord) -> GraphRecordResult<Handle<NodeIndexKind>> {
        match self {
            Self::Handle(handle) => {
                <Handle<NodeIndexKind> as AsLookup<NodeIndexKind>>::resolve(&handle.0, graph)
            }
            Self::Name(name) => {
                let core_name: &NodeIndex = name;
                <&NodeIndex as AsLookup<NodeIndexKind>>::resolve(&core_name, graph)
            }
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum PyGroupInput {
    Handle(PyGroupHandle),
    Name(PyGraphRecordAttribute),
}

impl FromPyObject<'_, '_> for PyGroupInput {
    type Error = PyErr;

    fn extract(ob: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        if let Ok(handle) = ob.extract::<PyGroupHandle>() {
            return Ok(Self::Handle(handle));
        }

        ob.extract::<PyGraphRecordAttribute>().map(Self::Name)
    }
}

impl<'py> IntoPyObject<'py> for PyGroupInput {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            Self::Handle(handle) => handle.into_bound_py_any(py),
            Self::Name(name) => name.into_bound_py_any(py),
        }
    }
}

impl AsLookup<GroupKind> for PyGroupInput {
    fn resolve(&self, graph: &GraphRecord) -> GraphRecordResult<Handle<GroupKind>> {
        match self {
            Self::Handle(handle) => {
                <Handle<GroupKind> as AsLookup<GroupKind>>::resolve(&handle.0, graph)
            }
            Self::Name(name) => {
                let core_name: &Group = name;
                <&Group as AsLookup<GroupKind>>::resolve(&core_name, graph)
            }
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum PyAttributeInput {
    Handle(PyAttributeHandle),
    Name(PyGraphRecordAttribute),
}

impl FromPyObject<'_, '_> for PyAttributeInput {
    type Error = PyErr;

    fn extract(ob: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        if let Ok(handle) = ob.extract::<PyAttributeHandle>() {
            return Ok(Self::Handle(handle));
        }

        ob.extract::<PyGraphRecordAttribute>().map(Self::Name)
    }
}

impl<'py> IntoPyObject<'py> for PyAttributeInput {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            Self::Handle(handle) => handle.into_bound_py_any(py),
            Self::Name(name) => name.into_bound_py_any(py),
        }
    }
}

impl AsLookup<AttributeNameKind> for PyAttributeInput {
    fn resolve(&self, graph: &GraphRecord) -> GraphRecordResult<Handle<AttributeNameKind>> {
        match self {
            Self::Handle(handle) => <Handle<AttributeNameKind> as AsLookup<
                AttributeNameKind,
            >>::resolve(&handle.0, graph),
            Self::Name(name) => {
                let core_name: &GraphRecordAttribute = name;
                <&GraphRecordAttribute as AsLookup<AttributeNameKind>>::resolve(&core_name, graph)
            }
        }
    }
}

impl AsAttributeName for PyAttributeInput {
    fn as_attribute_name<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<&'a GraphRecordAttribute> {
        match self {
            Self::Handle(handle) => handle.0.as_attribute_name(graphrecord),
            Self::Name(name) => Ok(name),
        }
    }
}
