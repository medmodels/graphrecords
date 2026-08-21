use crate::{
    graphrecord::{PyAttributeName, PyEdgeIndex, PyGroupIndex, PyNodeIndex, value::PyValue},
    querying::{endpoint::PyEdgeEndpointRole, failure_kind::PyFailureKind},
};
use graphrecords_core::graphrecord::{AttributeName, EdgeIndex, GroupIndex, NodeIndex, Value};
use graphrecords_query::{
    FailureKind,
    dynamic::DynIndexOwned,
    index::{EdgeEndpointRole, Position, Positional},
    registry::IndexDescriptor,
};
use pyo3::{exceptions::PyTypeError, prelude::*};

pub(super) trait IndexConversion: Sized {
    fn from_python(object: &Bound<'_, PyAny>, descriptor: &IndexDescriptor) -> PyResult<Self>;

    fn to_python(&self, py: Python<'_>) -> PyResult<Py<PyAny>>;
}

impl IndexConversion for DynIndexOwned {
    fn from_python(object: &Bound<'_, PyAny>, descriptor: &IndexDescriptor) -> PyResult<Self> {
        let IndexDescriptor::Domain(domain) = descriptor else {
            return Err(PyTypeError::new_err(
                "an expanded index has no literal form",
            ));
        };

        if domain.is::<Positional>() {
            return object.extract::<Position>().map(Self::Positional);
        }
        if domain.is::<NodeIndex>() {
            return Ok(Self::Node(object.extract::<PyNodeIndex>()?.into()));
        }
        if domain.is::<EdgeIndex>() {
            return Ok(Self::Edge(object.extract::<PyEdgeIndex>()?.into()));
        }
        if domain.is::<GroupIndex>() {
            return Ok(Self::Group(object.extract::<PyGroupIndex>()?.into()));
        }
        if domain.is::<AttributeName>() {
            return Ok(Self::Attribute(object.extract::<PyAttributeName>()?.into()));
        }
        if domain.is::<Value>() {
            return Ok(Self::Value(object.extract::<PyValue>()?.into()));
        }
        if domain.is::<bool>() {
            return object.extract().map(Self::Bool);
        }
        if domain.is::<EdgeEndpointRole>() {
            return Ok(Self::EndpointRole(
                object.extract::<PyEdgeEndpointRole>()?.into(),
            ));
        }
        if domain.is::<FailureKind>() {
            return Ok(Self::FailureKind(object.extract::<PyFailureKind>()?.into()));
        }

        Err(PyTypeError::new_err(format!(
            "index domain `{}` has no literal form",
            domain.name()
        )))
    }

    fn to_python(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self {
            Self::Positional(position) => Ok(position.into_pyobject(py)?.into_any().unbind()),
            Self::Node(index) => Ok(PyNodeIndex::from(index.clone()).into_pyobject(py)?.unbind()),
            Self::Edge(index) => Ok(PyEdgeIndex::from(*index)
                .into_pyobject(py)?
                .into_any()
                .unbind()),
            Self::Group(index) => Ok(PyGroupIndex::from(index.clone())
                .into_pyobject(py)?
                .unbind()),
            Self::Attribute(index) => Ok(PyAttributeName::from(index.clone())
                .into_pyobject(py)?
                .unbind()),
            Self::Value(value) => Ok(PyValue::from(value.clone()).into_pyobject(py)?.unbind()),
            Self::Bool(value) => Ok(value.into_pyobject(py)?.to_owned().into_any().unbind()),
            Self::EndpointRole(role) => Ok(PyEdgeEndpointRole::from(*role)
                .into_pyobject(py)?
                .into_any()
                .unbind()),
            Self::FailureKind(kind) => Ok(PyFailureKind::from(*kind)
                .into_pyobject(py)?
                .into_any()
                .unbind()),
            Self::Expanded(expanded) => {
                let parent = expanded.parent_index().to_python(py)?;
                let child = match expanded.child_index() {
                    Some(child) => child.to_python(py)?,
                    None => py.None(),
                };

                Ok((parent, child).into_pyobject(py)?.into_any().unbind())
            }
        }
    }
}
