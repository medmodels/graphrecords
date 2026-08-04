use crate::{
    graphrecord::{attribute::PyGraphRecordAttribute, value::PyGraphRecordValue},
    querying::{endpoint::PyEdgeEndpointRole, failure_kind::PyFailureKind},
};
use graphrecords_core::graphrecord::{EdgeIndex, GraphRecordValue, NodeIndex};
use graphrecords_query::{
    AttributeName, FailureKind,
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
            return Ok(Self::Node(
                object.extract::<PyGraphRecordAttribute>()?.into(),
            ));
        }
        if domain.is::<EdgeIndex>() {
            return object.extract().map(Self::Edge);
        }
        if domain.is::<AttributeName>() {
            return Ok(Self::Attribute(
                object.extract::<PyGraphRecordAttribute>()?.into(),
            ));
        }
        if domain.is::<GraphRecordValue>() {
            return Ok(Self::Value(object.extract::<PyGraphRecordValue>()?.into()));
        }
        if domain.is::<bool>() {
            return object.extract().map(Self::Bool);
        }
        if domain.is::<EdgeEndpointRole>() {
            return Err(PyTypeError::new_err(
                "an edge-endpoint role has no literal form",
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
            Self::Node(index) | Self::Attribute(index) => {
                Ok(PyGraphRecordAttribute::from(index.clone())
                    .into_pyobject(py)?
                    .unbind())
            }
            Self::Edge(index) => Ok(index.into_pyobject(py)?.into_any().unbind()),
            Self::Value(value) => Ok(PyGraphRecordValue::from(value.clone())
                .into_pyobject(py)?
                .unbind()),
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
