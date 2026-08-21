use crate::{
    graphrecord::{PyAttributeName, PyEdgeIndex, PyGroupIndex, PyNodeIndex, value::PyValue},
    querying::{
        exception::FailureConversion, failure_kind::PyFailureKind,
        index_conversion::IndexConversion,
    },
};
use graphrecords_core::graphrecord::AttributeName;
use graphrecords_query::{
    FailureKindValue, Scalar,
    dynamic::{DynEntityReferenceKind, DynIndexOwned, DynValue},
    registry::{ValueDescriptor, ValueRole},
};
use pyo3::{exceptions::PyTypeError, prelude::*};

pub(super) trait ValueConversion: Sized {
    fn from_python(object: &Bound<'_, PyAny>, descriptor: &ValueDescriptor) -> PyResult<Self>;

    fn to_python(&self, py: Python<'_>) -> PyResult<Py<PyAny>>;
}

impl ValueConversion for DynValue {
    fn from_python(object: &Bound<'_, PyAny>, descriptor: &ValueDescriptor) -> PyResult<Self> {
        match descriptor.role() {
            ValueRole::Value => {
                if descriptor.domain().is::<Scalar>() {
                    return Ok(Self::Scalar(object.extract::<PyValue>()?.into()));
                }
                if descriptor.domain().is::<AttributeName>() {
                    return Ok(Self::Attribute(object.extract::<PyAttributeName>()?.into()));
                }
                if descriptor.domain().is::<FailureKindValue>() {
                    return Ok(Self::FailureKind(object.extract::<PyFailureKind>()?.into()));
                }

                Err(PyTypeError::new_err(format!(
                    "value domain `{}` has no literal form",
                    descriptor.domain().name()
                )))
            }
            ValueRole::Index(index) => DynIndexOwned::from_python(object, index).map(Self::Index),
            ValueRole::EntityReference(_) => Err(PyTypeError::new_err(
                "a verified reference has no literal form; compare indices via `index()` instead",
            )),
            ValueRole::Unit => Err(PyTypeError::new_err(
                "a membership lane carries no value, so it has no literal form",
            )),
        }
    }

    fn to_python(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self {
            Self::Scalar(value) => Ok(PyValue::from(value.clone()).into_pyobject(py)?.unbind()),
            Self::Attribute(attribute) => Ok(PyAttributeName::from(attribute.clone())
                .into_pyobject(py)?
                .unbind()),
            Self::Index(index) => index.to_python(py),
            Self::EntityReference(reference) => match reference.kind() {
                DynEntityReferenceKind::Node(index) => {
                    Ok(PyNodeIndex::from(index.clone()).into_pyobject(py)?.unbind())
                }
                DynEntityReferenceKind::Edge(index) => Ok(PyEdgeIndex::from(*index)
                    .into_pyobject(py)?
                    .into_any()
                    .unbind()),
                DynEntityReferenceKind::Group(index) => Ok(PyGroupIndex::from(index.clone())
                    .into_pyobject(py)?
                    .unbind()),
            },
            Self::Failure(failure) => Ok(failure.to_python(py)),
            Self::FailureKind(kind) => Ok(PyFailureKind::from(*kind)
                .into_pyobject(py)?
                .into_any()
                .unbind()),
        }
    }
}
