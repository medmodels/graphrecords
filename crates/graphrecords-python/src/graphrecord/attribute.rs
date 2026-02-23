use super::{traits::DeepFrom, value::convert_pyobject_to_graphrecordvalue};
use crate::graphrecord::errors::PyGraphRecordError;
use graphrecords_core::graphrecord::GraphRecordAttribute;
use pyo3::{
    Borrowed, Bound, FromPyObject, IntoPyObject, IntoPyObjectExt, PyAny, PyErr, PyResult, Python,
};
use std::{hash::Hash, ops::Deref};

#[repr(transparent)]
#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct PyGraphRecordAttribute(GraphRecordAttribute);

impl From<GraphRecordAttribute> for PyGraphRecordAttribute {
    fn from(value: GraphRecordAttribute) -> Self {
        Self(value)
    }
}

impl From<PyGraphRecordAttribute> for GraphRecordAttribute {
    fn from(value: PyGraphRecordAttribute) -> Self {
        value.0
    }
}

impl DeepFrom<PyGraphRecordAttribute> for GraphRecordAttribute {
    fn deep_from(value: PyGraphRecordAttribute) -> Self {
        value.into()
    }
}

impl DeepFrom<GraphRecordAttribute> for PyGraphRecordAttribute {
    fn deep_from(value: GraphRecordAttribute) -> Self {
        value.into()
    }
}

impl Deref for PyGraphRecordAttribute {
    type Target = GraphRecordAttribute;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub(crate) fn convert_pyobject_to_graphrecordattribute(
    ob: &Bound<'_, PyAny>,
) -> PyResult<GraphRecordAttribute> {
    Ok(convert_pyobject_to_graphrecordvalue(ob)?
        .try_into()
        .map_err(PyGraphRecordError::from)?)
}

impl FromPyObject<'_, '_> for PyGraphRecordAttribute {
    type Error = PyErr;

    fn extract(ob: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        convert_pyobject_to_graphrecordattribute(&ob).map(Self::from)
    }
}

impl<'py> IntoPyObject<'py> for PyGraphRecordAttribute {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self.0 {
            GraphRecordAttribute::String(value) => value.into_bound_py_any(py),
            GraphRecordAttribute::Int(value) => value.into_bound_py_any(py),
        }
    }
}
