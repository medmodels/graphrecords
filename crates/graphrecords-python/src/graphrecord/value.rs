use super::{Lut, traits::DeepFrom};
use crate::{conversion_lut::ConversionLut, graphrecord::errors::PyGraphRecordError};
use chrono::{NaiveDateTime, TimeDelta};
use graphrecords_core::{errors::GraphRecordError, graphrecord::GraphRecordValue};
use pyo3::{
    Borrowed, Bound, FromPyObject, IntoPyObject, IntoPyObjectExt, PyAny, PyErr, PyResult, Python,
    types::{PyAnyMethods, PyBool, PyDateTime, PyDelta, PyFloat, PyInt, PyString},
};
use std::ops::Deref;

#[repr(transparent)]
#[derive(Clone, Debug)]
pub struct PyGraphRecordValue(GraphRecordValue);

impl From<GraphRecordValue> for PyGraphRecordValue {
    fn from(value: GraphRecordValue) -> Self {
        Self(value)
    }
}

impl From<PyGraphRecordValue> for GraphRecordValue {
    fn from(value: PyGraphRecordValue) -> Self {
        value.0
    }
}

impl DeepFrom<PyGraphRecordValue> for GraphRecordValue {
    fn deep_from(value: PyGraphRecordValue) -> Self {
        value.into()
    }
}

impl DeepFrom<GraphRecordValue> for PyGraphRecordValue {
    fn deep_from(value: GraphRecordValue) -> Self {
        value.into()
    }
}

impl Deref for PyGraphRecordValue {
    type Target = GraphRecordValue;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

static GRAPHRECORDVALUE_CONVERSION_LUT: Lut<GraphRecordValue> = ConversionLut::new();

#[allow(clippy::unnecessary_wraps)]
pub(crate) fn convert_pyobject_to_graphrecordvalue(
    ob: &Bound<'_, PyAny>,
) -> PyResult<GraphRecordValue> {
    fn convert_string(ob: &Bound<'_, PyAny>) -> PyResult<GraphRecordValue> {
        Ok(GraphRecordValue::String(
            ob.extract::<String>().expect("Extraction must succeed"),
        ))
    }

    fn convert_int(ob: &Bound<'_, PyAny>) -> PyResult<GraphRecordValue> {
        Ok(GraphRecordValue::Int(
            ob.extract::<i64>().expect("Extraction must succeed"),
        ))
    }

    fn convert_float(ob: &Bound<'_, PyAny>) -> PyResult<GraphRecordValue> {
        Ok(GraphRecordValue::Float(
            ob.extract::<f64>().expect("Extraction must succeed"),
        ))
    }

    fn convert_bool(ob: &Bound<'_, PyAny>) -> PyResult<GraphRecordValue> {
        Ok(GraphRecordValue::Bool(
            ob.extract::<bool>().expect("Extraction must succeed"),
        ))
    }

    fn convert_datetime(ob: &Bound<'_, PyAny>) -> PyResult<GraphRecordValue> {
        Ok(GraphRecordValue::DateTime(
            ob.extract::<NaiveDateTime>()
                .expect("Extraction must succeed"),
        ))
    }

    fn convert_duration(ob: &Bound<'_, PyAny>) -> PyResult<GraphRecordValue> {
        Ok(GraphRecordValue::Duration(
            ob.extract::<TimeDelta>().expect("Extraction must succeed"),
        ))
    }

    const fn convert_null(_ob: &Bound<'_, PyAny>) -> PyResult<GraphRecordValue> {
        Ok(GraphRecordValue::Null)
    }

    fn throw_error(ob: &Bound<'_, PyAny>) -> PyResult<GraphRecordValue> {
        Err(
            PyGraphRecordError::from(GraphRecordError::ConversionError(format!(
                "Failed to convert {ob} into GraphRecordValue",
            )))
            .into(),
        )
    }

    let type_pointer = ob.get_type_ptr() as usize;

    let conversion_function = GRAPHRECORDVALUE_CONVERSION_LUT.get_or_insert(type_pointer, || {
        if ob.is_instance_of::<PyString>() {
            convert_string
        } else if ob.is_instance_of::<PyBool>() {
            convert_bool
        } else if ob.is_instance_of::<PyInt>() {
            convert_int
        } else if ob.is_instance_of::<PyFloat>() {
            convert_float
        } else if ob.is_instance_of::<PyDateTime>() {
            convert_datetime
        } else if ob.is_instance_of::<PyDelta>() {
            convert_duration
        } else if ob.is_none() {
            convert_null
        } else {
            throw_error
        }
    });

    conversion_function(ob)
}

impl FromPyObject<'_, '_> for PyGraphRecordValue {
    type Error = PyErr;

    fn extract(ob: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        convert_pyobject_to_graphrecordvalue(&ob).map(Self::from)
    }
}

impl<'py> IntoPyObject<'py> for PyGraphRecordValue {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self.0 {
            GraphRecordValue::String(value) => value.into_bound_py_any(py),
            GraphRecordValue::Int(value) => value.into_bound_py_any(py),
            GraphRecordValue::Float(value) => value.into_bound_py_any(py),
            GraphRecordValue::Bool(value) => value.into_bound_py_any(py),
            GraphRecordValue::DateTime(value) => value.into_bound_py_any(py),
            GraphRecordValue::Duration(value) => value.into_bound_py_any(py),
            GraphRecordValue::Null => py.None().into_bound_py_any(py),
        }
    }
}
