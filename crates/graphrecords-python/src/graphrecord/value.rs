use super::{Lut, traits::DeepFrom};
use crate::{conversion_lut::ConversionLut, graphrecord::errors::PyGraphRecordError};
use chrono::{NaiveDateTime, TimeDelta};
use graphrecords_core::graphrecord::Value;
use pyo3::{
    Borrowed, Bound, FromPyObject, IntoPyObject, IntoPyObjectExt, PyAny, PyErr, PyResult, Python,
    types::{PyAnyMethods, PyBool, PyDateTime, PyDelta, PyFloat, PyInt, PyString},
};
use std::ops::Deref;

#[repr(transparent)]
#[derive(Clone, Debug)]
pub struct PyValue(Value);

impl From<Value> for PyValue {
    fn from(value: Value) -> Self {
        Self(value)
    }
}

impl From<PyValue> for Value {
    fn from(value: PyValue) -> Self {
        value.0
    }
}

impl DeepFrom<PyValue> for Value {
    fn deep_from(value: PyValue) -> Self {
        value.into()
    }
}

impl DeepFrom<Value> for PyValue {
    fn deep_from(value: Value) -> Self {
        value.into()
    }
}

impl Deref for PyValue {
    type Target = Value;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

static VALUE_CONVERSION_LUT: Lut<Value> = ConversionLut::new();

#[allow(clippy::unnecessary_wraps)]
pub(crate) fn convert_pyobject_to_value(ob: &Bound<'_, PyAny>) -> PyResult<Value> {
    fn convert_string(ob: &Bound<'_, PyAny>) -> PyResult<Value> {
        Ok(Value::String(
            ob.extract::<String>().expect("Extraction must succeed"),
        ))
    }

    fn convert_int(ob: &Bound<'_, PyAny>) -> PyResult<Value> {
        Ok(Value::Int(
            ob.extract::<i64>().expect("Extraction must succeed"),
        ))
    }

    fn convert_float(ob: &Bound<'_, PyAny>) -> PyResult<Value> {
        Ok(Value::Float(
            ob.extract::<f64>().expect("Extraction must succeed"),
        ))
    }

    fn convert_bool(ob: &Bound<'_, PyAny>) -> PyResult<Value> {
        Ok(Value::Bool(
            ob.extract::<bool>().expect("Extraction must succeed"),
        ))
    }

    fn convert_datetime(ob: &Bound<'_, PyAny>) -> PyResult<Value> {
        Ok(Value::DateTime(
            ob.extract::<NaiveDateTime>()
                .expect("Extraction must succeed"),
        ))
    }

    fn convert_duration(ob: &Bound<'_, PyAny>) -> PyResult<Value> {
        Ok(Value::Duration(
            ob.extract::<TimeDelta>().expect("Extraction must succeed"),
        ))
    }

    const fn convert_null(_ob: &Bound<'_, PyAny>) -> PyResult<Value> {
        Ok(Value::Null)
    }

    fn throw_error(ob: &Bound<'_, PyAny>) -> PyResult<Value> {
        Err(PyGraphRecordError::Conversion(format!("Failed to convert {ob} into Value")).into())
    }

    let type_pointer = ob.get_type_ptr() as usize;

    let conversion_function = VALUE_CONVERSION_LUT.get_or_insert(type_pointer, || {
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

impl FromPyObject<'_, '_> for PyValue {
    type Error = PyErr;

    fn extract(ob: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        convert_pyobject_to_value(&ob).map(Self::from)
    }
}

impl<'py> IntoPyObject<'py> for PyValue {
    type Error = PyErr;
    type Output = Bound<'py, Self::Target>;
    type Target = PyAny;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self.0 {
            Value::String(value) => value.into_bound_py_any(py),
            Value::Int(value) => value.into_bound_py_any(py),
            Value::Float(value) => value.into_bound_py_any(py),
            Value::Bool(value) => value.into_bound_py_any(py),
            Value::DateTime(value) => value.into_bound_py_any(py),
            Value::Duration(value) => value.into_bound_py_any(py),
            Value::Null => py.None().into_bound_py_any(py),
        }
    }
}
