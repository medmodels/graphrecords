use super::{Converter, Lut, traits::DeepFrom};
use crate::{
    conversion_lut::{ConversionLut, TypeObjectKey},
    graphrecord::errors::PyGraphRecordError,
};
use graphrecords_core::graphrecord::{Value, ValueView};
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

impl DeepFrom<ValueView<'_>> for PyValue {
    fn deep_from(value: ValueView<'_>) -> Self {
        Value::from(value).into()
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

pub(crate) fn value_converter(ob: &Bound<'_, PyAny>) -> Option<Converter<Value>> {
    fn convert_string(ob: &Bound<'_, PyAny>) -> PyResult<Value> {
        Ok(Value::String(ob.extract()?))
    }

    fn convert_int(ob: &Bound<'_, PyAny>) -> PyResult<Value> {
        Ok(Value::Int(ob.extract()?))
    }

    fn convert_float(ob: &Bound<'_, PyAny>) -> PyResult<Value> {
        Ok(Value::Float(ob.extract()?))
    }

    fn convert_bool(ob: &Bound<'_, PyAny>) -> PyResult<Value> {
        Ok(Value::Bool(ob.extract()?))
    }

    fn convert_datetime(ob: &Bound<'_, PyAny>) -> PyResult<Value> {
        Ok(Value::DateTime(ob.extract()?))
    }

    fn convert_duration(ob: &Bound<'_, PyAny>) -> PyResult<Value> {
        Ok(Value::Duration(ob.extract()?))
    }

    #[expect(
        clippy::unnecessary_wraps,
        reason = "the conversion table requires uniform fallible signatures"
    )]
    const fn convert_null(_ob: &Bound<'_, PyAny>) -> PyResult<Value> {
        Ok(Value::Null)
    }

    let type_object = TypeObjectKey::from(ob.get_type().unbind());

    VALUE_CONVERSION_LUT.get_or_insert(type_object, || {
        if ob.is_instance_of::<PyString>() {
            Some(convert_string)
        } else if ob.is_instance_of::<PyBool>() {
            Some(convert_bool)
        } else if ob.is_instance_of::<PyInt>() {
            Some(convert_int)
        } else if ob.is_instance_of::<PyFloat>() {
            Some(convert_float)
        } else if ob.is_instance_of::<PyDateTime>() {
            Some(convert_datetime)
        } else if ob.is_instance_of::<PyDelta>() {
            Some(convert_duration)
        } else if ob.is_none() {
            Some(convert_null)
        } else if ob.extract::<bool>().is_ok() {
            Some(convert_bool)
        } else if ob.extract::<i64>().is_ok() {
            Some(convert_int)
        } else if ob.extract::<f64>().is_ok() {
            Some(convert_float)
        } else {
            None
        }
    })
}

pub(crate) fn convert_pyobject_to_value(ob: &Bound<'_, PyAny>) -> PyResult<Value> {
    let Some(convert) = value_converter(ob) else {
        return Err(
            PyGraphRecordError::Conversion(format!("Failed to convert {ob} into Value")).into(),
        );
    };

    convert(ob)
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
