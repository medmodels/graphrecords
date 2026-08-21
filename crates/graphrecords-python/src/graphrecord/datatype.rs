use super::{Lut, traits::DeepFrom};
use crate::{
    conversion_lut::{ConversionLut, TypeObjectKey},
    graphrecord::errors::PyGraphRecordError,
};
use graphrecords_core::graphrecord::datatypes::DataType;
use pyo3::{IntoPyObjectExt, prelude::*};

macro_rules! implement_pymethods {
    ($struct:ty) => {
        #[pymethods]
        impl $struct {
            #[new]
            pub const fn new() -> Self {
                Self {}
            }
        }

        impl Default for $struct {
            fn default() -> Self {
                Self::new()
            }
        }
    };
}

#[repr(transparent)]
#[derive(Clone, Debug)]
pub struct PyDataType(DataType);

impl From<DataType> for PyDataType {
    fn from(value: DataType) -> Self {
        Self(value)
    }
}

impl From<PyDataType> for DataType {
    fn from(value: PyDataType) -> Self {
        value.0
    }
}

impl DeepFrom<PyDataType> for DataType {
    fn deep_from(value: PyDataType) -> Self {
        value.into()
    }
}

impl DeepFrom<DataType> for PyDataType {
    fn deep_from(value: DataType) -> Self {
        value.into()
    }
}

static DATATYPE_CONVERSION_LUT: Lut<DataType> = ConversionLut::new();

#[expect(
    clippy::unnecessary_wraps,
    reason = "the conversion table requires uniform fallible signatures"
)]
pub(crate) fn convert_pyobject_to_datatype(ob: &Bound<'_, pyo3::PyAny>) -> PyResult<DataType> {
    const fn convert_string(_ob: &Bound<'_, pyo3::PyAny>) -> PyResult<DataType> {
        Ok(DataType::String)
    }

    const fn convert_int(_ob: &Bound<'_, pyo3::PyAny>) -> PyResult<DataType> {
        Ok(DataType::Int)
    }

    const fn convert_float(_ob: &Bound<'_, pyo3::PyAny>) -> PyResult<DataType> {
        Ok(DataType::Float)
    }

    const fn convert_bool(_ob: &Bound<'_, pyo3::PyAny>) -> PyResult<DataType> {
        Ok(DataType::Bool)
    }

    const fn convert_datetime(_ob: &Bound<'_, pyo3::PyAny>) -> PyResult<DataType> {
        Ok(DataType::DateTime)
    }

    const fn convert_duration(_ob: &Bound<'_, pyo3::PyAny>) -> PyResult<DataType> {
        Ok(DataType::Duration)
    }

    const fn convert_null(_ob: &Bound<'_, pyo3::PyAny>) -> PyResult<DataType> {
        Ok(DataType::Null)
    }

    const fn convert_any(_ob: &Bound<'_, pyo3::PyAny>) -> PyResult<DataType> {
        Ok(DataType::Any)
    }

    fn convert_union(ob: &Bound<'_, pyo3::PyAny>) -> PyResult<DataType> {
        let union = ob.extract::<PyRef<PyUnion>>()?;

        let datatypes = union.0.clone();

        Ok(DataType::Union((
            Box::new(datatypes.0.into()),
            Box::new(datatypes.1.into()),
        )))
    }

    fn convert_option(ob: &Bound<'_, pyo3::PyAny>) -> PyResult<DataType> {
        let option = ob.extract::<PyRef<PyOption>>()?;

        Ok(DataType::Option(Box::new(option.0.clone().into())))
    }

    let type_object = TypeObjectKey::from(ob.get_type().unbind());

    let conversion_function = DATATYPE_CONVERSION_LUT.get_or_insert(type_object, || {
        if ob.is_instance_of::<PyString>() {
            Some(convert_string)
        } else if ob.is_instance_of::<PyInt>() {
            Some(convert_int)
        } else if ob.is_instance_of::<PyFloat>() {
            Some(convert_float)
        } else if ob.is_instance_of::<PyBool>() {
            Some(convert_bool)
        } else if ob.is_instance_of::<PyDateTime>() {
            Some(convert_datetime)
        } else if ob.is_instance_of::<PyDuration>() {
            Some(convert_duration)
        } else if ob.is_instance_of::<PyNull>() {
            Some(convert_null)
        } else if ob.is_instance_of::<PyAny>() {
            Some(convert_any)
        } else if ob.is_instance_of::<PyUnion>() {
            Some(convert_union)
        } else if ob.is_instance_of::<PyOption>() {
            Some(convert_option)
        } else {
            None
        }
    });

    let Some(convert) = conversion_function else {
        return Err(PyGraphRecordError::Conversion(format!(
            "Failed to convert {ob} into DataType"
        ))
        .into());
    };

    convert(ob)
}

impl FromPyObject<'_, '_> for PyDataType {
    type Error = PyErr;

    fn extract(ob: Borrowed<'_, '_, pyo3::PyAny>) -> PyResult<Self> {
        convert_pyobject_to_datatype(&ob).map(Self)
    }
}

impl<'py> IntoPyObject<'py> for PyDataType {
    type Error = PyErr;
    type Output = Bound<'py, Self::Target>;
    type Target = pyo3::PyAny;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self.0 {
            DataType::String => PyString {}.into_bound_py_any(py),
            DataType::Int => PyInt {}.into_bound_py_any(py),
            DataType::Float => PyFloat {}.into_bound_py_any(py),
            DataType::Bool => PyBool {}.into_bound_py_any(py),
            DataType::DateTime => PyDateTime {}.into_bound_py_any(py),
            DataType::Duration => PyDuration {}.into_bound_py_any(py),
            DataType::Null => PyNull {}.into_bound_py_any(py),
            DataType::Any => PyAny {}.into_bound_py_any(py),
            DataType::Union((left, right)) => {
                PyUnion(((*left).into(), (*right).into())).into_bound_py_any(py)
            }
            DataType::Option(datatype) => PyOption((*datatype).into()).into_bound_py_any(py),
        }
    }
}

#[pyclass(frozen, module = "graphrecords._graphrecords.datatype")]
pub struct PyString;
implement_pymethods!(PyString);

#[pyclass(frozen, module = "graphrecords._graphrecords.datatype")]
pub struct PyInt;
implement_pymethods!(PyInt);

#[pyclass(frozen, module = "graphrecords._graphrecords.datatype")]
pub struct PyFloat;
implement_pymethods!(PyFloat);

#[pyclass(frozen, module = "graphrecords._graphrecords.datatype")]
pub struct PyBool;
implement_pymethods!(PyBool);

#[pyclass(frozen, module = "graphrecords._graphrecords.datatype")]
pub struct PyDateTime;
implement_pymethods!(PyDateTime);

#[pyclass(frozen, module = "graphrecords._graphrecords.datatype")]
pub struct PyDuration;
implement_pymethods!(PyDuration);

#[pyclass(frozen, module = "graphrecords._graphrecords.datatype")]
pub struct PyNull;
implement_pymethods!(PyNull);

#[pyclass(frozen, module = "graphrecords._graphrecords.datatype")]
pub struct PyAny;
implement_pymethods!(PyAny);

#[pyclass(frozen, module = "graphrecords._graphrecords.datatype")]
pub struct PyUnion((PyDataType, PyDataType));

#[pymethods]
impl PyUnion {
    #[new]
    const fn new(left: PyDataType, right: PyDataType) -> Self {
        Self((left, right))
    }

    #[getter]
    fn left(&self) -> PyDataType {
        self.0.0.clone()
    }

    #[getter]
    fn right(&self) -> PyDataType {
        self.0.1.clone()
    }
}

#[pyclass(frozen, module = "graphrecords._graphrecords.datatype")]
pub struct PyOption(PyDataType);

#[pymethods]
impl PyOption {
    #[new]
    const fn new(datatype: PyDataType) -> Self {
        Self(datatype)
    }

    #[getter]
    fn datatype(&self) -> PyDataType {
        self.0.clone()
    }
}
