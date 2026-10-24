use graphrecords::prelude::{
    AttributeDataType, AttributeType, DataType, EdgeDirection, EdgeIndex, GraphRecord, GroupSchema,
    OnConflict, Schema, SchemaType,
};
use graphrecords_pyo3::PyGraphRecordError;
use pyo3::{
    Borrowed, Bound, FromPyObject, IntoPyObject, Py, PyAny, PyErr, PyResult, Python,
    exceptions::PyTypeError,
    sync::PyOnceLock,
    types::{PyAnyMethods, PyBytes, PyBytesMethods, PyModule, PyType},
};

const PACKAGE: &str = "graphrecords";

macro_rules! implement_bytes_conversion {
    (
        $name:ident,
        $inner:ty,class =
        $class:literal,internal_module =
        $internal_module:literal,internal_class =
        $internal_class:literal,internal_attribute =
        $internal_attribute:literal,constructor =
        $constructor:literal $(,)?
    ) => {
        #[repr(transparent)]
        #[derive(Debug, Clone)]
        pub struct $name(pub $inner);

        impl $name {
            fn class(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
                static CLASS: PyOnceLock<Py<PyType>> = PyOnceLock::new();

                CLASS.import(py, PACKAGE, $class)
            }

            fn internal_class(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
                static INTERNAL_CLASS: PyOnceLock<Py<PyType>> = PyOnceLock::new();

                INTERNAL_CLASS.import(py, $internal_module, $internal_class)
            }
        }

        impl From<$inner> for $name {
            fn from(value: $inner) -> Self {
                Self(value)
            }
        }

        impl From<$name> for $inner {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl AsRef<$inner> for $name {
            fn as_ref(&self) -> &$inner {
                &self.0
            }
        }

        impl FromPyObject<'_, '_> for $name {
            type Error = PyErr;

            fn extract(obj: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
                let py = obj.py();

                if !obj.is_instance(Self::class(py)?)? {
                    return Err(PyTypeError::new_err(format!(
                        "Failed to convert {} into {}",
                        obj.as_any(),
                        $class
                    )));
                }

                let bytes = obj
                    .getattr($internal_attribute)?
                    .call_method0("_to_bytes")?;
                let bytes = bytes.cast::<PyBytes>()?.as_bytes();

                let value = py.detach(|| bincode::deserialize(bytes)).map_err(|_| {
                    PyGraphRecordError::Conversion(format!("Failed to deserialize {}", $class))
                })?;

                Ok(Self(value))
            }
        }

        impl<'py> IntoPyObject<'py> for $name {
            type Error = PyErr;
            type Output = Bound<'py, PyAny>;
            type Target = PyAny;

            fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
                let bytes = py.detach(|| bincode::serialize(&self.0)).map_err(|_| {
                    PyGraphRecordError::Conversion(format!("Failed to serialize {}", $class))
                })?;
                let bytes = PyBytes::new(py, &bytes);

                let internal = Self::internal_class(py)?.call_method1("_from_bytes", (bytes,))?;

                Self::class(py)?.call_method1($constructor, (internal,))
            }
        }
    };
}

macro_rules! implement_enum_conversion {
    ($name:ident, $inner:ty, class = $class:literal, [$($variant:ident),+ $(,)?]) => {
        #[repr(transparent)]
        #[derive(Debug, Clone)]
        pub struct $name(pub $inner);

        impl $name {
            fn class(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
                static CLASS: PyOnceLock<Py<PyType>> = PyOnceLock::new();

                CLASS.import(py, PACKAGE, $class)
            }
        }

        impl From<$inner> for $name {
            fn from(value: $inner) -> Self {
                Self(value)
            }
        }

        impl From<$name> for $inner {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl AsRef<$inner> for $name {
            fn as_ref(&self) -> &$inner {
                &self.0
            }
        }

        impl FromPyObject<'_, '_> for $name {
            type Error = PyErr;

            fn extract(obj: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
                let class = Self::class(obj.py())?;

                $(if obj.is(class.getattr(stringify!($variant))?) {
                    return Ok(Self(<$inner>::$variant));
                })+

                Err(PyTypeError::new_err(format!(
                    "Failed to convert {} into {}",
                    obj.as_any(),
                    $class
                )))
            }
        }

        impl<'py> IntoPyObject<'py> for $name {
            type Error = PyErr;
            type Output = Bound<'py, PyAny>;
            type Target = PyAny;

            fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
                let class = Self::class(py)?;

                match self.0 {
                    $(<$inner>::$variant => class.getattr(stringify!($variant)),)+
                }
            }
        }
    };
}

implement_bytes_conversion!(
    PyGraphRecord,
    GraphRecord,
    class = "GraphRecord",
    internal_module = "graphrecords._graphrecords.graphrecord",
    internal_class = "PyGraphRecord",
    internal_attribute = "_py_graphrecord",
    constructor = "_from_py_graphrecord",
);
implement_bytes_conversion!(
    PyEdgeIndex,
    EdgeIndex,
    class = "EdgeIndex",
    internal_module = "graphrecords._graphrecords.graphrecord",
    internal_class = "PyEdgeIndex",
    internal_attribute = "_py_edge_index",
    constructor = "_from_py_edge_index",
);
implement_bytes_conversion!(
    PyAttributeDataType,
    AttributeDataType,
    class = "AttributeDataType",
    internal_module = "graphrecords._graphrecords.schema",
    internal_class = "PyAttributeDataType",
    internal_attribute = "_py_attribute_data_type",
    constructor = "_from_py_attribute_data_type",
);
implement_bytes_conversion!(
    PyGroupSchema,
    GroupSchema,
    class = "GroupSchema",
    internal_module = "graphrecords._graphrecords.schema",
    internal_class = "PyGroupSchema",
    internal_attribute = "_py_group_schema",
    constructor = "_from_py_group_schema",
);
implement_bytes_conversion!(
    PySchema,
    Schema,
    class = "Schema",
    internal_module = "graphrecords._graphrecords.schema",
    internal_class = "PySchema",
    internal_attribute = "_py_schema",
    constructor = "_from_py_schema",
);

implement_enum_conversion!(
    PyAttributeType,
    AttributeType,
    class = "AttributeType",
    [Categorical, Continuous, Temporal, Unstructured]
);
implement_enum_conversion!(
    PySchemaType,
    SchemaType,
    class = "SchemaType",
    [Provided, Inferred]
);
implement_enum_conversion!(
    PyEdgeDirection,
    EdgeDirection,
    class = "EdgeDirection",
    [Incoming, Outgoing, Both]
);
implement_enum_conversion!(
    PyOnConflict,
    OnConflict,
    class = "OnConflict",
    [Raise, KeepSelf, KeepOther]
);

#[repr(transparent)]
#[derive(Debug, Clone)]
pub struct PyDataType(pub DataType);

impl PyDataType {
    fn package(py: Python<'_>) -> PyResult<&Bound<'_, PyModule>> {
        static PACKAGE_MODULE: PyOnceLock<Py<PyModule>> = PyOnceLock::new();

        PACKAGE_MODULE
            .get_or_try_init(py, || py.import(PACKAGE).map(Bound::unbind))
            .map(|package| package.bind(py))
    }

    fn data_type(package: &Bound<'_, PyModule>, object: &Bound<'_, PyAny>) -> PyResult<DataType> {
        let is_instance_of = |class| object.is_instance(&package.getattr(class)?);

        if is_instance_of("String")? {
            return Ok(DataType::String);
        }

        if is_instance_of("Int")? {
            return Ok(DataType::Int);
        }

        if is_instance_of("Float")? {
            return Ok(DataType::Float);
        }

        if is_instance_of("Bool")? {
            return Ok(DataType::Bool);
        }

        if is_instance_of("DateTime")? {
            return Ok(DataType::DateTime);
        }

        if is_instance_of("Duration")? {
            return Ok(DataType::Duration);
        }

        if is_instance_of("Null")? {
            return Ok(DataType::Null);
        }

        if is_instance_of("Any")? {
            return Ok(DataType::Any);
        }

        if is_instance_of("Union")? {
            return Ok(DataType::Union((
                Box::new(Self::data_type(package, &object.getattr("left")?)?),
                Box::new(Self::data_type(package, &object.getattr("right")?)?),
            )));
        }

        if is_instance_of("Option")? {
            return Ok(DataType::Option(Box::new(Self::data_type(
                package,
                &object.getattr("datatype")?,
            )?)));
        }

        Err(PyTypeError::new_err(format!(
            "Failed to convert {object} into DataType"
        )))
    }

    fn python_data_type<'py>(
        package: &Bound<'py, PyModule>,
        data_type: DataType,
    ) -> PyResult<Bound<'py, PyAny>> {
        match data_type {
            DataType::String => package.getattr("String")?.call0(),
            DataType::Int => package.getattr("Int")?.call0(),
            DataType::Float => package.getattr("Float")?.call0(),
            DataType::Bool => package.getattr("Bool")?.call0(),
            DataType::DateTime => package.getattr("DateTime")?.call0(),
            DataType::Duration => package.getattr("Duration")?.call0(),
            DataType::Null => package.getattr("Null")?.call0(),
            DataType::Any => package.getattr("Any")?.call0(),
            DataType::Union((left, right)) => package.getattr("Union")?.call1((
                Self::python_data_type(package, *left)?,
                Self::python_data_type(package, *right)?,
            )),
            DataType::Option(inner) => package
                .getattr("Option")?
                .call1((Self::python_data_type(package, *inner)?,)),
        }
    }
}

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

impl AsRef<DataType> for PyDataType {
    fn as_ref(&self) -> &DataType {
        &self.0
    }
}

impl FromPyObject<'_, '_> for PyDataType {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        let package = Self::package(obj.py())?;

        Self::data_type(package, &obj).map(Self)
    }
}

impl<'py> IntoPyObject<'py> for PyDataType {
    type Error = PyErr;
    type Output = Bound<'py, PyAny>;
    type Target = PyAny;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let package = Self::package(py)?;

        Self::python_data_type(package, self.0)
    }
}
