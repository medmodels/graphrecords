use crate::{errors::PyGraphRecordError, traits::DeepFrom, value::value_converter};
use graphrecords_core::graphrecord::{
    AttributeName, GroupIndex, Identifier, NodeIndex, NodeIndexView, PluginName,
};
use pyo3::{
    Borrowed, Bound, FromPyObject, IntoPyObject, IntoPyObjectExt, PyAny, PyErr, PyResult, Python,
    exceptions::PyTypeError,
    types::{PyAnyMethods, PyBytes},
};
use std::{hash::Hash, ops::Deref};

#[repr(transparent)]
#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct PyIdentifier(Identifier);
pub type PyAttributeName = PyIdentifier;
pub type PyGroupIndex = PyIdentifier;
pub type PyNodeIndex = PyIdentifier;
pub type PyPluginName = PyIdentifier;

impl From<Identifier> for PyIdentifier {
    fn from(value: Identifier) -> Self {
        Self(value)
    }
}

impl From<PyIdentifier> for Identifier {
    fn from(value: PyIdentifier) -> Self {
        value.0
    }
}

impl DeepFrom<PyIdentifier> for Identifier {
    fn deep_from(value: PyIdentifier) -> Self {
        value.into()
    }
}

impl DeepFrom<Identifier> for PyIdentifier {
    fn deep_from(value: Identifier) -> Self {
        value.into()
    }
}

macro_rules! implement_identity_conversion {
    ($name:ident) => {
        impl From<$name> for PyIdentifier {
            fn from(value: $name) -> Self {
                Self(Identifier::from(value))
            }
        }

        impl From<PyIdentifier> for $name {
            fn from(value: PyIdentifier) -> Self {
                Self::from(value.0)
            }
        }

        impl DeepFrom<$name> for PyIdentifier {
            fn deep_from(value: $name) -> Self {
                value.into()
            }
        }

        impl DeepFrom<&$name> for PyIdentifier {
            fn deep_from(value: &$name) -> Self {
                value.clone().into()
            }
        }

        impl DeepFrom<PyIdentifier> for $name {
            fn deep_from(value: PyIdentifier) -> Self {
                value.into()
            }
        }
    };
}

implement_identity_conversion!(NodeIndex);

impl DeepFrom<NodeIndexView<'_>> for PyIdentifier {
    fn deep_from(value: NodeIndexView<'_>) -> Self {
        NodeIndex::from(value).into()
    }
}
implement_identity_conversion!(GroupIndex);
implement_identity_conversion!(AttributeName);
implement_identity_conversion!(PluginName);

impl Deref for PyIdentifier {
    type Target = Identifier;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub(crate) fn convert_pyobject_to_identifier(obj: &Bound<'_, PyAny>) -> PyResult<Identifier> {
    if obj.is_instance_of::<PyBytes>() {
        return Err(PyTypeError::new_err(format!(
            "Failed to convert {obj} into Identifier"
        )));
    }

    let Some(convert) = value_converter(obj) else {
        return Err(PyGraphRecordError::Conversion(format!(
            "Failed to convert {obj} into Identifier"
        ))
        .into());
    };

    Ok(convert(obj)?.try_into().map_err(PyGraphRecordError::from)?)
}

impl FromPyObject<'_, '_> for PyIdentifier {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        convert_pyobject_to_identifier(&obj).map(Self::from)
    }
}

impl<'py> IntoPyObject<'py> for PyIdentifier {
    type Error = PyErr;
    type Output = Bound<'py, Self::Target>;
    type Target = PyAny;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self.0 {
            Identifier::String(value) => value.into_bound_py_any(py),
            Identifier::Int(value) => value.into_bound_py_any(py),
        }
    }
}
