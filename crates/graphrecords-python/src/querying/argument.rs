use crate::querying::{PyOperand, value_conversion::ValueConversion};
use graphrecords_query::{
    Mask, Scalar,
    dynamic::{DynArgumentSource, DynInvokeArgument, DynValue},
    registry::ValueDescriptor,
};
use pyo3::{
    exceptions::PyTypeError,
    prelude::*,
    types::{PyBytes, PyString},
};

#[pyclass(frozen)]
pub struct PyArgument(DynArgumentSource);

impl PyArgument {
    pub(super) const fn new(source: DynArgumentSource) -> Self {
        Self(source)
    }

    pub(super) const fn source(&self) -> &DynArgumentSource {
        &self.0
    }
}

impl PyOperand {
    pub(super) fn mask_argument(object: &Bound<'_, PyAny>) -> PyResult<DynInvokeArgument> {
        if let Some(source) = Self::operand_source(object) {
            return Ok(DynInvokeArgument::Source(source));
        }

        Ok(DynInvokeArgument::Source(DynArgumentSource::mask(
            object.extract()?,
        )))
    }

    pub(super) fn scalar_argument(object: &Bound<'_, PyAny>) -> PyResult<DynInvokeArgument> {
        if let Some(source) = Self::operand_source(object) {
            return Ok(DynInvokeArgument::Source(source));
        }
        let value = DynValue::from_python(object, &ValueDescriptor::value::<Scalar>())?;

        Ok(DynInvokeArgument::Source(DynArgumentSource::value(value)))
    }

    pub(super) fn operand_argument(object: &Bound<'_, PyAny>) -> PyResult<DynInvokeArgument> {
        object
            .cast::<Self>()
            .map(|operand| DynInvokeArgument::Operand(operand.get().operand().clone()))
            .map_err(|_| PyTypeError::new_err("expected an operand argument"))
    }

    pub(super) fn value_argument(&self, object: &Bound<'_, PyAny>) -> PyResult<DynInvokeArgument> {
        if let Some(source) = Self::operand_source(object) {
            return Ok(DynInvokeArgument::Source(source));
        }
        let descriptor = self.operand().descriptor().lane_shape().value();

        if descriptor.domain().is::<Mask>() {
            return Ok(DynInvokeArgument::Source(DynArgumentSource::mask(
                object.extract()?,
            )));
        }

        DynValue::from_python(object, descriptor)
            .map(DynArgumentSource::value)
            .map(DynInvokeArgument::Source)
    }

    pub(super) fn set_argument(&self, values: &Bound<'_, PyAny>) -> PyResult<DynInvokeArgument> {
        if let Some(source) = Self::operand_source(values) {
            return Ok(DynInvokeArgument::Source(source));
        }
        if values.is_instance_of::<PyString>() || values.is_instance_of::<PyBytes>() {
            return Err(PyTypeError::new_err(
                "expected a sequence of values; `str` and `bytes` are single values",
            ));
        }
        let descriptor = self.operand().descriptor().lane_shape().value();

        if descriptor.domain().is::<Mask>() {
            let values = values
                .try_iter()?
                .map(|value| value?.extract())
                .collect::<PyResult<Vec<_>>>()?;

            return Ok(DynInvokeArgument::Source(DynArgumentSource::mask_values(
                values,
            )));
        }
        let values = values
            .try_iter()?
            .map(|value| DynValue::from_python(&value?, descriptor))
            .collect::<PyResult<Vec<_>>>()?;

        Ok(DynInvokeArgument::Source(DynArgumentSource::values(values)))
    }

    pub(super) fn dropping_argument(&self) -> PyArgument {
        PyArgument::new(DynArgumentSource::operand(self.operand().clone()))
    }

    pub(super) fn replacing_argument(
        &self,
        replacement: &Bound<'_, PyAny>,
    ) -> PyResult<PyArgument> {
        if replacement.cast::<PyArgument>().is_ok() {
            return Err(PyTypeError::new_err(
                "an `on_missing` argument cannot itself be used as a replacement",
            ));
        }
        let source = self.operand().clone();

        if let Ok(operand) = replacement.cast::<Self>() {
            let replacement = operand.get().operand().clone();

            return Ok(PyArgument::new(
                DynArgumentSource::replace_missing_with_operand(source, replacement),
            ));
        }
        let descriptor = self.operand().descriptor().lane_shape().value();

        if descriptor.domain().is::<Mask>() {
            return Ok(PyArgument::new(
                DynArgumentSource::replace_missing_with_mask(source, replacement.extract()?),
            ));
        }

        DynValue::from_python(replacement, descriptor).map(|value| {
            PyArgument::new(DynArgumentSource::replace_missing_with_value(source, value))
        })
    }

    fn operand_source(object: &Bound<'_, PyAny>) -> Option<DynArgumentSource> {
        if let Ok(argument) = object.cast::<PyArgument>() {
            return Some(argument.get().source().clone());
        }

        object
            .cast::<Self>()
            .ok()
            .map(|operand| DynArgumentSource::operand(operand.get().operand().clone()))
    }
}
