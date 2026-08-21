use crate::querying::{PyExpression, PySeries, value_conversion::ValueConversion};
use graphrecords_query::{
    Mask, Scalar,
    dynamic::{DynArgumentLane, DynArgumentSource, DynExpression, DynInvokeArgument, DynValue},
    registry::{ArityDescriptor, ExpressionDescriptor, LaneShapeDescriptor, ValueDescriptor},
};
use pyo3::{
    exceptions::PyTypeError,
    prelude::*,
    types::{PyBytes, PyString},
};

#[pyclass(frozen, module = "graphrecords._graphrecords.querying")]
pub struct PyArgument(DynArgumentSource);

impl PyArgument {
    pub(super) const fn new(source: DynArgumentSource) -> Self {
        Self(source)
    }

    pub(crate) const fn source(&self) -> &DynArgumentSource {
        &self.0
    }
}

impl PyExpression {
    pub(super) fn mask_argument(object: &Bound<'_, PyAny>) -> PyResult<DynInvokeArgument> {
        if let Some(source) = Self::argument_source(object)? {
            return Ok(DynInvokeArgument::Source(source));
        }

        Ok(DynInvokeArgument::Source(DynArgumentSource::mask(
            object.extract()?,
        )))
    }

    pub(super) fn scalar_argument(object: &Bound<'_, PyAny>) -> PyResult<DynInvokeArgument> {
        if let Some(source) = Self::argument_source(object)? {
            return Ok(DynInvokeArgument::Source(source));
        }
        let value = DynValue::from_python(object, &ValueDescriptor::value::<Scalar>())?;

        Ok(DynInvokeArgument::Source(DynArgumentSource::value(value)))
    }

    pub(super) fn expression_argument(object: &Bound<'_, PyAny>) -> PyResult<DynInvokeArgument> {
        Self::argument_lane(object)?
            .map(DynInvokeArgument::Lane)
            .ok_or_else(|| PyTypeError::new_err("expected an expression argument"))
    }

    pub(super) fn value_argument(
        expression: &DynExpression,
        object: &Bound<'_, PyAny>,
    ) -> PyResult<DynInvokeArgument> {
        if let Some(source) = Self::argument_source(object)? {
            return Ok(DynInvokeArgument::Source(source));
        }
        let descriptor = expression.descriptor().lane_shape().value();

        if descriptor.domain().is::<Mask>() {
            return Ok(DynInvokeArgument::Source(DynArgumentSource::mask(
                object.extract()?,
            )));
        }

        DynValue::from_python(object, descriptor)
            .map(DynArgumentSource::value)
            .map(DynInvokeArgument::Source)
    }

    pub(super) fn set_argument(
        expression: &DynExpression,
        values: &Bound<'_, PyAny>,
    ) -> PyResult<DynInvokeArgument> {
        if let Ok(argument) = values.cast::<PyArgument>() {
            return Ok(DynInvokeArgument::Source(argument.get().source().clone()));
        }
        if let Some(lane) = Self::set_lane(values) {
            return Ok(DynInvokeArgument::Source(DynArgumentSource::lane(lane)));
        }
        if values.is_instance_of::<PyString>() || values.is_instance_of::<PyBytes>() {
            return Err(PyTypeError::new_err(
                "expected a sequence of values; `str` and `bytes` are single values",
            ));
        }
        let descriptor = expression.descriptor().lane_shape().value();

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

    pub(super) fn dropping_argument(source: DynArgumentLane) -> PyResult<PyArgument> {
        Self::verify_missing_policy(&source)?;

        Ok(PyArgument::new(DynArgumentSource::drop_missing(source)))
    }

    pub(super) fn replacing_argument(
        source: DynArgumentLane,
        replacement: &Bound<'_, PyAny>,
    ) -> PyResult<PyArgument> {
        Self::verify_missing_policy(&source)?;

        if let Ok(argument) = replacement.cast::<PyArgument>() {
            return Ok(PyArgument::new(
                DynArgumentSource::replace_missing_with_source(
                    source,
                    argument.get().source().clone(),
                ),
            ));
        }

        if let Some(lane) = Self::argument_lane(replacement)? {
            return Ok(PyArgument::new(
                DynArgumentSource::replace_missing_with_lane(source, lane),
            ));
        }
        let descriptor = source.descriptor().lane_shape().value().clone();

        if descriptor.domain().is::<Mask>() {
            return Ok(PyArgument::new(
                DynArgumentSource::replace_missing_with_mask(source, replacement.extract()?),
            ));
        }

        DynValue::from_python(replacement, &descriptor).map(|value| {
            PyArgument::new(DynArgumentSource::replace_missing_with_value(source, value))
        })
    }

    fn verify_missing_policy(source: &DynArgumentLane) -> PyResult<()> {
        let admitted = matches!(
            source.descriptor(),
            ExpressionDescriptor::Lane {
                shape: LaneShapeDescriptor::Indexed { .. },
                arity: ArityDescriptor::Multiple { .. }
            } | ExpressionDescriptor::Lane {
                shape: LaneShapeDescriptor::Bare { .. },
                arity: ArityDescriptor::Single
            }
        );

        if admitted {
            return Ok(());
        }

        Err(PyTypeError::new_err(
            "an `on_missing` policy needs indexed elements or at most one value",
        ))
    }

    fn argument_source(object: &Bound<'_, PyAny>) -> PyResult<Option<DynArgumentSource>> {
        if let Ok(argument) = object.cast::<PyArgument>() {
            return Ok(Some(argument.get().source().clone()));
        }

        Ok(Self::argument_lane(object)?.map(DynArgumentSource::lane))
    }

    fn set_lane(object: &Bound<'_, PyAny>) -> Option<DynArgumentLane> {
        if let Ok(series) = object.cast::<PySeries>() {
            return Some(series.get().lane());
        }

        object
            .cast::<Self>()
            .ok()
            .map(|expression| DynArgumentLane::Expression(expression.get().expression().clone()))
    }

    fn argument_lane(object: &Bound<'_, PyAny>) -> PyResult<Option<DynArgumentLane>> {
        if let Ok(series) = object.cast::<PySeries>() {
            let lane = series.get().lane();
            let admitted = matches!(
                lane.descriptor(),
                ExpressionDescriptor::Lane {
                    shape: LaneShapeDescriptor::Indexed { .. },
                    arity: ArityDescriptor::Multiple { .. }
                } | ExpressionDescriptor::Lane {
                    shape: LaneShapeDescriptor::Bare { .. },
                    arity: ArityDescriptor::Single | ArityDescriptor::Definite
                }
            );

            if !admitted {
                return Err(PyTypeError::new_err(
                    "a series argument must hold indexed elements or at most one value",
                ));
            }

            return Ok(Some(lane));
        }

        Ok(object
            .cast::<Self>()
            .ok()
            .map(|expression| DynArgumentLane::Expression(expression.get().expression().clone())))
    }
}
