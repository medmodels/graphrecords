mod argument;
mod cast_target;
mod direction;
mod endpoint;
pub(crate) mod exception;
mod failure_kind;
mod index_conversion;
mod terminal;
mod value_conversion;
mod value_target;

use crate::{
    graphrecord::{PyAttributeName, PyGroup},
    querying::exception::FailureConversion,
};
pub use argument::PyArgument;
pub use cast_target::PyCastTarget;
pub use direction::PyEdgeDirection;
pub use endpoint::PyEdgeEndpointRole;
pub use exception::{
    ArgumentAbsentError, DivisionByZeroError, DuplicateExpandedChildIndexError,
    DuplicateIndexError, EmptySplitDelimiterError, EvaluationCacheGraphRecordMismatchError,
    ExternalError, GraphRecordError, IncomparableIndicesError, IncomparableValuesAtError,
    IncomparableValuesError, IntegerOverflowError, InvalidCastError, InvalidClipBoundsError,
    InvalidMedianValueError, InvalidPaddingCharacterError, InvalidPartitionBucketArityError,
    InvalidRegexPatternError, InvalidStandardDeviationValueError, InvalidStringSliceError,
    InvalidTransitionError, InvalidVarianceValueError, MissingAttributeError,
    MissingGroupAggregateError, MissingTraversedAttributeError, ModuloByZeroError,
    NegativeLengthError, NegativeSquareRootError, NoChildIndexError, NonIntegerValueError,
    NonNumericValueError, NonPositiveLogarithmError, NonStringValueError, QueryError,
    StringLengthOverflowError, StringPaddingOverflowError, UnresolvedBucketFailuresError,
    UnresolvedGroupKeyFailuresError, UnsupportedValueRoleError,
};
pub use failure_kind::PyFailureKind;
use graphrecords_query::dynamic::{DynInvokeArgument, DynOperand};
use pyo3::prelude::*;
pub use value_target::PyValueTarget;

#[pyclass(frozen)]
pub struct PyOperand(DynOperand);

#[pymethods]
impl PyOperand {
    fn on_missing_drop(&self) -> PyArgument {
        self.dropping_argument()
    }

    fn on_missing_replace(&self, replacement: &Bound<'_, PyAny>) -> PyResult<PyArgument> {
        self.replacing_argument(replacement)
    }

    fn cache(&self) -> Self {
        Self(self.operand().cache())
    }

    fn filter(&self, mask: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.invoke("filter", &[Self::mask_argument(mask)?])
    }

    fn and_(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.invoke("and", &[Self::mask_argument(other)?])
    }

    fn or_(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.invoke("or", &[Self::mask_argument(other)?])
    }

    fn xor(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.invoke("xor", &[Self::mask_argument(other)?])
    }

    fn not_(&self) -> PyResult<Self> {
        self.invoke("not", &[])
    }

    fn first(&self) -> PyResult<Self> {
        self.invoke("first", &[])
    }

    fn last(&self) -> PyResult<Self> {
        self.invoke("last", &[])
    }

    fn reverse_order(&self) -> PyResult<Self> {
        self.invoke("reverse_order", &[])
    }

    fn shuffle(&self) -> PyResult<Self> {
        self.invoke("shuffle", &[])
    }

    fn unorder(&self) -> PyResult<Self> {
        self.invoke("unorder", &[])
    }

    fn sort(&self) -> PyResult<Self> {
        self.invoke("sort", &[])
    }

    fn sort_by(&self, key: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.invoke("sort_by", &[Self::scalar_argument(key)?])
    }

    fn drop_duplicates(&self) -> PyResult<Self> {
        self.invoke("drop_duplicates", &[])
    }

    fn is_duplicated(&self) -> PyResult<Self> {
        self.invoke("is_duplicated", &[])
    }

    fn unique(&self) -> PyResult<Self> {
        self.invoke("unique", &[])
    }

    fn take(&self, elements: usize) -> PyResult<Self> {
        self.invoke("take", &[DynInvokeArgument::Position(elements)])
    }

    fn is_bool(&self) -> PyResult<Self> {
        self.invoke("is_bool", &[])
    }

    fn is_datetime(&self) -> PyResult<Self> {
        self.invoke("is_datetime", &[])
    }

    fn is_duration(&self) -> PyResult<Self> {
        self.invoke("is_duration", &[])
    }

    fn is_float(&self) -> PyResult<Self> {
        self.invoke("is_float", &[])
    }

    fn is_null(&self) -> PyResult<Self> {
        self.invoke("is_null", &[])
    }

    fn is_int(&self) -> PyResult<Self> {
        self.invoke("is_int", &[])
    }

    fn is_string(&self) -> PyResult<Self> {
        self.invoke("is_string", &[])
    }

    fn abs(&self) -> PyResult<Self> {
        self.invoke("abs", &[])
    }

    fn neg(&self) -> PyResult<Self> {
        self.invoke("neg", &[])
    }

    fn sign(&self) -> PyResult<Self> {
        self.invoke("sign", &[])
    }

    fn ceil(&self) -> PyResult<Self> {
        self.invoke("ceil", &[])
    }

    fn cbrt(&self) -> PyResult<Self> {
        self.invoke("cbrt", &[])
    }

    fn exp(&self) -> PyResult<Self> {
        self.invoke("exp", &[])
    }

    fn floor(&self) -> PyResult<Self> {
        self.invoke("floor", &[])
    }

    fn log(&self) -> PyResult<Self> {
        self.invoke("log", &[])
    }

    fn round(&self) -> PyResult<Self> {
        self.invoke("round", &[])
    }

    fn sqrt(&self) -> PyResult<Self> {
        self.invoke("sqrt", &[])
    }

    fn trim(&self) -> PyResult<Self> {
        self.invoke("trim", &[])
    }

    fn trim_start(&self) -> PyResult<Self> {
        self.invoke("trim_start", &[])
    }

    fn trim_end(&self) -> PyResult<Self> {
        self.invoke("trim_end", &[])
    }

    fn lowercase(&self) -> PyResult<Self> {
        self.invoke("lowercase", &[])
    }

    fn uppercase(&self) -> PyResult<Self> {
        self.invoke("uppercase", &[])
    }

    fn reverse(&self) -> PyResult<Self> {
        self.invoke("reverse", &[])
    }

    fn length(&self) -> PyResult<Self> {
        self.invoke("length", &[])
    }

    fn slice(&self, start: usize, end: usize) -> PyResult<Self> {
        self.invoke(
            "slice",
            &[
                DynInvokeArgument::Position(start),
                DynInvokeArgument::Position(end),
            ],
        )
    }

    fn starts_with(&self, prefix: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.invoke("starts_with", &[Self::scalar_argument(prefix)?])
    }

    fn ends_with(&self, suffix: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.invoke("ends_with", &[Self::scalar_argument(suffix)?])
    }

    fn contains(&self, part: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.invoke("contains", &[Self::scalar_argument(part)?])
    }

    fn matches(&self, pattern: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.invoke("matches", &[Self::scalar_argument(pattern)?])
    }

    fn strip_prefix(&self, prefix: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.invoke("strip_prefix", &[Self::scalar_argument(prefix)?])
    }

    fn strip_suffix(&self, suffix: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.invoke("strip_suffix", &[Self::scalar_argument(suffix)?])
    }

    fn replace(&self, old: &Bound<'_, PyAny>, new: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.invoke(
            "replace",
            &[Self::scalar_argument(old)?, Self::scalar_argument(new)?],
        )
    }

    fn replace_all(&self, old: &Bound<'_, PyAny>, new: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.invoke(
            "replace_all",
            &[Self::scalar_argument(old)?, Self::scalar_argument(new)?],
        )
    }

    fn pad_start(&self, width: &Bound<'_, PyAny>, character: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.invoke(
            "pad_start",
            &[
                Self::scalar_argument(width)?,
                Self::scalar_argument(character)?,
            ],
        )
    }

    fn pad_end(&self, width: &Bound<'_, PyAny>, character: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.invoke(
            "pad_end",
            &[
                Self::scalar_argument(width)?,
                Self::scalar_argument(character)?,
            ],
        )
    }

    fn split(&self, delimiter: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.invoke("split", &[Self::scalar_argument(delimiter)?])
    }

    fn attribute(&self, attribute: PyAttributeName) -> PyResult<Self> {
        self.invoke(
            "attribute",
            &[DynInvokeArgument::Attribute(attribute.into())],
        )
    }

    fn attributes(&self) -> PyResult<Self> {
        self.invoke("attributes", &[])
    }

    fn resolve(&self) -> PyResult<Self> {
        self.invoke("resolve", &[])
    }

    fn select(&self) -> PyResult<Self> {
        self.invoke("select", &[])
    }

    fn parent_index(&self) -> PyResult<Self> {
        self.invoke("parent_index", &[])
    }

    fn child_index(&self) -> PyResult<Self> {
        self.invoke("child_index", &[])
    }

    fn has_attribute(&self, attribute: PyAttributeName) -> PyResult<Self> {
        self.invoke(
            "has_attribute",
            &[DynInvokeArgument::Attribute(attribute.into())],
        )
    }

    fn in_group(&self, group: PyGroup) -> PyResult<Self> {
        self.invoke("in_group", &[DynInvokeArgument::Group(group.into())])
    }

    fn add(&self, value: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.invoke("add", &[self.value_argument(value)?])
    }

    fn subtract(&self, value: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.invoke("subtract", &[self.value_argument(value)?])
    }

    fn multiply(&self, value: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.invoke("multiply", &[self.value_argument(value)?])
    }

    fn power(&self, value: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.invoke("power", &[self.value_argument(value)?])
    }

    fn modulo(&self, value: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.invoke("modulo", &[self.value_argument(value)?])
    }

    fn divide(&self, value: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.invoke("divide", &[self.value_argument(value)?])
    }

    fn clip(&self, lower: &Bound<'_, PyAny>, upper: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.invoke(
            "clip",
            &[self.value_argument(lower)?, self.value_argument(upper)?],
        )
    }

    fn cast(&self, target: PyCastTarget) -> PyResult<Self> {
        self.invoke("cast", &[DynInvokeArgument::CastTarget(target.into())])
    }

    fn equal_to(&self, value: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.invoke("equal_to", &[self.value_argument(value)?])
    }

    fn not_equal_to(&self, value: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.invoke("not_equal_to", &[self.value_argument(value)?])
    }

    fn greater_than(&self, value: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.invoke("greater_than", &[self.value_argument(value)?])
    }

    fn greater_than_or_equal_to(&self, value: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.invoke("greater_than_or_equal_to", &[self.value_argument(value)?])
    }

    fn less_than(&self, value: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.invoke("less_than", &[self.value_argument(value)?])
    }

    fn less_than_or_equal_to(&self, value: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.invoke("less_than_or_equal_to", &[self.value_argument(value)?])
    }

    fn is_in(&self, values: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.invoke("is_in", &[self.set_argument(values)?])
    }

    fn index(&self) -> PyResult<Self> {
        self.invoke("index", &[])
    }

    fn discard_index(&self) -> PyResult<Self> {
        self.invoke("discard_index", &[])
    }

    fn discard_value(&self) -> PyResult<Self> {
        self.invoke("discard_value", &[])
    }

    fn enumerate(&self) -> PyResult<Self> {
        self.invoke("enumerate", &[])
    }

    fn errors(&self) -> PyResult<Self> {
        self.invoke("errors", &[])
    }

    fn on_error_raise(&self) -> PyResult<Self> {
        self.invoke("on_error_raise", &[])
    }

    fn on_error_drop(&self) -> PyResult<Self> {
        self.invoke("on_error_drop", &[])
    }

    fn on_error_replace(&self, replacement: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.invoke("on_error_replace", &[self.value_argument(replacement)?])
    }

    fn raise_when(&self, condition: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.invoke("raise_when", &[Self::mask_argument(condition)?])
    }

    fn kind(&self) -> PyResult<Self> {
        self.invoke("kind", &[])
    }

    fn name(&self) -> PyResult<Self> {
        self.invoke("name", &[])
    }

    fn count(&self) -> PyResult<Self> {
        self.invoke("count", &[])
    }

    fn sum(&self) -> PyResult<Self> {
        self.invoke("sum", &[])
    }

    fn mean(&self) -> PyResult<Self> {
        self.invoke("mean", &[])
    }

    fn std(&self) -> PyResult<Self> {
        self.invoke("std", &[])
    }

    fn var(&self) -> PyResult<Self> {
        self.invoke("var", &[])
    }

    fn all(&self) -> PyResult<Self> {
        self.invoke("all", &[])
    }

    fn any(&self) -> PyResult<Self> {
        self.invoke("any", &[])
    }

    fn max(&self) -> PyResult<Self> {
        self.invoke("max", &[])
    }

    fn min(&self) -> PyResult<Self> {
        self.invoke("min", &[])
    }

    fn median(&self) -> PyResult<Self> {
        self.invoke("median", &[])
    }

    fn mode(&self) -> PyResult<Self> {
        self.invoke("mode", &[])
    }

    fn product(&self) -> PyResult<Self> {
        self.invoke("product", &[])
    }

    fn n_unique(&self) -> PyResult<Self> {
        self.invoke("n_unique", &[])
    }

    fn random(&self) -> PyResult<Self> {
        self.invoke("random", &[])
    }

    fn edges(&self, direction: PyEdgeDirection) -> PyResult<Self> {
        self.invoke("edges", &[DynInvokeArgument::Direction(direction.into())])
    }

    fn neighbors(&self, direction: PyEdgeDirection) -> PyResult<Self> {
        self.invoke(
            "neighbors",
            &[DynInvokeArgument::Direction(direction.into())],
        )
    }

    fn via_edges(&self, direction: PyEdgeDirection) -> PyResult<Self> {
        self.invoke(
            "via_edges",
            &[DynInvokeArgument::Direction(direction.into())],
        )
    }

    fn via_neighbors(&self, direction: PyEdgeDirection) -> PyResult<Self> {
        self.invoke(
            "via_neighbors",
            &[DynInvokeArgument::Direction(direction.into())],
        )
    }

    fn nodes(&self) -> PyResult<Self> {
        self.invoke("nodes", &[])
    }

    fn via_nodes(&self) -> PyResult<Self> {
        self.invoke("via_nodes", &[])
    }

    fn source_node(&self) -> PyResult<Self> {
        self.via_source_node()?.select()
    }

    fn target_node(&self) -> PyResult<Self> {
        self.via_target_node()?.select()
    }

    fn via_source_node(&self) -> PyResult<Self> {
        self.invoke("via_source_node", &[])
    }

    fn via_target_node(&self) -> PyResult<Self> {
        self.invoke("via_target_node", &[])
    }

    #[getter]
    fn group_depth(&self) -> usize {
        self.operand().descriptor().group_depth()
    }

    fn group_by(&self, key: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.invoke("group_by", &[Self::scalar_argument(key)?])
    }

    fn having(&self, predicate: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.invoke("having", &[Self::mask_argument(predicate)?])
    }

    fn keys(&self) -> PyResult<Self> {
        self.invoke("keys", &[])
    }

    fn ungroup(&self) -> PyResult<Self> {
        self.invoke("ungroup", &[])
    }

    fn ungroup_keyed(&self) -> PyResult<Self> {
        self.invoke("ungroup_keyed", &[])
    }

    fn broadcast(&self) -> PyResult<Self> {
        self.invoke("broadcast", &[])
    }

    fn broadcast_via(&self, population: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.invoke("broadcast_via", &[Self::operand_argument(population)?])
    }

    fn bucket_errors(&self) -> PyResult<Self> {
        self.invoke("bucket_errors", &[])
    }

    fn key_errors(&self) -> PyResult<Self> {
        self.invoke("key_errors", &[])
    }

    fn on_bucket_error_drop(&self) -> PyResult<Self> {
        self.invoke("on_bucket_error_drop", &[])
    }

    fn on_bucket_error_raise(&self) -> PyResult<Self> {
        self.invoke("on_bucket_error_raise", &[])
    }

    fn on_key_error_drop(&self) -> PyResult<Self> {
        self.invoke("on_key_error_drop", &[])
    }

    fn on_key_error_raise(&self) -> PyResult<Self> {
        self.invoke("on_key_error_raise", &[])
    }

    fn transition(&self, target: PyValueTarget) -> PyResult<Self> {
        self.invoke(
            "transition",
            &[DynInvokeArgument::ValueTarget(target.into())],
        )
    }

    fn expand_to(&self, values: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.invoke("expand_to", &[Self::scalar_argument(values)?])
    }
}

impl PyOperand {
    const fn operand(&self) -> &DynOperand {
        &self.0
    }

    fn invoke(&self, method: &str, arguments: &[DynInvokeArgument]) -> PyResult<Self> {
        self.operand()
            .invoke(method, arguments)
            .map(Self)
            .map_err(|failure| failure.to_python_error())
    }
}
