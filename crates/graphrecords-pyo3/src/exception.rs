use graphrecords_core::{
    errors::GraphRecordError as CoreGraphRecordError,
    graphrecord::{NodeIndex, Value},
};
use graphrecords_query::{
    Diagnostic, External, Failure,
    error::{
        aggregation::{InvalidMedianValue, InvalidStandardDeviationValue, InvalidVarianceValue},
        argument::ArgumentMissing,
        arithmetic::{DivisionByZero, ModuloByZero},
        comparison::{IncomparableValues, IncomparableValuesAt},
        conversion::{InvalidCast, InvalidTransition},
        dispatch::{OperationNotApplicable, UnsupportedValueRole},
        execution::EvaluationCacheGraphRecordMismatch,
        grouping::{
            InvalidPartitionBucketArity, MissingGroupAggregate, MissingGroupBucket,
            UnresolvedBucketFailures, UnresolvedGroupKeyFailures,
        },
        index::{
            DuplicateExpandedChildIndex, DuplicateIndex, NoChildIndex, UncoveredIndices,
            UnresolvedIndex,
        },
        numeric::{
            IntegerOverflow, InvalidClipBounds, NegativeSquareRoot, NonIntegerValue,
            NonNumericValue, NonPositiveLogarithm,
        },
        ordering::IncomparableIndices,
        policy::RaisedFailures,
        string::{
            EmptySplitDelimiter, InvalidPaddingCharacter, InvalidRegexPattern, InvalidStringSlice,
            NonStringValue, StringLengthOverflow, StringPaddingOverflow,
        },
        structure::{MissingAttribute, MissingTraversedAttribute},
    },
};
use pyo3::{
    exceptions::PyTypeError,
    prelude::*,
    sync::PyOnceLock,
    types::{PyModule, PyType},
};

pub trait FailureConversion {
    fn to_python_error(&self) -> PyErr;

    fn to_python(&self, py: Python<'_>) -> Py<PyAny>;
}

impl FailureConversion for Failure {
    fn to_python_error(&self) -> PyErr {
        let message = self.to_string();

        if self.is_kind::<OperationNotApplicable>() {
            return PyTypeError::new_err(message);
        }

        Python::attach(|py| {
            QueryException::class(py, QueryException::name(self))
                .map_or_else(|error| error, |class| PyErr::from_type(class, message))
        })
    }

    fn to_python(&self, py: Python<'_>) -> Py<PyAny> {
        self.to_python_error().into_value(py).into_any()
    }
}

struct QueryException;

impl QueryException {
    fn class<'py>(py: Python<'py>, name: &str) -> PyResult<Bound<'py, PyType>> {
        static QUERYING: PyOnceLock<Py<PyModule>> = PyOnceLock::new();

        let querying = QUERYING.get_or_try_init(py, || {
            py.import("graphrecords._graphrecords.querying")
                .map(Bound::unbind)
        })?;

        Ok(querying.bind(py).getattr(name)?.cast_into()?)
    }

    fn name(failure: &Failure) -> &'static str {
        match failure.kind().name() {
            name if name == InvalidMedianValue::name() => "InvalidMedianValueError",
            name if name == InvalidStandardDeviationValue::name() => {
                "InvalidStandardDeviationValueError"
            }
            name if name == InvalidVarianceValue::name() => "InvalidVarianceValueError",
            name if name == ArgumentMissing::name() => "ArgumentMissingError",
            name if name == DivisionByZero::name() => "DivisionByZeroError",
            name if name == ModuloByZero::name() => "ModuloByZeroError",
            name if name == <IncomparableValues<Value>>::name() => "IncomparableValuesError",
            name if name == <IncomparableValuesAt<Value, NodeIndex>>::name() => {
                "IncomparableValuesAtError"
            }
            name if name == <InvalidCast<Value>>::name() => "InvalidCastError",
            name if name == <InvalidTransition<Value>>::name() => "InvalidTransitionError",
            name if name == UnsupportedValueRole::name() => "UnsupportedValueRoleError",
            name if name == EvaluationCacheGraphRecordMismatch::name() => {
                "EvaluationCacheGraphRecordMismatchError"
            }
            name if name == InvalidPartitionBucketArity::name() => {
                "InvalidPartitionBucketArityError"
            }
            name if name == MissingGroupAggregate::name() => "MissingGroupAggregateError",
            name if name == MissingGroupBucket::name() => "MissingGroupBucketError",
            name if name == UnresolvedBucketFailures::name() => "UnresolvedBucketFailuresError",
            name if name == UnresolvedGroupKeyFailures::name() => "UnresolvedGroupKeyFailuresError",
            name if name == <DuplicateExpandedChildIndex<NodeIndex>>::name() => {
                "DuplicateExpandedChildIndexError"
            }
            name if name == <DuplicateIndex<NodeIndex>>::name() => "DuplicateIndexError",
            name if name == <NoChildIndex<NodeIndex>>::name() => "NoChildIndexError",
            name if name == <UncoveredIndices<NodeIndex>>::name() => "UncoveredIndicesError",
            name if name == <UnresolvedIndex<NodeIndex>>::name() => "UnresolvedIndexError",
            name if name == <IntegerOverflow<Value>>::name() => "IntegerOverflowError",
            name if name == <InvalidClipBounds<Value>>::name() => "InvalidClipBoundsError",
            name if name == NegativeSquareRoot::name() => "NegativeSquareRootError",
            name if name == <NonIntegerValue<Value>>::name() => "NonIntegerValueError",
            name if name == <NonNumericValue<Value>>::name() => "NonNumericValueError",
            name if name == NonPositiveLogarithm::name() => "NonPositiveLogarithmError",
            name if name == <IncomparableIndices<Value, NodeIndex>>::name() => {
                "IncomparableIndicesError"
            }
            name if name == RaisedFailures::name() => "RaisedFailuresError",
            name if name == EmptySplitDelimiter::name() => "EmptySplitDelimiterError",
            name if name == InvalidPaddingCharacter::name() => "InvalidPaddingCharacterError",
            name if name == InvalidRegexPattern::name() => "InvalidRegexPatternError",
            name if name == InvalidStringSlice::name() => "InvalidStringSliceError",
            name if name == <NonStringValue<Value>>::name() => "NonStringValueError",
            name if name == StringLengthOverflow::name() => "StringLengthOverflowError",
            name if name == StringPaddingOverflow::name() => "StringPaddingOverflowError",
            name if name == MissingAttribute::name() => "MissingAttributeError",
            name if name == <MissingTraversedAttribute<NodeIndex>>::name() => {
                "MissingTraversedAttributeError"
            }
            name if name == <External<CoreGraphRecordError>>::name() => "ExternalError",
            name if name == CoreGraphRecordError::name() => "GraphRecordError",
            _ => "QueryError",
        }
    }
}
