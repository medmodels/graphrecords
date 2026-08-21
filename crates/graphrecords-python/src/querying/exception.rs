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
    create_exception,
    exceptions::{PyException, PyRuntimeError, PyTypeError},
    prelude::*,
};

create_exception!(
    graphrecords,
    QueryError,
    PyException,
    "Base class for every graphrecords query diagnostic."
);

create_exception!(graphrecords, InvalidMedianValueError, QueryError);
create_exception!(graphrecords, InvalidStandardDeviationValueError, QueryError);
create_exception!(graphrecords, InvalidVarianceValueError, QueryError);
create_exception!(graphrecords, ArgumentMissingError, QueryError);
create_exception!(graphrecords, DivisionByZeroError, QueryError);
create_exception!(graphrecords, ModuloByZeroError, QueryError);
create_exception!(graphrecords, IncomparableValuesError, QueryError);
create_exception!(graphrecords, IncomparableValuesAtError, QueryError);
create_exception!(graphrecords, InvalidCastError, QueryError);
create_exception!(graphrecords, InvalidTransitionError, QueryError);
create_exception!(graphrecords, UnsupportedValueRoleError, QueryError);
create_exception!(
    graphrecords,
    EvaluationCacheGraphRecordMismatchError,
    QueryError
);
create_exception!(graphrecords, InvalidPartitionBucketArityError, QueryError);
create_exception!(graphrecords, MissingGroupAggregateError, QueryError);
create_exception!(graphrecords, MissingGroupBucketError, QueryError);
create_exception!(graphrecords, UnresolvedBucketFailuresError, QueryError);
create_exception!(graphrecords, UnresolvedGroupKeyFailuresError, QueryError);
create_exception!(graphrecords, DuplicateExpandedChildIndexError, QueryError);
create_exception!(graphrecords, DuplicateIndexError, QueryError);
create_exception!(graphrecords, NoChildIndexError, QueryError);
create_exception!(graphrecords, UncoveredIndicesError, QueryError);
create_exception!(graphrecords, UnresolvedIndexError, QueryError);
create_exception!(graphrecords, IntegerOverflowError, QueryError);
create_exception!(graphrecords, InvalidClipBoundsError, QueryError);
create_exception!(graphrecords, NegativeSquareRootError, QueryError);
create_exception!(graphrecords, NonIntegerValueError, QueryError);
create_exception!(graphrecords, NonNumericValueError, QueryError);
create_exception!(graphrecords, NonPositiveLogarithmError, QueryError);
create_exception!(graphrecords, IncomparableIndicesError, QueryError);
create_exception!(graphrecords, RaisedFailuresError, QueryError);
create_exception!(graphrecords, EmptySplitDelimiterError, QueryError);
create_exception!(graphrecords, InvalidPaddingCharacterError, QueryError);
create_exception!(graphrecords, InvalidRegexPatternError, QueryError);
create_exception!(graphrecords, InvalidStringSliceError, QueryError);
create_exception!(graphrecords, NonStringValueError, QueryError);
create_exception!(graphrecords, StringLengthOverflowError, QueryError);
create_exception!(graphrecords, StringPaddingOverflowError, QueryError);
create_exception!(graphrecords, MissingAttributeError, QueryError);
create_exception!(graphrecords, MissingTraversedAttributeError, QueryError);
create_exception!(graphrecords, ExternalError, QueryError);
create_exception!(graphrecords, GraphRecordError, QueryError);

create_exception!(
    graphrecords,
    ResultConsumedError,
    PyRuntimeError,
    "The result view was already iterated; call `evaluate()` again or collect it into a list."
);

pub trait FailureConversion {
    fn to_python_error(&self) -> PyErr;

    fn to_python(&self, py: Python<'_>) -> Py<PyAny>;
}

impl FailureConversion for Failure {
    fn to_python_error(&self) -> PyErr {
        let message = self.to_string();

        match self.kind().name() {
            name if name == InvalidMedianValue::name() => InvalidMedianValueError::new_err(message),
            name if name == InvalidStandardDeviationValue::name() => {
                InvalidStandardDeviationValueError::new_err(message)
            }
            name if name == InvalidVarianceValue::name() => {
                InvalidVarianceValueError::new_err(message)
            }
            name if name == ArgumentMissing::name() => ArgumentMissingError::new_err(message),
            name if name == DivisionByZero::name() => DivisionByZeroError::new_err(message),
            name if name == ModuloByZero::name() => ModuloByZeroError::new_err(message),
            name if name == <IncomparableValues<Value>>::name() => {
                IncomparableValuesError::new_err(message)
            }
            name if name == <IncomparableValuesAt<Value, NodeIndex>>::name() => {
                IncomparableValuesAtError::new_err(message)
            }
            name if name == <InvalidCast<Value>>::name() => InvalidCastError::new_err(message),
            name if name == <InvalidTransition<Value>>::name() => {
                InvalidTransitionError::new_err(message)
            }
            name if name == UnsupportedValueRole::name() => {
                UnsupportedValueRoleError::new_err(message)
            }
            name if name == EvaluationCacheGraphRecordMismatch::name() => {
                EvaluationCacheGraphRecordMismatchError::new_err(message)
            }
            name if name == InvalidPartitionBucketArity::name() => {
                InvalidPartitionBucketArityError::new_err(message)
            }
            name if name == MissingGroupAggregate::name() => {
                MissingGroupAggregateError::new_err(message)
            }
            name if name == MissingGroupBucket::name() => MissingGroupBucketError::new_err(message),
            name if name == UnresolvedBucketFailures::name() => {
                UnresolvedBucketFailuresError::new_err(message)
            }
            name if name == UnresolvedGroupKeyFailures::name() => {
                UnresolvedGroupKeyFailuresError::new_err(message)
            }
            name if name == <DuplicateExpandedChildIndex<NodeIndex>>::name() => {
                DuplicateExpandedChildIndexError::new_err(message)
            }
            name if name == <DuplicateIndex<NodeIndex>>::name() => {
                DuplicateIndexError::new_err(message)
            }
            name if name == <NoChildIndex<NodeIndex>>::name() => {
                NoChildIndexError::new_err(message)
            }
            name if name == <UncoveredIndices<NodeIndex>>::name() => {
                UncoveredIndicesError::new_err(message)
            }
            name if name == <UnresolvedIndex<NodeIndex>>::name() => {
                UnresolvedIndexError::new_err(message)
            }
            name if name == <IntegerOverflow<Value>>::name() => {
                IntegerOverflowError::new_err(message)
            }
            name if name == <InvalidClipBounds<Value>>::name() => {
                InvalidClipBoundsError::new_err(message)
            }
            name if name == NegativeSquareRoot::name() => NegativeSquareRootError::new_err(message),
            name if name == <NonIntegerValue<Value>>::name() => {
                NonIntegerValueError::new_err(message)
            }
            name if name == <NonNumericValue<Value>>::name() => {
                NonNumericValueError::new_err(message)
            }
            name if name == NonPositiveLogarithm::name() => {
                NonPositiveLogarithmError::new_err(message)
            }
            name if name == <IncomparableIndices<Value, NodeIndex>>::name() => {
                IncomparableIndicesError::new_err(message)
            }
            name if name == RaisedFailures::name() => RaisedFailuresError::new_err(message),
            name if name == EmptySplitDelimiter::name() => {
                EmptySplitDelimiterError::new_err(message)
            }
            name if name == InvalidPaddingCharacter::name() => {
                InvalidPaddingCharacterError::new_err(message)
            }
            name if name == InvalidRegexPattern::name() => {
                InvalidRegexPatternError::new_err(message)
            }
            name if name == InvalidStringSlice::name() => InvalidStringSliceError::new_err(message),
            name if name == <NonStringValue<Value>>::name() => {
                NonStringValueError::new_err(message)
            }
            name if name == StringLengthOverflow::name() => {
                StringLengthOverflowError::new_err(message)
            }
            name if name == StringPaddingOverflow::name() => {
                StringPaddingOverflowError::new_err(message)
            }
            name if name == MissingAttribute::name() => MissingAttributeError::new_err(message),
            name if name == <MissingTraversedAttribute<NodeIndex>>::name() => {
                MissingTraversedAttributeError::new_err(message)
            }
            name if name == <External<CoreGraphRecordError>>::name() => {
                ExternalError::new_err(message)
            }
            name if name == CoreGraphRecordError::name() => GraphRecordError::new_err(message),
            name if name == OperationNotApplicable::name() => PyTypeError::new_err(message),
            _ => QueryError::new_err(message),
        }
    }

    fn to_python(&self, py: Python<'_>) -> Py<PyAny> {
        self.to_python_error().into_value(py).into_any()
    }
}
