use pyo3::{
    create_exception,
    exceptions::{PyException, PyRuntimeError},
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
