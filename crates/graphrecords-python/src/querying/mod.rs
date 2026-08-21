mod argument;
mod cast_target;
mod endpoint;
pub(crate) mod exception;
mod failure_kind;
mod index_conversion;
mod results;
mod series;
mod surface;
mod value_conversion;
mod value_target;

use crate::{
    graphrecord::{PyAttributeName, PyEdgeDirection, PyGroupIndex},
    querying::exception::FailureConversion,
};
pub use argument::PyArgument;
pub use cast_target::PyCastTarget;
pub use endpoint::PyEdgeEndpointRole;
pub use exception::{
    ArgumentMissingError, DivisionByZeroError, DuplicateExpandedChildIndexError,
    DuplicateIndexError, EmptySplitDelimiterError, EvaluationCacheGraphRecordMismatchError,
    ExternalError, GraphRecordError, IncomparableIndicesError, IncomparableValuesAtError,
    IncomparableValuesError, IntegerOverflowError, InvalidCastError, InvalidClipBoundsError,
    InvalidMedianValueError, InvalidPaddingCharacterError, InvalidPartitionBucketArityError,
    InvalidRegexPatternError, InvalidStandardDeviationValueError, InvalidStringSliceError,
    InvalidTransitionError, InvalidVarianceValueError, MissingAttributeError,
    MissingGroupAggregateError, MissingGroupBucketError, MissingTraversedAttributeError,
    ModuloByZeroError, NegativeSquareRootError, NoChildIndexError, NonIntegerValueError,
    NonNumericValueError, NonPositiveLogarithmError, NonStringValueError, QueryError,
    RaisedFailuresError, ResultConsumedError, StringLengthOverflowError,
    StringPaddingOverflowError, UncoveredIndicesError, UnresolvedBucketFailuresError,
    UnresolvedGroupKeyFailuresError, UnresolvedIndexError, UnsupportedValueRoleError,
};
pub use failure_kind::PyFailureKind;
use graphrecords_core::{
    errors::GraphRecordResult,
    graphrecord::{EdgeIndex, GraphRecord, GroupIndex, NodeIndex},
};
use graphrecords_query::{
    dynamic::{self, DynArgumentLane, DynExpression, DynInvokeArgument},
    registry::{ExpressionDescriptor, IndexDescriptor, LaneShapeDescriptor, ValueRole},
};
use pyo3::prelude::*;
pub use results::{PyGroupedResult, PyResultView};
pub use series::PySeries;
use surface::expression_surface;
pub use value_target::PyValueTarget;

#[pyclass(frozen, module = "graphrecords._graphrecords.querying")]
pub struct PyExpression(DynExpression);

impl From<DynExpression> for PyExpression {
    fn from(value: DynExpression) -> Self {
        Self(value)
    }
}

impl From<PyExpression> for DynExpression {
    fn from(value: PyExpression) -> Self {
        value.0
    }
}

impl PyExpression {
    pub(crate) const fn expression(&self) -> &DynExpression {
        &self.0
    }

    fn invoke(&self, method: &str, arguments: &[DynInvokeArgument]) -> PyResult<Self> {
        self.expression()
            .invoke(method, arguments)
            .map(Self::from)
            .map_err(|failure| failure.to_python_error())
    }

    fn lane(&self) -> DynArgumentLane {
        DynArgumentLane::Expression(self.0.clone())
    }

    pub(super) fn group_entity_lane(descriptor: &ExpressionDescriptor) -> bool {
        match descriptor.lane_shape() {
            LaneShapeDescriptor::Indexed { index, value } => match value.role() {
                ValueRole::EntityReference(IndexDescriptor::Domain(domain)) => {
                    domain.is::<GroupIndex>()
                }
                ValueRole::Unit => {
                    matches!(index, IndexDescriptor::Domain(domain) if domain.is::<GroupIndex>())
                }
                ValueRole::EntityReference(_) | ValueRole::Value | ValueRole::Index(_) => false,
            },
            LaneShapeDescriptor::Bare { .. } => false,
        }
    }

    pub(crate) fn resolve_nodes(
        &self,
        graphrecord: &GraphRecord,
    ) -> Option<GraphRecordResult<Vec<NodeIndex>>> {
        self.expression().resolve_nodes(graphrecord)
    }

    pub(crate) fn resolve_edges(
        &self,
        graphrecord: &GraphRecord,
    ) -> Option<GraphRecordResult<Vec<EdgeIndex>>> {
        self.expression().resolve_edges(graphrecord)
    }

    pub(crate) fn resolve_groups(
        &self,
        graphrecord: &GraphRecord,
    ) -> Option<GraphRecordResult<Vec<GroupIndex>>> {
        self.expression().resolve_groups(graphrecord)
    }
}

expression_surface! {
    PyExpression {
        fn cache(&self) -> Self {
            Self::from(self.expression().cache())
        }

        fn explain(&self) -> String {
            self.expression().explanation().to_string()
        }

        fn __repr__(&self) -> String {
            format!("{:?}", self.expression())
        }
    }
}

#[pyfunction]
pub fn nodes() -> PyExpression {
    dynamic::nodes().into()
}

#[pyfunction]
pub fn edges() -> PyExpression {
    dynamic::edges().into()
}

#[pyfunction]
pub fn groups() -> PyExpression {
    dynamic::groups().into()
}
