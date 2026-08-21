use crate::{
    graphrecord::{PyAttributeName, PyEdgeDirection, PyGroupIndex},
    querying::{
        PyArgument, PyCastTarget, PyExpression, PyValueTarget, exception::FailureConversion,
        results::TerminalConversion, surface::expression_surface,
    },
};
use graphrecords_core::{
    errors::GraphRecordResult,
    graphrecord::{EdgeIndex, GraphRecord, GroupIndex, NodeIndex},
};
use graphrecords_query::{
    Series,
    dynamic::{DynArgumentLane, DynExpression, DynInvokeArgument},
};
use pyo3::prelude::*;

#[pyclass(frozen, module = "graphrecords._graphrecords.querying")]
pub struct PySeries(Series<DynExpression>);

impl From<Series<DynExpression>> for PySeries {
    fn from(value: Series<DynExpression>) -> Self {
        Self(value)
    }
}

impl From<PySeries> for Series<DynExpression> {
    fn from(value: PySeries) -> Self {
        value.0
    }
}

impl PySeries {
    pub(crate) const fn expression(&self) -> &DynExpression {
        self.0.expression()
    }

    fn bind(&self, expression: DynExpression) -> Self {
        Self(self.0.bind(expression))
    }

    fn invoke(&self, method: &str, arguments: &[DynInvokeArgument]) -> PyResult<Self> {
        self.expression()
            .invoke(method, arguments)
            .map(|expression| self.bind(expression))
            .map_err(|failure| failure.to_python_error())
    }

    pub(super) fn lane(&self) -> DynArgumentLane {
        DynArgumentLane::Series(Box::new(self.0.clone()))
    }

    pub(crate) fn resolve_nodes(
        &self,
        graphrecord: &GraphRecord,
    ) -> Option<GraphRecordResult<Vec<NodeIndex>>> {
        self.0.resolve_nodes(graphrecord)
    }

    pub(crate) fn resolve_edges(
        &self,
        graphrecord: &GraphRecord,
    ) -> Option<GraphRecordResult<Vec<EdgeIndex>>> {
        self.0.resolve_edges(graphrecord)
    }

    pub(crate) fn resolve_groups(
        &self,
        graphrecord: &GraphRecord,
    ) -> Option<GraphRecordResult<Vec<GroupIndex>>> {
        self.0.resolve_groups(graphrecord)
    }
}

expression_surface! {
    PySeries {
        fn cache(&self) -> Self {
            self.bind(self.expression().cache())
        }

        fn evaluate(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
            let terminal = py
                .detach(|| self.0.evaluate())
                .map_err(|failure| failure.to_python_error())?;

            terminal.into_python(py)
        }

        fn explain(&self) -> String {
            self.0.explain().to_string()
        }

        fn explain_unoptimized(&self) -> String {
            self.0.explain_unoptimized().to_string()
        }

        fn __repr__(&self) -> String {
            format!("{:?}", self.0)
        }
    }
}
