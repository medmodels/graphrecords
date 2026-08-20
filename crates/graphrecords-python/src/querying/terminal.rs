use crate::querying::{
    PyOperand, exception::FailureConversion, index_conversion::IndexConversion,
    value_conversion::ValueConversion,
};
use graphrecords_core::GraphRecord;
use graphrecords_query::{
    QueryResult,
    dynamic::{
        DynArityContainer, DynIndexOwned, DynOperand, DynTerminal, DynTerminalLane, DynValue,
        query_edges, query_nodes,
    },
};
use pyo3::{
    prelude::*,
    types::{PyFunction, PyList},
};

pub(super) trait TerminalConversion {
    fn into_python(self, py: Python<'_>) -> PyResult<Py<PyAny>>;
}

impl PyOperand {
    pub(crate) fn query_nodes(
        graphrecord: &GraphRecord,
        query: &Bound<'_, PyFunction>,
    ) -> PyResult<Py<PyAny>> {
        Self::evaluate(graphrecord, query, query_nodes())
    }

    pub(crate) fn query_edges(
        graphrecord: &GraphRecord,
        query: &Bound<'_, PyFunction>,
    ) -> PyResult<Py<PyAny>> {
        Self::evaluate(graphrecord, query, query_edges())
    }

    fn evaluate(
        graphrecord: &GraphRecord,
        query: &Bound<'_, PyFunction>,
        root: DynOperand,
    ) -> PyResult<Py<PyAny>> {
        let returned = query.call1((Self(root),))?;
        let operand = returned.cast::<Self>()?.get().operand().clone();
        let terminal = operand
            .evaluate(graphrecord)
            .map_err(|failure| failure.to_python_error())?;

        terminal.into_python(query.py())
    }
}

impl TerminalConversion for DynTerminal {
    fn into_python(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self {
            Self::Lane(lane) => lane.into_python(py),
            Self::Group(partition) => {
                let (buckets, key_failures) = partition.into_parts();
                let buckets = buckets
                    .into_iter()
                    .map(|bucket| {
                        let (key, members, payload) = bucket.into_parts();
                        let key = key.to_python(py)?;
                        let members = members
                            .iter()
                            .map(|member| member.to_python(py))
                            .collect::<PyResult<Vec<_>>>()?;
                        let members = PyList::new(py, members)?;
                        let payload = payload.into_python(py)?;

                        Ok((key, members, payload)
                            .into_pyobject(py)?
                            .into_any()
                            .unbind())
                    })
                    .collect::<PyResult<Vec<_>>>()?;
                let key_failures = key_failures
                    .into_iter()
                    .map(|key_failure| {
                        let (member, failure) = key_failure.into_parts();
                        let member = member.to_python(py)?;
                        let failure = failure.to_python(py);

                        Ok((member, failure).into_pyobject(py)?.into_any().unbind())
                    })
                    .collect::<PyResult<Vec<_>>>()?;

                Ok((PyList::new(py, buckets)?, PyList::new(py, key_failures)?)
                    .into_pyobject(py)?
                    .into_any()
                    .unbind())
            }
        }
    }
}

impl TerminalConversion for QueryResult<DynTerminal> {
    fn into_python(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self {
            Ok(terminal) => terminal.into_python(py),
            Err(failure) => Ok(failure.to_python(py)),
        }
    }
}

impl TerminalConversion for DynTerminalLane {
    fn into_python(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self {
            Self::IndexedValue(elements) => elements.into_python(py),
            Self::IndexedMask(elements) => elements.into_python(py),
            Self::IndexedUnit(elements) => elements.into_python(py),
            Self::BareValue(elements) => elements.into_python(py),
            Self::BareMask(elements) => elements.into_python(py),
        }
    }
}

impl<T: TerminalConversion> TerminalConversion for DynArityContainer<T> {
    fn into_python(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self {
            Self::MultipleOrdered(elements) | Self::MultipleUnordered(elements) => {
                let elements = elements
                    .into_iter()
                    .map(|element| element.into_python(py))
                    .collect::<PyResult<Vec<_>>>()?;

                Ok(PyList::new(py, elements)?.into_any().unbind())
            }
            Self::Single(element) => match element {
                Some(element) => element.into_python(py),
                None => Ok(py.None()),
            },
            Self::Definite(element) => element.into_python(py),
        }
    }
}

impl TerminalConversion for (DynIndexOwned, QueryResult<DynValue>) {
    fn into_python(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let index = self.0.to_python(py)?;
        let value = self.1.into_python(py)?;

        Ok((index, value).into_pyobject(py)?.into_any().unbind())
    }
}

impl TerminalConversion for (DynIndexOwned, QueryResult<bool>) {
    fn into_python(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let index = self.0.to_python(py)?;
        let value = self.1.into_python(py)?;

        Ok((index, value).into_pyobject(py)?.into_any().unbind())
    }
}

impl TerminalConversion for (DynIndexOwned, QueryResult<()>) {
    fn into_python(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self.1 {
            Ok(()) => self.0.to_python(py),
            Err(failure) => Ok(failure.to_python(py)),
        }
    }
}

impl TerminalConversion for QueryResult<DynValue> {
    fn into_python(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self {
            Ok(value) => value.to_python(py),
            Err(failure) => Ok(failure.to_python(py)),
        }
    }
}

impl TerminalConversion for QueryResult<bool> {
    fn into_python(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self {
            Ok(value) => Ok(value.into_pyobject(py)?.to_owned().into_any().unbind()),
            Err(failure) => Ok(failure.to_python(py)),
        }
    }
}
