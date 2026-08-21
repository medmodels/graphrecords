use crate::querying::{
    exception::FailureConversion, index_conversion::IndexConversion,
    value_conversion::ValueConversion,
};
use graphrecords_query::{
    QueryResult,
    dynamic::{
        DynArityContainer, DynIndexOwned, DynTerminal, DynTerminalLane, DynTerminalPartition,
        DynValue,
    },
};
use pyo3::{
    IntoPyObjectExt,
    exceptions::{PyKeyError, PyValueError},
    prelude::*,
    types::{PyDict, PyList},
};

pub(super) trait TerminalConversion {
    fn into_python(self, py: Python<'_>) -> PyResult<Py<PyAny>>;
}

impl TerminalConversion for DynTerminal {
    fn into_python(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self {
            Self::Lane(lane) => lane.into_python(py),
            Self::Group(partition) => PyGroupedResult::new(py, partition)?.into_py_any(py),
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
            Self::IndexedValue(container) => {
                PyResultView::from_container(py, container, LaneElements::IndexedValue)
            }
            Self::IndexedMask(container) => {
                PyResultView::from_container(py, container, LaneElements::IndexedMask)
            }
            Self::IndexedUnit(container) => {
                PyResultView::from_container(py, container, LaneElements::IndexedUnit)
            }
            Self::BareValue(container) => {
                PyResultView::from_container(py, container, LaneElements::BareValue)
            }
            Self::BareMask(container) => {
                PyResultView::from_container(py, container, LaneElements::BareMask)
            }
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

enum LaneElements {
    IndexedValue(std::vec::IntoIter<(DynIndexOwned, QueryResult<DynValue>)>),
    IndexedMask(std::vec::IntoIter<(DynIndexOwned, QueryResult<bool>)>),
    IndexedUnit(std::vec::IntoIter<(DynIndexOwned, QueryResult<()>)>),
    BareValue(std::vec::IntoIter<QueryResult<DynValue>>),
    BareMask(std::vec::IntoIter<QueryResult<bool>>),
}

impl LaneElements {
    fn next(&mut self, py: Python<'_>) -> Option<PyResult<Py<PyAny>>> {
        match self {
            Self::IndexedValue(elements) => elements.next().map(|element| element.into_python(py)),
            Self::IndexedMask(elements) => elements.next().map(|element| element.into_python(py)),
            Self::IndexedUnit(elements) => elements.next().map(|element| element.into_python(py)),
            Self::BareValue(elements) => elements.next().map(|element| element.into_python(py)),
            Self::BareMask(elements) => elements.next().map(|element| element.into_python(py)),
        }
    }
}

#[pyclass(module = "graphrecords._graphrecords.querying")]
pub struct PyResultView {
    elements: LaneElements,
}

impl PyResultView {
    fn from_container<T: TerminalConversion>(
        py: Python<'_>,
        container: DynArityContainer<T>,
        variant: fn(std::vec::IntoIter<T>) -> LaneElements,
    ) -> PyResult<Py<PyAny>> {
        match container {
            DynArityContainer::MultipleOrdered(elements)
            | DynArityContainer::MultipleUnordered(elements) => Self {
                elements: variant(elements.into_iter()),
            }
            .into_py_any(py),
            DynArityContainer::Single(Some(element)) | DynArityContainer::Definite(element) => {
                element.into_python(py)
            }
            DynArityContainer::Single(None) => Ok(py.None()),
        }
    }
}

#[pymethods]
impl PyResultView {
    const fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        self.elements.next(py).transpose()
    }
}

#[pyclass(frozen, module = "graphrecords._graphrecords.querying")]
pub struct PyGroupedResult {
    buckets: Py<PyDict>,
    key_failures: Py<PyList>,
}

impl PyGroupedResult {
    fn new(py: Python<'_>, partition: DynTerminalPartition) -> PyResult<Self> {
        let (buckets, key_failures) = partition.into_parts();
        let bucket_count = buckets.len();

        let realized = PyDict::new(py);
        for bucket in buckets {
            let (key, _, payload) = bucket.into_parts();
            let key = key.to_python(py)?;
            let payload = payload.into_python(py)?;

            realized.set_item(key, payload)?;
        }
        if realized.len() != bucket_count {
            return Err(PyValueError::new_err(
                "distinct group keys collide after python conversion",
            ));
        }

        let failures = key_failures
            .into_iter()
            .map(|key_failure| {
                let (member, failure) = key_failure.into_parts();
                let member = member.to_python(py)?;
                let failure = failure.to_python(py);

                (member, failure).into_py_any(py)
            })
            .collect::<PyResult<Vec<_>>>()?;

        Ok(Self {
            buckets: realized.unbind(),
            key_failures: PyList::new(py, failures)?.unbind(),
        })
    }
}

#[pymethods]
impl PyGroupedResult {
    fn __len__(&self, py: Python<'_>) -> usize {
        self.buckets.bind(py).len()
    }

    fn __contains__(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<bool> {
        self.buckets.bind(py).contains(key)
    }

    fn __getitem__(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.buckets
            .bind(py)
            .get_item(key)?
            .map(Bound::unbind)
            .ok_or_else(|| PyKeyError::new_err(key.clone().unbind()))
    }

    fn __iter__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(self.buckets.bind(py).try_iter()?.unbind().into_any())
    }

    fn keys(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.buckets.bind(py).keys().into_py_any(py)
    }

    #[getter]
    fn key_failures(&self, py: Python<'_>) -> Py<PyList> {
        self.key_failures.clone_ref(py)
    }
}
