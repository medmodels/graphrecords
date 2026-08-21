use super::{PyAttributes, PyNodeIndex, errors::PyGraphRecordError, traits::DeepInto};
use arrow::{
    array::RecordBatch,
    ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream},
};
use graphrecords_core::{
    errors::GraphRecordResult,
    graphrecord::{EdgeBatch, EdgeSource, NodeBatch, NodeIndex, NodeSource},
};
use polars::frame::DataFrame;
use pyo3::{
    Borrowed, Bound, FromPyObject, PyAny, PyErr, PyResult, exceptions::PyTypeError, prelude::*,
    types::PyCapsule,
};
use pyo3_polars::PyDataFrame;

#[repr(transparent)]
pub struct PyArrowTable(Vec<RecordBatch>);

impl PyArrowTable {
    fn read(table: &Bound<'_, PyAny>) -> PyResult<Self> {
        let capsule = table.call_method0("__arrow_c_stream__")?;
        let capsule = capsule.cast::<PyCapsule>()?;
        let pointer = capsule.pointer_checked(Some(c"arrow_array_stream"))?;

        // SAFETY: The Arrow PyCapsule Interface guarantees a capsule named `arrow_array_stream`
        // holds an `FFI_ArrowArrayStream` and hands its ownership to the caller. Replacing it
        // with an empty stream marks the capsule consumed, so the producer's release callback
        // runs exactly once, from the reader below.
        let stream =
            unsafe { std::ptr::replace(pointer.as_ptr().cast(), FFI_ArrowArrayStream::empty()) };

        let reader = ArrowArrayStreamReader::try_new(stream)
            .map_err(|error| PyTypeError::new_err(error.to_string()))?;

        reader
            .collect::<Result<_, _>>()
            .map(Self)
            .map_err(|error| PyTypeError::new_err(error.to_string()))
    }
}

#[repr(transparent)]
pub struct PyNodeSource(NodeBatch);

impl PyNodeSource {
    fn from_rows(rows: &Bound<'_, PyAny>) -> PyResult<Self> {
        let batch: NodeBatch = rows
            .try_iter()
            .map_err(|_| PyTypeError::new_err("Expected a node source"))?
            .map(|element| {
                let row = element?.extract::<(PyNodeIndex, PyAttributes)>()?;

                Ok((NodeIndex::from(row.0), row.1.deep_into()))
            })
            .collect::<PyResult<_>>()?;

        Ok(Self(batch))
    }
}

impl NodeSource for PyNodeSource {
    fn collect_nodes(self) -> GraphRecordResult<NodeBatch> {
        Ok(self.0)
    }
}

impl FromPyObject<'_, '_> for PyNodeSource {
    type Error = PyErr;

    fn extract(ob: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        if let Ok(collector) = ob.getattr("collect_nodes") {
            return Self::from_rows(&collector.call0()?);
        }

        if let Ok(source) = ob.extract::<(PyDataFrame, String)>() {
            let batch = (DataFrame::from(source.0), source.1)
                .collect_nodes()
                .map_err(PyGraphRecordError::from)?;

            return Ok(Self(batch));
        }

        if let Ok(source) = ob.extract::<(Bound<'_, PyAny>, String)>()
            && source.0.hasattr("__arrow_c_stream__")?
        {
            let table = PyArrowTable::read(&source.0)?;
            let batch = (table.0, source.1)
                .collect_nodes()
                .map_err(PyGraphRecordError::from)?;

            return Ok(Self(batch));
        }

        Self::from_rows(&ob)
    }
}

#[repr(transparent)]
pub struct PyEdgeSource(EdgeBatch);

impl PyEdgeSource {
    fn from_rows(rows: &Bound<'_, PyAny>) -> PyResult<Self> {
        let batch: EdgeBatch = rows
            .try_iter()
            .map_err(|_| PyTypeError::new_err("Expected an edge source"))?
            .map(|element| {
                let row = element?.extract::<(PyNodeIndex, PyNodeIndex, PyAttributes)>()?;

                Ok((
                    NodeIndex::from(row.0),
                    NodeIndex::from(row.1),
                    row.2.deep_into(),
                ))
            })
            .collect::<PyResult<_>>()?;

        Ok(Self(batch))
    }
}

impl EdgeSource for PyEdgeSource {
    fn collect_edges(self) -> GraphRecordResult<EdgeBatch> {
        Ok(self.0)
    }
}

impl FromPyObject<'_, '_> for PyEdgeSource {
    type Error = PyErr;

    fn extract(ob: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        if let Ok(collector) = ob.getattr("collect_edges") {
            return Self::from_rows(&collector.call0()?);
        }

        if let Ok(source) = ob.extract::<(PyDataFrame, String, String)>() {
            let batch = (DataFrame::from(source.0), source.1, source.2)
                .collect_edges()
                .map_err(PyGraphRecordError::from)?;

            return Ok(Self(batch));
        }

        if let Ok(source) = ob.extract::<(Bound<'_, PyAny>, String, String)>()
            && source.0.hasattr("__arrow_c_stream__")?
        {
            let table = PyArrowTable::read(&source.0)?;
            let batch = (table.0, source.1, source.2)
                .collect_edges()
                .map_err(PyGraphRecordError::from)?;

            return Ok(Self(batch));
        }

        Self::from_rows(&ob)
    }
}
