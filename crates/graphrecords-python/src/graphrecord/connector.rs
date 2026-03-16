use crate::prelude::PyGraphRecord;
use graphrecords_core::{
    GraphRecord,
    errors::{GraphRecordError, GraphRecordResult},
    graphrecord::connector::{Connector, ExportConnector, IngestConnector},
};
use pyo3::{Py, PyAny, Python, types::PyAnyMethods};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug)]
pub struct PyConnector(Py<PyAny>);

impl PyConnector {
    pub const fn new(connector: Py<PyAny>) -> Self {
        Self(connector)
    }
}

impl Clone for PyConnector {
    fn clone(&self) -> Self {
        Python::attach(|py| Self(self.0.clone_ref(py)))
    }
}

impl Serialize for PyConnector {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        Python::attach(|py| {
            let cloudpickle = py
                .import("cloudpickle")
                .map_err(serde::ser::Error::custom)?;

            let bytes: Vec<u8> = cloudpickle
                .call_method1("dumps", (&self.0,))
                .map_err(serde::ser::Error::custom)?
                .extract()
                .map_err(serde::ser::Error::custom)?;

            serializer.serialize_bytes(&bytes)
        })
    }
}

impl<'de> Deserialize<'de> for PyConnector {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes: Vec<u8> = Deserialize::deserialize(deserializer)?;

        Python::attach(|py| {
            let cloudpickle = py.import("cloudpickle").map_err(serde::de::Error::custom)?;

            let obj: Py<PyAny> = cloudpickle
                .call_method1("loads", (bytes.as_slice(),))
                .map_err(serde::de::Error::custom)?
                .into();

            Ok(Self(obj))
        })
    }
}

impl Connector for PyConnector {
    fn initialize(&self, graphrecord: &mut GraphRecord) -> GraphRecordResult<()> {
        Python::attach(|py| {
            PyGraphRecord::scope_mut(py, graphrecord, |py, graphrecord| {
                self.0
                    .call_method1(py, "initialize", (graphrecord,))
                    .map_err(|err| GraphRecordError::ConversionError(format!("{err}")))?;

                Ok(())
            })
        })
    }

    fn disconnect(&self, graphrecord: &mut GraphRecord) -> GraphRecordResult<()> {
        Python::attach(|py| {
            PyGraphRecord::scope_mut(py, graphrecord, |py, graphrecord| {
                self.0
                    .call_method1(py, "disconnect", (graphrecord,))
                    .map_err(|err| GraphRecordError::ConversionError(format!("{err}")))?;

                Ok(())
            })
        })
    }
}

impl IngestConnector for PyConnector {
    type DataSet = Py<PyAny>;

    fn ingest(&self, graphrecord: &mut GraphRecord, data: Self::DataSet) -> GraphRecordResult<()> {
        Python::attach(|py| {
            PyGraphRecord::scope_mut(py, graphrecord, |py, graphrecord| {
                self.0
                    .call_method1(py, "ingest", (graphrecord, data))
                    .map_err(|err| GraphRecordError::ConversionError(format!("{err}")))?;

                Ok(())
            })
        })
    }
}

impl ExportConnector for PyConnector {
    type DataSet = Py<PyAny>;

    fn export(&self, graphrecord: &GraphRecord) -> GraphRecordResult<Self::DataSet> {
        Python::attach(|py| {
            PyGraphRecord::scope(py, graphrecord, |py, graphrecord| {
                let data = self
                    .0
                    .call_method1(py, "export", (graphrecord,))
                    .map_err(|err| GraphRecordError::ConversionError(format!("{err}")))?;

                Ok(data)
            })
        })
    }
}
