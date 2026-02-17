use crate::prelude::PyGraphRecord;
use graphrecords_core::{GraphRecord, graphrecord::plugins::Plugin};
use pyo3::{Py, PyAny, Python, types::PyAnyMethods};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug)]
pub struct PyPlugin(Py<PyAny>);

impl Serialize for PyPlugin {
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

impl<'de> Deserialize<'de> for PyPlugin {
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

impl PyPlugin {
    pub const fn new(py_obj: Py<PyAny>) -> Self {
        Self(py_obj)
    }
}

#[typetag::serde]
impl Plugin for PyPlugin {
    fn initialize(&self, graphrecord: &mut GraphRecord) {
        Python::attach(|py| {
            PyGraphRecord::scope(py, graphrecord, |py, graphrecord| {
                self.0.call_method1(py, "initialize", (graphrecord,))?;
                Ok(())
            })
            .expect("Python initialize() failed");
        });
    }

    fn clone_box(&self) -> Box<dyn Plugin> {
        Python::attach(|py| Box::new(Self(self.0.clone_ref(py))))
    }
}
