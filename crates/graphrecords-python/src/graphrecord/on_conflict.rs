use graphrecords_core::graphrecord::OnConflict;
use pyo3::prelude::*;

#[pyclass(
    frozen,
    eq,
    eq_int,
    hash,
    module = "graphrecords._graphrecords.graphrecord"
)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PyOnConflict {
    Raise = 0,
    KeepSelf = 1,
    KeepOther = 2,
}

impl From<OnConflict> for PyOnConflict {
    fn from(value: OnConflict) -> Self {
        match value {
            OnConflict::Raise => Self::Raise,
            OnConflict::KeepSelf => Self::KeepSelf,
            OnConflict::KeepOther => Self::KeepOther,
        }
    }
}

impl From<PyOnConflict> for OnConflict {
    fn from(value: PyOnConflict) -> Self {
        match value {
            PyOnConflict::Raise => Self::Raise,
            PyOnConflict::KeepSelf => Self::KeepSelf,
            PyOnConflict::KeepOther => Self::KeepOther,
        }
    }
}
