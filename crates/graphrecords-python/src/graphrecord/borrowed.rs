use super::{PyGraphRecord, PyGraphRecordInner};
use graphrecords_core::graphrecord::GraphRecord;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use pyo3::{Py, PyResult, Python};
use std::{
    fmt::{Debug, Formatter, Result},
    ptr::NonNull,
};

/// Wrapper around a borrowed `GraphRecord` pointer, protected by an [`RwLock`].
///
/// The inner [`NonNull`] is only set to `Some` inside [`PyGraphRecord::scope`] and is
/// cleared when the scope ends. A "dead" handle (`None`) returns a runtime error
/// on every access attempt.
///
/// # Construction invariant
///
/// Only [`PyGraphRecord::scope`] (defined in this module) can create a *live*
/// `BorrowedGraphRecord`. Outside code can only obtain a *dead* handle via
/// [`BorrowedGraphRecord::dead`], because the tuple field is private to this module.
pub(super) struct BorrowedGraphRecord(RwLock<Option<NonNull<GraphRecord>>>);

// SAFETY: The `NonNull<GraphRecord>` is protected by an `RwLock`, ensuring synchronized
// access. The pointer is only valid during the `scope()` call, and the scope's Drop guard
// acquires a write lock to clear it, which cannot proceed while any read/write guard is
// held, preventing use-after-free.
unsafe impl Send for BorrowedGraphRecord {}
unsafe impl Sync for BorrowedGraphRecord {}

impl Debug for BorrowedGraphRecord {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.debug_struct("BorrowedGraphRecord")
            .field("alive", &self.0.read().is_some())
            .finish()
    }
}

impl BorrowedGraphRecord {
    /// Creates a dead handle with no pointer. Any access will return an error.
    pub(super) const fn dead() -> Self {
        Self(RwLock::new(None))
    }

    pub(super) fn read(&self) -> RwLockReadGuard<'_, Option<NonNull<GraphRecord>>> {
        self.0.read()
    }

    pub(super) fn write(&self) -> RwLockWriteGuard<'_, Option<NonNull<GraphRecord>>> {
        self.0.write()
    }
}

impl PyGraphRecord {
    /// Safely pass a `&mut GraphRecord` to Python as a `PyGraphRecord` for the
    /// duration of the callback. The pointer is invalidated when `function` returns.
    ///
    /// Based on the discussion in:
    /// - <https://github.com/PyO3/pyo3/issues/1180>
    ///
    /// Guard pattern adapted from:
    /// - <https://github.com/PyO3/pyo3/issues/1180#issuecomment-692898577>
    pub fn scope<R>(
        py: Python<'_>,
        graphrecord: &mut GraphRecord,
        function: impl FnOnce(Python<'_>, &Py<Self>) -> PyResult<R>,
    ) -> PyResult<R> {
        struct PanicOnDrop(bool);
        impl Drop for PanicOnDrop {
            fn drop(&mut self) {
                assert!(!self.0, "failed to clear PyGraphRecord borrow");
            }
        }

        struct Guard<'py>(Python<'py>, Py<PyGraphRecord>, NonNull<GraphRecord>);
        impl Drop for Guard<'_> {
            #[allow(clippy::significant_drop_tightening)]
            fn drop(&mut self) {
                let panic_on_drop = PanicOnDrop(true);
                let py_graphrecord = self.1.bind(self.0).get();
                match &py_graphrecord.inner {
                    PyGraphRecordInner::Borrowed(borrowed) => {
                        let mut guard = borrowed.write();
                        assert_eq!(
                            guard.take(),
                            Some(self.2),
                            "PyGraphRecord was tampered with"
                        );
                    }
                    PyGraphRecordInner::Owned(_) => {
                        panic!("PyGraphRecord was replaced with an owned variant");
                    }
                }
                std::mem::forget(panic_on_drop);
            }
        }

        let pointer = NonNull::from(graphrecord);
        let guard = Guard(
            py,
            Py::new(
                py,
                Self {
                    inner: PyGraphRecordInner::Borrowed(BorrowedGraphRecord(RwLock::new(Some(
                        pointer,
                    )))),
                },
            )?,
            pointer,
        );
        function(py, &guard.1)
    }
}
