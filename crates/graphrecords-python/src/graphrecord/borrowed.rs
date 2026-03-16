use super::{PyGraphRecord, PyGraphRecordInner};
use graphrecords_core::{
    GraphRecord,
    errors::{GraphRecordError, GraphRecordResult},
};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use pyo3::{Py, Python};
use std::{
    fmt::{Debug, Formatter, Result},
    ptr::NonNull,
};

/// Wrapper around a borrowed `GraphRecord` pointer, protected by an [`RwLock`].
///
/// The inner [`NonNull`] is only set to `Some` inside [`PyGraphRecord::scope`] /
/// [`PyGraphRecord::scope_mut`] and is cleared when the scope ends. A "dead" handle
/// (`None`) returns a runtime error on every access attempt.
///
/// The `mutable` flag tracks whether the pointer originated from `&mut GraphRecord`
/// (via [`PyGraphRecord::scope_mut`]) or `&GraphRecord` (via [`PyGraphRecord::scope`]).
/// When `mutable` is `false`, [`PyGraphRecord::inner_mut`] refuses to hand out an
/// `InnerRefMut`, preventing `NonNull::as_mut()` from ever being called on a pointer
/// that came from a shared reference.
///
/// # Construction invariant
///
/// Only [`PyGraphRecord::scope`] and [`PyGraphRecord::scope_mut`] (defined in this
/// module) can create a *live* `BorrowedGraphRecord`. Outside code can only obtain a
/// *dead* handle via [`BorrowedGraphRecord::dead`], because the fields are private to
/// this module.
pub(super) struct BorrowedGraphRecord {
    ptr: RwLock<Option<NonNull<GraphRecord>>>,
    mutable: bool,
}

// SAFETY: The `NonNull<GraphRecord>` is protected by an `RwLock`, ensuring synchronized
// access. The pointer is only valid during the `scope()`/`scope_mut()` call, and the
// scope's Drop guard acquires a write lock to clear it, which cannot proceed while any
// read/write guard is held, preventing use-after-free. The `mutable` field is set at
// construction and never modified, so concurrent reads are safe.
unsafe impl Send for BorrowedGraphRecord {}
unsafe impl Sync for BorrowedGraphRecord {}

impl Debug for BorrowedGraphRecord {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.debug_struct("BorrowedGraphRecord")
            .field("alive", &self.ptr.read().is_some())
            .field("mutable", &self.mutable)
            .finish()
    }
}

impl BorrowedGraphRecord {
    /// Creates a dead handle with no pointer. Any access will return an error.
    pub(super) const fn dead() -> Self {
        Self {
            ptr: RwLock::new(None),
            mutable: false,
        }
    }

    pub(super) const fn is_mutable(&self) -> bool {
        self.mutable
    }

    pub(super) fn read(&self) -> RwLockReadGuard<'_, Option<NonNull<GraphRecord>>> {
        self.ptr.read()
    }

    pub(super) fn write(&self) -> RwLockWriteGuard<'_, Option<NonNull<GraphRecord>>> {
        self.ptr.write()
    }
}

/// Generates a scoped borrow method on [`PyGraphRecord`].
///
/// Each invocation produces a standalone function with its own `Guard` and `PanicOnDrop`
/// types. The macro's `$ref_type` parameter determines the input reference kind
/// (`&GraphRecord` or `&mut GraphRecord`), and `$mutable` controls whether the resulting
/// `BorrowedGraphRecord` permits mutation — so the safety properties are fixed at compile
/// time per expansion, with no shared runtime helper that could be misused.
///
/// Based on the discussion in:
/// - <https://github.com/PyO3/pyo3/issues/1180>
///
/// Guard pattern adapted from:
/// - <https://github.com/PyO3/pyo3/issues/1180#issuecomment-692898577>
macro_rules! impl_scope {
    ($(#[$meta:meta])* $name:ident, $ref_type:ty, $mutable:expr) => {
        $(#[$meta])*
        pub fn $name<R>(
            py: Python<'_>,
            graphrecord: $ref_type,
            function: impl FnOnce(Python<'_>, &Py<Self>) -> GraphRecordResult<R>,
        ) -> GraphRecordResult<R> {
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
                        PyGraphRecordInner::Owned(_)
                        | PyGraphRecordInner::Connected(_) => {
                            panic!("PyGraphRecord was replaced with a non-borrowed variant");
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
                        inner: PyGraphRecordInner::Borrowed(BorrowedGraphRecord {
                            ptr: RwLock::new(Some(pointer)),
                            mutable: $mutable,
                        }),
                    },
                )
                .map_err(|error| {
                    GraphRecordError::ConversionError(format!(
                        "Failed to create PyGraphRecord: {error}"
                    ))
                })?,
                pointer,
            );
            function(py, &guard.1)
        }
    };
}

impl PyGraphRecord {
    impl_scope!(
        /// Safely pass a `&GraphRecord` to Python as a read-only `PyGraphRecord` for the
        /// duration of the callback. The pointer is invalidated when `function` returns.
        ///
        /// The resulting `PyGraphRecord` will reject any mutation attempts with a runtime
        /// error. See [`Self::scope_mut`] for the read-write variant.
        scope, &GraphRecord, false
    );

    impl_scope!(
        /// Safely pass a `&mut GraphRecord` to Python as a `PyGraphRecord` for the
        /// duration of the callback. The pointer is invalidated when `function` returns.
        ///
        /// The resulting `PyGraphRecord` allows both reads and mutations. See [`Self::scope`]
        /// for the read-only variant.
        scope_mut, &mut GraphRecord, true
    );
}
