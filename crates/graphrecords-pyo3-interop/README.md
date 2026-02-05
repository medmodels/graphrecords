# graphrecords-pyo3-interop

Interoperability layer for passing GraphRecords types between independent PyO3 extension modules.

## Overview

PyO3's `#[pyclass]` stores type objects in static storage, meaning each compiled extension module gets its own copy. When two separate PyO3 extensions try to share a type like `GraphRecord`, Python sees them as distinct types, breaking compatibility ([PyO3#1444](https://github.com/PyO3/pyo3/issues/1444)).

This crate provides a workaround by serializing `GraphRecord` to bytes (via bincode) when crossing extension boundaries, allowing external Rust-based Python extensions to accept and return `GraphRecord` objects from the main `graphrecords` package.

## Key Components

- **`PyGraphRecord`** - A wrapper around `GraphRecord` implementing PyO3's `FromPyObject` and `IntoPyObject` traits via binary serialization
- **Type Re-exports** - Common Python binding types (`PyNodeIndex`, `PyEdgeIndex`, `PyGroup`, `PyAttributes`, etc.)

## Usage

```rust
use graphrecords_pyo3_interop::PyGraphRecord;
use pyo3::prelude::*;

#[pyfunction]
fn process_graphrecord(record: PyGraphRecord) -> PyResult<PyGraphRecord> {
    // Access the inner GraphRecord
    let inner = record.0;

    // Process and return
    Ok(inner.into())
}
```
