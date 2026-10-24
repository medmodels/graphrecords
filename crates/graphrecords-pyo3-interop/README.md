# graphrecords-pyo3-interop

Interoperability layer for passing GraphRecords types between independent PyO3 extension modules.

## Overview

PyO3's `#[pyclass]` stores type objects in static storage, meaning each compiled extension module gets its own copy. When two separate PyO3 extensions try to share a type like `GraphRecord`, Python sees them as distinct types, breaking compatibility ([PyO3#1444](https://github.com/PyO3/pyo3/issues/1444)).

This crate provides a workaround by serializing `GraphRecord` to bytes (via bincode) when crossing extension boundaries, allowing external Rust-based Python extensions to accept and return `GraphRecord` objects from the main `graphrecords` package.

## Key Components

- **Serialized types** - `PyGraphRecord`, `PyEdgeIndex`, `PySchema`, `PyGroupSchema` and `PyAttributeDataType` cross the boundary as bincode bytes. The receiving side gets its own copy.
- **Enums** - `PyAttributeType`, `PySchemaType`, `PyEdgeDirection` and `PyOnConflict` convert from and to the members of the corresponding `graphrecords` enums.
- **Data types** - `PyDataType` converts from and to the `graphrecords` data type classes (`String`, `Int`, `Union`, `Option`, ...).
- **Values and identifiers** - `PyValue`, `PyIdentifier`, `PyNodeIndex`, `PyGroupIndex` and `PyAttributes` map to Python builtins, re-exported from [`graphrecords-pyo3`](../graphrecords-pyo3).
- **Errors** - `PyGraphRecordError` converts into Python exceptions. Query failures raise the query exception classes of the installed `graphrecords` package, so `except graphrecords.QueryError` catches them. Other errors raise the same builtin exceptions as the package itself.

## Notes

- Plugins attached to a `GraphRecord` are not carried across the boundary.
- The extension must be built against the same graphrecords version as the installed `graphrecords` Python package, because the serialized layout follows the Rust types.

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

```python
import graphrecords as gr
import my_extension

record = gr.GraphRecord().add_nodes([("lorem", {})])
processed = my_extension.process_graphrecord(record)
```
