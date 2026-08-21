use super::PyGroupIndex;
use graphrecords_core::graphrecord::Export;
use pyo3::{prelude::*, types::PyDict};

fn tables(py: Python<'_>, nodes: Py<PyAny>, edges: Py<PyAny>) -> PyResult<Py<PyAny>> {
    let tables = PyDict::new(py);
    tables.set_item("nodes", nodes)?;
    tables.set_item("edges", edges)?;

    Ok(tables.into())
}

pub(super) fn partitioned<T, F>(py: Python<'_>, export: Export<T>, table: F) -> PyResult<Py<PyAny>>
where
    F: Fn(Python<'_>, T) -> PyResult<Py<PyAny>>,
{
    let groups = PyDict::new(py);

    for (group_index, group_tables) in export.groups {
        let nodes = table(py, group_tables.nodes)?;
        let edges = table(py, group_tables.edges)?;

        groups.set_item(PyGroupIndex::from(group_index), tables(py, nodes, edges)?)?;
    }

    let ungrouped_nodes = table(py, export.ungrouped.nodes)?;
    let ungrouped_edges = table(py, export.ungrouped.edges)?;

    let partitioned = PyDict::new(py);
    partitioned.set_item("ungrouped", tables(py, ungrouped_nodes, ungrouped_edges)?)?;
    partitioned.set_item("groups", groups)?;

    Ok(partitioned.into())
}
