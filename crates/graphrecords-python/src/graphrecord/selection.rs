use super::{
    edge_index::PyEdgeIndex, errors::PyGraphRecordError, identifier::convert_pyobject_to_identifier,
};
use crate::querying::{PyArgument, PyExpression, PySeries};
use graphrecords_core::{
    errors::{GraphRecordError, GraphRecordResult},
    graphrecord::{AttributeName, EdgeIndex, GraphRecord, GroupIndex, NodeIndex},
};
use graphrecords_query::{dynamic::DynArgumentLane, registry::ArityDescriptor};
use pyo3::{
    Bound, PyAny, PyResult,
    exceptions::PyTypeError,
    prelude::PyAnyMethods,
    types::{PyBytes, PyString},
};

pub struct ResolvedSelection;

impl ResolvedSelection {
    fn from_query<E>(
        graphrecord: &GraphRecord,
        selection: &Bound<'_, PyAny>,
        resolve_expression: impl Fn(&PyExpression, &GraphRecord) -> Option<GraphRecordResult<Vec<E>>>,
        resolve_series: impl Fn(&PySeries, &GraphRecord) -> Option<GraphRecordResult<Vec<E>>>,
        resolve_dropping: impl Fn(&DynArgumentLane, &GraphRecord) -> Option<GraphRecordResult<Vec<E>>>,
        expected: &'static str,
    ) -> PyResult<Option<Vec<E>>> {
        if let Ok(argument) = selection.cast::<PyArgument>() {
            let Some(lane) = argument.get().source().dropping_lane() else {
                return Err(PyTypeError::new_err(
                    "only an `on_missing(Drop())` argument is a selection",
                ));
            };

            return match resolve_dropping(lane, graphrecord) {
                Some(selected) => Ok(Some(selected.map_err(PyGraphRecordError::from)?)),
                None => Err(PyTypeError::new_err(expected)),
            };
        }

        let resolved = if let Ok(expression) = selection.cast::<PyExpression>() {
            Some(resolve_expression(expression.get(), graphrecord))
        } else if let Ok(series) = selection.cast::<PySeries>() {
            Some(resolve_series(series.get(), graphrecord))
        } else {
            None
        };

        match resolved {
            Some(Some(selected)) => Ok(Some(selected.map_err(PyGraphRecordError::from)?)),
            Some(None) => Err(PyTypeError::new_err(expected)),
            None => Ok(None),
        }
    }

    fn single_arity(selection: &Bound<'_, PyAny>) -> bool {
        let arity = if let Ok(expression) = selection.cast::<PyExpression>() {
            Some(expression.get().expression().descriptor().lane_arity())
        } else if let Ok(series) = selection.cast::<PySeries>() {
            Some(series.get().expression().descriptor().lane_arity())
        } else {
            None
        };

        arity.is_some_and(|arity| {
            matches!(arity, ArityDescriptor::Single | ArityDescriptor::Definite)
        })
    }

    pub fn nodes(
        graphrecord: &GraphRecord,
        selection: &Bound<'_, PyAny>,
    ) -> PyResult<Vec<NodeIndex>> {
        if let Some(selected) = Self::from_query(
            graphrecord,
            selection,
            PyExpression::resolve_nodes,
            PySeries::resolve_nodes,
            DynArgumentLane::resolve_dropping_nodes,
            "Expected a selection of nodes",
        )? {
            return Ok(selected);
        }

        if selection.is_instance_of::<PyString>() || selection.extract::<i64>().is_ok() {
            return Ok(vec![NodeIndex::from(convert_pyobject_to_identifier(
                selection,
            )?)]);
        }
        if selection.is_instance_of::<PyBytes>() {
            return Err(PyTypeError::new_err("Expected a selection of nodes"));
        }

        selection
            .try_iter()
            .map_err(|_| PyTypeError::new_err("Expected a selection of nodes"))?
            .map(|element| Ok(NodeIndex::from(convert_pyobject_to_identifier(&element?)?)))
            .collect()
    }

    pub fn edges(
        graphrecord: &GraphRecord,
        selection: &Bound<'_, PyAny>,
    ) -> PyResult<Vec<EdgeIndex>> {
        if let Some(selected) = Self::from_query(
            graphrecord,
            selection,
            PyExpression::resolve_edges,
            PySeries::resolve_edges,
            DynArgumentLane::resolve_dropping_edges,
            "Expected a selection of edges",
        )? {
            return Ok(selected);
        }

        if let Ok(edge_index) = selection.extract::<PyEdgeIndex>() {
            return Ok(vec![edge_index.into()]);
        }

        selection
            .try_iter()
            .map_err(|_| PyTypeError::new_err("Expected a selection of edges"))?
            .map(|element| Ok(EdgeIndex::from(element?.extract::<PyEdgeIndex>()?)))
            .collect()
    }

    pub fn groups(
        graphrecord: &GraphRecord,
        selection: &Bound<'_, PyAny>,
    ) -> PyResult<Vec<GroupIndex>> {
        if let Some(selected) = Self::from_query(
            graphrecord,
            selection,
            PyExpression::resolve_groups,
            PySeries::resolve_groups,
            DynArgumentLane::resolve_dropping_groups,
            "Expected a selection of groups",
        )? {
            return Ok(selected);
        }

        if selection.is_instance_of::<PyString>() || selection.extract::<i64>().is_ok() {
            return Ok(vec![GroupIndex::from(convert_pyobject_to_identifier(
                selection,
            )?)]);
        }
        if selection.is_instance_of::<PyBytes>() {
            return Err(PyTypeError::new_err("Expected a selection of groups"));
        }

        selection
            .try_iter()
            .map_err(|_| PyTypeError::new_err("Expected a selection of groups"))?
            .map(|element| Ok(GroupIndex::from(convert_pyobject_to_identifier(&element?)?)))
            .collect()
    }

    pub fn attribute_names(attribute_names: &Bound<'_, PyAny>) -> PyResult<Vec<AttributeName>> {
        if attribute_names.is_instance_of::<PyString>()
            || attribute_names.is_instance_of::<PyBytes>()
        {
            return Err(PyTypeError::new_err("Expected attribute names"));
        }

        attribute_names
            .try_iter()
            .map_err(|_| PyTypeError::new_err("Expected attribute names"))?
            .map(|element| {
                Ok(AttributeName::from(convert_pyobject_to_identifier(
                    &element?,
                )?))
            })
            .collect()
    }

    pub fn single_node(
        graphrecord: &GraphRecord,
        selection: &Bound<'_, PyAny>,
    ) -> PyResult<NodeIndex> {
        if selection.is_instance_of::<PyString>() || selection.extract::<i64>().is_ok() {
            return Ok(NodeIndex::from(convert_pyobject_to_identifier(selection)?));
        }
        if !Self::single_arity(selection) {
            return Err(PyTypeError::new_err("Expected a single node selection"));
        }

        match Self::nodes(graphrecord, selection)?.pop() {
            Some(node_index) => Ok(node_index),
            None => Err(PyGraphRecordError::from(GraphRecordError::NoNodeSelected).into()),
        }
    }

    pub fn single_group(
        graphrecord: &GraphRecord,
        selection: &Bound<'_, PyAny>,
    ) -> PyResult<GroupIndex> {
        if selection.is_instance_of::<PyString>() || selection.extract::<i64>().is_ok() {
            return Ok(GroupIndex::from(convert_pyobject_to_identifier(selection)?));
        }
        if !Self::single_arity(selection) {
            return Err(PyTypeError::new_err("Expected a single group selection"));
        }

        match Self::groups(graphrecord, selection)?.pop() {
            Some(group_index) => Ok(group_index),
            None => Err(PyGraphRecordError::from(GraphRecordError::NoGroupSelected).into()),
        }
    }
}
