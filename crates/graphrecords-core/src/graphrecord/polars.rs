use crate::{
    GraphRecord,
    errors::{GraphRecordError, GraphRecordResult},
    graphrecord::{Attributes, GraphRecordAttribute, GraphRecordValue, NodeIndex},
    prelude::{EdgeIndex, Group},
};
use chrono::{DateTime, TimeDelta};
use graphrecords_utils::aliases::{GrHashMap, GrHashSet};
use polars::{datatypes::AnyValue, frame::DataFrame, prelude::Column};
use std::collections::HashMap;

// TODO: Add tests for Duration
impl<'a> TryFrom<AnyValue<'a>> for GraphRecordValue {
    type Error = GraphRecordError;

    fn try_from(value: AnyValue<'a>) -> Result<Self, Self::Error> {
        match value {
            AnyValue::String(value) => Ok(Self::String(value.into())),
            AnyValue::StringOwned(value) => Ok(Self::String((*value).into())),
            AnyValue::Int8(value) => Ok(Self::Int(value.into())),
            AnyValue::Int16(value) => Ok(Self::Int(value.into())),
            AnyValue::Int32(value) => Ok(Self::Int(value.into())),
            AnyValue::Int64(value) => Ok(Self::Int(value)),
            AnyValue::UInt8(value) => Ok(Self::Int(value.into())),
            AnyValue::UInt16(value) => Ok(Self::Int(value.into())),
            AnyValue::UInt32(value) => Ok(Self::Int(value.into())),
            AnyValue::Float32(value) => Ok(Self::Float(value.into())),
            AnyValue::Float64(value) => Ok(Self::Float(value)),
            AnyValue::Boolean(value) => Ok(Self::Bool(value)),
            AnyValue::Datetime(value, unit, _) => {
                // TODO: handle timezone
                Ok(match unit {
                    polars::prelude::TimeUnit::Nanoseconds => {
                        Self::DateTime(DateTime::from_timestamp_nanos(value).naive_utc())
                    }
                    polars::prelude::TimeUnit::Microseconds => Self::DateTime(
                        DateTime::from_timestamp_micros(value)
                            .ok_or_else(|| {
                                GraphRecordError::ConversionError(format!(
                                    "Cannot convert {value}ms into GraphRecordValue"
                                ))
                            })?
                            .naive_utc(),
                    ),
                    polars::prelude::TimeUnit::Milliseconds => Self::DateTime(
                        DateTime::from_timestamp_millis(value)
                            .ok_or_else(|| {
                                GraphRecordError::ConversionError(format!(
                                    "Cannot convert {value}ms into GraphRecordValue"
                                ))
                            })?
                            .naive_utc(),
                    ),
                })
            }
            AnyValue::Duration(value, unit) => Ok(match unit {
                polars::prelude::TimeUnit::Nanoseconds => {
                    Self::Duration(TimeDelta::nanoseconds(value))
                }
                polars::prelude::TimeUnit::Microseconds => {
                    Self::Duration(TimeDelta::microseconds(value))
                }
                polars::prelude::TimeUnit::Milliseconds => {
                    Self::Duration(TimeDelta::milliseconds(value))
                }
            }),
            AnyValue::Null => Ok(Self::Null),
            _ => Err(GraphRecordError::ConversionError(format!(
                "Cannot convert {value} into GraphRecordValue"
            ))),
        }
    }
}

impl<'a> TryFrom<AnyValue<'a>> for GraphRecordAttribute {
    type Error = GraphRecordError;

    fn try_from(value: AnyValue<'a>) -> Result<Self, Self::Error> {
        match value {
            AnyValue::String(value) => Ok(Self::String(value.into())),
            AnyValue::StringOwned(value) => Ok(Self::String((*value).into())),
            AnyValue::Int8(value) => Ok(Self::Int(value.into())),
            AnyValue::Int16(value) => Ok(Self::Int(value.into())),
            AnyValue::Int32(value) => Ok(Self::Int(value.into())),
            AnyValue::Int64(value) => Ok(Self::Int(value)),
            AnyValue::UInt8(value) => Ok(Self::Int(value.into())),
            AnyValue::UInt16(value) => Ok(Self::Int(value.into())),
            AnyValue::UInt32(value) => Ok(Self::Int(value.into())),
            _ => Err(GraphRecordError::ConversionError(format!(
                "Cannot convert {value} into GraphRecordAttribute"
            ))),
        }
    }
}

impl From<GraphRecordValue> for AnyValue<'_> {
    fn from(value: GraphRecordValue) -> Self {
        match value {
            GraphRecordValue::String(value) => AnyValue::StringOwned(value.into()),
            GraphRecordValue::Int(value) => AnyValue::Int64(value),
            GraphRecordValue::Float(value) => AnyValue::Float64(value),
            GraphRecordValue::Bool(value) => AnyValue::Boolean(value),
            GraphRecordValue::DateTime(value) => {
                let timestamp = value.and_utc().timestamp_millis();

                AnyValue::Datetime(timestamp, polars::prelude::TimeUnit::Milliseconds, None)
            }
            GraphRecordValue::Duration(value) => {
                let duration_ms = value.num_milliseconds();

                AnyValue::Duration(duration_ms, polars::prelude::TimeUnit::Milliseconds)
            }
            GraphRecordValue::Null => AnyValue::Null,
        }
    }
}

impl From<GraphRecordAttribute> for AnyValue<'_> {
    fn from(value: GraphRecordAttribute) -> Self {
        match value {
            GraphRecordAttribute::String(value) => AnyValue::StringOwned(value.into()),
            GraphRecordAttribute::Int(value) => AnyValue::Int64(value),
        }
    }
}

pub fn dataframe_to_nodes(
    mut nodes: DataFrame,
    index_column_name: &str,
) -> GraphRecordResult<Vec<(NodeIndex, Attributes)>> {
    if nodes.max_n_chunks() > 1 {
        nodes.rechunk_mut();
    }

    let attribute_column_names: GrHashSet<_> = nodes
        .get_column_names()
        .into_iter()
        .filter(|name| *name != index_column_name)
        .collect();

    let index = nodes
        .column(index_column_name)
        .map_err(|_| {
            GraphRecordError::ConversionError(format!(
                "Cannot find column with name {index_column_name} in dataframe"
            ))
        })?
        .as_materialized_series()
        .iter();

    // This can probably be improved.
    let mut columns: Vec<_> = nodes
        .columns()
        .iter()
        .filter(|column| attribute_column_names.contains(column.name()))
        .map(|s| (s.as_materialized_series().iter(), s.name().clone()))
        .collect();

    index
        .map(|index_value| {
            Ok((
                index_value.try_into()?,
                columns
                    .iter_mut()
                    .map(|(column, column_name)| {
                        Ok((
                            column_name.as_str().into(),
                            column.next().expect("msg").try_into()?,
                        ))
                    })
                    .collect::<GraphRecordResult<_>>()?,
            ))
        })
        .collect()
}

pub fn dataframe_to_edges(
    mut edges: DataFrame,
    source_index_column_name: &str,
    target_index_column_name: &str,
) -> GraphRecordResult<Vec<(NodeIndex, NodeIndex, Attributes)>> {
    if edges.max_n_chunks() > 1 {
        edges.rechunk_mut();
    }

    let attribute_column_names: GrHashSet<_> = edges
        .get_column_names()
        .into_iter()
        .filter(|name| *name != source_index_column_name && *name != target_index_column_name)
        .collect();

    let source_index = edges
        .column(source_index_column_name)
        .map_err(|_| {
            GraphRecordError::ConversionError(format!(
                "Cannot find column with name {source_index_column_name} in dataframe"
            ))
        })?
        .as_materialized_series()
        .iter();
    let target_index = edges
        .column(target_index_column_name)
        .map_err(|_| {
            GraphRecordError::ConversionError(format!(
                "Cannot find column with name {target_index_column_name} in dataframe"
            ))
        })?
        .as_materialized_series()
        .iter();

    // This can probably be improved.
    let mut columns: Vec<_> = edges
        .columns()
        .iter()
        .filter(|column| attribute_column_names.contains(column.name()))
        .map(|s| (s.as_materialized_series().iter(), s.name().clone()))
        .collect();

    source_index
        .zip(target_index)
        .map(|(source_index_value, target_index_value)| {
            Ok((
                source_index_value.try_into()?,
                target_index_value.try_into()?,
                columns
                    .iter_mut()
                    .map(|(column, column_name)| {
                        Ok((
                            column_name.as_str().into(),
                            column
                                .next()
                                .expect("Should have as many iterations as rows")
                                .try_into()?,
                        ))
                    })
                    .collect::<GraphRecordResult<_>>()?,
            ))
        })
        .collect()
}

pub struct DataFramesGroupExport {
    pub nodes: DataFrame,
    pub edges: DataFrame,
}

impl DataFramesGroupExport {
    fn new(graphrecord: &GraphRecord, group: Option<&Group>) -> GraphRecordResult<Self> {
        let group_schema = match group {
            Some(group) => graphrecord.get_schema().group(group)?,
            None => graphrecord.get_schema().ungrouped(),
        };
        let group_string = match group {
            Some(group) => format!("{group}"),
            None => "ungrouped".to_string(),
        };

        let node_indices: Box<dyn Iterator<Item = &NodeIndex>> = match group {
            Some(group) => Box::new(graphrecord.nodes_in_group(group)?),
            None => Box::new(graphrecord.ungrouped_nodes()),
        };

        let group_node_attributes = node_indices.map(|node_index| {
            (
                node_index,
                graphrecord
                    .node_attributes(node_index)
                    .expect("Node index must exist"),
            )
        });

        let node_attributes: Vec<_> = group_schema.nodes().keys().collect();

        let mut node_columns: GrHashMap<GraphRecordAttribute, Vec<AnyValue>> = node_attributes
            .iter()
            .map(|attribute_name| ((*attribute_name).clone(), Vec::new()))
            .collect();

        let node_index_attribute = GraphRecordAttribute::String("node_index".into());

        if node_columns.contains_key(&node_index_attribute) {
            return Err(GraphRecordError::ConversionError(
                "Node attribute name 'node_index' is reserved".into(),
            ));
        }

        node_columns.insert(node_index_attribute.clone(), Vec::new());

        for (node_index, attributes) in group_node_attributes {
            node_columns
                .get_mut(&node_index_attribute)
                .expect("Attribute must exist in columns")
                .push(node_index.clone().into());

            for attribute_name in &node_attributes {
                let attribute_value = attributes
                    .get(attribute_name)
                    .cloned()
                    .unwrap_or(GraphRecordValue::Null);

                node_columns
                    .get_mut(*attribute_name)
                    .expect("Attribute must exist in columns")
                    .push(attribute_value.into());
            }
        }

        let node_columns: Vec<_> = node_columns
            .into_iter()
            .map(|(attribute_name, values)| Column::new(attribute_name.to_string().into(), values))
            .collect();

        let node_dataframe = DataFrame::new_infer_height(node_columns).map_err(|_| {
            GraphRecordError::ConversionError(format!(
                "Failed to create node DataFrame for group {group_string}"
            ))
        })?;

        let edge_indices: Box<dyn Iterator<Item = &EdgeIndex>> = match group {
            Some(group) => Box::new(graphrecord.edges_in_group(group)?),
            None => Box::new(graphrecord.ungrouped_edges()),
        };

        let group_edge_attributes = edge_indices.map(|edge_index| {
            let edge_endpoints = graphrecord
                .edge_endpoints(edge_index)
                .expect("Edge index must exist");

            (
                edge_index,
                edge_endpoints,
                graphrecord
                    .edge_attributes(edge_index)
                    .expect("Edge index must exist"),
            )
        });

        let edge_attributes: Vec<_> = group_schema.edges().keys().collect();

        let mut edge_columns: GrHashMap<GraphRecordAttribute, Vec<AnyValue>> = edge_attributes
            .iter()
            .map(|attribute_name| ((*attribute_name).clone(), Vec::new()))
            .collect();

        let edge_index_attribute = GraphRecordAttribute::String("edge_index".into());
        let source_node_index_attribute = GraphRecordAttribute::String("source_node_index".into());
        let target_node_index_attribute = GraphRecordAttribute::String("target_node_index".into());

        if edge_columns.contains_key(&edge_index_attribute) {
            return Err(GraphRecordError::ConversionError(
                "Edge attribute name 'edge_index' is reserved".into(),
            ));
        }
        if edge_columns.contains_key(&source_node_index_attribute) {
            return Err(GraphRecordError::ConversionError(
                "Edge attribute name 'source_node_index' is reserved".into(),
            ));
        }
        if edge_columns.contains_key(&target_node_index_attribute) {
            return Err(GraphRecordError::ConversionError(
                "Edge attribute name 'target_node_index' is reserved".into(),
            ));
        }

        edge_columns.insert(edge_index_attribute.clone(), Vec::new());
        edge_columns.insert(source_node_index_attribute.clone(), Vec::new());
        edge_columns.insert(target_node_index_attribute.clone(), Vec::new());

        for (edge_index, edge_endpoints, attributes) in group_edge_attributes {
            edge_columns
                .get_mut(&edge_index_attribute)
                .expect("Attribute must exist in columns")
                .push((*edge_index).into());
            edge_columns
                .get_mut(&source_node_index_attribute)
                .expect("Attribute must exist in columns")
                .push(edge_endpoints.0.clone().into());
            edge_columns
                .get_mut(&target_node_index_attribute)
                .expect("Attribute must exist in columns")
                .push(edge_endpoints.1.clone().into());

            for attribute_name in &edge_attributes {
                let attribute_value = attributes
                    .get(attribute_name)
                    .cloned()
                    .unwrap_or(GraphRecordValue::Null);

                edge_columns
                    .get_mut(*attribute_name)
                    .expect("Attribute must exist in columns")
                    .push(attribute_value.into());
            }
        }

        let edge_columns: Vec<_> = edge_columns
            .into_iter()
            .map(|(attribute_name, values)| Column::new(attribute_name.to_string().into(), values))
            .collect();

        let edge_dataframe = DataFrame::new_infer_height(edge_columns).map_err(|_| {
            GraphRecordError::ConversionError(format!(
                "Failed to create edge DataFrame for group {group_string}"
            ))
        })?;

        Ok(Self {
            nodes: node_dataframe,
            edges: edge_dataframe,
        })
    }
}

pub struct DataFramesExport {
    pub ungrouped: DataFramesGroupExport,
    pub groups: HashMap<Group, DataFramesGroupExport>,
}

impl DataFramesExport {
    pub fn new(graphrecord: &GraphRecord) -> GraphRecordResult<Self> {
        let ungrouped = DataFramesGroupExport::new(graphrecord, None)?;

        let groups = graphrecord
            .groups()
            .map(|group| {
                Ok::<_, GraphRecordError>((
                    group.clone(),
                    DataFramesGroupExport::new(graphrecord, Some(group))?,
                ))
            })
            .collect::<Result<_, _>>()?;

        Ok(Self { ungrouped, groups })
    }
}

#[cfg(test)]
mod test {
    use super::{GraphRecordValue, dataframe_to_edges, dataframe_to_nodes};
    use crate::errors::GraphRecordError;
    use chrono::NaiveDateTime;
    use polars::prelude::*;
    use std::collections::HashMap;

    #[test]
    fn test_try_from_anyvalue_string() {
        let any_value = AnyValue::String("value");

        let value = GraphRecordValue::try_from(any_value).unwrap();

        assert_eq!(GraphRecordValue::String("value".to_string()), value);
    }

    #[test]
    fn test_from_anyvalue_int8() {
        let any_value = AnyValue::Int8(0);

        let value = GraphRecordValue::try_from(any_value).unwrap();

        assert_eq!(GraphRecordValue::Int(0), value);
    }

    #[test]
    fn test_from_anyvalue_int16() {
        let any_value = AnyValue::Int16(0);

        let value = GraphRecordValue::try_from(any_value).unwrap();

        assert_eq!(GraphRecordValue::Int(0), value);
    }

    #[test]
    fn test_from_anyvalue_int32() {
        let any_value = AnyValue::Int32(0);

        let value = GraphRecordValue::try_from(any_value).unwrap();

        assert_eq!(GraphRecordValue::Int(0), value);
    }

    #[test]
    fn test_from_anyvalue_int64() {
        let any_value = AnyValue::Int64(0);

        let value = GraphRecordValue::try_from(any_value).unwrap();

        assert_eq!(GraphRecordValue::Int(0), value);
    }

    #[test]
    fn test_from_anyvalue_float32() {
        let any_value = AnyValue::Float32(0.0);

        let value = GraphRecordValue::try_from(any_value).unwrap();

        assert_eq!(GraphRecordValue::Float(0.0), value);
    }

    #[test]
    fn test_from_anyvalue_float64() {
        let any_value = AnyValue::Float64(0.0);

        let value = GraphRecordValue::try_from(any_value).unwrap();

        assert_eq!(GraphRecordValue::Float(0.0), value);
    }

    #[test]
    fn test_from_anyvalue_bool() {
        let any_value = AnyValue::Boolean(false);

        let value = GraphRecordValue::try_from(any_value).unwrap();

        assert_eq!(GraphRecordValue::Bool(false), value);
    }

    #[test]
    fn test_from_anyvalue_datetime() {
        let any_value = AnyValue::Datetime(0, polars::prelude::TimeUnit::Microseconds, None);

        let value = GraphRecordValue::try_from(any_value).unwrap();

        assert_eq!(
            GraphRecordValue::DateTime(
                NaiveDateTime::parse_from_str("1970-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap()
            ),
            value
        );

        let any_value = AnyValue::Datetime(0, polars::prelude::TimeUnit::Milliseconds, None);

        let value = GraphRecordValue::try_from(any_value).unwrap();

        assert_eq!(
            GraphRecordValue::DateTime(
                NaiveDateTime::parse_from_str("1970-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap()
            ),
            value
        );

        let any_value = AnyValue::Datetime(0, polars::prelude::TimeUnit::Nanoseconds, None);

        let value = GraphRecordValue::try_from(any_value).unwrap();

        assert_eq!(
            GraphRecordValue::DateTime(
                NaiveDateTime::parse_from_str("1970-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap()
            ),
            value
        );
    }

    #[test]
    fn test_from_anyvalue_null() {
        let any_value = AnyValue::Null;

        let value = GraphRecordValue::try_from(any_value).unwrap();

        assert_eq!(GraphRecordValue::Null, value);
    }

    #[test]
    fn test_dataframe_to_nodes() {
        let s0 = Series::new("index".into(), &["0", "1"]);
        let s1 = Series::new("attribute".into(), &[1, 2]);
        let nodes_dataframe = DataFrame::new(2, vec![s0.into(), s1.into()]).unwrap();

        let nodes = dataframe_to_nodes(nodes_dataframe, "index").unwrap();

        assert_eq!(
            vec![
                ("0".into(), HashMap::from([("attribute".into(), 1.into())])),
                ("1".into(), HashMap::from([("attribute".into(), 2.into())]))
            ],
            nodes
        );
    }

    #[test]
    fn test_invalid_dataframe_to_nodes() {
        let s0 = Series::new("index".into(), &["0", "1"]);
        let s1 = Series::new("attribute".into(), &[1, 2]);
        let nodes_dataframe = DataFrame::new(2, vec![s0.into(), s1.into()]).unwrap();

        // Providing the wrong index column name should fail
        assert!(
            dataframe_to_nodes(nodes_dataframe, "wrong_column")
                .is_err_and(|e| matches!(e, GraphRecordError::ConversionError(_)))
        );
    }

    #[test]
    fn test_dataframe_to_edges() {
        let s0 = Series::new("source".into(), &["0", "1"]);
        let s1 = Series::new("target".into(), &["1", "0"]);
        let s2 = Series::new("attribute".into(), &[1, 2]);
        let edges_dataframe = DataFrame::new(2, vec![s0.into(), s1.into(), s2.into()]).unwrap();

        let edges = dataframe_to_edges(edges_dataframe, "source", "target").unwrap();

        assert_eq!(
            vec![
                (
                    "0".into(),
                    "1".into(),
                    HashMap::from([("attribute".into(), 1.into())])
                ),
                (
                    "1".into(),
                    "0".into(),
                    HashMap::from([("attribute".into(), 2.into())])
                )
            ],
            edges
        );
    }

    #[test]
    fn test_invalid_dataframe_to_edges() {
        let s0 = Series::new("source".into(), &["0", "1"]);
        let s1 = Series::new("target".into(), &["1", "0"]);
        let s2 = Series::new("attribute".into(), &[1, 2]);
        let edges_dataframe = DataFrame::new(2, vec![s0.into(), s1.into(), s2.into()]).unwrap();

        // Providing the wrong source index column name should fail
        assert!(
            dataframe_to_edges(edges_dataframe.clone(), "wrong_column", "target")
                .is_err_and(|e| matches!(e, GraphRecordError::ConversionError(_)))
        );

        // Providing the wrong target index column name should fail
        assert!(
            dataframe_to_edges(edges_dataframe, "source", "wrong_column")
                .is_err_and(|e| matches!(e, GraphRecordError::ConversionError(_)))
        );
    }
}
