use crate::{
    GraphRecord,
    errors::{ConversionError, GraphRecordError, GraphRecordResult},
    graphrecord::{AttributeMap, AttributeName, Identifier, NodeIndex, Value},
    prelude::{EdgeIndex, Group},
};
use chrono::{DateTime, TimeDelta};
use graphrecords_utils::aliases::{GrHashMap, GrHashSet};
use polars::{datatypes::AnyValue, frame::DataFrame, prelude::Column};
use std::collections::HashMap;

// TODO: Add tests for Duration
impl<'a> TryFrom<AnyValue<'a>> for Value {
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
                            .ok_or(GraphRecordError::Conversion(
                                ConversionError::TimestampOutOfRange { timestamp: value },
                            ))?
                            .naive_utc(),
                    ),
                    polars::prelude::TimeUnit::Milliseconds => Self::DateTime(
                        DateTime::from_timestamp_millis(value)
                            .ok_or(GraphRecordError::Conversion(
                                ConversionError::TimestampOutOfRange { timestamp: value },
                            ))?
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
            _ => Err(GraphRecordError::Conversion(
                ConversionError::UnsupportedPolarsValue {
                    value: value.to_string(),
                },
            )),
        }
    }
}

impl<'a> TryFrom<AnyValue<'a>> for Identifier {
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
            _ => Err(GraphRecordError::Conversion(
                ConversionError::UnsupportedPolarsIdentifier {
                    value: value.to_string(),
                },
            )),
        }
    }
}

impl From<Value> for AnyValue<'_> {
    fn from(value: Value) -> Self {
        match value {
            Value::String(value) => AnyValue::StringOwned(value.into()),
            Value::Int(value) => AnyValue::Int64(value),
            Value::Float(value) => AnyValue::Float64(value),
            Value::Bool(value) => AnyValue::Boolean(value),
            Value::DateTime(value) => {
                let timestamp = value.and_utc().timestamp_millis();

                AnyValue::Datetime(timestamp, polars::prelude::TimeUnit::Milliseconds, None)
            }
            Value::Duration(value) => {
                let duration_ms = value.num_milliseconds();

                AnyValue::Duration(duration_ms, polars::prelude::TimeUnit::Milliseconds)
            }
            Value::Null => AnyValue::Null,
        }
    }
}

impl From<Identifier> for AnyValue<'_> {
    fn from(value: Identifier) -> Self {
        match value {
            Identifier::String(value) => AnyValue::StringOwned(value.into()),
            Identifier::Int(value) => AnyValue::Int64(value),
        }
    }
}

pub fn dataframe_to_nodes(
    mut nodes: DataFrame,
    index_column_name: &str,
) -> GraphRecordResult<Vec<(NodeIndex, AttributeMap)>> {
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
            GraphRecordError::Conversion(ConversionError::ColumnNotFound {
                column_name: index_column_name.to_string(),
            })
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
                NodeIndex::from(Identifier::try_from(index_value)?),
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

pub fn dataframe_to_edges(
    mut edges: DataFrame,
    source_index_column_name: &str,
    target_index_column_name: &str,
) -> GraphRecordResult<Vec<(NodeIndex, NodeIndex, AttributeMap)>> {
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
            GraphRecordError::Conversion(ConversionError::ColumnNotFound {
                column_name: source_index_column_name.to_string(),
            })
        })?
        .as_materialized_series()
        .iter();
    let target_index = edges
        .column(target_index_column_name)
        .map_err(|_| {
            GraphRecordError::Conversion(ConversionError::ColumnNotFound {
                column_name: target_index_column_name.to_string(),
            })
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
                NodeIndex::from(Identifier::try_from(source_index_value)?),
                NodeIndex::from(Identifier::try_from(target_index_value)?),
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

        let mut node_columns: GrHashMap<AttributeName, Vec<AnyValue>> = node_attributes
            .iter()
            .map(|attribute_name| ((*attribute_name).clone(), Vec::new()))
            .collect();

        let node_index_attribute = AttributeName::from("node_index");

        if node_columns.contains_key(&node_index_attribute) {
            return Err(GraphRecordError::Conversion(
                ConversionError::ReservedAttributeName {
                    attribute: node_index_attribute,
                },
            ));
        }

        node_columns.insert(node_index_attribute.clone(), Vec::new());

        for (node_index, attributes) in group_node_attributes {
            node_columns
                .get_mut(&node_index_attribute)
                .expect("Attribute must exist in columns")
                .push(Identifier::from(node_index.clone()).into());

            for attribute_name in &node_attributes {
                let attribute_value = attributes
                    .get(attribute_name)
                    .cloned()
                    .unwrap_or(Value::Null);

                node_columns
                    .get_mut(*attribute_name)
                    .expect("Attribute must exist in columns")
                    .push(attribute_value.into());
            }
        }

        let node_columns: Vec<_> = node_columns
            .into_iter()
            .map(|(attribute_name, values)| {
                let column_name = match Identifier::from(attribute_name) {
                    Identifier::String(value) => value,
                    Identifier::Int(value) => value.to_string(),
                };

                Column::new(column_name.into(), values)
            })
            .collect();

        let node_dataframe = DataFrame::new_infer_height(node_columns).map_err(|_| {
            GraphRecordError::Conversion(ConversionError::NodeDataFrameCreation {
                group: group_string.clone(),
            })
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

        let mut edge_columns: GrHashMap<AttributeName, Vec<AnyValue>> = edge_attributes
            .iter()
            .map(|attribute_name| ((*attribute_name).clone(), Vec::new()))
            .collect();

        let edge_index_attribute = AttributeName::from("edge_index");
        let source_node_index_attribute = AttributeName::from("source_node_index");
        let target_node_index_attribute = AttributeName::from("target_node_index");

        if edge_columns.contains_key(&edge_index_attribute) {
            return Err(GraphRecordError::Conversion(
                ConversionError::ReservedAttributeName {
                    attribute: edge_index_attribute,
                },
            ));
        }
        if edge_columns.contains_key(&source_node_index_attribute) {
            return Err(GraphRecordError::Conversion(
                ConversionError::ReservedAttributeName {
                    attribute: source_node_index_attribute,
                },
            ));
        }
        if edge_columns.contains_key(&target_node_index_attribute) {
            return Err(GraphRecordError::Conversion(
                ConversionError::ReservedAttributeName {
                    attribute: target_node_index_attribute,
                },
            ));
        }

        edge_columns.insert(edge_index_attribute.clone(), Vec::new());
        edge_columns.insert(source_node_index_attribute.clone(), Vec::new());
        edge_columns.insert(target_node_index_attribute.clone(), Vec::new());

        for (edge_index, edge_endpoints, attributes) in group_edge_attributes {
            let source_node_index = edge_endpoints.0.clone();
            let target_node_index = edge_endpoints.1.clone();

            edge_columns
                .get_mut(&edge_index_attribute)
                .expect("Attribute must exist in columns")
                .push((*edge_index).into());
            edge_columns
                .get_mut(&source_node_index_attribute)
                .expect("Attribute must exist in columns")
                .push(Identifier::from(source_node_index).into());
            edge_columns
                .get_mut(&target_node_index_attribute)
                .expect("Attribute must exist in columns")
                .push(Identifier::from(target_node_index).into());

            for attribute_name in &edge_attributes {
                let attribute_value = attributes
                    .get(attribute_name)
                    .cloned()
                    .unwrap_or(Value::Null);

                edge_columns
                    .get_mut(*attribute_name)
                    .expect("Attribute must exist in columns")
                    .push(attribute_value.into());
            }
        }

        let edge_columns: Vec<_> = edge_columns
            .into_iter()
            .map(|(attribute_name, values)| {
                let column_name = match Identifier::from(attribute_name) {
                    Identifier::String(value) => value,
                    Identifier::Int(value) => value.to_string(),
                };

                Column::new(column_name.into(), values)
            })
            .collect();

        let edge_dataframe = DataFrame::new_infer_height(edge_columns).map_err(|_| {
            GraphRecordError::Conversion(ConversionError::EdgeDataFrameCreation {
                group: group_string.clone(),
            })
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
    use super::{Value, dataframe_to_edges, dataframe_to_nodes};
    use crate::errors::GraphRecordError;
    use chrono::NaiveDateTime;
    use polars::prelude::*;
    use std::collections::HashMap;

    #[test]
    fn test_try_from_anyvalue_string() {
        let any_value = AnyValue::String("value");

        let value = Value::try_from(any_value).unwrap();

        assert_eq!(Value::String("value".to_string()), value);
    }

    #[test]
    fn test_from_anyvalue_int8() {
        let any_value = AnyValue::Int8(0);

        let value = Value::try_from(any_value).unwrap();

        assert_eq!(Value::Int(0), value);
    }

    #[test]
    fn test_from_anyvalue_int16() {
        let any_value = AnyValue::Int16(0);

        let value = Value::try_from(any_value).unwrap();

        assert_eq!(Value::Int(0), value);
    }

    #[test]
    fn test_from_anyvalue_int32() {
        let any_value = AnyValue::Int32(0);

        let value = Value::try_from(any_value).unwrap();

        assert_eq!(Value::Int(0), value);
    }

    #[test]
    fn test_from_anyvalue_int64() {
        let any_value = AnyValue::Int64(0);

        let value = Value::try_from(any_value).unwrap();

        assert_eq!(Value::Int(0), value);
    }

    #[test]
    fn test_from_anyvalue_float32() {
        let any_value = AnyValue::Float32(0.0);

        let value = Value::try_from(any_value).unwrap();

        assert_eq!(Value::Float(0.0), value);
    }

    #[test]
    fn test_from_anyvalue_float64() {
        let any_value = AnyValue::Float64(0.0);

        let value = Value::try_from(any_value).unwrap();

        assert_eq!(Value::Float(0.0), value);
    }

    #[test]
    fn test_from_anyvalue_bool() {
        let any_value = AnyValue::Boolean(false);

        let value = Value::try_from(any_value).unwrap();

        assert_eq!(Value::Bool(false), value);
    }

    #[test]
    fn test_from_anyvalue_datetime() {
        let any_value = AnyValue::Datetime(0, polars::prelude::TimeUnit::Microseconds, None);

        let value = Value::try_from(any_value).unwrap();

        assert_eq!(
            Value::DateTime(
                NaiveDateTime::parse_from_str("1970-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap()
            ),
            value
        );

        let any_value = AnyValue::Datetime(0, polars::prelude::TimeUnit::Milliseconds, None);

        let value = Value::try_from(any_value).unwrap();

        assert_eq!(
            Value::DateTime(
                NaiveDateTime::parse_from_str("1970-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap()
            ),
            value
        );

        let any_value = AnyValue::Datetime(0, polars::prelude::TimeUnit::Nanoseconds, None);

        let value = Value::try_from(any_value).unwrap();

        assert_eq!(
            Value::DateTime(
                NaiveDateTime::parse_from_str("1970-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap()
            ),
            value
        );
    }

    #[test]
    fn test_from_anyvalue_null() {
        let any_value = AnyValue::Null;

        let value = Value::try_from(any_value).unwrap();

        assert_eq!(Value::Null, value);
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
                .is_err_and(|e| matches!(e, GraphRecordError::Conversion(_)))
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
                .is_err_and(|e| matches!(e, GraphRecordError::Conversion(_)))
        );

        // Providing the wrong target index column name should fail
        assert!(
            dataframe_to_edges(edges_dataframe, "source", "wrong_column")
                .is_err_and(|e| matches!(e, GraphRecordError::Conversion(_)))
        );
    }
}
