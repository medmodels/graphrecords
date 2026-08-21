use super::{
    GraphRecord,
    batch::{EdgeBatch, NodeBatch},
    datatypes::{
        AttributeName, DataType, GroupIndex, Identifier, IdentifierView, NodeIndex, Value,
        ValueView,
    },
    frame::{Export, Tables},
    schema::{AttributeDataType, GroupSchema},
    source::{EdgeSource, NodeSource},
    state::{EdgeAddress, GraphState, NodeAddress},
    writer::Writer,
};
use crate::errors::{ConversionError, GraphRecordError, GraphRecordResult};
use chrono::{DateTime, TimeDelta};
use polars::{
    datatypes::AnyValue,
    frame::DataFrame,
    prelude::{Column, DataType as PolarsDataType, Series, TimeUnit},
};
use std::{borrow::Cow, collections::HashMap};

impl TryFrom<AnyValue<'_>> for Value {
    type Error = GraphRecordError;

    fn try_from(value: AnyValue<'_>) -> Result<Self, Self::Error> {
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
            AnyValue::Datetime(value, unit, _) => match unit {
                TimeUnit::Nanoseconds => Ok(Self::DateTime(
                    DateTime::from_timestamp_nanos(value).naive_utc(),
                )),
                TimeUnit::Microseconds => DateTime::from_timestamp_micros(value)
                    .map(|datetime| Self::DateTime(datetime.naive_utc()))
                    .ok_or(GraphRecordError::Conversion(
                        ConversionError::TimestampOutOfRange { timestamp: value },
                    )),
                TimeUnit::Milliseconds => DateTime::from_timestamp_millis(value)
                    .map(|datetime| Self::DateTime(datetime.naive_utc()))
                    .ok_or(GraphRecordError::Conversion(
                        ConversionError::TimestampOutOfRange { timestamp: value },
                    )),
            },
            AnyValue::Duration(value, unit) => Ok(match unit {
                TimeUnit::Nanoseconds => Self::Duration(TimeDelta::nanoseconds(value)),
                TimeUnit::Microseconds => Self::Duration(TimeDelta::microseconds(value)),
                TimeUnit::Milliseconds => Self::Duration(TimeDelta::milliseconds(value)),
            }),
            AnyValue::Null => Ok(Self::Null),
            _ => Err(GraphRecordError::Conversion(
                ConversionError::UnsupportedFrameValue {
                    value: value.to_string(),
                },
            )),
        }
    }
}

impl<S: Into<String>> NodeSource for (DataFrame, S) {
    fn collect_nodes(self) -> GraphRecordResult<NodeBatch> {
        let (mut dataframe, index_column) = self;
        let index_column = index_column.into();

        if dataframe.max_n_chunks() > 1 {
            dataframe.rechunk_mut();
        }

        let index_values = dataframe
            .column(&index_column)
            .map_err(|_| ConversionError::ColumnNotFound {
                column_name: index_column.clone(),
            })?
            .as_materialized_series()
            .iter();

        let mut attribute_columns: Vec<_> = dataframe
            .columns()
            .iter()
            .filter(|column| column.name().as_str() != index_column)
            .map(|column| {
                (
                    AttributeName::from(column.name().as_str()),
                    column.as_materialized_series().iter(),
                )
            })
            .collect();

        index_values
            .map(|index_value| {
                let node_index = NodeIndex::try_from(Value::try_from(index_value)?)?;

                let attributes = attribute_columns
                    .iter_mut()
                    .map(|(attribute_name, values)| {
                        let value = values
                            .next()
                            .expect("Attribute column must have a value for every row.");

                        Ok((attribute_name.clone(), Value::try_from(value)?))
                    })
                    .collect::<GraphRecordResult<_>>()?;

                Ok((node_index, attributes))
            })
            .collect()
    }
}

impl<S: Into<String>> EdgeSource for (DataFrame, S, S) {
    fn collect_edges(self) -> GraphRecordResult<EdgeBatch> {
        let (mut dataframe, source_index_column, target_index_column) = self;
        let source_index_column = source_index_column.into();
        let target_index_column = target_index_column.into();

        if dataframe.max_n_chunks() > 1 {
            dataframe.rechunk_mut();
        }

        let source_index_values = dataframe
            .column(&source_index_column)
            .map_err(|_| ConversionError::ColumnNotFound {
                column_name: source_index_column.clone(),
            })?
            .as_materialized_series()
            .iter();
        let target_index_values = dataframe
            .column(&target_index_column)
            .map_err(|_| ConversionError::ColumnNotFound {
                column_name: target_index_column.clone(),
            })?
            .as_materialized_series()
            .iter();

        let mut attribute_columns: Vec<_> = dataframe
            .columns()
            .iter()
            .filter(|column| {
                column.name().as_str() != source_index_column
                    && column.name().as_str() != target_index_column
            })
            .map(|column| {
                (
                    AttributeName::from(column.name().as_str()),
                    column.as_materialized_series().iter(),
                )
            })
            .collect();

        source_index_values
            .zip(target_index_values)
            .map(|(source_index_value, target_index_value)| {
                let source_node_index = NodeIndex::try_from(Value::try_from(source_index_value)?)?;
                let target_node_index = NodeIndex::try_from(Value::try_from(target_index_value)?)?;

                let attributes = attribute_columns
                    .iter_mut()
                    .map(|(attribute_name, values)| {
                        let value = values
                            .next()
                            .expect("Attribute column must have a value for every row.");

                        Ok((attribute_name.clone(), Value::try_from(value)?))
                    })
                    .collect::<GraphRecordResult<_>>()?;

                Ok((source_node_index, target_node_index, attributes))
            })
            .collect()
    }
}

impl<'a> From<ValueView<'a>> for AnyValue<'a> {
    fn from(value: ValueView<'a>) -> Self {
        match value {
            ValueView::String(Cow::Borrowed(value)) => Self::String(value),
            ValueView::String(Cow::Owned(value)) => Self::StringOwned(value.into()),
            ValueView::Int(value) => Self::Int64(value),
            ValueView::Float(value) => Self::Float64(value),
            ValueView::Bool(value) => Self::Boolean(value),
            ValueView::DateTime(value) => Self::Datetime(
                value.and_utc().timestamp_millis(),
                TimeUnit::Milliseconds,
                None,
            ),
            ValueView::Duration(value) => {
                Self::Duration(value.num_milliseconds(), TimeUnit::Milliseconds)
            }
            ValueView::Null => Self::Null,
        }
    }
}

pub struct PolarsFrames;

impl PolarsFrames {
    fn identifier_value(identifier: IdentifierView<'_>) -> ValueView<'_> {
        match identifier {
            IdentifierView::String(value) => ValueView::String(value),
            IdentifierView::Int(value) => ValueView::Int(value),
        }
    }

    fn column_name(attribute_name: &AttributeName) -> String {
        match attribute_name.identifier() {
            Identifier::String(value) => value.clone(),
            Identifier::Int(value) => value.to_string(),
        }
    }

    fn sorted_attribute_names(
        attribute_schema: &HashMap<AttributeName, AttributeDataType>,
    ) -> Vec<(String, &AttributeName)> {
        let mut attribute_names: Vec<_> = attribute_schema
            .keys()
            .map(|attribute_name| (Self::column_name(attribute_name), attribute_name))
            .collect();

        attribute_names.sort_by(|left, right| left.0.cmp(&right.0));

        attribute_names
    }

    fn column(column_name: &str, values: Vec<ValueView<'_>>) -> GraphRecordResult<Column> {
        let mut data_types = Vec::new();

        for value in &values {
            let data_type = match value {
                ValueView::String(_) => DataType::String,
                ValueView::Int(_) => DataType::Int,
                ValueView::Float(_) => DataType::Float,
                ValueView::Bool(_) => DataType::Bool,
                ValueView::DateTime(_) => DataType::DateTime,
                ValueView::Duration(_) => DataType::Duration,
                ValueView::Null => continue,
            };

            if !data_types.contains(&data_type) {
                data_types.push(data_type);
            }
        }

        if data_types.len() > 1 {
            return Err(GraphRecordError::Conversion(
                ConversionError::MixedColumnTypes {
                    column_name: column_name.to_string(),
                    data_types,
                },
            ));
        }

        let data_type = values
            .iter()
            .find(|value| !matches!(value, ValueView::Null))
            .map_or(PolarsDataType::Null, |value| match value {
                ValueView::String(_) => PolarsDataType::String,
                ValueView::Int(_) => PolarsDataType::Int64,
                ValueView::Float(_) => PolarsDataType::Float64,
                ValueView::Bool(_) => PolarsDataType::Boolean,
                ValueView::DateTime(_) => PolarsDataType::Datetime(TimeUnit::Milliseconds, None),
                ValueView::Duration(_) => PolarsDataType::Duration(TimeUnit::Milliseconds),
                ValueView::Null => PolarsDataType::Null,
            });

        let values: Vec<_> = values.into_iter().map(AnyValue::from).collect();

        let series =
            Series::from_any_values_and_dtype(column_name.into(), &values, &data_type, true)
                .expect("Column values must match the common type.");

        Ok(series.into())
    }

    fn node_frame(
        state: &GraphState,
        attribute_schema: &HashMap<AttributeName, AttributeDataType>,
        node_addresses: &[NodeAddress],
        group_index: Option<&GroupIndex>,
    ) -> GraphRecordResult<DataFrame> {
        let node_index_name = AttributeName::from("node_index");

        if attribute_schema.contains_key(&node_index_name) {
            return Err(GraphRecordError::Conversion(
                ConversionError::ReservedAttributeName {
                    attribute_name: node_index_name,
                },
            ));
        }

        let node_index_values = node_addresses
            .iter()
            .map(|address| {
                Self::identifier_value(state.node_key(*address).expect("Node must exist."))
            })
            .collect();

        let mut columns = vec![Self::column("node_index", node_index_values)?];

        for (column_name, attribute_name) in Self::sorted_attribute_names(attribute_schema) {
            let values = node_addresses
                .iter()
                .map(|address| {
                    state
                        .node_attribute_by_name(*address, attribute_name)
                        .unwrap_or(ValueView::Null)
                })
                .collect();

            columns.push(Self::column(&column_name, values)?);
        }

        DataFrame::new(node_addresses.len(), columns).map_err(|_| {
            GraphRecordError::Conversion(ConversionError::NodeDataFrameCreation {
                group_index: group_index.cloned(),
            })
        })
    }

    fn edge_frame(
        state: &GraphState,
        attribute_schema: &HashMap<AttributeName, AttributeDataType>,
        edge_addresses: &[EdgeAddress],
        group_index: Option<&GroupIndex>,
    ) -> GraphRecordResult<DataFrame> {
        for attribute_name in [
            AttributeName::from("source_node_index"),
            AttributeName::from("target_node_index"),
        ] {
            if attribute_schema.contains_key(&attribute_name) {
                return Err(GraphRecordError::Conversion(
                    ConversionError::ReservedAttributeName { attribute_name },
                ));
            }
        }

        let endpoints: Vec<_> = edge_addresses
            .iter()
            .map(|address| state.edge_endpoints(*address).expect("Edge must exist."))
            .collect();

        let source_node_index_values = endpoints
            .iter()
            .map(|edge_endpoints| {
                Self::identifier_value(
                    state
                        .node_key(edge_endpoints.source_address)
                        .expect("Node must exist."),
                )
            })
            .collect();
        let target_node_index_values = endpoints
            .iter()
            .map(|edge_endpoints| {
                Self::identifier_value(
                    state
                        .node_key(edge_endpoints.target_address)
                        .expect("Node must exist."),
                )
            })
            .collect();

        let mut columns = vec![
            Self::column("source_node_index", source_node_index_values)?,
            Self::column("target_node_index", target_node_index_values)?,
        ];

        for (column_name, attribute_name) in Self::sorted_attribute_names(attribute_schema) {
            let values = edge_addresses
                .iter()
                .map(|address| {
                    state
                        .edge_attribute_by_name(*address, attribute_name)
                        .unwrap_or(ValueView::Null)
                })
                .collect();

            columns.push(Self::column(&column_name, values)?);
        }

        DataFrame::new(edge_addresses.len(), columns).map_err(|_| {
            GraphRecordError::Conversion(ConversionError::EdgeDataFrameCreation {
                group_index: group_index.cloned(),
            })
        })
    }
}

impl Writer for PolarsFrames {
    type Output = Export<DataFrame>;

    fn write(self, graphrecord: &GraphRecord) -> GraphRecordResult<Self::Output> {
        let state = graphrecord.state();
        let schema = graphrecord.schema();

        let ungrouped_node_addresses: Vec<_> = state
            .node_addresses()
            .filter(|address| state.node_memberships(*address).next().is_none())
            .collect();
        let ungrouped_edge_addresses: Vec<_> = state
            .edge_addresses()
            .filter(|address| state.edge_memberships(*address).next().is_none())
            .collect();

        let ungrouped = Tables {
            nodes: Self::node_frame(
                state,
                schema.ungrouped().nodes(),
                &ungrouped_node_addresses,
                None,
            )?,
            edges: Self::edge_frame(
                state,
                schema.ungrouped().edges(),
                &ungrouped_edge_addresses,
                None,
            )?,
        };

        let empty_group_schema = GroupSchema::default();

        let groups = state
            .group_addresses()
            .map(|group_address| {
                let group_index = state
                    .group_index(group_address)
                    .cloned()
                    .expect("Group must exist.");
                let group_schema = schema
                    .groups()
                    .get(&group_index)
                    .unwrap_or(&empty_group_schema);

                let node_addresses: Vec<_> =
                    state.group_node_member_addresses(group_address).collect();
                let edge_addresses: Vec<_> =
                    state.group_edge_member_addresses(group_address).collect();

                let tables = Tables {
                    nodes: Self::node_frame(
                        state,
                        group_schema.nodes(),
                        &node_addresses,
                        Some(&group_index),
                    )?,
                    edges: Self::edge_frame(
                        state,
                        group_schema.edges(),
                        &edge_addresses,
                        Some(&group_index),
                    )?,
                };

                Ok((group_index, tables))
            })
            .collect::<GraphRecordResult<_>>()?;

        Ok(Export { ungrouped, groups })
    }
}

impl GraphRecord {
    pub fn to_polars(&self) -> GraphRecordResult<Export<DataFrame>> {
        self.export(PolarsFrames)
    }
}

#[cfg(test)]
mod test {
    use super::PolarsFrames;
    use crate::{
        errors::{ConversionError, GraphRecordError},
        graphrecord::{
            AttributeMap, GraphRecord,
            datatypes::{DataType, Value, ValueView},
            source::{EdgeSource, NodeSource},
        },
    };
    use chrono::{DateTime, TimeDelta};
    use polars::{
        datatypes::AnyValue,
        frame::DataFrame,
        prelude::{DataType as PolarsDataType, NamedFrom, PlSmallStr, Series, TimeUnit},
    };
    use std::borrow::Cow;

    fn create_node_dataframe() -> DataFrame {
        let index_column = Series::new("index".into(), &["lorem", "ipsum"]);
        let attribute_column = Series::new("sed".into(), &[Some(1), None]);

        DataFrame::new(2, vec![index_column.into(), attribute_column.into()]).unwrap()
    }

    fn create_edge_dataframe() -> DataFrame {
        let source_index_column = Series::new("source".into(), &["lorem", "ipsum"]);
        let target_index_column = Series::new("target".into(), &["ipsum", "dolor"]);
        let attribute_column = Series::new("sed".into(), &[1.5, 3.5]);

        DataFrame::new(
            2,
            vec![
                source_index_column.into(),
                target_index_column.into(),
                attribute_column.into(),
            ],
        )
        .unwrap()
    }

    fn create_export_graphrecord() -> GraphRecord {
        GraphRecord::new()
            .add_node(
                "lorem",
                AttributeMap::from([
                    ("count".into(), 1.into()),
                    ("score".into(), 1.5.into()),
                    ("active".into(), true.into()),
                    (
                        "created".into(),
                        Value::DateTime(DateTime::UNIX_EPOCH.naive_utc()),
                    ),
                    ("span".into(), Value::Duration(TimeDelta::seconds(5))),
                    ("label".into(), "ipsum dolor".into()),
                ]),
            )
            .unwrap()
            .add_node("ipsum", AttributeMap::from([("count".into(), 2.into())]))
            .unwrap()
            .add_edge(
                "lorem",
                "ipsum",
                AttributeMap::from([("weight".into(), 2.5.into())]),
            )
            .unwrap()
            .add_group("dolor")
            .unwrap()
            .add_node_in_group(
                "sit",
                AttributeMap::from([("count".into(), 3.into())]),
                "dolor",
            )
            .unwrap()
            .add_edge_in_group(
                "sit",
                "sit",
                AttributeMap::from([("weight".into(), 1.into())]),
                "dolor",
            )
            .unwrap()
            .add_group("elit")
            .unwrap()
            .add_node_in_group(
                "consectetur",
                AttributeMap::from([("count".into(), "tres".into())]),
                "elit",
            )
            .unwrap()
            .add_group("amet")
            .unwrap()
    }

    fn column_names(dataframe: &DataFrame) -> Vec<&str> {
        dataframe
            .get_column_names()
            .into_iter()
            .map(PlSmallStr::as_str)
            .collect()
    }

    #[test]
    fn test_try_from() {
        assert_eq!(
            Value::String("lorem".to_string()),
            Value::try_from(AnyValue::String("lorem")).unwrap()
        );
        assert_eq!(
            Value::String("lorem".to_string()),
            Value::try_from(AnyValue::StringOwned("lorem".into())).unwrap()
        );
        assert_eq!(Value::Int(1), Value::try_from(AnyValue::Int8(1)).unwrap());
        assert_eq!(Value::Int(1), Value::try_from(AnyValue::Int16(1)).unwrap());
        assert_eq!(Value::Int(1), Value::try_from(AnyValue::Int32(1)).unwrap());
        assert_eq!(Value::Int(1), Value::try_from(AnyValue::Int64(1)).unwrap());
        assert_eq!(Value::Int(1), Value::try_from(AnyValue::UInt8(1)).unwrap());
        assert_eq!(Value::Int(1), Value::try_from(AnyValue::UInt16(1)).unwrap());
        assert_eq!(Value::Int(1), Value::try_from(AnyValue::UInt32(1)).unwrap());
        assert_eq!(
            Value::Float(1.0),
            Value::try_from(AnyValue::Float32(1.0)).unwrap()
        );
        assert_eq!(
            Value::Float(1.0),
            Value::try_from(AnyValue::Float64(1.0)).unwrap()
        );
        assert_eq!(
            Value::Bool(true),
            Value::try_from(AnyValue::Boolean(true)).unwrap()
        );
        assert_eq!(
            Value::DateTime(DateTime::UNIX_EPOCH.naive_utc()),
            Value::try_from(AnyValue::Datetime(0, TimeUnit::Nanoseconds, None)).unwrap()
        );
        assert_eq!(
            Value::DateTime(DateTime::UNIX_EPOCH.naive_utc()),
            Value::try_from(AnyValue::Datetime(0, TimeUnit::Microseconds, None)).unwrap()
        );
        assert_eq!(
            Value::DateTime(DateTime::UNIX_EPOCH.naive_utc()),
            Value::try_from(AnyValue::Datetime(0, TimeUnit::Milliseconds, None)).unwrap()
        );
        assert_eq!(
            Value::Duration(TimeDelta::nanoseconds(1)),
            Value::try_from(AnyValue::Duration(1, TimeUnit::Nanoseconds)).unwrap()
        );
        assert_eq!(
            Value::Duration(TimeDelta::microseconds(1)),
            Value::try_from(AnyValue::Duration(1, TimeUnit::Microseconds)).unwrap()
        );
        assert_eq!(
            Value::Duration(TimeDelta::milliseconds(1)),
            Value::try_from(AnyValue::Duration(1, TimeUnit::Milliseconds)).unwrap()
        );
        assert_eq!(Value::Null, Value::try_from(AnyValue::Null).unwrap());
    }

    #[test]
    fn test_invalid_try_from() {
        assert!(
            Value::try_from(AnyValue::UInt64(1)).is_err_and(|error| matches!(
                error,
                GraphRecordError::Conversion(conversion_error)
                    if conversion_error == ConversionError::UnsupportedFrameValue {
                        value: "1".to_string()
                    }
            ))
        );
        assert!(
            Value::try_from(AnyValue::Datetime(i64::MAX, TimeUnit::Microseconds, None)).is_err_and(
                |error| matches!(
                    error,
                    GraphRecordError::Conversion(conversion_error)
                        if conversion_error == ConversionError::TimestampOutOfRange {
                            timestamp: i64::MAX
                        }
                )
            )
        );
        assert!(
            Value::try_from(AnyValue::Datetime(i64::MAX, TimeUnit::Milliseconds, None)).is_err_and(
                |error| matches!(
                    error,
                    GraphRecordError::Conversion(conversion_error)
                        if conversion_error == ConversionError::TimestampOutOfRange {
                            timestamp: i64::MAX
                        }
                )
            )
        );
    }

    #[test]
    fn test_collect_nodes() {
        let batch = (create_node_dataframe(), "index").collect_nodes().unwrap();

        assert_eq!(
            vec![
                (
                    "lorem".into(),
                    AttributeMap::from([("sed".into(), 1.into())]),
                ),
                (
                    "ipsum".into(),
                    AttributeMap::from([("sed".into(), Value::Null)]),
                ),
            ],
            batch.into_iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_invalid_collect_nodes() {
        assert!(
            (create_node_dataframe(), "dolor")
                .collect_nodes()
                .is_err_and(|error| matches!(
                    error,
                    GraphRecordError::Conversion(conversion_error)
                        if conversion_error == ConversionError::ColumnNotFound {
                            column_name: "dolor".to_string()
                        }
                ))
        );

        assert!(
            (create_node_dataframe(), "sed")
                .collect_nodes()
                .is_err_and(|error| matches!(
                    error,
                    GraphRecordError::Conversion(conversion_error)
                        if conversion_error == ConversionError::ValueToIdentifier {
                            value: Value::Null
                        }
                ))
        );
    }

    #[test]
    fn test_collect_edges() {
        let batch = (create_edge_dataframe(), "source", "target")
            .collect_edges()
            .unwrap();

        assert_eq!(
            vec![
                (
                    "lorem".into(),
                    "ipsum".into(),
                    AttributeMap::from([("sed".into(), 1.5.into())]),
                ),
                (
                    "ipsum".into(),
                    "dolor".into(),
                    AttributeMap::from([("sed".into(), 3.5.into())]),
                ),
            ],
            batch.into_iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_invalid_collect_edges() {
        assert!(
            (create_edge_dataframe(), "dolor", "target")
                .collect_edges()
                .is_err_and(|error| matches!(
                    error,
                    GraphRecordError::Conversion(conversion_error)
                        if conversion_error == ConversionError::ColumnNotFound {
                            column_name: "dolor".to_string()
                        }
                ))
        );

        assert!(
            (create_edge_dataframe(), "source", "dolor")
                .collect_edges()
                .is_err_and(|error| matches!(
                    error,
                    GraphRecordError::Conversion(conversion_error)
                        if conversion_error == ConversionError::ColumnNotFound {
                            column_name: "dolor".to_string()
                        }
                ))
        );

        assert!(
            (create_edge_dataframe(), "sed", "target")
                .collect_edges()
                .is_err_and(|error| matches!(
                    error,
                    GraphRecordError::Conversion(conversion_error)
                        if conversion_error == ConversionError::ValueToIdentifier {
                            value: Value::Float(1.5)
                        }
                ))
        );
    }

    #[test]
    fn test_from() {
        assert_eq!(
            AnyValue::String("lorem"),
            AnyValue::from(ValueView::String(Cow::Borrowed("lorem")))
        );
        assert_eq!(
            AnyValue::StringOwned("lorem".into()),
            AnyValue::from(ValueView::String(Cow::Owned("lorem".to_string())))
        );
        assert_eq!(AnyValue::Int64(1), AnyValue::from(ValueView::Int(1)));
        assert_eq!(
            AnyValue::Float64(1.5),
            AnyValue::from(ValueView::Float(1.5))
        );
        assert_eq!(
            AnyValue::Boolean(true),
            AnyValue::from(ValueView::Bool(true))
        );
        assert_eq!(
            AnyValue::Datetime(0, TimeUnit::Milliseconds, None),
            AnyValue::from(ValueView::DateTime(DateTime::UNIX_EPOCH.naive_utc()))
        );
        assert_eq!(
            AnyValue::Duration(5000, TimeUnit::Milliseconds),
            AnyValue::from(ValueView::Duration(TimeDelta::seconds(5)))
        );
        assert_eq!(AnyValue::Null, AnyValue::from(ValueView::Null));
    }

    #[test]
    fn test_write() {
        let graphrecord = create_export_graphrecord();

        let export = graphrecord.export(PolarsFrames).unwrap();

        let ungrouped_nodes = &export.ungrouped.nodes;

        assert_eq!(2, ungrouped_nodes.height());
        assert_eq!(
            vec![
                "node_index",
                "active",
                "count",
                "created",
                "label",
                "score",
                "span"
            ],
            column_names(ungrouped_nodes)
        );
        assert_eq!(
            AnyValue::String("lorem"),
            ungrouped_nodes
                .column("node_index")
                .unwrap()
                .get(0)
                .unwrap()
        );
        assert_eq!(
            AnyValue::String("ipsum"),
            ungrouped_nodes
                .column("node_index")
                .unwrap()
                .get(1)
                .unwrap()
        );
        assert_eq!(
            &PolarsDataType::Int64,
            ungrouped_nodes.column("count").unwrap().dtype()
        );
        assert_eq!(
            AnyValue::Int64(1),
            ungrouped_nodes.column("count").unwrap().get(0).unwrap()
        );
        assert_eq!(
            AnyValue::Int64(2),
            ungrouped_nodes.column("count").unwrap().get(1).unwrap()
        );
        assert_eq!(
            &PolarsDataType::Boolean,
            ungrouped_nodes.column("active").unwrap().dtype()
        );
        assert_eq!(
            AnyValue::Boolean(true),
            ungrouped_nodes.column("active").unwrap().get(0).unwrap()
        );
        assert_eq!(
            AnyValue::Null,
            ungrouped_nodes.column("active").unwrap().get(1).unwrap()
        );
        assert_eq!(
            &PolarsDataType::Datetime(TimeUnit::Milliseconds, None),
            ungrouped_nodes.column("created").unwrap().dtype()
        );
        assert_eq!(
            AnyValue::Datetime(0, TimeUnit::Milliseconds, None),
            ungrouped_nodes.column("created").unwrap().get(0).unwrap()
        );
        assert_eq!(
            &PolarsDataType::String,
            ungrouped_nodes.column("label").unwrap().dtype()
        );
        assert_eq!(
            AnyValue::String("ipsum dolor"),
            ungrouped_nodes.column("label").unwrap().get(0).unwrap()
        );
        assert_eq!(
            &PolarsDataType::Float64,
            ungrouped_nodes.column("score").unwrap().dtype()
        );
        assert_eq!(
            AnyValue::Float64(1.5),
            ungrouped_nodes.column("score").unwrap().get(0).unwrap()
        );
        assert_eq!(
            &PolarsDataType::Duration(TimeUnit::Milliseconds),
            ungrouped_nodes.column("span").unwrap().dtype()
        );
        assert_eq!(
            AnyValue::Duration(5000, TimeUnit::Milliseconds),
            ungrouped_nodes.column("span").unwrap().get(0).unwrap()
        );

        let ungrouped_edges = &export.ungrouped.edges;

        assert_eq!(1, ungrouped_edges.height());
        assert_eq!(
            vec!["source_node_index", "target_node_index", "weight"],
            column_names(ungrouped_edges)
        );
        assert_eq!(
            AnyValue::String("lorem"),
            ungrouped_edges
                .column("source_node_index")
                .unwrap()
                .get(0)
                .unwrap()
        );
        assert_eq!(
            AnyValue::String("ipsum"),
            ungrouped_edges
                .column("target_node_index")
                .unwrap()
                .get(0)
                .unwrap()
        );
        assert_eq!(
            AnyValue::Float64(2.5),
            ungrouped_edges.column("weight").unwrap().get(0).unwrap()
        );

        assert_eq!(3, export.groups.len());

        let dolor_tables = &export.groups[&"dolor".into()];

        assert_eq!(1, dolor_tables.nodes.height());
        assert_eq!(
            vec!["node_index", "count"],
            column_names(&dolor_tables.nodes)
        );
        assert_eq!(
            AnyValue::String("sit"),
            dolor_tables
                .nodes
                .column("node_index")
                .unwrap()
                .get(0)
                .unwrap()
        );
        assert_eq!(1, dolor_tables.edges.height());
        assert_eq!(
            vec!["source_node_index", "target_node_index", "weight"],
            column_names(&dolor_tables.edges)
        );
        assert_eq!(
            AnyValue::Int64(1),
            dolor_tables.edges.column("weight").unwrap().get(0).unwrap()
        );

        let elit_tables = &export.groups[&"elit".into()];

        assert_eq!(
            &PolarsDataType::String,
            elit_tables.nodes.column("count").unwrap().dtype()
        );
        assert_eq!(
            AnyValue::String("tres"),
            elit_tables.nodes.column("count").unwrap().get(0).unwrap()
        );

        let amet_tables = &export.groups[&"amet".into()];

        assert_eq!(0, amet_tables.nodes.height());
        assert_eq!(vec!["node_index"], column_names(&amet_tables.nodes));
        assert_eq!(0, amet_tables.edges.height());
        assert_eq!(
            vec!["source_node_index", "target_node_index"],
            column_names(&amet_tables.edges)
        );
    }

    #[test]
    fn test_invalid_write() {
        let mixed_attributes = GraphRecord::new()
            .add_node("lorem", AttributeMap::from([("count".into(), 1.into())]))
            .unwrap()
            .add_node(
                "ipsum",
                AttributeMap::from([("count".into(), "text".into())]),
            )
            .unwrap();

        assert!(
            mixed_attributes
                .export(PolarsFrames)
                .is_err_and(|error| matches!(
                    error,
                    GraphRecordError::Conversion(conversion_error)
                        if conversion_error == ConversionError::MixedColumnTypes {
                            column_name: "count".to_string(),
                            data_types: vec![DataType::Int, DataType::String]
                        }
                ))
        );

        let mixed_node_indices = GraphRecord::new()
            .add_node(1_i64, AttributeMap::new())
            .unwrap()
            .add_node("lorem", AttributeMap::new())
            .unwrap();

        assert!(
            mixed_node_indices
                .export(PolarsFrames)
                .is_err_and(|error| matches!(
                    error,
                    GraphRecordError::Conversion(conversion_error)
                        if conversion_error == ConversionError::MixedColumnTypes {
                            column_name: "node_index".to_string(),
                            data_types: vec![DataType::Int, DataType::String]
                        }
                ))
        );

        let reserved_node_attribute = GraphRecord::new()
            .add_node(
                "lorem",
                AttributeMap::from([("node_index".into(), 1.into())]),
            )
            .unwrap();

        assert!(
            reserved_node_attribute
                .export(PolarsFrames)
                .is_err_and(|error| matches!(
                    error,
                    GraphRecordError::Conversion(conversion_error)
                        if conversion_error == ConversionError::ReservedAttributeName {
                            attribute_name: "node_index".into()
                        }
                ))
        );

        let reserved_edge_attribute = GraphRecord::new()
            .add_node("lorem", AttributeMap::new())
            .unwrap()
            .add_edge(
                "lorem",
                "lorem",
                AttributeMap::from([("target_node_index".into(), 1.into())]),
            )
            .unwrap();

        assert!(
            reserved_edge_attribute
                .export(PolarsFrames)
                .is_err_and(|error| matches!(
                    error,
                    GraphRecordError::Conversion(conversion_error)
                        if conversion_error == ConversionError::ReservedAttributeName {
                            attribute_name: "target_node_index".into()
                        }
                ))
        );

        let colliding_node_attributes = GraphRecord::new()
            .add_node(
                "lorem",
                AttributeMap::from([(1_i64.into(), 1.into()), ("1".into(), 2.into())]),
            )
            .unwrap();

        assert!(
            colliding_node_attributes
                .export(PolarsFrames)
                .is_err_and(|error| matches!(
                    error,
                    GraphRecordError::Conversion(conversion_error)
                        if conversion_error == ConversionError::NodeDataFrameCreation { group_index: None }
                ))
        );

        let colliding_edge_attributes = GraphRecord::new()
            .add_node("lorem", AttributeMap::new())
            .unwrap()
            .add_edge(
                "lorem",
                "lorem",
                AttributeMap::from([(1_i64.into(), 1.into()), ("1".into(), 2.into())]),
            )
            .unwrap();

        assert!(
            colliding_edge_attributes
                .export(PolarsFrames)
                .is_err_and(|error| matches!(
                    error,
                    GraphRecordError::Conversion(conversion_error)
                        if conversion_error == ConversionError::EdgeDataFrameCreation { group_index: None }
                ))
        );
    }

    #[test]
    fn test_to_polars() {
        let export = create_export_graphrecord().to_polars().unwrap();

        assert_eq!(2, export.ungrouped.nodes.height());
        assert_eq!(1, export.ungrouped.edges.height());
        assert_eq!(3, export.groups.len());
    }
}
