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
use arrow::{
    array::{
        Array, ArrayRef, AsArray, BooleanArray, DurationMillisecondArray, Float64Array, Int64Array,
        NullArray, RecordBatch, StringArray, TimestampMillisecondArray,
    },
    datatypes::{
        DataType as ArrowDataType, DurationMicrosecondType, DurationMillisecondType,
        DurationNanosecondType, DurationSecondType, Float32Type, Float64Type, Int8Type, Int16Type,
        Int32Type, Int64Type, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
        TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type, UInt32Type,
    },
    util::display::array_value_to_string,
};
use chrono::{DateTime, TimeDelta};
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    sync::Arc,
};

#[derive(Debug, PartialEq)]
enum ArrowValue<'a> {
    String(&'a str),
    StringOwned(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Timestamp(i64, TimeUnit),
    Duration(i64, TimeUnit),
    Null,
    Unsupported(String),
}

impl<'a> ArrowValue<'a> {
    fn from_array(array: &'a dyn Array, row_index: usize) -> Self {
        let data_type = array.data_type();

        if matches!(data_type, ArrowDataType::Null) {
            return Self::Null;
        }

        if array.is_null(row_index) {
            return Self::Null;
        }

        match data_type {
            ArrowDataType::Utf8 => Self::String(array.as_string::<i32>().value(row_index)),
            ArrowDataType::LargeUtf8 => Self::String(array.as_string::<i64>().value(row_index)),
            ArrowDataType::Int8 => {
                Self::Int(array.as_primitive::<Int8Type>().value(row_index).into())
            }
            ArrowDataType::Int16 => {
                Self::Int(array.as_primitive::<Int16Type>().value(row_index).into())
            }
            ArrowDataType::Int32 => {
                Self::Int(array.as_primitive::<Int32Type>().value(row_index).into())
            }
            ArrowDataType::Int64 => Self::Int(array.as_primitive::<Int64Type>().value(row_index)),
            ArrowDataType::UInt8 => {
                Self::Int(array.as_primitive::<UInt8Type>().value(row_index).into())
            }
            ArrowDataType::UInt16 => {
                Self::Int(array.as_primitive::<UInt16Type>().value(row_index).into())
            }
            ArrowDataType::UInt32 => {
                Self::Int(array.as_primitive::<UInt32Type>().value(row_index).into())
            }
            ArrowDataType::Float32 => {
                Self::Float(array.as_primitive::<Float32Type>().value(row_index).into())
            }
            ArrowDataType::Float64 => {
                Self::Float(array.as_primitive::<Float64Type>().value(row_index))
            }
            ArrowDataType::Boolean => Self::Bool(array.as_boolean().value(row_index)),
            ArrowDataType::Timestamp(time_unit, _) => Self::Timestamp(
                match time_unit {
                    TimeUnit::Second => {
                        array.as_primitive::<TimestampSecondType>().value(row_index)
                    }
                    TimeUnit::Millisecond => array
                        .as_primitive::<TimestampMillisecondType>()
                        .value(row_index),
                    TimeUnit::Microsecond => array
                        .as_primitive::<TimestampMicrosecondType>()
                        .value(row_index),
                    TimeUnit::Nanosecond => array
                        .as_primitive::<TimestampNanosecondType>()
                        .value(row_index),
                },
                *time_unit,
            ),
            ArrowDataType::Duration(time_unit) => Self::Duration(
                match time_unit {
                    TimeUnit::Second => array.as_primitive::<DurationSecondType>().value(row_index),
                    TimeUnit::Millisecond => array
                        .as_primitive::<DurationMillisecondType>()
                        .value(row_index),
                    TimeUnit::Microsecond => array
                        .as_primitive::<DurationMicrosecondType>()
                        .value(row_index),
                    TimeUnit::Nanosecond => array
                        .as_primitive::<DurationNanosecondType>()
                        .value(row_index),
                },
                *time_unit,
            ),
            _ => Self::Unsupported(
                array_value_to_string(array, row_index)
                    .unwrap_or_else(|arrow_error| arrow_error.to_string()),
            ),
        }
    }
}

impl TryFrom<ArrowValue<'_>> for Value {
    type Error = GraphRecordError;

    fn try_from(value: ArrowValue<'_>) -> Result<Self, Self::Error> {
        match value {
            ArrowValue::String(value) => Ok(Self::String(value.into())),
            ArrowValue::StringOwned(value) => Ok(Self::String(value)),
            ArrowValue::Int(value) => Ok(Self::Int(value)),
            ArrowValue::Float(value) => Ok(Self::Float(value)),
            ArrowValue::Bool(value) => Ok(Self::Bool(value)),
            ArrowValue::Timestamp(value, time_unit) => match time_unit {
                TimeUnit::Second => DateTime::from_timestamp(value, 0)
                    .map(|datetime| Self::DateTime(datetime.naive_utc()))
                    .ok_or(GraphRecordError::Conversion(
                        ConversionError::TimestampOutOfRange { timestamp: value },
                    )),
                TimeUnit::Millisecond => DateTime::from_timestamp_millis(value)
                    .map(|datetime| Self::DateTime(datetime.naive_utc()))
                    .ok_or(GraphRecordError::Conversion(
                        ConversionError::TimestampOutOfRange { timestamp: value },
                    )),
                TimeUnit::Microsecond => DateTime::from_timestamp_micros(value)
                    .map(|datetime| Self::DateTime(datetime.naive_utc()))
                    .ok_or(GraphRecordError::Conversion(
                        ConversionError::TimestampOutOfRange { timestamp: value },
                    )),
                TimeUnit::Nanosecond => Ok(Self::DateTime(
                    DateTime::from_timestamp_nanos(value).naive_utc(),
                )),
            },
            ArrowValue::Duration(value, time_unit) => Ok(Self::Duration(match time_unit {
                TimeUnit::Second => TimeDelta::seconds(value),
                TimeUnit::Millisecond => TimeDelta::milliseconds(value),
                TimeUnit::Microsecond => TimeDelta::microseconds(value),
                TimeUnit::Nanosecond => TimeDelta::nanoseconds(value),
            })),
            ArrowValue::Null => Ok(Self::Null),
            ArrowValue::Unsupported(value) => Err(GraphRecordError::Conversion(
                ConversionError::UnsupportedFrameValue { value },
            )),
        }
    }
}

impl<S: Into<String>> NodeSource for (Vec<RecordBatch>, S) {
    fn collect_nodes(self) -> GraphRecordResult<NodeBatch> {
        let (record_batches, index_column) = self;
        let index_column = index_column.into();

        record_batches
            .iter()
            .map(|record_batch| {
                let index_array = record_batch.column_by_name(&index_column).ok_or_else(|| {
                    GraphRecordError::Conversion(ConversionError::ColumnNotFound {
                        column_name: index_column.clone(),
                    })
                })?;

                let attribute_columns: Vec<_> = record_batch
                    .schema()
                    .fields()
                    .iter()
                    .zip(record_batch.columns())
                    .filter(|(field, _)| field.name().as_str() != index_column)
                    .map(|(field, array)| (AttributeName::from(field.name().as_str()), array))
                    .collect();

                (0..record_batch.num_rows())
                    .map(|row_index| {
                        let node_index = NodeIndex::try_from(Value::try_from(
                            ArrowValue::from_array(index_array, row_index),
                        )?)?;

                        let attributes = attribute_columns
                            .iter()
                            .map(|(attribute_name, array)| {
                                Ok((
                                    attribute_name.clone(),
                                    Value::try_from(ArrowValue::from_array(array, row_index))?,
                                ))
                            })
                            .collect::<GraphRecordResult<_>>()?;

                        Ok((node_index, attributes))
                    })
                    .collect::<GraphRecordResult<Vec<_>>>()
            })
            .collect::<GraphRecordResult<Vec<Vec<_>>>>()
            .map(|node_batches| node_batches.into_iter().flatten().collect())
    }
}

impl<S: Into<String>> EdgeSource for (Vec<RecordBatch>, S, S) {
    fn collect_edges(self) -> GraphRecordResult<EdgeBatch> {
        let (record_batches, source_index_column, target_index_column) = self;
        let source_index_column = source_index_column.into();
        let target_index_column = target_index_column.into();

        record_batches
            .iter()
            .map(|record_batch| {
                let source_index_array = record_batch
                    .column_by_name(&source_index_column)
                    .ok_or_else(|| {
                        GraphRecordError::Conversion(ConversionError::ColumnNotFound {
                            column_name: source_index_column.clone(),
                        })
                    })?;
                let target_index_array = record_batch
                    .column_by_name(&target_index_column)
                    .ok_or_else(|| {
                        GraphRecordError::Conversion(ConversionError::ColumnNotFound {
                            column_name: target_index_column.clone(),
                        })
                    })?;

                let attribute_columns: Vec<_> = record_batch
                    .schema()
                    .fields()
                    .iter()
                    .zip(record_batch.columns())
                    .filter(|(field, _)| {
                        field.name().as_str() != source_index_column
                            && field.name().as_str() != target_index_column
                    })
                    .map(|(field, array)| (AttributeName::from(field.name().as_str()), array))
                    .collect();

                (0..record_batch.num_rows())
                    .map(|row_index| {
                        let source_node_index = NodeIndex::try_from(Value::try_from(
                            ArrowValue::from_array(source_index_array, row_index),
                        )?)?;
                        let target_node_index = NodeIndex::try_from(Value::try_from(
                            ArrowValue::from_array(target_index_array, row_index),
                        )?)?;

                        let attributes = attribute_columns
                            .iter()
                            .map(|(attribute_name, array)| {
                                Ok((
                                    attribute_name.clone(),
                                    Value::try_from(ArrowValue::from_array(array, row_index))?,
                                ))
                            })
                            .collect::<GraphRecordResult<_>>()?;

                        Ok((source_node_index, target_node_index, attributes))
                    })
                    .collect::<GraphRecordResult<Vec<_>>>()
            })
            .collect::<GraphRecordResult<Vec<Vec<_>>>>()
            .map(|edge_batches| edge_batches.into_iter().flatten().collect())
    }
}

impl<'a> From<ValueView<'a>> for ArrowValue<'a> {
    fn from(value: ValueView<'a>) -> Self {
        match value {
            ValueView::String(Cow::Borrowed(value)) => Self::String(value),
            ValueView::String(Cow::Owned(value)) => Self::StringOwned(value),
            ValueView::Int(value) => Self::Int(value),
            ValueView::Float(value) => Self::Float(value),
            ValueView::Bool(value) => Self::Bool(value),
            ValueView::DateTime(value) => {
                Self::Timestamp(value.and_utc().timestamp_millis(), TimeUnit::Millisecond)
            }
            ValueView::Duration(value) => {
                Self::Duration(value.num_milliseconds(), TimeUnit::Millisecond)
            }
            ValueView::Null => Self::Null,
        }
    }
}

pub struct ArrowTables;

impl ArrowTables {
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

    fn column(column_name: &str, values: Vec<ValueView<'_>>) -> GraphRecordResult<ArrayRef> {
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

        let row_count = values.len();
        let values: Vec<_> = values.into_iter().map(ArrowValue::from).collect();

        let array: ArrayRef = match data_types.first() {
            None => Arc::new(NullArray::new(row_count)),
            Some(DataType::String) => Arc::new(
                values
                    .into_iter()
                    .map(|value| match value {
                        ArrowValue::String(value) => Some(Cow::Borrowed(value)),
                        ArrowValue::StringOwned(value) => Some(Cow::Owned(value)),
                        ArrowValue::Null => None,
                        _ => unreachable!("Column values must match the common type."),
                    })
                    .collect::<StringArray>(),
            ),
            Some(DataType::Int) => Arc::new(
                values
                    .into_iter()
                    .map(|value| match value {
                        ArrowValue::Int(value) => Some(value),
                        ArrowValue::Null => None,
                        _ => unreachable!("Column values must match the common type."),
                    })
                    .collect::<Int64Array>(),
            ),
            Some(DataType::Float) => Arc::new(
                values
                    .into_iter()
                    .map(|value| match value {
                        ArrowValue::Float(value) => Some(value),
                        ArrowValue::Null => None,
                        _ => unreachable!("Column values must match the common type."),
                    })
                    .collect::<Float64Array>(),
            ),
            Some(DataType::Bool) => Arc::new(
                values
                    .into_iter()
                    .map(|value| match value {
                        ArrowValue::Bool(value) => Some(value),
                        ArrowValue::Null => None,
                        _ => unreachable!("Column values must match the common type."),
                    })
                    .collect::<BooleanArray>(),
            ),
            Some(DataType::DateTime) => Arc::new(
                values
                    .into_iter()
                    .map(|value| match value {
                        ArrowValue::Timestamp(value, _) => Some(value),
                        ArrowValue::Null => None,
                        _ => unreachable!("Column values must match the common type."),
                    })
                    .collect::<TimestampMillisecondArray>(),
            ),
            Some(DataType::Duration) => Arc::new(
                values
                    .into_iter()
                    .map(|value| match value {
                        ArrowValue::Duration(value, _) => Some(value),
                        ArrowValue::Null => None,
                        _ => unreachable!("Column values must match the common type."),
                    })
                    .collect::<DurationMillisecondArray>(),
            ),
            _ => unreachable!(
                "Column data type must be String, Int, Float, Bool, DateTime, or Duration."
            ),
        };

        Ok(array)
    }

    fn node_frame(
        state: &GraphState,
        attribute_schema: &HashMap<AttributeName, AttributeDataType>,
        node_addresses: &[NodeAddress],
        group_index: Option<&GroupIndex>,
    ) -> GraphRecordResult<RecordBatch> {
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

        let node_index_array = Self::column("node_index", node_index_values)?;

        let mut columns = vec![("node_index".to_string(), node_index_array)];

        for (column_name, attribute_name) in Self::sorted_attribute_names(attribute_schema) {
            let values = node_addresses
                .iter()
                .map(|address| {
                    state
                        .node_attribute_by_name(*address, attribute_name)
                        .unwrap_or(ValueView::Null)
                })
                .collect();

            let array = Self::column(&column_name, values)?;

            columns.push((column_name, array));
        }

        let unique_column_names: HashSet<_> =
            columns.iter().map(|(name, _)| name.as_str()).collect();

        if unique_column_names.len() != columns.len() {
            return Err(GraphRecordError::Conversion(
                ConversionError::NodeDataFrameCreation {
                    group_index: group_index.cloned(),
                },
            ));
        }

        RecordBatch::try_from_iter(columns).map_err(|_| {
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
    ) -> GraphRecordResult<RecordBatch> {
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

        let source_node_index_array = Self::column("source_node_index", source_node_index_values)?;
        let target_node_index_array = Self::column("target_node_index", target_node_index_values)?;

        let mut columns = vec![
            ("source_node_index".to_string(), source_node_index_array),
            ("target_node_index".to_string(), target_node_index_array),
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

            let array = Self::column(&column_name, values)?;

            columns.push((column_name, array));
        }

        let unique_column_names: HashSet<_> =
            columns.iter().map(|(name, _)| name.as_str()).collect();

        if unique_column_names.len() != columns.len() {
            return Err(GraphRecordError::Conversion(
                ConversionError::EdgeDataFrameCreation {
                    group_index: group_index.cloned(),
                },
            ));
        }

        RecordBatch::try_from_iter(columns).map_err(|_| {
            GraphRecordError::Conversion(ConversionError::EdgeDataFrameCreation {
                group_index: group_index.cloned(),
            })
        })
    }
}

impl Writer for ArrowTables {
    type Output = Export<RecordBatch>;

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
    pub fn to_arrow(&self) -> GraphRecordResult<Export<RecordBatch>> {
        self.export(ArrowTables)
    }
}

#[cfg(test)]
mod test {
    use super::{ArrowTables, ArrowValue};
    use crate::{
        errors::{ConversionError, GraphRecordError},
        graphrecord::{
            AttributeMap, GraphRecord,
            datatypes::{DataType, Value, ValueView},
            source::{EdgeSource, NodeSource},
        },
    };
    use arrow::{
        array::{ArrayRef, Float64Array, Int64Array, ListArray, RecordBatch, StringArray},
        datatypes::{DataType as ArrowDataType, TimeUnit},
    };
    use chrono::{DateTime, TimeDelta};
    use std::{borrow::Cow, sync::Arc};

    fn create_node_record_batches() -> Vec<RecordBatch> {
        vec![
            RecordBatch::try_from_iter([
                (
                    "index",
                    Arc::new(StringArray::from(vec!["lorem"])) as ArrayRef,
                ),
                ("sed", Arc::new(Int64Array::from(vec![Some(1)])) as ArrayRef),
            ])
            .unwrap(),
            RecordBatch::try_from_iter([
                (
                    "index",
                    Arc::new(StringArray::from(vec!["ipsum"])) as ArrayRef,
                ),
                ("sed", Arc::new(Int64Array::from(vec![None])) as ArrayRef),
            ])
            .unwrap(),
        ]
    }

    fn create_edge_record_batches() -> Vec<RecordBatch> {
        vec![
            RecordBatch::try_from_iter([
                (
                    "source",
                    Arc::new(StringArray::from(vec!["lorem"])) as ArrayRef,
                ),
                (
                    "target",
                    Arc::new(StringArray::from(vec!["ipsum"])) as ArrayRef,
                ),
                ("sed", Arc::new(Float64Array::from(vec![1.5])) as ArrayRef),
            ])
            .unwrap(),
            RecordBatch::try_from_iter([
                (
                    "source",
                    Arc::new(StringArray::from(vec!["ipsum"])) as ArrayRef,
                ),
                (
                    "target",
                    Arc::new(StringArray::from(vec!["dolor"])) as ArrayRef,
                ),
                ("sed", Arc::new(Float64Array::from(vec![3.5])) as ArrayRef),
            ])
            .unwrap(),
        ]
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

    fn column_names(record_batch: &RecordBatch) -> Vec<&str> {
        record_batch
            .schema_ref()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect()
    }

    #[test]
    fn test_try_from() {
        assert_eq!(
            Value::String("lorem".to_string()),
            Value::try_from(ArrowValue::String("lorem")).unwrap()
        );
        assert_eq!(Value::Int(1), Value::try_from(ArrowValue::Int(1)).unwrap());
        assert_eq!(
            Value::Float(1.5),
            Value::try_from(ArrowValue::Float(1.5)).unwrap()
        );
        assert_eq!(
            Value::Bool(true),
            Value::try_from(ArrowValue::Bool(true)).unwrap()
        );
        assert_eq!(
            Value::DateTime(DateTime::UNIX_EPOCH.naive_utc()),
            Value::try_from(ArrowValue::Timestamp(0, TimeUnit::Second)).unwrap()
        );
        assert_eq!(
            Value::DateTime(DateTime::UNIX_EPOCH.naive_utc()),
            Value::try_from(ArrowValue::Timestamp(0, TimeUnit::Millisecond)).unwrap()
        );
        assert_eq!(
            Value::DateTime(DateTime::UNIX_EPOCH.naive_utc()),
            Value::try_from(ArrowValue::Timestamp(0, TimeUnit::Microsecond)).unwrap()
        );
        assert_eq!(
            Value::DateTime(DateTime::UNIX_EPOCH.naive_utc()),
            Value::try_from(ArrowValue::Timestamp(0, TimeUnit::Nanosecond)).unwrap()
        );
        assert_eq!(
            Value::Duration(TimeDelta::seconds(1)),
            Value::try_from(ArrowValue::Duration(1, TimeUnit::Second)).unwrap()
        );
        assert_eq!(
            Value::Duration(TimeDelta::milliseconds(1)),
            Value::try_from(ArrowValue::Duration(1, TimeUnit::Millisecond)).unwrap()
        );
        assert_eq!(
            Value::Duration(TimeDelta::microseconds(1)),
            Value::try_from(ArrowValue::Duration(1, TimeUnit::Microsecond)).unwrap()
        );
        assert_eq!(
            Value::Duration(TimeDelta::nanoseconds(1)),
            Value::try_from(ArrowValue::Duration(1, TimeUnit::Nanosecond)).unwrap()
        );
        assert_eq!(Value::Null, Value::try_from(ArrowValue::Null).unwrap());
    }

    #[test]
    fn test_from_array() {
        let list_values =
            ListArray::from_iter_primitive::<arrow::datatypes::Int64Type, _, _>(vec![Some(vec![
                Some(1),
                Some(2),
            ])]);

        assert!(matches!(
            ArrowValue::from_array(&list_values, 0),
            ArrowValue::Unsupported(_)
        ));
    }

    #[test]
    fn test_invalid_try_from() {
        assert!(
            Value::try_from(ArrowValue::Unsupported("1".to_string())).is_err_and(|error| matches!(
                error,
                GraphRecordError::Conversion(conversion_error)
                    if conversion_error == ConversionError::UnsupportedFrameValue {
                        value: "1".to_string()
                    }
            ))
        );
        assert!(
            Value::try_from(ArrowValue::Timestamp(i64::MAX, TimeUnit::Second)).is_err_and(
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
            Value::try_from(ArrowValue::Timestamp(i64::MAX, TimeUnit::Millisecond)).is_err_and(
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
            Value::try_from(ArrowValue::Timestamp(i64::MAX, TimeUnit::Microsecond)).is_err_and(
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
        let batch = (create_node_record_batches(), "index")
            .collect_nodes()
            .unwrap();

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
            (create_node_record_batches(), "dolor")
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
            (create_node_record_batches(), "sed")
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
        let batch = (create_edge_record_batches(), "source", "target")
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
            (create_edge_record_batches(), "dolor", "target")
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
            (create_edge_record_batches(), "source", "dolor")
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
            (create_edge_record_batches(), "sed", "target")
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
            ArrowValue::String("lorem"),
            ArrowValue::from(ValueView::String(Cow::Borrowed("lorem")))
        );
        assert_eq!(
            ArrowValue::StringOwned("lorem".to_string()),
            ArrowValue::from(ValueView::String(Cow::Owned("lorem".to_string())))
        );
        assert_eq!(ArrowValue::Int(1), ArrowValue::from(ValueView::Int(1)));
        assert_eq!(
            ArrowValue::Float(1.5),
            ArrowValue::from(ValueView::Float(1.5))
        );
        assert_eq!(
            ArrowValue::Bool(true),
            ArrowValue::from(ValueView::Bool(true))
        );
        assert_eq!(
            ArrowValue::Timestamp(0, TimeUnit::Millisecond),
            ArrowValue::from(ValueView::DateTime(DateTime::UNIX_EPOCH.naive_utc()))
        );
        assert_eq!(
            ArrowValue::Duration(5000, TimeUnit::Millisecond),
            ArrowValue::from(ValueView::Duration(TimeDelta::seconds(5)))
        );
        assert_eq!(ArrowValue::Null, ArrowValue::from(ValueView::Null));
    }

    #[test]
    fn test_write() {
        let graphrecord = create_export_graphrecord();

        let export = graphrecord.export(ArrowTables).unwrap();

        let ungrouped_nodes = &export.ungrouped.nodes;

        assert_eq!(2, ungrouped_nodes.num_rows());
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
            ArrowValue::String("lorem"),
            ArrowValue::from_array(ungrouped_nodes.column_by_name("node_index").unwrap(), 0)
        );
        assert_eq!(
            ArrowValue::String("ipsum"),
            ArrowValue::from_array(ungrouped_nodes.column_by_name("node_index").unwrap(), 1)
        );
        assert_eq!(
            &ArrowDataType::Int64,
            ungrouped_nodes.column_by_name("count").unwrap().data_type()
        );
        assert_eq!(
            ArrowValue::Int(1),
            ArrowValue::from_array(ungrouped_nodes.column_by_name("count").unwrap(), 0)
        );
        assert_eq!(
            ArrowValue::Int(2),
            ArrowValue::from_array(ungrouped_nodes.column_by_name("count").unwrap(), 1)
        );
        assert_eq!(
            &ArrowDataType::Boolean,
            ungrouped_nodes
                .column_by_name("active")
                .unwrap()
                .data_type()
        );
        assert_eq!(
            ArrowValue::Bool(true),
            ArrowValue::from_array(ungrouped_nodes.column_by_name("active").unwrap(), 0)
        );
        assert_eq!(
            ArrowValue::Null,
            ArrowValue::from_array(ungrouped_nodes.column_by_name("active").unwrap(), 1)
        );
        assert_eq!(
            &ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
            ungrouped_nodes
                .column_by_name("created")
                .unwrap()
                .data_type()
        );
        assert_eq!(
            ArrowValue::Timestamp(0, TimeUnit::Millisecond),
            ArrowValue::from_array(ungrouped_nodes.column_by_name("created").unwrap(), 0)
        );
        assert_eq!(
            &ArrowDataType::Utf8,
            ungrouped_nodes.column_by_name("label").unwrap().data_type()
        );
        assert_eq!(
            ArrowValue::String("ipsum dolor"),
            ArrowValue::from_array(ungrouped_nodes.column_by_name("label").unwrap(), 0)
        );
        assert_eq!(
            &ArrowDataType::Float64,
            ungrouped_nodes.column_by_name("score").unwrap().data_type()
        );
        assert_eq!(
            ArrowValue::Float(1.5),
            ArrowValue::from_array(ungrouped_nodes.column_by_name("score").unwrap(), 0)
        );
        assert_eq!(
            &ArrowDataType::Duration(TimeUnit::Millisecond),
            ungrouped_nodes.column_by_name("span").unwrap().data_type()
        );
        assert_eq!(
            ArrowValue::Duration(5000, TimeUnit::Millisecond),
            ArrowValue::from_array(ungrouped_nodes.column_by_name("span").unwrap(), 0)
        );

        let ungrouped_edges = &export.ungrouped.edges;

        assert_eq!(1, ungrouped_edges.num_rows());
        assert_eq!(
            vec!["source_node_index", "target_node_index", "weight"],
            column_names(ungrouped_edges)
        );
        assert_eq!(
            ArrowValue::String("lorem"),
            ArrowValue::from_array(
                ungrouped_edges.column_by_name("source_node_index").unwrap(),
                0
            )
        );
        assert_eq!(
            ArrowValue::String("ipsum"),
            ArrowValue::from_array(
                ungrouped_edges.column_by_name("target_node_index").unwrap(),
                0
            )
        );
        assert_eq!(
            ArrowValue::Float(2.5),
            ArrowValue::from_array(ungrouped_edges.column_by_name("weight").unwrap(), 0)
        );

        assert_eq!(3, export.groups.len());

        let dolor_tables = &export.groups[&"dolor".into()];

        assert_eq!(1, dolor_tables.nodes.num_rows());
        assert_eq!(
            vec!["node_index", "count"],
            column_names(&dolor_tables.nodes)
        );
        assert_eq!(
            ArrowValue::String("sit"),
            ArrowValue::from_array(dolor_tables.nodes.column_by_name("node_index").unwrap(), 0)
        );
        assert_eq!(1, dolor_tables.edges.num_rows());
        assert_eq!(
            vec!["source_node_index", "target_node_index", "weight"],
            column_names(&dolor_tables.edges)
        );
        assert_eq!(
            ArrowValue::Int(1),
            ArrowValue::from_array(dolor_tables.edges.column_by_name("weight").unwrap(), 0)
        );

        let elit_tables = &export.groups[&"elit".into()];

        assert_eq!(
            &ArrowDataType::Utf8,
            elit_tables
                .nodes
                .column_by_name("count")
                .unwrap()
                .data_type()
        );
        assert_eq!(
            ArrowValue::String("tres"),
            ArrowValue::from_array(elit_tables.nodes.column_by_name("count").unwrap(), 0)
        );

        let amet_tables = &export.groups[&"amet".into()];

        assert_eq!(0, amet_tables.nodes.num_rows());
        assert_eq!(vec!["node_index"], column_names(&amet_tables.nodes));
        assert_eq!(0, amet_tables.edges.num_rows());
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
                .export(ArrowTables)
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
                .export(ArrowTables)
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
                .export(ArrowTables)
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
                .export(ArrowTables)
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
                .export(ArrowTables)
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
                .export(ArrowTables)
                .is_err_and(|error| matches!(
                    error,
                    GraphRecordError::Conversion(conversion_error)
                        if conversion_error == ConversionError::EdgeDataFrameCreation { group_index: None }
                ))
        );
    }

    #[test]
    fn test_to_arrow() {
        let export = create_export_graphrecord().to_arrow().unwrap();

        assert_eq!(2, export.ungrouped.nodes.num_rows());
        assert_eq!(1, export.ungrouped.edges.num_rows());
        assert_eq!(3, export.groups.len());
    }
}
