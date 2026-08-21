use crate::{
    AttributeOverview, AttributeOverviewData, OverviewResult,
    tabled_modifiers::MergeDuplicatesVerticalByColumn,
};
use graphrecords_core::{
    GraphRecord,
    errors::GraphRecordError,
    graphrecord::ValueView,
    prelude::{
        AttributeName, AttributeType, DataType, GroupIndex, GroupIndexView, GroupSchema, Value,
    },
};
use graphrecords_query::{
    Attribute, Count, DiscardIndex, EdgesSeries, EqualTo, Filter, GroupBy, Index, IsIn, IsNull,
    Maximum, Mean, MemberEdges, Minimum, Nodes, NodesSeries, OnErrorOf, OnKeyError, QueryResult,
    Queryable, ReturnPartition, Unique, UniqueCount,
    error::structure::MissingAttribute,
    expressions::groups,
    operations::policy::{Drop, Raise},
};
use graphrecords_utils::aliases::GrHashMap;
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use std::fmt::{Display, Formatter};
use tabled::{
    builder::Builder,
    settings::{Alignment, Panel, Style, Width, object::Columns, themes::BorderCorrection},
};

#[derive(Debug, Clone)]
pub struct NodeGroupOverview {
    pub count: usize,
    pub attributes: GrHashMap<AttributeName, AttributeOverview>,

    truncate_details: Option<usize>,
}

impl Display for NodeGroupOverview {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut builder = Builder::new();

        builder.push_record([
            "Node Count",
            "Attribute",
            "Attribute Type",
            "Data Type",
            "Details",
        ]);

        for (attribute, overview) in &self.attributes {
            let details = overview.data.details();

            builder.push_record([
                &self.count.to_string(),
                &attribute.to_string(),
                overview.data.attribute_type_name(),
                &overview.data_type.to_string(),
                &details,
            ]);
        }

        if self.attributes.is_empty() && self.count > 0 {
            builder.push_record([&self.count.to_string(), "-", "-", "-", "-"]);
        }

        let mut table = builder.build();
        table.with(Style::modern());
        table.with(Panel::header("Node Overview"));
        table.with(MergeDuplicatesVerticalByColumn::new(vec![0]));
        table.with(Alignment::center_vertical());
        table.with(BorderCorrection {});

        if let Some(truncate_details) = self.truncate_details {
            table.modify(Columns::last(), Width::truncate(truncate_details));
        }

        writeln!(f, "{table}")
    }
}

impl NodeGroupOverview {
    fn new(
        members: &NodesSeries,
        group_schema: &GroupSchema,
        truncate_details: Option<usize>,
    ) -> OverviewResult<Self> {
        let count_series = members.count();
        let count = match Value::from(count_series.evaluate()??) {
            Value::Int(count) => usize::try_from(count).expect("Count must be non-negative."),
            _ => unreachable!("Count must be an integer."),
        };

        let attributes = group_schema
            .nodes()
            .par_iter()
            .map(|(key, attribute_data_type)| {
                let data_type = attribute_data_type.data_type().clone();
                let values = members
                    .attribute(key.clone())
                    .on_error_of::<MissingAttribute>(Drop);

                let data = match attribute_data_type.attribute_type() {
                    AttributeType::Categorical => {
                        let distinct = values.discard_index().unique();
                        let distinct_values = distinct
                            .evaluate()?
                            .map(|value| value.map(Value::from))
                            .collect::<QueryResult<_>>()?;

                        AttributeOverviewData::categorical(distinct_values)
                    }
                    AttributeType::Continuous => {
                        let non_null = values.filter(!values.is_null());
                        let minimum_series = non_null.min();
                        let mean_series = non_null.mean();
                        let maximum_series = non_null.max();
                        let minimum = minimum_series.evaluate()?;
                        let mean = mean_series.evaluate()?;
                        let maximum = maximum_series.evaluate()?;

                        AttributeOverviewData::continuous(
                            minimum.transpose()?.map(Value::from),
                            mean.transpose()?.map(Value::from),
                            maximum.transpose()?.map(Value::from),
                        )
                    }
                    AttributeType::Temporal => {
                        let non_null = values.filter(!values.is_null());
                        let minimum_series = non_null.min();
                        let maximum_series = non_null.max();
                        let minimum = minimum_series.evaluate()?;
                        let maximum = maximum_series.evaluate()?;

                        AttributeOverviewData::temporal(
                            minimum.transpose()?.map(Value::from),
                            maximum.transpose()?.map(Value::from),
                        )
                    }
                    AttributeType::Unstructured => {
                        let distinct_count = values.n_unique();
                        let count_value = distinct_count.evaluate()??;

                        AttributeOverviewData::unstructured(Some(Value::from(count_value)))
                    }
                };

                Ok((key.clone(), AttributeOverview { data_type, data }))
            })
            .collect::<OverviewResult<_>>()?;

        Ok(Self {
            count,
            attributes,
            truncate_details,
        })
    }

    pub(crate) fn for_groups(
        graphrecord: &GraphRecord,
        live_groups: &[GroupIndex],
        truncate_details: Option<usize>,
    ) -> OverviewResult<Vec<Self>> {
        let groups = graphrecord.groups();
        let partitioned_members = groups.group_by(groups.index()).on_key_error(Raise).nodes();
        let partition = partitioned_members.evaluate()?;
        let mut counts: GrHashMap<_, _> = partition
            .into_parts()
            .0
            .into_iter()
            .map(|bucket| {
                let (group_index, _, payload) = bucket.into_parts();
                let count = payload?.try_fold(0, |count, member| member.map(|_| count + 1))?;

                Ok((group_index, count))
            })
            .collect::<OverviewResult<_>>()?;

        let schema = graphrecord.schema();
        let mut categorical: GrHashMap<_, GrHashMap<_, _>> = GrHashMap::default();
        let mut continuous = GrHashMap::default();
        let mut temporal = GrHashMap::default();
        let mut unstructured = GrHashMap::default();

        for group_index in live_groups {
            let group_schema = schema.group(group_index).map_err(GraphRecordError::from)?;

            for (key, attribute_data_type) in group_schema.nodes().iter() {
                let arm = match attribute_data_type.attribute_type() {
                    AttributeType::Categorical => &mut categorical,
                    AttributeType::Continuous => &mut continuous,
                    AttributeType::Temporal => &mut temporal,
                    AttributeType::Unstructured => &mut unstructured,
                };

                arm.entry(key.clone())
                    .or_default()
                    .insert(group_index.clone(), attribute_data_type.data_type().clone());
            }
        }

        let requests: Vec<_> = [
            (AttributeType::Categorical, categorical),
            (AttributeType::Continuous, continuous),
            (AttributeType::Temporal, temporal),
            (AttributeType::Unstructured, unstructured),
        ]
        .into_iter()
        .flat_map(|(attribute_type, arm)| {
            arm.into_iter()
                .map(move |(key, receivers)| (key, attribute_type, receivers))
        })
        .collect();

        let cell_results: Vec<_> = requests
            .into_par_iter()
            .map(|(key, attribute_type, receivers)| {
                let cells = Self::attribute_cells(graphrecord, &key, attribute_type, receivers)?;

                Ok((key, cells))
            })
            .collect::<OverviewResult<_>>()?;

        let mut attributes: GrHashMap<_, GrHashMap<_, _>> = GrHashMap::default();
        for (key, cells) in cell_results {
            for (group_index, overview) in cells {
                attributes
                    .entry(group_index)
                    .or_default()
                    .insert(key.clone(), overview);
            }
        }

        Ok(live_groups
            .iter()
            .map(|group_index| Self {
                count: counts.remove(group_index).unwrap_or(0),
                attributes: attributes.remove(group_index).unwrap_or_default(),
                truncate_details,
            })
            .collect())
    }

    fn attribute_cells(
        graphrecord: &GraphRecord,
        key: &AttributeName,
        attribute_type: AttributeType,
        receivers: GrHashMap<GroupIndex, DataType>,
    ) -> OverviewResult<Vec<(GroupIndex, AttributeOverview)>> {
        let scoped_groups: Vec<_> = receivers.keys().cloned().collect();
        let groups = graphrecord.groups();
        let scoped = groups.filter(groups.index().is_in(scoped_groups));
        let members = scoped.group_by(scoped.index()).on_key_error(Raise).nodes();
        let values = members
            .attribute(key.clone())
            .on_error_of::<MissingAttribute>(Drop);

        match attribute_type {
            AttributeType::Categorical => {
                let distinct = values.discard_index().unique();
                let partition = distinct.evaluate()?;
                let mut distinct_values: GrHashMap<_, _> = partition
                    .into_parts()
                    .0
                    .into_iter()
                    .map(|bucket| {
                        let (group_index, _, payload) = bucket.into_parts();
                        let values = payload?
                            .map(|value| value.map(Value::from))
                            .collect::<QueryResult<_>>()?;

                        Ok((group_index, values))
                    })
                    .collect::<OverviewResult<_>>()?;

                Ok(receivers
                    .into_iter()
                    .map(|(group_index, data_type)| {
                        let data = AttributeOverviewData::categorical(
                            distinct_values.remove(&group_index).unwrap_or_default(),
                        );

                        (group_index, AttributeOverview { data_type, data })
                    })
                    .collect())
            }
            AttributeType::Continuous => {
                let all_values = graphrecord
                    .nodes()
                    .attribute(key.clone())
                    .on_error_of::<MissingAttribute>(Drop);
                let non_null = values.filter(!all_values.is_null());
                let minimum_series = non_null.min();
                let mean_series = non_null.mean();
                let maximum_series = non_null.max();
                let minimum = minimum_series.evaluate()?;
                let mean = mean_series.evaluate()?;
                let maximum = maximum_series.evaluate()?;
                let mut minimums = Self::aggregate_values(minimum)?;
                let mut means = Self::aggregate_values(mean)?;
                let mut maximums = Self::aggregate_values(maximum)?;

                Ok(receivers
                    .into_iter()
                    .map(|(group_index, data_type)| {
                        let data = AttributeOverviewData::continuous(
                            minimums.remove(&group_index),
                            means.remove(&group_index),
                            maximums.remove(&group_index),
                        );

                        (group_index, AttributeOverview { data_type, data })
                    })
                    .collect())
            }
            AttributeType::Temporal => {
                let all_values = graphrecord
                    .nodes()
                    .attribute(key.clone())
                    .on_error_of::<MissingAttribute>(Drop);
                let non_null = values.filter(!all_values.is_null());
                let minimum_series = non_null.min();
                let maximum_series = non_null.max();
                let minimum = minimum_series.evaluate()?;
                let maximum = maximum_series.evaluate()?;
                let mut minimums = Self::aggregate_values(minimum)?;
                let mut maximums = Self::aggregate_values(maximum)?;

                Ok(receivers
                    .into_iter()
                    .map(|(group_index, data_type)| {
                        let data = AttributeOverviewData::temporal(
                            minimums.remove(&group_index),
                            maximums.remove(&group_index),
                        );

                        (group_index, AttributeOverview { data_type, data })
                    })
                    .collect())
            }
            AttributeType::Unstructured => {
                let distinct_counts = values.n_unique();
                let partition = distinct_counts.evaluate()?;
                let mut counts: GrHashMap<_, _> = partition
                    .into_parts()
                    .0
                    .into_iter()
                    .map(|bucket| {
                        let (group_index, _, payload) = bucket.into_parts();
                        let count = payload??;

                        Ok((group_index, Value::from(count)))
                    })
                    .collect::<OverviewResult<_>>()?;

                Ok(receivers
                    .into_iter()
                    .map(|(group_index, data_type)| {
                        let data = AttributeOverviewData::unstructured(counts.remove(&group_index));

                        (group_index, AttributeOverview { data_type, data })
                    })
                    .collect())
            }
        }
    }

    fn aggregate_values(
        partition: ReturnPartition<'_, GroupIndex, GroupIndex, Option<QueryResult<ValueView<'_>>>>,
    ) -> OverviewResult<GrHashMap<GroupIndex, Value>> {
        partition
            .into_parts()
            .0
            .into_iter()
            .filter_map(|bucket| {
                let (group_index, _, payload) = bucket.into_parts();

                match payload.and_then(Option::transpose) {
                    Ok(Some(value)) => Some(Ok((group_index, Value::from(value)))),
                    Ok(None) => None,
                    Err(failure) => Some(Err(failure.into())),
                }
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct EdgeGroupOverview {
    pub count: usize,
    pub attributes: GrHashMap<AttributeName, AttributeOverview>,

    truncate_details: Option<usize>,
}

impl Display for EdgeGroupOverview {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut builder = Builder::new();

        builder.push_record([
            "Edge Count",
            "Attribute",
            "Attribute Type",
            "Data Type",
            "Details",
        ]);

        for (attribute, overview) in &self.attributes {
            let details = overview.data.details();

            builder.push_record([
                &self.count.to_string(),
                &attribute.to_string(),
                overview.data.attribute_type_name(),
                &overview.data_type.to_string(),
                &details,
            ]);
        }

        let mut table = builder.build();
        table.with(Style::modern());
        table.with(Panel::header("Edge Overview"));
        table.with(MergeDuplicatesVerticalByColumn::new(vec![0]));
        table.with(Alignment::center_vertical());
        table.with(BorderCorrection {});

        if let Some(truncate_details) = self.truncate_details {
            table.modify(Columns::last(), Width::truncate(truncate_details));
        }

        writeln!(f, "{table}")
    }
}

impl EdgeGroupOverview {
    fn new(
        members: &EdgesSeries,
        group_schema: &GroupSchema,
        truncate_details: Option<usize>,
    ) -> OverviewResult<Self> {
        let count_series = members.count();
        let count = match Value::from(count_series.evaluate()??) {
            Value::Int(count) => usize::try_from(count).expect("Count must be non-negative."),
            _ => unreachable!("Count must be an integer."),
        };

        let attributes = group_schema
            .edges()
            .par_iter()
            .map(|(key, attribute_data_type)| {
                let data_type = attribute_data_type.data_type().clone();
                let values = members
                    .attribute(key.clone())
                    .on_error_of::<MissingAttribute>(Drop);

                let data = match attribute_data_type.attribute_type() {
                    AttributeType::Categorical => {
                        let distinct = values.discard_index().unique();
                        let distinct_values = distinct
                            .evaluate()?
                            .map(|value| value.map(Value::from))
                            .collect::<QueryResult<_>>()?;

                        AttributeOverviewData::categorical(distinct_values)
                    }
                    AttributeType::Continuous => {
                        let non_null = values.filter(!values.is_null());
                        let minimum_series = non_null.min();
                        let mean_series = non_null.mean();
                        let maximum_series = non_null.max();
                        let minimum = minimum_series.evaluate()?;
                        let mean = mean_series.evaluate()?;
                        let maximum = maximum_series.evaluate()?;

                        AttributeOverviewData::continuous(
                            minimum.transpose()?.map(Value::from),
                            mean.transpose()?.map(Value::from),
                            maximum.transpose()?.map(Value::from),
                        )
                    }
                    AttributeType::Temporal => {
                        let non_null = values.filter(!values.is_null());
                        let minimum_series = non_null.min();
                        let maximum_series = non_null.max();
                        let minimum = minimum_series.evaluate()?;
                        let maximum = maximum_series.evaluate()?;

                        AttributeOverviewData::temporal(
                            minimum.transpose()?.map(Value::from),
                            maximum.transpose()?.map(Value::from),
                        )
                    }
                    AttributeType::Unstructured => {
                        let distinct_count = values.n_unique();
                        let count_value = distinct_count.evaluate()??;

                        AttributeOverviewData::unstructured(Some(Value::from(count_value)))
                    }
                };

                Ok((key.clone(), AttributeOverview { data_type, data }))
            })
            .collect::<OverviewResult<_>>()?;

        Ok(Self {
            count,
            attributes,
            truncate_details,
        })
    }

    pub(crate) fn for_groups(
        graphrecord: &GraphRecord,
        live_groups: &[GroupIndex],
        truncate_details: Option<usize>,
    ) -> OverviewResult<Vec<Self>> {
        let groups = graphrecord.groups();
        let partitioned_members = groups.group_by(groups.index()).on_key_error(Raise).edges();
        let partition = partitioned_members.evaluate()?;
        let mut counts: GrHashMap<_, _> = partition
            .into_parts()
            .0
            .into_iter()
            .map(|bucket| {
                let (group_index, _, payload) = bucket.into_parts();
                let count = payload?.try_fold(0, |count, member| member.map(|_| count + 1))?;

                Ok((group_index, count))
            })
            .collect::<OverviewResult<_>>()?;

        let schema = graphrecord.schema();
        let mut categorical: GrHashMap<_, GrHashMap<_, _>> = GrHashMap::default();
        let mut continuous = GrHashMap::default();
        let mut temporal = GrHashMap::default();
        let mut unstructured = GrHashMap::default();

        for group_index in live_groups {
            let group_schema = schema.group(group_index).map_err(GraphRecordError::from)?;

            for (key, attribute_data_type) in group_schema.edges().iter() {
                let arm = match attribute_data_type.attribute_type() {
                    AttributeType::Categorical => &mut categorical,
                    AttributeType::Continuous => &mut continuous,
                    AttributeType::Temporal => &mut temporal,
                    AttributeType::Unstructured => &mut unstructured,
                };

                arm.entry(key.clone())
                    .or_default()
                    .insert(group_index.clone(), attribute_data_type.data_type().clone());
            }
        }

        let requests: Vec<_> = [
            (AttributeType::Categorical, categorical),
            (AttributeType::Continuous, continuous),
            (AttributeType::Temporal, temporal),
            (AttributeType::Unstructured, unstructured),
        ]
        .into_iter()
        .flat_map(|(attribute_type, arm)| {
            arm.into_iter()
                .map(move |(key, receivers)| (key, attribute_type, receivers))
        })
        .collect();

        let cell_results: Vec<_> = requests
            .into_par_iter()
            .map(|(key, attribute_type, receivers)| {
                let cells = Self::attribute_cells(graphrecord, &key, attribute_type, receivers)?;

                Ok((key, cells))
            })
            .collect::<OverviewResult<_>>()?;

        let mut attributes: GrHashMap<_, GrHashMap<_, _>> = GrHashMap::default();
        for (key, cells) in cell_results {
            for (group_index, overview) in cells {
                attributes
                    .entry(group_index)
                    .or_default()
                    .insert(key.clone(), overview);
            }
        }

        Ok(live_groups
            .iter()
            .map(|group_index| Self {
                count: counts.remove(group_index).unwrap_or(0),
                attributes: attributes.remove(group_index).unwrap_or_default(),
                truncate_details,
            })
            .collect())
    }

    fn attribute_cells(
        graphrecord: &GraphRecord,
        key: &AttributeName,
        attribute_type: AttributeType,
        receivers: GrHashMap<GroupIndex, DataType>,
    ) -> OverviewResult<Vec<(GroupIndex, AttributeOverview)>> {
        let scoped_groups: Vec<_> = receivers.keys().cloned().collect();
        let groups = graphrecord.groups();
        let scoped = groups.filter(groups.index().is_in(scoped_groups));
        let members = scoped.group_by(scoped.index()).on_key_error(Raise).edges();
        let values = members
            .attribute(key.clone())
            .on_error_of::<MissingAttribute>(Drop);

        match attribute_type {
            AttributeType::Categorical => {
                let distinct = values.discard_index().unique();
                let partition = distinct.evaluate()?;
                let mut distinct_values: GrHashMap<_, _> = partition
                    .into_parts()
                    .0
                    .into_iter()
                    .map(|bucket| {
                        let (group_index, _, payload) = bucket.into_parts();
                        let values = payload?
                            .map(|value| value.map(Value::from))
                            .collect::<QueryResult<_>>()?;

                        Ok((group_index, values))
                    })
                    .collect::<OverviewResult<_>>()?;

                Ok(receivers
                    .into_iter()
                    .map(|(group_index, data_type)| {
                        let data = AttributeOverviewData::categorical(
                            distinct_values.remove(&group_index).unwrap_or_default(),
                        );

                        (group_index, AttributeOverview { data_type, data })
                    })
                    .collect())
            }
            AttributeType::Continuous => {
                let all_values = graphrecord
                    .edges()
                    .attribute(key.clone())
                    .on_error_of::<MissingAttribute>(Drop);
                let non_null = values.filter(!all_values.is_null());
                let minimum_series = non_null.min();
                let mean_series = non_null.mean();
                let maximum_series = non_null.max();
                let minimum = minimum_series.evaluate()?;
                let mean = mean_series.evaluate()?;
                let maximum = maximum_series.evaluate()?;
                let mut minimums = Self::aggregate_values(minimum)?;
                let mut means = Self::aggregate_values(mean)?;
                let mut maximums = Self::aggregate_values(maximum)?;

                Ok(receivers
                    .into_iter()
                    .map(|(group_index, data_type)| {
                        let data = AttributeOverviewData::continuous(
                            minimums.remove(&group_index),
                            means.remove(&group_index),
                            maximums.remove(&group_index),
                        );

                        (group_index, AttributeOverview { data_type, data })
                    })
                    .collect())
            }
            AttributeType::Temporal => {
                let all_values = graphrecord
                    .edges()
                    .attribute(key.clone())
                    .on_error_of::<MissingAttribute>(Drop);
                let non_null = values.filter(!all_values.is_null());
                let minimum_series = non_null.min();
                let maximum_series = non_null.max();
                let minimum = minimum_series.evaluate()?;
                let maximum = maximum_series.evaluate()?;
                let mut minimums = Self::aggregate_values(minimum)?;
                let mut maximums = Self::aggregate_values(maximum)?;

                Ok(receivers
                    .into_iter()
                    .map(|(group_index, data_type)| {
                        let data = AttributeOverviewData::temporal(
                            minimums.remove(&group_index),
                            maximums.remove(&group_index),
                        );

                        (group_index, AttributeOverview { data_type, data })
                    })
                    .collect())
            }
            AttributeType::Unstructured => {
                let distinct_counts = values.n_unique();
                let partition = distinct_counts.evaluate()?;
                let mut counts: GrHashMap<_, _> = partition
                    .into_parts()
                    .0
                    .into_iter()
                    .map(|bucket| {
                        let (group_index, _, payload) = bucket.into_parts();
                        let count = payload??;

                        Ok((group_index, Value::from(count)))
                    })
                    .collect::<OverviewResult<_>>()?;

                Ok(receivers
                    .into_iter()
                    .map(|(group_index, data_type)| {
                        let data = AttributeOverviewData::unstructured(counts.remove(&group_index));

                        (group_index, AttributeOverview { data_type, data })
                    })
                    .collect())
            }
        }
    }

    fn aggregate_values(
        partition: ReturnPartition<'_, GroupIndex, GroupIndex, Option<QueryResult<ValueView<'_>>>>,
    ) -> OverviewResult<GrHashMap<GroupIndex, Value>> {
        partition
            .into_parts()
            .0
            .into_iter()
            .filter_map(|bucket| {
                let (group_index, _, payload) = bucket.into_parts();

                match payload.and_then(Option::transpose) {
                    Ok(Some(value)) => Some(Ok((group_index, Value::from(value)))),
                    Ok(None) => None,
                    Err(failure) => Some(Err(failure.into())),
                }
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct GroupOverview {
    pub node_overview: NodeGroupOverview,
    pub edge_overview: EdgeGroupOverview,
}

impl Display for GroupOverview {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{}", self.node_overview)?;
        writeln!(f, "{}", self.edge_overview)
    }
}

impl GroupOverview {
    pub(crate) fn new(
        graphrecord: &GraphRecord,
        group_index: Option<&GroupIndex>,
        truncate_details: Option<usize>,
    ) -> OverviewResult<Self> {
        let schema = graphrecord.schema();

        let group_schema = match group_index {
            Some(group_index) => {
                if !graphrecord.contains_group(group_index) {
                    return Err(GraphRecordError::GroupNotFound {
                        group_index: group_index.clone(),
                    }
                    .into());
                }

                schema.group(group_index).map_err(GraphRecordError::from)?
            }
            None => schema.ungrouped(),
        };

        let (node_members, edge_members) = if let Some(group_index) = group_index {
            let groups = graphrecord.groups();
            let singled = groups.filter(groups.index().equal_to(group_index.clone()));

            (singled.nodes(), singled.edges())
        } else {
            let nodes = graphrecord.nodes();
            let edges = graphrecord.edges();

            (
                nodes.filter(!nodes.index().is_in(groups().nodes().index())),
                edges.filter(!edges.index().is_in(groups().edges().index())),
            )
        };

        Ok(Self {
            node_overview: NodeGroupOverview::new(&node_members, group_schema, truncate_details)?,
            edge_overview: EdgeGroupOverview::new(&edge_members, group_schema, truncate_details)?,
        })
    }
}

pub trait GroupOverviewable {
    fn group_overview<'a>(
        &self,
        group_index: impl Into<GroupIndexView<'a>>,
        truncate_details: Option<usize>,
    ) -> OverviewResult<GroupOverview>;
}

impl GroupOverviewable for GraphRecord {
    fn group_overview<'a>(
        &self,
        group_index: impl Into<GroupIndexView<'a>>,
        truncate_details: Option<usize>,
    ) -> OverviewResult<GroupOverview> {
        let group_index = GroupIndex::from(group_index.into());

        GroupOverview::new(self, Some(&group_index), truncate_details)
    }
}
