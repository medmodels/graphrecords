use crate::{
    AttributeOverview, AttributeOverviewData, OverviewResult,
    tabled_modifiers::MergeDuplicatesVerticalByColumn,
};
use graphrecords_core::{
    GraphRecord,
    errors::GraphRecordError,
    prelude::{AttributeName, AttributeType, Group, GroupSchema, Value},
};
use graphrecords_query::{
    Attribute, Filter, Index, IsIn, IsNull, Maximum, Mean, Minimum, OnErrorOf, QueryEdges,
    QueryNodes, QueryResult, error::structure::MissingAttribute, operations::policy::Drop,
};
use graphrecords_utils::aliases::{GrHashMap, GrHashSet};
use itertools::Itertools;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
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
        graphrecord: &GraphRecord,
        group_schema: &GroupSchema,
        group: Option<&Group>,
        truncate_details: Option<usize>,
    ) -> OverviewResult<Self> {
        let nodes_in_group: GrHashSet<_> = match group {
            Some(group) => graphrecord.nodes_in_group(group)?.cloned().collect(),
            None => graphrecord.ungrouped_nodes().cloned().collect(),
        };
        let count = nodes_in_group.len();

        let attributes: GrHashMap<_, _> = group_schema
            .nodes()
            .par_iter()
            .map(|(key, attribute_data_type)| {
                let attribute_type = attribute_data_type.attribute_type();
                let data_type = attribute_data_type.data_type().clone();

                let attribute_overview = match attribute_type {
                    AttributeType::Categorical => {
                        let selection = graphrecord.query_nodes(|nodes| {
                            let nodes = nodes.filter(nodes.index().is_in(nodes_in_group.clone()));

                            nodes
                                .attribute(key.clone())
                                .on_error_of::<MissingAttribute>(Drop)
                        });

                        let values: Vec<_> = selection
                            .evaluate()?
                            .map(|(_, value)| value)
                            .collect::<QueryResult<_>>()?;

                        AttributeOverview {
                            data_type,
                            data: AttributeOverviewData::Categorical {
                                distinct_values: values
                                    .into_iter()
                                    .sorted_by(Value::total_cmp)
                                    .dedup()
                                    .collect(),
                            },
                        }
                    }
                    AttributeType::Continuous => {
                        let selection = graphrecord.query_nodes(|nodes| {
                            let nodes = nodes.filter(nodes.index().is_in(nodes_in_group.clone()));
                            let values = nodes
                                .attribute(key.clone())
                                .on_error_of::<MissingAttribute>(Drop);
                            let values = values.filter(!values.is_null());

                            (values.min(), values.mean(), values.max())
                        });

                        let (minimum, mean, maximum) = selection.evaluate()?;

                        AttributeOverview {
                            data_type,
                            data: AttributeOverviewData::Continuous {
                                min: minimum.transpose()?.unwrap_or(Value::Null),
                                mean: mean.transpose()?.unwrap_or(Value::Null),
                                max: maximum.transpose()?.unwrap_or(Value::Null),
                            },
                        }
                    }
                    AttributeType::Temporal => {
                        let selection = graphrecord.query_nodes(|nodes| {
                            let nodes = nodes.filter(nodes.index().is_in(nodes_in_group.clone()));
                            let values = nodes
                                .attribute(key.clone())
                                .on_error_of::<MissingAttribute>(Drop);
                            let values = values.filter(!values.is_null());

                            (values.min(), values.max())
                        });

                        let (minimum, maximum) = selection.evaluate()?;

                        AttributeOverview {
                            data_type,
                            data: AttributeOverviewData::Temporal {
                                min: minimum.transpose()?.unwrap_or(Value::Null),
                                max: maximum.transpose()?.unwrap_or(Value::Null),
                            },
                        }
                    }
                    AttributeType::Unstructured => {
                        let selection = graphrecord.query_nodes(|nodes| {
                            let nodes = nodes.filter(nodes.index().is_in(nodes_in_group.clone()));

                            nodes
                                .attribute(key.clone())
                                .on_error_of::<MissingAttribute>(Drop)
                        });

                        let values: Vec<_> = selection
                            .evaluate()?
                            .map(|(_, value)| value)
                            .collect::<QueryResult<_>>()?;

                        AttributeOverview {
                            data_type,
                            data: AttributeOverviewData::Unstructured {
                                distinct_count: values
                                    .into_iter()
                                    .sorted_by(Value::total_cmp)
                                    .dedup()
                                    .count(),
                            },
                        }
                    }
                };

                Ok((key.clone(), attribute_overview))
            })
            .collect::<QueryResult<_>>()?;

        Ok(Self {
            count,
            attributes,
            truncate_details,
        })
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
        graphrecord: &GraphRecord,
        group_schema: &GroupSchema,
        group: Option<&Group>,
        truncate_details: Option<usize>,
    ) -> OverviewResult<Self> {
        let edges_in_group: GrHashSet<_> = match group {
            Some(group) => graphrecord.edges_in_group(group)?.copied().collect(),
            None => graphrecord.ungrouped_edges().copied().collect(),
        };
        let count = edges_in_group.len();

        let attributes: GrHashMap<_, _> = group_schema
            .edges()
            .par_iter()
            .map(|(key, attribute_data_type)| {
                let attribute_type = attribute_data_type.attribute_type();
                let data_type = attribute_data_type.data_type().clone();

                let attribute_overview = match attribute_type {
                    AttributeType::Categorical => {
                        let selection = graphrecord.query_edges(|edges| {
                            let edges = edges.filter(edges.index().is_in(edges_in_group.clone()));

                            edges
                                .attribute(key.clone())
                                .on_error_of::<MissingAttribute>(Drop)
                        });

                        let values: Vec<_> = selection
                            .evaluate()?
                            .map(|(_, value)| value)
                            .collect::<QueryResult<_>>()?;

                        AttributeOverview {
                            data_type,
                            data: AttributeOverviewData::Categorical {
                                distinct_values: values
                                    .into_iter()
                                    .sorted_by(Value::total_cmp)
                                    .dedup()
                                    .collect(),
                            },
                        }
                    }
                    AttributeType::Continuous => {
                        let selection = graphrecord.query_edges(|edges| {
                            let edges = edges.filter(edges.index().is_in(edges_in_group.clone()));
                            let values = edges
                                .attribute(key.clone())
                                .on_error_of::<MissingAttribute>(Drop);
                            let values = values.filter(!values.is_null());

                            (values.min(), values.mean(), values.max())
                        });

                        let (minimum, mean, maximum) = selection.evaluate()?;

                        AttributeOverview {
                            data_type,
                            data: AttributeOverviewData::Continuous {
                                min: minimum.transpose()?.unwrap_or(Value::Null),
                                mean: mean.transpose()?.unwrap_or(Value::Null),
                                max: maximum.transpose()?.unwrap_or(Value::Null),
                            },
                        }
                    }
                    AttributeType::Temporal => {
                        let selection = graphrecord.query_edges(|edges| {
                            let edges = edges.filter(edges.index().is_in(edges_in_group.clone()));
                            let values = edges
                                .attribute(key.clone())
                                .on_error_of::<MissingAttribute>(Drop);
                            let values = values.filter(!values.is_null());

                            (values.min(), values.max())
                        });

                        let (minimum, maximum) = selection.evaluate()?;

                        AttributeOverview {
                            data_type,
                            data: AttributeOverviewData::Temporal {
                                min: minimum.transpose()?.unwrap_or(Value::Null),
                                max: maximum.transpose()?.unwrap_or(Value::Null),
                            },
                        }
                    }
                    AttributeType::Unstructured => {
                        let selection = graphrecord.query_edges(|edges| {
                            let edges = edges.filter(edges.index().is_in(edges_in_group.clone()));

                            edges
                                .attribute(key.clone())
                                .on_error_of::<MissingAttribute>(Drop)
                        });

                        let values: Vec<_> = selection
                            .evaluate()?
                            .map(|(_, value)| value)
                            .collect::<QueryResult<_>>()?;

                        AttributeOverview {
                            data_type,
                            data: AttributeOverviewData::Unstructured {
                                distinct_count: values
                                    .into_iter()
                                    .sorted_by(Value::total_cmp)
                                    .dedup()
                                    .count(),
                            },
                        }
                    }
                };

                Ok((key.clone(), attribute_overview))
            })
            .collect::<QueryResult<_>>()?;

        Ok(Self {
            count,
            attributes,
            truncate_details,
        })
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
        group: Option<&Group>,
        truncate_details: Option<usize>,
    ) -> OverviewResult<Self> {
        let schema = graphrecord.get_schema();

        let group_schema = match group {
            Some(group) => schema.group(group).map_err(GraphRecordError::from)?,
            None => schema.ungrouped(),
        };

        Ok(Self {
            node_overview: NodeGroupOverview::new(
                graphrecord,
                group_schema,
                group,
                truncate_details,
            )?,
            edge_overview: EdgeGroupOverview::new(
                graphrecord,
                group_schema,
                group,
                truncate_details,
            )?,
        })
    }
}

pub trait GroupOverviewable {
    fn group_overview(
        &self,
        group: &Group,
        truncate_details: Option<usize>,
    ) -> OverviewResult<GroupOverview>;
}

impl GroupOverviewable for GraphRecord {
    fn group_overview(
        &self,
        group: &Group,
        truncate_details: Option<usize>,
    ) -> OverviewResult<GroupOverview> {
        GroupOverview::new(self, Some(group), truncate_details)
    }
}
