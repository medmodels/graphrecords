use crate::{
    GroupOverview, OverviewResult,
    group::{EdgeGroupOverview, NodeGroupOverview},
    tabled_modifiers::MergeDuplicatesVerticalByColumn,
};
use graphrecords_core::{GraphRecord, prelude::GroupIndex};
use graphrecords_query::{QueryResult, Queryable};
use graphrecords_utils::aliases::GrHashMap;
use std::fmt::{Display, Formatter};
use tabled::{
    builder::Builder,
    settings::{Alignment, Panel, Style, Width, object::Columns, themes::BorderCorrection},
};

#[derive(Debug, Clone)]
pub struct Overview {
    pub ungrouped_overview: GroupOverview,
    pub grouped_overviews: GrHashMap<GroupIndex, GroupOverview>,

    truncate_details: Option<usize>,
}

impl Display for Overview {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut builder = Builder::new();

        builder.push_record([
            "Group",
            "Node Count",
            "Attribute",
            "Attribute Type",
            "Data Type",
            "Details",
        ]);

        for (group_index, group_overview) in std::iter::once((None, &self.ungrouped_overview))
            .chain(
                self.grouped_overviews
                    .iter()
                    .map(|(group_index, overview)| (Some(group_index), overview)),
            )
        {
            let group_label = group_index
                .map_or_else(|| "Ungrouped".to_string(), std::string::ToString::to_string);
            let count = group_overview.node_overview.count;

            for (attribute, overview) in &group_overview.node_overview.attributes {
                let details = overview.data.details();

                builder.push_record([
                    &group_label,
                    &count.to_string(),
                    &attribute.to_string(),
                    overview.data.attribute_type_name(),
                    &overview.data_type.to_string(),
                    &details,
                ]);
            }

            if group_overview.node_overview.attributes.is_empty() && count > 0 {
                builder.push_record([&group_label, &count.to_string(), "-", "-", "-", "-"]);
            }
        }

        let mut table = builder.build();
        table.with(Style::modern());
        table.with(Panel::header("Node Overview"));
        table.with(MergeDuplicatesVerticalByColumn::new(vec![0, 1]));
        table.with(Alignment::center_vertical());
        table.with(BorderCorrection {});

        if let Some(truncate_details) = self.truncate_details {
            table.modify(Columns::last(), Width::truncate(truncate_details));
        }

        writeln!(f, "{table}")?;

        let mut builder = Builder::new();

        builder.push_record([
            "Group",
            "Edge Count",
            "Attribute",
            "Attribute Type",
            "Data Type",
            "Details",
        ]);

        for (group_index, group_overview) in std::iter::once((None, &self.ungrouped_overview))
            .chain(
                self.grouped_overviews
                    .iter()
                    .map(|(group_index, overview)| (Some(group_index), overview)),
            )
        {
            let group_label = group_index
                .map_or_else(|| "Ungrouped".to_string(), std::string::ToString::to_string);
            let count = group_overview.edge_overview.count;

            for (attribute, overview) in &group_overview.edge_overview.attributes {
                let details = overview.data.details();

                builder.push_record([
                    &group_label,
                    &count.to_string(),
                    &attribute.to_string(),
                    overview.data.attribute_type_name(),
                    &overview.data_type.to_string(),
                    &details,
                ]);
            }

            if group_overview.edge_overview.attributes.is_empty() && count > 0 {
                builder.push_record([&group_label, &count.to_string(), "-", "-", "-", "-"]);
            }
        }

        let mut table = builder.build();
        table.with(Style::modern());
        table.with(Panel::header("Edge Overview"));
        table.with(MergeDuplicatesVerticalByColumn::new(vec![0, 1]));
        table.with(Alignment::center_vertical());
        table.with(BorderCorrection {});

        if let Some(truncate_details) = self.truncate_details {
            table.modify(Columns::last(), Width::truncate(truncate_details));
        }

        writeln!(f, "{table}")
    }
}

impl Overview {
    fn new(graphrecord: &GraphRecord, truncate_details: Option<usize>) -> OverviewResult<Self> {
        let groups = graphrecord.groups();
        let live_groups: Vec<_> = groups.evaluate()?.collect::<QueryResult<_>>()?;

        let node_overviews =
            NodeGroupOverview::for_groups(graphrecord, &live_groups, truncate_details)?;
        let edge_overviews =
            EdgeGroupOverview::for_groups(graphrecord, &live_groups, truncate_details)?;

        let grouped_overviews = live_groups
            .into_iter()
            .zip(node_overviews.into_iter().zip(edge_overviews))
            .map(|(group_index, (node_overview, edge_overview))| {
                (
                    group_index,
                    GroupOverview {
                        node_overview,
                        edge_overview,
                    },
                )
            })
            .collect();

        Ok(Self {
            ungrouped_overview: GroupOverview::new(graphrecord, None, truncate_details)?,
            grouped_overviews,
            truncate_details,
        })
    }
}

pub trait Overviewable {
    fn overview(&self, truncate_details: Option<usize>) -> OverviewResult<Overview>;
}

impl Overviewable for GraphRecord {
    fn overview(&self, truncate_details: Option<usize>) -> OverviewResult<Overview> {
        Overview::new(self, truncate_details)
    }
}
