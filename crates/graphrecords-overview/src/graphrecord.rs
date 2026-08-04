use crate::{GroupOverview, OverviewResult, tabled_modifiers::MergeDuplicatesVerticalByColumn};
use graphrecords_core::{GraphRecord, prelude::Group};
use graphrecords_utils::aliases::GrHashMap;
use std::fmt::{Display, Formatter};
use tabled::{
    builder::Builder,
    settings::{Alignment, Panel, Style, Width, object::Columns, themes::BorderCorrection},
};

#[derive(Debug, Clone)]
pub struct Overview {
    pub ungrouped_overview: GroupOverview,
    pub grouped_overviews: GrHashMap<Group, GroupOverview>,

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

        for (group, group_overview) in std::iter::once((None, &self.ungrouped_overview)).chain(
            self.grouped_overviews
                .iter()
                .map(|(group, overview)| (Some(group), overview)),
        ) {
            let group_name =
                group.map_or_else(|| "Ungrouped".to_string(), std::string::ToString::to_string);
            let count = group_overview.node_overview.count;

            for (attribute, overview) in &group_overview.node_overview.attributes {
                let details = overview.data.details();

                builder.push_record([
                    &group_name,
                    &count.to_string(),
                    &attribute.to_string(),
                    overview.data.attribute_type_name(),
                    &overview.data_type.to_string(),
                    &details,
                ]);
            }

            if group_overview.node_overview.attributes.is_empty() && count > 0 {
                builder.push_record([&group_name, &count.to_string(), "-", "-", "-", "-"]);
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

        for (group, group_overview) in std::iter::once((None, &self.ungrouped_overview)).chain(
            self.grouped_overviews
                .iter()
                .map(|(group, overview)| (Some(group), overview)),
        ) {
            let group_name =
                group.map_or_else(|| "Ungrouped".to_string(), std::string::ToString::to_string);
            let count = group_overview.edge_overview.count;

            for (attribute, overview) in &group_overview.edge_overview.attributes {
                let details = overview.data.details();

                builder.push_record([
                    &group_name,
                    &count.to_string(),
                    &attribute.to_string(),
                    overview.data.attribute_type_name(),
                    &overview.data_type.to_string(),
                    &details,
                ]);
            }

            if group_overview.edge_overview.attributes.is_empty() && count > 0 {
                builder.push_record([&group_name, &count.to_string(), "-", "-", "-", "-"]);
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
        Ok(Self {
            ungrouped_overview: GroupOverview::new(graphrecord, None, truncate_details)?,
            grouped_overviews: graphrecord
                .groups()
                .map(|group| {
                    Ok((
                        group.clone(),
                        GroupOverview::new(graphrecord, Some(group), truncate_details)?,
                    ))
                })
                .collect::<OverviewResult<_>>()?,
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
