mod attribute;
mod error;
mod graphrecord;
mod group;
pub mod prelude;
mod tabled_modifiers;

pub use attribute::{AttributeOverview, AttributeOverviewData};
pub use error::{OverviewError, OverviewResult};
pub use graphrecord::{Overview, Overviewable};
pub use group::{EdgeGroupOverview, GroupOverview, GroupOverviewable, NodeGroupOverview};

pub const DEFAULT_TRUNCATE_DETAILS: usize = 80;
