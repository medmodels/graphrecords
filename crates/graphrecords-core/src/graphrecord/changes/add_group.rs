#[cfg(feature = "plugins")]
use super::Changes;
use super::{Change, sealed::Sealed};
#[cfg(feature = "plugins")]
use crate::graphrecord::{GraphRecord, plugins::Plugin};
use crate::{
    errors::GraphRecordResult,
    graphrecord::{datatypes::GroupIndex, state::GraphState},
};

pub struct AddGroup {
    group_index: GroupIndex,
}

impl AddGroup {
    #[must_use]
    pub const fn new(group_index: GroupIndex) -> Self {
        Self { group_index }
    }

    #[must_use]
    pub const fn group_index(&self) -> &GroupIndex {
        &self.group_index
    }
}

impl Sealed for AddGroup {}

impl Change for AddGroup {
    fn apply(&self, mut state: GraphState) -> GraphRecordResult<GraphState> {
        state.insert_group(&self.group_index)?;

        Ok(state)
    }

    #[cfg(feature = "plugins")]
    fn pre_dispatch(
        self: Box<Self>,
        plugin: &dyn Plugin,
        record: &GraphRecord,
    ) -> GraphRecordResult<Changes> {
        plugin.pre_add_group(record, *self)
    }

    #[cfg(feature = "plugins")]
    fn post_dispatch(
        &self,
        plugin: &dyn Plugin,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        plugin.post_add_group(previous, candidate, self)
    }
}
