#[cfg(feature = "plugins")]
use super::Changes;
use super::{Change, sealed::Sealed};
#[cfg(feature = "plugins")]
use crate::graphrecord::{GraphRecord, plugins::Plugin};
use crate::{errors::GraphRecordResult, graphrecord::state::GraphState};

pub struct FreezeSchema;

impl FreezeSchema {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Default for FreezeSchema {
    fn default() -> Self {
        Self::new()
    }
}

impl Sealed for FreezeSchema {}

impl Change for FreezeSchema {
    fn apply(&self, mut state: GraphState) -> GraphRecordResult<GraphState> {
        state.freeze_schema();

        Ok(state)
    }

    #[cfg(feature = "plugins")]
    fn pre_dispatch(
        self: Box<Self>,
        plugin: &dyn Plugin,
        record: &GraphRecord,
    ) -> GraphRecordResult<Changes> {
        plugin.pre_freeze_schema(record, *self)
    }

    #[cfg(feature = "plugins")]
    fn post_dispatch(
        &self,
        plugin: &dyn Plugin,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        plugin.post_freeze_schema(previous, candidate, self)
    }
}
