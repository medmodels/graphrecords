#[cfg(feature = "plugins")]
use super::Changes;
use super::{Change, sealed::Sealed};
#[cfg(feature = "plugins")]
use crate::graphrecord::{GraphRecord, plugins::Plugin};
use crate::{errors::GraphRecordResult, graphrecord::state::GraphState};

pub struct Clear;

impl Clear {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Default for Clear {
    fn default() -> Self {
        Self::new()
    }
}

impl Sealed for Clear {}

impl Change for Clear {
    fn apply(&self, mut state: GraphState) -> GraphRecordResult<GraphState> {
        state.clear_content();

        Ok(state)
    }

    #[cfg(feature = "plugins")]
    fn pre_dispatch(
        self: Box<Self>,
        plugin: &dyn Plugin,
        record: &GraphRecord,
    ) -> GraphRecordResult<Changes> {
        plugin.pre_clear(record, *self)
    }

    #[cfg(feature = "plugins")]
    fn post_dispatch(
        &self,
        plugin: &dyn Plugin,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        plugin.post_clear(previous, candidate, self)
    }
}
