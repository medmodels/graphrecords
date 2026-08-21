#[cfg(feature = "plugins")]
use super::Changes;
use super::{Change, sealed::Sealed};
#[cfg(feature = "plugins")]
use crate::graphrecord::{GraphRecord, plugins::Plugin};
use crate::{errors::GraphRecordResult, graphrecord::state::GraphState};

pub struct UnfreezeSchema;

impl UnfreezeSchema {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Default for UnfreezeSchema {
    fn default() -> Self {
        Self::new()
    }
}

impl Sealed for UnfreezeSchema {}

impl Change for UnfreezeSchema {
    fn apply(self: Box<Self>, mut state: GraphState) -> GraphRecordResult<GraphState> {
        state.unfreeze_schema();

        Ok(state)
    }

    #[cfg(feature = "plugins")]
    fn dispatch(
        self: Box<Self>,
        plugin: &dyn Plugin,
        record: &GraphRecord,
    ) -> GraphRecordResult<Changes> {
        plugin.on_unfreeze_schema(record, *self)
    }

    #[cfg(feature = "plugins")]
    fn post_dispatch_hook(
        &self,
    ) -> fn(
        plugin: &dyn Plugin,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        |plugin, previous, candidate| plugin.post_unfreeze_schema(previous, candidate)
    }
}
