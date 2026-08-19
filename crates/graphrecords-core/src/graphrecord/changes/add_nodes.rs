#[cfg(feature = "plugins")]
use super::Changes;
use super::{Change, sealed::Sealed};
#[cfg(feature = "plugins")]
use crate::graphrecord::{GraphRecord, plugins::Plugin};
use crate::{
    errors::GraphRecordResult,
    graphrecord::{batch::NodeBatch, state::GraphState},
};

pub struct AddNodes {
    batch: NodeBatch,
}

impl AddNodes {
    #[must_use]
    pub const fn new(batch: NodeBatch) -> Self {
        Self { batch }
    }

    #[must_use]
    pub const fn batch(&self) -> &NodeBatch {
        &self.batch
    }
}

impl Sealed for AddNodes {}

impl Change for AddNodes {
    fn apply(self: Box<Self>, mut state: GraphState) -> GraphRecordResult<GraphState> {
        for (node_index, attributes) in self.batch {
            state.insert_node(node_index, &attributes, &[])?;
        }

        Ok(state)
    }

    #[cfg(feature = "plugins")]
    fn dispatch(
        self: Box<Self>,
        plugin: &dyn Plugin,
        record: &GraphRecord,
    ) -> GraphRecordResult<Changes> {
        plugin.on_add_nodes(record, *self)
    }

    #[cfg(feature = "plugins")]
    fn post_dispatch_hook(
        &self,
    ) -> fn(
        plugin: &dyn Plugin,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        |plugin, previous, candidate| plugin.post_add_nodes(previous, candidate)
    }
}
