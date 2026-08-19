#[cfg(feature = "plugins")]
use super::Changes;
use super::{Change, sealed::Sealed};
#[cfg(feature = "plugins")]
use crate::graphrecord::{GraphRecord, plugins::Plugin};
use crate::{
    errors::{GraphRecordError, GraphRecordResult},
    graphrecord::{batch::EdgeBatch, state::GraphState},
};

pub struct AddEdges {
    batch: EdgeBatch,
}

impl AddEdges {
    #[must_use]
    pub const fn new(batch: EdgeBatch) -> Self {
        Self { batch }
    }

    #[must_use]
    pub const fn batch(&self) -> &EdgeBatch {
        &self.batch
    }
}

impl Sealed for AddEdges {}

impl Change for AddEdges {
    fn apply(self: Box<Self>, mut state: GraphState) -> GraphRecordResult<GraphState> {
        let resolved_edges: Vec<_> = self
            .batch
            .into_iter()
            .map(|(source_node_index, target_node_index, attributes)| {
                let source_address = state.resolve_node_address(&source_node_index).ok_or(
                    GraphRecordError::NodeNotFound {
                        node_index: source_node_index,
                    },
                )?;
                let target_address = state.resolve_node_address(&target_node_index).ok_or(
                    GraphRecordError::NodeNotFound {
                        node_index: target_node_index,
                    },
                )?;

                Ok((source_address, target_address, attributes))
            })
            .collect::<GraphRecordResult<_>>()?;

        state.insert_edges(resolved_edges, &[])?;

        Ok(state)
    }

    #[cfg(feature = "plugins")]
    fn dispatch(
        self: Box<Self>,
        plugin: &dyn Plugin,
        record: &GraphRecord,
    ) -> GraphRecordResult<Changes> {
        plugin.on_add_edges(record, *self)
    }

    #[cfg(feature = "plugins")]
    fn post_dispatch_hook(
        &self,
    ) -> fn(
        plugin: &dyn Plugin,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        |plugin, previous, candidate| plugin.post_add_edges(previous, candidate)
    }
}
