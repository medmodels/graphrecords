use crate::BoxedIterator;
use graphrecords_core::{GraphRecord, errors::GraphRecordResult, graphrecord::EdgeIndex};

pub trait EdgeOperandContext {
    fn evaluate<'a>(
        &self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, &'a EdgeIndex>>;
}

pub(crate) enum EdgeContext {
    AllEdges(AllEdges),
    Custom(Box<dyn EdgeOperandContext>),
}

impl EdgeContext {
    pub(super) fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, &'a EdgeIndex>> {
        match self {
            EdgeContext::AllEdges(context) => context.evaluate(graphrecord),
            EdgeContext::Custom(context) => context.evaluate(graphrecord),
        }
    }
}

pub(crate) struct AllEdges;

impl From<AllEdges> for EdgeContext {
    fn from(context: AllEdges) -> Self {
        EdgeContext::AllEdges(context)
    }
}

impl EdgeOperandContext for AllEdges {
    fn evaluate<'a>(
        &self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, &'a EdgeIndex>> {
        Ok(Box::new(graphrecord.edge_indices()))
    }
}
