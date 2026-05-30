use crate::{
    BoxedIterator,
    edges::{EdgeContext, EdgeOperandContext},
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult, graphrecord::EdgeIndex};

pub struct AllEdges;

impl From<AllEdges> for EdgeContext {
    fn from(context: AllEdges) -> Self {
        Self::AllEdges(context)
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
