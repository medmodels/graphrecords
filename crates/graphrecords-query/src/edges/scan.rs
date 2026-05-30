use crate::{BoxedIterator, edges::EdgeOperandContext};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult, graphrecord::EdgeIndex};

pub struct AllEdges;

impl EdgeOperandContext for AllEdges {
    fn evaluate<'a>(
        &self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, &'a EdgeIndex>> {
        Ok(Box::new(graphrecord.edge_indices()))
    }
}
