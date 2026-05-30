use crate::{BoxedIterator, EdgeOperand, bool::BoolMaskOperand, edges::EdgeOperandContext, traits};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult, graphrecord::EdgeIndex};

pub struct Where {
    parent: BoolMaskOperand<EdgeOperand>,
}

impl EdgeOperandContext for Where {
    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, &'a EdgeIndex>> {
        let edge_indices_with_mask = self.parent.evaluate(graphrecord)?;

        Ok(Box::new(edge_indices_with_mask.filter_map(
            |(edge_index, mask)| {
                if mask { Some(edge_index) } else { None }
            },
        )))
    }
}

impl traits::Where for EdgeOperand {
    type MaskOperand = BoolMaskOperand<Self>;
    type ReturnOperand = Self;

    fn r#where(&self, predicate: Self::MaskOperand) -> Self::ReturnOperand {
        Self::new(Where { parent: predicate })
    }
}
