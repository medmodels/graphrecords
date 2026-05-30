use crate::{BoxedIterator, EdgeOperand, bool::BoolMaskOperand, edges::EdgeOperandContext, traits};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult, graphrecord::EdgeIndex};

pub struct Filter {
    parent: BoolMaskOperand<EdgeOperand>,
}

impl EdgeOperandContext for Filter {
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

impl traits::Filter for EdgeOperand {
    type MaskOperand = BoolMaskOperand<Self>;
    type ReturnOperand = Self;

    fn filter(&self, mask: Self::MaskOperand) -> Self::ReturnOperand {
        Self::new(Filter { parent: mask })
    }
}
