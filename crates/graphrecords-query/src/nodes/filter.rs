use crate::{
    BoxedIterator,
    bool::BoolMaskOperand,
    nodes::{NodeContext, NodeOperand, NodeOperandContext},
    traits,
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult, graphrecord::NodeIndex};

pub struct Where {
    parent: BoolMaskOperand<NodeOperand>,
}

impl From<Where> for NodeContext {
    fn from(context: Where) -> Self {
        Self::Where(context)
    }
}

impl NodeOperandContext for Where {
    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, &'a NodeIndex>> {
        let node_indices_with_mask = self.parent.evaluate(graphrecord)?;

        Ok(Box::new(node_indices_with_mask.filter_map(
            |(node_index, mask)| {
                if mask { Some(node_index) } else { None }
            },
        )))
    }
}

impl traits::Where for NodeOperand {
    type MaskOperand = BoolMaskOperand<Self>;
    type ReturnOperand = Self;

    fn r#where(&self, predicate: Self::MaskOperand) -> Self::ReturnOperand {
        Self::new(Where { parent: predicate })
    }
}
