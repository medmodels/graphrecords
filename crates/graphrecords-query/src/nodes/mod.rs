mod scan;
mod structure;

use crate::{
    BoxedIterator, Operand, RootOperand,
    execution::ExecutionContext,
    optimizer::{Cardinality, OptimizeInputs, PlanNode},
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult, graphrecord::NodeIndex};
pub use scan::AllNodes;
use std::sync::Arc;
pub use structure::{AttributeContext, FilterContext, GroupedAttributeContext, InGroupContext};

pub trait NodeOperandContext:
    PlanNode + OptimizeInputs<Output = NodeOperand> + Cardinality
{
    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<BoxedIterator<'a, &'a NodeIndex>>;
}

#[derive(Clone, Operand)]
#[operand(crate = "crate")]
pub struct NodeOperand {
    #[operand(context)]
    context: Arc<dyn NodeOperandContext>,
}

impl RootOperand for NodeOperand {
    type Index<'a> = &'a NodeIndex;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<BoxedIterator<'a, Self::Index<'a>>> {
        self.context.evaluate(graphrecord, context)
    }
}

impl NodeOperand {
    #[must_use]
    pub fn new<C: NodeOperandContext>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }
}
