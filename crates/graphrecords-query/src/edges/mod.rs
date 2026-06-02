mod attribute;
mod filter;
mod in_group;
mod scan;

use crate::{
    BoxedIterator, Operand, RootOperand,
    execution::ExecutionContext,
    optimizer::{Cardinality, OptimizeInputs, PlanNode},
};
pub use attribute::AttributeContext;
pub use filter::FilterContext;
use graphrecords_core::{GraphRecord, errors::GraphRecordResult, graphrecord::EdgeIndex};
pub use in_group::InGroupContext;
pub use scan::AllEdges;
use std::sync::Arc;

pub trait EdgeOperandContext:
    PlanNode + OptimizeInputs<Output = EdgeOperand> + Cardinality
{
    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<BoxedIterator<'a, &'a EdgeIndex>>;
}

#[derive(Clone, Operand)]
#[operand(crate = "crate")]
pub struct EdgeOperand {
    #[operand(context)]
    context: Arc<dyn EdgeOperandContext>,
}

impl RootOperand for EdgeOperand {
    type Index<'a> = &'a EdgeIndex;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<BoxedIterator<'a, Self::Index<'a>>> {
        self.context.evaluate(graphrecord, context)
    }
}

impl EdgeOperand {
    #[must_use]
    pub fn new<C: EdgeOperandContext>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }
}
