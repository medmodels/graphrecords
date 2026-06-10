mod scan;
mod structure;

use crate::{
    BoxedIterator, EvaluateContext, EvaluateOperand, Explain, Operand, RootOperand,
    execution::ExecutionContext,
    optimizer::{Cardinality, OptimizeInputs, PlanNode},
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult, graphrecord::EdgeIndex};
pub use scan::AllEdges;
use std::sync::Arc;
pub use structure::{AttributeContext, FilterContext, GroupedAttributeContext, InGroupContext};

pub trait EdgeOperandContext:
    PlanNode
    + OptimizeInputs<Output = EdgeOperand>
    + Cardinality
    + Explain
    + EvaluateContext<Operand = EdgeOperand>
{
}

#[derive(Clone, Operand)]
pub struct EdgeOperand {
    #[operand(context)]
    context: Arc<dyn EdgeOperandContext>,
}

impl RootOperand for EdgeOperand {
    type Index<'a> = &'a EdgeIndex;
}

impl EvaluateOperand for EdgeOperand {
    type ReturnValue<'a> = BoxedIterator<'a, &'a EdgeIndex>;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<Self::ReturnValue<'a>> {
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
