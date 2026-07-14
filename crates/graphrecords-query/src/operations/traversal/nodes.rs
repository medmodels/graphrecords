use crate::{
    EvaluateOperand, Explain, Indexed, Multiple, Operand, OrderState, QueryResult, Unit, Unordered,
    execution::EvaluationCache,
    operands::{EdgeOperand, NodeOperand, OperandHandle},
    operations::{Kernel, KeyedStream, Operation, OperationContext, Prepare},
    optimizer::{EstimateCost, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Nodes,
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{EdgeIndex, NodeIndex},
};
use graphrecords_utils::aliases::GrHashSet;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "Nodes")]
#[plan(optimizer_hints(distinct))]
pub struct NodesOperation;

impl Prepare for NodesOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<O: OrderState> Kernel<Indexed<EdgeIndex, Unit>, Multiple<O>> for NodesOperation {
    type Output = NodeOperand<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, EdgeIndex, Unit, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let nodes: GrHashSet<_> = values
            .map(|(edge, membership)| {
                membership.map(|()| graphrecord.edge_endpoints(edge).expect("Edge must exist"))
            })
            .collect::<QueryResult<Vec<(&NodeIndex, &NodeIndex)>>>()?
            .into_iter()
            .flat_map(|(source, target)| std::iter::once(source).chain(std::iter::once(target)))
            .collect();

        Ok(Box::new(nodes.into_iter().map(|node| (node, Ok(())))))
    }
}

impl<O: OrderState> EstimateCost<NodesOperation>
    for OperandHandle<Indexed<EdgeIndex, Unit>, Multiple<O>>
{
    type OutputCost = <NodeOperand<Unordered> as Operand>::Cost;

    fn estimate(
        _operation: &NodesOperation,
        input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        input_cost
    }
}

impl<O: OrderState> Nodes for EdgeOperand<O> {
    type ReturnOperand = NodeOperand<Unordered>;

    fn nodes(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), NodesOperation))
    }
}
