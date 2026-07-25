use crate::{
    Definite, EvaluateOperand, Explain, Failure, Indexed, Labeled, Multiple, Operand, OrderState,
    QueryResult, Single, Unit, Unordered,
    execution::EvaluationCache,
    operands::NodesOperand,
    operations::{Apply, Kernel, KeyedStream, Operation, OperationContext, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
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
    type Output = NodesOperand<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, EdgeIndex, Unit, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let nodes: GrHashSet<_> = values
            .map(|(edge, membership)| {
                membership.and_then(|()| {
                    graphrecord
                        .edge_endpoints(edge)
                        .map_err(|error| Failure::new_at(Self::LABEL, error, &edge))
                })
            })
            .collect::<QueryResult<Vec<(&NodeIndex, &NodeIndex)>>>()?
            .into_iter()
            .flat_map(|(source, target)| std::iter::once(source).chain(std::iter::once(target)))
            .collect();

        Ok(Box::new(nodes.into_iter().map(|node| (node, Ok(())))))
    }
}

impl Kernel<Indexed<EdgeIndex, Unit>, Single> for NodesOperation {
    type Output = NodesOperand<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, EdgeIndex, Unit, Single>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let Some((edge, membership)) = value else {
            return Ok(Box::new(std::iter::empty()));
        };
        membership?;

        let (source, target) = graphrecord
            .edge_endpoints(edge)
            .map_err(|error| Failure::new_at(Self::LABEL, error, &edge))?;
        let nodes: GrHashSet<_> = std::iter::once(source)
            .chain(std::iter::once(target))
            .collect();

        Ok(Box::new(nodes.into_iter().map(|node| (node, Ok(())))))
    }
}

impl Kernel<Indexed<EdgeIndex, Unit>, Definite> for NodesOperation {
    type Output = NodesOperand<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, EdgeIndex, Unit, Definite>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let (edge, membership) = value;
        membership?;

        let (source, target) = graphrecord
            .edge_endpoints(edge)
            .map_err(|error| Failure::new_at(Self::LABEL, error, &edge))?;
        let nodes: GrHashSet<_> = std::iter::once(source)
            .chain(std::iter::once(target))
            .collect();

        Ok(Box::new(nodes.into_iter().map(|node| (node, Ok(())))))
    }
}

impl<O> Nodes for O
where
    O: Apply<NodesOperation>,
{
    type ReturnOperand = <O as Apply<NodesOperation>>::Output;

    fn nodes(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), NodesOperation))
    }
}
