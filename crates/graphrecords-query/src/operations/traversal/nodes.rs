use crate::{
    Definite, EvaluateOperand, Explain, Failure, Indexed, Labeled, Multiple, Operand, OrderState,
    QueryResult, Single, Unit, Unordered,
    execution::EvaluationCache,
    operands::NodesOperand,
    operations::{Apply, KeyedStream, LaneKernel, Operation, OperationContext, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    traits::Nodes,
};
use graphrecords_core::{GraphRecord, graphrecord::EdgeIndex};
use graphrecords_utils::aliases::GrHashSet;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "Nodes")]
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

impl<O: OrderState> LaneKernel<Indexed<EdgeIndex, Unit>, Multiple<O>> for NodesOperation {
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
                        .map_err(|error| Failure::new_at::<EdgeIndex, _>(Self::LABEL, error, &edge))
                })
            })
            .collect::<QueryResult<Vec<_>>>()?
            .into_iter()
            .flat_map(|(source, target)| std::iter::once(source).chain(std::iter::once(target)))
            .collect();

        Ok(Box::new(nodes.into_iter().map(|node| (node, Ok(())))))
    }
}

impl LaneKernel<Indexed<EdgeIndex, Unit>, Single> for NodesOperation {
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
            .map_err(|error| Failure::new_at::<EdgeIndex, _>(Self::LABEL, error, &edge))?;
        let nodes: GrHashSet<_> = std::iter::once(source)
            .chain(std::iter::once(target))
            .collect();

        Ok(Box::new(nodes.into_iter().map(|node| (node, Ok(())))))
    }
}

impl LaneKernel<Indexed<EdgeIndex, Unit>, Definite> for NodesOperation {
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
            .map_err(|error| Failure::new_at::<EdgeIndex, _>(Self::LABEL, error, &edge))?;
        let nodes: GrHashSet<_> = std::iter::once(source)
            .chain(std::iter::once(target))
            .collect();

        Ok(Box::new(nodes.into_iter().map(|node| (node, Ok(())))))
    }
}

impl<O: Apply<NodesOperation>> Nodes for O {
    type ReturnOperand = O::Output;

    fn nodes(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), NodesOperation))
    }
}
