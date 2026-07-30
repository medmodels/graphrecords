use crate::{
    EdgeEndpointRole, EntityReference, Explain, IndexDomain, Indexed, Operand, QueryResult, Unit,
    element::{Pipeline, Preserving},
    execution::EvaluationCache,
    operations::{Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Prepare},
    optimizer::{
        Count, CountKind, Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs,
        Stats,
    },
    traits::{Select, SourceNode, TargetNode, ViaSourceNode, ViaTargetNode},
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{EdgeIndex, NodeIndex},
};

fn endpoint_estimate(input: Estimate, stats: &Stats) -> Estimate {
    let node_count = stats.get::<Count>(&CountKind::Nodes);
    let distinct = input
        .distinct
        .map_or(node_count, |distinct| node_count.min(distinct));
    let distinct = input
        .elements
        .map_or(distinct, |elements| distinct.min(elements));

    Estimate {
        distinct: Some(distinct),
        selectivity: None,
        ..input
    }
}

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "Endpoint")]
#[plan(optimizer_hints(empty = if_any))]
pub struct EndpointOperation {
    #[explain(label)]
    role: EdgeEndpointRole,
}

impl Prepare for EndpointOperation {
    type Prepared<'a> = EdgeEndpointRole;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self.role)
    }
}

impl ElementKernel<Indexed<EdgeIndex, Unit>> for EndpointOperation {
    type Emission = Preserving;
    type OutShape = Indexed<EdgeIndex, EntityReference<NodeIndex>>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<EdgeIndex, Unit>, Self>> {
        Ok(Pipeline::keyed(move |edge, membership: QueryResult<_>| {
            membership.map(|()| {
                let (source, target) = graphrecord.edge_endpoints(edge).expect("Edge must exist");

                match prepared {
                    EdgeEndpointRole::Source => source,
                    EdgeEndpointRole::Target => target,
                }
            })
        }))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        endpoint_estimate(input, stats)
    }
}

impl<I: IndexDomain> ElementKernel<Indexed<I, EntityReference<EdgeIndex>>> for EndpointOperation {
    type Emission = Preserving;
    type OutShape = Indexed<I, EntityReference<NodeIndex>>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, EntityReference<EdgeIndex>>, Self>> {
        Ok(Pipeline::unkeyed(move |edge: QueryResult<_>| {
            edge.map(|edge| {
                let (source, target) = graphrecord.edge_endpoints(edge).expect("Edge must exist");

                match prepared {
                    EdgeEndpointRole::Source => source,
                    EdgeEndpointRole::Target => target,
                }
            })
        }))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        endpoint_estimate(input, stats)
    }
}

impl<O: Apply<EndpointOperation>> ViaSourceNode for O {
    type ReturnOperand = O::Output;

    fn via_source_node(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            EndpointOperation {
                role: EdgeEndpointRole::Source,
            },
        ))
    }
}

impl<O: Apply<EndpointOperation>> ViaTargetNode for O {
    type ReturnOperand = O::Output;

    fn via_target_node(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            EndpointOperation {
                role: EdgeEndpointRole::Target,
            },
        ))
    }
}

impl<O> SourceNode for O
where
    O: ViaSourceNode,
    O::ReturnOperand: Select,
{
    type ReturnOperand = <O::ReturnOperand as Select>::ReturnOperand;

    fn source_node(&self) -> Self::ReturnOperand {
        self.via_source_node().select()
    }
}

impl<O> TargetNode for O
where
    O: ViaTargetNode,
    O::ReturnOperand: Select,
{
    type ReturnOperand = <O::ReturnOperand as Select>::ReturnOperand;

    fn target_node(&self) -> Self::ReturnOperand {
        self.via_target_node().select()
    }
}
