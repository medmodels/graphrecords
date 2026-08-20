use crate::{
    EdgeEndpointRole, EntityRef, EntityReference, Explain, IndexDomain, Indexed, QueryResult, Unit,
    element::{Pipeline, Preserving},
    execution::EvaluationCache,
    operations::{Build, ElementKernel, ElementPipeline, Operation, Prepare},
    optimizer::{
        Count, CountKind, Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs,
        Stats,
    },
    registry::operation_manifest,
    traits::{Select, SourceNode, TargetNode, ViaSourceNode, ViaTargetNode},
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{EdgeIndex, NodeIndex, StateView},
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
#[plan(optimizer_hints(commutes_with_filter, allows_limit_pushdown, empty = if_any))]
pub struct EndpointOperation {
    #[explain(label)]
    role: EdgeEndpointRole,
}

impl Prepare for EndpointOperation {
    type Prepared<'a> = EdgeEndpointRole;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
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
                let (source, target) = StateView::of(graphrecord).edge_endpoints(edge);

                match prepared {
                    EdgeEndpointRole::Source => EntityRef::new(graphrecord, source),
                    EdgeEndpointRole::Target => EntityRef::new(graphrecord, target),
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
        Ok(Pipeline::unkeyed(
            move |edge: QueryResult<EntityRef<'a, EdgeIndex>>| {
                edge.map(|edge| {
                    let (source, target) =
                        StateView::of(graphrecord).edge_endpoints(*edge.address());

                    match prepared {
                        EdgeEndpointRole::Source => EntityRef::new(graphrecord, source),
                        EdgeEndpointRole::Target => EntityRef::new(graphrecord, target),
                    }
                })
            },
        ))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        endpoint_estimate(input, stats)
    }
}

impl<E: Build<EndpointOperation>> ViaSourceNode for E {
    type Output = E::Output;

    fn via_source_node(&self) -> Self::Output {
        self.build(EndpointOperation {
            role: EdgeEndpointRole::Source,
        })
    }
}

impl<E: Build<EndpointOperation>> ViaTargetNode for E {
    type Output = E::Output;

    fn via_target_node(&self) -> Self::Output {
        self.build(EndpointOperation {
            role: EdgeEndpointRole::Target,
        })
    }
}

impl<E> SourceNode for E
where
    E: ViaSourceNode,
    E::Output: Select,
{
    type Output = <E::Output as Select>::Output;

    fn source_node(&self) -> Self::Output {
        self.via_source_node().select()
    }
}

impl<E> TargetNode for E
where
    E: ViaTargetNode,
    E::Output: Select,
{
    type Output = <E::Output as Select>::Output;

    fn target_node(&self) -> Self::Output {
        self.via_target_node().select()
    }
}

pub(super) mod via_source_node {
    use super::{
        EdgeIndex, EndpointOperation, EntityReference, Indexed, NodeIndex, Preserving, Unit,
        ViaSourceNode, operation_manifest,
    };

    operation_manifest! {
        EndpointOperation {
            method: ViaSourceNode::via_source_node;
            scope: element;

            kernel {
                parameters: <>;
                input: Indexed<EdgeIndex, Unit>;
                output: Indexed<EdgeIndex, EntityReference<NodeIndex>>;
                emission: Preserving;
            }

            kernel {
                parameters: <I: IndexDomain>;
                input: Indexed<I, EntityReference<EdgeIndex>>;
                output: Indexed<I, EntityReference<NodeIndex>>;
                emission: Preserving;
            }
        }
    }
}

pub(super) mod via_target_node {
    use super::{
        EdgeIndex, EndpointOperation, EntityReference, Indexed, NodeIndex, Preserving, Unit,
        ViaTargetNode, operation_manifest,
    };

    operation_manifest! {
        EndpointOperation {
            method: ViaTargetNode::via_target_node;
            scope: element;

            kernel {
                parameters: <>;
                input: Indexed<EdgeIndex, Unit>;
                output: Indexed<EdgeIndex, EntityReference<NodeIndex>>;
                emission: Preserving;
            }

            kernel {
                parameters: <I: IndexDomain>;
                input: Indexed<I, EntityReference<EdgeIndex>>;
                output: Indexed<I, EntityReference<NodeIndex>>;
                emission: Preserving;
            }
        }
    }
}
