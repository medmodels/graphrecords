use crate::{
    EdgeEndpointRole, EntityReference, ExpandedChild, ExpandedIndex, Explain, IndexDomain, Indexed,
    Operand, Ordered, QueryResult, Unit,
    element::{Expanding, Pipeline},
    execution::EvaluationCache,
    operations::{Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    traits::ViaNodes,
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{EdgeIndex, NodeIndex},
};

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "ViaNodes")]
#[plan(optimizer_hints(empty = if_any))]
pub struct ViaNodesOperation;

impl Prepare for ViaNodesOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl ElementKernel<Indexed<EdgeIndex, Unit>> for ViaNodesOperation {
    type Emission = Expanding<Ordered>;
    type OutShape = Indexed<ExpandedIndex<EdgeIndex, EdgeEndpointRole>, EntityReference<NodeIndex>>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<EdgeIndex, Unit>, Self>> {
        Ok(Pipeline::keyed(move |parent_index, ()| {
            let (source, target) = graphrecord
                .edge_endpoints(parent_index)
                .expect("Edge must exist");

            Ok(vec![
                ExpandedChild::success(EdgeEndpointRole::Source, source),
                ExpandedChild::success(EdgeEndpointRole::Target, target),
            ])
        }))
    }
}

impl<I: IndexDomain> ElementKernel<Indexed<I, EntityReference<EdgeIndex>>> for ViaNodesOperation {
    type Emission = Expanding<Ordered>;
    type OutShape = Indexed<ExpandedIndex<I, EdgeEndpointRole>, EntityReference<NodeIndex>>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, EntityReference<EdgeIndex>>, Self>> {
        Ok(Pipeline::unkeyed(move |edge| {
            let (source, target) = graphrecord.edge_endpoints(edge).expect("Edge must exist");

            Ok(vec![
                ExpandedChild::success(EdgeEndpointRole::Source, source),
                ExpandedChild::success(EdgeEndpointRole::Target, target),
            ])
        }))
    }
}

impl<O: Apply<ViaNodesOperation>> ViaNodes for O {
    type ReturnOperand = O::Output;

    fn via_nodes(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), ViaNodesOperation))
    }
}
