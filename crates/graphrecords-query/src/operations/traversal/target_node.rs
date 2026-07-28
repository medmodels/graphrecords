use crate::{
    Explain, Failure, IndexDomain, Labeled, Operand, QueryResult,
    execution::EvaluationCache,
    operations::{
        Apply, OperationContext, Prepare, Relation, RelationOperation, SelectRelationOperation,
    },
    optimizer::{Count, CountKind, PlanIdentity, PlanInputs, Stats},
    traits::{TargetNode, ViaTargetNode},
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{EdgeIndex, NodeIndex},
};

#[derive(Clone, Explain, PlanIdentity, PlanInputs)]
#[explain(label = "Target")]
pub struct EdgeTarget;

impl Prepare for EdgeTarget {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl Relation for EdgeTarget {
    type From = EdgeIndex;
    type To = NodeIndex;

    fn resolve<'a>(
        _prepared: &Self::Prepared<'a>,
        graphrecord: &'a GraphRecord,
        from: <Self::From as IndexDomain>::Index<'a>,
    ) -> QueryResult<<Self::To as IndexDomain>::Index<'a>> {
        let endpoints = graphrecord
            .edge_endpoints(from)
            .map_err(|error| Failure::new_at::<EdgeIndex, _>(Self::LABEL, error, &from))?;

        Ok(endpoints.1)
    }

    fn codomain_count(stats: &Stats) -> Option<usize> {
        Some(stats.get::<Count>(&CountKind::Nodes))
    }
}

impl<O: Apply<RelationOperation<EdgeTarget>>> ViaTargetNode for O {
    type ReturnOperand = O::Output;

    fn via_target_node(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            RelationOperation::new(EdgeTarget),
        ))
    }
}

impl<O: Apply<SelectRelationOperation<EdgeTarget>>> TargetNode for O {
    type ReturnOperand = O::Output;

    fn target_node(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            SelectRelationOperation::new(EdgeTarget),
        ))
    }
}
