use crate::{
    Explain, Failure, IndexDomain, Labeled, Operand, OrderState, QueryResult, Unordered,
    execution::EvaluationCache,
    operands::{EdgeOperand, NodeOperand, ReferenceOperand},
    operations::{OperationContext, Prepare, Relation, RelationOperation},
    optimizer::{Count, CountKind, PlanIdentity, PlanInputs, Stats},
    traits::{Index, Select, TargetNode, ViaTargetNode},
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
        let (_source, target) = graphrecord
            .edge_endpoints(from)
            .map_err(|error| Failure::new_at(Self::LABEL, error, &from))?;

        Ok(target)
    }

    fn codomain_count(stats: &Stats) -> Option<usize> {
        Some(stats.get::<Count>(&CountKind::Nodes))
    }
}

impl<O: OrderState> ViaTargetNode for EdgeOperand<O> {
    type ReturnOperand = ReferenceOperand<EdgeIndex, NodeIndex, O>;

    fn via_target_node(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.index(),
            RelationOperation::new(EdgeTarget),
        ))
    }
}

impl<K: IndexDomain, O: OrderState> ViaTargetNode for ReferenceOperand<K, EdgeIndex, O> {
    type ReturnOperand = ReferenceOperand<K, NodeIndex, O>;

    fn via_target_node(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            RelationOperation::new(EdgeTarget),
        ))
    }
}

impl<O: OrderState> TargetNode for EdgeOperand<O> {
    type ReturnOperand = NodeOperand<Unordered>;

    fn target_node(&self) -> Self::ReturnOperand {
        self.via_target_node().select()
    }
}
