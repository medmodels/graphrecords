use crate::{
    Explain, Failure, IndexDomain, Labeled, Operand, QueryResult,
    execution::EvaluationCache,
    operands::{EdgeOperand, NodeOperand, ReferenceOperand},
    operations::{OperationContext, Prepare, Relation, RelationOperation},
    optimizer::{Cardinality, Count, CountKind, PlanIdentity, PlanInputs, Stats},
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
            .map_err(|error| Failure::new(Self::LABEL, error))?;

        Ok(target)
    }

    fn codomain_count(stats: &Stats) -> Option<Cardinality> {
        Some(Cardinality(stats.get::<Count>(&CountKind::Nodes)))
    }
}

impl ViaTargetNode for EdgeOperand {
    type ReturnOperand = ReferenceOperand<EdgeIndex, NodeIndex>;

    fn via_target_node(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.index(),
            RelationOperation::new(EdgeTarget),
        ))
    }
}

impl<K: IndexDomain> ViaTargetNode for ReferenceOperand<K, EdgeIndex> {
    type ReturnOperand = ReferenceOperand<K, NodeIndex>;

    fn via_target_node(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            RelationOperation::new(EdgeTarget),
        ))
    }
}

impl TargetNode for EdgeOperand {
    type ReturnOperand = NodeOperand;

    fn target_node(&self) -> Self::ReturnOperand {
        self.via_target_node().select()
    }
}
