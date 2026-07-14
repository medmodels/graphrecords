use crate::{
    Explain, Failure, IndexDomain, Labeled, Operand, OrderState, QueryResult, Unordered,
    execution::EvaluationCache,
    operands::{EdgeOperand, NodeOperand, ReferenceOperand},
    operations::{OperationContext, Prepare, Relation, RelationOperation},
    optimizer::{Cardinality, Count, CountKind, PlanIdentity, PlanInputs, Stats},
    traits::{Index, Select, SourceNode, ViaSourceNode},
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{EdgeIndex, NodeIndex},
};

#[derive(Clone, Explain, PlanIdentity, PlanInputs)]
#[explain(label = "Source")]
pub struct EdgeSource;

impl Prepare for EdgeSource {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl Relation for EdgeSource {
    type From = EdgeIndex;
    type To = NodeIndex;

    fn resolve<'a>(
        _prepared: &Self::Prepared<'a>,
        graphrecord: &'a GraphRecord,
        from: <Self::From as IndexDomain>::Index<'a>,
    ) -> QueryResult<<Self::To as IndexDomain>::Index<'a>> {
        let (source, _target) = graphrecord
            .edge_endpoints(from)
            .map_err(|error| Failure::new(Self::LABEL, error))?;

        Ok(source)
    }

    fn codomain_count(stats: &Stats) -> Option<Cardinality> {
        Some(Cardinality(stats.get::<Count>(&CountKind::Nodes)))
    }
}

impl<O: OrderState> ViaSourceNode for EdgeOperand<O> {
    type ReturnOperand = ReferenceOperand<EdgeIndex, NodeIndex, O>;

    fn via_source_node(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.index(),
            RelationOperation::new(EdgeSource),
        ))
    }
}

impl<K: IndexDomain, O: OrderState> ViaSourceNode for ReferenceOperand<K, EdgeIndex, O> {
    type ReturnOperand = ReferenceOperand<K, NodeIndex, O>;

    fn via_source_node(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            RelationOperation::new(EdgeSource),
        ))
    }
}

impl<O: OrderState> SourceNode for EdgeOperand<O> {
    type ReturnOperand = NodeOperand<Unordered>;

    fn source_node(&self) -> Self::ReturnOperand {
        self.via_source_node().select()
    }
}
