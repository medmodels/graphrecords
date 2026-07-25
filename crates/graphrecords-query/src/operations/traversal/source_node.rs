use crate::{
    Explain, Failure, IndexDomain, Labeled, Operand, QueryResult,
    execution::EvaluationCache,
    operations::{
        Apply, OperationContext, Prepare, Relation, RelationOperation, SelectRelationOperation,
    },
    optimizer::{Count, CountKind, PlanIdentity, PlanInputs, Stats},
    traits::{SourceNode, ViaSourceNode},
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
            .map_err(|error| Failure::new_at(Self::LABEL, error, &from))?;

        Ok(source)
    }

    fn codomain_count(stats: &Stats) -> Option<usize> {
        Some(stats.get::<Count>(&CountKind::Nodes))
    }
}

impl<O> ViaSourceNode for O
where
    O: Apply<RelationOperation<EdgeSource>>,
{
    type ReturnOperand = <O as Apply<RelationOperation<EdgeSource>>>::Output;

    fn via_source_node(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            RelationOperation::new(EdgeSource),
        ))
    }
}

impl<O> SourceNode for O
where
    O: Apply<SelectRelationOperation<EdgeSource>>,
{
    type ReturnOperand = <O as Apply<SelectRelationOperation<EdgeSource>>>::Output;

    fn source_node(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            SelectRelationOperation::new(EdgeSource),
        ))
    }
}
