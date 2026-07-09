use crate::{
    Explain, IndexDomain, IndexValue, Indexed, Multiple, Operand, OrderState, QueryResult,
    execution::EvaluationCache,
    operands::{OperandHandle, ReferenceOperand},
    operations::{ElementKernel, Operation, Pipeline, Prepare},
    optimizer::{
        Cardinality, EstimateCost, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs,
        Stats, ValueCost,
    },
};
use graphrecords_core::GraphRecord;

pub trait Relation: Prepare + Clone + Explain + PlanIdentity + PlanInputs {
    type From: IndexDomain;
    type To: IndexDomain;

    fn resolve<'a>(
        prepared: &Self::Prepared<'a>,
        graphrecord: &'a GraphRecord,
        from: <Self::From as IndexDomain>::Index<'a>,
    ) -> QueryResult<<Self::To as IndexDomain>::Index<'a>>;

    fn codomain_count(_stats: &Stats) -> Option<Cardinality> {
        None
    }
}

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "Relation")]
pub struct RelationOperation<R> {
    #[argument]
    relation: R,
}

impl<R: Relation> RelationOperation<R> {
    #[must_use]
    pub const fn new(relation: R) -> Self {
        Self { relation }
    }
}

impl<R: Relation> Prepare for RelationOperation<R> {
    type Prepared<'a>
        = R::Prepared<'a>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.relation.prepare(graphrecord, cache)
    }
}

impl<R: Relation, K: IndexDomain> ElementKernel<Indexed<K, IndexValue<R::From>>>
    for RelationOperation<R>
{
    type OutShape = Indexed<K, IndexValue<R::To>>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<
        Pipeline<
            'a,
            (
                K::Index<'a>,
                QueryResult<<R::From as IndexDomain>::Index<'a>>,
            ),
            (K::Index<'a>, QueryResult<<R::To as IndexDomain>::Index<'a>>),
        >,
    > {
        Ok(Pipeline::default().map(
            move |(key, reference): (
                K::Index<'a>,
                QueryResult<<R::From as IndexDomain>::Index<'a>>,
            )| {
                let resolved =
                    reference.and_then(|entity| R::resolve(&prepared, graphrecord, entity));

                (key, resolved)
            },
        ))
    }
}

impl<R: Relation, K: IndexDomain, O: OrderState> EstimateCost<RelationOperation<R>>
    for OperandHandle<Indexed<K, IndexValue<R::From>>, Multiple<O>>
{
    type OutputCost = <ReferenceOperand<K, R::To> as Operand>::Cost;

    fn estimate(
        _operation: &RelationOperation<R>,
        input_cost: <Self as Operand>::Cost,
        stats: &Stats,
    ) -> Self::OutputCost {
        let distinct = match R::codomain_count(stats) {
            Some(codomain) => codomain.min(input_cost.distinct()),
            None => input_cost.distinct(),
        };

        ValueCost::new(input_cost.rows(), distinct)
    }
}
