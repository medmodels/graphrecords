use crate::{
    Explain, IndexDomain, IndexValue, Indexed, QueryResult,
    execution::EvaluationCache,
    operations::{ElementKernel, Operation, Pipeline, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
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

    fn codomain_count(_stats: &Stats) -> Option<usize> {
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

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        let mut distinct = match (R::codomain_count(stats), input.distinct) {
            (Some(codomain), Some(distinct)) => Some(codomain.min(distinct)),
            (codomain, distinct) => codomain.or(distinct),
        };
        if let Some(elements) = input.elements {
            distinct = distinct.map(|distinct| distinct.min(elements));
        }

        Estimate {
            distinct,
            selectivity: None,
            ..input
        }
    }
}
