use crate::{
    Definite, EntityDomain, EntityReference, EvaluateOperand, Explain, IndexDomain, Indexed,
    Multiple, OrderState, QueryResult, Single, Unit, Unordered,
    element::{Pipeline, Preserving},
    execution::EvaluationCache,
    operands::{DefiniteElementOperand, ElementOperand, ElementsOperand},
    operations::{ElementKernel, ElementPipeline, KeyedStream, LaneKernel, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
};
use graphrecords_core::GraphRecord;
use graphrecords_utils::aliases::GrHashSet;

pub trait Relation: Prepare + Clone + Explain + PlanIdentity + PlanInputs {
    type From: EntityDomain;
    type To: EntityDomain;

    fn resolve<'a>(
        prepared: &Self::Prepared<'a>,
        graphrecord: &'a GraphRecord,
        from: <Self::From as IndexDomain>::Index<'a>,
    ) -> QueryResult<<Self::To as IndexDomain>::Index<'a>>;

    #[allow(unused_variables)]
    fn codomain_count(stats: &Stats) -> Option<usize> {
        None
    }
}

fn relation_estimate<R: Relation>(input: Estimate, stats: &Stats) -> Estimate {
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

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
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

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "SelectRelation")]
pub struct SelectRelationOperation<R> {
    #[argument]
    relation: R,
}

impl<R: Relation> SelectRelationOperation<R> {
    #[must_use]
    pub const fn new(relation: R) -> Self {
        Self { relation }
    }
}

impl<R: Relation> Prepare for SelectRelationOperation<R> {
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

impl<R: Relation> ElementKernel<Indexed<R::From, Unit>> for RelationOperation<R> {
    type Emission = Preserving;
    type OutShape = Indexed<R::From, EntityReference<R::To>>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<R::From, Unit>, Self>> {
        Ok(Pipeline::keyed(move |index, membership: QueryResult<_>| {
            membership.and_then(|()| R::resolve(&prepared, graphrecord, index))
        }))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        relation_estimate::<R>(input, stats)
    }
}

impl<R: Relation, O: OrderState> LaneKernel<Indexed<R::From, Unit>, Multiple<O>>
    for SelectRelationOperation<R>
{
    type Output = ElementsOperand<R::To, Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, R::From, Unit, Multiple<O>>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let targets: GrHashSet<_> = values
            .map(|(from, membership)| {
                membership.and_then(|()| R::resolve(&prepared, graphrecord, from))
            })
            .collect::<QueryResult<_>>()?;

        Ok(Box::new(targets.into_iter().map(|target| (target, Ok(())))))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        let relation = relation_estimate::<R>(input, stats);

        Estimate {
            elements: relation.distinct,
            distinct: relation.distinct,
            selectivity: None,
            per_group: None,
        }
    }
}

impl<R: Relation> LaneKernel<Indexed<R::From, Unit>, Single> for SelectRelationOperation<R> {
    type Output = ElementOperand<R::To>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, R::From, Unit, Single>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let Some((from, membership)) = value else {
            return Ok(None);
        };
        membership?;

        let target = R::resolve(&prepared, graphrecord, from)?;

        Ok(Some((target, Ok(()))))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            elements: input.elements,
            distinct: input.elements,
            selectivity: None,
            per_group: None,
        }
    }
}

impl<R: Relation> LaneKernel<Indexed<R::From, Unit>, Definite> for SelectRelationOperation<R> {
    type Output = DefiniteElementOperand<R::To>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, R::From, Unit, Definite>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let (from, membership) = value;
        membership?;

        let target = R::resolve(&prepared, graphrecord, from)?;

        Ok((target, Ok(())))
    }

    fn estimate(&self, _input: Estimate, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl<R: Relation, I: IndexDomain> ElementKernel<Indexed<I, EntityReference<R::From>>>
    for RelationOperation<R>
{
    type Emission = Preserving;
    type OutShape = Indexed<I, EntityReference<R::To>>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, EntityReference<R::From>>, Self>> {
        Ok(Pipeline::unkeyed(move |reference: QueryResult<_>| {
            reference.and_then(|entity| R::resolve(&prepared, graphrecord, entity))
        }))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        relation_estimate::<R>(input, stats)
    }
}
