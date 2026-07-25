use crate::{
    EntityDomain, EntityReference, Explain, Failure, IndexDomain, Indexed, Labeled, Mask, Operand,
    QueryResult, Unit,
    execution::EvaluationCache,
    operations::{
        Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Pipeline, Prepare,
        Preserving,
    },
    optimizer::{
        EdgeGroupSize, Estimate, NodeGroupSize, OperationInputs, OptimizerHints, PlanIdentity,
        PlanInputs, Stats,
    },
    traits::InGroup,
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{EdgeIndex, Group, NodeIndex},
};
use graphrecords_utils::aliases::GrHashSet;

pub trait IndicesInGroup: EntityDomain {
    fn indices_in_group<'a>(
        graphrecord: &'a GraphRecord,
        group: &Group,
    ) -> QueryResult<GrHashSet<Self::Index<'a>>>;

    fn group_size(stats: &Stats, group: &Group) -> usize;
}

impl IndicesInGroup for NodeIndex {
    fn indices_in_group<'a>(
        graphrecord: &'a GraphRecord,
        group: &Group,
    ) -> QueryResult<GrHashSet<Self::Index<'a>>> {
        Ok(graphrecord
            .nodes_in_group(group)
            .map_err(|error| Failure::new(InGroupOperation::LABEL, error))?
            .collect())
    }

    fn group_size(stats: &Stats, group: &Group) -> usize {
        stats.get::<NodeGroupSize>(group)
    }
}

impl IndicesInGroup for EdgeIndex {
    fn indices_in_group<'a>(
        graphrecord: &'a GraphRecord,
        group: &Group,
    ) -> QueryResult<GrHashSet<Self::Index<'a>>> {
        Ok(graphrecord
            .edges_in_group(group)
            .map_err(|error| Failure::new(InGroupOperation::LABEL, error))?
            .collect())
    }

    fn group_size(stats: &Stats, group: &Group) -> usize {
        stats.get::<EdgeGroupSize>(group)
    }
}

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "InGroup")]
#[plan(optimizer_hints(empty = if_any))]
pub struct InGroupOperation {
    #[explain(label)]
    group: Group,
}

impl Prepare for InGroupOperation {
    type Prepared<'a> = &'a Group;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(&self.group)
    }
}

impl<I: IndicesInGroup> ElementKernel<Indexed<I, Unit>> for InGroupOperation {
    type OutShape = Indexed<I, Mask>;
    type Retention = Preserving;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        group: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, Unit>, Self>> {
        let members = I::indices_in_group(graphrecord, group)?;

        Ok(Pipeline::default().map(
            move |(index, membership): (I::Index<'a>, QueryResult<()>)| {
                let in_group = membership.map(|()| members.contains(&index));

                (index, in_group)
            },
        ))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        let size = I::group_size(stats, &self.group);
        let selectivity = input
            .elements
            .map(|elements| size.min(elements) as f64 / elements.max(1) as f64);

        Estimate {
            selectivity,
            ..input
        }
    }
}

impl<E: IndicesInGroup, I: IndexDomain> ElementKernel<Indexed<I, EntityReference<E>>>
    for InGroupOperation
{
    type OutShape = Indexed<I, Mask>;
    type Retention = Preserving;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        group: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, EntityReference<E>>, Self>> {
        let members = E::indices_in_group(graphrecord, group)?;

        Ok(Pipeline::default().map(
            move |(key, reference): (I::Index<'a>, QueryResult<E::Index<'a>>)| {
                let in_group = reference.map(|entity| members.contains(&entity));

                (key, in_group)
            },
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            selectivity: None,
            ..input
        }
    }
}

impl<O> InGroup for O
where
    O: Apply<InGroupOperation>,
{
    type ReturnOperand = <O as Apply<InGroupOperation>>::Output;

    fn in_group(&self, group: Group) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            InGroupOperation { group },
        ))
    }
}
