use crate::{
    EntityReference, Explain, IndexDomain, Indexed, Labeled, Mask, Operand, QueryResult, Unit,
    element::{Pipeline, Preserving},
    execution::EvaluationCache,
    index::IndicesInGroup,
    operations::{Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::InGroup,
};
use graphrecords_core::{GraphRecord, graphrecord::Group};

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "InGroup")]
#[plan(optimizer_hints(commutes_with_filter, allows_limit_pushdown, empty = if_any))]
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
    type Emission = Preserving;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, Unit>, Self>> {
        let members = I::indices_in_group(Self::LABEL, graphrecord, prepared)?;

        Ok(Pipeline::keyed(move |index, membership: QueryResult<_>| {
            membership.map(|()| members.contains(&index))
        }))
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
    type Emission = Preserving;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, EntityReference<E>>, Self>> {
        let members = E::indices_in_group(Self::LABEL, graphrecord, prepared)?;

        Ok(Pipeline::unkeyed(move |reference: QueryResult<_>| {
            reference.map(|entity| members.contains(&entity))
        }))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            selectivity: None,
            ..input
        }
    }
}

impl<O: Apply<InGroupOperation>> InGroup for O {
    type ReturnOperand = O::Output;

    fn in_group(&self, group: Group) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            InGroupOperation { group },
        ))
    }
}

operation_manifest! {
    InGroupOperation {
        method: InGroup::in_group;
        scope: element;

        kernel {
            parameters: <I: IndicesInGroup>;
            field: group: Group;
            input: Indexed<I, Unit>;
            output: Indexed<I, Mask>;
            emission: Preserving;
        }

        kernel {
            parameters: <E: IndicesInGroup, I: IndexDomain>;
            field: group: Group;
            input: Indexed<I, EntityReference<E>>;
            output: Indexed<I, Mask>;
            emission: Preserving;
        }
    }
}
