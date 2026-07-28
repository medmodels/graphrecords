use crate::{
    Bare, ExpandedIndex, ExpandedIndexOwned, Explain, Failure, IndexDomain, IndexValue, Indexed,
    Labeled, NoChildIndex, Operand, QueryResult,
    element::{Pipeline, Preserving},
    execution::EvaluationCache,
    operations::{Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    traits::{ChildIndex, ParentIndex},
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "ParentIndex")]
#[plan(optimizer_hints(empty = if_any))]
pub struct ParentIndexOperation;

impl Prepare for ParentIndexOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I: IndexDomain, P: IndexDomain, C: IndexDomain>
    ElementKernel<Indexed<I, IndexValue<ExpandedIndex<P, C>>>> for ParentIndexOperation
{
    type Emission = Preserving;
    type OutShape = Indexed<I, IndexValue<P>>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, IndexValue<ExpandedIndex<P, C>>>, Self>> {
        Ok(Pipeline::unkeyed(
            |outcome: QueryResult<ExpandedIndexOwned<_, _>>| {
                outcome.map(|address| address.into_parts().0)
            },
        ))
    }
}

impl<P: IndexDomain, C: IndexDomain> ElementKernel<Bare<IndexValue<ExpandedIndex<P, C>>>>
    for ParentIndexOperation
{
    type Emission = Preserving;
    type OutShape = Bare<IndexValue<P>>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<IndexValue<ExpandedIndex<P, C>>>, Self>> {
        Ok(Pipeline::new(
            |outcome: QueryResult<ExpandedIndexOwned<_, _>>| {
                outcome.map(|address| address.into_parts().0)
            },
        ))
    }
}

impl<O: Apply<ParentIndexOperation>> ParentIndex for O {
    type ReturnOperand = O::Output;

    fn parent_index(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), ParentIndexOperation))
    }
}

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "ChildIndex")]
#[plan(optimizer_hints(empty = if_any))]
pub struct ChildIndexOperation;

impl Prepare for ChildIndexOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I: IndexDomain, P: IndexDomain, C: IndexDomain>
    ElementKernel<Indexed<I, IndexValue<ExpandedIndex<P, C>>>> for ChildIndexOperation
{
    type Emission = Preserving;
    type OutShape = Indexed<I, IndexValue<C>>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, IndexValue<ExpandedIndex<P, C>>>, Self>> {
        Ok(Pipeline::keyed(
            |lane_index, outcome: QueryResult<ExpandedIndexOwned<_, _>>| {
                let (parent, child) = outcome?.into_parts();

                child.ok_or_else(|| {
                    Failure::new_at::<I, _>(
                        Self::LABEL,
                        NoChildIndex::<P, C>::new(parent),
                        &lane_index,
                    )
                })
            },
        ))
    }
}

impl<P: IndexDomain, C: IndexDomain> ElementKernel<Bare<IndexValue<ExpandedIndex<P, C>>>>
    for ChildIndexOperation
{
    type Emission = Preserving;
    type OutShape = Bare<IndexValue<C>>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<IndexValue<ExpandedIndex<P, C>>>, Self>> {
        Ok(Pipeline::new(
            |outcome: QueryResult<ExpandedIndexOwned<_, _>>| {
                let (parent, child) = outcome?.into_parts();

                child.ok_or_else(|| Failure::new(Self::LABEL, NoChildIndex::<P, C>::new(parent)))
            },
        ))
    }
}

impl<O: Apply<ChildIndexOperation>> ChildIndex for O {
    type ReturnOperand = O::Output;

    fn child_index(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), ChildIndexOperation))
    }
}
