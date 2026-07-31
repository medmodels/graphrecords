use crate::{
    Bare, ExpandedIndex, ExpandedIndexOwned, Explain, Failure, IndexDomain, IndexValue, Indexed,
    Labeled, Operand, QueryResult,
    element::{Pipeline, Preserving},
    error::index::NoChildIndex,
    execution::EvaluationCache,
    operations::{Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    registry::operation_manifest,
    traits::{ChildIndex, ParentIndex},
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "ParentIndex")]
#[plan(optimizer_hints(commutes_with_filter, allows_limit_pushdown, empty = if_any))]
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

pub(super) mod parent_index {
    use super::{
        Bare, ExpandedIndex, IndexValue, Indexed, ParentIndex, ParentIndexOperation, Preserving,
        operation_manifest,
    };

    operation_manifest! {
        ParentIndexOperation {
            method: ParentIndex::parent_index;
            scope: element;

            kernel {
                parameters: <I: IndexDomain, P: IndexDomain, C: IndexDomain>;
                input: Indexed<I, IndexValue<ExpandedIndex<P, C>>>;
                output: Indexed<I, IndexValue<P>>;
                emission: Preserving;
            }
            kernel {
                parameters: <P: IndexDomain, C: IndexDomain>;
                input: Bare<IndexValue<ExpandedIndex<P, C>>>;
                output: Bare<IndexValue<P>>;
                emission: Preserving;
            }
        }
    }
}

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "ChildIndex")]
#[plan(optimizer_hints(allows_limit_pushdown, empty = if_any))]
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
                        NoChildIndex::<P>::new(parent),
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

                child.ok_or_else(|| Failure::new(Self::LABEL, NoChildIndex::<P>::new(parent)))
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

pub(super) mod child_index {
    use super::{
        Bare, ChildIndex, ChildIndexOperation, ExpandedIndex, IndexValue, Indexed, Preserving,
        operation_manifest,
    };

    operation_manifest! {
        ChildIndexOperation {
            method: ChildIndex::child_index;
            scope: element;

            kernel {
                parameters: <I: IndexDomain, P: IndexDomain, C: IndexDomain>;
                input: Indexed<I, IndexValue<ExpandedIndex<P, C>>>;
                output: Indexed<I, IndexValue<C>>;
                emission: Preserving;
            }
            kernel {
                parameters: <P: IndexDomain, C: IndexDomain>;
                input: Bare<IndexValue<ExpandedIndex<P, C>>>;
                output: Bare<IndexValue<C>>;
                emission: Preserving;
            }
        }
    }
}
