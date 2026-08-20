use crate::{
    Bare, ExpandedIndex, ExpandedIndexReference, Explain, Failure, IndexDomain, IndexValue,
    Indexed, Labeled, QueryResult,
    element::{Pipeline, Preserving},
    error::index::NoChildIndex,
    operations::{Build, ElementKernel, ElementPipeline, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::{ChildIndex, ParentIndex},
};
use graphrecords_core::GraphRecord;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Element)]
#[explain(label = "ParentIndex")]
#[plan(optimizer_hints(commutes_with_filter, allows_limit_pushdown, empty = if_any))]
pub struct ParentIndexOperation;

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
            |outcome: QueryResult<ExpandedIndexReference<'a, P, C>>| {
                outcome.map(|index| index.parent_index().clone())
            },
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
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
            |outcome: QueryResult<ExpandedIndexReference<'a, P, C>>| {
                outcome.map(|index| index.parent_index().clone())
            },
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<E: Build<ParentIndexOperation>> ParentIndex for E {
    type Output = E::Output;

    fn parent_index(&self) -> Self::Output {
        self.build(ParentIndexOperation)
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

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Element)]
#[explain(label = "ChildIndex")]
#[plan(optimizer_hints(allows_limit_pushdown, empty = if_any))]
pub struct ChildIndexOperation;

impl<I: IndexDomain, P: IndexDomain, C: IndexDomain>
    ElementKernel<Indexed<I, IndexValue<ExpandedIndex<P, C>>>> for ChildIndexOperation
{
    type Emission = Preserving;
    type OutShape = Indexed<I, IndexValue<C>>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, IndexValue<ExpandedIndex<P, C>>>, Self>> {
        Ok(Pipeline::keyed(
            move |address, outcome: QueryResult<ExpandedIndexReference<'a, P, C>>| {
                let index = outcome?;

                index.child_index().cloned().ok_or_else(|| {
                    Failure::new_at_address::<I, _>(
                        NoChildIndex::<P>::new(P::own_index(index.parent_index())),
                        graphrecord,
                        &address,
                        Self::LABEL,
                    )
                })
            },
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
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
            |outcome: QueryResult<ExpandedIndexReference<'a, P, C>>| {
                let index = outcome?;

                index.child_index().cloned().ok_or_else(|| {
                    Failure::new(
                        NoChildIndex::<P>::new(P::own_index(index.parent_index())),
                        Self::LABEL,
                    )
                })
            },
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<E: Build<ChildIndexOperation>> ChildIndex for E {
    type Output = E::Output;

    fn child_index(&self) -> Self::Output {
        self.build(ChildIndexOperation)
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
