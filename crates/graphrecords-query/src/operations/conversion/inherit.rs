use crate::{
    ExpandedIndex, ExpandedIndexAddress, Explain, IndexDomain, Indexed, Labeled, QueryResult,
    ValueDomain,
    element::{Pipeline, Retention},
    operations::{
        ArgumentSource, Build, ElementKernel, ElementPipeline, Keyed, Operation, Prepare,
    },
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    registry::{describe::ArgumentRetention, operation_manifest},
    traits::Inherit,
};
use graphrecords_core::GraphRecord;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Element)]
#[explain(label = "Inherit")]
#[plan(optimizer_hints(empty = if_all))]
pub struct InheritOperation<S> {
    #[argument]
    values: S,
}

impl<P: IndexDomain, C: IndexDomain, W: ValueDomain, S: ArgumentSource<Keyed<P>>>
    ElementKernel<Indexed<ExpandedIndex<P, C>, W>> for InheritOperation<S>
{
    type Emission = S::Retention;
    type OutShape = Indexed<ExpandedIndex<P, C>, S::ValueDomain>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<ExpandedIndex<P, C>, W>, Self>> {
        Ok(Pipeline::keyed(
            move |address: ExpandedIndexAddress<P, C>, template_outcome| match template_outcome {
                Err(failure) => Self::Emission::keep(Err(failure)),
                Ok(_) => S::resolve(graphrecord, &prepared, address.parent_index(), Self::LABEL),
            },
        ))
    }
}

impl<E, S> Inherit<S> for E
where
    InheritOperation<S>: Operation,
    E: Build<InheritOperation<S>>,
{
    type Output = E::Output;

    fn inherit(&self, values: S) -> Self::Output {
        self.build(InheritOperation { values })
    }
}

operation_manifest! {
    InheritOperation<S> {
        method: Inherit<S>::inherit;
        scope: element;

        kernel {
            parameters: <P: IndexDomain, C: IndexDomain, W: ValueDomain, X: ValueDomain>;
            argument: S: ArgumentSource<Keyed<P>, X>;
            input: Indexed<ExpandedIndex<P, C>, W>;
            output: Indexed<ExpandedIndex<P, C>, X>;
            emission: ArgumentRetention;
        }
    }
}
