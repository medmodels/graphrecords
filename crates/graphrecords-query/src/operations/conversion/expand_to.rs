use crate::{
    ExpandedIndex, ExpandedIndexReference, Explain, IndexDomain, Indexed, Labeled, Operand,
    QueryResult, ValueDomain,
    element::{Pipeline, Retention},
    execution::EvaluationCache,
    operations::{
        Apply, ArgumentSource, ElementKernel, ElementPipeline, Keyed, Operation, OperationContext,
        Prepare,
    },
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    registry::{describe::ArgumentRetention, operation_manifest},
    traits::ExpandTo,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "ExpandTo")]
#[plan(optimizer_hints(empty = if_all))]
pub struct ExpandToOperation<S> {
    #[argument]
    parent: S,
}

impl<S: Prepare> Prepare for ExpandToOperation<S> {
    type Prepared<'a>
        = S::Prepared<'a>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.parent.prepare(graphrecord, cache)
    }
}

impl<P, C, W, S> ElementKernel<Indexed<ExpandedIndex<P, C>, W>> for ExpandToOperation<S>
where
    P: IndexDomain,
    C: IndexDomain,
    W: ValueDomain,
    S: ArgumentSource<Keyed<P>> + Clone,
{
    type Emission = S::Retention;
    type OutShape = Indexed<ExpandedIndex<P, C>, S::ValueDomain>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<ExpandedIndex<P, C>, W>, Self>> {
        Ok(Pipeline::keyed(
            move |address: ExpandedIndexReference<'_, _, _>, template_outcome| {
                match template_outcome {
                    Err(failure) => Self::Emission::keep(Err(failure)),
                    Ok(_) => S::resolve(&prepared, address.parent_index(), Self::LABEL),
                }
            },
        ))
    }
}

impl<S, T> ExpandTo<T> for S
where
    S: Clone,
    T: Apply<ExpandToOperation<S>>,
    ExpandToOperation<S>: Operation,
{
    type ReturnOperand = T::Output;

    fn expand_to(&self, template: &T) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            template.clone(),
            ExpandToOperation {
                parent: self.clone(),
            },
        ))
    }
}

operation_manifest! {
    ExpandToOperation<S> {
        method: ExpandTo::expand_to;
        scope: element;

        kernel {
            parameters: <P: IndexDomain, C: IndexDomain, W: ValueDomain, X: ValueDomain>;
            argument: S: ArgumentSource<Keyed<P>, X>;
            receiver: S;
            input: Indexed<ExpandedIndex<P, C>, W>;
            output: Indexed<ExpandedIndex<P, C>, X>;
            emission: ArgumentRetention;
        }
    }
}
