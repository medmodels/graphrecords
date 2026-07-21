use crate::{
    Explain, IndexDomain, IndexValue, Indexed, Operand, QueryResult, Unit,
    execution::EvaluationCache,
    operations::{
        Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Pipeline, Prepare,
        Preserving,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Index,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "Index")]
#[plan(optimizer_hints(distinct))]
pub struct IndexOperation;

impl Prepare for IndexOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I: IndexDomain> ElementKernel<Indexed<I, Unit>> for IndexOperation {
    type OutShape = Indexed<I, IndexValue<I>>;
    type Retention = Preserving;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, Unit>, Self>> {
        Ok(
            Pipeline::default().map(|(index, membership): (I::Index<'a>, QueryResult<()>)| {
                let promoted = membership.map(|()| index.clone());

                (index, promoted)
            }),
        )
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: input.elements,
            ..input
        }
    }
}

impl<O> Index for O
where
    O: Apply<IndexOperation>,
{
    type ReturnOperand = <O as Apply<IndexOperation>>::Output;

    fn index(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), IndexOperation))
    }
}
