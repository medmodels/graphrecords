use crate::{
    Explain, Operand, QueryResult,
    execution::EvaluationCache,
    operations::{Apply, Operation, OperationContext, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    traits::Modulo,
};
use graphrecords_core::GraphRecord;

mod values;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "Modulo")]
pub struct ModuloOperation<A> {
    #[argument]
    argument: A,
}

impl<A: Prepare> Prepare for ModuloOperation<A> {
    type Prepared<'a>
        = A::Prepared<'a>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.argument.prepare(graphrecord, cache)
    }
}

impl<O, A> Modulo<A> for O
where
    ModuloOperation<A>: Operation,
    O: Apply<ModuloOperation<A>>,
{
    type Output = <O as Apply<ModuloOperation<A>>>::Output;

    fn modulo(&self, argument: A) -> Self::Output {
        Self::Output::new(OperationContext::new(
            self.clone(),
            ModuloOperation { argument },
        ))
    }
}
