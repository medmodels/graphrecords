use crate::{
    Bare, Definite, EvaluateOperand, Explain, IndexDomain, Indexed, Multiple, Operand, OrderState,
    QueryResult, Scalar, Single,
    execution::EvaluationCache,
    operands::{
        BareValueOperand, BareValuesOperand, DefiniteValueOperand, ValueOperand, ValuesOperand,
    },
    operations::{
        Apply, BareStream, ErrorPolicy, Kernel, KeyedStream, Operation, OperationContext, Prepare,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "Raise")]
pub struct Raise;

impl Prepare for Raise {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I: IndexDomain, O: OrderState> Kernel<Indexed<I, Scalar>, Multiple<O>> for Raise {
    type Output = ValuesOperand<I, O>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, Scalar, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let raised: Vec<_> = values
            .map(|(index, result)| result.map(|value| (index, value)))
            .collect::<QueryResult<_>>()?;

        Ok(Box::new(
            raised.into_iter().map(|(index, value)| (index, Ok(value))),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<O: OrderState> Kernel<Bare<Scalar>, Multiple<O>> for Raise {
    type Output = BareValuesOperand<O>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, Scalar, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let raised: Vec<_> = values.collect::<QueryResult<_>>()?;

        Ok(Box::new(raised.into_iter().map(Ok)))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<I: IndexDomain> Kernel<Indexed<I, Scalar>, Single> for Raise {
    type Output = ValueOperand<I>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, Scalar, Single>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        match value {
            Some((index, result)) => Ok(Some((index, Ok(result?)))),
            None => Ok(None),
        }
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl Kernel<Bare<Scalar>, Single> for Raise {
    type Output = BareValueOperand;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: BareStream<'a, Scalar, Single>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        match value {
            Some(result) => Ok(Some(Ok(result?))),
            None => Ok(None),
        }
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl Kernel<Bare<Scalar>, Definite> for Raise {
    type Output = DefiniteValueOperand;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: BareStream<'a, Scalar, Definite>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(Ok(value?))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<I> ErrorPolicy<I> for Raise
where
    I: Apply<Self>,
{
    type Output = <I as Apply<Self>>::Output;

    fn build(&self, input: I) -> Self::Output {
        Self::Output::new(OperationContext::new(input, Self))
    }
}
