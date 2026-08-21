use crate::{
    Bare, BareValueDomain, Definite, EvaluateExpression, Explain, IndexDomain, Indexed, Multiple,
    OrderState, QueryResult, Single, ValueDomain,
    expressions::DefiniteBareValueExpression,
    operations::{BareStream, Build, KeyedStream, LaneKernel, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Count,
};
use graphrecords_core::{GraphRecord, graphrecord::ValueView};

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Lane)]
#[explain(label = "Count")]
pub struct CountOperation;

impl<I: IndexDomain, V: ValueDomain, O: OrderState> LaneKernel<Indexed<I, V>, Multiple<O>>
    for CountOperation
{
    type Output = DefiniteBareValueExpression;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: KeyedStream<'a, I, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let count = values.try_fold(0_i64, |count, (_, item)| item.map(|_| count + 1));

        Ok(count.map(ValueView::Int))
    }

    fn estimate(&self, _input: Estimate, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl<V: BareValueDomain, O: OrderState> LaneKernel<Bare<V>, Multiple<O>> for CountOperation {
    type Output = DefiniteBareValueExpression;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let count = values.try_fold(0_i64, |count, item| item.map(|_| count + 1));

        Ok(count.map(ValueView::Int))
    }

    fn estimate(&self, _input: Estimate, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl<I: IndexDomain, V: ValueDomain> LaneKernel<Indexed<I, V>, Single> for CountOperation {
    type Output = DefiniteBareValueExpression;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, V, Single>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let count = match value {
            Some((_, item)) => item.map(|_| 1_i64),
            None => Ok(0_i64),
        };

        Ok(count.map(ValueView::Int))
    }

    fn estimate(&self, _input: Estimate, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl<V: BareValueDomain> LaneKernel<Bare<V>, Single> for CountOperation {
    type Output = DefiniteBareValueExpression;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: BareStream<'a, V, Single>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let count = match value {
            Some(item) => item.map(|_| 1_i64),
            None => Ok(0_i64),
        };

        Ok(count.map(ValueView::Int))
    }

    fn estimate(&self, _input: Estimate, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl<I: IndexDomain, V: ValueDomain> LaneKernel<Indexed<I, V>, Definite> for CountOperation {
    type Output = DefiniteBareValueExpression;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, V, Definite>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        Ok(value.1.map(|_| ValueView::Int(1)))
    }

    fn estimate(&self, _input: Estimate, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl<V: BareValueDomain> LaneKernel<Bare<V>, Definite> for CountOperation {
    type Output = DefiniteBareValueExpression;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: BareStream<'a, V, Definite>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        Ok(value.map(|_| ValueView::Int(1)))
    }

    fn estimate(&self, _input: Estimate, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl<E: Build<CountOperation>> Count for E {
    type Output = E::Output;

    fn count(&self) -> Self::Output {
        self.build(CountOperation)
    }
}

operation_manifest! {
    CountOperation {
        method: Count::count;
        scope: lane;

        kernel {
            parameters: <
                I: IndexDomain,
                V: ValueDomain,
                O: OrderState,
            >;
            input: (Indexed<I, V>, Multiple<O>);
            output: DefiniteBareValueExpression;
        }

        kernel {
            parameters: <
                V: BareValueDomain,
                O: OrderState,
            >;
            input: (Bare<V>, Multiple<O>);
            output: DefiniteBareValueExpression;
        }

        kernel {
            parameters: <
                I: IndexDomain,
                V: ValueDomain,
            >;
            input: (Indexed<I, V>, Single);
            output: DefiniteBareValueExpression;
        }

        kernel {
            parameters: <V: BareValueDomain>;
            input: (Bare<V>, Single);
            output: DefiniteBareValueExpression;
        }

        kernel {
            parameters: <
                I: IndexDomain,
                V: ValueDomain,
            >;
            input: (Indexed<I, V>, Definite);
            output: DefiniteBareValueExpression;
        }

        kernel {
            parameters: <V: BareValueDomain>;
            input: (Bare<V>, Definite);
            output: DefiniteBareValueExpression;
        }
    }
}
