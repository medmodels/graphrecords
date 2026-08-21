use crate::{
    Bare, BareValueDomain, EvaluateExpression, Explain, IndexDomain, Indexed, Multiple, OrderState,
    QueryResult,
    capabilities::ValueEquivalence,
    expressions::DefiniteBareValueExpression,
    operations::{BareStream, Build, KeyedStream, LaneKernel, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::UniqueCount,
};
use graphrecords_core::{GraphRecord, graphrecord::ValueView};
use graphrecords_utils::aliases::GrHashSet;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Lane)]
#[explain(label = "UniqueCount")]
pub struct UniqueCountOperation;

impl<I: IndexDomain, V: ValueEquivalence, O: OrderState> LaneKernel<Indexed<I, V>, Multiple<O>>
    for UniqueCountOperation
{
    type Output = DefiniteBareValueExpression;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: KeyedStream<'a, I, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let count = values.try_fold(
            (GrHashSet::default(), 0),
            |(mut unique, count), (_, value)| {
                let value = value?;
                let inserted = unique.insert(V::equivalence_key(&value));

                Ok((unique, count + i64::from(inserted)))
            },
        );

        Ok(count.map(|(_, count)| ValueView::Int(count)))
    }

    fn estimate(&self, _input: Estimate, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl<V: ValueEquivalence + BareValueDomain, O: OrderState> LaneKernel<Bare<V>, Multiple<O>>
    for UniqueCountOperation
{
    type Output = DefiniteBareValueExpression;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let count = values.try_fold((GrHashSet::default(), 0), |(mut unique, count), value| {
            let value = value?;
            let inserted = unique.insert(V::equivalence_key(&value));

            Ok((unique, count + i64::from(inserted)))
        });

        Ok(count.map(|(_, count)| ValueView::Int(count)))
    }

    fn estimate(&self, _input: Estimate, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl<E: Build<UniqueCountOperation>> UniqueCount for E {
    type Output = E::Output;

    fn n_unique(&self) -> Self::Output {
        self.build(UniqueCountOperation)
    }
}

operation_manifest! {
    UniqueCountOperation {
        method: UniqueCount::n_unique;
        scope: lane;

        kernel {
            parameters: <
                I: IndexDomain,
                V: ValueEquivalence,
                O: OrderState,
            >;
            input: (Indexed<I, V>, Multiple<O>);
            output: DefiniteBareValueExpression;
        }

        kernel {
            parameters: <
                V: ValueEquivalence + BareValueDomain,
                O: OrderState,
            >;
            input: (Bare<V>, Multiple<O>);
            output: DefiniteBareValueExpression;
        }
    }
}
