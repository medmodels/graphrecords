use crate::{
    AttributeName, Bare, EvaluateOperand, Explain, Failure, IndexDomain, IndexValue, Indexed,
    Labeled, Multiple, Operand, OrderState, Positional, QueryResult, Scalar,
    execution::EvaluationCache,
    operands::{BareAttributeOperand, BareIndexOperand, BareValueOperand},
    operations::{
        Apply, BareStream, KeyedStream, LaneKernel, Operation, OperationContext, Prepare,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Sum,
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{EdgeIndex, GraphRecordAttribute, GraphRecordValue, NodeIndex},
};
use std::ops::Add;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "Sum")]
pub struct SumOperation;

impl Prepare for SumOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I: IndexDomain, O: OrderState> LaneKernel<Indexed<I, Scalar>, Multiple<O>> for SumOperation {
    type Output = BareValueOperand;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: KeyedStream<'a, I, Scalar, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let sum = values.try_fold(None, |sum: Option<GraphRecordValue>, (index, value)| {
            let value = value?;

            match sum {
                Some(sum) => sum
                    .add(value)
                    .map(Some)
                    .map_err(|error| Failure::new_at::<I, _>(Self::LABEL, error, &index)),
                None => Ok(Some(value)),
            }
        });

        Ok(sum.transpose())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<O: OrderState> LaneKernel<Bare<Scalar>, Multiple<O>> for SumOperation {
    type Output = BareValueOperand;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: BareStream<'a, Scalar, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let sum = values.try_fold(None, |sum: Option<GraphRecordValue>, value| {
            let value = value?;

            match sum {
                Some(sum) => sum
                    .add(value)
                    .map(Some)
                    .map_err(|error| Failure::new(Self::LABEL, error)),
                None => Ok(Some(value)),
            }
        });

        Ok(sum.transpose())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<I: IndexDomain, O: OrderState> LaneKernel<Indexed<I, AttributeName>, Multiple<O>>
    for SumOperation
{
    type Output = BareAttributeOperand;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: KeyedStream<'a, I, AttributeName, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let sum = values.try_fold(None, |sum: Option<GraphRecordAttribute>, (index, value)| {
            let value = value?;

            match sum {
                Some(sum) => sum
                    .add(value)
                    .map(Some)
                    .map_err(|error| Failure::new_at::<I, _>(Self::LABEL, error, &index)),
                None => Ok(Some(value)),
            }
        });

        Ok(sum.transpose())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<O: OrderState> LaneKernel<Bare<AttributeName>, Multiple<O>> for SumOperation {
    type Output = BareAttributeOperand;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: BareStream<'a, AttributeName, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let sum = values.try_fold(None, |sum: Option<GraphRecordAttribute>, value| {
            let value = value?;

            match sum {
                Some(sum) => sum
                    .add(value)
                    .map(Some)
                    .map_err(|error| Failure::new(Self::LABEL, error)),
                None => Ok(Some(value)),
            }
        });

        Ok(sum.transpose())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<I: IndexDomain, O: OrderState> LaneKernel<Indexed<I, IndexValue<Positional>>, Multiple<O>>
    for SumOperation
{
    type Output = BareIndexOperand<Positional>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: KeyedStream<'a, I, IndexValue<Positional>, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let sum = values.try_fold(None, |sum, (_, value)| {
            let value = value?;

            match sum {
                Some(sum) => Ok(Some(sum + value)),
                None => Ok(Some(value)),
            }
        });

        Ok(sum.transpose())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<O: OrderState> LaneKernel<Bare<IndexValue<Positional>>, Multiple<O>> for SumOperation {
    type Output = BareIndexOperand<Positional>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: BareStream<'a, IndexValue<Positional>, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let sum = values.try_fold(None, |sum, value| {
            let value = value?;

            match sum {
                Some(sum) => Ok(Some(sum + value)),
                None => Ok(Some(value)),
            }
        });

        Ok(sum.transpose())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<I: IndexDomain, O: OrderState> LaneKernel<Indexed<I, IndexValue<NodeIndex>>, Multiple<O>>
    for SumOperation
{
    type Output = BareIndexOperand<NodeIndex>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: KeyedStream<'a, I, IndexValue<NodeIndex>, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let sum = values.try_fold(None, |sum: Option<NodeIndex>, (index, value)| {
            let value = value?;

            match sum {
                Some(sum) => sum
                    .add(value)
                    .map(Some)
                    .map_err(|error| Failure::new_at::<I, _>(Self::LABEL, error, &index)),
                None => Ok(Some(value)),
            }
        });

        Ok(sum.transpose())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<O: OrderState> LaneKernel<Bare<IndexValue<NodeIndex>>, Multiple<O>> for SumOperation {
    type Output = BareIndexOperand<NodeIndex>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: BareStream<'a, IndexValue<NodeIndex>, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let sum = values.try_fold(None, |sum: Option<NodeIndex>, value| {
            let value = value?;

            match sum {
                Some(sum) => sum
                    .add(value)
                    .map(Some)
                    .map_err(|error| Failure::new(Self::LABEL, error)),
                None => Ok(Some(value)),
            }
        });

        Ok(sum.transpose())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<I: IndexDomain, O: OrderState> LaneKernel<Indexed<I, IndexValue<EdgeIndex>>, Multiple<O>>
    for SumOperation
{
    type Output = BareIndexOperand<EdgeIndex>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: KeyedStream<'a, I, IndexValue<EdgeIndex>, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let sum = values.try_fold(None, |sum, (_, value)| {
            let value = value?;

            match sum {
                Some(sum) => Ok(Some(sum + value)),
                None => Ok(Some(value)),
            }
        });

        Ok(sum.transpose())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<O: OrderState> LaneKernel<Bare<IndexValue<EdgeIndex>>, Multiple<O>> for SumOperation {
    type Output = BareIndexOperand<EdgeIndex>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: BareStream<'a, IndexValue<EdgeIndex>, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let sum = values.try_fold(None, |sum, value| {
            let value = value?;

            match sum {
                Some(sum) => Ok(Some(sum + value)),
                None => Ok(Some(value)),
            }
        });

        Ok(sum.transpose())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<I: IndexDomain, O: OrderState>
    LaneKernel<Indexed<I, IndexValue<GraphRecordValue>>, Multiple<O>> for SumOperation
{
    type Output = BareIndexOperand<GraphRecordValue>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: KeyedStream<'a, I, IndexValue<GraphRecordValue>, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let sum = values.try_fold(None, |sum: Option<GraphRecordValue>, (index, value)| {
            let value = value?;

            match sum {
                Some(sum) => sum
                    .add(value)
                    .map(Some)
                    .map_err(|error| Failure::new_at::<I, _>(Self::LABEL, error, &index)),
                None => Ok(Some(value)),
            }
        });

        Ok(sum.transpose())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<O: OrderState> LaneKernel<Bare<IndexValue<GraphRecordValue>>, Multiple<O>> for SumOperation {
    type Output = BareIndexOperand<GraphRecordValue>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: BareStream<'a, IndexValue<GraphRecordValue>, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let sum = values.try_fold(None, |sum: Option<GraphRecordValue>, value| {
            let value = value?;

            match sum {
                Some(sum) => sum
                    .add(value)
                    .map(Some)
                    .map_err(|error| Failure::new(Self::LABEL, error)),
                None => Ok(Some(value)),
            }
        });

        Ok(sum.transpose())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<O: Apply<SumOperation>> Sum for O {
    type ReturnOperand = O::Output;

    fn sum(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), SumOperation))
    }
}
