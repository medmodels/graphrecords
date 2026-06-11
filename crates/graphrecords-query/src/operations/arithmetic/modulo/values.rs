use super::ModuloOperation;
use crate::{
    BoxedIterator, EvaluateOperand, Failure, IndexDomain, Indexed, Labeled, Multiple, Operand,
    QueryResult, Scalar,
    operands::ValuesOperand,
    operations::{ArgumentSource, Kernel, KeyedStream, OnMissing, Operation},
    optimizer::{EstimateCost, Stats},
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{GraphRecordValue, datatypes::Mod},
};

fn modulo_values<'a, I, A>(
    values: BoxedIterator<'a, (I::Index<'a>, QueryResult<GraphRecordValue>)>,
    prepared: A::Prepared<'a>,
) -> BoxedIterator<'a, (I::Index<'a>, QueryResult<GraphRecordValue>)>
where
    I: IndexDomain,
    A: ArgumentSource<I, Value = GraphRecordValue>,
    A::Prepared<'a>: 'a,
{
    let label = <ModuloOperation<A> as Labeled>::LABEL;

    Box::new(values.filter_map(move |(index, item)| {
        let value = match item {
            Ok(value) => value,
            Err(original) => return Some((index, Err(original))),
        };

        let modulus = match A::resolve(&prepared, &index, label, OnMissing::Raise) {
            Ok(Some(modulus)) => modulus,
            Ok(None) => return None,
            Err(failure) => return Some((index, Err(failure))),
        };

        let result = value
            .r#mod(modulus)
            .map_err(|error| Failure::new(label, error).at(&index));

        Some((index, result))
    }))
}

impl<I, A> Kernel<Indexed<I, Scalar>, Multiple> for ModuloOperation<A>
where
    I: IndexDomain,
    A: ArgumentSource<I, Value = GraphRecordValue>,
{
    type Output = ValuesOperand<I>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, Scalar, Multiple>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(modulo_values::<I, A>(values, prepared))
    }
}

impl<I: IndexDomain, A> EstimateCost<ModuloOperation<A>> for ValuesOperand<I>
where
    A: ArgumentSource<I, Value = GraphRecordValue>,
    ModuloOperation<A>: Operation,
{
    type OutputCost = <Self as Operand>::Cost;

    fn estimate(
        _operation: &ModuloOperation<A>,
        input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        input_cost
    }
}
