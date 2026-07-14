use super::ModuloOperation;
use crate::{
    Failure, IndexDomain, Indexed, Labeled, Operand, OrderState, QueryResult, Scalar,
    operands::ValuesOperand,
    operations::{ArgumentSource, ElementKernel, Keyed, OnMissing, Operation, Pipeline},
    optimizer::{EstimateCost, Stats, ValueCost},
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{GraphRecordValue, datatypes::Mod},
};

impl<I, A> ElementKernel<Indexed<I, Scalar>> for ModuloOperation<A>
where
    I: IndexDomain,
    for<'a> A: ArgumentSource<Keyed<I>, Value<'a> = GraphRecordValue>,
{
    type OutShape = Indexed<I, Scalar>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<
        Pipeline<
            'a,
            (I::Index<'a>, QueryResult<GraphRecordValue>),
            (I::Index<'a>, QueryResult<GraphRecordValue>),
        >,
    > {
        let label = Self::LABEL;

        Ok(Pipeline::default().filter_map(
            move |(index, item): (I::Index<'a>, QueryResult<GraphRecordValue>)| {
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
            },
        ))
    }
}

impl<I: IndexDomain, A, O: OrderState> EstimateCost<ModuloOperation<A>> for ValuesOperand<I, O>
where
    for<'a> A: ArgumentSource<Keyed<I>, Value<'a> = GraphRecordValue>,
    ModuloOperation<A>: Operation,
{
    type OutputCost = <Self as Operand>::Cost;

    fn estimate(
        _operation: &ModuloOperation<A>,
        input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        ValueCost::unknown(input_cost.rows())
    }
}

#[cfg(test)]
mod test {
    use crate::{
        Ordered,
        operands::ValuesOperand,
        operations::{Apply, ModuloOperation},
    };
    use graphrecords_core::graphrecord::{GraphRecordValue, NodeIndex};

    #[test]
    fn test_modulo_auto_lifts_onto_sorted_operand() {
        fn assert_applies()
        where
            ValuesOperand<NodeIndex, Ordered>: Apply<ModuloOperation<GraphRecordValue>>,
        {
        }

        assert_applies();
    }
}
