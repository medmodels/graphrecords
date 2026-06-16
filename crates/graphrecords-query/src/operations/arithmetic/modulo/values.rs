use super::ModuloOperation;
use crate::{
    Failure, IndexDomain, Indexed, Labeled, Operand, QueryResult, Scalar,
    operands::ValuesOperand,
    operations::{ArgumentSource, ElementKernel, Keyed, OnMissing, Operation, Pipeline},
    optimizer::{EstimateCost, Stats},
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{GraphRecordValue, datatypes::Mod},
};

impl<I, A> ElementKernel<Indexed<I, Scalar>> for ModuloOperation<A>
where
    I: IndexDomain,
    A: ArgumentSource<Keyed<I>, Value = GraphRecordValue>,
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

impl<I: IndexDomain, A> EstimateCost<ModuloOperation<A>> for ValuesOperand<I>
where
    A: ArgumentSource<Keyed<I>, Value = GraphRecordValue>,
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

#[cfg(test)]
mod test {
    use crate::{
        Indexed, Multiple, Ordered, Scalar,
        operands::OperandHandle,
        operations::{Apply, ModuloOperation},
    };
    use graphrecords_core::graphrecord::{GraphRecordValue, NodeIndex};

    #[test]
    fn test_modulo_auto_lifts_onto_ordered_operand() {
        fn assert_applies()
        where
            OperandHandle<Ordered<Indexed<NodeIndex, Scalar>>, Multiple>:
                Apply<ModuloOperation<GraphRecordValue>>,
        {
        }

        assert_applies();
    }
}
