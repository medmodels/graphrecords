use super::{numeric_bare, numeric_indexed};
use crate::{
    Bare, BareValueDomain, Explain, IndexDomain, Indexed, Labeled, QueryResult,
    capabilities::ValueSquareRoot,
    element::Preserving,
    operations::{Build, ElementKernel, ElementPipeline, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::SquareRoot,
};
use graphrecords_core::GraphRecord;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Element)]
#[explain(label = "SquareRoot")]
#[plan(optimizer_hints(allows_limit_pushdown, empty = if_any))]
pub struct SquareRootOperation;

impl<I: IndexDomain, V: ValueSquareRoot> ElementKernel<Indexed<I, V>> for SquareRootOperation {
    type Emission = Preserving;
    type OutShape = Indexed<I, V>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(numeric_indexed::<I, V>(
            graphrecord,
            V::square_root,
            Self::LABEL,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<V: ValueSquareRoot + BareValueDomain> ElementKernel<Bare<V>> for SquareRootOperation {
    type Emission = Preserving;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(numeric_bare::<V>(V::square_root, Self::LABEL))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<E: Build<SquareRootOperation>> SquareRoot for E {
    type Output = E::Output;

    fn sqrt(&self) -> Self::Output {
        self.build(SquareRootOperation)
    }
}

operation_manifest! {
    SquareRootOperation {
        method: SquareRoot::sqrt;
        scope: element;

        kernel {
            parameters: <I: IndexDomain, V: ValueSquareRoot>;
            input: Indexed<I, V>;
            output: Indexed<I, V>;
            emission: Preserving;
        }

        kernel {
            parameters: <V: ValueSquareRoot + BareValueDomain>;
            input: Bare<V>;
            output: Bare<V>;
            emission: Preserving;
        }
    }
}
