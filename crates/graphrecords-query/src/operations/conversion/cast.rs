use crate::{
    Bare, BareValueDomain, Explain, IndexDomain, Indexed, Labeled, Operand, QueryResult,
    capabilities::ValueCast,
    cast::{Bool, CastTarget, DateTime, Duration, Float, Int, String},
    element::{Pipeline, Preserving},
    execution::EvaluationCache,
    operations::{Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Cast,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "Cast")]
#[plan(optimizer_hints(allows_limit_pushdown, empty = if_any))]
pub struct CastOperation<T: CastTarget> {
    #[explain(label)]
    target: T,
}

impl<T: CastTarget> Prepare for CastOperation<T> {
    type Prepared<'a>
        = &'a T
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(&self.target)
    }
}

impl<I, V, T> ElementKernel<Indexed<I, V>> for CastOperation<T>
where
    I: IndexDomain,
    V: ValueCast<T>,
    T: CastTarget,
{
    type Emission = Preserving;
    type OutShape = Indexed<I, V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(Pipeline::keyed(move |index, outcome: QueryResult<_>| {
            outcome.and_then(|value| {
                V::cast(Self::LABEL, value, prepared).map_err(|failure| failure.at::<I>(&index))
            })
        }))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<V, T> ElementKernel<Bare<V>> for CastOperation<T>
where
    V: ValueCast<T> + BareValueDomain,
    T: CastTarget,
{
    type Emission = Preserving;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(Pipeline::new(move |outcome: QueryResult<_>| {
            outcome.and_then(|value| V::cast(Self::LABEL, value, prepared))
        }))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<O, T> Cast<T> for O
where
    CastOperation<T>: Operation,
    O: Apply<CastOperation<T>>,
    T: CastTarget,
{
    type ReturnOperand = O::Output;

    fn cast(&self, target: T) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            CastOperation { target },
        ))
    }
}

pub(super) mod bool {
    use super::{Bare, Bool, Cast, CastOperation, Indexed, Preserving, operation_manifest};

    operation_manifest! {
        CastOperation<Bool> {
            method: Cast<Bool>::cast;
            scope: element;

            kernel {
                parameters: <I: IndexDomain, V: ValueCast<Bool>,>;
                selector: Bool;
                input: Indexed<I, V>;
                output: Indexed<I, V>;
                emission: Preserving;
            }

            kernel {
                parameters: <V: ValueCast<Bool> + BareValueDomain>;
                selector: Bool;
                input: Bare<V>;
                output: Bare<V>;
                emission: Preserving;
            }
        }
    }
}

pub(super) mod date_time {
    use super::{Bare, Cast, CastOperation, DateTime, Indexed, Preserving, operation_manifest};

    operation_manifest! {
        CastOperation<DateTime> {
            method: Cast<DateTime>::cast;
            scope: element;

            kernel {
                parameters: <I: IndexDomain, V: ValueCast<DateTime>,>;
                selector: DateTime;
                input: Indexed<I, V>;
                output: Indexed<I, V>;
                emission: Preserving;
            }

            kernel {
                parameters: <V: ValueCast<DateTime> + BareValueDomain>;
                selector: DateTime;
                input: Bare<V>;
                output: Bare<V>;
                emission: Preserving;
            }
        }
    }
}

pub(super) mod duration {
    use super::{Bare, Cast, CastOperation, Duration, Indexed, Preserving, operation_manifest};

    operation_manifest! {
        CastOperation<Duration> {
            method: Cast<Duration>::cast;
            scope: element;

            kernel {
                parameters: <I: IndexDomain, V: ValueCast<Duration>,>;
                selector: Duration;
                input: Indexed<I, V>;
                output: Indexed<I, V>;
                emission: Preserving;
            }

            kernel {
                parameters: <V: ValueCast<Duration> + BareValueDomain>;
                selector: Duration;
                input: Bare<V>;
                output: Bare<V>;
                emission: Preserving;
            }
        }
    }
}

pub(super) mod float {
    use super::{Bare, Cast, CastOperation, Float, Indexed, Preserving, operation_manifest};

    operation_manifest! {
        CastOperation<Float> {
            method: Cast<Float>::cast;
            scope: element;

            kernel {
                parameters: <I: IndexDomain, V: ValueCast<Float>,>;
                selector: Float;
                input: Indexed<I, V>;
                output: Indexed<I, V>;
                emission: Preserving;
            }

            kernel {
                parameters: <V: ValueCast<Float> + BareValueDomain>;
                selector: Float;
                input: Bare<V>;
                output: Bare<V>;
                emission: Preserving;
            }
        }
    }
}

pub(super) mod int {
    use super::{Bare, Cast, CastOperation, Indexed, Int, Preserving, operation_manifest};

    operation_manifest! {
        CastOperation<Int> {
            method: Cast<Int>::cast;
            scope: element;

            kernel {
                parameters: <I: IndexDomain, V: ValueCast<Int>,>;
                selector: Int;
                input: Indexed<I, V>;
                output: Indexed<I, V>;
                emission: Preserving;
            }

            kernel {
                parameters: <V: ValueCast<Int> + BareValueDomain>;
                selector: Int;
                input: Bare<V>;
                output: Bare<V>;
                emission: Preserving;
            }
        }
    }
}

pub(super) mod string {
    use super::{Bare, Cast, CastOperation, Indexed, Preserving, String, operation_manifest};

    operation_manifest! {
        CastOperation<String> {
            method: Cast<String>::cast;
            scope: element;

            kernel {
                parameters: <I: IndexDomain, V: ValueCast<String>,>;
                selector: String;
                input: Indexed<I, V>;
                output: Indexed<I, V>;
                emission: Preserving;
            }

            kernel {
                parameters: <V: ValueCast<String> + BareValueDomain>;
                selector: String;
                input: Bare<V>;
                output: Bare<V>;
                emission: Preserving;
            }
        }
    }
}
