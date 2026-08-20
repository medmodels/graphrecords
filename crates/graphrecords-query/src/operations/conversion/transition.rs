use crate::{
    Bare, BareValueDomain, Explain, Expression, FailureKind, FailureKindValue, IndexDomain,
    IndexValue, Indexed, Labeled, Mask, Positional, QueryResult, Scalar, Series, ValueDomain,
    capabilities::ValueTransition,
    element::{Pipeline, Preserving},
    explain::ExplainFormatter,
    operations::{Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Transition,
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{AttributeName, Group, NodeIndex, Value},
};
use std::{
    any::type_name,
    fmt::{self, Write},
    marker::PhantomData,
};

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare)]
#[operation(scope = Element)]
#[plan(optimizer_hints(allows_limit_pushdown, empty = if_any))]
pub struct TransitionOperation<T: ValueDomain> {
    marker: PhantomData<fn() -> T>,
}

impl<T: ValueDomain> TransitionOperation<T> {
    const fn new() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<T: ValueDomain> Clone for TransitionOperation<T> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<T: ValueDomain> Labeled for TransitionOperation<T> {
    const LABEL: &'static str = "Transition";
}

impl<T: ValueDomain> Explain for TransitionOperation<T> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{} target={}", Self::LABEL, type_name::<T>())
    }
}

impl<I: IndexDomain, S: ValueTransition<T>, T: ValueDomain> ElementKernel<Indexed<I, S>>
    for TransitionOperation<T>
{
    type Emission = Preserving;
    type OutShape = Indexed<I, T>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, S>, Self>> {
        Ok(Pipeline::keyed(move |address, outcome: QueryResult<_>| {
            outcome.and_then(|value| {
                S::transition(value, Self::LABEL)
                    .map_err(|failure| failure.at_address::<I>(graphrecord, &address))
            })
        }))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<S: ValueTransition<T> + BareValueDomain, T: BareValueDomain> ElementKernel<Bare<S>>
    for TransitionOperation<T>
{
    type Emission = Preserving;
    type OutShape = Bare<T>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<S>, Self>> {
        Ok(Pipeline::new(|outcome: QueryResult<_>| {
            outcome.and_then(|value| S::transition(value, Self::LABEL))
        }))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<E: Expression> Transition for E {
    type Expression = E;
    type Output<T>
        = E::Output
    where
        T: ValueDomain,
        E: Apply<TransitionOperation<T>>;

    fn transition<T>(&self) -> Self::Output<T>
    where
        T: ValueDomain,
        Self: Apply<TransitionOperation<T>>,
    {
        Self::Output::new(OperationContext::new(
            self.clone(),
            TransitionOperation::new(),
        ))
    }
}

impl<E: Expression> Transition for Series<E> {
    type Expression = E;
    type Output<T>
        = Series<E::Output>
    where
        T: ValueDomain,
        E: Apply<TransitionOperation<T>>;

    fn transition<T>(&self) -> Self::Output<T>
    where
        T: ValueDomain,
        E: Apply<TransitionOperation<T>>,
    {
        self.bind(self.expression().transition())
    }
}

pub(super) mod attribute_name {
    use super::{
        AttributeName, Bare, Indexed, Preserving, Transition, TransitionOperation,
        operation_manifest,
    };

    operation_manifest! {
        TransitionOperation<AttributeName> {
            method: Transition::transition;
            scope: element;

            kernel {
                parameters: <I: IndexDomain, S: ValueTransition<AttributeName>,>;
                selector: AttributeName;
                input: Indexed<I, S>;
                output: Indexed<I, AttributeName>;
                emission: Preserving;
            }

            kernel {
                parameters: <S: ValueTransition<AttributeName> + BareValueDomain>;
                selector: AttributeName;
                input: Bare<S>;
                output: Bare<AttributeName>;
                emission: Preserving;
            }
        }
    }
}

pub(super) mod attribute_name_index {
    use super::{
        AttributeName, Bare, IndexValue, Indexed, Preserving, Transition, TransitionOperation,
        operation_manifest,
    };

    operation_manifest! {
        TransitionOperation<IndexValue<AttributeName>> {
            method: Transition::transition;
            scope: element;

            kernel {
                parameters: <I: IndexDomain, S: ValueTransition<(IndexValue<AttributeName>)>,>;
                selector: IndexValue<AttributeName>;
                input: Indexed<I, S>;
                output: Indexed<I, IndexValue<AttributeName>>;
                emission: Preserving;
            }

            kernel {
                parameters: <S: ValueTransition<(IndexValue<AttributeName>)> + BareValueDomain>;
                selector: IndexValue<AttributeName>;
                input: Bare<S>;
                output: Bare<IndexValue<AttributeName>>;
                emission: Preserving;
            }
        }
    }
}

pub(super) mod bool_index {
    use super::{
        Bare, IndexValue, Indexed, Preserving, Transition, TransitionOperation, operation_manifest,
    };

    operation_manifest! {
        TransitionOperation<IndexValue<bool>> {
            method: Transition::transition;
            scope: element;

            kernel {
                parameters: <I: IndexDomain, S: ValueTransition<(IndexValue<bool>)>,>;
                selector: IndexValue<bool>;
                input: Indexed<I, S>;
                output: Indexed<I, IndexValue<bool>>;
                emission: Preserving;
            }

            kernel {
                parameters: <S: ValueTransition<(IndexValue<bool>)> + BareValueDomain>;
                selector: IndexValue<bool>;
                input: Bare<S>;
                output: Bare<IndexValue<bool>>;
                emission: Preserving;
            }
        }
    }
}

pub(super) mod failure_kind_index {
    use super::{
        Bare, FailureKind, IndexValue, Indexed, Preserving, Transition, TransitionOperation,
        operation_manifest,
    };

    operation_manifest! {
        TransitionOperation<IndexValue<FailureKind>> {
            method: Transition::transition;
            scope: element;

            kernel {
                parameters: <I: IndexDomain, S: ValueTransition<(IndexValue<FailureKind>)>,>;
                selector: IndexValue<FailureKind>;
                input: Indexed<I, S>;
                output: Indexed<I, IndexValue<FailureKind>>;
                emission: Preserving;
            }

            kernel {
                parameters: <S: ValueTransition<(IndexValue<FailureKind>)> + BareValueDomain>;
                selector: IndexValue<FailureKind>;
                input: Bare<S>;
                output: Bare<IndexValue<FailureKind>>;
                emission: Preserving;
            }
        }
    }
}

pub(super) mod failure_kind_value {
    use super::{
        Bare, FailureKindValue, Indexed, Preserving, Transition, TransitionOperation,
        operation_manifest,
    };

    operation_manifest! {
        TransitionOperation<FailureKindValue> {
            method: Transition::transition;
            scope: element;

            kernel {
                parameters: <I: IndexDomain, S: ValueTransition<FailureKindValue>,>;
                selector: FailureKindValue;
                input: Indexed<I, S>;
                output: Indexed<I, FailureKindValue>;
                emission: Preserving;
            }

            kernel {
                parameters: <S: ValueTransition<FailureKindValue> + BareValueDomain>;
                selector: FailureKindValue;
                input: Bare<S>;
                output: Bare<FailureKindValue>;
                emission: Preserving;
            }
        }
    }
}

pub(super) mod group_index {
    use super::{
        Bare, Group, IndexValue, Indexed, Preserving, Transition, TransitionOperation,
        operation_manifest,
    };

    operation_manifest! {
        TransitionOperation<IndexValue<Group>> {
            method: Transition::transition;
            scope: element;

            kernel {
                parameters: <I: IndexDomain, S: ValueTransition<(IndexValue<Group>)>,>;
                selector: IndexValue<Group>;
                input: Indexed<I, S>;
                output: Indexed<I, IndexValue<Group>>;
                emission: Preserving;
            }

            kernel {
                parameters: <S: ValueTransition<(IndexValue<Group>)> + BareValueDomain>;
                selector: IndexValue<Group>;
                input: Bare<S>;
                output: Bare<IndexValue<Group>>;
                emission: Preserving;
            }
        }
    }
}

pub(super) mod mask {
    use super::{
        Bare, Indexed, Mask, Preserving, Transition, TransitionOperation, operation_manifest,
    };

    operation_manifest! {
        TransitionOperation<Mask> {
            method: Transition::transition;
            scope: element;

            kernel {
                parameters: <I: IndexDomain, S: ValueTransition<Mask>,>;
                selector: Mask;
                input: Indexed<I, S>;
                output: Indexed<I, Mask>;
                emission: Preserving;
            }

            kernel {
                parameters: <S: ValueTransition<Mask> + BareValueDomain>;
                selector: Mask;
                input: Bare<S>;
                output: Bare<Mask>;
                emission: Preserving;
            }
        }
    }
}

pub(super) mod node_index {
    use super::{
        Bare, IndexValue, Indexed, NodeIndex, Preserving, Transition, TransitionOperation,
        operation_manifest,
    };

    operation_manifest! {
        TransitionOperation<IndexValue<NodeIndex>> {
            method: Transition::transition;
            scope: element;

            kernel {
                parameters: <I: IndexDomain, S: ValueTransition<(IndexValue<NodeIndex>)>,>;
                selector: IndexValue<NodeIndex>;
                input: Indexed<I, S>;
                output: Indexed<I, IndexValue<NodeIndex>>;
                emission: Preserving;
            }

            kernel {
                parameters: <S: ValueTransition<(IndexValue<NodeIndex>)> + BareValueDomain>;
                selector: IndexValue<NodeIndex>;
                input: Bare<S>;
                output: Bare<IndexValue<NodeIndex>>;
                emission: Preserving;
            }
        }
    }
}

pub(super) mod positional_index {
    use super::{
        Bare, IndexValue, Indexed, Positional, Preserving, Transition, TransitionOperation,
        operation_manifest,
    };

    operation_manifest! {
        TransitionOperation<IndexValue<Positional>> {
            method: Transition::transition;
            scope: element;

            kernel {
                parameters: <I: IndexDomain, S: ValueTransition<(IndexValue<Positional>)>,>;
                selector: IndexValue<Positional>;
                input: Indexed<I, S>;
                output: Indexed<I, IndexValue<Positional>>;
                emission: Preserving;
            }

            kernel {
                parameters: <S: ValueTransition<(IndexValue<Positional>)> + BareValueDomain>;
                selector: IndexValue<Positional>;
                input: Bare<S>;
                output: Bare<IndexValue<Positional>>;
                emission: Preserving;
            }
        }
    }
}

pub(super) mod scalar {
    use super::{
        Bare, Indexed, Preserving, Scalar, Transition, TransitionOperation, operation_manifest,
    };

    operation_manifest! {
        TransitionOperation<Scalar> {
            method: Transition::transition;
            scope: element;

            kernel {
                parameters: <I: IndexDomain, S: ValueTransition<Scalar>,>;
                selector: Scalar;
                input: Indexed<I, S>;
                output: Indexed<I, Scalar>;
                emission: Preserving;
            }

            kernel {
                parameters: <S: ValueTransition<Scalar> + BareValueDomain>;
                selector: Scalar;
                input: Bare<S>;
                output: Bare<Scalar>;
                emission: Preserving;
            }
        }
    }
}

pub(super) mod value_index {
    use super::{
        Bare, IndexValue, Indexed, Preserving, Transition, TransitionOperation, Value,
        operation_manifest,
    };

    operation_manifest! {
        TransitionOperation<IndexValue<Value>> {
            method: Transition::transition;
            scope: element;

            kernel {
                parameters: <I: IndexDomain, S: ValueTransition<(IndexValue<Value>)>,>;
                selector: IndexValue<Value>;
                input: Indexed<I, S>;
                output: Indexed<I, IndexValue<Value>>;
                emission: Preserving;
            }

            kernel {
                parameters: <S: ValueTransition<(IndexValue<Value>)> + BareValueDomain>;
                selector: IndexValue<Value>;
                input: Bare<S>;
                output: Bare<IndexValue<Value>>;
                emission: Preserving;
            }
        }
    }
}
