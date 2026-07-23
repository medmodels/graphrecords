use crate::{
    Bare, Definite, Diagnostic, ErrorGroup, EvaluateOperand, Explain, Failure, IndexDomain,
    Indexed, Labeled, Multiple, Operand, OrderState, QueryResult, Single, ValueType,
    execution::EvaluationCache,
    explain::ExplainFormatter,
    operands::OperandHandle,
    operations::{
        Apply, ArgumentSource, BareStream, ErrorPolicy, ErrorPolicyIn, ErrorPolicyOf,
        ErrorPolicyWithCause, Kernel, KeyedStream, Operation, OperationContext, Prepare,
        Preserving, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
};
use graphrecords_core::GraphRecord;
use std::{
    any::type_name,
    error::Error,
    fmt::{self, Write},
    marker::PhantomData,
};

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "Raise")]
pub struct Raise;

#[derive(Clone, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
pub struct RaiseWhen<C> {
    #[argument]
    condition: C,
}

impl<C: Explain> Explain for RaiseWhen<C> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        formatter.write_str("RaiseWhen")?;
        formatter.labeled_child("condition", &self.condition);

        Ok(())
    }
}

impl Raise {
    #[must_use]
    pub const fn when<C>(self, condition: C) -> RaiseWhen<C> {
        RaiseWhen { condition }
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
pub struct RaiseErrorsOf<D: Diagnostic> {
    marker: PhantomData<fn() -> D>,
}

impl<D: Diagnostic> RaiseErrorsOf<D> {
    const fn new() -> Self {
        Self {
            marker: PhantomData,
        }
    }

    fn matches(failure: &Failure) -> bool {
        failure.is_kind::<D>()
    }
}

impl<D: Diagnostic> Clone for RaiseErrorsOf<D> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<D: Diagnostic> Explain for RaiseErrorsOf<D> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "RaiseErrorsOf kind={}", D::name())
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
pub struct RaiseErrorsIn<G: ErrorGroup> {
    marker: PhantomData<fn() -> G>,
}

impl<G: ErrorGroup> RaiseErrorsIn<G> {
    const fn new() -> Self {
        Self {
            marker: PhantomData,
        }
    }

    fn matches(failure: &Failure) -> bool {
        G::contains(&failure.kind())
    }
}

impl<G: ErrorGroup> Clone for RaiseErrorsIn<G> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<G: ErrorGroup> Explain for RaiseErrorsIn<G> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "RaiseErrorsIn group={}", G::name())
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
pub struct RaiseErrorsWithCause<E: Error + 'static> {
    marker: PhantomData<fn() -> E>,
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
pub struct RaiseWhenErrorsOf<D: Diagnostic, C> {
    #[argument]
    condition: C,
    marker: PhantomData<fn() -> D>,
}

impl<D: Diagnostic, C> RaiseWhenErrorsOf<D, C> {
    const fn new(condition: C) -> Self {
        Self {
            condition,
            marker: PhantomData,
        }
    }
}

impl<D: Diagnostic, C: Clone> Clone for RaiseWhenErrorsOf<D, C> {
    fn clone(&self) -> Self {
        Self::new(self.condition.clone())
    }
}

impl<D: Diagnostic, C: Explain> Explain for RaiseWhenErrorsOf<D, C> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "RaiseWhenErrorsOf kind={}", D::name())?;
        formatter.labeled_child("condition", &self.condition);

        Ok(())
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
pub struct RaiseWhenErrorsIn<G: ErrorGroup, C> {
    #[argument]
    condition: C,
    marker: PhantomData<fn() -> G>,
}

impl<G: ErrorGroup, C> RaiseWhenErrorsIn<G, C> {
    const fn new(condition: C) -> Self {
        Self {
            condition,
            marker: PhantomData,
        }
    }
}

impl<G: ErrorGroup, C: Clone> Clone for RaiseWhenErrorsIn<G, C> {
    fn clone(&self) -> Self {
        Self::new(self.condition.clone())
    }
}

impl<G: ErrorGroup, C: Explain> Explain for RaiseWhenErrorsIn<G, C> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "RaiseWhenErrorsIn group={}", G::name())?;
        formatter.labeled_child("condition", &self.condition);

        Ok(())
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
pub struct RaiseWhenErrorsWithCause<E: Error + 'static, C> {
    #[argument]
    condition: C,
    marker: PhantomData<fn() -> E>,
}

impl<E: Error + 'static, C> RaiseWhenErrorsWithCause<E, C> {
    const fn new(condition: C) -> Self {
        Self {
            condition,
            marker: PhantomData,
        }
    }
}

impl<E: Error + 'static, C: Clone> Clone for RaiseWhenErrorsWithCause<E, C> {
    fn clone(&self) -> Self {
        Self::new(self.condition.clone())
    }
}

impl<E: Error + 'static, C: Explain> Explain for RaiseWhenErrorsWithCause<E, C> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(
            formatter,
            "RaiseWhenErrorsWithCause cause={}",
            type_name::<E>()
        )?;
        formatter.labeled_child("condition", &self.condition);

        Ok(())
    }
}

impl<E: Error + 'static> RaiseErrorsWithCause<E> {
    const fn new() -> Self {
        Self {
            marker: PhantomData,
        }
    }

    fn matches(failure: &Failure) -> bool {
        failure.has_cause::<E>()
    }
}

impl<E: Error + 'static> Clone for RaiseErrorsWithCause<E> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<E: Error + 'static> Explain for RaiseErrorsWithCause<E> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "RaiseErrorsWithCause cause={}", type_name::<E>())
    }
}

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

impl<C> Prepare for RaiseWhen<C>
where
    for<'a> C: ArgumentSource<Unaligned, Retention = Preserving, Value<'a> = bool>,
{
    type Prepared<'a> = QueryResult<bool>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        let condition = self.condition.prepare(graphrecord, cache)?;

        Ok(C::resolve(&condition, &(), Raise::LABEL))
    }
}

impl<D: Diagnostic> Prepare for RaiseErrorsOf<D> {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<G: ErrorGroup> Prepare for RaiseErrorsIn<G> {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<E: Error + 'static> Prepare for RaiseErrorsWithCause<E> {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<D: Diagnostic, C> Prepare for RaiseWhenErrorsOf<D, C>
where
    for<'a> C: ArgumentSource<Unaligned, Retention = Preserving, Value<'a> = bool>,
{
    type Prepared<'a> = QueryResult<bool>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        let condition = self.condition.prepare(graphrecord, cache)?;

        Ok(C::resolve(&condition, &(), Raise::LABEL))
    }
}

impl<G: ErrorGroup, C> Prepare for RaiseWhenErrorsIn<G, C>
where
    for<'a> C: ArgumentSource<Unaligned, Retention = Preserving, Value<'a> = bool>,
{
    type Prepared<'a> = QueryResult<bool>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        let condition = self.condition.prepare(graphrecord, cache)?;

        Ok(C::resolve(&condition, &(), Raise::LABEL))
    }
}

impl<E: Error + 'static, C> Prepare for RaiseWhenErrorsWithCause<E, C>
where
    for<'a> C: ArgumentSource<Unaligned, Retention = Preserving, Value<'a> = bool>,
{
    type Prepared<'a> = QueryResult<bool>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        let condition = self.condition.prepare(graphrecord, cache)?;

        Ok(C::resolve(&condition, &(), Raise::LABEL))
    }
}

impl<I: IndexDomain, V: ValueType, O: OrderState> Kernel<Indexed<I, V>, Multiple<O>> for Raise {
    type Output = OperandHandle<Indexed<I, V>, Multiple<O>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
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

impl<V: ValueType, O: OrderState> Kernel<Bare<V>, Multiple<O>> for Raise {
    type Output = OperandHandle<Bare<V>, Multiple<O>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let raised: Vec<_> = values.collect::<QueryResult<_>>()?;

        Ok(Box::new(raised.into_iter().map(Ok)))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<I: IndexDomain, V: ValueType> Kernel<Indexed<I, V>, Single> for Raise {
    type Output = OperandHandle<Indexed<I, V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, V, Single>,
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

impl<V: ValueType> Kernel<Bare<V>, Single> for Raise {
    type Output = OperandHandle<Bare<V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: BareStream<'a, V, Single>,
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

impl<I: IndexDomain, V: ValueType> Kernel<Indexed<I, V>, Definite> for Raise {
    type Output = OperandHandle<Indexed<I, V>, Definite>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, V, Definite>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let (index, result) = value;

        Ok((index, Ok(result?)))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<V: ValueType> Kernel<Bare<V>, Definite> for Raise {
    type Output = OperandHandle<Bare<V>, Definite>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: BareStream<'a, V, Definite>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(Ok(value?))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<I: IndexDomain, V: ValueType, O: OrderState, C> Kernel<Indexed<I, V>, Multiple<O>>
    for RaiseWhen<C>
where
    for<'a> C: ArgumentSource<Unaligned, Retention = Preserving, Value<'a> = bool>,
{
    type Output = OperandHandle<Indexed<I, V>, Multiple<O>>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        if condition? {
            <Raise as Kernel<Indexed<I, V>, Multiple<O>>>::execute(graphrecord, values, ())
        } else {
            Ok(values)
        }
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<V: ValueType, O: OrderState, C> Kernel<Bare<V>, Multiple<O>> for RaiseWhen<C>
where
    for<'a> C: ArgumentSource<Unaligned, Retention = Preserving, Value<'a> = bool>,
{
    type Output = OperandHandle<Bare<V>, Multiple<O>>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        if condition? {
            <Raise as Kernel<Bare<V>, Multiple<O>>>::execute(graphrecord, values, ())
        } else {
            Ok(values)
        }
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<I: IndexDomain, V: ValueType, C> Kernel<Indexed<I, V>, Single> for RaiseWhen<C>
where
    for<'a> C: ArgumentSource<Unaligned, Retention = Preserving, Value<'a> = bool>,
{
    type Output = OperandHandle<Indexed<I, V>, Single>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, V, Single>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        if condition? {
            <Raise as Kernel<Indexed<I, V>, Single>>::execute(graphrecord, value, ())
        } else {
            Ok(value)
        }
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<V: ValueType, C> Kernel<Bare<V>, Single> for RaiseWhen<C>
where
    for<'a> C: ArgumentSource<Unaligned, Retention = Preserving, Value<'a> = bool>,
{
    type Output = OperandHandle<Bare<V>, Single>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: BareStream<'a, V, Single>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        if condition? {
            <Raise as Kernel<Bare<V>, Single>>::execute(graphrecord, value, ())
        } else {
            Ok(value)
        }
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<I: IndexDomain, V: ValueType, C> Kernel<Indexed<I, V>, Definite> for RaiseWhen<C>
where
    for<'a> C: ArgumentSource<Unaligned, Retention = Preserving, Value<'a> = bool>,
{
    type Output = OperandHandle<Indexed<I, V>, Definite>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, V, Definite>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        if condition? {
            <Raise as Kernel<Indexed<I, V>, Definite>>::execute(graphrecord, value, ())
        } else {
            Ok(value)
        }
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<V: ValueType, C> Kernel<Bare<V>, Definite> for RaiseWhen<C>
where
    for<'a> C: ArgumentSource<Unaligned, Retention = Preserving, Value<'a> = bool>,
{
    type Output = OperandHandle<Bare<V>, Definite>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: BareStream<'a, V, Definite>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        if condition? {
            <Raise as Kernel<Bare<V>, Definite>>::execute(graphrecord, value, ())
        } else {
            Ok(value)
        }
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<I: IndexDomain, V: ValueType, O: OrderState, D: Diagnostic> Kernel<Indexed<I, V>, Multiple<O>>
    for RaiseErrorsOf<D>
{
    type Output = OperandHandle<Indexed<I, V>, Multiple<O>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let values = values
            .map(|element| match element {
                (_index, Err(failure)) if Self::matches(&failure) => Err(failure),
                element => Ok(element),
            })
            .collect::<QueryResult<Vec<_>>>()?;

        Ok(Box::new(values.into_iter()))
    }
}

impl<V: ValueType, O: OrderState, D: Diagnostic> Kernel<Bare<V>, Multiple<O>> for RaiseErrorsOf<D> {
    type Output = OperandHandle<Bare<V>, Multiple<O>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let values = values
            .map(|result| match result {
                Err(failure) if Self::matches(&failure) => Err(failure),
                result => Ok(result),
            })
            .collect::<QueryResult<Vec<_>>>()?;

        Ok(Box::new(values.into_iter()))
    }
}

impl<I: IndexDomain, V: ValueType, D: Diagnostic> Kernel<Indexed<I, V>, Single>
    for RaiseErrorsOf<D>
{
    type Output = OperandHandle<Indexed<I, V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, V, Single>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        match value {
            Some((_index, Err(failure))) if Self::matches(&failure) => Err(failure),
            value => Ok(value),
        }
    }
}

impl<V: ValueType, D: Diagnostic> Kernel<Bare<V>, Single> for RaiseErrorsOf<D> {
    type Output = OperandHandle<Bare<V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: BareStream<'a, V, Single>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        match value {
            Some(Err(failure)) if Self::matches(&failure) => Err(failure),
            value => Ok(value),
        }
    }
}

impl<I: IndexDomain, V: ValueType, D: Diagnostic> Kernel<Indexed<I, V>, Definite>
    for RaiseErrorsOf<D>
{
    type Output = OperandHandle<Indexed<I, V>, Definite>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, V, Definite>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        match value {
            (_index, Err(failure)) if Self::matches(&failure) => Err(failure),
            value => Ok(value),
        }
    }
}

impl<V: ValueType, D: Diagnostic> Kernel<Bare<V>, Definite> for RaiseErrorsOf<D> {
    type Output = OperandHandle<Bare<V>, Definite>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: BareStream<'a, V, Definite>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        match value {
            Err(failure) if Self::matches(&failure) => Err(failure),
            value => Ok(value),
        }
    }
}

impl<I: IndexDomain, V: ValueType, O: OrderState, G: ErrorGroup> Kernel<Indexed<I, V>, Multiple<O>>
    for RaiseErrorsIn<G>
{
    type Output = OperandHandle<Indexed<I, V>, Multiple<O>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let values = values
            .map(|element| match element {
                (_index, Err(failure)) if Self::matches(&failure) => Err(failure),
                element => Ok(element),
            })
            .collect::<QueryResult<Vec<_>>>()?;

        Ok(Box::new(values.into_iter()))
    }
}

impl<V: ValueType, O: OrderState, G: ErrorGroup> Kernel<Bare<V>, Multiple<O>> for RaiseErrorsIn<G> {
    type Output = OperandHandle<Bare<V>, Multiple<O>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let values = values
            .map(|result| match result {
                Err(failure) if Self::matches(&failure) => Err(failure),
                result => Ok(result),
            })
            .collect::<QueryResult<Vec<_>>>()?;

        Ok(Box::new(values.into_iter()))
    }
}

impl<I: IndexDomain, V: ValueType, G: ErrorGroup> Kernel<Indexed<I, V>, Single>
    for RaiseErrorsIn<G>
{
    type Output = OperandHandle<Indexed<I, V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, V, Single>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        match value {
            Some((_index, Err(failure))) if Self::matches(&failure) => Err(failure),
            value => Ok(value),
        }
    }
}

impl<V: ValueType, G: ErrorGroup> Kernel<Bare<V>, Single> for RaiseErrorsIn<G> {
    type Output = OperandHandle<Bare<V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: BareStream<'a, V, Single>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        match value {
            Some(Err(failure)) if Self::matches(&failure) => Err(failure),
            value => Ok(value),
        }
    }
}

impl<I: IndexDomain, V: ValueType, G: ErrorGroup> Kernel<Indexed<I, V>, Definite>
    for RaiseErrorsIn<G>
{
    type Output = OperandHandle<Indexed<I, V>, Definite>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, V, Definite>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        match value {
            (_index, Err(failure)) if Self::matches(&failure) => Err(failure),
            value => Ok(value),
        }
    }
}

impl<V: ValueType, G: ErrorGroup> Kernel<Bare<V>, Definite> for RaiseErrorsIn<G> {
    type Output = OperandHandle<Bare<V>, Definite>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: BareStream<'a, V, Definite>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        match value {
            Err(failure) if Self::matches(&failure) => Err(failure),
            value => Ok(value),
        }
    }
}

impl<I: IndexDomain, V: ValueType, O: OrderState, E: Error + 'static>
    Kernel<Indexed<I, V>, Multiple<O>> for RaiseErrorsWithCause<E>
{
    type Output = OperandHandle<Indexed<I, V>, Multiple<O>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let values = values
            .map(|element| match element {
                (_index, Err(failure)) if Self::matches(&failure) => Err(failure),
                element => Ok(element),
            })
            .collect::<QueryResult<Vec<_>>>()?;

        Ok(Box::new(values.into_iter()))
    }
}

impl<V: ValueType, O: OrderState, E: Error + 'static> Kernel<Bare<V>, Multiple<O>>
    for RaiseErrorsWithCause<E>
{
    type Output = OperandHandle<Bare<V>, Multiple<O>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let values = values
            .map(|result| match result {
                Err(failure) if Self::matches(&failure) => Err(failure),
                result => Ok(result),
            })
            .collect::<QueryResult<Vec<_>>>()?;

        Ok(Box::new(values.into_iter()))
    }
}

impl<I: IndexDomain, V: ValueType, E: Error + 'static> Kernel<Indexed<I, V>, Single>
    for RaiseErrorsWithCause<E>
{
    type Output = OperandHandle<Indexed<I, V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, V, Single>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        match value {
            Some((_index, Err(failure))) if Self::matches(&failure) => Err(failure),
            value => Ok(value),
        }
    }
}

impl<V: ValueType, E: Error + 'static> Kernel<Bare<V>, Single> for RaiseErrorsWithCause<E> {
    type Output = OperandHandle<Bare<V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: BareStream<'a, V, Single>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        match value {
            Some(Err(failure)) if Self::matches(&failure) => Err(failure),
            value => Ok(value),
        }
    }
}

impl<I: IndexDomain, V: ValueType, E: Error + 'static> Kernel<Indexed<I, V>, Definite>
    for RaiseErrorsWithCause<E>
{
    type Output = OperandHandle<Indexed<I, V>, Definite>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, V, Definite>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        match value {
            (_index, Err(failure)) if Self::matches(&failure) => Err(failure),
            value => Ok(value),
        }
    }
}

impl<V: ValueType, E: Error + 'static> Kernel<Bare<V>, Definite> for RaiseErrorsWithCause<E> {
    type Output = OperandHandle<Bare<V>, Definite>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: BareStream<'a, V, Definite>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        match value {
            Err(failure) if Self::matches(&failure) => Err(failure),
            value => Ok(value),
        }
    }
}

impl<I: IndexDomain, V: ValueType, O: OrderState, D: Diagnostic, C>
    Kernel<Indexed<I, V>, Multiple<O>> for RaiseWhenErrorsOf<D, C>
where
    for<'a> C: ArgumentSource<Unaligned, Retention = Preserving, Value<'a> = bool>,
{
    type Output = OperandHandle<Indexed<I, V>, Multiple<O>>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        if condition? {
            <RaiseErrorsOf<D> as Kernel<Indexed<I, V>, Multiple<O>>>::execute(
                graphrecord,
                values,
                (),
            )
        } else {
            Ok(values)
        }
    }
}

impl<V: ValueType, O: OrderState, D: Diagnostic, C> Kernel<Bare<V>, Multiple<O>>
    for RaiseWhenErrorsOf<D, C>
where
    for<'a> C: ArgumentSource<Unaligned, Retention = Preserving, Value<'a> = bool>,
{
    type Output = OperandHandle<Bare<V>, Multiple<O>>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        if condition? {
            <RaiseErrorsOf<D> as Kernel<Bare<V>, Multiple<O>>>::execute(graphrecord, values, ())
        } else {
            Ok(values)
        }
    }
}

impl<I: IndexDomain, V: ValueType, D: Diagnostic, C> Kernel<Indexed<I, V>, Single>
    for RaiseWhenErrorsOf<D, C>
where
    for<'a> C: ArgumentSource<Unaligned, Retention = Preserving, Value<'a> = bool>,
{
    type Output = OperandHandle<Indexed<I, V>, Single>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, V, Single>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        if condition? {
            <RaiseErrorsOf<D> as Kernel<Indexed<I, V>, Single>>::execute(graphrecord, value, ())
        } else {
            Ok(value)
        }
    }
}

impl<V: ValueType, D: Diagnostic, C> Kernel<Bare<V>, Single> for RaiseWhenErrorsOf<D, C>
where
    for<'a> C: ArgumentSource<Unaligned, Retention = Preserving, Value<'a> = bool>,
{
    type Output = OperandHandle<Bare<V>, Single>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: BareStream<'a, V, Single>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        if condition? {
            <RaiseErrorsOf<D> as Kernel<Bare<V>, Single>>::execute(graphrecord, value, ())
        } else {
            Ok(value)
        }
    }
}

impl<I: IndexDomain, V: ValueType, D: Diagnostic, C> Kernel<Indexed<I, V>, Definite>
    for RaiseWhenErrorsOf<D, C>
where
    for<'a> C: ArgumentSource<Unaligned, Retention = Preserving, Value<'a> = bool>,
{
    type Output = OperandHandle<Indexed<I, V>, Definite>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, V, Definite>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        if condition? {
            <RaiseErrorsOf<D> as Kernel<Indexed<I, V>, Definite>>::execute(graphrecord, value, ())
        } else {
            Ok(value)
        }
    }
}

impl<V: ValueType, D: Diagnostic, C> Kernel<Bare<V>, Definite> for RaiseWhenErrorsOf<D, C>
where
    for<'a> C: ArgumentSource<Unaligned, Retention = Preserving, Value<'a> = bool>,
{
    type Output = OperandHandle<Bare<V>, Definite>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: BareStream<'a, V, Definite>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        if condition? {
            <RaiseErrorsOf<D> as Kernel<Bare<V>, Definite>>::execute(graphrecord, value, ())
        } else {
            Ok(value)
        }
    }
}

impl<I: IndexDomain, V: ValueType, O: OrderState, G: ErrorGroup, C>
    Kernel<Indexed<I, V>, Multiple<O>> for RaiseWhenErrorsIn<G, C>
where
    for<'a> C: ArgumentSource<Unaligned, Retention = Preserving, Value<'a> = bool>,
{
    type Output = OperandHandle<Indexed<I, V>, Multiple<O>>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        if condition? {
            <RaiseErrorsIn<G> as Kernel<Indexed<I, V>, Multiple<O>>>::execute(
                graphrecord,
                values,
                (),
            )
        } else {
            Ok(values)
        }
    }
}

impl<V: ValueType, O: OrderState, G: ErrorGroup, C> Kernel<Bare<V>, Multiple<O>>
    for RaiseWhenErrorsIn<G, C>
where
    for<'a> C: ArgumentSource<Unaligned, Retention = Preserving, Value<'a> = bool>,
{
    type Output = OperandHandle<Bare<V>, Multiple<O>>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        if condition? {
            <RaiseErrorsIn<G> as Kernel<Bare<V>, Multiple<O>>>::execute(graphrecord, values, ())
        } else {
            Ok(values)
        }
    }
}

impl<I: IndexDomain, V: ValueType, G: ErrorGroup, C> Kernel<Indexed<I, V>, Single>
    for RaiseWhenErrorsIn<G, C>
where
    for<'a> C: ArgumentSource<Unaligned, Retention = Preserving, Value<'a> = bool>,
{
    type Output = OperandHandle<Indexed<I, V>, Single>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, V, Single>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        if condition? {
            <RaiseErrorsIn<G> as Kernel<Indexed<I, V>, Single>>::execute(graphrecord, value, ())
        } else {
            Ok(value)
        }
    }
}

impl<V: ValueType, G: ErrorGroup, C> Kernel<Bare<V>, Single> for RaiseWhenErrorsIn<G, C>
where
    for<'a> C: ArgumentSource<Unaligned, Retention = Preserving, Value<'a> = bool>,
{
    type Output = OperandHandle<Bare<V>, Single>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: BareStream<'a, V, Single>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        if condition? {
            <RaiseErrorsIn<G> as Kernel<Bare<V>, Single>>::execute(graphrecord, value, ())
        } else {
            Ok(value)
        }
    }
}

impl<I: IndexDomain, V: ValueType, G: ErrorGroup, C> Kernel<Indexed<I, V>, Definite>
    for RaiseWhenErrorsIn<G, C>
where
    for<'a> C: ArgumentSource<Unaligned, Retention = Preserving, Value<'a> = bool>,
{
    type Output = OperandHandle<Indexed<I, V>, Definite>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, V, Definite>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        if condition? {
            <RaiseErrorsIn<G> as Kernel<Indexed<I, V>, Definite>>::execute(graphrecord, value, ())
        } else {
            Ok(value)
        }
    }
}

impl<V: ValueType, G: ErrorGroup, C> Kernel<Bare<V>, Definite> for RaiseWhenErrorsIn<G, C>
where
    for<'a> C: ArgumentSource<Unaligned, Retention = Preserving, Value<'a> = bool>,
{
    type Output = OperandHandle<Bare<V>, Definite>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: BareStream<'a, V, Definite>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        if condition? {
            <RaiseErrorsIn<G> as Kernel<Bare<V>, Definite>>::execute(graphrecord, value, ())
        } else {
            Ok(value)
        }
    }
}

impl<I: IndexDomain, V: ValueType, O: OrderState, E: Error + 'static, C>
    Kernel<Indexed<I, V>, Multiple<O>> for RaiseWhenErrorsWithCause<E, C>
where
    for<'a> C: ArgumentSource<Unaligned, Retention = Preserving, Value<'a> = bool>,
{
    type Output = OperandHandle<Indexed<I, V>, Multiple<O>>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        if condition? {
            <RaiseErrorsWithCause<E> as Kernel<Indexed<I, V>, Multiple<O>>>::execute(
                graphrecord,
                values,
                (),
            )
        } else {
            Ok(values)
        }
    }
}

impl<V: ValueType, O: OrderState, E: Error + 'static, C> Kernel<Bare<V>, Multiple<O>>
    for RaiseWhenErrorsWithCause<E, C>
where
    for<'a> C: ArgumentSource<Unaligned, Retention = Preserving, Value<'a> = bool>,
{
    type Output = OperandHandle<Bare<V>, Multiple<O>>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        if condition? {
            <RaiseErrorsWithCause<E> as Kernel<Bare<V>, Multiple<O>>>::execute(
                graphrecord,
                values,
                (),
            )
        } else {
            Ok(values)
        }
    }
}

impl<I: IndexDomain, V: ValueType, E: Error + 'static, C> Kernel<Indexed<I, V>, Single>
    for RaiseWhenErrorsWithCause<E, C>
where
    for<'a> C: ArgumentSource<Unaligned, Retention = Preserving, Value<'a> = bool>,
{
    type Output = OperandHandle<Indexed<I, V>, Single>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, V, Single>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        if condition? {
            <RaiseErrorsWithCause<E> as Kernel<Indexed<I, V>, Single>>::execute(
                graphrecord,
                value,
                (),
            )
        } else {
            Ok(value)
        }
    }
}

impl<V: ValueType, E: Error + 'static, C> Kernel<Bare<V>, Single> for RaiseWhenErrorsWithCause<E, C>
where
    for<'a> C: ArgumentSource<Unaligned, Retention = Preserving, Value<'a> = bool>,
{
    type Output = OperandHandle<Bare<V>, Single>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: BareStream<'a, V, Single>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        if condition? {
            <RaiseErrorsWithCause<E> as Kernel<Bare<V>, Single>>::execute(graphrecord, value, ())
        } else {
            Ok(value)
        }
    }
}

impl<I: IndexDomain, V: ValueType, E: Error + 'static, C> Kernel<Indexed<I, V>, Definite>
    for RaiseWhenErrorsWithCause<E, C>
where
    for<'a> C: ArgumentSource<Unaligned, Retention = Preserving, Value<'a> = bool>,
{
    type Output = OperandHandle<Indexed<I, V>, Definite>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, V, Definite>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        if condition? {
            <RaiseErrorsWithCause<E> as Kernel<Indexed<I, V>, Definite>>::execute(
                graphrecord,
                value,
                (),
            )
        } else {
            Ok(value)
        }
    }
}

impl<V: ValueType, E: Error + 'static, C> Kernel<Bare<V>, Definite>
    for RaiseWhenErrorsWithCause<E, C>
where
    for<'a> C: ArgumentSource<Unaligned, Retention = Preserving, Value<'a> = bool>,
{
    type Output = OperandHandle<Bare<V>, Definite>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: BareStream<'a, V, Definite>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        if condition? {
            <RaiseErrorsWithCause<E> as Kernel<Bare<V>, Definite>>::execute(graphrecord, value, ())
        } else {
            Ok(value)
        }
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

impl<I, C> ErrorPolicy<I> for RaiseWhen<C>
where
    C: Clone + 'static,
    Self: Operation,
    I: Apply<Self>,
{
    type Output = <I as Apply<Self>>::Output;

    fn build(&self, input: I) -> Self::Output {
        Self::Output::new(OperationContext::new(input, self.clone()))
    }
}

impl<I, D> ErrorPolicyOf<I, D> for Raise
where
    I: Apply<RaiseErrorsOf<D>>,
    D: Diagnostic,
{
    type Output = <I as Apply<RaiseErrorsOf<D>>>::Output;

    fn build(&self, input: I) -> Self::Output {
        Self::Output::new(OperationContext::new(input, RaiseErrorsOf::new()))
    }
}

impl<I, G> ErrorPolicyIn<I, G> for Raise
where
    I: Apply<RaiseErrorsIn<G>>,
    G: ErrorGroup,
{
    type Output = <I as Apply<RaiseErrorsIn<G>>>::Output;

    fn build(&self, input: I) -> Self::Output {
        Self::Output::new(OperationContext::new(input, RaiseErrorsIn::new()))
    }
}

impl<I, E> ErrorPolicyWithCause<I, E> for Raise
where
    I: Apply<RaiseErrorsWithCause<E>>,
    E: Error + 'static,
{
    type Output = <I as Apply<RaiseErrorsWithCause<E>>>::Output;

    fn build(&self, input: I) -> Self::Output {
        Self::Output::new(OperationContext::new(input, RaiseErrorsWithCause::new()))
    }
}

impl<I, D, C> ErrorPolicyOf<I, D> for RaiseWhen<C>
where
    D: Diagnostic,
    C: Clone + 'static,
    RaiseWhenErrorsOf<D, C>: Operation,
    I: Apply<RaiseWhenErrorsOf<D, C>>,
{
    type Output = <I as Apply<RaiseWhenErrorsOf<D, C>>>::Output;

    fn build(&self, input: I) -> Self::Output {
        Self::Output::new(OperationContext::new(
            input,
            RaiseWhenErrorsOf::new(self.condition.clone()),
        ))
    }
}

impl<I, G, C> ErrorPolicyIn<I, G> for RaiseWhen<C>
where
    G: ErrorGroup,
    C: Clone + 'static,
    RaiseWhenErrorsIn<G, C>: Operation,
    I: Apply<RaiseWhenErrorsIn<G, C>>,
{
    type Output = <I as Apply<RaiseWhenErrorsIn<G, C>>>::Output;

    fn build(&self, input: I) -> Self::Output {
        Self::Output::new(OperationContext::new(
            input,
            RaiseWhenErrorsIn::new(self.condition.clone()),
        ))
    }
}

impl<I, E, C> ErrorPolicyWithCause<I, E> for RaiseWhen<C>
where
    E: Error + 'static,
    C: Clone + 'static,
    RaiseWhenErrorsWithCause<E, C>: Operation,
    I: Apply<RaiseWhenErrorsWithCause<E, C>>,
{
    type Output = <I as Apply<RaiseWhenErrorsWithCause<E, C>>>::Output;

    fn build(&self, input: I) -> Self::Output {
        Self::Output::new(OperationContext::new(
            input,
            RaiseWhenErrorsWithCause::new(self.condition.clone()),
        ))
    }
}
