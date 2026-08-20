use crate::{
    Bare, BareValueDomain, BoxedIterator, Definite, Diagnostic, ErrorGroup, EvaluateExpression,
    Explain, Expression, Failure, IndexDomain, Indexed, Labeled, Mask, Multiple, OrderState,
    QueryResult, Single, ValueDomain,
    element::Preserving,
    error::policy::RaisedFailures,
    execution::EvaluationCache,
    explain::ExplainFormatter,
    expressions::ExpressionHandle,
    operations::{
        Apply, ArgumentSource, BareStream, ErrorPolicy, ErrorPolicyIn, ErrorPolicyOf,
        ErrorPolicyWithCause, KeyedStream, LaneKernel, Operation, OperationContext, Prepare,
        Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::OnError,
};
use graphrecords_core::GraphRecord;
use std::{
    any::type_name,
    error::Error,
    fmt::{self, Write},
    marker::PhantomData,
};

fn raise_indexed<'a, K: 'a, X: 'a>(
    values: BoxedIterator<'a, (K, QueryResult<X>)>,
    matches: impl Fn(&Failure) -> bool,
    label: &'static str,
) -> QueryResult<BoxedIterator<'a, (K, QueryResult<X>)>> {
    let mut kept = Vec::new();
    let mut raised = Vec::new();

    for (index, outcome) in values {
        match outcome {
            Err(failure) if matches(&failure) => raised.push(*failure),
            outcome => kept.push((index, outcome)),
        }
    }

    if !raised.is_empty() {
        return Err(Failure::new(RaisedFailures::new(raised), label));
    }

    Ok(Box::new(kept.into_iter()))
}

fn raise_bare<'a, X: 'a>(
    values: BoxedIterator<'a, QueryResult<X>>,
    matches: impl Fn(&Failure) -> bool,
    label: &'static str,
) -> QueryResult<BoxedIterator<'a, QueryResult<X>>> {
    let mut kept = Vec::new();
    let mut raised = Vec::new();

    for outcome in values {
        match outcome {
            Err(failure) if matches(&failure) => raised.push(*failure),
            outcome => kept.push(outcome),
        }
    }

    if !raised.is_empty() {
        return Err(Failure::new(RaisedFailures::new(raised), label));
    }

    Ok(Box::new(kept.into_iter()))
}

fn raise_outcome<T>(
    outcome: QueryResult<T>,
    matches: impl Fn(&Failure) -> bool,
    label: &'static str,
) -> QueryResult<QueryResult<T>> {
    match outcome {
        Err(failure) if matches(&failure) => {
            Err(Failure::new(RaisedFailures::new(vec![*failure]), label))
        }
        outcome => Ok(outcome),
    }
}

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Lane)]
#[explain(label = "Raise")]
#[plan(optimizer_hints(empty = if_any))]
pub struct Raise;

#[derive(Clone, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[plan(optimizer_hints(empty = if_all))]
pub struct RaiseWhen<C> {
    #[argument]
    condition: C,
}

impl<C> Labeled for RaiseWhen<C> {
    const LABEL: &'static str = "RaiseWhen";
}

impl<C: Explain> Explain for RaiseWhen<C> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        formatter.write_str(Self::LABEL)?;
        formatter.labeled_child(&self.condition, "condition");

        Ok(())
    }
}

impl Raise {
    #[must_use]
    pub const fn when<C>(self, condition: C) -> RaiseWhen<C> {
        RaiseWhen { condition }
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare)]
#[operation(scope = Lane)]
#[plan(optimizer_hints(empty = if_any))]
pub struct RaiseErrorsOf<D: Diagnostic> {
    marker: PhantomData<fn() -> D>,
}

impl<D: Diagnostic> Labeled for RaiseErrorsOf<D> {
    const LABEL: &'static str = "RaiseErrorsOf";
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
        write!(formatter, "{} kind={}", Self::LABEL, D::name())
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare)]
#[operation(scope = Lane)]
#[plan(optimizer_hints(empty = if_any))]
pub struct RaiseErrorsIn<G: ErrorGroup> {
    marker: PhantomData<fn() -> G>,
}

impl<G: ErrorGroup> Labeled for RaiseErrorsIn<G> {
    const LABEL: &'static str = "RaiseErrorsIn";
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
        write!(formatter, "{} group={}", Self::LABEL, G::name())
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare)]
#[operation(scope = Lane)]
#[plan(optimizer_hints(empty = if_any))]
pub struct RaiseErrorsWithCause<E: Error + 'static> {
    marker: PhantomData<fn() -> E>,
}

impl<E: Error + 'static> Labeled for RaiseErrorsWithCause<E> {
    const LABEL: &'static str = "RaiseErrorsWithCause";
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
        write!(formatter, "{} cause={}", Self::LABEL, type_name::<E>())
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[plan(optimizer_hints(empty = if_all))]
pub struct RaiseWhenErrorsOf<D: Diagnostic, C> {
    #[argument]
    condition: C,
    marker: PhantomData<fn() -> D>,
}

impl<D: Diagnostic, C> Labeled for RaiseWhenErrorsOf<D, C> {
    const LABEL: &'static str = "RaiseWhenErrorsOf";
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
        write!(formatter, "{} kind={}", Self::LABEL, D::name())?;
        formatter.labeled_child(&self.condition, "condition");

        Ok(())
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[plan(optimizer_hints(empty = if_all))]
pub struct RaiseWhenErrorsIn<G: ErrorGroup, C> {
    #[argument]
    condition: C,
    marker: PhantomData<fn() -> G>,
}

impl<G: ErrorGroup, C> Labeled for RaiseWhenErrorsIn<G, C> {
    const LABEL: &'static str = "RaiseWhenErrorsIn";
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
        write!(formatter, "{} group={}", Self::LABEL, G::name())?;
        formatter.labeled_child(&self.condition, "condition");

        Ok(())
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[plan(optimizer_hints(empty = if_all))]
pub struct RaiseWhenErrorsWithCause<E: Error + 'static, C> {
    #[argument]
    condition: C,
    marker: PhantomData<fn() -> E>,
}

impl<E: Error + 'static, C> Labeled for RaiseWhenErrorsWithCause<E, C> {
    const LABEL: &'static str = "RaiseWhenErrorsWithCause";
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
        write!(formatter, "{} cause={}", Self::LABEL, type_name::<E>())?;
        formatter.labeled_child(&self.condition, "condition");

        Ok(())
    }
}

impl<C> Prepare for RaiseWhen<C>
where
    C: ArgumentSource<Unaligned, Mask, Retention = Preserving>,
{
    type Prepared<'a> = QueryResult<bool>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        let condition = self.condition.prepare(graphrecord, cache)?;

        Ok(C::resolve(graphrecord, &condition, &(), Self::LABEL))
    }
}

impl<D, C> Prepare for RaiseWhenErrorsOf<D, C>
where
    D: Diagnostic,
    C: ArgumentSource<Unaligned, Mask, Retention = Preserving>,
{
    type Prepared<'a> = QueryResult<bool>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        let condition = self.condition.prepare(graphrecord, cache)?;

        Ok(C::resolve(graphrecord, &condition, &(), Self::LABEL))
    }
}

impl<G, C> Prepare for RaiseWhenErrorsIn<G, C>
where
    G: ErrorGroup,
    C: ArgumentSource<Unaligned, Mask, Retention = Preserving>,
{
    type Prepared<'a> = QueryResult<bool>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        let condition = self.condition.prepare(graphrecord, cache)?;

        Ok(C::resolve(graphrecord, &condition, &(), Self::LABEL))
    }
}

impl<E, C> Prepare for RaiseWhenErrorsWithCause<E, C>
where
    E: Error + 'static,
    C: ArgumentSource<Unaligned, Mask, Retention = Preserving>,
{
    type Prepared<'a> = QueryResult<bool>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        let condition = self.condition.prepare(graphrecord, cache)?;

        Ok(C::resolve(graphrecord, &condition, &(), Self::LABEL))
    }
}

impl<I: IndexDomain, V: ValueDomain, O: OrderState> LaneKernel<Indexed<I, V>, Multiple<O>>
    for Raise
{
    type Output = ExpressionHandle<Indexed<I, V>, Multiple<O>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        raise_indexed(values, |_| true, Self::LABEL)
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<V: BareValueDomain, O: OrderState> LaneKernel<Bare<V>, Multiple<O>> for Raise {
    type Output = ExpressionHandle<Bare<V>, Multiple<O>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        raise_bare(values, |_| true, Self::LABEL)
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<I: IndexDomain, V: ValueDomain> LaneKernel<Indexed<I, V>, Single> for Raise {
    type Output = ExpressionHandle<Indexed<I, V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, V, Single>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        value
            .map(|(index, outcome)| Ok((index, raise_outcome(outcome, |_| true, Self::LABEL)?)))
            .transpose()
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<V: BareValueDomain> LaneKernel<Bare<V>, Single> for Raise {
    type Output = ExpressionHandle<Bare<V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: BareStream<'a, V, Single>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        value
            .map(|outcome| raise_outcome(outcome, |_| true, Self::LABEL))
            .transpose()
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<I: IndexDomain, V: ValueDomain> LaneKernel<Indexed<I, V>, Definite> for Raise {
    type Output = ExpressionHandle<Indexed<I, V>, Definite>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, V, Definite>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        Ok((value.0, raise_outcome(value.1, |_| true, Self::LABEL)?))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<V: BareValueDomain> LaneKernel<Bare<V>, Definite> for Raise {
    type Output = ExpressionHandle<Bare<V>, Definite>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: BareStream<'a, V, Definite>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        raise_outcome(value, |_| true, Self::LABEL)
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<I, V, O, C> LaneKernel<Indexed<I, V>, Multiple<O>> for RaiseWhen<C>
where
    I: IndexDomain,
    V: ValueDomain,
    O: OrderState,
    C: ArgumentSource<Unaligned, Mask, Retention = Preserving>,
{
    type Output = ExpressionHandle<Indexed<I, V>, Multiple<O>>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        if condition? {
            <Raise as LaneKernel<Indexed<I, V>, Multiple<O>>>::execute(graphrecord, values, ())
        } else {
            Ok(values)
        }
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<V, O, C> LaneKernel<Bare<V>, Multiple<O>> for RaiseWhen<C>
where
    V: BareValueDomain,
    O: OrderState,
    C: ArgumentSource<Unaligned, Mask, Retention = Preserving>,
{
    type Output = ExpressionHandle<Bare<V>, Multiple<O>>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        if condition? {
            <Raise as LaneKernel<Bare<V>, Multiple<O>>>::execute(graphrecord, values, ())
        } else {
            Ok(values)
        }
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<I, V, C> LaneKernel<Indexed<I, V>, Single> for RaiseWhen<C>
where
    I: IndexDomain,
    V: ValueDomain,
    C: ArgumentSource<Unaligned, Mask, Retention = Preserving>,
{
    type Output = ExpressionHandle<Indexed<I, V>, Single>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, V, Single>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        if condition? {
            <Raise as LaneKernel<Indexed<I, V>, Single>>::execute(graphrecord, value, ())
        } else {
            Ok(value)
        }
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<V, C> LaneKernel<Bare<V>, Single> for RaiseWhen<C>
where
    V: BareValueDomain,
    C: ArgumentSource<Unaligned, Mask, Retention = Preserving>,
{
    type Output = ExpressionHandle<Bare<V>, Single>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: BareStream<'a, V, Single>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        if condition? {
            <Raise as LaneKernel<Bare<V>, Single>>::execute(graphrecord, value, ())
        } else {
            Ok(value)
        }
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<I, V, C> LaneKernel<Indexed<I, V>, Definite> for RaiseWhen<C>
where
    I: IndexDomain,
    V: ValueDomain,
    C: ArgumentSource<Unaligned, Mask, Retention = Preserving>,
{
    type Output = ExpressionHandle<Indexed<I, V>, Definite>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, V, Definite>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        if condition? {
            <Raise as LaneKernel<Indexed<I, V>, Definite>>::execute(graphrecord, value, ())
        } else {
            Ok(value)
        }
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<V, C> LaneKernel<Bare<V>, Definite> for RaiseWhen<C>
where
    V: BareValueDomain,
    C: ArgumentSource<Unaligned, Mask, Retention = Preserving>,
{
    type Output = ExpressionHandle<Bare<V>, Definite>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: BareStream<'a, V, Definite>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        if condition? {
            <Raise as LaneKernel<Bare<V>, Definite>>::execute(graphrecord, value, ())
        } else {
            Ok(value)
        }
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<I: IndexDomain, V: ValueDomain, O: OrderState, D: Diagnostic>
    LaneKernel<Indexed<I, V>, Multiple<O>> for RaiseErrorsOf<D>
{
    type Output = ExpressionHandle<Indexed<I, V>, Multiple<O>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        raise_indexed(values, Self::matches, Self::LABEL)
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<V: BareValueDomain, O: OrderState, D: Diagnostic> LaneKernel<Bare<V>, Multiple<O>>
    for RaiseErrorsOf<D>
{
    type Output = ExpressionHandle<Bare<V>, Multiple<O>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        raise_bare(values, Self::matches, Self::LABEL)
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<I: IndexDomain, V: ValueDomain, D: Diagnostic> LaneKernel<Indexed<I, V>, Single>
    for RaiseErrorsOf<D>
{
    type Output = ExpressionHandle<Indexed<I, V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, V, Single>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        value
            .map(|(index, outcome)| {
                Ok((index, raise_outcome(outcome, Self::matches, Self::LABEL)?))
            })
            .transpose()
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<V: BareValueDomain, D: Diagnostic> LaneKernel<Bare<V>, Single> for RaiseErrorsOf<D> {
    type Output = ExpressionHandle<Bare<V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: BareStream<'a, V, Single>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        value
            .map(|outcome| raise_outcome(outcome, Self::matches, Self::LABEL))
            .transpose()
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<I: IndexDomain, V: ValueDomain, D: Diagnostic> LaneKernel<Indexed<I, V>, Definite>
    for RaiseErrorsOf<D>
{
    type Output = ExpressionHandle<Indexed<I, V>, Definite>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, V, Definite>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        Ok((value.0, raise_outcome(value.1, Self::matches, Self::LABEL)?))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<V: BareValueDomain, D: Diagnostic> LaneKernel<Bare<V>, Definite> for RaiseErrorsOf<D> {
    type Output = ExpressionHandle<Bare<V>, Definite>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: BareStream<'a, V, Definite>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        raise_outcome(value, Self::matches, Self::LABEL)
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<I: IndexDomain, V: ValueDomain, O: OrderState, G: ErrorGroup>
    LaneKernel<Indexed<I, V>, Multiple<O>> for RaiseErrorsIn<G>
{
    type Output = ExpressionHandle<Indexed<I, V>, Multiple<O>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        raise_indexed(values, Self::matches, Self::LABEL)
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<V: BareValueDomain, O: OrderState, G: ErrorGroup> LaneKernel<Bare<V>, Multiple<O>>
    for RaiseErrorsIn<G>
{
    type Output = ExpressionHandle<Bare<V>, Multiple<O>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        raise_bare(values, Self::matches, Self::LABEL)
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<I: IndexDomain, V: ValueDomain, G: ErrorGroup> LaneKernel<Indexed<I, V>, Single>
    for RaiseErrorsIn<G>
{
    type Output = ExpressionHandle<Indexed<I, V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, V, Single>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        value
            .map(|(index, outcome)| {
                Ok((index, raise_outcome(outcome, Self::matches, Self::LABEL)?))
            })
            .transpose()
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<V: BareValueDomain, G: ErrorGroup> LaneKernel<Bare<V>, Single> for RaiseErrorsIn<G> {
    type Output = ExpressionHandle<Bare<V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: BareStream<'a, V, Single>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        value
            .map(|outcome| raise_outcome(outcome, Self::matches, Self::LABEL))
            .transpose()
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<I: IndexDomain, V: ValueDomain, G: ErrorGroup> LaneKernel<Indexed<I, V>, Definite>
    for RaiseErrorsIn<G>
{
    type Output = ExpressionHandle<Indexed<I, V>, Definite>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, V, Definite>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        Ok((value.0, raise_outcome(value.1, Self::matches, Self::LABEL)?))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<V: BareValueDomain, G: ErrorGroup> LaneKernel<Bare<V>, Definite> for RaiseErrorsIn<G> {
    type Output = ExpressionHandle<Bare<V>, Definite>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: BareStream<'a, V, Definite>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        raise_outcome(value, Self::matches, Self::LABEL)
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<I: IndexDomain, V: ValueDomain, O: OrderState, E: Error + 'static>
    LaneKernel<Indexed<I, V>, Multiple<O>> for RaiseErrorsWithCause<E>
{
    type Output = ExpressionHandle<Indexed<I, V>, Multiple<O>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        raise_indexed(values, Self::matches, Self::LABEL)
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<V: BareValueDomain, O: OrderState, E: Error + 'static> LaneKernel<Bare<V>, Multiple<O>>
    for RaiseErrorsWithCause<E>
{
    type Output = ExpressionHandle<Bare<V>, Multiple<O>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        raise_bare(values, Self::matches, Self::LABEL)
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<I: IndexDomain, V: ValueDomain, E: Error + 'static> LaneKernel<Indexed<I, V>, Single>
    for RaiseErrorsWithCause<E>
{
    type Output = ExpressionHandle<Indexed<I, V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, V, Single>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        value
            .map(|(index, outcome)| {
                Ok((index, raise_outcome(outcome, Self::matches, Self::LABEL)?))
            })
            .transpose()
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<V: BareValueDomain, E: Error + 'static> LaneKernel<Bare<V>, Single>
    for RaiseErrorsWithCause<E>
{
    type Output = ExpressionHandle<Bare<V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: BareStream<'a, V, Single>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        value
            .map(|outcome| raise_outcome(outcome, Self::matches, Self::LABEL))
            .transpose()
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<I: IndexDomain, V: ValueDomain, E: Error + 'static> LaneKernel<Indexed<I, V>, Definite>
    for RaiseErrorsWithCause<E>
{
    type Output = ExpressionHandle<Indexed<I, V>, Definite>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, V, Definite>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        Ok((value.0, raise_outcome(value.1, Self::matches, Self::LABEL)?))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<V: BareValueDomain, E: Error + 'static> LaneKernel<Bare<V>, Definite>
    for RaiseErrorsWithCause<E>
{
    type Output = ExpressionHandle<Bare<V>, Definite>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: BareStream<'a, V, Definite>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        raise_outcome(value, Self::matches, Self::LABEL)
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<I, V, O, D, C> LaneKernel<Indexed<I, V>, Multiple<O>> for RaiseWhenErrorsOf<D, C>
where
    I: IndexDomain,
    V: ValueDomain,
    O: OrderState,
    D: Diagnostic,
    C: ArgumentSource<Unaligned, Mask, Retention = Preserving>,
{
    type Output = ExpressionHandle<Indexed<I, V>, Multiple<O>>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        if condition? {
            <RaiseErrorsOf<D> as LaneKernel<Indexed<I, V>, Multiple<O>>>::execute(
                graphrecord,
                values,
                (),
            )
        } else {
            Ok(values)
        }
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<V, O, D, C> LaneKernel<Bare<V>, Multiple<O>> for RaiseWhenErrorsOf<D, C>
where
    V: BareValueDomain,
    O: OrderState,
    D: Diagnostic,
    C: ArgumentSource<Unaligned, Mask, Retention = Preserving>,
{
    type Output = ExpressionHandle<Bare<V>, Multiple<O>>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        if condition? {
            <RaiseErrorsOf<D> as LaneKernel<Bare<V>, Multiple<O>>>::execute(graphrecord, values, ())
        } else {
            Ok(values)
        }
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<I, V, D, C> LaneKernel<Indexed<I, V>, Single> for RaiseWhenErrorsOf<D, C>
where
    I: IndexDomain,
    V: ValueDomain,
    D: Diagnostic,
    C: ArgumentSource<Unaligned, Mask, Retention = Preserving>,
{
    type Output = ExpressionHandle<Indexed<I, V>, Single>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, V, Single>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        if condition? {
            <RaiseErrorsOf<D> as LaneKernel<Indexed<I, V>, Single>>::execute(graphrecord, value, ())
        } else {
            Ok(value)
        }
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<V, D, C> LaneKernel<Bare<V>, Single> for RaiseWhenErrorsOf<D, C>
where
    V: BareValueDomain,
    D: Diagnostic,
    C: ArgumentSource<Unaligned, Mask, Retention = Preserving>,
{
    type Output = ExpressionHandle<Bare<V>, Single>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: BareStream<'a, V, Single>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        if condition? {
            <RaiseErrorsOf<D> as LaneKernel<Bare<V>, Single>>::execute(graphrecord, value, ())
        } else {
            Ok(value)
        }
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<I, V, D, C> LaneKernel<Indexed<I, V>, Definite> for RaiseWhenErrorsOf<D, C>
where
    I: IndexDomain,
    V: ValueDomain,
    D: Diagnostic,
    C: ArgumentSource<Unaligned, Mask, Retention = Preserving>,
{
    type Output = ExpressionHandle<Indexed<I, V>, Definite>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, V, Definite>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        if condition? {
            <RaiseErrorsOf<D> as LaneKernel<Indexed<I, V>, Definite>>::execute(
                graphrecord,
                value,
                (),
            )
        } else {
            Ok(value)
        }
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<V, D, C> LaneKernel<Bare<V>, Definite> for RaiseWhenErrorsOf<D, C>
where
    V: BareValueDomain,
    D: Diagnostic,
    C: ArgumentSource<Unaligned, Mask, Retention = Preserving>,
{
    type Output = ExpressionHandle<Bare<V>, Definite>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: BareStream<'a, V, Definite>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        if condition? {
            <RaiseErrorsOf<D> as LaneKernel<Bare<V>, Definite>>::execute(graphrecord, value, ())
        } else {
            Ok(value)
        }
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<I, V, O, G, C> LaneKernel<Indexed<I, V>, Multiple<O>> for RaiseWhenErrorsIn<G, C>
where
    I: IndexDomain,
    V: ValueDomain,
    O: OrderState,
    G: ErrorGroup,
    C: ArgumentSource<Unaligned, Mask, Retention = Preserving>,
{
    type Output = ExpressionHandle<Indexed<I, V>, Multiple<O>>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        if condition? {
            <RaiseErrorsIn<G> as LaneKernel<Indexed<I, V>, Multiple<O>>>::execute(
                graphrecord,
                values,
                (),
            )
        } else {
            Ok(values)
        }
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<V, O, G, C> LaneKernel<Bare<V>, Multiple<O>> for RaiseWhenErrorsIn<G, C>
where
    V: BareValueDomain,
    O: OrderState,
    G: ErrorGroup,
    C: ArgumentSource<Unaligned, Mask, Retention = Preserving>,
{
    type Output = ExpressionHandle<Bare<V>, Multiple<O>>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        if condition? {
            <RaiseErrorsIn<G> as LaneKernel<Bare<V>, Multiple<O>>>::execute(graphrecord, values, ())
        } else {
            Ok(values)
        }
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<I, V, G, C> LaneKernel<Indexed<I, V>, Single> for RaiseWhenErrorsIn<G, C>
where
    I: IndexDomain,
    V: ValueDomain,
    G: ErrorGroup,
    C: ArgumentSource<Unaligned, Mask, Retention = Preserving>,
{
    type Output = ExpressionHandle<Indexed<I, V>, Single>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, V, Single>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        if condition? {
            <RaiseErrorsIn<G> as LaneKernel<Indexed<I, V>, Single>>::execute(graphrecord, value, ())
        } else {
            Ok(value)
        }
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<V, G, C> LaneKernel<Bare<V>, Single> for RaiseWhenErrorsIn<G, C>
where
    V: BareValueDomain,
    G: ErrorGroup,
    C: ArgumentSource<Unaligned, Mask, Retention = Preserving>,
{
    type Output = ExpressionHandle<Bare<V>, Single>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: BareStream<'a, V, Single>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        if condition? {
            <RaiseErrorsIn<G> as LaneKernel<Bare<V>, Single>>::execute(graphrecord, value, ())
        } else {
            Ok(value)
        }
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<I, V, G, C> LaneKernel<Indexed<I, V>, Definite> for RaiseWhenErrorsIn<G, C>
where
    I: IndexDomain,
    V: ValueDomain,
    G: ErrorGroup,
    C: ArgumentSource<Unaligned, Mask, Retention = Preserving>,
{
    type Output = ExpressionHandle<Indexed<I, V>, Definite>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, V, Definite>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        if condition? {
            <RaiseErrorsIn<G> as LaneKernel<Indexed<I, V>, Definite>>::execute(
                graphrecord,
                value,
                (),
            )
        } else {
            Ok(value)
        }
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<V, G, C> LaneKernel<Bare<V>, Definite> for RaiseWhenErrorsIn<G, C>
where
    V: BareValueDomain,
    G: ErrorGroup,
    C: ArgumentSource<Unaligned, Mask, Retention = Preserving>,
{
    type Output = ExpressionHandle<Bare<V>, Definite>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: BareStream<'a, V, Definite>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        if condition? {
            <RaiseErrorsIn<G> as LaneKernel<Bare<V>, Definite>>::execute(graphrecord, value, ())
        } else {
            Ok(value)
        }
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<I, V, O, E, C> LaneKernel<Indexed<I, V>, Multiple<O>> for RaiseWhenErrorsWithCause<E, C>
where
    I: IndexDomain,
    V: ValueDomain,
    O: OrderState,
    E: Error + 'static,
    C: ArgumentSource<Unaligned, Mask, Retention = Preserving>,
{
    type Output = ExpressionHandle<Indexed<I, V>, Multiple<O>>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<O>>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        if condition? {
            <RaiseErrorsWithCause<E> as LaneKernel<Indexed<I, V>, Multiple<O>>>::execute(
                graphrecord,
                values,
                (),
            )
        } else {
            Ok(values)
        }
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<V, O, E, C> LaneKernel<Bare<V>, Multiple<O>> for RaiseWhenErrorsWithCause<E, C>
where
    V: BareValueDomain,
    O: OrderState,
    E: Error + 'static,
    C: ArgumentSource<Unaligned, Mask, Retention = Preserving>,
{
    type Output = ExpressionHandle<Bare<V>, Multiple<O>>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        if condition? {
            <RaiseErrorsWithCause<E> as LaneKernel<Bare<V>, Multiple<O>>>::execute(
                graphrecord,
                values,
                (),
            )
        } else {
            Ok(values)
        }
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<I, V, E, C> LaneKernel<Indexed<I, V>, Single> for RaiseWhenErrorsWithCause<E, C>
where
    I: IndexDomain,
    V: ValueDomain,
    E: Error + 'static,
    C: ArgumentSource<Unaligned, Mask, Retention = Preserving>,
{
    type Output = ExpressionHandle<Indexed<I, V>, Single>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, V, Single>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        if condition? {
            <RaiseErrorsWithCause<E> as LaneKernel<Indexed<I, V>, Single>>::execute(
                graphrecord,
                value,
                (),
            )
        } else {
            Ok(value)
        }
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<V, E, C> LaneKernel<Bare<V>, Single> for RaiseWhenErrorsWithCause<E, C>
where
    V: BareValueDomain,
    E: Error + 'static,
    C: ArgumentSource<Unaligned, Mask, Retention = Preserving>,
{
    type Output = ExpressionHandle<Bare<V>, Single>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: BareStream<'a, V, Single>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        if condition? {
            <RaiseErrorsWithCause<E> as LaneKernel<Bare<V>, Single>>::execute(
                graphrecord,
                value,
                (),
            )
        } else {
            Ok(value)
        }
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<I, V, E, C> LaneKernel<Indexed<I, V>, Definite> for RaiseWhenErrorsWithCause<E, C>
where
    I: IndexDomain,
    V: ValueDomain,
    E: Error + 'static,
    C: ArgumentSource<Unaligned, Mask, Retention = Preserving>,
{
    type Output = ExpressionHandle<Indexed<I, V>, Definite>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, V, Definite>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        if condition? {
            <RaiseErrorsWithCause<E> as LaneKernel<Indexed<I, V>, Definite>>::execute(
                graphrecord,
                value,
                (),
            )
        } else {
            Ok(value)
        }
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<V, E, C> LaneKernel<Bare<V>, Definite> for RaiseWhenErrorsWithCause<E, C>
where
    V: BareValueDomain,
    E: Error + 'static,
    C: ArgumentSource<Unaligned, Mask, Retention = Preserving>,
{
    type Output = ExpressionHandle<Bare<V>, Definite>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: BareStream<'a, V, Definite>,
        condition: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        if condition? {
            <RaiseErrorsWithCause<E> as LaneKernel<Bare<V>, Definite>>::execute(
                graphrecord,
                value,
                (),
            )
        } else {
            Ok(value)
        }
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<E: Apply<Self>> ErrorPolicy<E> for Raise {
    type Output = E::Output;

    fn build(&self, input: E) -> Self::Output {
        Self::Output::new(OperationContext::new(input, Self))
    }
}

impl<E, C> ErrorPolicy<E> for RaiseWhen<C>
where
    C: Clone + 'static,
    Self: Operation,
    E: Apply<Self>,
{
    type Output = E::Output;

    fn build(&self, input: E) -> Self::Output {
        Self::Output::new(OperationContext::new(input, self.clone()))
    }
}

impl<E: Apply<RaiseErrorsOf<D>>, D: Diagnostic> ErrorPolicyOf<E, D> for Raise {
    type Output = E::Output;

    fn build(&self, input: E) -> Self::Output {
        Self::Output::new(OperationContext::new(input, RaiseErrorsOf::new()))
    }
}

impl<E: Apply<RaiseErrorsIn<G>>, G: ErrorGroup> ErrorPolicyIn<E, G> for Raise {
    type Output = E::Output;

    fn build(&self, input: E) -> Self::Output {
        Self::Output::new(OperationContext::new(input, RaiseErrorsIn::new()))
    }
}

impl<E: Apply<RaiseErrorsWithCause<C>>, C: Error + 'static> ErrorPolicyWithCause<E, C> for Raise {
    type Output = E::Output;

    fn build(&self, input: E) -> Self::Output {
        Self::Output::new(OperationContext::new(input, RaiseErrorsWithCause::new()))
    }
}

impl<E, D, C> ErrorPolicyOf<E, D> for RaiseWhen<C>
where
    D: Diagnostic,
    C: Clone + 'static,
    RaiseWhenErrorsOf<D, C>: Operation,
    E: Apply<RaiseWhenErrorsOf<D, C>>,
{
    type Output = E::Output;

    fn build(&self, input: E) -> Self::Output {
        Self::Output::new(OperationContext::new(
            input,
            RaiseWhenErrorsOf::new(self.condition.clone()),
        ))
    }
}

impl<E, G, C> ErrorPolicyIn<E, G> for RaiseWhen<C>
where
    G: ErrorGroup,
    C: Clone + 'static,
    RaiseWhenErrorsIn<G, C>: Operation,
    E: Apply<RaiseWhenErrorsIn<G, C>>,
{
    type Output = E::Output;

    fn build(&self, input: E) -> Self::Output {
        Self::Output::new(OperationContext::new(
            input,
            RaiseWhenErrorsIn::new(self.condition.clone()),
        ))
    }
}

impl<E, C, M> ErrorPolicyWithCause<E, C> for RaiseWhen<M>
where
    C: Error + 'static,
    M: Clone + 'static,
    RaiseWhenErrorsWithCause<C, M>: Operation,
    E: Apply<RaiseWhenErrorsWithCause<C, M>>,
{
    type Output = E::Output;

    fn build(&self, input: E) -> Self::Output {
        Self::Output::new(OperationContext::new(
            input,
            RaiseWhenErrorsWithCause::new(self.condition.clone()),
        ))
    }
}

operation_manifest! {
    Raise as "on_error_raise" {
        method: OnError::on_error;
        policy: Raise;
        scope: lane;

        kernel {
            parameters: <I: IndexDomain, V: ValueDomain, O: OrderState>;
            input: (Indexed<I, V>, Multiple<O>);
            output: ExpressionHandle<Indexed<I, V>, Multiple<O>>;
        }

        kernel {
            parameters: <I: IndexDomain, V: ValueDomain>;
            input: (Indexed<I, V>, Single);
            output: ExpressionHandle<Indexed<I, V>, Single>;
        }

        kernel {
            parameters: <I: IndexDomain, V: ValueDomain>;
            input: (Indexed<I, V>, Definite);
            output: ExpressionHandle<Indexed<I, V>, Definite>;
        }

        kernel {
            parameters: <V: BareValueDomain, O: OrderState>;
            input: (Bare<V>, Multiple<O>);
            output: ExpressionHandle<Bare<V>, Multiple<O>>;
        }

        kernel {
            parameters: <V: BareValueDomain>;
            input: (Bare<V>, Single);
            output: ExpressionHandle<Bare<V>, Single>;
        }

        kernel {
            parameters: <V: BareValueDomain>;
            input: (Bare<V>, Definite);
            output: ExpressionHandle<Bare<V>, Definite>;
        }
    }
}

pub mod raise_when {
    #[cfg(feature = "dynamic")]
    use super::Raise;
    use super::RaiseWhen;
    use crate::{
        Bare, Definite, Indexed, Mask, Multiple, Single, element::Preserving,
        expressions::ExpressionHandle, operations::Unaligned, registry::operation_manifest,
        traits::OnError,
    };

    operation_manifest! {
        RaiseWhen<C> as "on_error_raise_when" {
            method: OnError::on_error;
            policy: RaiseWhen<C> = Raise.when(C);
            scope: lane;

            kernel {
                parameters: <I: IndexDomain, V: ValueDomain, O: OrderState>;
                argument: C: ArgumentSource<Unaligned, Mask, Retention = Preserving>;
                input: (Indexed<I, V>, Multiple<O>);
                output: ExpressionHandle<Indexed<I, V>, Multiple<O>>;
            }

            kernel {
                parameters: <I: IndexDomain, V: ValueDomain>;
                argument: C: ArgumentSource<Unaligned, Mask, Retention = Preserving>;
                input: (Indexed<I, V>, Single);
                output: ExpressionHandle<Indexed<I, V>, Single>;
            }

            kernel {
                parameters: <I: IndexDomain, V: ValueDomain>;
                argument: C: ArgumentSource<Unaligned, Mask, Retention = Preserving>;
                input: (Indexed<I, V>, Definite);
                output: ExpressionHandle<Indexed<I, V>, Definite>;
            }

            kernel {
                parameters: <V: BareValueDomain, O: OrderState>;
                argument: C: ArgumentSource<Unaligned, Mask, Retention = Preserving>;
                input: (Bare<V>, Multiple<O>);
                output: ExpressionHandle<Bare<V>, Multiple<O>>;
            }

            kernel {
                parameters: <V: BareValueDomain>;
                argument: C: ArgumentSource<Unaligned, Mask, Retention = Preserving>;
                input: (Bare<V>, Single);
                output: ExpressionHandle<Bare<V>, Single>;
            }

            kernel {
                parameters: <V: BareValueDomain>;
                argument: C: ArgumentSource<Unaligned, Mask, Retention = Preserving>;
                input: (Bare<V>, Definite);
                output: ExpressionHandle<Bare<V>, Definite>;
            }
        }
    }
}
