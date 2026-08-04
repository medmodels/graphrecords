use super::{
    DynArgumentSource, DynArity, DynArityHandle, DynGroupHandle, DynHandle, DynIndex,
    DynInvokeArgument, DynOperand, DynPayload, DynPayloadOutput, DynStream, DynStreamShape,
    DynYield, IntoDynArityHandle, IntoDynLaneHandle, IntoDynOperand,
};
use crate::{
    EdgeDirection, EvaluateContext, EvaluateOperand, Explain, Mask, Operand, QueryResult,
    execution::EvaluationCache,
    explain::ExplainFormatter,
    operands::{OperandContext, OperandHandle, Partition},
    operations::{Apply, GroupKernel, Operation, OperationContext, OperationScope},
    optimizer::{
        EmptyRule, Estimate, Estimated, MatchInputs, OperationInputs, OptimizePlan, OptimizerHints,
        PlanInputs, PlanNode, Session, Stats, Transformed,
    },
    registry::{IndexDescriptor, LaneShapeDescriptor, OperandDescriptor, ValueRole},
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{EdgeIndex, GraphRecordAttribute, Group, NodeIndex},
};
use std::{
    any::Any,
    fmt::{self, Write},
    hash::{Hash, Hasher},
    marker::PhantomData,
    sync::Arc,
};

pub type DynApplier = fn(&DynOperand, &[DynInvokeArgument], OperandDescriptor) -> DynOperand;

#[derive(Clone, Copy)]
pub enum DynEntityDomain {
    Node,
    Edge,
}

#[derive(Clone, Copy)]
pub enum DynLaneKind {
    IndexedValue,
    IndexedMask,
    IndexedUnit,
    BareValue,
    BareMask,
}

pub struct OperationCapture<P: Operation> {
    context: Arc<dyn OperandContext<Self>>,
    marker: PhantomData<fn() -> P>,
}

pub struct CapturedOperation<P: Operation> {
    context: Arc<dyn OperandContext<Self>>,
    marker: PhantomData<fn() -> P>,
}

struct CaptureContext<P: Operation>(PhantomData<fn() -> P>);

pub struct DynGroupedOperationContext<P, S, C> {
    input: DynGroupHandle,
    operation: P,
    marker: PhantomData<fn() -> (S, C)>,
}

struct DynNestedGroupContext<P> {
    input: DynGroupHandle,
    operation: P,
    layers: usize,
}

impl<P: Operation> Clone for OperationCapture<P> {
    fn clone(&self) -> Self {
        Self {
            context: Arc::clone(&self.context),
            marker: PhantomData,
        }
    }
}

impl<P: Operation> Clone for CapturedOperation<P> {
    fn clone(&self) -> Self {
        Self {
            context: Arc::clone(&self.context),
            marker: PhantomData,
        }
    }
}

impl<P: Operation> OperationCapture<P> {
    pub fn capture() -> Self {
        Self::new(CaptureContext(PhantomData))
    }
}

impl<P: Operation + Clone> CapturedOperation<P> {
    pub fn operation(&self) -> P {
        self.as_plan_node()
            .downcast::<OperationContext<OperationCapture<P>, P>>()
            .unwrap_or_else(|| {
                panic!(
                    "dynamic operation capture expected an OperationContext<OperationCapture<P>, P>"
                )
            })
            .operation()
            .clone()
    }
}

impl<P: Operation> PlanNode for CaptureContext<P> {}

impl<P: Operation> OptimizerHints for CaptureContext<P> {}

impl<P: Operation> Explain for CaptureContext<P> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        formatter.write_str("operation capture")
    }
}

impl<P: Operation> Estimated for CaptureContext<P> {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        Estimate::UNKNOWN
    }
}

impl<P: Operation> OptimizePlan for CaptureContext<P> {
    type Output = OperationCapture<P>;

    fn optimize(&self, original: &Self::Output, _session: &Session) -> Transformed<Self::Output> {
        Transformed::unchanged(original.clone())
    }
}

impl<P: Operation> EvaluateContext for CaptureContext<P> {
    type Operand = OperationCapture<P>;

    fn evaluate<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<<Self::Operand as EvaluateOperand>::ReturnValue<'a>> {
        panic!("dynamic operation capture reached evaluation")
    }
}

impl<P: Operation> EvaluateOperand for OperationCapture<P> {
    type ReturnValue<'a>
        = ()
    where
        Self: 'a;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::ReturnValue<'a>> {
        self.context.evaluate(graphrecord, cache)
    }
}

impl<P: Operation> Operand for OperationCapture<P> {
    fn context(&self) -> &dyn OperandContext<Self> {
        self.context.as_ref()
    }

    fn as_plan_node(&self) -> &dyn PlanNode {
        self.context.as_ref()
    }

    fn from_context(context: Arc<dyn OperandContext<Self>>) -> Self {
        Self {
            context,
            marker: PhantomData,
        }
    }
}

impl<P: Operation> EvaluateOperand for CapturedOperation<P> {
    type ReturnValue<'a>
        = ()
    where
        Self: 'a;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::ReturnValue<'a>> {
        self.context.evaluate(graphrecord, cache)
    }
}

impl<P: Operation> Operand for CapturedOperation<P> {
    fn context(&self) -> &dyn OperandContext<Self> {
        self.context.as_ref()
    }

    fn as_plan_node(&self) -> &dyn PlanNode {
        self.context.as_ref()
    }

    fn from_context(context: Arc<dyn OperandContext<Self>>) -> Self {
        Self {
            context,
            marker: PhantomData,
        }
    }
}

impl<P, S> Apply<P, S> for OperationCapture<P>
where
    P: Operation<Scope = S>,
    S: OperationScope,
{
    type Output = CapturedOperation<P>;

    fn apply<'a>(
        _graphrecord: &'a GraphRecord,
        _values: Self::ReturnValue<'a>,
        _prepared: P::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        panic!("dynamic operation capture reached typed application")
    }

    fn estimate(_operation: &P, _input: Estimate, _stats: &Stats) -> Estimate {
        Estimate::UNKNOWN
    }
}

impl<P> Apply<P> for DynGroupHandle
where
    P: GroupKernel<DynIndex, DynIndex, DynPayload>,
{
    type Output = P::Output;

    fn apply<'a>(
        graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        prepared: P::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        P::execute(graphrecord, values, prepared)
    }

    fn estimate(operation: &P, input: Estimate, stats: &Stats) -> Estimate {
        operation.estimate(input, stats)
    }
}

pub fn apply_operation<I, P>(input: I, operation: P, descriptor: OperandDescriptor) -> DynOperand
where
    I: Apply<P>,
    P: Operation,
    I::Output: IntoDynOperand,
{
    <I as Apply<P>>::Output::new(OperationContext::new(input, operation)).into_dyn(descriptor)
}

pub fn apply_lane_operation<S, C, P>(
    handles: &DynArityHandle<S>,
    operation: P,
    descriptor: OperandDescriptor,
) -> DynOperand
where
    S: DynStreamShape + IntoDynLaneHandle,
    C: IntoDynArityHandle,
    P: Operation,
    OperandHandle<S, C>: Apply<P>,
    <OperandHandle<S, C> as Apply<P>>::Output: IntoDynOperand,
{
    apply_operation(C::clone_handle(handles), operation, descriptor)
}

impl<P: Operation, S, C> MatchInputs for DynGroupedOperationContext<P, S, C> {
    type Inputs<'a>
        = P::Inputs<'a, DynGroupHandle>
    where
        Self: 'a;

    fn inputs(&self) -> Self::Inputs<'_> {
        OperationInputs::inputs(&self.operation, &self.input)
    }
}

impl<P, S, C> PlanNode for DynGroupedOperationContext<P, S, C>
where
    P: Operation,
    S: DynStreamShape,
    C: DynArity,
    OperandHandle<S, C>: Apply<P>,
    <OperandHandle<S, C> as Apply<P>>::Output: DynPayloadOutput,
{
    fn inputs(&self) -> Vec<&dyn PlanNode> {
        let mut inputs = vec![self.input.as_plan_node()];
        inputs.extend(PlanInputs::inputs(&self.operation));

        inputs
    }

    fn dyn_eq(&self, other: &dyn PlanNode) -> bool {
        let Some(other) = other.downcast::<Self>() else {
            return false;
        };

        self.operation.identity_eq(&other.operation)
            && self.input.as_plan_node().dyn_eq(other.input.as_plan_node())
    }

    fn dyn_hash(&self, mut state: &mut dyn Hasher) {
        self.type_id().hash(&mut state);
        self.operation.identity_hash(&mut state);
        self.input.as_plan_node().dyn_hash(state);
    }
}

impl<P, S, C> OptimizerHints for DynGroupedOperationContext<P, S, C>
where
    P: Operation,
    S: DynStreamShape,
    C: DynArity,
    OperandHandle<S, C>: Apply<P>,
    <OperandHandle<S, C> as Apply<P>>::Output: DynPayloadOutput,
{
    fn commutes_with_filter(&self) -> bool {
        self.operation.commutes_with_filter()
    }

    fn allows_limit_pushdown(&self) -> bool {
        self.operation.allows_limit_pushdown()
    }

    fn is_volatile(&self) -> bool {
        self.operation.is_volatile()
    }

    fn empty_rule(&self) -> EmptyRule {
        self.operation.empty_rule()
    }
}

impl<P, S, C> Explain for DynGroupedOperationContext<P, S, C>
where
    P: Operation,
    S: DynStreamShape,
    C: DynArity,
    OperandHandle<S, C>: Apply<P>,
    <OperandHandle<S, C> as Apply<P>>::Output: DynPayloadOutput,
{
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        formatter.child(&self.input);
        self.operation.describe(formatter)
    }
}

impl<P, S, C> Estimated for DynGroupedOperationContext<P, S, C>
where
    P: Operation,
    S: DynStreamShape,
    C: DynArity,
    OperandHandle<S, C>: Apply<P>,
    <OperandHandle<S, C> as Apply<P>>::Output: DynPayloadOutput,
{
    fn estimate(&self, stats: &Stats) -> Estimate {
        estimate_grouped_operation::<S, C, P>(
            &self.operation,
            self.input.context().estimate(stats),
            stats,
        )
    }
}

impl<P, S, C> OptimizePlan for DynGroupedOperationContext<P, S, C>
where
    P: Operation,
    S: DynStreamShape,
    C: DynArity,
    OperandHandle<S, C>: Apply<P>,
    <OperandHandle<S, C> as Apply<P>>::Output: DynPayloadOutput,
{
    type Output = DynGroupHandle;

    fn optimize(&self, original: &Self::Output, session: &Session) -> Transformed<Self::Output> {
        let input = session.optimize(&self.input);
        let operation = self.operation.optimize(session);

        if !input.is_changed() && !operation.is_changed() {
            return Transformed::unchanged(original.clone());
        }

        Transformed::changed(Self::Output::new(Self {
            input: input.into_parts().0,
            operation: operation.into_parts().0,
            marker: PhantomData,
        }))
    }
}

impl<P, S, C> EvaluateContext for DynGroupedOperationContext<P, S, C>
where
    P: Operation,
    S: DynStreamShape,
    C: DynArity,
    OperandHandle<S, C>: Apply<P>,
    <OperandHandle<S, C> as Apply<P>>::Output: DynPayloadOutput,
{
    type Operand = DynGroupHandle;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<<Self::Operand as EvaluateOperand>::ReturnValue<'a>> {
        let partition = self.input.evaluate(graphrecord, cache)?;
        let prepared = self.operation.prepare(graphrecord, cache)?;

        Ok(lift_grouped_operation::<S, C, P>(
            graphrecord,
            partition,
            &prepared,
        ))
    }
}

pub fn apply_grouped_operation<S, C, P>(
    input: &DynOperand,
    operation: P,
    descriptor: OperandDescriptor,
) -> DynOperand
where
    S: DynStreamShape,
    C: DynArity,
    P: Operation,
    OperandHandle<S, C>: Apply<P>,
    <OperandHandle<S, C> as Apply<P>>::Output: DynPayloadOutput,
{
    let DynHandle::Group(input) = &input.handle else {
        panic!("registry selected dynamic grouped lifting for an ungrouped operand")
    };

    let handle = DynGroupHandle::new(DynGroupedOperationContext {
        input: input.clone(),
        operation,
        marker: PhantomData,
    });

    DynOperand::from_group(handle, descriptor)
}

fn lift_grouped_operation<'a, S, C, P>(
    graphrecord: &'a GraphRecord,
    partition: Partition<'a, DynIndex, DynIndex, DynPayload>,
    prepared: &P::Prepared<'a>,
) -> Partition<'a, DynIndex, DynIndex, DynPayload>
where
    S: DynStreamShape,
    C: DynArity,
    P: Operation,
    OperandHandle<S, C>: Apply<P>,
    <OperandHandle<S, C> as Apply<P>>::Output: DynPayloadOutput,
{
    partition.map_payloads(|_, _, payload| {
        payload.and_then(|yielded| match yielded {
            DynYield::Lane(stream) => {
                let values = project_stream::<S, C>(stream);

                <OperandHandle<S, C> as Apply<P>>::apply(graphrecord, values, prepared.clone()).map(
                    <<OperandHandle<S, C> as Apply<P>>::Output as DynPayloadOutput>::into_yield,
                )
            }
            DynYield::Group(partition) => Ok(DynYield::Group(lift_grouped_operation::<S, C, P>(
                graphrecord,
                partition,
                prepared,
            ))),
        })
    })
}

fn project_stream<S: DynStreamShape, C: DynArity>(
    stream: DynStream<'_>,
) -> <OperandHandle<S, C> as EvaluateOperand>::ReturnValue<'_> {
    S::project::<C>(stream)
}

fn estimate_grouped_operation<S, C, P>(
    operation: &P,
    mut estimate: Estimate,
    stats: &Stats,
) -> Estimate
where
    S: DynStreamShape,
    C: DynArity,
    P: Operation,
    OperandHandle<S, C>: Apply<P>,
    <OperandHandle<S, C> as Apply<P>>::Output: DynPayloadOutput,
{
    let Some(payload) = estimate.per_group.take() else {
        panic!("registry selected dynamic grouped lifting for an estimate without group payload")
    };

    let payload = if payload.per_group.is_some() {
        estimate_grouped_operation::<S, C, P>(operation, *payload, stats)
    } else {
        <OperandHandle<S, C> as Apply<P>>::estimate(operation, *payload, stats)
    };

    estimate.per_group = Some(Box::new(payload));

    estimate
}

impl<P> PlanNode for DynNestedGroupContext<P>
where
    P: GroupKernel<DynIndex, DynIndex, DynPayload>,
    P::Output: DynPayloadOutput + IntoDynOperand,
{
    fn inputs(&self) -> Vec<&dyn PlanNode> {
        let mut inputs = vec![self.input.as_plan_node()];
        inputs.extend(PlanInputs::inputs(&self.operation));

        inputs
    }

    fn dyn_eq(&self, other: &dyn PlanNode) -> bool {
        let Some(other) = other.downcast::<Self>() else {
            return false;
        };

        self.layers == other.layers
            && self.operation.identity_eq(&other.operation)
            && self.input.as_plan_node().dyn_eq(other.input.as_plan_node())
    }

    fn dyn_hash(&self, mut state: &mut dyn Hasher) {
        self.type_id().hash(&mut state);
        self.layers.hash(&mut state);
        self.operation.identity_hash(&mut state);
        self.input.as_plan_node().dyn_hash(state);
    }
}

impl<P> OptimizerHints for DynNestedGroupContext<P>
where
    P: GroupKernel<DynIndex, DynIndex, DynPayload>,
    P::Output: DynPayloadOutput + IntoDynOperand,
{
    fn commutes_with_filter(&self) -> bool {
        self.operation.commutes_with_filter()
    }

    fn allows_limit_pushdown(&self) -> bool {
        self.operation.allows_limit_pushdown()
    }

    fn is_volatile(&self) -> bool {
        self.operation.is_volatile()
    }

    fn empty_rule(&self) -> EmptyRule {
        self.operation.empty_rule()
    }
}

impl<P> Explain for DynNestedGroupContext<P>
where
    P: GroupKernel<DynIndex, DynIndex, DynPayload>,
    P::Output: DynPayloadOutput + IntoDynOperand,
{
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        formatter.child(&self.input);
        self.operation.describe(formatter)
    }
}

impl<P> Estimated for DynNestedGroupContext<P>
where
    P: GroupKernel<DynIndex, DynIndex, DynPayload>,
    P::Output: DynPayloadOutput + IntoDynOperand,
{
    fn estimate(&self, stats: &Stats) -> Estimate {
        estimate_nested_group_operation(
            &self.operation,
            self.input.context().estimate(stats),
            self.layers,
            stats,
        )
    }
}

impl<P> OptimizePlan for DynNestedGroupContext<P>
where
    P: GroupKernel<DynIndex, DynIndex, DynPayload>,
    P::Output: DynPayloadOutput + IntoDynOperand,
{
    type Output = DynGroupHandle;

    fn optimize(&self, original: &Self::Output, session: &Session) -> Transformed<Self::Output> {
        let input = session.optimize(&self.input);
        let operation = self.operation.optimize(session);

        if !input.is_changed() && !operation.is_changed() {
            return Transformed::unchanged(original.clone());
        }

        Transformed::changed(Self::Output::new(Self {
            input: input.into_parts().0,
            operation: operation.into_parts().0,
            layers: self.layers,
        }))
    }
}

impl<P> EvaluateContext for DynNestedGroupContext<P>
where
    P: GroupKernel<DynIndex, DynIndex, DynPayload>,
    P::Output: DynPayloadOutput + IntoDynOperand,
{
    type Operand = DynGroupHandle;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<<Self::Operand as EvaluateOperand>::ReturnValue<'a>> {
        let partition = self.input.evaluate(graphrecord, cache)?;
        let prepared = self.operation.prepare(graphrecord, cache)?;

        Ok(apply_nested_group_operation::<P>(
            graphrecord,
            partition,
            &prepared,
            self.layers,
        ))
    }
}

pub fn apply_group_operation<P>(
    input: &DynOperand,
    operation: P,
    descriptor: OperandDescriptor,
) -> DynOperand
where
    P: GroupKernel<DynIndex, DynIndex, DynPayload>,
    P::Output: DynPayloadOutput + IntoDynOperand,
{
    let DynHandle::Group(handle) = &input.handle else {
        panic!("registry selected a dynamic group operation for an ungrouped operand")
    };

    let depth = input.descriptor().group_depth();

    if depth == 1 {
        return apply_operation(handle.clone(), operation, descriptor);
    }

    let handle = DynGroupHandle::new(DynNestedGroupContext {
        input: handle.clone(),
        operation,
        layers: depth - 1,
    });

    DynOperand::from_group(handle, descriptor)
}

fn apply_nested_group_operation<'a, P>(
    graphrecord: &'a GraphRecord,
    partition: Partition<'a, DynIndex, DynIndex, DynPayload>,
    prepared: &P::Prepared<'a>,
    layers: usize,
) -> Partition<'a, DynIndex, DynIndex, DynPayload>
where
    P: GroupKernel<DynIndex, DynIndex, DynPayload>,
    P::Output: DynPayloadOutput + IntoDynOperand,
{
    if layers != 0 {
        return partition.map_payloads(|_, _, payload| {
            payload.and_then(|yielded| {
                let DynYield::Group(partition) = yielded else {
                    panic!("registry selected a nested group operation for a lane payload")
                };

                if layers == 1 {
                    return <DynGroupHandle as Apply<P>>::apply(
                        graphrecord,
                        partition,
                        prepared.clone(),
                    )
                    .map(P::Output::into_yield);
                }

                Ok(DynYield::Group(apply_nested_group_operation::<P>(
                    graphrecord,
                    partition,
                    prepared,
                    layers - 1,
                )))
            })
        });
    }

    panic!("registry selected nested group dispatch without an outer group layer")
}

fn estimate_nested_group_operation<P>(
    operation: &P,
    mut estimate: Estimate,
    layers: usize,
    stats: &Stats,
) -> Estimate
where
    P: GroupKernel<DynIndex, DynIndex, DynPayload>,
    P::Output: DynPayloadOutput + IntoDynOperand,
{
    let Some(payload) = estimate.per_group.take() else {
        panic!("registry selected a nested group operation for an estimate without group payload")
    };

    let payload = if layers == 1 {
        <DynGroupHandle as Apply<P>>::estimate(operation, *payload, stats)
    } else {
        estimate_nested_group_operation(operation, *payload, layers - 1, stats)
    };

    estimate.per_group = Some(Box::new(payload));

    estimate
}

pub fn invoke_argument_source(
    arguments: &[DynInvokeArgument],
    position: usize,
) -> &DynArgumentSource {
    let Some(DynInvokeArgument::Source(source)) = arguments.get(position) else {
        panic!("registry routed an operation without its declared dynamic argument source")
    };

    source
}

pub fn invoke_operand(arguments: &[DynInvokeArgument], position: usize) -> &DynOperand {
    let Some(DynInvokeArgument::Operand(operand)) = arguments.get(position) else {
        panic!("registry routed an operation without its declared dynamic operand argument")
    };

    operand
}

pub fn invoke_attribute(arguments: &[DynInvokeArgument], position: usize) -> GraphRecordAttribute {
    let Some(DynInvokeArgument::Attribute(attribute)) = arguments.get(position) else {
        panic!("registry routed an operation without its declared attribute argument")
    };

    attribute.clone()
}

pub fn invoke_group(arguments: &[DynInvokeArgument], position: usize) -> Group {
    let Some(DynInvokeArgument::Group(group)) = arguments.get(position) else {
        panic!("registry routed an operation without its declared group argument")
    };

    group.clone()
}

pub fn invoke_direction(arguments: &[DynInvokeArgument], position: usize) -> EdgeDirection {
    let Some(DynInvokeArgument::Direction(direction)) = arguments.get(position) else {
        panic!("registry routed an operation without its declared direction argument")
    };

    *direction
}

pub fn invoke_position(arguments: &[DynInvokeArgument], position: usize) -> usize {
    let Some(DynInvokeArgument::Position(value)) = arguments.get(position) else {
        panic!("registry routed an operation without its declared position argument")
    };

    *value
}

pub fn entity_domain(input: &DynOperand) -> DynEntityDomain {
    let lane = input.descriptor().lane_shape();

    if let ValueRole::EntityReference(index) = lane.value().role() {
        return index_entity_domain(index);
    }

    let LaneShapeDescriptor::Indexed { index, .. } = lane else {
        panic!("registry selected an entity operation for a bare dynamic lane")
    };

    index_entity_domain(index)
}

pub fn innermost_lane_kind(descriptor: &OperandDescriptor) -> DynLaneKind {
    match descriptor.lane_shape() {
        LaneShapeDescriptor::Indexed { value, .. } if value.domain().is::<Mask>() => {
            DynLaneKind::IndexedMask
        }
        LaneShapeDescriptor::Indexed { value, .. } if matches!(value.role(), ValueRole::Unit) => {
            DynLaneKind::IndexedUnit
        }
        LaneShapeDescriptor::Indexed { .. } => DynLaneKind::IndexedValue,
        LaneShapeDescriptor::Bare { value } if value.domain().is::<Mask>() => DynLaneKind::BareMask,
        LaneShapeDescriptor::Bare { .. } => DynLaneKind::BareValue,
    }
}

fn index_entity_domain(index: &IndexDescriptor) -> DynEntityDomain {
    match index {
        IndexDescriptor::Domain(domain) if domain.is::<NodeIndex>() => DynEntityDomain::Node,
        IndexDescriptor::Domain(domain) if domain.is::<EdgeIndex>() => DynEntityDomain::Edge,
        IndexDescriptor::Expanded { child, .. } => index_entity_domain(child),
        IndexDescriptor::Domain(_) | IndexDescriptor::ExpandedSource { .. } => {
            panic!("registry selected an entity operation for a non-entity dynamic index domain")
        }
    }
}
