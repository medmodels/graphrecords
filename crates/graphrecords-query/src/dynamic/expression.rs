use super::{
    DynArity, DynElementOperation, DynExpressionProjection, DynIndex, DynInvokeArgument,
    DynPayload, DynShapeProjection, DynStream, DynStreamShape, DynTerminal, DynValue, DynYield,
    OperationCapture, apply_grouped_operation, apply_lane_operation,
};
use crate::{
    Bare, Cache, Definite, ElementShape, EvaluateExpression, Explain, ExplainFormatter,
    Explanation, Expression, Failure, Indexed, Mask, Multiple, Ordered, QueryResult,
    ReturnExpression, Single, Unit, Unordered,
    error::dispatch::OperationNotApplicable,
    execution::{CacheableShape, EvaluationCache},
    explain::{CompactPlan, write_truncated_plan},
    expressions::{
        AllEdges, AllGroups, AllNodes, EdgesExpression, ExpressionHandle, GroupedExpression,
        GroupsExpression, NodesExpression,
    },
    operations::{ElementKernel, TransitionOperation},
    optimizer::{OptimizationReport, Optimizer, Stats},
    registry::{
        ArgumentDescriptor, ArityDescriptor, ExpressionDescriptor, IndexDescriptor,
        LaneShapeDescriptor, OperationRegistry, OrderDescriptor, ValueDescriptor, ValueRole,
    },
    traits::Transition,
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{EdgeIndex, GroupIndex, NodeIndex},
};
use std::{
    fmt::{self, Display, Formatter},
    sync::OnceLock,
};

pub type DynGroupHandle = GroupedExpression<DynIndex, DynIndex, DynPayload>;

pub enum DynArityHandle<S: ElementShape> {
    MultipleOrdered(ExpressionHandle<S, Multiple<Ordered>>),
    MultipleUnordered(ExpressionHandle<S, Multiple<Unordered>>),
    Single(ExpressionHandle<S, Single>),
    Definite(ExpressionHandle<S, Definite>),
}

#[derive(Clone)]
pub enum DynLaneHandle {
    IndexedValue(DynArityHandle<Indexed<DynIndex, DynValue>>),
    IndexedMask(DynArityHandle<Indexed<DynIndex, Mask>>),
    IndexedUnit(DynArityHandle<Indexed<DynIndex, Unit>>),
    BareValue(DynArityHandle<Bare<DynValue>>),
    BareMask(DynArityHandle<Bare<Mask>>),
}

#[derive(Clone)]
pub enum DynHandle {
    Lane(DynLaneHandle),
    Group(DynGroupHandle),
}

#[derive(Clone)]
pub struct DynExpression {
    pub(crate) handle: DynHandle,
    descriptor: ExpressionDescriptor,
}

pub struct DynExplanation {
    expression: DynExpression,
    report: OptimizationReport,
}

#[must_use]
pub fn nodes() -> DynExpression {
    let handle = NodesExpression::new(AllNodes).erase_expression();
    let descriptor = ExpressionDescriptor::Lane {
        shape: LaneShapeDescriptor::Indexed {
            index: IndexDescriptor::domain::<NodeIndex>(),
            value: ValueDescriptor::unit(),
        },
        arity: ArityDescriptor::Multiple {
            order: OrderDescriptor::Unordered,
        },
    };

    DynExpression::from_lane(handle, descriptor)
}

#[must_use]
pub fn edges() -> DynExpression {
    let handle = EdgesExpression::new(AllEdges).erase_expression();
    let descriptor = ExpressionDescriptor::Lane {
        shape: LaneShapeDescriptor::Indexed {
            index: IndexDescriptor::domain::<EdgeIndex>(),
            value: ValueDescriptor::unit(),
        },
        arity: ArityDescriptor::Multiple {
            order: OrderDescriptor::Unordered,
        },
    };

    DynExpression::from_lane(handle, descriptor)
}

#[must_use]
pub fn groups() -> DynExpression {
    let handle = GroupsExpression::new(AllGroups).erase_expression();
    let descriptor = ExpressionDescriptor::Lane {
        shape: LaneShapeDescriptor::Indexed {
            index: IndexDescriptor::domain::<GroupIndex>(),
            value: ValueDescriptor::unit(),
        },
        arity: ArityDescriptor::Multiple {
            order: OrderDescriptor::Unordered,
        },
    };

    DynExpression::from_lane(handle, descriptor)
}

pub trait IntoDynArityHandle: DynArity + Sized {
    fn into_handle<S: DynStreamShape>(handle: ExpressionHandle<S, Self>) -> DynArityHandle<S>;

    fn clone_handle<S: DynStreamShape>(handles: &DynArityHandle<S>) -> ExpressionHandle<S, Self>;
}

impl IntoDynArityHandle for Multiple<Ordered> {
    fn into_handle<S: DynStreamShape>(handle: ExpressionHandle<S, Self>) -> DynArityHandle<S> {
        DynArityHandle::MultipleOrdered(handle)
    }

    fn clone_handle<S: DynStreamShape>(handles: &DynArityHandle<S>) -> ExpressionHandle<S, Self> {
        let DynArityHandle::MultipleOrdered(handle) = handles else {
            panic!("registry selected an ordered-multiple operation for a different dynamic arity")
        };
        handle.clone()
    }
}

impl IntoDynArityHandle for Multiple<Unordered> {
    fn into_handle<S: DynStreamShape>(handle: ExpressionHandle<S, Self>) -> DynArityHandle<S> {
        DynArityHandle::MultipleUnordered(handle)
    }

    fn clone_handle<S: DynStreamShape>(handles: &DynArityHandle<S>) -> ExpressionHandle<S, Self> {
        let DynArityHandle::MultipleUnordered(handle) = handles else {
            panic!(
                "registry selected an unordered-multiple operation for a different dynamic arity"
            )
        };
        handle.clone()
    }
}

impl IntoDynArityHandle for Single {
    fn into_handle<S: DynStreamShape>(handle: ExpressionHandle<S, Self>) -> DynArityHandle<S> {
        DynArityHandle::Single(handle)
    }

    fn clone_handle<S: DynStreamShape>(handles: &DynArityHandle<S>) -> ExpressionHandle<S, Self> {
        let DynArityHandle::Single(handle) = handles else {
            panic!("registry selected a single operation for a different dynamic arity")
        };
        handle.clone()
    }
}

impl IntoDynArityHandle for Definite {
    fn into_handle<S: DynStreamShape>(handle: ExpressionHandle<S, Self>) -> DynArityHandle<S> {
        DynArityHandle::Definite(handle)
    }

    fn clone_handle<S: DynStreamShape>(handles: &DynArityHandle<S>) -> ExpressionHandle<S, Self> {
        let DynArityHandle::Definite(handle) = handles else {
            panic!("registry selected a definite operation for a different dynamic arity")
        };
        handle.clone()
    }
}

pub trait IntoDynLaneHandle: DynStreamShape + Sized {
    fn into_lane<C: IntoDynArityHandle>(handle: ExpressionHandle<Self, C>) -> DynLaneHandle;
}

pub trait DynLaneState: IntoDynLaneHandle {
    fn handles(handle: &DynLaneHandle) -> &DynArityHandle<Self>;
}

pub trait IntoDynExpression: Expression + Sized {
    fn into_dyn(self, descriptor: ExpressionDescriptor) -> DynExpression;
}

impl IntoDynLaneHandle for Indexed<DynIndex, DynValue> {
    fn into_lane<C: IntoDynArityHandle>(handle: ExpressionHandle<Self, C>) -> DynLaneHandle {
        DynLaneHandle::IndexedValue(C::into_handle(handle))
    }
}

impl DynLaneState for Indexed<DynIndex, DynValue> {
    fn handles(handle: &DynLaneHandle) -> &DynArityHandle<Self> {
        let DynLaneHandle::IndexedValue(handles) = handle else {
            panic!("registry selected an indexed dynamic-value operation for a different lane")
        };
        handles
    }
}

impl IntoDynLaneHandle for Indexed<DynIndex, Mask> {
    fn into_lane<C: IntoDynArityHandle>(handle: ExpressionHandle<Self, C>) -> DynLaneHandle {
        DynLaneHandle::IndexedMask(C::into_handle(handle))
    }
}

impl DynLaneState for Indexed<DynIndex, Mask> {
    fn handles(handle: &DynLaneHandle) -> &DynArityHandle<Self> {
        let DynLaneHandle::IndexedMask(handles) = handle else {
            panic!("registry selected an indexed mask operation for a different lane")
        };
        handles
    }
}

impl IntoDynLaneHandle for Indexed<DynIndex, Unit> {
    fn into_lane<C: IntoDynArityHandle>(handle: ExpressionHandle<Self, C>) -> DynLaneHandle {
        DynLaneHandle::IndexedUnit(C::into_handle(handle))
    }
}

impl DynLaneState for Indexed<DynIndex, Unit> {
    fn handles(handle: &DynLaneHandle) -> &DynArityHandle<Self> {
        let DynLaneHandle::IndexedUnit(handles) = handle else {
            panic!("registry selected an indexed unit operation for a different lane")
        };
        handles
    }
}

impl IntoDynLaneHandle for Bare<DynValue> {
    fn into_lane<C: IntoDynArityHandle>(handle: ExpressionHandle<Self, C>) -> DynLaneHandle {
        DynLaneHandle::BareValue(C::into_handle(handle))
    }
}

impl DynLaneState for Bare<DynValue> {
    fn handles(handle: &DynLaneHandle) -> &DynArityHandle<Self> {
        let DynLaneHandle::BareValue(handles) = handle else {
            panic!("registry selected a bare dynamic-value operation for a different lane")
        };
        handles
    }
}

impl IntoDynLaneHandle for Bare<Mask> {
    fn into_lane<C: IntoDynArityHandle>(handle: ExpressionHandle<Self, C>) -> DynLaneHandle {
        DynLaneHandle::BareMask(C::into_handle(handle))
    }
}

impl DynLaneState for Bare<Mask> {
    fn handles(handle: &DynLaneHandle) -> &DynArityHandle<Self> {
        let DynLaneHandle::BareMask(handles) = handle else {
            panic!("registry selected a bare mask operation for a different lane")
        };
        handles
    }
}

impl<S, C> IntoDynExpression for ExpressionHandle<S, C>
where
    S: IntoDynLaneHandle,
    C: IntoDynArityHandle,
{
    fn into_dyn(self, descriptor: ExpressionDescriptor) -> DynExpression {
        DynExpression::from_lane(self, descriptor)
    }
}

impl IntoDynExpression for DynGroupHandle {
    fn into_dyn(self, descriptor: ExpressionDescriptor) -> DynExpression {
        DynExpression::from_group(self, descriptor)
    }
}

impl<S: ElementShape> Clone for DynArityHandle<S> {
    fn clone(&self) -> Self {
        match self {
            Self::MultipleOrdered(handle) => Self::MultipleOrdered(handle.clone()),
            Self::MultipleUnordered(handle) => Self::MultipleUnordered(handle.clone()),
            Self::Single(handle) => Self::Single(handle.clone()),
            Self::Definite(handle) => Self::Definite(handle.clone()),
        }
    }
}

impl<S: DynStreamShape> DynArityHandle<S> {
    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache,
    ) -> QueryResult<DynStream<'a>> {
        match self {
            Self::MultipleOrdered(handle) => handle
                .evaluate(graphrecord, cache)
                .map(S::erase::<Multiple<Ordered>>),
            Self::MultipleUnordered(handle) => handle
                .evaluate(graphrecord, cache)
                .map(S::erase::<Multiple<Unordered>>),
            Self::Single(handle) => handle.evaluate(graphrecord, cache).map(S::erase::<Single>),
            Self::Definite(handle) => handle
                .evaluate(graphrecord, cache)
                .map(S::erase::<Definite>),
        }
    }

    fn cache(&self) -> Self
    where
        S: CacheableShape,
    {
        match self {
            Self::MultipleOrdered(handle) => Self::MultipleOrdered(handle.cache()),
            Self::MultipleUnordered(handle) => Self::MultipleUnordered(handle.cache()),
            Self::Single(handle) => Self::Single(handle.cache()),
            Self::Definite(handle) => Self::Definite(handle.cache()),
        }
    }

    fn optimize(&self, optimizer: &Optimizer, stats: &Stats) -> (Self, OptimizationReport) {
        match self {
            Self::MultipleOrdered(handle) => {
                let (handle, report) = optimizer.run_reported(stats, handle);
                (Self::MultipleOrdered(handle), report)
            }
            Self::MultipleUnordered(handle) => {
                let (handle, report) = optimizer.run_reported(stats, handle);
                (Self::MultipleUnordered(handle), report)
            }
            Self::Single(handle) => {
                let (handle, report) = optimizer.run_reported(stats, handle);
                (Self::Single(handle), report)
            }
            Self::Definite(handle) => {
                let (handle, report) = optimizer.run_reported(stats, handle);
                (Self::Definite(handle), report)
            }
        }
    }

    fn explanation(&self) -> Explanation<'_> {
        match self {
            Self::MultipleOrdered(handle) => handle.explain(),
            Self::MultipleUnordered(handle) => handle.explain(),
            Self::Single(handle) => handle.explain(),
            Self::Definite(handle) => handle.explain(),
        }
    }

    const fn matches_arity(&self, descriptor: ArityDescriptor) -> bool {
        matches!(
            (self, descriptor),
            (
                Self::MultipleOrdered(_),
                ArityDescriptor::Multiple {
                    order: OrderDescriptor::Ordered
                }
            ) | (
                Self::MultipleUnordered(_),
                ArityDescriptor::Multiple {
                    order: OrderDescriptor::Unordered
                }
            ) | (Self::Single(_), ArityDescriptor::Single)
                | (Self::Definite(_), ArityDescriptor::Definite)
        )
    }
}

impl<S: ElementShape> Explain for DynArityHandle<S> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        match self {
            Self::MultipleOrdered(handle) => handle.describe(formatter),
            Self::MultipleUnordered(handle) => handle.describe(formatter),
            Self::Single(handle) => handle.describe(formatter),
            Self::Definite(handle) => handle.describe(formatter),
        }
    }
}

impl DynLaneHandle {
    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache,
    ) -> QueryResult<DynStream<'a>> {
        match self {
            Self::IndexedValue(handles) => handles.evaluate(graphrecord, cache),
            Self::IndexedMask(handles) => handles.evaluate(graphrecord, cache),
            Self::IndexedUnit(handles) => handles.evaluate(graphrecord, cache),
            Self::BareValue(handles) => handles.evaluate(graphrecord, cache),
            Self::BareMask(handles) => handles.evaluate(graphrecord, cache),
        }
    }

    fn cache(&self) -> Self {
        match self {
            Self::IndexedValue(handles) => Self::IndexedValue(handles.cache()),
            Self::IndexedMask(handles) => Self::IndexedMask(handles.cache()),
            Self::IndexedUnit(handles) => Self::IndexedUnit(handles.cache()),
            Self::BareValue(handles) => Self::BareValue(handles.cache()),
            Self::BareMask(handles) => Self::BareMask(handles.cache()),
        }
    }

    fn optimize(&self, optimizer: &Optimizer, stats: &Stats) -> (Self, OptimizationReport) {
        match self {
            Self::IndexedValue(handles) => {
                let (handles, report) = handles.optimize(optimizer, stats);
                (Self::IndexedValue(handles), report)
            }
            Self::IndexedMask(handles) => {
                let (handles, report) = handles.optimize(optimizer, stats);
                (Self::IndexedMask(handles), report)
            }
            Self::IndexedUnit(handles) => {
                let (handles, report) = handles.optimize(optimizer, stats);
                (Self::IndexedUnit(handles), report)
            }
            Self::BareValue(handles) => {
                let (handles, report) = handles.optimize(optimizer, stats);
                (Self::BareValue(handles), report)
            }
            Self::BareMask(handles) => {
                let (handles, report) = handles.optimize(optimizer, stats);
                (Self::BareMask(handles), report)
            }
        }
    }

    fn explanation(&self) -> Explanation<'_> {
        match self {
            Self::IndexedValue(handles) => handles.explanation(),
            Self::IndexedMask(handles) => handles.explanation(),
            Self::IndexedUnit(handles) => handles.explanation(),
            Self::BareValue(handles) => handles.explanation(),
            Self::BareMask(handles) => handles.explanation(),
        }
    }
}

impl Explain for DynLaneHandle {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        match self {
            Self::IndexedValue(handles) => handles.describe(formatter),
            Self::IndexedMask(handles) => handles.describe(formatter),
            Self::IndexedUnit(handles) => handles.describe(formatter),
            Self::BareValue(handles) => handles.describe(formatter),
            Self::BareMask(handles) => handles.describe(formatter),
        }
    }
}

impl DynExpression {
    #[must_use]
    pub fn from_lane<S, C>(handle: ExpressionHandle<S, C>, descriptor: ExpressionDescriptor) -> Self
    where
        S: IntoDynLaneHandle,
        C: IntoDynArityHandle,
    {
        let handle = DynHandle::Lane(S::into_lane(handle));
        Self::new(handle, descriptor)
    }

    #[must_use]
    pub fn from_group(handle: DynGroupHandle, descriptor: ExpressionDescriptor) -> Self {
        Self::new(DynHandle::Group(handle), descriptor)
    }

    fn new(handle: DynHandle, descriptor: ExpressionDescriptor) -> Self {
        Self::verify_descriptor(&handle, &descriptor);
        Self { handle, descriptor }
    }

    #[must_use]
    pub const fn descriptor(&self) -> &ExpressionDescriptor {
        &self.descriptor
    }

    #[must_use]
    pub fn cache(&self) -> Self {
        let handle = match &self.handle {
            DynHandle::Lane(handle) => DynHandle::Lane(handle.cache()),
            DynHandle::Group(handle) => DynHandle::Group(handle.cache()),
        };
        Self::new(handle, self.descriptor.clone())
    }

    pub fn invoke(&self, method: &str, arguments: &[DynInvokeArgument]) -> QueryResult<Self> {
        static REGISTRY: OnceLock<OperationRegistry> = OnceLock::new();

        let argument_descriptors: Vec<_> = arguments
            .iter()
            .map(DynInvokeArgument::descriptor)
            .collect();

        let registry = REGISTRY.get_or_init(OperationRegistry::builtins);

        let Some((output, applier)) =
            registry.resolve_dispatch(method, &self.descriptor, &argument_descriptors)
        else {
            return self.inapplicable(method, argument_descriptors);
        };

        Ok(applier(self, arguments, output))
    }

    pub(crate) fn inapplicable(
        &self,
        method: &str,
        arguments: Vec<ArgumentDescriptor>,
    ) -> QueryResult<Self> {
        Err(Failure::new(
            OperationNotApplicable::new(method.to_string(), self.descriptor.clone(), arguments),
            "dynamic invocation",
        ))
    }

    pub(crate) fn erase_mask_lane(&self) -> Self {
        type Emission =
            <TransitionOperation<DynValue> as ElementKernel<Indexed<DynIndex, Mask>>>::Emission;
        type DynamicOperation = DynElementOperation<
            TransitionOperation<DynValue>,
            Indexed<DynIndex, Mask>,
            Indexed<DynIndex, DynValue>,
            Emission,
        >;
        type DynamicShape = <Indexed<DynIndex, Mask> as DynShapeProjection>::Dynamic;

        let capture = OperationCapture::<TransitionOperation<DynValue>>::capture();
        let operation = capture.transition::<DynValue>().operation();
        let output = self
            .descriptor()
            .with_lane_value(ValueDescriptor::index(IndexDescriptor::domain::<bool>()));

        let operation = DynamicOperation::new(operation);

        let DynHandle::Lane(handle) = &self.handle else {
            return match self.descriptor().lane_arity() {
                ArityDescriptor::Multiple {
                    order: OrderDescriptor::Ordered,
                } => apply_grouped_operation::<DynamicShape, Multiple<Ordered>, _>(
                    self, operation, output,
                ),
                ArityDescriptor::Multiple {
                    order: OrderDescriptor::Unordered,
                } => apply_grouped_operation::<DynamicShape, Multiple<Unordered>, _>(
                    self, operation, output,
                ),
                ArityDescriptor::Single => {
                    apply_grouped_operation::<DynamicShape, Single, _>(self, operation, output)
                }
                ArityDescriptor::Definite => {
                    apply_grouped_operation::<DynamicShape, Definite, _>(self, operation, output)
                }
            };
        };

        let handles = <DynamicShape as DynLaneState>::handles(handle);

        match handles {
            DynArityHandle::MultipleOrdered(_) => {
                apply_lane_operation::<DynamicShape, Multiple<Ordered>, _>(
                    handles, operation, output,
                )
            }
            DynArityHandle::MultipleUnordered(_) => {
                apply_lane_operation::<DynamicShape, Multiple<Unordered>, _>(
                    handles, operation, output,
                )
            }
            DynArityHandle::Single(_) => {
                apply_lane_operation::<DynamicShape, Single, _>(handles, operation, output)
            }
            DynArityHandle::Definite(_) => {
                apply_lane_operation::<DynamicShape, Definite, _>(handles, operation, output)
            }
        }
    }

    pub fn evaluate(&self, graphrecord: &GraphRecord) -> QueryResult<DynTerminal> {
        let optimized = self.optimize_for(graphrecord).0;
        let cache = EvaluationCache::new(graphrecord);

        ReturnExpression::evaluate(&optimized, graphrecord, &cache)
    }

    #[must_use]
    pub fn explain(&self, graphrecord: &GraphRecord) -> DynExplanation {
        let (expression, report) = self.optimize_for(graphrecord);

        DynExplanation { expression, report }
    }

    #[must_use]
    pub fn explanation(&self) -> Explanation<'_> {
        match &self.handle {
            DynHandle::Lane(handle) => handle.explanation(),
            DynHandle::Group(handle) => handle.explain(),
        }
    }

    fn optimize_for(&self, graphrecord: &GraphRecord) -> (Self, OptimizationReport) {
        let optimizer = Optimizer::builtin();

        if optimizer.is_empty() {
            return (self.clone(), OptimizationReport::default());
        }

        let stats = Stats::new(graphrecord);

        self.optimize_with(optimizer, &stats)
    }

    fn optimize_with(&self, optimizer: &Optimizer, stats: &Stats) -> (Self, OptimizationReport) {
        let (handle, report) = match &self.handle {
            DynHandle::Lane(handle) => {
                let (handle, report) = handle.optimize(optimizer, stats);
                (DynHandle::Lane(handle), report)
            }
            DynHandle::Group(handle) => {
                let (handle, report) = optimizer.run_reported(stats, handle);
                (DynHandle::Group(handle), report)
            }
        };

        (Self::new(handle, self.descriptor.clone()), report)
    }

    fn verify_descriptor(handle: &DynHandle, descriptor: &ExpressionDescriptor) {
        match (handle, descriptor) {
            (DynHandle::Lane(handle), ExpressionDescriptor::Lane { shape, arity }) => {
                Self::verify_lane_shape(handle, shape);
                Self::verify_lane_arity(handle, *arity);
            }
            (DynHandle::Group(_), ExpressionDescriptor::Group { .. }) => {}
            _ => {
                panic!(
                    "registry paired a dynamic expression handle with a different descriptor state"
                )
            }
        }
    }

    fn verify_lane_shape(handle: &DynLaneHandle, descriptor: &LaneShapeDescriptor) {
        let matches = match (handle, descriptor) {
            (DynLaneHandle::IndexedValue(_), LaneShapeDescriptor::Indexed { value, .. })
            | (DynLaneHandle::BareValue(_), LaneShapeDescriptor::Bare { value }) => {
                !value.domain().is::<Mask>() && !matches!(value.role(), ValueRole::Unit)
            }
            (DynLaneHandle::IndexedMask(_), LaneShapeDescriptor::Indexed { value, .. })
            | (DynLaneHandle::BareMask(_), LaneShapeDescriptor::Bare { value }) => {
                value == &ValueDescriptor::value::<Mask>()
            }
            (DynLaneHandle::IndexedUnit(_), LaneShapeDescriptor::Indexed { value, .. }) => {
                value == &ValueDescriptor::unit()
            }
            _ => false,
        };

        if matches {
            return;
        }

        panic!("registry paired a dynamic lane handle with a different shape descriptor")
    }

    fn verify_lane_arity(handle: &DynLaneHandle, descriptor: ArityDescriptor) {
        let matches = match handle {
            DynLaneHandle::IndexedValue(handles) => handles.matches_arity(descriptor),
            DynLaneHandle::IndexedMask(handles) => handles.matches_arity(descriptor),
            DynLaneHandle::IndexedUnit(handles) => handles.matches_arity(descriptor),
            DynLaneHandle::BareValue(handles) => handles.matches_arity(descriptor),
            DynLaneHandle::BareMask(handles) => handles.matches_arity(descriptor),
        };

        if matches {
            return;
        }

        panic!("registry paired a dynamic lane handle with a different arity descriptor")
    }
}

impl Explain for DynExpression {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        match &self.handle {
            DynHandle::Lane(handle) => handle.describe(formatter),
            DynHandle::Group(handle) => handle.describe(formatter),
        }
    }
}

impl fmt::Debug for DynExpression {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("Expression [")?;
        write_truncated_plan(formatter, self)?;
        formatter.write_str("]")
    }
}

impl ReturnExpression for DynExpression {
    type ReturnValue<'a>
        = DynTerminal
    where
        Self: 'a;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache,
    ) -> QueryResult<Self::ReturnValue<'a>> {
        match &self.handle {
            DynHandle::Lane(handle) => handle
                .evaluate(graphrecord, cache)
                .map(DynYield::Lane)
                .map(|yielded| DynTerminal::from_yield(graphrecord, yielded)),
            DynHandle::Group(handle) => handle
                .evaluate(graphrecord, cache)
                .map(DynYield::Group)
                .map(|yielded| DynTerminal::from_yield(graphrecord, yielded)),
        }
    }

    fn optimize(self, optimizer: &Optimizer, stats: &Stats) -> (Self, OptimizationReport) {
        self.optimize_with(optimizer, stats)
    }

    fn fmt_plan(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", Explanation::new(self))
    }

    fn compact_plan(&self) -> String {
        CompactPlan::new(self).to_string()
    }
}

impl Display for DynExplanation {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.expression.explanation())?;

        if !self.report.phases.is_empty() {
            write!(formatter, "\n\noptimization:\n{}", self.report.display())?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::{edges, groups, nodes};
    use crate::{
        Mask, Scalar,
        dynamic::{DynArityContainer, DynInvokeArgument, DynTerminal, DynTerminalLane, DynValue},
        registry::{
            ArityDescriptor, ExpressionDescriptor, IndexDescriptor, LaneShapeDescriptor,
            OrderDescriptor, ValueDescriptor,
        },
    };
    use graphrecords_core::{
        GraphRecord,
        graphrecord::{AttributeMap, EdgeIndex, GroupIndex, NodeIndex, Value},
    };
    use std::collections::HashMap;

    fn create_nodes() -> Vec<(NodeIndex, AttributeMap)> {
        vec![
            (
                "0".into(),
                HashMap::from([("lorem".into(), "ipsum".into())]),
            ),
            (
                "1".into(),
                HashMap::from([("amet".into(), "consectetur".into())]),
            ),
            (
                "2".into(),
                HashMap::from([("adipiscing".into(), "elit".into())]),
            ),
            ("3".into(), HashMap::new()),
        ]
    }

    fn create_edges() -> Vec<(NodeIndex, NodeIndex, AttributeMap)> {
        vec![
            (
                "0".into(),
                "1".into(),
                HashMap::from([("sed".into(), "do".into())]),
            ),
            (
                "1".into(),
                "2".into(),
                HashMap::from([("incididunt".into(), "ut".into())]),
            ),
        ]
    }

    fn create_graphrecord() -> GraphRecord {
        let nodes = create_nodes();
        let edges = create_edges();

        GraphRecord::new()
            .add_nodes(nodes)
            .unwrap()
            .add_edges(edges)
            .unwrap()
    }

    fn create_unit_node_descriptor() -> ExpressionDescriptor {
        ExpressionDescriptor::Lane {
            shape: LaneShapeDescriptor::Indexed {
                index: IndexDescriptor::domain::<NodeIndex>(),
                value: ValueDescriptor::unit(),
            },
            arity: ArityDescriptor::Multiple {
                order: OrderDescriptor::Unordered,
            },
        }
    }

    fn create_unit_edge_descriptor() -> ExpressionDescriptor {
        ExpressionDescriptor::Lane {
            shape: LaneShapeDescriptor::Indexed {
                index: IndexDescriptor::domain::<EdgeIndex>(),
                value: ValueDescriptor::unit(),
            },
            arity: ArityDescriptor::Multiple {
                order: OrderDescriptor::Unordered,
            },
        }
    }

    fn create_unit_group_descriptor() -> ExpressionDescriptor {
        ExpressionDescriptor::Lane {
            shape: LaneShapeDescriptor::Indexed {
                index: IndexDescriptor::domain::<GroupIndex>(),
                value: ValueDescriptor::unit(),
            },
            arity: ArityDescriptor::Multiple {
                order: OrderDescriptor::Unordered,
            },
        }
    }

    fn create_scalar_node_descriptor() -> ExpressionDescriptor {
        ExpressionDescriptor::Lane {
            shape: LaneShapeDescriptor::Indexed {
                index: IndexDescriptor::domain::<NodeIndex>(),
                value: ValueDescriptor::value::<Scalar>(),
            },
            arity: ArityDescriptor::Multiple {
                order: OrderDescriptor::Unordered,
            },
        }
    }

    fn create_mask_node_descriptor() -> ExpressionDescriptor {
        ExpressionDescriptor::Lane {
            shape: LaneShapeDescriptor::Indexed {
                index: IndexDescriptor::domain::<NodeIndex>(),
                value: ValueDescriptor::value::<Mask>(),
            },
            arity: ArityDescriptor::Multiple {
                order: OrderDescriptor::Unordered,
            },
        }
    }

    fn create_scalar_count_descriptor() -> ExpressionDescriptor {
        ExpressionDescriptor::Lane {
            shape: LaneShapeDescriptor::Bare {
                value: ValueDescriptor::value::<Scalar>(),
            },
            arity: ArityDescriptor::Definite,
        }
    }

    #[test]
    fn test_nodes() {
        assert_eq!(&create_unit_node_descriptor(), nodes().descriptor());
    }

    #[test]
    fn test_edges() {
        assert_eq!(&create_unit_edge_descriptor(), edges().descriptor());
    }

    #[test]
    fn test_groups() {
        assert_eq!(&create_unit_group_descriptor(), groups().descriptor());
    }

    #[test]
    fn test_cache() {
        assert_eq!(&create_unit_node_descriptor(), nodes().cache().descriptor());
    }

    #[test]
    fn test_invoke() {
        let attribute = nodes()
            .invoke("attribute", &[DynInvokeArgument::Attribute("lorem".into())])
            .unwrap();

        assert_eq!(&create_scalar_node_descriptor(), attribute.descriptor());

        let is_null = attribute.invoke("is_null", &[]).unwrap();

        assert_eq!(&create_mask_node_descriptor(), is_null.descriptor());

        let count = nodes().invoke("count", &[]).unwrap();

        assert_eq!(&create_scalar_count_descriptor(), count.descriptor());
    }

    #[test]
    fn test_invalid_invoke() {
        assert!(nodes().invoke("lorem", &[]).is_err());
        assert!(nodes().invoke("sum", &[]).is_err());
        assert!(nodes().invoke("attribute", &[]).is_err());
        assert!(nodes().invoke("first", &[]).is_err());
    }

    #[test]
    fn test_evaluate() {
        let graphrecord = create_graphrecord();

        assert!(matches!(
            nodes().evaluate(&graphrecord).unwrap(),
            DynTerminal::Lane(DynTerminalLane::IndexedUnit(
                DynArityContainer::MultipleUnordered(elements)
            )) if elements.len() == 4
        ));
        assert!(matches!(
            edges().evaluate(&graphrecord).unwrap(),
            DynTerminal::Lane(DynTerminalLane::IndexedUnit(
                DynArityContainer::MultipleUnordered(elements)
            )) if elements.len() == 2
        ));

        let count = nodes().invoke("count", &[]).unwrap();

        assert!(matches!(
            count.evaluate(&graphrecord).unwrap(),
            DynTerminal::Lane(DynTerminalLane::BareValue(DynArityContainer::Definite(Ok(
                DynValue::Scalar(Value::Int(4))
            ))))
        ));

        let attribute = nodes()
            .invoke("attribute", &[DynInvokeArgument::Attribute("lorem".into())])
            .unwrap();

        assert!(matches!(
            attribute.evaluate(&graphrecord).unwrap(),
            DynTerminal::Lane(DynTerminalLane::IndexedValue(
                DynArityContainer::MultipleUnordered(elements)
            )) if elements.len() == 4
                && elements.iter().filter(|element| element.1.is_ok()).count() == 1
        ));
    }
}
