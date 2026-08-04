use super::{
    DynArity, DynIndex, DynInvokeArgument, DynOperandProjection, DynPayload, DynStream,
    DynStreamShape, DynTerminal, DynValue, DynYield,
};
use crate::{
    Bare, Cache, Definite, ElementShape, EvaluateOperand, Explanation, Failure, Indexed, Mask,
    Multiple, Operand, Ordered, QueryResult, Single, Unit, Unordered,
    error::dispatch::OperationNotApplicable,
    execution::{CacheableShape, EvaluationCache},
    operands::{AllEdges, AllNodes, EdgesOperand, GroupOperand, NodesOperand, OperandHandle},
    optimizer::{OptimizationReport, Optimizer, Stats},
    registry::{
        ArgumentDescriptor, ArityDescriptor, IndexDescriptor, LaneShapeDescriptor,
        OperandDescriptor, OperationRegistry, OrderDescriptor, ValueDescriptor, ValueRole,
    },
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{EdgeIndex, NodeIndex},
};
use std::{
    fmt::{self, Display, Formatter},
    sync::OnceLock,
};

pub type DynGroupHandle = GroupOperand<DynIndex, DynIndex, DynPayload>;

pub const TRANSITION: &str = "transition";

pub enum DynArityHandle<S: ElementShape> {
    MultipleOrdered(OperandHandle<S, Multiple<Ordered>>),
    MultipleUnordered(OperandHandle<S, Multiple<Unordered>>),
    Single(OperandHandle<S, Single>),
    Definite(OperandHandle<S, Definite>),
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
pub struct DynOperand {
    pub(crate) handle: DynHandle,
    descriptor: OperandDescriptor,
}

pub struct DynExplanation {
    operand: DynOperand,
    report: OptimizationReport,
}

#[must_use]
pub fn query_nodes() -> DynOperand {
    let handle = NodesOperand::new(AllNodes).erase_operand();
    let descriptor = OperandDescriptor::Lane {
        shape: LaneShapeDescriptor::Indexed {
            index: IndexDescriptor::domain::<NodeIndex>(),
            value: ValueDescriptor::unit(),
        },
        arity: ArityDescriptor::Multiple {
            order: OrderDescriptor::Unordered,
        },
    };

    DynOperand::from_lane(handle, descriptor)
}

#[must_use]
pub fn query_edges() -> DynOperand {
    let handle = EdgesOperand::new(AllEdges).erase_operand();
    let descriptor = OperandDescriptor::Lane {
        shape: LaneShapeDescriptor::Indexed {
            index: IndexDescriptor::domain::<EdgeIndex>(),
            value: ValueDescriptor::unit(),
        },
        arity: ArityDescriptor::Multiple {
            order: OrderDescriptor::Unordered,
        },
    };

    DynOperand::from_lane(handle, descriptor)
}

pub trait IntoDynArityHandle: DynArity + Sized {
    fn into_handle<S: DynStreamShape>(handle: OperandHandle<S, Self>) -> DynArityHandle<S>;

    fn clone_handle<S: DynStreamShape>(handles: &DynArityHandle<S>) -> OperandHandle<S, Self>;
}

impl IntoDynArityHandle for Multiple<Ordered> {
    fn into_handle<S: DynStreamShape>(handle: OperandHandle<S, Self>) -> DynArityHandle<S> {
        DynArityHandle::MultipleOrdered(handle)
    }

    fn clone_handle<S: DynStreamShape>(handles: &DynArityHandle<S>) -> OperandHandle<S, Self> {
        let DynArityHandle::MultipleOrdered(handle) = handles else {
            panic!("registry selected an ordered-multiple operation for a different dynamic arity")
        };
        handle.clone()
    }
}

impl IntoDynArityHandle for Multiple<Unordered> {
    fn into_handle<S: DynStreamShape>(handle: OperandHandle<S, Self>) -> DynArityHandle<S> {
        DynArityHandle::MultipleUnordered(handle)
    }

    fn clone_handle<S: DynStreamShape>(handles: &DynArityHandle<S>) -> OperandHandle<S, Self> {
        let DynArityHandle::MultipleUnordered(handle) = handles else {
            panic!(
                "registry selected an unordered-multiple operation for a different dynamic arity"
            )
        };
        handle.clone()
    }
}

impl IntoDynArityHandle for Single {
    fn into_handle<S: DynStreamShape>(handle: OperandHandle<S, Self>) -> DynArityHandle<S> {
        DynArityHandle::Single(handle)
    }

    fn clone_handle<S: DynStreamShape>(handles: &DynArityHandle<S>) -> OperandHandle<S, Self> {
        let DynArityHandle::Single(handle) = handles else {
            panic!("registry selected a single operation for a different dynamic arity")
        };
        handle.clone()
    }
}

impl IntoDynArityHandle for Definite {
    fn into_handle<S: DynStreamShape>(handle: OperandHandle<S, Self>) -> DynArityHandle<S> {
        DynArityHandle::Definite(handle)
    }

    fn clone_handle<S: DynStreamShape>(handles: &DynArityHandle<S>) -> OperandHandle<S, Self> {
        let DynArityHandle::Definite(handle) = handles else {
            panic!("registry selected a definite operation for a different dynamic arity")
        };
        handle.clone()
    }
}

pub trait IntoDynLaneHandle: DynStreamShape + Sized {
    fn into_lane<C: IntoDynArityHandle>(handle: OperandHandle<Self, C>) -> DynLaneHandle;
}

pub trait DynLaneState: IntoDynLaneHandle {
    fn handles(handle: &DynLaneHandle) -> &DynArityHandle<Self>;
}

pub trait IntoDynOperand: Operand + Sized {
    fn into_dyn(self, descriptor: OperandDescriptor) -> DynOperand;
}

impl IntoDynLaneHandle for Indexed<DynIndex, DynValue> {
    fn into_lane<C: IntoDynArityHandle>(handle: OperandHandle<Self, C>) -> DynLaneHandle {
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
    fn into_lane<C: IntoDynArityHandle>(handle: OperandHandle<Self, C>) -> DynLaneHandle {
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
    fn into_lane<C: IntoDynArityHandle>(handle: OperandHandle<Self, C>) -> DynLaneHandle {
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
    fn into_lane<C: IntoDynArityHandle>(handle: OperandHandle<Self, C>) -> DynLaneHandle {
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
    fn into_lane<C: IntoDynArityHandle>(handle: OperandHandle<Self, C>) -> DynLaneHandle {
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

impl<S, C> IntoDynOperand for OperandHandle<S, C>
where
    S: IntoDynLaneHandle,
    C: IntoDynArityHandle,
{
    fn into_dyn(self, descriptor: OperandDescriptor) -> DynOperand {
        DynOperand::from_lane(self, descriptor)
    }
}

impl IntoDynOperand for DynGroupHandle {
    fn into_dyn(self, descriptor: OperandDescriptor) -> DynOperand {
        DynOperand::from_group(self, descriptor)
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
        cache: &'a EvaluationCache<'a>,
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

impl DynLaneHandle {
    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
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

impl DynOperand {
    #[must_use]
    pub fn from_lane<S, C>(handle: OperandHandle<S, C>, descriptor: OperandDescriptor) -> Self
    where
        S: IntoDynLaneHandle,
        C: IntoDynArityHandle,
    {
        let handle = DynHandle::Lane(S::into_lane(handle));
        Self::new(handle, descriptor)
    }

    #[must_use]
    pub fn from_group(handle: DynGroupHandle, descriptor: OperandDescriptor) -> Self {
        Self::new(DynHandle::Group(handle), descriptor)
    }

    fn new(handle: DynHandle, descriptor: OperandDescriptor) -> Self {
        Self::verify_descriptor(&handle, &descriptor);
        Self { handle, descriptor }
    }

    #[must_use]
    pub const fn descriptor(&self) -> &OperandDescriptor {
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

        if method == TRANSITION {
            let [DynInvokeArgument::ValueTarget(target)] = arguments else {
                return self.inapplicable(method, argument_descriptors);
            };

            return self.retarget(*target);
        }

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
            "dynamic invocation",
            OperationNotApplicable::new(method.to_string(), self.descriptor.clone(), arguments),
        ))
    }

    pub fn evaluate(&self, graphrecord: &GraphRecord) -> QueryResult<DynTerminal> {
        let optimized = self.optimize(graphrecord).0;
        let cache = EvaluationCache::new(graphrecord);

        match &optimized.handle {
            DynHandle::Lane(handle) => handle
                .evaluate(graphrecord, &cache)
                .map(DynYield::Lane)
                .map(DynTerminal::from_yield),
            DynHandle::Group(handle) => handle
                .evaluate(graphrecord, &cache)
                .map(DynYield::Group)
                .map(DynTerminal::from_yield),
        }
    }

    #[must_use]
    pub fn explain(&self, graphrecord: &GraphRecord) -> DynExplanation {
        let (operand, report) = self.optimize(graphrecord);

        DynExplanation { operand, report }
    }

    #[must_use]
    pub fn explanation(&self) -> Explanation<'_> {
        match &self.handle {
            DynHandle::Lane(handle) => handle.explanation(),
            DynHandle::Group(handle) => handle.explain(),
        }
    }

    fn optimize(&self, graphrecord: &GraphRecord) -> (Self, OptimizationReport) {
        let optimizer = Optimizer::shared_builtin();

        if optimizer.is_empty() {
            return (self.clone(), OptimizationReport::default());
        }

        let stats = Stats::new(graphrecord);
        let (handle, report) = match &self.handle {
            DynHandle::Lane(handle) => {
                let (handle, report) = handle.optimize(optimizer, &stats);
                (DynHandle::Lane(handle), report)
            }
            DynHandle::Group(handle) => {
                let (handle, report) = optimizer.run_reported(&stats, handle);
                (DynHandle::Group(handle), report)
            }
        };

        (Self::new(handle, self.descriptor.clone()), report)
    }

    fn verify_descriptor(handle: &DynHandle, descriptor: &OperandDescriptor) {
        match (handle, descriptor) {
            (DynHandle::Lane(handle), OperandDescriptor::Lane { shape, arity }) => {
                Self::verify_lane_shape(handle, shape);
                Self::verify_lane_arity(handle, *arity);
            }
            (DynHandle::Group(_), OperandDescriptor::Group { .. }) => {}
            _ => {
                panic!("registry paired a dynamic operand handle with a different descriptor state")
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

impl Display for DynExplanation {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.operand.explanation())?;

        if !self.report.phases.is_empty() {
            write!(formatter, "\n\noptimization:\n{}", self.report.display())?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::{query_edges, query_nodes};
    use crate::{
        Mask, Scalar,
        dynamic::{DynInvokeArgument, DynTerminal, DynTerminalArity, DynTerminalLane, DynValue},
        registry::{
            ArityDescriptor, IndexDescriptor, LaneShapeDescriptor, OperandDescriptor,
            OrderDescriptor, ValueDescriptor,
        },
    };
    use graphrecords_core::{
        GraphRecord,
        graphrecord::{AttributeMap, EdgeIndex, GraphRecordValue, NodeIndex},
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

        GraphRecord::from_tuples(nodes, Some(edges), None).unwrap()
    }

    fn create_unit_node_descriptor() -> OperandDescriptor {
        OperandDescriptor::Lane {
            shape: LaneShapeDescriptor::Indexed {
                index: IndexDescriptor::domain::<NodeIndex>(),
                value: ValueDescriptor::unit(),
            },
            arity: ArityDescriptor::Multiple {
                order: OrderDescriptor::Unordered,
            },
        }
    }

    fn create_unit_edge_descriptor() -> OperandDescriptor {
        OperandDescriptor::Lane {
            shape: LaneShapeDescriptor::Indexed {
                index: IndexDescriptor::domain::<EdgeIndex>(),
                value: ValueDescriptor::unit(),
            },
            arity: ArityDescriptor::Multiple {
                order: OrderDescriptor::Unordered,
            },
        }
    }

    fn create_scalar_node_descriptor() -> OperandDescriptor {
        OperandDescriptor::Lane {
            shape: LaneShapeDescriptor::Indexed {
                index: IndexDescriptor::domain::<NodeIndex>(),
                value: ValueDescriptor::value::<Scalar>(),
            },
            arity: ArityDescriptor::Multiple {
                order: OrderDescriptor::Unordered,
            },
        }
    }

    fn create_mask_node_descriptor() -> OperandDescriptor {
        OperandDescriptor::Lane {
            shape: LaneShapeDescriptor::Indexed {
                index: IndexDescriptor::domain::<NodeIndex>(),
                value: ValueDescriptor::value::<Mask>(),
            },
            arity: ArityDescriptor::Multiple {
                order: OrderDescriptor::Unordered,
            },
        }
    }

    fn create_scalar_count_descriptor() -> OperandDescriptor {
        OperandDescriptor::Lane {
            shape: LaneShapeDescriptor::Bare {
                value: ValueDescriptor::value::<Scalar>(),
            },
            arity: ArityDescriptor::Definite,
        }
    }

    #[test]
    fn test_query_nodes() {
        assert_eq!(&create_unit_node_descriptor(), query_nodes().descriptor());
    }

    #[test]
    fn test_query_edges() {
        assert_eq!(&create_unit_edge_descriptor(), query_edges().descriptor());
    }

    #[test]
    fn test_cache() {
        let nodes = query_nodes();

        assert_eq!(nodes.descriptor(), nodes.cache().descriptor());
    }

    #[test]
    fn test_invoke() {
        let attribute = query_nodes()
            .invoke("attribute", &[DynInvokeArgument::Attribute("lorem".into())])
            .unwrap();

        assert_eq!(&create_scalar_node_descriptor(), attribute.descriptor());

        let is_null = attribute.invoke("is_null", &[]).unwrap();

        assert_eq!(&create_mask_node_descriptor(), is_null.descriptor());

        let count = query_nodes().invoke("count", &[]).unwrap();

        assert_eq!(&create_scalar_count_descriptor(), count.descriptor());
    }

    #[test]
    fn test_invalid_invoke() {
        // Invoking a method that is not registered should fail
        assert!(query_nodes().invoke("lorem", &[]).is_err());

        // Summing a lane that carries no values should fail
        assert!(query_nodes().invoke("sum", &[]).is_err());

        // Reading an attribute without naming one should fail
        assert!(query_nodes().invoke("attribute", &[]).is_err());

        // Taking the first element of an unordered lane should fail
        assert!(query_nodes().invoke("first", &[]).is_err());
    }

    #[test]
    fn test_evaluate() {
        let graphrecord = create_graphrecord();

        assert!(matches!(
            query_nodes().evaluate(&graphrecord).unwrap(),
            DynTerminal::Lane(DynTerminalLane::IndexedUnit(
                DynTerminalArity::MultipleUnordered(elements)
            )) if elements.len() == 4
        ));
        assert!(matches!(
            query_edges().evaluate(&graphrecord).unwrap(),
            DynTerminal::Lane(DynTerminalLane::IndexedUnit(
                DynTerminalArity::MultipleUnordered(elements)
            )) if elements.len() == 2
        ));

        let count = query_nodes().invoke("count", &[]).unwrap();

        assert!(matches!(
            count.evaluate(&graphrecord).unwrap(),
            DynTerminal::Lane(DynTerminalLane::BareValue(DynTerminalArity::Definite(Ok(
                DynValue::Scalar(GraphRecordValue::Int(4))
            ))))
        ));

        let attribute = query_nodes()
            .invoke("attribute", &[DynInvokeArgument::Attribute("lorem".into())])
            .unwrap();

        assert!(matches!(
            attribute.evaluate(&graphrecord).unwrap(),
            DynTerminal::Lane(DynTerminalLane::IndexedValue(
                DynTerminalArity::MultipleUnordered(elements)
            )) if elements.len() == 4
                && elements.iter().filter(|element| element.1.is_ok()).count() == 1
        ));
    }
}
