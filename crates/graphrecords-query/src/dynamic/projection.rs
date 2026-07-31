use super::{
    DynArity, DynExpandedOwned, DynExpandedRef, DynIndex, DynIndexOwned, DynIndexRef, DynPayload,
    DynStreamShape, DynValue, DynYield,
};
use crate::{
    AttributeName, Bare, BareValueDomain, EdgeEndpointRole, ElementShape, EntityDomain,
    EntityReference, EvaluateContext, EvaluateOperand, ExpandedChild, ExpandedIndex,
    ExpandedIndexOwned, ExpandedIndexReference, Explain, Failure, FailureKind, FailureKindValue,
    FailureValue, IndexDomain, IndexValue, Indexed, Mask, Operand, OrderState, Ordered, Positional,
    QueryResult, Scalar, Unit, Unordered, ValueDomain,
    dynamic::DynEntityReference,
    element::{
        Arity, ElementEmission, ElementTransition, Expanding, IndexedExpansionPipeline, Pipeline,
        Retention,
    },
    error::index::DuplicateExpandedChildIndex,
    execution::EvaluationCache,
    explain::ExplainFormatter,
    index::GroupKey,
    operands::{GroupOperand, OperandHandle, Partition},
    operations::{
        Element, ElementKernel, ElementPipeline, Group, GroupKernel, Lane, LaneKernel, Operation,
        Prepare,
    },
    optimizer::{
        EmptyRule, Estimate, Estimated, OperationInputs, OptimizePlan, OptimizerHints,
        PlanIdentity, PlanInputs, PlanNode, Session, Stats, Transformed,
    },
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{EdgeIndex, GraphRecordValue, NodeIndex},
};
use graphrecords_utils::aliases::GrHashSet;
use std::{
    any::{Any, type_name},
    fmt,
    hash::{Hash, Hasher},
    marker::PhantomData,
};

type DynElementOperationMarker<S, T, E> = PhantomData<fn() -> (S, T, E)>;
type DynExpansionOperationMarker<I, V, C, W, O> = PhantomData<fn() -> (I, V, C, W, O)>;
type DynLaneOperationMarker<S, C, O> = PhantomData<fn() -> (S, C, O)>;

pub trait DynIndexProjection: IndexDomain {
    fn project_index<'a>(graphrecord: &'a GraphRecord, index: DynIndexRef<'a>) -> Self::Index<'a>;

    fn erase_index(index: Self::Index<'_>) -> DynIndexRef<'_>;

    fn project_owned(index: DynIndexOwned) -> Self::Owned;

    fn erase_owned(index: Self::Owned) -> DynIndexOwned;
}

fn index_projection_mismatch<I: IndexDomain>() -> ! {
    let expected = type_name::<I>();
    panic!("registry selected dynamic index data outside the typed domain {expected}")
}

impl DynIndexProjection for DynIndex {
    fn project_index<'a>(_graphrecord: &'a GraphRecord, index: DynIndexRef<'a>) -> Self::Index<'a> {
        index
    }

    fn erase_index(index: Self::Index<'_>) -> DynIndexRef<'_> {
        index
    }

    fn project_owned(index: DynIndexOwned) -> Self::Owned {
        index
    }

    fn erase_owned(index: Self::Owned) -> DynIndexOwned {
        index
    }
}

impl DynIndexProjection for Positional {
    fn project_index<'a>(_graphrecord: &'a GraphRecord, index: DynIndexRef<'a>) -> Self::Index<'a> {
        let DynIndexRef::Positional(index) = index else {
            index_projection_mismatch::<Self>()
        };
        index
    }

    fn erase_index(index: Self::Index<'_>) -> DynIndexRef<'_> {
        DynIndexRef::Positional(index)
    }

    fn project_owned(index: DynIndexOwned) -> Self::Owned {
        let DynIndexOwned::Positional(index) = index else {
            index_projection_mismatch::<Self>()
        };
        index
    }

    fn erase_owned(index: Self::Owned) -> DynIndexOwned {
        DynIndexOwned::Positional(index)
    }
}

impl DynIndexProjection for NodeIndex {
    fn project_index<'a>(graphrecord: &'a GraphRecord, index: DynIndexRef<'a>) -> Self::Index<'a> {
        let DynIndexRef::Node(index) = index else {
            index_projection_mismatch::<Self>()
        };
        Self::resolve_index(graphrecord, index).unwrap_or_else(|_| {
            panic!("registry admitted a dynamic node index that the graphrecord cannot resolve")
        })
    }

    fn erase_index(index: Self::Index<'_>) -> DynIndexRef<'_> {
        DynIndexRef::Node(index)
    }

    fn project_owned(index: DynIndexOwned) -> Self::Owned {
        let DynIndexOwned::Node(index) = index else {
            index_projection_mismatch::<Self>()
        };
        index
    }

    fn erase_owned(index: Self::Owned) -> DynIndexOwned {
        DynIndexOwned::Node(index)
    }
}

impl DynIndexProjection for EdgeIndex {
    fn project_index<'a>(graphrecord: &'a GraphRecord, index: DynIndexRef<'a>) -> Self::Index<'a> {
        let DynIndexRef::Edge(index) = index else {
            index_projection_mismatch::<Self>()
        };
        Self::resolve_index(graphrecord, index).unwrap_or_else(|_| {
            panic!("registry admitted a dynamic edge index that the graphrecord cannot resolve")
        })
    }

    fn erase_index(index: Self::Index<'_>) -> DynIndexRef<'_> {
        DynIndexRef::Edge(index)
    }

    fn project_owned(index: DynIndexOwned) -> Self::Owned {
        let DynIndexOwned::Edge(index) = index else {
            index_projection_mismatch::<Self>()
        };
        index
    }

    fn erase_owned(index: Self::Owned) -> DynIndexOwned {
        DynIndexOwned::Edge(index)
    }
}

macro_rules! implement_copy_index_projection {
    ($domain:ty, $reference:ident, $owned:ident) => {
        impl DynIndexProjection for $domain {
            fn project_index<'a>(
                _graphrecord: &'a GraphRecord,
                index: DynIndexRef<'a>,
            ) -> Self::Index<'a> {
                let DynIndexRef::$reference(index) = index else {
                    index_projection_mismatch::<Self>()
                };
                index
            }

            fn erase_index(index: Self::Index<'_>) -> DynIndexRef<'_> {
                DynIndexRef::$reference(index)
            }

            fn project_owned(index: DynIndexOwned) -> Self::Owned {
                let DynIndexOwned::$owned(index) = index else {
                    index_projection_mismatch::<Self>()
                };
                index
            }

            fn erase_owned(index: Self::Owned) -> DynIndexOwned {
                DynIndexOwned::$owned(index)
            }
        }
    };
}

implement_copy_index_projection!(AttributeName, Attribute, Attribute);
implement_copy_index_projection!(GraphRecordValue, Value, Value);
implement_copy_index_projection!(bool, Bool, Bool);
implement_copy_index_projection!(EdgeEndpointRole, EndpointRole, EndpointRole);
implement_copy_index_projection!(FailureKind, FailureKind, FailureKind);

impl<P: DynIndexProjection, C: DynIndexProjection> DynIndexProjection for ExpandedIndex<P, C> {
    fn project_index<'a>(graphrecord: &'a GraphRecord, index: DynIndexRef<'a>) -> Self::Index<'a> {
        let DynIndexRef::Expanded(index) = index else {
            index_projection_mismatch::<Self>()
        };
        let (parent, child) = index.into_parts();
        let parent = P::project_index(graphrecord, parent);
        match child {
            None => ExpandedIndexReference::source(parent),
            Some(child) => {
                ExpandedIndexReference::child(parent, C::project_index(graphrecord, child))
            }
        }
    }

    fn erase_index(index: Self::Index<'_>) -> DynIndexRef<'_> {
        let parent = P::erase_index(index.parent_index().clone());
        let index = match index.child_index() {
            None => DynExpandedRef::source(parent),
            Some(child) => DynExpandedRef::child(parent, C::erase_index(child.clone())),
        };
        DynIndexRef::Expanded(Box::new(index))
    }

    fn project_owned(index: DynIndexOwned) -> Self::Owned {
        let DynIndexOwned::Expanded(index) = index else {
            index_projection_mismatch::<Self>()
        };
        let (parent, child) = index.into_parts();
        let parent = P::project_owned(parent);
        match child {
            None => ExpandedIndexOwned::source(parent),
            Some(child) => ExpandedIndexOwned::child(parent, C::project_owned(child)),
        }
    }

    fn erase_owned(index: Self::Owned) -> DynIndexOwned {
        let (parent, child) = index.into_parts();
        let parent = P::erase_owned(parent);
        let index = match child {
            None => DynExpandedOwned::source(parent),
            Some(child) => DynExpandedOwned::child(parent, C::erase_owned(child)),
        };
        DynIndexOwned::Expanded(Box::new(index))
    }
}

pub trait DynValueProjection: ValueDomain {
    type Dynamic: ValueDomain;

    fn project<'a>(
        graphrecord: &'a GraphRecord,
        value: <Self::Dynamic as ValueDomain>::Value<'a>,
    ) -> Self::Value<'a>;

    fn erase(value: Self::Value<'_>) -> <Self::Dynamic as ValueDomain>::Value<'_>;
}

fn value_projection_mismatch<V: ValueDomain>() -> ! {
    let expected = type_name::<V>();
    panic!("registry selected dynamic value data outside the typed domain {expected}")
}

impl DynValueProjection for DynValue {
    type Dynamic = Self;

    fn project<'a>(
        _graphrecord: &'a GraphRecord,
        value: <Self::Dynamic as ValueDomain>::Value<'a>,
    ) -> Self::Value<'a> {
        value
    }

    fn erase(value: Self::Value<'_>) -> <Self::Dynamic as ValueDomain>::Value<'_> {
        value
    }
}

impl DynValueProjection for Scalar {
    type Dynamic = DynValue;

    fn project<'a>(
        _graphrecord: &'a GraphRecord,
        value: <Self::Dynamic as ValueDomain>::Value<'a>,
    ) -> Self::Value<'a> {
        let DynValue::Scalar(value) = value else {
            value_projection_mismatch::<Self>()
        };
        value
    }

    fn erase(value: Self::Value<'_>) -> <Self::Dynamic as ValueDomain>::Value<'_> {
        DynValue::Scalar(value)
    }
}

impl DynValueProjection for AttributeName {
    type Dynamic = DynValue;

    fn project<'a>(
        _graphrecord: &'a GraphRecord,
        value: <Self::Dynamic as ValueDomain>::Value<'a>,
    ) -> Self::Value<'a> {
        let DynValue::Attribute(value) = value else {
            value_projection_mismatch::<Self>()
        };
        value
    }

    fn erase(value: Self::Value<'_>) -> <Self::Dynamic as ValueDomain>::Value<'_> {
        DynValue::Attribute(value)
    }
}

impl<I: DynIndexProjection> DynValueProjection for IndexValue<I> {
    type Dynamic = DynValue;

    fn project<'a>(
        _graphrecord: &'a GraphRecord,
        value: <Self::Dynamic as ValueDomain>::Value<'a>,
    ) -> Self::Value<'a> {
        let DynValue::Index(value) = value else {
            value_projection_mismatch::<Self>()
        };
        I::project_owned(value)
    }

    fn erase(value: Self::Value<'_>) -> <Self::Dynamic as ValueDomain>::Value<'_> {
        DynValue::Index(I::erase_owned(value))
    }
}

impl DynValueProjection for EntityReference<NodeIndex> {
    type Dynamic = DynValue;

    fn project<'a>(
        graphrecord: &'a GraphRecord,
        value: <Self::Dynamic as ValueDomain>::Value<'a>,
    ) -> Self::Value<'a> {
        let DynValue::EntityReference(value) = value else {
            value_projection_mismatch::<Self>()
        };
        let Some(index) = value.node_index() else {
            value_projection_mismatch::<Self>()
        };
        NodeIndex::resolve_index(graphrecord, index).unwrap_or_else(|_| {
            panic!("registry admitted a node reference that the graphrecord cannot resolve")
        })
    }

    fn erase(value: Self::Value<'_>) -> <Self::Dynamic as ValueDomain>::Value<'_> {
        DynValue::EntityReference(DynEntityReference::node(
            <NodeIndex as IndexDomain>::to_owned(&value),
        ))
    }
}

impl DynValueProjection for EntityReference<EdgeIndex> {
    type Dynamic = DynValue;

    fn project<'a>(
        graphrecord: &'a GraphRecord,
        value: <Self::Dynamic as ValueDomain>::Value<'a>,
    ) -> Self::Value<'a> {
        let DynValue::EntityReference(value) = value else {
            value_projection_mismatch::<Self>()
        };
        let Some(index) = value.edge_index() else {
            value_projection_mismatch::<Self>()
        };
        EdgeIndex::resolve_index(graphrecord, index).unwrap_or_else(|_| {
            panic!("registry admitted an edge reference that the graphrecord cannot resolve")
        })
    }

    fn erase(value: Self::Value<'_>) -> <Self::Dynamic as ValueDomain>::Value<'_> {
        DynValue::EntityReference(DynEntityReference::edge(
            <EdgeIndex as IndexDomain>::to_owned(&value),
        ))
    }
}

impl DynValueProjection for FailureValue {
    type Dynamic = DynValue;

    fn project<'a>(
        _graphrecord: &'a GraphRecord,
        value: <Self::Dynamic as ValueDomain>::Value<'a>,
    ) -> Self::Value<'a> {
        let DynValue::Failure(value) = value else {
            value_projection_mismatch::<Self>()
        };
        *value
    }

    fn erase(value: Self::Value<'_>) -> <Self::Dynamic as ValueDomain>::Value<'_> {
        DynValue::Failure(Box::new(value))
    }
}

impl DynValueProjection for FailureKindValue {
    type Dynamic = DynValue;

    fn project<'a>(
        _graphrecord: &'a GraphRecord,
        value: <Self::Dynamic as ValueDomain>::Value<'a>,
    ) -> Self::Value<'a> {
        let DynValue::FailureKind(value) = value else {
            value_projection_mismatch::<Self>()
        };
        value
    }

    fn erase(value: Self::Value<'_>) -> <Self::Dynamic as ValueDomain>::Value<'_> {
        DynValue::FailureKind(value)
    }
}

impl DynValueProjection for Mask {
    type Dynamic = Self;

    fn project<'a>(
        _graphrecord: &'a GraphRecord,
        value: <Self::Dynamic as ValueDomain>::Value<'a>,
    ) -> Self::Value<'a> {
        value
    }

    fn erase(value: Self::Value<'_>) -> <Self::Dynamic as ValueDomain>::Value<'_> {
        value
    }
}

impl DynValueProjection for Unit {
    type Dynamic = Self;

    fn project<'a>(
        _graphrecord: &'a GraphRecord,
        _value: <Self::Dynamic as ValueDomain>::Value<'a>,
    ) -> Self::Value<'a> {
    }

    fn erase(_value: Self::Value<'_>) -> <Self::Dynamic as ValueDomain>::Value<'_> {}
}

pub trait DynShapeProjection: ElementShape {
    type Dynamic: ElementShape;

    fn project_element<'a>(
        graphrecord: &'a GraphRecord,
        element: <Self::Dynamic as ElementShape>::Element<'a>,
    ) -> Self::Element<'a>;

    fn erase_element(element: Self::Element<'_>) -> <Self::Dynamic as ElementShape>::Element<'_>;
}

impl<I: DynIndexProjection, V: DynValueProjection> DynShapeProjection for Indexed<I, V> {
    type Dynamic = Indexed<DynIndex, V::Dynamic>;

    fn project_element<'a>(
        graphrecord: &'a GraphRecord,
        element: <Self::Dynamic as ElementShape>::Element<'a>,
    ) -> Self::Element<'a> {
        let (index, outcome) = element;
        (
            I::project_index(graphrecord, index),
            outcome.map(|value| V::project(graphrecord, value)),
        )
    }

    fn erase_element(element: Self::Element<'_>) -> <Self::Dynamic as ElementShape>::Element<'_> {
        let (index, outcome) = element;
        (I::erase_index(index), outcome.map(V::erase))
    }
}

impl<V> DynShapeProjection for Bare<V>
where
    V: DynValueProjection + BareValueDomain,
    V::Dynamic: BareValueDomain,
{
    type Dynamic = Bare<V::Dynamic>;

    fn project_element<'a>(
        graphrecord: &'a GraphRecord,
        element: <Self::Dynamic as ElementShape>::Element<'a>,
    ) -> Self::Element<'a> {
        element.map(|value| V::project(graphrecord, value))
    }

    fn erase_element(element: Self::Element<'_>) -> <Self::Dynamic as ElementShape>::Element<'_> {
        element.map(V::erase)
    }
}

pub trait DynOperandProjection: Operand {
    type Dynamic: Operand;

    fn erase_operand(self) -> Self::Dynamic {
        Self::Dynamic::new(DynProjectionContext { input: self })
    }

    fn erase<'a>(
        values: Self::ReturnValue<'a>,
    ) -> <Self::Dynamic as EvaluateOperand>::ReturnValue<'a>
    where
        Self: 'a;
}

pub trait DynPayloadOutput: Operand {
    fn into_yield<'a>(values: Self::ReturnValue<'a>) -> DynYield<'a>
    where
        Self: 'a;
}

impl<S, C> DynOperandProjection for OperandHandle<S, C>
where
    S: DynShapeProjection,
    S::Dynamic: ElementShape,
    C: Arity,
{
    type Dynamic = OperandHandle<S::Dynamic, C>;

    fn erase<'a>(
        values: Self::ReturnValue<'a>,
    ) -> <Self::Dynamic as EvaluateOperand>::ReturnValue<'a>
    where
        Self: 'a,
    {
        C::map_elements(values, S::erase_element)
    }
}

impl<S: DynStreamShape, C: DynArity> DynPayloadOutput for OperandHandle<S, C> {
    fn into_yield<'a>(values: Self::ReturnValue<'a>) -> DynYield<'a>
    where
        Self: 'a,
    {
        DynYield::Lane(S::erase::<C>(values))
    }
}

impl<M, K, S, C> DynOperandProjection for GroupOperand<M, K, OperandHandle<S, C>>
where
    M: DynIndexProjection,
    K: DynIndexProjection + GroupKey,
    S: DynStreamShape,
    C: DynArity,
{
    type Dynamic = GroupOperand<DynIndex, DynIndex, DynPayload>;

    fn erase<'a>(
        values: Self::ReturnValue<'a>,
    ) -> <Self::Dynamic as EvaluateOperand>::ReturnValue<'a>
    where
        Self: 'a,
    {
        values
            .map_domains(M::erase_index, K::erase_owned)
            .map_payloads(|_, _, payload| {
                payload.map(|values| DynYield::Lane(S::erase::<C>(values)))
            })
    }
}

impl DynPayloadOutput for GroupOperand<DynIndex, DynIndex, DynPayload> {
    fn into_yield<'a>(values: Self::ReturnValue<'a>) -> DynYield<'a>
    where
        Self: 'a,
    {
        DynYield::Group(values)
    }
}

struct DynProjectionContext<O: DynOperandProjection> {
    input: O,
}

impl<O: DynOperandProjection> PlanNode for DynProjectionContext<O> {
    fn inputs(&self) -> Vec<&dyn PlanNode> {
        vec![self.input.as_plan_node()]
    }

    fn dyn_eq(&self, other: &dyn PlanNode) -> bool {
        let Some(other) = other.downcast::<Self>() else {
            return false;
        };
        self.input.as_plan_node().dyn_eq(other.input.as_plan_node())
    }

    fn dyn_hash(&self, mut state: &mut dyn Hasher) {
        self.type_id().hash(&mut state);
        self.input.as_plan_node().dyn_hash(state);
    }
}

impl<O: DynOperandProjection> OptimizerHints for DynProjectionContext<O> {
    fn commutes_with_filter(&self) -> bool {
        self.input.context().commutes_with_filter()
    }

    fn allows_limit_pushdown(&self) -> bool {
        self.input.context().allows_limit_pushdown()
    }

    fn is_volatile(&self) -> bool {
        self.input.context().is_volatile()
    }

    fn empty_rule(&self) -> EmptyRule {
        self.input.context().empty_rule()
    }
}

impl<O: DynOperandProjection> Explain for DynProjectionContext<O> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        self.input.describe(formatter)
    }
}

impl<O: DynOperandProjection> Estimated for DynProjectionContext<O> {
    fn estimate(&self, stats: &Stats) -> Estimate {
        self.input.context().estimate(stats)
    }
}

impl<O: DynOperandProjection> OptimizePlan for DynProjectionContext<O> {
    type Output = O::Dynamic;

    fn optimize(&self, original: &Self::Output, session: &Session) -> Transformed<Self::Output> {
        let input = session.optimize(&self.input);
        if !input.is_changed() {
            return Transformed::unchanged(original.clone());
        }
        Transformed::changed(Self::Output::new(Self {
            input: input.into_parts().0,
        }))
    }
}

impl<O: DynOperandProjection> EvaluateContext for DynProjectionContext<O> {
    type Operand = O::Dynamic;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<<Self::Operand as EvaluateOperand>::ReturnValue<'a>> {
        self.input.evaluate(graphrecord, cache).map(O::erase)
    }
}

macro_rules! implement_operation_forwarding {
    ($wrapper:ident<$($parameter:ident),+>, $scope:ident) => {
        impl<P: Clone, $($parameter),+> Clone for $wrapper<P, $($parameter),+> {
            fn clone(&self) -> Self {
                Self::new(self.operation.clone())
            }
        }

        impl<P: PlanIdentity, $($parameter: 'static),+> PlanIdentity
            for $wrapper<P, $($parameter),+>
        {
            fn identity_eq(&self, other: &Self) -> bool {
                self.operation.identity_eq(&other.operation)
            }

            fn identity_hash<H: Hasher>(&self, state: &mut H) {
                self.operation.identity_hash(state);
            }
        }

        impl<P: PlanInputs, $($parameter: 'static),+> PlanInputs for $wrapper<P, $($parameter),+> {
            fn inputs(&self) -> Vec<&dyn PlanNode> {
                self.operation.inputs()
            }

            fn optimize(&self, session: &Session) -> Transformed<Self> {
                let operation = self.operation.optimize(session);

                if !operation.is_changed() {
                    return Transformed::unchanged(self.clone());
                }

                Transformed::changed(Self::new(operation.into_parts().0))
            }
        }

        impl<P: OptimizerHints, $($parameter),+> OptimizerHints for $wrapper<P, $($parameter),+> {
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

        impl<P: OperationInputs, $($parameter: 'static),+> OperationInputs
            for $wrapper<P, $($parameter),+>
        {
            type Inputs<'a, X: 'a> = P::Inputs<'a, X>;

            fn inputs<'a, X: 'a>(&'a self, primary: &'a X) -> Self::Inputs<'a, X> {
                OperationInputs::inputs(&self.operation, primary)
            }
        }

        impl<P: Explain, $($parameter),+> Explain for $wrapper<P, $($parameter),+> {
            fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
                self.operation.describe(formatter)
            }
        }

        impl<P: Prepare, $($parameter: 'static),+> Prepare for $wrapper<P, $($parameter),+> {
            type Prepared<'a>
                = P::Prepared<'a>
            where
                Self: 'a;

            fn prepare<'a>(
                &'a self,
                graphrecord: &'a GraphRecord,
                cache: &'a EvaluationCache<'a>,
            ) -> QueryResult<Self::Prepared<'a>> {
                self.operation.prepare(graphrecord, cache)
            }
        }

        impl<P: Operation<Scope = $scope>, $($parameter: 'static),+> Operation
            for $wrapper<P, $($parameter),+>
        {
            type Scope = $scope;
        }
    };
}

pub struct DynElementOperation<P, S, T, E> {
    operation: P,
    marker: DynElementOperationMarker<S, T, E>,
}

impl<P, S, T, E> DynElementOperation<P, S, T, E> {
    pub const fn new(operation: P) -> Self {
        Self {
            operation,
            marker: PhantomData,
        }
    }
}

implement_operation_forwarding!(DynElementOperation<S, T, E>, Element);

impl<P, I, V, W, E> ElementKernel<Indexed<DynIndex, V::Dynamic>>
    for DynElementOperation<P, Indexed<I, V>, Indexed<I, W>, E>
where
    P: ElementKernel<Indexed<I, V>, OutShape = Indexed<I, W>, Emission = E>,
    I: DynIndexProjection,
    V: DynValueProjection,
    W: DynValueProjection,
    E: Retention,
{
    type Emission = E;
    type OutShape = Indexed<DynIndex, W::Dynamic>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<DynIndex, V::Dynamic>, Self>> {
        let pipeline = P::pipeline(graphrecord, prepared)?;
        Ok(Pipeline::keyed(move |index, outcome: QueryResult<_>| {
            let typed_index = I::project_index(graphrecord, index);
            let outcome = outcome.map(|value| V::project(graphrecord, value));
            let step = pipeline.run((typed_index, outcome));
            <E as ElementEmission>::map_step(step, |outcome| outcome.map(W::erase))
        }))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        self.operation.estimate(input, stats)
    }
}

impl<P, I, V, W, E> ElementKernel<Indexed<DynIndex, V::Dynamic>>
    for DynElementOperation<P, Indexed<I, V>, Bare<W>, E>
where
    P: ElementKernel<Indexed<I, V>, OutShape = Bare<W>, Emission = E>,
    I: DynIndexProjection,
    V: DynValueProjection,
    W: DynValueProjection + BareValueDomain,
    W::Dynamic: BareValueDomain,
    E: ElementEmission,
{
    type Emission = E;
    type OutShape = Bare<W::Dynamic>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<DynIndex, V::Dynamic>, Self>> {
        let pipeline = P::pipeline(graphrecord, prepared)?;
        Ok(Pipeline::keyed(move |index, outcome: QueryResult<_>| {
            let typed_index = I::project_index(graphrecord, index);
            let outcome = outcome.map(|value| V::project(graphrecord, value));
            let step = pipeline.run((typed_index, outcome));
            E::map_step(step, |outcome| outcome.map(W::erase))
        }))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        self.operation.estimate(input, stats)
    }
}

impl<P, V, W, E> ElementKernel<Bare<V::Dynamic>> for DynElementOperation<P, Bare<V>, Bare<W>, E>
where
    P: ElementKernel<Bare<V>, OutShape = Bare<W>, Emission = E>,
    V: DynValueProjection + BareValueDomain,
    V::Dynamic: BareValueDomain,
    W: DynValueProjection + BareValueDomain,
    W::Dynamic: BareValueDomain,
    E: ElementEmission,
{
    type Emission = E;
    type OutShape = Bare<W::Dynamic>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V::Dynamic>, Self>> {
        let pipeline = P::pipeline(graphrecord, prepared)?;
        Ok(Pipeline::new(move |outcome: QueryResult<_>| {
            let outcome = outcome.map(|value| V::project(graphrecord, value));
            let step = pipeline.run(outcome);
            E::map_step(step, |outcome| outcome.map(W::erase))
        }))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        self.operation.estimate(input, stats)
    }
}

fn expand_dyn_source<'a, V, W, O>(
    parent: DynIndexRef<'a>,
    source: QueryResult<V::Value<'a>>,
    pipeline: &IndexedExpansionPipeline<'a, DynIndex, DynIndex, V, W, O>,
) -> Vec<(DynIndexRef<'a>, QueryResult<W::Value<'a>>)>
where
    V: ValueDomain,
    W: ValueDomain,
    O: OrderState,
    Expanding<O>: ElementEmission,
{
    let source_value = match source {
        Ok(value) => value,
        Err(failure) => {
            let source = DynExpandedRef::source(parent);
            return vec![(DynIndexRef::Expanded(Box::new(source)), Err(failure))];
        }
    };

    let children = match pipeline.run((parent.clone(), source_value)) {
        Ok(children) => children,
        Err(failure) => {
            let source = DynExpandedRef::source(parent);
            return vec![(DynIndexRef::Expanded(Box::new(source)), Err(failure))];
        }
    };

    let mut seen_children = GrHashSet::default();
    let mut fragment = Vec::with_capacity(children.len());

    for child in children {
        let (child_index, outcome) = child.into_parts();

        if !seen_children.insert(<DynIndex as IndexDomain>::to_owned(&child_index)) {
            let source = DynExpandedRef::source(parent.clone());
            let source_address = DynIndexRef::Expanded(Box::new(source));
            let failure = Failure::new_at::<DynIndex, _>(
                "indexed expansion",
                DuplicateExpandedChildIndex::<DynIndex>::new(<DynIndex as IndexDomain>::to_owned(
                    &child_index,
                )),
                &source_address,
            );

            return vec![(source_address, Err(failure))];
        }

        let child = DynExpandedRef::child(parent.clone(), child_index);
        fragment.push((DynIndexRef::Expanded(Box::new(child)), outcome));
    }

    fragment
}

impl<V: ValueDomain, W: ValueDomain> ElementTransition<Indexed<DynIndex, W>, Expanding<Ordered>>
    for Indexed<DynIndex, V>
{
    type Pipeline<'a>
        = IndexedExpansionPipeline<'a, DynIndex, DynIndex, V, W, Ordered>
    where
        Self: 'a,
        Indexed<DynIndex, W>: 'a;

    fn apply<'a, A: Arity>(
        values: A::Container<'a, Self::Element<'a>>,
        pipeline: Self::Pipeline<'a>,
    ) -> <<Expanding<Ordered> as ElementEmission>::OutArity<A> as Arity>::Container<
        'a,
        <Indexed<DynIndex, W> as ElementShape>::Element<'a>,
    > {
        Expanding::<Ordered>::apply::<A, _, _>(values, move |(parent, source)| {
            expand_dyn_source::<V, _, _>(parent, source, &pipeline)
        })
    }
}

impl<V: ValueDomain, W: ValueDomain> ElementTransition<Indexed<DynIndex, W>, Expanding<Unordered>>
    for Indexed<DynIndex, V>
{
    type Pipeline<'a>
        = IndexedExpansionPipeline<'a, DynIndex, DynIndex, V, W, Unordered>
    where
        Self: 'a,
        Indexed<DynIndex, W>: 'a;

    fn apply<'a, A: Arity>(
        values: A::Container<'a, Self::Element<'a>>,
        pipeline: Self::Pipeline<'a>,
    ) -> <<Expanding<Unordered> as ElementEmission>::OutArity<A> as Arity>::Container<
        'a,
        <Indexed<DynIndex, W> as ElementShape>::Element<'a>,
    > {
        Expanding::<Unordered>::apply::<A, _, _>(values, move |(parent, source)| {
            expand_dyn_source::<V, _, _>(parent, source, &pipeline)
        })
    }
}

pub struct DynExpansionOperation<P, I, V, C, W, O> {
    operation: P,
    marker: DynExpansionOperationMarker<I, V, C, W, O>,
}

impl<P, I, V, C, W, O> DynExpansionOperation<P, I, V, C, W, O> {
    pub const fn new(operation: P) -> Self {
        Self {
            operation,
            marker: PhantomData,
        }
    }
}

implement_operation_forwarding!(DynExpansionOperation<I, V, C, W, O>, Element);

impl<P, I, V, C, W, O> ElementKernel<Indexed<DynIndex, V::Dynamic>>
    for DynExpansionOperation<P, I, V, C, W, O>
where
    P: ElementKernel<
            Indexed<I, V>,
            OutShape = Indexed<ExpandedIndex<I, C>, W>,
            Emission = Expanding<O>,
        >,
    I: DynIndexProjection,
    V: DynValueProjection,
    C: DynIndexProjection,
    W: DynValueProjection,
    O: OrderState,
    Expanding<O>: ElementEmission,
    for<'a> Indexed<I, V>: ElementTransition<
            Indexed<ExpandedIndex<I, C>, W>,
            Expanding<O>,
            Pipeline<'a> = IndexedExpansionPipeline<'a, I, C, V, W, O>,
        >,
    for<'a> Indexed<DynIndex, V::Dynamic>: ElementTransition<
            Indexed<DynIndex, W::Dynamic>,
            Expanding<O>,
            Pipeline<'a> = IndexedExpansionPipeline<
                'a,
                DynIndex,
                DynIndex,
                V::Dynamic,
                W::Dynamic,
                O,
            >,
        >,
{
    type Emission = Expanding<O>;
    type OutShape = Indexed<DynIndex, W::Dynamic>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<DynIndex, V::Dynamic>, Self>> {
        let pipeline = P::pipeline(graphrecord, prepared)?;
        Ok(Pipeline::new(move |(index, value)| {
            let index = I::project_index(graphrecord, index);
            let value = V::project(graphrecord, value);
            pipeline.run((index, value)).map(|children| {
                children
                    .into_iter()
                    .map(|child| {
                        let (index, outcome) = child.into_parts();
                        ExpandedChild::from_outcome(C::erase_index(index), outcome.map(W::erase))
                    })
                    .collect()
            })
        }))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        self.operation.estimate(input, stats)
    }
}

pub struct DynGroupOperation<P, S, C> {
    operation: P,
    marker: PhantomData<fn() -> (S, C)>,
}

impl<P, S, C> DynGroupOperation<P, S, C> {
    pub const fn new(operation: P) -> Self {
        Self {
            operation,
            marker: PhantomData,
        }
    }
}

implement_operation_forwarding!(DynGroupOperation<S, C>, Group);

impl<P, S, C> GroupKernel<DynIndex, DynIndex, DynPayload> for DynGroupOperation<P, S, C>
where
    P: GroupKernel<DynIndex, DynIndex, OperandHandle<S, C>>,
    S: DynStreamShape,
    C: DynArity,
    P::Output: DynOperandProjection,
{
    type Output = <P::Output as DynOperandProjection>::Dynamic;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        partition: Partition<'a, DynIndex, DynIndex, DynPayload>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let partition = project_group_partition::<S, C>(partition);

        P::execute(graphrecord, partition, prepared).map(P::Output::erase)
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        self.operation.estimate(input, stats)
    }
}

fn project_group_partition<S: DynStreamShape, C: DynArity>(
    partition: Partition<'_, DynIndex, DynIndex, DynPayload>,
) -> Partition<'_, DynIndex, DynIndex, OperandHandle<S, C>> {
    partition.map_payloads(|_, _, payload| {
        payload.map(|yielded| {
            let DynYield::Lane(stream) = yielded else {
                panic!("registry selected a group kernel at the wrong dynamic nesting depth")
            };

            S::project::<C>(stream)
        })
    })
}

pub struct DynLaneOperation<P, S, C, O> {
    operation: P,
    marker: DynLaneOperationMarker<S, C, O>,
}

impl<P, S, C, O> DynLaneOperation<P, S, C, O> {
    pub const fn new(operation: P) -> Self {
        Self {
            operation,
            marker: PhantomData,
        }
    }

    pub const fn operation(&self) -> &P {
        &self.operation
    }
}

implement_operation_forwarding!(DynLaneOperation<S, C, O>, Lane);

impl<P, S, C, O> LaneKernel<S::Dynamic, C> for DynLaneOperation<P, S, C, O>
where
    P: LaneKernel<S, C, Output = O>,
    S: DynShapeProjection,
    S::Dynamic: ElementShape,
    C: Arity,
    O: DynOperandProjection,
{
    type Output = O::Dynamic;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: <OperandHandle<S::Dynamic, C> as EvaluateOperand>::ReturnValue<'a>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let values = C::map_elements(values, |element| S::project_element(graphrecord, element));
        P::execute(graphrecord, values, prepared).map(O::erase)
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        self.operation.estimate(input, stats)
    }
}
