use super::{
    DynArity, DynExpandedAddress, DynExpandedOwned, DynExpandedView, DynIndex, DynIndexAddress,
    DynIndexOwned, DynIndexView, DynPayload, DynStreamShape, DynValue, DynValueView, DynYield,
};
use crate::{
    Bare, BareValueDomain, EdgeEndpointRole, ElementShape, EntityRef, EntityReference,
    EvaluateContext, EvaluateExpression, ExpandedChild, ExpandedIndex, ExpandedIndexAddress,
    ExpandedIndexReference, Explain, Expression, Failure, FailureKind, FailureKindValue,
    FailureValue, IndexDomain, IndexValue, Indexed, Mask, OrderState, Ordered, Positional,
    QueryResult, Scalar, Unit, Unordered, ValueDomain,
    dynamic::DynEntityRef,
    element::{
        Arity, ElementEmission, ElementTransition, Expanding, IndexedExpansionPipeline, Pipeline,
        Retention,
    },
    error::index::DuplicateExpandedChildIndex,
    execution::EvaluationCache,
    explain::ExplainFormatter,
    expressions::{ExpressionHandle, GroupedExpression, Partition},
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
    graphrecord::{AttributeName, EdgeIndex, GroupIndex, NodeIndex, Value},
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
type DynLaneOperationMarker<S, C, E> = PhantomData<fn() -> (S, C, E)>;

pub trait DynIndexProjection: IndexDomain {
    fn project_index<'a>(graphrecord: &'a GraphRecord, index: DynIndexView<'a>) -> Self::Index<'a>;

    fn erase_index(index: Self::Index<'_>) -> DynIndexView<'_>;

    fn erase_owned(index: Self::Owned) -> DynIndexOwned;

    fn project_address(address: DynIndexAddress) -> Self::Address;

    fn erase_address(address: Self::Address) -> DynIndexAddress;
}

fn index_projection_mismatch<I: IndexDomain>() -> ! {
    let expected = type_name::<I>();
    panic!("registry selected dynamic index data outside the typed domain {expected}")
}

impl DynIndexProjection for DynIndex {
    fn project_index<'a>(
        _graphrecord: &'a GraphRecord,
        index: DynIndexView<'a>,
    ) -> Self::Index<'a> {
        index
    }

    fn erase_index(index: Self::Index<'_>) -> DynIndexView<'_> {
        index
    }

    fn erase_owned(index: Self::Owned) -> DynIndexOwned {
        index
    }

    fn project_address(address: DynIndexAddress) -> Self::Address {
        address
    }

    fn erase_address(address: Self::Address) -> DynIndexAddress {
        address
    }
}

macro_rules! implement_direct_index_projection {
    ($domain:ty, $variant:ident) => {
        impl DynIndexProjection for $domain {
            fn project_index<'a>(
                _graphrecord: &'a GraphRecord,
                index: DynIndexView<'a>,
            ) -> Self::Index<'a> {
                let DynIndexView::$variant(index) = index else {
                    index_projection_mismatch::<Self>()
                };
                index
            }

            fn erase_index(index: Self::Index<'_>) -> DynIndexView<'_> {
                DynIndexView::$variant(index)
            }

            fn erase_owned(index: Self::Owned) -> DynIndexOwned {
                DynIndexOwned::$variant(index)
            }

            fn project_address(address: DynIndexAddress) -> Self::Address {
                let DynIndexAddress::$variant(address) = address else {
                    index_projection_mismatch::<Self>()
                };
                address
            }

            fn erase_address(address: Self::Address) -> DynIndexAddress {
                DynIndexAddress::$variant(address)
            }
        }
    };
}

implement_direct_index_projection!(Positional, Positional);
implement_direct_index_projection!(NodeIndex, Node);
implement_direct_index_projection!(EdgeIndex, Edge);
implement_direct_index_projection!(GroupIndex, Group);
implement_direct_index_projection!(AttributeName, Attribute);
implement_direct_index_projection!(Value, Value);
implement_direct_index_projection!(bool, Bool);
implement_direct_index_projection!(EdgeEndpointRole, EndpointRole);
implement_direct_index_projection!(FailureKind, FailureKind);

impl<P: DynIndexProjection, C: DynIndexProjection> DynIndexProjection for ExpandedIndex<P, C> {
    fn project_index<'a>(graphrecord: &'a GraphRecord, index: DynIndexView<'a>) -> Self::Index<'a> {
        let DynIndexView::Expanded(index) = index else {
            index_projection_mismatch::<Self>()
        };
        let (parent, child) = index.into_parts();
        let parent = P::project_index(graphrecord, parent);
        match child {
            None => ExpandedIndexReference::parent(parent),
            Some(child) => {
                ExpandedIndexReference::child(parent, C::project_index(graphrecord, child))
            }
        }
    }

    fn erase_index(index: Self::Index<'_>) -> DynIndexView<'_> {
        let parent = P::erase_index(index.parent_index().clone());
        let index = match index.child_index() {
            None => DynExpandedView::parent(parent),
            Some(child) => DynExpandedView::child(parent, C::erase_index(child.clone())),
        };
        DynIndexView::Expanded(Box::new(index))
    }

    fn erase_owned(index: Self::Owned) -> DynIndexOwned {
        let (parent, child) = index.into_parts();
        let parent = P::erase_owned(parent);
        let index = match child {
            None => DynExpandedOwned::parent(parent),
            Some(child) => DynExpandedOwned::child(parent, C::erase_owned(child)),
        };
        DynIndexOwned::Expanded(Box::new(index))
    }

    fn project_address(address: DynIndexAddress) -> Self::Address {
        let DynIndexAddress::Expanded(address) = address else {
            index_projection_mismatch::<Self>()
        };
        let (parent, child) = address.into_parts();
        let parent = P::project_address(parent);
        match child {
            None => ExpandedIndexAddress::parent(parent),
            Some(child) => ExpandedIndexAddress::child(parent, C::project_address(child)),
        }
    }

    fn erase_address(address: Self::Address) -> DynIndexAddress {
        let (parent, child) = address.into_parts();
        let parent = P::erase_address(parent);
        let address = match child {
            None => DynExpandedAddress::parent(parent),
            Some(child) => DynExpandedAddress::child(parent, C::erase_address(child)),
        };
        DynIndexAddress::Expanded(Box::new(address))
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
        let DynValueView::Scalar(value) = value else {
            value_projection_mismatch::<Self>()
        };
        value
    }

    fn erase(value: Self::Value<'_>) -> <Self::Dynamic as ValueDomain>::Value<'_> {
        DynValueView::Scalar(value)
    }
}

impl DynValueProjection for AttributeName {
    type Dynamic = DynValue;

    fn project<'a>(
        _graphrecord: &'a GraphRecord,
        value: <Self::Dynamic as ValueDomain>::Value<'a>,
    ) -> Self::Value<'a> {
        let DynValueView::Attribute(value) = value else {
            value_projection_mismatch::<Self>()
        };
        value
    }

    fn erase(value: Self::Value<'_>) -> <Self::Dynamic as ValueDomain>::Value<'_> {
        DynValueView::Attribute(value)
    }
}

impl<I: DynIndexProjection> DynValueProjection for IndexValue<I> {
    type Dynamic = DynValue;

    fn project<'a>(
        graphrecord: &'a GraphRecord,
        value: <Self::Dynamic as ValueDomain>::Value<'a>,
    ) -> Self::Value<'a> {
        let DynValueView::Index(value) = value else {
            value_projection_mismatch::<Self>()
        };
        I::project_index(graphrecord, value)
    }

    fn erase(value: Self::Value<'_>) -> <Self::Dynamic as ValueDomain>::Value<'_> {
        DynValueView::Index(I::erase_index(value))
    }
}

impl DynValueProjection for EntityReference<NodeIndex> {
    type Dynamic = DynValue;

    fn project<'a>(
        _graphrecord: &'a GraphRecord,
        value: <Self::Dynamic as ValueDomain>::Value<'a>,
    ) -> Self::Value<'a> {
        let DynValueView::EntityReference(value) = value else {
            value_projection_mismatch::<Self>()
        };

        EntityRef::new(
            value.graphrecord(),
            NodeIndex::project_address(value.address().clone()),
        )
    }

    fn erase(value: Self::Value<'_>) -> <Self::Dynamic as ValueDomain>::Value<'_> {
        DynValueView::EntityReference(DynEntityRef::new(
            value.graphrecord(),
            NodeIndex::erase_address(*value.address()),
        ))
    }
}

impl DynValueProjection for EntityReference<EdgeIndex> {
    type Dynamic = DynValue;

    fn project<'a>(
        _graphrecord: &'a GraphRecord,
        value: <Self::Dynamic as ValueDomain>::Value<'a>,
    ) -> Self::Value<'a> {
        let DynValueView::EntityReference(value) = value else {
            value_projection_mismatch::<Self>()
        };

        EntityRef::new(
            value.graphrecord(),
            EdgeIndex::project_address(value.address().clone()),
        )
    }

    fn erase(value: Self::Value<'_>) -> <Self::Dynamic as ValueDomain>::Value<'_> {
        DynValueView::EntityReference(DynEntityRef::new(
            value.graphrecord(),
            EdgeIndex::erase_address(*value.address()),
        ))
    }
}

impl DynValueProjection for EntityReference<GroupIndex> {
    type Dynamic = DynValue;

    fn project<'a>(
        _graphrecord: &'a GraphRecord,
        value: <Self::Dynamic as ValueDomain>::Value<'a>,
    ) -> Self::Value<'a> {
        let DynValueView::EntityReference(value) = value else {
            value_projection_mismatch::<Self>()
        };

        EntityRef::new(
            value.graphrecord(),
            GroupIndex::project_address(value.address().clone()),
        )
    }

    fn erase(value: Self::Value<'_>) -> <Self::Dynamic as ValueDomain>::Value<'_> {
        DynValueView::EntityReference(DynEntityRef::new(
            value.graphrecord(),
            GroupIndex::erase_address(*value.address()),
        ))
    }
}

impl DynValueProjection for FailureValue {
    type Dynamic = DynValue;

    fn project<'a>(
        _graphrecord: &'a GraphRecord,
        value: <Self::Dynamic as ValueDomain>::Value<'a>,
    ) -> Self::Value<'a> {
        let DynValueView::Failure(value) = value else {
            value_projection_mismatch::<Self>()
        };
        *value
    }

    fn erase(value: Self::Value<'_>) -> <Self::Dynamic as ValueDomain>::Value<'_> {
        DynValueView::Failure(Box::new(value))
    }
}

impl DynValueProjection for FailureKindValue {
    type Dynamic = DynValue;

    fn project<'a>(
        _graphrecord: &'a GraphRecord,
        value: <Self::Dynamic as ValueDomain>::Value<'a>,
    ) -> Self::Value<'a> {
        let DynValueView::FailureKind(value) = value else {
            value_projection_mismatch::<Self>()
        };
        value
    }

    fn erase(value: Self::Value<'_>) -> <Self::Dynamic as ValueDomain>::Value<'_> {
        DynValueView::FailureKind(value)
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
        let (address, outcome) = element;
        (
            I::project_address(address),
            outcome.map(|value| V::project(graphrecord, value)),
        )
    }

    fn erase_element(element: Self::Element<'_>) -> <Self::Dynamic as ElementShape>::Element<'_> {
        let (address, outcome) = element;
        (I::erase_address(address), outcome.map(V::erase))
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

pub trait DynExpressionProjection: Expression {
    type Dynamic: Expression;

    fn erase_expression(self) -> Self::Dynamic {
        Self::Dynamic::new(DynProjectionContext { input: self })
    }

    fn erase<'a>(
        values: Self::ReturnValue<'a>,
    ) -> <Self::Dynamic as EvaluateExpression>::ReturnValue<'a>
    where
        Self: 'a;
}

pub trait DynPayloadOutput: Expression {
    fn into_yield<'a>(values: Self::ReturnValue<'a>) -> DynYield<'a>
    where
        Self: 'a;
}

impl<S, C> DynExpressionProjection for ExpressionHandle<S, C>
where
    S: DynShapeProjection,
    S::Dynamic: ElementShape,
    C: Arity,
{
    type Dynamic = ExpressionHandle<S::Dynamic, C>;

    fn erase<'a>(
        values: Self::ReturnValue<'a>,
    ) -> <Self::Dynamic as EvaluateExpression>::ReturnValue<'a>
    where
        Self: 'a,
    {
        C::map_elements(values, S::erase_element)
    }
}

impl<S: DynStreamShape, C: DynArity> DynPayloadOutput for ExpressionHandle<S, C> {
    fn into_yield<'a>(values: Self::ReturnValue<'a>) -> DynYield<'a>
    where
        Self: 'a,
    {
        DynYield::Lane(S::erase::<C>(values))
    }
}

impl<M, K, S, C> DynExpressionProjection for GroupedExpression<M, K, ExpressionHandle<S, C>>
where
    M: DynIndexProjection,
    K: DynIndexProjection + IndexDomain,
    S: DynStreamShape,
    C: DynArity,
{
    type Dynamic = GroupedExpression<DynIndex, DynIndex, DynPayload>;

    fn erase<'a>(
        values: Self::ReturnValue<'a>,
    ) -> <Self::Dynamic as EvaluateExpression>::ReturnValue<'a>
    where
        Self: 'a,
    {
        values
            .map_domains(M::erase_address, K::erase_owned)
            .map_payloads(|_, _, payload| {
                payload.map(|values| DynYield::Lane(S::erase::<C>(values)))
            })
    }
}

impl DynPayloadOutput for GroupedExpression<DynIndex, DynIndex, DynPayload> {
    fn into_yield<'a>(values: Self::ReturnValue<'a>) -> DynYield<'a>
    where
        Self: 'a,
    {
        DynYield::Group(values)
    }
}

struct DynProjectionContext<E: DynExpressionProjection> {
    input: E,
}

impl<E: DynExpressionProjection> PlanNode for DynProjectionContext<E> {
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

impl<E: DynExpressionProjection> OptimizerHints for DynProjectionContext<E> {
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

impl<E: DynExpressionProjection> Explain for DynProjectionContext<E> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        self.input.describe(formatter)
    }
}

impl<E: DynExpressionProjection> Estimated for DynProjectionContext<E> {
    fn estimate(&self, stats: &Stats) -> Estimate {
        self.input.context().estimate(stats)
    }
}

impl<E: DynExpressionProjection> OptimizePlan for DynProjectionContext<E> {
    type Output = E::Dynamic;

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

impl<E: DynExpressionProjection> EvaluateContext for DynProjectionContext<E> {
    type Expression = E::Dynamic;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache,
    ) -> QueryResult<<Self::Expression as EvaluateExpression>::ReturnValue<'a>> {
        self.input.evaluate(graphrecord, cache).map(E::erase)
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
                cache: &'a EvaluationCache,
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
        Ok(Pipeline::keyed(move |address, outcome: QueryResult<_>| {
            let typed_address = I::project_address(address);
            let outcome = outcome.map(|value| V::project(graphrecord, value));
            let step = pipeline.run((typed_address, outcome));
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
        Ok(Pipeline::keyed(move |address, outcome: QueryResult<_>| {
            let typed_address = I::project_address(address);
            let outcome = outcome.map(|value| V::project(graphrecord, value));
            let step = pipeline.run((typed_address, outcome));
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
    graphrecord: &'a GraphRecord,
    parent: DynIndexAddress,
    source: QueryResult<V::Value<'a>>,
    pipeline: &IndexedExpansionPipeline<'a, DynIndex, DynIndex, V, W, O>,
) -> Vec<(DynIndexAddress, QueryResult<W::Value<'a>>)>
where
    V: ValueDomain,
    W: ValueDomain,
    O: OrderState,
    Expanding<O>: ElementEmission,
{
    let source_value = match source {
        Ok(value) => value,
        Err(failure) => {
            let parent_address = DynExpandedAddress::parent(parent);
            return vec![(
                DynIndexAddress::Expanded(Box::new(parent_address)),
                Err(failure),
            )];
        }
    };

    let children = match pipeline.run((parent.clone(), source_value)) {
        Ok(children) => children,
        Err(failure) => {
            let parent_address = DynExpandedAddress::parent(parent);
            return vec![(
                DynIndexAddress::Expanded(Box::new(parent_address)),
                Err(failure),
            )];
        }
    };

    let mut seen_children = GrHashSet::default();
    let mut fragment = Vec::with_capacity(children.len());

    for child in children {
        let (child_address, outcome) = child.into_parts();

        if !seen_children.insert(child_address.clone()) {
            let parent_address = DynExpandedAddress::parent(parent);
            let address = DynIndexAddress::Expanded(Box::new(parent_address));
            let failure = Failure::new_at_address::<DynIndex, _>(
                DuplicateExpandedChildIndex::<DynIndex>::new(DynIndex::own_index(
                    &DynIndex::index(graphrecord, &child_address),
                )),
                graphrecord,
                &address,
                "indexed expansion",
            );

            return vec![(address, Err(failure))];
        }

        let child = DynExpandedAddress::child(parent.clone(), child_address);
        fragment.push((DynIndexAddress::Expanded(Box::new(child)), outcome));
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
        graphrecord: &'a GraphRecord,
        values: A::Container<'a, Self::Element<'a>>,
        pipeline: Self::Pipeline<'a>,
    ) -> <<Expanding<Ordered> as ElementEmission>::OutArity<A> as Arity>::Container<
        'a,
        <Indexed<DynIndex, W> as ElementShape>::Element<'a>,
    > {
        Expanding::<Ordered>::apply::<A, _, _>(values, move |(parent, source)| {
            expand_dyn_source::<V, _, _>(graphrecord, parent, source, &pipeline)
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
        graphrecord: &'a GraphRecord,
        values: A::Container<'a, Self::Element<'a>>,
        pipeline: Self::Pipeline<'a>,
    ) -> <<Expanding<Unordered> as ElementEmission>::OutArity<A> as Arity>::Container<
        'a,
        <Indexed<DynIndex, W> as ElementShape>::Element<'a>,
    > {
        Expanding::<Unordered>::apply::<A, _, _>(values, move |(parent, source)| {
            expand_dyn_source::<V, _, _>(graphrecord, parent, source, &pipeline)
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
        Ok(Pipeline::new(move |(address, value)| {
            let address = I::project_address(address);
            let value = V::project(graphrecord, value);
            pipeline.run((address, value)).map(|children| {
                children
                    .into_iter()
                    .map(|child| {
                        let (address, outcome) = child.into_parts();
                        ExpandedChild::from_outcome(
                            C::erase_address(address),
                            outcome.map(W::erase),
                        )
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
    P: GroupKernel<DynIndex, DynIndex, ExpressionHandle<S, C>>,
    S: DynStreamShape,
    C: DynArity,
    P::Output: DynExpressionProjection,
{
    type Output = <P::Output as DynExpressionProjection>::Dynamic;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        partition: Partition<'a, DynIndex, DynIndex, DynPayload>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let partition = project_group_partition::<S, C>(partition);

        P::execute(graphrecord, partition, prepared).map(P::Output::erase)
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        self.operation.estimate(input, stats)
    }
}

fn project_group_partition<S: DynStreamShape, C: DynArity>(
    partition: Partition<'_, DynIndex, DynIndex, DynPayload>,
) -> Partition<'_, DynIndex, DynIndex, ExpressionHandle<S, C>> {
    partition.map_payloads(|_, _, payload| {
        payload.map(|yielded| {
            let DynYield::Lane(stream) = yielded else {
                panic!("registry selected a group kernel at the wrong dynamic nesting depth")
            };

            S::project::<C>(stream)
        })
    })
}

pub struct DynLaneOperation<P, S, C, E> {
    operation: P,
    marker: DynLaneOperationMarker<S, C, E>,
}

impl<P, S, C, E> DynLaneOperation<P, S, C, E> {
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

implement_operation_forwarding!(DynLaneOperation<S, C, E>, Lane);

impl<P, S, C, E> LaneKernel<S::Dynamic, C> for DynLaneOperation<P, S, C, E>
where
    P: LaneKernel<S, C, Output = E>,
    S: DynShapeProjection,
    S::Dynamic: ElementShape,
    C: Arity,
    E: DynExpressionProjection,
{
    type Output = E::Dynamic;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: <ExpressionHandle<S::Dynamic, C> as EvaluateExpression>::ReturnValue<'a>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let values = C::map_elements(values, |element| S::project_element(graphrecord, element));
        P::execute(graphrecord, values, prepared).map(E::erase)
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        self.operation.estimate(input, stats)
    }
}
