mod aggregation;
mod argument;
mod arithmetic;
mod cache;
mod comparison;
mod conversion;
mod errors;
mod grouping;
mod indexing;
mod is_type;
mod kernel;
mod logic;
mod membership;
mod numeric;
mod on_missing;
mod ordering;
pub mod policy;
mod string_operations;
mod structure;
mod traversal;
mod uniqueness;

use crate::{
    EvaluateContext, EvaluateOperand, Explain, Operand, QueryResult,
    execution::EvaluationCache,
    explain::ExplainFormatter,
    optimizer::{
        EmptyRule, Estimate, Estimated, MatchInputs, OperationInputs, OptimizePlan, OptimizerHints,
        PlanInputs, PlanNode, Session, Stats, Transformed,
    },
    registry::OperationManifest,
    sealed::Sealed,
};
pub use aggregation::{
    AllOperation, AnyOperation, CountOperation, MaximumOperation, MeanOperation, MedianOperation,
    MinimumOperation, ModeOperation, ProductOperation, RandomOperation, StandardDeviationOperation,
    SumOperation, UniqueCountOperation, VarianceOperation,
};
pub use argument::{
    AlignableArity, Alignment, Argument, ArgumentPlan, ArgumentSource, EnumerableArity,
    IndexedElementContainer, IndexedElementSource, IntoArgument, Keyed, Lookup, Prepare,
    PreparedArgument, PreparedArity, PreparedIndexedMultiple, SetArity, SetSource, SourceDomain,
    Unaligned,
};
pub use arithmetic::{
    AddOperation, DivideOperation, ModuloOperation, MultiplyOperation, PowerOperation,
    SubtractOperation,
};
pub use cache::CacheContext;
pub use comparison::{
    EqualToOperation, GreaterThanOperation, GreaterThanOrEqualToOperation, LessThanOperation,
    LessThanOrEqualToOperation, NotEqualToOperation,
};
pub use conversion::{
    CastOperation, DiscardIndexOperation, DiscardValueOperation, EnumerateOperation,
    ExpandToOperation, TransitionOperation,
};
pub use errors::{
    DropErrorsIn, DropErrorsOf, DropErrorsWithCause, ErrorKindNameOperation, ErrorKindOperation,
    ErrorPolicy, ErrorPolicyIn, ErrorPolicyOf, ErrorPolicyWithCause, ErrorsOperation,
    HasErrorCauseOperation, InErrorGroupOperation, IsErrorKindOperation, RaiseErrorsIn,
    RaiseErrorsOf, RaiseErrorsWithCause, RaiseWhenErrorsIn, RaiseWhenErrorsOf,
    RaiseWhenErrorsWithCause, ReplaceErrorsIn, ReplaceErrorsOf, ReplaceErrorsWithCause,
};
use graphrecords_core::GraphRecord;
pub use graphrecords_macros::Operation;
pub use grouping::{
    BroadcastOperation, BroadcastViaOperation, BucketErrorPolicy, BucketErrorPolicyIn,
    BucketErrorPolicyOf, BucketErrorPolicyWithCause, BucketErrorsOperation, BucketFailureArity,
    DropBucketErrors, DropBucketErrorsIn, DropBucketErrorsOf, DropBucketErrorsWithCause,
    DropKeyErrors, DropKeyErrorsIn, DropKeyErrorsOf, DropKeyErrorsWithCause, GroupByOperation,
    HavingOperation, KeyErrorPolicy, KeyErrorPolicyIn, KeyErrorPolicyOf, KeyErrorPolicyWithCause,
    KeyErrorsOperation, KeysOperation, RaiseBucketErrors, RaiseBucketErrorsIn, RaiseBucketErrorsOf,
    RaiseBucketErrorsWithCause, RaiseKeyErrors, RaiseKeyErrorsIn, RaiseKeyErrorsOf,
    RaiseKeyErrorsWithCause, UngroupKeyedOperation, UngroupOperation,
};
pub use indexing::{
    ChildIndexOperation, IndexOperation, ParentIndexOperation, ResolveOperation, SelectOperation,
};
pub use is_type::{
    IsBoolOperation, IsDateTimeOperation, IsDurationOperation, IsFloatOperation, IsIntOperation,
    IsNullOperation, IsStringOperation,
};
pub use kernel::{
    BareStream, ElementKernel, ElementPipeline, GroupKernel, KeyedStream, LaneKernel,
};
pub use logic::{AndOperation, ExclusiveOrOperation, NotOperation, OrOperation};
pub use membership::IsInOperation;
pub use numeric::{
    AbsoluteOperation, CeilOperation, ClipOperation, CubeRootOperation, ExponentialOperation,
    FloorOperation, LogarithmOperation, NegateOperation, RoundOperation, SignOperation,
    SquareRootOperation,
};
pub use on_missing::{MaybeAbsent, MissingPolicy, WithMissing};
pub use ordering::{
    FirstOperation, LastOperation, ReverseOrderOperation, ShuffleOperation, SortByOperation,
    SortOperation, TakeOperation, UnorderOperation,
};
use std::{
    any::Any,
    fmt,
    hash::{Hash, Hasher},
};
pub use string_operations::{
    ContainsOperation, EndsWithOperation, LengthOperation, LowercaseOperation, MatchesOperation,
    PadEndOperation, PadStartOperation, ReplaceAllOperation, ReplaceOperation, ReverseOperation,
    SliceOperation, SplitOperation, StartsWithOperation, StripPrefixOperation,
    StripSuffixOperation, TrimEndOperation, TrimOperation, TrimStartOperation, UppercaseOperation,
};
pub use structure::{
    AttributeOperation, AttributesOperation, FilterOperation, HasAttributeOperation,
    InGroupOperation,
};
pub use traversal::{
    EdgeDirection, EdgesOperation, EndpointOperation, NeighborsOperation, NodesOperation,
    ViaEdgesOperation, ViaNeighborsOperation, ViaNodesOperation,
};
pub use uniqueness::{DropDuplicatesOperation, IsDuplicatedOperation, UniqueOperation};

pub trait OperationScope: Sealed + 'static {}

pub struct Element;
pub struct Lane;
pub struct Group;

impl Sealed for Element {}
impl Sealed for Lane {}
impl Sealed for Group {}

impl OperationScope for Element {}
impl OperationScope for Lane {}
impl OperationScope for Group {}

pub trait Operation: Prepare + OperationInputs + Explain {
    type Scope: OperationScope;
}

pub trait Apply<P: Operation<Scope = S>, S: OperationScope = <P as Operation>::Scope>:
    Operand
{
    type Output: Operand;

    fn apply<'a>(
        graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        prepared: P::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a;

    fn estimate(operation: &P, input: Estimate, stats: &Stats) -> Estimate;
}

pub struct OperationContext<I: Apply<P>, P: Operation> {
    input: I,
    operation: P,
}

impl<I: Apply<P>, P: Operation> OperationContext<I, P> {
    #[must_use]
    pub const fn new(input: I, operation: P) -> Self {
        Self { input, operation }
    }

    #[must_use]
    pub const fn operation(&self) -> &P {
        &self.operation
    }
}

impl<I: Apply<P>, P: Operation> MatchInputs for OperationContext<I, P> {
    type Inputs<'a> = P::Inputs<'a, I>;

    fn inputs(&self) -> Self::Inputs<'_> {
        OperationInputs::inputs(&self.operation, &self.input)
    }
}

impl<I: Apply<P>, P: Operation> PlanNode for OperationContext<I, P> {
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

impl<I: Apply<P>, P: Operation> OptimizerHints for OperationContext<I, P> {
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

impl<I: Apply<P>, P: Operation> Explain for OperationContext<I, P> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        formatter.child(&self.input);
        self.operation.describe(formatter)?;

        Ok(())
    }
}

impl<I: Apply<P>, P: Operation> EvaluateContext for OperationContext<I, P> {
    type Operand = I::Output;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<<Self::Operand as EvaluateOperand>::ReturnValue<'a>> {
        let values = self.input.evaluate(graphrecord, cache)?;
        let prepared = self.operation.prepare(graphrecord, cache)?;

        I::apply(graphrecord, values, prepared)
    }
}

impl<I: Apply<P>, P: Operation> Estimated for OperationContext<I, P> {
    fn estimate(&self, stats: &Stats) -> Estimate {
        I::estimate(&self.operation, self.input.context().estimate(stats), stats)
    }
}

impl<I: Apply<P>, P: Operation> OptimizePlan for OperationContext<I, P> {
    type Output = I::Output;

    fn optimize(&self, original: &Self::Output, session: &Session) -> Transformed<Self::Output> {
        let input = session.optimize(&self.input);
        let operation = self.operation.optimize(session);

        if !input.is_changed() && !operation.is_changed() {
            return Transformed::unchanged(original.clone());
        }

        let input = input.into_parts().0;
        let operation = operation.into_parts().0;

        Transformed::changed(Self::Output::new(Self { input, operation }))
    }
}

pub(crate) fn operation_manifests() -> Vec<OperationManifest> {
    aggregation::operation_manifests()
        .into_iter()
        .chain(arithmetic::operation_manifests())
        .chain(comparison::operation_manifests())
        .chain(conversion::operation_manifests())
        .chain(errors::operation_manifests())
        .chain(grouping::operation_manifests())
        .chain(indexing::operation_manifests())
        .chain(is_type::operation_manifests())
        .chain(logic::operation_manifests())
        .chain(membership::operation_manifests())
        .chain(numeric::operation_manifests())
        .chain(ordering::operation_manifests())
        .chain(string_operations::operation_manifests())
        .chain(structure::operation_manifests())
        .chain(traversal::operation_manifests())
        .chain(uniqueness::operation_manifests())
        .collect()
}
