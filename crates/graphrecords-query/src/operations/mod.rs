mod aggregation;
mod argument;
mod arithmetic;
mod cache;
mod comparison;
mod conversion;
mod grouping;
mod indexing;
mod is_type;
mod kernel;
mod logic;
mod membership;
mod numeric;
mod on_error;
mod on_missing;
mod ordering;
mod retention;
mod string_operations;
mod structure;
mod traversal;

use crate::{
    EvaluateContext, EvaluateOperand, Explain, Operand, QueryResult,
    execution::EvaluationCache,
    explain::ExplainFormatter,
    optimizer::{
        EmptyRule, Estimate, Estimated, MatchInputs, OperationInputs, OptimizePlan, OptimizerHints,
        PlanInputs, PlanNode, Session, Stats, Transformed,
    },
};
pub use aggregation::{CountOperation, MaxOperation};
pub use argument::{
    Absent, Alignment, ArgumentAbsent, ArgumentSource, Keyed, Lookup, Prepare, Unaligned,
};
pub use arithmetic::ModuloOperation;
pub use cache::CacheContext;
pub use conversion::EnumerateOperation;
use graphrecords_core::GraphRecord;
pub use graphrecords_macros::Operation;
pub use grouping::{
    BroadcastOperation, GroupByOperation, GroupKey, KeyOperand, MissingGroupAggregate,
    UngroupContext, Ungroupable,
};
pub use indexing::{IndexOperation, SelectOperation};
pub use kernel::{BareStream, ElementKernel, ElementPipeline, Kernel, KeyedStream, Pipeline};
pub use logic::{AndOperation, NotOperation, OrOperation, XorOperation};
pub use on_error::{Drop, ErrorPolicy, Raise, Replace};
pub use on_missing::{MissingPolicy, WithMissing};
pub use ordering::{
    EnsureSortable, FirstOperation, IncomparableIndices, LastOperation, SortByOperation,
    SortOperation, UnorderOperation, incomparable_with_first,
};
pub use retention::{Dropping, Preserving, Retention};
use std::{
    any::Any,
    fmt,
    hash::{Hash, Hasher},
};
pub use structure::{
    AttributeOperation, EntityAttributes, FilterOperation, InGroupOperation, IndicesInGroup,
    MissingAttribute, MissingTraversedAttribute,
};
pub use traversal::{
    EdgeSource, EdgeTarget, EdgesOperation, NeighborsOperation, NodesOperation, Relation,
    RelationOperation,
};

pub trait Operation: Prepare + OperationInputs + Explain {}

pub trait Apply<P: Operation>: Operand {
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

pub struct OperationContext<I, P>
where
    I: Apply<P>,
    P: Operation,
{
    input: I,
    operation: P,
}

impl<I, P> OperationContext<I, P>
where
    I: Apply<P>,
    P: Operation,
{
    #[must_use]
    pub const fn new(input: I, operation: P) -> Self {
        Self { input, operation }
    }

    #[must_use]
    pub const fn operation(&self) -> &P {
        &self.operation
    }
}

impl<I, P> MatchInputs for OperationContext<I, P>
where
    I: Apply<P>,
    P: Operation,
{
    type Inputs<'a> = P::Inputs<'a, I>;

    fn inputs(&self) -> Self::Inputs<'_> {
        OperationInputs::inputs(&self.operation, &self.input)
    }
}

impl<I, P> PlanNode for OperationContext<I, P>
where
    I: Apply<P>,
    P: Operation,
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
        Any::type_id(self).hash(&mut state);
        self.operation.identity_hash(&mut state);
        self.input.as_plan_node().dyn_hash(state);
    }
}

impl<I, P> OptimizerHints for OperationContext<I, P>
where
    I: Apply<P>,
    P: Operation,
{
    fn commutes_with_filter(&self) -> bool {
        self.operation.commutes_with_filter()
    }

    fn allows_limit_pushdown(&self) -> bool {
        self.operation.allows_limit_pushdown()
    }

    fn is_distinct(&self) -> bool {
        self.operation.is_distinct()
    }

    fn is_volatile(&self) -> bool {
        self.operation.is_volatile()
    }

    fn empty_rule(&self) -> EmptyRule {
        self.operation.empty_rule()
    }
}

impl<I, P> Explain for OperationContext<I, P>
where
    I: Apply<P>,
    P: Operation,
{
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        formatter.child(&self.input);
        self.operation.describe(formatter)?;

        Ok(())
    }
}

impl<I, P> EvaluateContext for OperationContext<I, P>
where
    I: Apply<P>,
    P: Operation,
{
    type Operand = <I as Apply<P>>::Output;

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

impl<I, P> Estimated for OperationContext<I, P>
where
    I: Apply<P>,
    P: Operation,
{
    fn estimate(&self, stats: &Stats) -> Estimate {
        <I as Apply<P>>::estimate(&self.operation, self.input.context().estimate(stats), stats)
    }
}

impl<I, P> OptimizePlan for OperationContext<I, P>
where
    I: Apply<P>,
    P: Operation,
{
    type Output = <I as Apply<P>>::Output;

    fn optimize(&self, original: &Self::Output, session: &Session) -> Transformed<Self::Output> {
        let input = session.optimize(&self.input);
        let operation = self.operation.optimize(session);

        if !input.changed && !operation.changed {
            return Transformed::unchanged(original.clone());
        }

        Transformed {
            value: <Self::Output as Operand>::new(Self {
                input: input.value,
                operation: operation.value,
            }),
            changed: true,
        }
    }
}
