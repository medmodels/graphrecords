mod attributes;
mod bool;
mod context;
mod edges;
mod elements;
mod errors;
mod grouped;
mod groups;
mod indices;
mod nodes;
mod references;
mod values;

use crate::{
    BoxedIterator, Failure, IndexDomain, QueryResult,
    element::{Arity, ElementShape, Return},
    error::index::DuplicateIndex,
    execution::EvaluationCache,
    explain::{Explanation, write_truncated_plan},
    optimizer::{Estimate, Estimated, PlanNode, Stats},
    value::ValueDomain,
};
pub use attributes::{
    AttributeExpression, AttributesExpression, BareAttributeExpression, BareAttributesExpression,
    DefiniteAttributeExpression, DefiniteBareAttributeExpression,
};
pub use bool::{
    BareBoolExpression, BareBoolMaskExpression, BoolExpression, BoolMaskExpression,
    DefiniteBareBoolExpression, DefiniteBoolExpression,
};
pub use context::{EvaluateContext, ExpressionContext};
pub use edges::{AllEdges, DefiniteEdgeExpression, EdgeExpression, EdgesExpression};
pub use elements::{DefiniteElementExpression, ElementExpression, ElementsExpression};
pub use errors::{
    BareFailureExpression, BareFailureKindExpression, BareFailureKindsExpression,
    BareFailuresExpression, DefiniteBareFailureExpression, DefiniteBareFailureKindExpression,
    DefiniteFailureExpression, DefiniteFailureKindExpression, FailureExpression,
    FailureKindExpression, FailureKindsExpression, FailuresExpression,
};
use graphrecords_core::GraphRecord;
use graphrecords_utils::aliases::GrHashSet;
pub use grouped::{
    Bucket, BucketChange, GroupedExpression, KeyFailure, KeyFailureChange, OwnedBucket,
    OwnedKeyFailure, OwnedPartition, OwnedPartitionParts, Partition, PartitionArity,
    PartitionBucketParts, PartitionBuilder, PartitionClassification, PartitionKeyFailureParts,
    PartitionParts, PartitionShape, ReturnBucket, ReturnKeyFailure, ReturnPartition,
    ReturnPartitionParts,
};
pub use groups::{AllGroups, DefiniteGroupExpression, GroupExpression, GroupsExpression};
pub use indices::{
    BareIndexExpression, BareIndicesExpression, DefiniteBareIndexExpression,
    DefiniteIndexExpression, IndexExpression, IndicesExpression,
};
pub use nodes::{AllNodes, DefiniteNodeExpression, NodeExpression, NodesExpression};
pub use references::{
    BareReferenceExpression, BareReferencesExpression, DefiniteBareReferenceExpression,
    DefiniteReferenceExpression, DefiniteReferenceIndexExpression, ReferenceExpression,
    ReferenceIndexExpression, ReferenceIndicesExpression, ReferencesExpression,
};
use std::{fmt, sync::Arc};
pub use values::{
    BareValueExpression, BareValuesExpression, DefiniteBareValueExpression,
    DefiniteValueExpression, ValueExpression, ValuesExpression,
};

pub trait EvaluateExpression {
    type ReturnValue<'a>: 'a
    where
        Self: 'a;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache,
    ) -> QueryResult<Self::ReturnValue<'a>>;
}

pub trait Expression: 'static + Sized + Clone + EvaluateExpression + Send + Sync {
    fn context(&self) -> &dyn ExpressionContext<Self>;

    fn as_plan_node(&self) -> &dyn PlanNode;

    fn from_context(context: Arc<dyn ExpressionContext<Self>>) -> Self;

    #[must_use]
    fn new<C: ExpressionContext<Self>>(context: C) -> Self {
        Self::from_context(Arc::new(context))
    }

    fn explain(&self) -> Explanation<'_> {
        Explanation::new(self)
    }
}

pub struct ExpressionHandle<S: ElementShape, C: Arity> {
    context: Arc<dyn ExpressionContext<Self>>,
}

impl<S: ElementShape, C: Arity> Clone for ExpressionHandle<S, C> {
    fn clone(&self) -> Self {
        Self {
            context: Arc::clone(&self.context),
        }
    }
}

impl<S: ElementShape, C: Arity> fmt::Debug for ExpressionHandle<S, C> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Expression [")?;
        write_truncated_plan(formatter, self)?;
        formatter.write_str("]")
    }
}

impl<S: ElementShape, C: Arity> EvaluateExpression for ExpressionHandle<S, C> {
    type ReturnValue<'a> = Return<'a, S, C>;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache,
    ) -> QueryResult<Self::ReturnValue<'a>> {
        self.context.evaluate(graphrecord, cache)
    }
}

impl<S: ElementShape, C: Arity> Estimated for ExpressionHandle<S, C> {
    fn estimate(&self, stats: &Stats) -> Estimate {
        self.context().estimate(stats)
    }
}

impl<S: ElementShape, C: Arity> Expression for ExpressionHandle<S, C> {
    fn context(&self) -> &dyn ExpressionContext<Self> {
        self.context.as_ref()
    }

    fn as_plan_node(&self) -> &dyn PlanNode {
        self.context.as_ref()
    }

    fn from_context(context: Arc<dyn ExpressionContext<Self>>) -> Self {
        Self { context }
    }
}

pub struct CheckedIndexedLaneBuilder<'a, I: IndexDomain, V: ValueDomain> {
    seen: GrHashSet<I::Address>,
    elements: Vec<(I::Address, QueryResult<V::Value<'a>>)>,
}

impl<'a, I: IndexDomain, V: ValueDomain> CheckedIndexedLaneBuilder<'a, I, V> {
    #[must_use]
    pub fn new() -> Self {
        Self {
            seen: GrHashSet::default(),
            elements: Vec::new(),
        }
    }

    pub fn push(
        &mut self,
        graphrecord: &'a GraphRecord,
        address: I::Address,
        outcome: QueryResult<V::Value<'a>>,
    ) -> QueryResult<()> {
        if !self.seen.insert(address.clone()) {
            let index = I::index(graphrecord, &address);

            return Err(Failure::new_at::<I, _>(
                DuplicateIndex::<I>::new(I::own_index(&index)),
                &index,
                "indexed values construction",
            ));
        }

        self.elements.push((address, outcome));

        Ok(())
    }

    #[must_use]
    pub fn finish(self) -> BoxedIterator<'a, (I::Address, QueryResult<V::Value<'a>>)> {
        Box::new(self.elements.into_iter())
    }
}

impl<I: IndexDomain, V: ValueDomain> Default for CheckedIndexedLaneBuilder<'_, I, V> {
    fn default() -> Self {
        Self::new()
    }
}
