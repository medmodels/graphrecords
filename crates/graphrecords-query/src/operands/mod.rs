mod attributes;
mod bool;
mod context;
mod edges;
mod elements;
mod errors;
mod group;
mod indices;
mod nodes;
mod references;
mod values;

use crate::{
    BoxedIterator, DuplicateIndex, Failure, IndexDomain, QueryResult,
    element::{Arity, ElementShape, Return},
    execution::EvaluationCache,
    explain::Explanation,
    optimizer::{Estimate, Estimated, PlanNode, Stats},
    value::ValueType,
};
pub use attributes::{
    AttributeOperand, AttributesOperand, BareAttributeOperand, BareAttributesOperand,
    DefiniteAttributeOperand, DefiniteBareAttributeOperand,
};
pub use bool::{
    BareBoolMaskOperand, BareBoolOperand, BoolMaskOperand, BoolOperand, DefiniteBareBoolOperand,
    DefiniteBoolOperand,
};
pub use context::{EvaluateContext, OperandContext};
pub use edges::{AllEdges, DefiniteEdgeOperand, EdgeOperand, EdgesOperand};
pub use elements::{DefiniteElementOperand, ElementOperand, ElementsOperand};
pub use errors::{
    BareFailureKindOperand, BareFailureKindsOperand, BareFailureOperand, BareFailuresOperand,
    DefiniteBareFailureKindOperand, DefiniteBareFailureOperand, DefiniteFailureKindOperand,
    DefiniteFailureOperand, FailureKindOperand, FailureKindsOperand, FailureOperand,
    FailuresOperand,
};
use graphrecords_core::GraphRecord;
use graphrecords_utils::aliases::GrHashSet;
pub use group::{
    Bucket, BucketChange, BucketOwned, GroupOperand, InvalidPartitionBucketArity, KeyFailure,
    KeyFailureChange, KeyFailureOwned, Partition, PartitionArity, PartitionBucketParts,
    PartitionBuilder, PartitionClassification, PartitionKeyFailureParts, PartitionOwned,
    PartitionOwnedParts, PartitionParts, PartitionShape, ReturnBucket, ReturnKeyFailure,
    ReturnPartition,
};
pub use indices::{
    BareIndexOperand, BareIndicesOperand, DefiniteBareIndexOperand, DefiniteIndexOperand,
    IndexOperand, IndicesOperand,
};
pub use nodes::{AllNodes, DefiniteNodeOperand, NodeOperand, NodesOperand};
pub use references::{
    BareReferenceOperand, BareReferencesOperand, DefiniteBareReferenceOperand,
    DefiniteReferenceIndexOperand, DefiniteReferenceOperand, ReferenceIndexOperand,
    ReferenceIndicesOperand, ReferenceOperand, ReferencesOperand,
};
use std::sync::Arc;
pub use values::{
    BareValueOperand, BareValuesOperand, DefiniteBareValueOperand, DefiniteValueOperand,
    ValueOperand, ValuesOperand,
};

pub trait EvaluateOperand {
    type ReturnValue<'a>: 'a
    where
        Self: 'a;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::ReturnValue<'a>>;
}

pub trait Operand: 'static + Sized + Clone + EvaluateOperand {
    fn context(&self) -> &dyn OperandContext<Self>;

    fn as_plan_node(&self) -> &dyn PlanNode;

    fn from_context(context: Arc<dyn OperandContext<Self>>) -> Self;

    #[must_use]
    fn new<C: OperandContext<Self>>(context: C) -> Self {
        Self::from_context(Arc::new(context))
    }

    fn explain(&self) -> Explanation<'_> {
        Explanation::new(self)
    }
}

pub struct OperandHandle<S: ElementShape, C: Arity> {
    context: Arc<dyn OperandContext<Self>>,
}

impl<S: ElementShape, C: Arity> Clone for OperandHandle<S, C> {
    fn clone(&self) -> Self {
        Self {
            context: Arc::clone(&self.context),
        }
    }
}

impl<S: ElementShape, C: Arity> EvaluateOperand for OperandHandle<S, C> {
    type ReturnValue<'a> = Return<'a, S, C>;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::ReturnValue<'a>> {
        self.context.evaluate(graphrecord, cache)
    }
}

impl<S: ElementShape, C: Arity> Estimated for OperandHandle<S, C> {
    fn estimate(&self, stats: &Stats) -> Estimate {
        self.context().estimate(stats)
    }
}

impl<S: ElementShape, C: Arity> Operand for OperandHandle<S, C> {
    fn context(&self) -> &dyn OperandContext<Self> {
        self.context.as_ref()
    }

    fn as_plan_node(&self) -> &dyn PlanNode {
        self.context.as_ref()
    }

    fn from_context(context: Arc<dyn OperandContext<Self>>) -> Self {
        Self { context }
    }
}

pub struct CheckedIndexedLaneBuilder<'a, I: IndexDomain, V: ValueType> {
    seen: GrHashSet<I::Owned>,
    elements: Vec<(I::Index<'a>, QueryResult<V::Value<'a>>)>,
}

impl<'a, I: IndexDomain, V: ValueType> CheckedIndexedLaneBuilder<'a, I, V> {
    #[must_use]
    pub fn new() -> Self {
        Self {
            seen: GrHashSet::default(),
            elements: Vec::new(),
        }
    }

    pub fn push(
        &mut self,
        index: I::Index<'a>,
        outcome: QueryResult<V::Value<'a>>,
    ) -> QueryResult<()> {
        if !self.seen.insert(I::to_owned(&index)) {
            return Err(Failure::new_at::<I, _>(
                "indexed lane construction",
                DuplicateIndex::<I>::new(I::to_owned(&index)),
                &index,
            ));
        }

        self.elements.push((index, outcome));

        Ok(())
    }

    #[must_use]
    pub fn finish(self) -> BoxedIterator<'a, (I::Index<'a>, QueryResult<V::Value<'a>>)> {
        Box::new(self.elements.into_iter())
    }
}

impl<I: IndexDomain, V: ValueType> Default for CheckedIndexedLaneBuilder<'_, I, V> {
    fn default() -> Self {
        Self::new()
    }
}
