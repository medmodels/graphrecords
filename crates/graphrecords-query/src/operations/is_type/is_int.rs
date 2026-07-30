use crate::{
    AttributeName, Bare, Explain, IndexDomain, IndexValue, Indexed, Mask, Operand, QueryResult,
    Scalar,
    element::{Pipeline, Preserving},
    execution::EvaluationCache,
    operations::{Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    traits::IsInt,
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{GraphRecordAttribute, GraphRecordValue, NodeIndex},
};

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "IsInt")]
#[plan(optimizer_hints(empty = if_any))]
pub struct IsIntOperation;

impl Prepare for IsIntOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I: IndexDomain> ElementKernel<Indexed<I, Scalar>> for IsIntOperation {
    type Emission = Preserving;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, Scalar>, Self>> {
        Ok(Pipeline::unkeyed(|value: QueryResult<_>| {
            value.map(|value| matches!(value, GraphRecordValue::Int(_)))
        }))
    }
}

impl ElementKernel<Bare<Scalar>> for IsIntOperation {
    type Emission = Preserving;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<Scalar>, Self>> {
        Ok(Pipeline::new(|value: QueryResult<_>| {
            value.map(|value| matches!(value, GraphRecordValue::Int(_)))
        }))
    }
}

impl<I: IndexDomain> ElementKernel<Indexed<I, AttributeName>> for IsIntOperation {
    type Emission = Preserving;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, AttributeName>, Self>> {
        Ok(Pipeline::unkeyed(|attribute: QueryResult<_>| {
            attribute.map(|attribute| matches!(attribute, GraphRecordAttribute::Int(_)))
        }))
    }
}

impl ElementKernel<Bare<AttributeName>> for IsIntOperation {
    type Emission = Preserving;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<AttributeName>, Self>> {
        Ok(Pipeline::new(|attribute: QueryResult<_>| {
            attribute.map(|attribute| matches!(attribute, GraphRecordAttribute::Int(_)))
        }))
    }
}

impl<I: IndexDomain> ElementKernel<Indexed<I, IndexValue<GraphRecordValue>>> for IsIntOperation {
    type Emission = Preserving;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, IndexValue<GraphRecordValue>>, Self>> {
        Ok(Pipeline::unkeyed(|value: QueryResult<_>| {
            value.map(|value| matches!(value, GraphRecordValue::Int(_)))
        }))
    }
}

impl ElementKernel<Bare<IndexValue<GraphRecordValue>>> for IsIntOperation {
    type Emission = Preserving;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<IndexValue<GraphRecordValue>>, Self>> {
        Ok(Pipeline::new(|value: QueryResult<_>| {
            value.map(|value| matches!(value, GraphRecordValue::Int(_)))
        }))
    }
}

impl<I: IndexDomain> ElementKernel<Indexed<I, IndexValue<NodeIndex>>> for IsIntOperation {
    type Emission = Preserving;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, IndexValue<NodeIndex>>, Self>> {
        Ok(Pipeline::unkeyed(|value: QueryResult<_>| {
            value.map(|value| matches!(value, GraphRecordAttribute::Int(_)))
        }))
    }
}

impl ElementKernel<Bare<IndexValue<NodeIndex>>> for IsIntOperation {
    type Emission = Preserving;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<IndexValue<NodeIndex>>, Self>> {
        Ok(Pipeline::new(|value: QueryResult<_>| {
            value.map(|value| matches!(value, GraphRecordAttribute::Int(_)))
        }))
    }
}

impl<I: IndexDomain> ElementKernel<Indexed<I, IndexValue<AttributeName>>> for IsIntOperation {
    type Emission = Preserving;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, IndexValue<AttributeName>>, Self>> {
        Ok(Pipeline::unkeyed(|value: QueryResult<_>| {
            value.map(|value| matches!(value, GraphRecordAttribute::Int(_)))
        }))
    }
}

impl ElementKernel<Bare<IndexValue<AttributeName>>> for IsIntOperation {
    type Emission = Preserving;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<IndexValue<AttributeName>>, Self>> {
        Ok(Pipeline::new(|value: QueryResult<_>| {
            value.map(|value| matches!(value, GraphRecordAttribute::Int(_)))
        }))
    }
}

impl<O: Apply<IsIntOperation>> IsInt for O {
    type ReturnOperand = O::Output;

    fn is_int(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), IsIntOperation))
    }
}
