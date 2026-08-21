use crate::{
    EntityRef, EntityReference, ExpandedChild, ExpandedIndex, Explain, IndexDomain, Indexed,
    QueryResult, Unit, Unordered,
    element::{Expanding, Pipeline},
    index::EntityAttributes,
    operations::{Build, ElementKernel, ElementPipeline, Operation, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    registry::operation_manifest,
    traits::Attributes,
};
use graphrecords_core::{GraphRecord, graphrecord::AttributeName};

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Element)]
#[explain(label = "Attributes")]
#[plan(optimizer_hints(empty = if_any))]
pub struct AttributesOperation;

impl<I: EntityAttributes> ElementKernel<Indexed<I, Unit>> for AttributesOperation {
    type Emission = Expanding<Unordered>;
    type OutShape = Indexed<ExpandedIndex<I, AttributeName>, AttributeName>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, Unit>, Self>> {
        Ok(Pipeline::keyed(move |element_address, ()| {
            let children = I::attribute_addresses(graphrecord)
                .filter(|&attribute_address| {
                    I::attribute(graphrecord, &element_address, attribute_address).is_some()
                })
                .map(|attribute_address| {
                    let attribute_name = I::attribute_name(graphrecord, attribute_address);

                    ExpandedChild::success(
                        AttributeName::from(attribute_name.clone()),
                        attribute_name,
                    )
                })
                .collect();

            Ok(children)
        }))
    }
}

impl<E: EntityAttributes, I: IndexDomain> ElementKernel<Indexed<I, EntityReference<E>>>
    for AttributesOperation
{
    type Emission = Expanding<Unordered>;
    type OutShape = Indexed<ExpandedIndex<I, AttributeName>, AttributeName>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, EntityReference<E>>, Self>> {
        Ok(Pipeline::unkeyed(move |entity: EntityRef<'a, E>| {
            let children = E::attribute_addresses(graphrecord)
                .filter(|&attribute_address| {
                    E::attribute(graphrecord, entity.address(), attribute_address).is_some()
                })
                .map(|attribute_address| {
                    let attribute_name = E::attribute_name(graphrecord, attribute_address);

                    ExpandedChild::success(
                        AttributeName::from(attribute_name.clone()),
                        attribute_name,
                    )
                })
                .collect();

            Ok(children)
        }))
    }
}

impl<E: Build<AttributesOperation>> Attributes for E {
    type Output = E::Output;

    fn attributes(&self) -> Self::Output {
        self.build(AttributesOperation)
    }
}

operation_manifest! {
    AttributesOperation {
        method: Attributes::attributes;
        scope: element;

        kernel {
            parameters: <I: EntityAttributes>;
            input: Indexed<I, Unit>;
            output: Indexed<ExpandedIndex<I, AttributeName>, AttributeName>;
            emission: Expanding<Unordered>;
        }

        kernel {
            parameters: <E: EntityAttributes, I: IndexDomain>;
            input: Indexed<I, EntityReference<E>>;
            output: Indexed<ExpandedIndex<I, AttributeName>, AttributeName>;
            emission: Expanding<Unordered>;
        }
    }
}
