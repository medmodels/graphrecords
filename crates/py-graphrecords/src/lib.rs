#![recursion_limit = "256"]

use graphrecords_core::graphrecord::overview::DEFAULT_TRUNCATE_DETAILS;
use graphrecords_python::prelude::*;
use pyo3::prelude::*;

#[pymodule(gil_used = false)]
fn _graphrecords(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyGraphRecord>()?;

    m.add_class::<PyString>()?;
    m.add_class::<PyInt>()?;
    m.add_class::<PyFloat>()?;
    m.add_class::<PyBool>()?;
    m.add_class::<PyDateTime>()?;
    m.add_class::<PyDuration>()?;
    m.add_class::<PyNull>()?;
    m.add_class::<graphrecords_python::prelude::PyAny>()?;
    m.add_class::<PyUnion>()?;
    m.add_class::<PyOption>()?;

    m.add_class::<PyAttributeDataType>()?;
    m.add_class::<PyAttributeType>()?;
    m.add_class::<PyGroupSchema>()?;
    m.add_class::<PySchemaType>()?;
    m.add_class::<PySchema>()?;

    m.add_class::<PyEdgeDirection>()?;

    m.add_class::<PyMatchMode>()?;

    m.add_class::<NodeOperandGroupDiscriminator>()?;
    m.add_class::<PyNodeOperand>()?;
    m.add_class::<PyNodeGroupOperand>()?;
    m.add_class::<PyNodeIndicesOperand>()?;
    m.add_class::<PyNodeIndicesGroupOperand>()?;
    m.add_class::<PyNodeIndexOperand>()?;
    m.add_class::<PyNodeIndexGroupOperand>()?;

    m.add_class::<EdgeOperandGroupDiscriminator>()?;
    m.add_class::<PyEdgeOperand>()?;
    m.add_class::<PyEdgeGroupOperand>()?;
    m.add_class::<PyEdgeIndicesOperand>()?;
    m.add_class::<PyEdgeIndicesGroupOperand>()?;
    m.add_class::<PyEdgeIndexOperand>()?;
    m.add_class::<PyEdgeIndexGroupOperand>()?;

    m.add_class::<PyNodeMultipleValuesWithIndexOperand>()?;
    m.add_class::<PyNodeMultipleValuesWithIndexGroupOperand>()?;
    m.add_class::<PyNodeMultipleValuesWithoutIndexOperand>()?;
    m.add_class::<PyEdgeMultipleValuesWithIndexOperand>()?;
    m.add_class::<PyEdgeMultipleValuesWithIndexGroupOperand>()?;
    m.add_class::<PyEdgeMultipleValuesWithoutIndexOperand>()?;
    m.add_class::<PyNodeSingleValueWithIndexOperand>()?;
    m.add_class::<PyNodeSingleValueWithIndexGroupOperand>()?;
    m.add_class::<PyNodeSingleValueWithoutIndexOperand>()?;
    m.add_class::<PyNodeSingleValueWithoutIndexGroupOperand>()?;
    m.add_class::<PyEdgeSingleValueWithIndexOperand>()?;
    m.add_class::<PyEdgeSingleValueWithIndexGroupOperand>()?;
    m.add_class::<PyEdgeSingleValueWithoutIndexOperand>()?;
    m.add_class::<PyEdgeSingleValueWithoutIndexGroupOperand>()?;

    m.add_class::<PyNodeAttributesTreeOperand>()?;
    m.add_class::<PyNodeAttributesTreeGroupOperand>()?;
    m.add_class::<PyEdgeAttributesTreeOperand>()?;
    m.add_class::<PyEdgeAttributesTreeGroupOperand>()?;
    m.add_class::<PyNodeMultipleAttributesWithIndexOperand>()?;
    m.add_class::<PyNodeMultipleAttributesWithIndexGroupOperand>()?;
    m.add_class::<PyNodeMultipleAttributesWithoutIndexOperand>()?;
    m.add_class::<PyEdgeMultipleAttributesWithIndexOperand>()?;
    m.add_class::<PyEdgeMultipleAttributesWithIndexGroupOperand>()?;
    m.add_class::<PyEdgeMultipleAttributesWithoutIndexOperand>()?;
    m.add_class::<PyNodeSingleAttributeWithIndexOperand>()?;
    m.add_class::<PyNodeSingleAttributeWithIndexGroupOperand>()?;
    m.add_class::<PyNodeSingleAttributeWithoutIndexOperand>()?;
    m.add_class::<PyNodeSingleAttributeWithoutIndexGroupOperand>()?;
    m.add_class::<PyEdgeSingleAttributeWithIndexOperand>()?;
    m.add_class::<PyEdgeSingleAttributeWithIndexGroupOperand>()?;
    m.add_class::<PyEdgeSingleAttributeWithoutIndexOperand>()?;
    m.add_class::<PyEdgeSingleAttributeWithoutIndexGroupOperand>()?;

    m.add("PY_DEFAULT_TRUNCATE_DETAILS", DEFAULT_TRUNCATE_DETAILS)?;
    m.add_class::<PyAttributeOverview>()?;
    m.add_class::<PyNodeGroupOverview>()?;
    m.add_class::<PyEdgeGroupOverview>()?;
    m.add_class::<PyGroupOverview>()?;
    m.add_class::<PyOverview>()?;

    m.add_class::<PyPreSetSchemaContext>()?;
    m.add_class::<PyPreAddNodeContext>()?;
    m.add_class::<PyPostAddNodeContext>()?;
    m.add_class::<PyPreAddNodeWithGroupContext>()?;
    m.add_class::<PyPostAddNodeWithGroupContext>()?;
    m.add_class::<PyPreRemoveNodeContext>()?;
    m.add_class::<PyPostRemoveNodeContext>()?;
    m.add_class::<PyPreAddNodesContext>()?;
    m.add_class::<PyPostAddNodesContext>()?;
    m.add_class::<PyPreAddNodesWithGroupContext>()?;
    m.add_class::<PyPostAddNodesWithGroupContext>()?;
    m.add_class::<PyPreAddNodesDataframesContext>()?;
    m.add_class::<PyPostAddNodesDataframesContext>()?;
    m.add_class::<PyPreAddNodesDataframesWithGroupContext>()?;
    m.add_class::<PyPostAddNodesDataframesWithGroupContext>()?;
    m.add_class::<PyPreAddEdgeContext>()?;
    m.add_class::<PyPostAddEdgeContext>()?;
    m.add_class::<PyPreAddEdgeWithGroupContext>()?;
    m.add_class::<PyPostAddEdgeWithGroupContext>()?;
    m.add_class::<PyPreRemoveEdgeContext>()?;
    m.add_class::<PyPostRemoveEdgeContext>()?;
    m.add_class::<PyPreAddEdgesContext>()?;
    m.add_class::<PyPostAddEdgesContext>()?;
    m.add_class::<PyPreAddEdgesWithGroupContext>()?;
    m.add_class::<PyPostAddEdgesWithGroupContext>()?;
    m.add_class::<PyPreAddEdgesDataframesContext>()?;
    m.add_class::<PyPostAddEdgesDataframesContext>()?;
    m.add_class::<PyPreAddEdgesDataframesWithGroupContext>()?;
    m.add_class::<PyPostAddEdgesDataframesWithGroupContext>()?;
    m.add_class::<PyPreAddGroupContext>()?;
    m.add_class::<PyPostAddGroupContext>()?;
    m.add_class::<PyPreRemoveGroupContext>()?;
    m.add_class::<PyPostRemoveGroupContext>()?;
    m.add_class::<PyPreAddNodeToGroupContext>()?;
    m.add_class::<PyPostAddNodeToGroupContext>()?;
    m.add_class::<PyPreAddEdgeToGroupContext>()?;
    m.add_class::<PyPostAddEdgeToGroupContext>()?;
    m.add_class::<PyPreRemoveNodeFromGroupContext>()?;
    m.add_class::<PyPostRemoveNodeFromGroupContext>()?;
    m.add_class::<PyPreRemoveEdgeFromGroupContext>()?;
    m.add_class::<PyPostRemoveEdgeFromGroupContext>()?;

    Ok(())
}
