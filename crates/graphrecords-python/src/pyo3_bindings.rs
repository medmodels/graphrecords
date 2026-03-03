#[pyo3::pymodule(gil_used = false)]
pub mod _graphrecords {
    use pyo3::prelude::*;

    #[pymodule]
    pub mod graphrecord {
        #[pymodule_export]
        use crate::prelude::PyGraphRecord;
    }

    #[pymodule]
    pub mod datatype {
        #[pymodule_export]
        use crate::prelude::PyAny;
        #[pymodule_export]
        use crate::prelude::PyBool;
        #[pymodule_export]
        use crate::prelude::PyDateTime;
        #[pymodule_export]
        use crate::prelude::PyDuration;
        #[pymodule_export]
        use crate::prelude::PyFloat;
        #[pymodule_export]
        use crate::prelude::PyInt;
        #[pymodule_export]
        use crate::prelude::PyNull;
        #[pymodule_export]
        use crate::prelude::PyOption;
        #[pymodule_export]
        use crate::prelude::PyString;
        #[pymodule_export]
        use crate::prelude::PyUnion;
    }

    #[pymodule]
    pub mod schema {
        #[pymodule_export]
        use crate::prelude::PyAttributeDataType;
        #[pymodule_export]
        use crate::prelude::PyAttributeType;
        #[pymodule_export]
        use crate::prelude::PyGroupSchema;
        #[pymodule_export]
        use crate::prelude::PySchema;
        #[pymodule_export]
        use crate::prelude::PySchemaType;
    }

    #[pymodule]
    pub mod querying {
        #[pymodule_export]
        use crate::prelude::EdgeOperandGroupDiscriminator;
        #[pymodule_export]
        use crate::prelude::NodeOperandGroupDiscriminator;
        #[pymodule_export]
        use crate::prelude::PyEdgeAttributesTreeGroupOperand;
        #[pymodule_export]
        use crate::prelude::PyEdgeAttributesTreeOperand;
        #[pymodule_export]
        use crate::prelude::PyEdgeDirection;
        #[pymodule_export]
        use crate::prelude::PyEdgeGroupOperand;
        #[pymodule_export]
        use crate::prelude::PyEdgeIndexGroupOperand;
        #[pymodule_export]
        use crate::prelude::PyEdgeIndexOperand;
        #[pymodule_export]
        use crate::prelude::PyEdgeIndicesGroupOperand;
        #[pymodule_export]
        use crate::prelude::PyEdgeIndicesOperand;
        #[pymodule_export]
        use crate::prelude::PyEdgeMultipleAttributesWithIndexGroupOperand;
        #[pymodule_export]
        use crate::prelude::PyEdgeMultipleAttributesWithIndexOperand;
        #[pymodule_export]
        use crate::prelude::PyEdgeMultipleAttributesWithoutIndexOperand;
        #[pymodule_export]
        use crate::prelude::PyEdgeMultipleValuesWithIndexGroupOperand;
        #[pymodule_export]
        use crate::prelude::PyEdgeMultipleValuesWithIndexOperand;
        #[pymodule_export]
        use crate::prelude::PyEdgeMultipleValuesWithoutIndexOperand;
        #[pymodule_export]
        use crate::prelude::PyEdgeOperand;
        #[pymodule_export]
        use crate::prelude::PyEdgeSingleAttributeWithIndexGroupOperand;
        #[pymodule_export]
        use crate::prelude::PyEdgeSingleAttributeWithIndexOperand;
        #[pymodule_export]
        use crate::prelude::PyEdgeSingleAttributeWithoutIndexGroupOperand;
        #[pymodule_export]
        use crate::prelude::PyEdgeSingleAttributeWithoutIndexOperand;
        #[pymodule_export]
        use crate::prelude::PyEdgeSingleValueWithIndexGroupOperand;
        #[pymodule_export]
        use crate::prelude::PyEdgeSingleValueWithIndexOperand;
        #[pymodule_export]
        use crate::prelude::PyEdgeSingleValueWithoutIndexGroupOperand;
        #[pymodule_export]
        use crate::prelude::PyEdgeSingleValueWithoutIndexOperand;
        #[pymodule_export]
        use crate::prelude::PyMatchMode;
        #[pymodule_export]
        use crate::prelude::PyNodeAttributesTreeGroupOperand;
        #[pymodule_export]
        use crate::prelude::PyNodeAttributesTreeOperand;
        #[pymodule_export]
        use crate::prelude::PyNodeGroupOperand;
        #[pymodule_export]
        use crate::prelude::PyNodeIndexGroupOperand;
        #[pymodule_export]
        use crate::prelude::PyNodeIndexOperand;
        #[pymodule_export]
        use crate::prelude::PyNodeIndicesGroupOperand;
        #[pymodule_export]
        use crate::prelude::PyNodeIndicesOperand;
        #[pymodule_export]
        use crate::prelude::PyNodeMultipleAttributesWithIndexGroupOperand;
        #[pymodule_export]
        use crate::prelude::PyNodeMultipleAttributesWithIndexOperand;
        #[pymodule_export]
        use crate::prelude::PyNodeMultipleAttributesWithoutIndexOperand;
        #[pymodule_export]
        use crate::prelude::PyNodeMultipleValuesWithIndexGroupOperand;
        #[pymodule_export]
        use crate::prelude::PyNodeMultipleValuesWithIndexOperand;
        #[pymodule_export]
        use crate::prelude::PyNodeMultipleValuesWithoutIndexOperand;
        #[pymodule_export]
        use crate::prelude::PyNodeOperand;
        #[pymodule_export]
        use crate::prelude::PyNodeSingleAttributeWithIndexGroupOperand;
        #[pymodule_export]
        use crate::prelude::PyNodeSingleAttributeWithIndexOperand;
        #[pymodule_export]
        use crate::prelude::PyNodeSingleAttributeWithoutIndexGroupOperand;
        #[pymodule_export]
        use crate::prelude::PyNodeSingleAttributeWithoutIndexOperand;
        #[pymodule_export]
        use crate::prelude::PyNodeSingleValueWithIndexGroupOperand;
        #[pymodule_export]
        use crate::prelude::PyNodeSingleValueWithIndexOperand;
        #[pymodule_export]
        use crate::prelude::PyNodeSingleValueWithoutIndexGroupOperand;
        #[pymodule_export]
        use crate::prelude::PyNodeSingleValueWithoutIndexOperand;
    }

    #[pymodule]
    pub mod overview {
        use pyo3::prelude::*;

        #[pymodule_export]
        use crate::prelude::PyAttributeOverview;
        #[pymodule_export]
        use crate::prelude::PyEdgeGroupOverview;
        #[pymodule_export]
        use crate::prelude::PyGroupOverview;
        #[pymodule_export]
        use crate::prelude::PyNodeGroupOverview;
        #[pymodule_export]
        use crate::prelude::PyOverview;

        #[pymodule_init]
        fn init(m: &Bound<'_, PyModule>) -> PyResult<()> {
            m.add(
                "PY_DEFAULT_TRUNCATE_DETAILS",
                graphrecords_core::graphrecord::overview::DEFAULT_TRUNCATE_DETAILS,
            )
        }
    }

    #[pymodule]
    pub mod plugins {
        #[pymodule_export]
        use crate::prelude::PyPostAddEdgeContext;
        #[pymodule_export]
        use crate::prelude::PyPostAddEdgeToGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPostAddEdgeWithGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPostAddEdgesContext;
        #[pymodule_export]
        use crate::prelude::PyPostAddEdgesDataframesContext;
        #[pymodule_export]
        use crate::prelude::PyPostAddEdgesDataframesWithGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPostAddEdgesWithGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPostAddGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPostAddNodeContext;
        #[pymodule_export]
        use crate::prelude::PyPostAddNodeToGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPostAddNodeWithGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPostAddNodesContext;
        #[pymodule_export]
        use crate::prelude::PyPostAddNodesDataframesContext;
        #[pymodule_export]
        use crate::prelude::PyPostAddNodesDataframesWithGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPostAddNodesWithGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPostRemoveEdgeContext;
        #[pymodule_export]
        use crate::prelude::PyPostRemoveEdgeFromGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPostRemoveGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPostRemoveNodeContext;
        #[pymodule_export]
        use crate::prelude::PyPostRemoveNodeFromGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddEdgeContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddEdgeToGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddEdgeWithGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddEdgesContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddEdgesDataframesContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddEdgesDataframesWithGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddEdgesWithGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddNodeContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddNodeToGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddNodeWithGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddNodesContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddNodesDataframesContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddNodesDataframesWithGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddNodesWithGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPreRemoveEdgeContext;
        #[pymodule_export]
        use crate::prelude::PyPreRemoveEdgeFromGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPreRemoveGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPreRemoveNodeContext;
        #[pymodule_export]
        use crate::prelude::PyPreRemoveNodeFromGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPreSetSchemaContext;
    }

    #[pymodule_init]
    fn init(module: &Bound<'_, PyModule>) -> PyResult<()> {
        let sys = module.py().import("sys")?;

        let sys_modules = sys.getattr("modules")?;

        let module_name: String = module.name()?.extract()?;

        for submodule_name in [
            "graphrecord",
            "datatype",
            "schema",
            "querying",
            "overview",
            "plugins",
        ] {
            let submodule = module.getattr(submodule_name)?;
            sys_modules.set_item(format!("{module_name}.{submodule_name}"), submodule)?;
        }

        Ok(())
    }
}
