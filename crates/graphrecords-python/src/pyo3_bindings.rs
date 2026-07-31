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
        use crate::prelude::ArgumentAbsentError;
        #[pymodule_export]
        use crate::prelude::DivisionByZeroError;
        #[pymodule_export]
        use crate::prelude::DuplicateExpandedChildIndexError;
        #[pymodule_export]
        use crate::prelude::DuplicateIndexError;
        #[pymodule_export]
        use crate::prelude::EmptySplitDelimiterError;
        #[pymodule_export]
        use crate::prelude::EvaluationCacheGraphRecordMismatchError;
        #[pymodule_export]
        use crate::prelude::ExternalError;
        #[pymodule_export]
        use crate::prelude::GraphRecordError;
        #[pymodule_export]
        use crate::prelude::IncomparableIndicesError;
        #[pymodule_export]
        use crate::prelude::IncomparableValuesAtError;
        #[pymodule_export]
        use crate::prelude::IncomparableValuesError;
        #[pymodule_export]
        use crate::prelude::IntegerOverflowError;
        #[pymodule_export]
        use crate::prelude::InvalidCastError;
        #[pymodule_export]
        use crate::prelude::InvalidClipBoundsError;
        #[pymodule_export]
        use crate::prelude::InvalidMedianValueError;
        #[pymodule_export]
        use crate::prelude::InvalidPaddingCharacterError;
        #[pymodule_export]
        use crate::prelude::InvalidPartitionBucketArityError;
        #[pymodule_export]
        use crate::prelude::InvalidRegexPatternError;
        #[pymodule_export]
        use crate::prelude::InvalidStandardDeviationValueError;
        #[pymodule_export]
        use crate::prelude::InvalidStringSliceError;
        #[pymodule_export]
        use crate::prelude::InvalidTransitionError;
        #[pymodule_export]
        use crate::prelude::InvalidVarianceValueError;
        #[pymodule_export]
        use crate::prelude::MissingAttributeError;
        #[pymodule_export]
        use crate::prelude::MissingGroupAggregateError;
        #[pymodule_export]
        use crate::prelude::MissingTraversedAttributeError;
        #[pymodule_export]
        use crate::prelude::ModuloByZeroError;
        #[pymodule_export]
        use crate::prelude::NegativeLengthError;
        #[pymodule_export]
        use crate::prelude::NegativeSquareRootError;
        #[pymodule_export]
        use crate::prelude::NoChildIndexError;
        #[pymodule_export]
        use crate::prelude::NonIntegerValueError;
        #[pymodule_export]
        use crate::prelude::NonNumericValueError;
        #[pymodule_export]
        use crate::prelude::NonPositiveLogarithmError;
        #[pymodule_export]
        use crate::prelude::NonStringValueError;
        #[pymodule_export]
        use crate::prelude::PyArgument;
        #[pymodule_export]
        use crate::prelude::PyCastTarget;
        #[pymodule_export]
        use crate::prelude::PyEdgeDirection;
        #[pymodule_export]
        use crate::prelude::PyEdgeEndpointRole;
        #[pymodule_export]
        use crate::prelude::PyFailureKind;
        #[pymodule_export]
        use crate::prelude::PyOperand;
        #[pymodule_export]
        use crate::prelude::PyValueTarget;
        #[pymodule_export]
        use crate::prelude::QueryError;
        #[pymodule_export]
        use crate::prelude::StringLengthOverflowError;
        #[pymodule_export]
        use crate::prelude::StringPaddingOverflowError;
        #[pymodule_export]
        use crate::prelude::UnresolvedBucketFailuresError;
        #[pymodule_export]
        use crate::prelude::UnresolvedGroupKeyFailuresError;
        #[pymodule_export]
        use crate::prelude::UnsupportedValueRoleError;
    }

    #[pymodule]
    pub mod overview {
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
        use pyo3::prelude::*;

        #[pymodule_init]
        fn init(m: &Bound<'_, PyModule>) -> PyResult<()> {
            m.add(
                "PY_DEFAULT_TRUNCATE_DETAILS",
                graphrecords_overview::DEFAULT_TRUNCATE_DETAILS,
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
        use crate::prelude::PyPostAddEdgeToGroupsContext;
        #[pymodule_export]
        use crate::prelude::PyPostAddEdgeWithGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPostAddEdgeWithGroupsContext;
        #[pymodule_export]
        use crate::prelude::PyPostAddEdgesContext;
        #[pymodule_export]
        use crate::prelude::PyPostAddEdgesDataframesContext;
        #[pymodule_export]
        use crate::prelude::PyPostAddEdgesDataframesWithGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPostAddEdgesDataframesWithGroupsContext;
        #[pymodule_export]
        use crate::prelude::PyPostAddEdgesToGroupsContext;
        #[pymodule_export]
        use crate::prelude::PyPostAddEdgesWithGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPostAddEdgesWithGroupsContext;
        #[pymodule_export]
        use crate::prelude::PyPostAddGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPostAddNodeContext;
        #[pymodule_export]
        use crate::prelude::PyPostAddNodeToGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPostAddNodeToGroupsContext;
        #[pymodule_export]
        use crate::prelude::PyPostAddNodeWithGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPostAddNodeWithGroupsContext;
        #[pymodule_export]
        use crate::prelude::PyPostAddNodesContext;
        #[pymodule_export]
        use crate::prelude::PyPostAddNodesDataframesContext;
        #[pymodule_export]
        use crate::prelude::PyPostAddNodesDataframesWithGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPostAddNodesDataframesWithGroupsContext;
        #[pymodule_export]
        use crate::prelude::PyPostAddNodesToGroupsContext;
        #[pymodule_export]
        use crate::prelude::PyPostAddNodesWithGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPostAddNodesWithGroupsContext;
        #[pymodule_export]
        use crate::prelude::PyPostRemoveEdgeContext;
        #[pymodule_export]
        use crate::prelude::PyPostRemoveEdgeFromGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPostRemoveEdgeFromGroupsContext;
        #[pymodule_export]
        use crate::prelude::PyPostRemoveEdgesFromGroupsContext;
        #[pymodule_export]
        use crate::prelude::PyPostRemoveGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPostRemoveNodeContext;
        #[pymodule_export]
        use crate::prelude::PyPostRemoveNodeFromGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPostRemoveNodeFromGroupsContext;
        #[pymodule_export]
        use crate::prelude::PyPostRemoveNodesFromGroupsContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddEdgeContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddEdgeToGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddEdgeToGroupsContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddEdgeWithGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddEdgeWithGroupsContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddEdgesContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddEdgesDataframesContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddEdgesDataframesWithGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddEdgesDataframesWithGroupsContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddEdgesToGroupsContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddEdgesWithGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddEdgesWithGroupsContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddNodeContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddNodeToGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddNodeToGroupsContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddNodeWithGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddNodeWithGroupsContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddNodesContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddNodesDataframesContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddNodesDataframesWithGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddNodesDataframesWithGroupsContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddNodesToGroupsContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddNodesWithGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPreAddNodesWithGroupsContext;
        #[pymodule_export]
        use crate::prelude::PyPreRemoveEdgeContext;
        #[pymodule_export]
        use crate::prelude::PyPreRemoveEdgeFromGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPreRemoveEdgeFromGroupsContext;
        #[pymodule_export]
        use crate::prelude::PyPreRemoveEdgesFromGroupsContext;
        #[pymodule_export]
        use crate::prelude::PyPreRemoveGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPreRemoveNodeContext;
        #[pymodule_export]
        use crate::prelude::PyPreRemoveNodeFromGroupContext;
        #[pymodule_export]
        use crate::prelude::PyPreRemoveNodeFromGroupsContext;
        #[pymodule_export]
        use crate::prelude::PyPreRemoveNodesFromGroupsContext;
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
