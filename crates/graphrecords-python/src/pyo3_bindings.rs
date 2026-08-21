#[pyo3::pymodule(gil_used = false)]
pub mod _graphrecords {
    use pyo3::prelude::*;

    #[pymodule]
    pub mod graphrecord {
        #[pymodule_export]
        use crate::prelude::PyEdgeDirection;
        #[pymodule_export]
        use crate::prelude::PyEdgeIndex;
        #[pymodule_export]
        use crate::prelude::PyEdgeView;
        #[pymodule_export]
        use crate::prelude::PyGraphRecord;
        #[pymodule_export]
        use crate::prelude::PyGroupView;
        #[pymodule_export]
        use crate::prelude::PyNodeView;
        #[pymodule_export]
        use crate::prelude::PyOnConflict;
        #[pymodule_export]
        use crate::prelude::PyRecordBatch;
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
        use crate::prelude::ArgumentMissingError;
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
        use crate::prelude::MissingGroupBucketError;
        #[pymodule_export]
        use crate::prelude::MissingTraversedAttributeError;
        #[pymodule_export]
        use crate::prelude::ModuloByZeroError;
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
        use crate::prelude::PyEdgeEndpointRole;
        #[pymodule_export]
        use crate::prelude::PyExpression;
        #[pymodule_export]
        use crate::prelude::PyFailureKind;
        #[pymodule_export]
        use crate::prelude::PyGroupedResult;
        #[pymodule_export]
        use crate::prelude::PyResultView;
        #[pymodule_export]
        use crate::prelude::PySeries;
        #[pymodule_export]
        use crate::prelude::PyValueTarget;
        #[pymodule_export]
        use crate::prelude::QueryError;
        #[pymodule_export]
        use crate::prelude::RaisedFailuresError;
        #[pymodule_export]
        use crate::prelude::ResultConsumedError;
        #[pymodule_export]
        use crate::prelude::StringLengthOverflowError;
        #[pymodule_export]
        use crate::prelude::StringPaddingOverflowError;
        #[pymodule_export]
        use crate::prelude::UncoveredIndicesError;
        #[pymodule_export]
        use crate::prelude::UnresolvedBucketFailuresError;
        #[pymodule_export]
        use crate::prelude::UnresolvedGroupKeyFailuresError;
        #[pymodule_export]
        use crate::prelude::UnresolvedIndexError;
        #[pymodule_export]
        use crate::prelude::UnsupportedValueRoleError;
        #[pymodule_export]
        use crate::prelude::edges;
        #[pymodule_export]
        use crate::prelude::groups;
        #[pymodule_export]
        use crate::prelude::nodes;
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
        use crate::prelude::PyAddEdges;
        #[pymodule_export]
        use crate::prelude::PyAddEdgesInGroup;
        #[pymodule_export]
        use crate::prelude::PyAddEdgesToGroup;
        #[pymodule_export]
        use crate::prelude::PyAddGroup;
        #[pymodule_export]
        use crate::prelude::PyAddNodes;
        #[pymodule_export]
        use crate::prelude::PyAddNodesInGroup;
        #[pymodule_export]
        use crate::prelude::PyAddNodesToGroup;
        #[pymodule_export]
        use crate::prelude::PyClear;
        #[pymodule_export]
        use crate::prelude::PyEdgeBatch;
        #[pymodule_export]
        use crate::prelude::PyEdgeBatchIterator;
        #[pymodule_export]
        use crate::prelude::PyFreezeSchema;
        #[pymodule_export]
        use crate::prelude::PyNodeBatch;
        #[pymodule_export]
        use crate::prelude::PyNodeBatchIterator;
        #[pymodule_export]
        use crate::prelude::PyRemoveEdgeAttributes;
        #[pymodule_export]
        use crate::prelude::PyRemoveEdges;
        #[pymodule_export]
        use crate::prelude::PyRemoveEdgesFromGroup;
        #[pymodule_export]
        use crate::prelude::PyRemoveGroups;
        #[pymodule_export]
        use crate::prelude::PyRemoveNodeAttributes;
        #[pymodule_export]
        use crate::prelude::PyRemoveNodes;
        #[pymodule_export]
        use crate::prelude::PyRemoveNodesFromGroup;
        #[pymodule_export]
        use crate::prelude::PyReplaceEdgeAttributes;
        #[pymodule_export]
        use crate::prelude::PyReplaceNodeAttributes;
        #[pymodule_export]
        use crate::prelude::PySetEdgeAttributes;
        #[pymodule_export]
        use crate::prelude::PySetNodeAttributes;
        #[pymodule_export]
        use crate::prelude::PySetSchema;
        #[pymodule_export]
        use crate::prelude::PyUnfreezeSchema;
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
