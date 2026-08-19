pub mod capabilities;
pub mod cast;
#[cfg(feature = "dynamic")]
pub mod dynamic;
pub mod element;
pub mod error;
pub mod execution;
pub mod explain;
pub mod index;
pub mod operands;
pub mod operations;
pub mod optimizer;
pub mod prelude;
pub mod registry;
pub mod selection;
pub mod traits;
pub mod value;

pub use element::{
    Arity, Bare, BoxedIterator, Definite, ElementShape, Indexed, Multiple, OrderState, Ordered,
    Return, ReturnShape, Single, Unordered,
};
pub use error::{Diagnostic, ErrorGroup, External, Failure, FailureKind, QueryResult};
pub use explain::{Explain, ExplainFormatter, Explanation, Labeled};
pub use index::{
    EdgeEndpointRole, EntityDomain, ExpandedChild, ExpandedIndex, ExpandedIndexOwned,
    ExpandedIndexReference, GroupKey, IndexDomain, OwnedIndex, Position, Positional,
};
pub use operands::{
    Bucket, BucketOwned, CheckedIndexedLaneBuilder, DefiniteEdgeOperand, DefiniteNodeOperand,
    DefiniteReferenceOperand, EdgeOperand, EdgesOperand, EvaluateContext, EvaluateOperand,
    GroupOperand, KeyFailure, KeyFailureOwned, NodeOperand, NodesOperand, Operand, OperandContext,
    Partition, PartitionBucketParts, PartitionKeyFailureParts, PartitionOwned, PartitionOwnedParts,
    PartitionParts, ReferenceOperand, ReferencesOperand, ReturnBucket, ReturnKeyFailure,
    ReturnPartition, ReturnPartitionParts,
};
pub use operations::{EdgeDirection, MaybeAbsent, PreparedIndexedMultiple};
pub use selection::{QueryEdges, QueryNodes};
pub use traits::*;
pub use value::{
    BareValueDomain, EntityReference, FailureKindValue, FailureValue, IndexValue, Mask,
    ReturnValueDomain, Scalar, Unit, ValueDomain,
};

mod sealed {
    pub trait Sealed {}
}

#[cfg(test)]
mod test {
    use crate::{Attribute, Filter, HasAttribute, InGroup, QueryEdges, QueryNodes};
    use graphrecords_core::{
        GraphRecord,
        errors::GraphRecordError,
        graphrecord::{AttributeMap, NodeIndex, Value},
    };
    use std::collections::HashMap;

    fn create_nodes() -> Vec<(NodeIndex, AttributeMap)> {
        vec![
            (
                "0".into(),
                HashMap::from([("lorem".into(), "ipsum".into())]),
            ),
            (
                "1".into(),
                HashMap::from([("amet".into(), "consectetur".into())]),
            ),
            (
                "2".into(),
                HashMap::from([("adipiscing".into(), "elit".into())]),
            ),
            ("3".into(), HashMap::new()),
        ]
    }

    fn create_edges() -> Vec<(NodeIndex, NodeIndex, AttributeMap)> {
        vec![
            (
                "0".into(),
                "1".into(),
                HashMap::from([
                    ("sed".into(), "do".into()),
                    ("eiusmod".into(), "tempor".into()),
                ]),
            ),
            (
                "1".into(),
                "0".into(),
                HashMap::from([
                    ("sed".into(), "do".into()),
                    ("eiusmod".into(), "tempor".into()),
                ]),
            ),
            (
                "1".into(),
                "2".into(),
                HashMap::from([("incididunt".into(), "ut".into())]),
            ),
            ("0".into(), "2".into(), HashMap::new()),
        ]
    }

    fn create_graphrecord() -> GraphRecord {
        let nodes = create_nodes();
        let edges = create_edges();

        let mut graphrecord = GraphRecord::from_tuples(nodes, Some(edges), None).unwrap();

        graphrecord
            .add_group("lorem".into(), Some(vec!["0".into(), "1".into()]), None)
            .unwrap();
        graphrecord
            .add_group("ipsum".into(), Some(vec!["0".into()]), None)
            .unwrap();

        graphrecord
    }

    #[test]
    fn test_query_nodes_attribute() {
        let graphrecord = create_graphrecord();

        let selection = graphrecord.query_nodes(|node| {
            let nodes = node.filter(node.has_attribute("lorem".into()));

            nodes.attribute("lorem".into())
        });

        let elements: Vec<_> = selection.evaluate().unwrap().collect();

        assert_eq!(1, elements.len());
        assert_eq!(&NodeIndex::from("0"), elements[0].0);
        assert_eq!(&Value::from("ipsum"), elements[0].1.as_ref().unwrap());
    }

    #[test]
    fn test_query_nodes_in_group() {
        let graphrecord = create_graphrecord();

        let selection = graphrecord.query_nodes(|node| {
            let nodes = node.filter(node.in_group("lorem".into()) & !node.in_group("ipsum".into()));

            nodes.attribute("amet".into())
        });

        let elements: Vec<_> = selection.evaluate().unwrap().collect();

        assert_eq!(1, elements.len());
        assert_eq!(&NodeIndex::from("1"), elements[0].0);
        assert_eq!(&Value::from("consectetur"), elements[0].1.as_ref().unwrap());
    }

    #[test]
    fn test_invalid_query_nodes_in_group() {
        let graphrecord = create_graphrecord();

        let selection =
            graphrecord.query_nodes(|node| node.filter(node.in_group("consectetur".into())));

        // Querying the nodes in a non-existing group should fail
        assert!(selection.evaluate().is_err_and(|failure| matches!(
            failure.downcast_cause::<GraphRecordError>(),
            Some(GraphRecordError::GroupNotFound { .. })
        )));
    }

    #[test]
    fn test_query_edges_attribute() {
        let graphrecord = create_graphrecord();

        let selection = graphrecord.query_edges(|edge| {
            let edges = edge.filter(edge.has_attribute("incididunt".into()));

            edges.attribute("incididunt".into())
        });

        let elements: Vec<_> = selection.evaluate().unwrap().collect();

        assert_eq!(1, elements.len());
        assert_eq!(&2, elements[0].0);
        assert_eq!(&Value::from("ut"), elements[0].1.as_ref().unwrap());
    }
}
