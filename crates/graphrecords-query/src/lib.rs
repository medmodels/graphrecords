pub mod capabilities;
pub mod element;
pub mod error;
pub mod execution;
pub mod explain;
pub mod index;
pub mod operands;
pub mod operations;
pub mod optimizer;
pub mod prelude;
pub mod selection;
mod traits;
pub mod value;

pub use element::{
    Arity, Bare, BoxedIterator, Definite, ElementShape, Indexed, Multiple, OrderState, Ordered,
    Return, ReturnShape, Single, Unordered,
};
pub use error::{Diagnostic, ErrorGroup, External, Failure, FailureKind, QueryResult};
pub use explain::{Explain, Explanation, Labeled};
pub use index::{
    EntityDomain, ExpandedChild, ExpandedIndex, ExpandedIndexOwned, ExpandedIndexReference,
    IndexDomain, OwnedIndex, Position, Positional,
};
pub use operands::{
    BucketOwned, CheckedIndexedLaneBuilder, DefiniteEdgeOperand, DefiniteNodeOperand,
    DefiniteReferenceOperand, EdgeOperand, EdgesOperand, EvaluateContext, EvaluateOperand,
    GroupOperand, KeyFailureOwned, NodeOperand, NodesOperand, Operand, OperandContext,
    PartitionBucketParts, PartitionKeyFailureParts, PartitionOwned, PartitionOwnedParts,
    PartitionParts, ReferenceOperand, ReferencesOperand,
};
pub use operations::{EdgeDirection, MaybeAbsent, PreparedIndexedMultiple};
pub use selection::{QueryEdges, QueryNodes};
pub use traits::*;
pub use value::{
    AttributeName, EntityReference, FailureKindValue, FailureValue, IndexValue, Mask,
    ReturnValueType, Scalar, Unit, ValueType,
};

mod sealed {
    pub trait Sealed {}
}

#[cfg(test)]
mod tests {
    use crate::{Attribute, Filter, InGroup, QueryNodes};
    use graphrecords_core::GraphRecord;
    use std::collections::HashMap;

    #[test]
    fn test_query_nodes() {
        let mut graphrecord = GraphRecord::from_tuples(
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
            ],
            None,
            None,
        )
        .unwrap();

        graphrecord
            .add_node_to_group("lorem".into(), "0".into())
            .unwrap();
        graphrecord
            .add_node_to_group("lorem".into(), "1".into())
            .unwrap();
        graphrecord
            .add_node_to_group("ipsum".into(), "0".into())
            .unwrap();

        let selection = QueryNodes::query_nodes(&graphrecord, |node| {
            let nodes = node.filter(node.in_group("lorem".into()) & !node.in_group("ipsum".into()));

            nodes.attribute("amet".into())
        });

        let elements: Vec<_> = selection.evaluate().unwrap().collect();

        assert_eq!(elements.len(), 1);
        let (index, value) = &elements[0];
        assert_eq!(format!("{index}"), "\"1\"");
        assert_eq!(format!("{}", value.as_ref().unwrap()), "\"consectetur\"");
    }
}
