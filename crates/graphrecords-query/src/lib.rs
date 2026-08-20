pub mod capabilities;
pub mod cast;
#[cfg(feature = "dynamic")]
pub mod dynamic;
pub mod element;
pub mod error;
pub mod execution;
pub mod explain;
pub mod expressions;
pub mod index;
pub mod operations;
pub mod optimizer;
pub mod prelude;
pub mod registry;
pub mod selection;
pub mod series;
pub mod traits;
pub mod value;

pub use element::{
    Arity, Bare, BoxedIterator, Definite, ElementShape, Indexed, Multiple, OrderState, Ordered,
    Return, ReturnShape, Single, Unordered,
};
pub use error::{Diagnostic, ErrorGroup, External, Failure, FailureKind, QueryResult};
pub use explain::{Explain, ExplainFormatter, Explanation, Labeled};
pub use expressions::{
    Bucket, CheckedIndexedLaneBuilder, DefiniteEdgeExpression, DefiniteNodeExpression,
    DefiniteReferenceExpression, EdgeExpression, EdgesExpression, EvaluateContext,
    EvaluateExpression, Expression, ExpressionContext, GroupedExpression, GroupsExpression,
    KeyFailure, NodeExpression, NodesExpression, OwnedBucket, OwnedKeyFailure, OwnedPartition,
    OwnedPartitionParts, Partition, PartitionBucketParts, PartitionKeyFailureParts, PartitionParts,
    ReferenceExpression, ReferencesExpression, ReturnBucket, ReturnKeyFailure, ReturnPartition,
    ReturnPartitionParts,
};
pub use index::{
    EdgeEndpointRole, EntityDomain, ExpandedChild, ExpandedIndex, ExpandedIndexAddress,
    ExpandedIndexOwned, ExpandedIndexReference, IndexDomain, OwnedIndex, Position, Positional,
};
pub use operations::{EdgeDirection, OnMissing, PreparedIndexedMultiple, PreparedSeriesArgument};
pub use selection::ReturnExpression;
pub use series::{EdgesSeries, GroupsSeries, NodesSeries, Queryable, Series};
pub use traits::*;
pub use value::{
    BareValueDomain, EntityRef, EntityReference, FailureKindValue, FailureValue, IndexValue, Mask,
    ReturnValueDomain, Scalar, Unit, ValueDomain,
};

mod sealed {
    pub trait Sealed {}
}

#[cfg(test)]
mod test {
    use crate::{
        And, Attribute, EdgeDirection, Expression, Filter, GroupsExpression, HasAttribute, InGroup,
        Inherit, NodesExpression, Not, OnError, Queryable, ReturnExpression, Uppercase, ViaEdges,
        error::argument::{Absent, ArgumentMissing},
        expressions::{AllGroups, AllNodes},
        operations::policy::Drop,
    };
    use graphrecords_core::{
        GraphRecord,
        errors::GraphRecordError,
        graphrecord::{
            AttributeMap, Group, NodeIndex, Value,
            batch::{EdgeBatch, NodeBatch},
        },
    };
    use std::collections::{HashMap, HashSet};

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

        GraphRecord::new()
            .add_nodes(NodeBatch::from_tuples(nodes))
            .unwrap()
            .add_edges(EdgeBatch::from_tuples(edges))
            .unwrap()
            .add_group("lorem".into())
            .unwrap()
            .add_nodes_to_group("lorem".into(), vec!["0".into(), "1".into()])
            .unwrap()
            .add_group("ipsum".into())
            .unwrap()
            .add_nodes_to_group("ipsum".into(), vec!["0".into()])
            .unwrap()
    }

    fn create_other_nodes() -> Vec<(NodeIndex, AttributeMap)> {
        vec![
            (
                "0".into(),
                HashMap::from([("lorem".into(), "ipsum".into())]),
            ),
            ("1".into(), HashMap::from([("dolor".into(), "sit".into())])),
        ]
    }

    fn create_other_graphrecord() -> GraphRecord {
        let nodes = create_other_nodes();

        GraphRecord::new()
            .add_nodes(NodeBatch::from_tuples(nodes))
            .unwrap()
    }

    #[test]
    fn test_expression() {
        let graphrecord = create_graphrecord();
        let nodes = graphrecord.nodes();

        assert_eq!("AllNodes", nodes.expression().compact_plan());
        assert_eq!(
            "AllNodes → InGroup group=\"lorem\"",
            nodes.in_group("lorem".into()).expression().compact_plan()
        );
    }

    #[test]
    fn test_bind() {
        let graphrecord = create_graphrecord();
        let nodes = graphrecord.nodes();

        let derived = nodes.bind(
            nodes
                .expression()
                .clone()
                .attribute("lorem".into())
                .on_error(Drop),
        );
        let elements: Vec<_> = derived.evaluate().unwrap().collect();

        assert_eq!(
            "AllNodes → Attribute attribute=\"lorem\" → Drop",
            derived.expression().compact_plan()
        );
        assert_eq!(1, elements.len());

        let (node_index, value) = elements.into_iter().next().unwrap();

        assert_eq!(NodeIndex::from("0"), NodeIndex::from(node_index));
        assert_eq!(Value::from("ipsum"), Value::from(value.unwrap()));
    }

    #[test]
    fn test_clone() {
        let graphrecord = create_graphrecord();
        let nodes = graphrecord.nodes();

        assert_eq!(4, nodes.evaluate().unwrap().count());
        assert_eq!("Series [AllNodes] optimized", format!("{nodes:?}"));

        let cloned = nodes.clone();

        assert_eq!("Series [AllNodes]", format!("{cloned:?}"));
        assert_eq!("Series [AllNodes] optimized", format!("{nodes:?}"));
        assert_eq!(4, cloned.evaluate().unwrap().count());
        assert_eq!("Series [AllNodes] optimized", format!("{cloned:?}"));
    }

    #[test]
    fn test_evaluate() {
        let graphrecord = create_graphrecord();
        let nodes = graphrecord.nodes();

        let indices: HashSet<_> = nodes
            .evaluate()
            .unwrap()
            .map(|element| element.unwrap())
            .collect();

        assert_eq!(
            HashSet::from([
                NodeIndex::from("0"),
                NodeIndex::from("1"),
                NodeIndex::from("2"),
                NodeIndex::from("3")
            ]),
            indices
        );

        let lorem = nodes.attribute("lorem".into()).on_error(Drop);
        let elements: Vec<_> = lorem.evaluate().unwrap().collect();

        assert_eq!(1, elements.len());

        let (node_index, value) = elements.into_iter().next().unwrap();

        assert_eq!(NodeIndex::from("0"), NodeIndex::from(node_index));
        assert_eq!(Value::from("ipsum"), Value::from(value.unwrap()));
    }

    #[test]
    fn test_explain() {
        let graphrecord = create_graphrecord();
        let nodes = graphrecord.nodes();

        let doubly_negated = nodes.in_group("lorem".into()).not().not();

        assert_eq!(
            "InGroup group=\"lorem\"
└─ AllNodes

optimization:
Source: skipped (no rules)
Simplify: converged (2 iterations)
Reorder: skipped (no rules)
Pushdown: skipped (no rules)
CommonSubexpressionElimination: skipped (no rules)
Limit: converged (1 iterations)
Graph: skipped (no rules)",
            doubly_negated.explain().to_string()
        );
    }

    #[test]
    fn test_explain_unoptimized() {
        let graphrecord = create_graphrecord();
        let nodes = graphrecord.nodes();

        let doubly_negated = nodes.in_group("lorem".into()).not().not();

        assert_eq!(
            "Not
└─ Not
   └─ InGroup group=\"lorem\"
      └─ AllNodes",
            doubly_negated.explain_unoptimized().to_string()
        );
    }

    #[test]
    fn test_debug() {
        let graphrecord = create_graphrecord();
        let nodes = graphrecord.nodes();

        assert_eq!("Series [AllNodes]", format!("{nodes:?}"));
        assert_eq!(
            "Series [AllNodes → Filter mask=(AllNodes → InGroup group=\"lorem\")]",
            format!("{:?}", nodes.filter(nodes.in_group("lorem".into())))
        );
        assert_eq!(
            "Series [AllNodes → Attribute attribute=\"adipiscing\" → Drop → Upperca…]",
            format!(
                "{:?}",
                nodes
                    .attribute("adipiscing".into())
                    .on_error(Drop)
                    .uppercase()
            )
        );
    }

    #[test]
    fn test_nodes_attribute() {
        let graphrecord = create_graphrecord();
        let nodes = graphrecord.nodes();

        let filtered = nodes.filter(nodes.has_attribute("lorem".into()));
        let lorem = filtered.attribute("lorem".into());
        let elements: Vec<_> = lorem.evaluate().unwrap().collect();

        assert_eq!(1, elements.len());

        let (node_index, value) = elements.into_iter().next().unwrap();

        assert_eq!(NodeIndex::from("0"), NodeIndex::from(node_index));
        assert_eq!(Value::from("ipsum"), Value::from(value.unwrap()));
    }

    #[test]
    fn test_nodes_in_group() {
        let graphrecord = create_graphrecord();
        let nodes = graphrecord.nodes();

        let excludes_ipsum = nodes.in_group("ipsum".into()).not();
        let mask = nodes.in_group("lorem".into()).and(excludes_ipsum);
        let filtered = nodes.filter(mask);
        let amet = filtered.attribute("amet".into());
        let elements: Vec<_> = amet.evaluate().unwrap().collect();

        assert_eq!(1, elements.len());

        let (node_index, value) = elements.into_iter().next().unwrap();

        assert_eq!(NodeIndex::from("1"), NodeIndex::from(node_index));
        assert_eq!(Value::from("consectetur"), Value::from(value.unwrap()));
    }

    #[test]
    fn test_invalid_nodes_in_group() {
        let graphrecord = create_graphrecord();
        let nodes = graphrecord.nodes();

        let filtered = nodes.filter(nodes.in_group("consectetur".into()));

        assert!(filtered.evaluate().is_err_and(|failure| matches!(
            failure.downcast_cause::<GraphRecordError>(),
            Some(GraphRecordError::GroupNotFound { .. })
        )));
    }

    #[test]
    fn test_edges_attribute() {
        let graphrecord = create_graphrecord();
        let edges = graphrecord.edges();
        let expected_edge_index = graphrecord.edge_indices().nth(2).unwrap();

        let filtered = edges.filter(edges.has_attribute("incididunt".into()));
        let incididunt = filtered.attribute("incididunt".into());
        let elements: Vec<_> = incididunt.evaluate().unwrap().collect();

        assert_eq!(1, elements.len());

        let (edge_index, value) = elements.into_iter().next().unwrap();

        assert_eq!(expected_edge_index, edge_index);
        assert_eq!(Value::from("ut"), Value::from(value.unwrap()));
    }

    #[test]
    fn test_nodes_filter_across_graphrecords() {
        let graphrecord = create_graphrecord();
        let other = create_other_graphrecord();

        let selected = graphrecord
            .nodes()
            .filter(other.nodes().has_attribute("lorem".into()));
        let elements: Vec<_> = selected.evaluate().unwrap().collect();
        let covered: Vec<_> = elements
            .iter()
            .filter_map(|outcome| outcome.as_ref().ok())
            .collect();
        let uncovered: Vec<_> = elements
            .iter()
            .filter_map(|outcome| outcome.as_ref().err())
            .collect();

        assert_eq!(3, elements.len());
        assert_eq!(vec![&NodeIndex::from("0")], covered);
        assert_eq!(2, uncovered.len());
        assert!(
            uncovered
                .iter()
                .all(|failure| failure.is_kind::<ArgumentMissing>())
        );
        assert!(
            uncovered.iter().all(|failure| matches!(
                failure.downcast_cause::<Absent>(),
                Some(Absent::Uncovered)
            ))
        );

        let keys: HashSet<_> = uncovered
            .iter()
            .filter_map(|failure| failure.downcast_element::<NodeIndex>().cloned())
            .collect();

        assert_eq!(
            HashSet::from([NodeIndex::from("2"), NodeIndex::from("3")]),
            keys
        );
    }

    #[test]
    fn test_inherit_across_graphrecords() {
        let graphrecord = create_graphrecord();
        let other = create_other_graphrecord();

        let carried = graphrecord
            .nodes()
            .via_edges(EdgeDirection::Outgoing)
            .inherit(other.nodes().attribute("dolor".into()).on_error(Drop));
        let elements: Vec<_> = carried.evaluate().unwrap().collect();
        let carried_values: Vec<_> = elements
            .iter()
            .filter_map(|element| element.1.as_ref().ok())
            .map(|value| Value::from(value.clone()))
            .collect();
        let uncovered: Vec<_> = elements
            .iter()
            .filter_map(|element| element.1.as_ref().err())
            .collect();

        assert_eq!(4, elements.len());
        assert_eq!(vec![Value::from("sit"), Value::from("sit")], carried_values);
        assert_eq!(2, uncovered.len());
        assert!(
            uncovered
                .iter()
                .all(|failure| failure.is_kind::<ArgumentMissing>())
        );
        assert!(
            uncovered.iter().all(|failure| matches!(
                failure.downcast_cause::<Absent>(),
                Some(Absent::Uncovered)
            ))
        );
    }

    #[test]
    fn test_groups() {
        let graphrecord = create_graphrecord();

        let groups: HashSet<_> = graphrecord
            .groups()
            .evaluate()
            .unwrap()
            .map(|element| element.unwrap())
            .collect();

        assert_eq!(
            HashSet::from([Group::from("lorem"), Group::from("ipsum")]),
            groups
        );
    }

    #[test]
    fn test_query() {
        let graphrecord = create_graphrecord();

        let lorem = graphrecord.query(
            NodesExpression::new(AllNodes)
                .attribute("lorem".into())
                .on_error(Drop),
        );
        let elements: Vec<_> = lorem.evaluate().unwrap().collect();

        assert_eq!(1, elements.len());

        let (node_index, value) = elements.into_iter().next().unwrap();

        assert_eq!(NodeIndex::from("0"), NodeIndex::from(node_index));
        assert_eq!(Value::from("ipsum"), Value::from(value.unwrap()));

        let paired = graphrecord.query((
            NodesExpression::new(AllNodes),
            GroupsExpression::new(AllGroups),
        ));
        let (nodes, groups) = paired.evaluate().unwrap();
        let node_indices: HashSet<_> = nodes.map(|element| element.unwrap()).collect();
        let group_names: HashSet<_> = groups.map(|element| element.unwrap()).collect();

        assert_eq!(
            HashSet::from([
                NodeIndex::from("0"),
                NodeIndex::from("1"),
                NodeIndex::from("2"),
                NodeIndex::from("3")
            ]),
            node_indices
        );
        assert_eq!(
            HashSet::from([Group::from("lorem"), Group::from("ipsum")]),
            group_names
        );
    }
}
