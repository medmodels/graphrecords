use super::{
    batch::{EdgeBatch, NodeBatch},
    datatypes::{AttributeName, NodeIndex, Value, collect_attributes},
};
use crate::errors::GraphRecordResult;

pub trait NodeSource {
    fn collect_nodes(self) -> GraphRecordResult<NodeBatch>;
}

impl NodeSource for NodeBatch {
    fn collect_nodes(self) -> GraphRecordResult<NodeBatch> {
        Ok(self)
    }
}

impl<I, A, K, V> NodeSource for Vec<(I, A)>
where
    I: Into<NodeIndex>,
    A: IntoIterator<Item = (K, V)>,
    K: Into<AttributeName>,
    V: Into<Value>,
{
    fn collect_nodes(self) -> GraphRecordResult<NodeBatch> {
        Ok(self
            .into_iter()
            .map(|(node_index, attributes)| (node_index.into(), collect_attributes(attributes)))
            .collect())
    }
}

impl<I, A, K, V, const N: usize> NodeSource for [(I, A); N]
where
    I: Into<NodeIndex>,
    A: IntoIterator<Item = (K, V)>,
    K: Into<AttributeName>,
    V: Into<Value>,
{
    fn collect_nodes(self) -> GraphRecordResult<NodeBatch> {
        Ok(self
            .into_iter()
            .map(|(node_index, attributes)| (node_index.into(), collect_attributes(attributes)))
            .collect())
    }
}

pub trait EdgeSource {
    fn collect_edges(self) -> GraphRecordResult<EdgeBatch>;
}

impl EdgeSource for EdgeBatch {
    fn collect_edges(self) -> GraphRecordResult<EdgeBatch> {
        Ok(self)
    }
}

impl<I1, I2, A, K, V> EdgeSource for Vec<(I1, I2, A)>
where
    I1: Into<NodeIndex>,
    I2: Into<NodeIndex>,
    A: IntoIterator<Item = (K, V)>,
    K: Into<AttributeName>,
    V: Into<Value>,
{
    fn collect_edges(self) -> GraphRecordResult<EdgeBatch> {
        Ok(self
            .into_iter()
            .map(|(source_node_index, target_node_index, attributes)| {
                (
                    source_node_index.into(),
                    target_node_index.into(),
                    collect_attributes(attributes),
                )
            })
            .collect())
    }
}

impl<I1, I2, A, K, V, const N: usize> EdgeSource for [(I1, I2, A); N]
where
    I1: Into<NodeIndex>,
    I2: Into<NodeIndex>,
    A: IntoIterator<Item = (K, V)>,
    K: Into<AttributeName>,
    V: Into<Value>,
{
    fn collect_edges(self) -> GraphRecordResult<EdgeBatch> {
        Ok(self
            .into_iter()
            .map(|(source_node_index, target_node_index, attributes)| {
                (
                    source_node_index.into(),
                    target_node_index.into(),
                    collect_attributes(attributes),
                )
            })
            .collect())
    }
}

#[cfg(test)]
mod test {
    use super::{EdgeSource, NodeSource};
    use crate::graphrecord::{
        AttributeMap,
        datatypes::{NodeIndex, Value},
    };

    #[test]
    fn test_collect_nodes() {
        let batch = vec![
            ("lorem", AttributeMap::from([("sed".into(), 1.into())])),
            ("ipsum", AttributeMap::new()),
        ]
        .collect_nodes()
        .unwrap();

        assert_eq!(
            vec![
                (
                    "lorem".into(),
                    AttributeMap::from([("sed".into(), 1.into())]),
                ),
                ("ipsum".into(), AttributeMap::new()),
            ],
            batch.into_iter().collect::<Vec<_>>()
        );

        let batch = [(
            NodeIndex::from("dolor"),
            AttributeMap::from([("sed".into(), Value::from(3))]),
        )]
        .collect_nodes()
        .unwrap();

        assert_eq!(
            vec![(
                "dolor".into(),
                AttributeMap::from([("sed".into(), 3.into())]),
            )],
            batch.into_iter().collect::<Vec<_>>()
        );

        let batch = vec![("amet", [("sed", 2)])].collect_nodes().unwrap();

        assert_eq!(
            vec![(
                "amet".into(),
                AttributeMap::from([("sed".into(), 2.into())])
            )],
            batch.into_iter().collect::<Vec<_>>()
        );

        let batch = Vec::<(NodeIndex, AttributeMap)>::new()
            .collect_nodes()
            .unwrap();

        assert!(batch.is_empty());
    }

    #[test]
    fn test_collect_edges() {
        let batch = vec![
            (
                "lorem",
                "ipsum",
                AttributeMap::from([("sed".into(), 1.into())]),
            ),
            ("ipsum", "dolor", AttributeMap::new()),
        ]
        .collect_edges()
        .unwrap();

        assert_eq!(
            vec![
                (
                    "lorem".into(),
                    "ipsum".into(),
                    AttributeMap::from([("sed".into(), 1.into())]),
                ),
                ("ipsum".into(), "dolor".into(), AttributeMap::new()),
            ],
            batch.into_iter().collect::<Vec<_>>()
        );

        let batch = [("dolor", 2_i64, AttributeMap::new())]
            .collect_edges()
            .unwrap();

        assert_eq!(
            vec![("dolor".into(), 2_i64.into(), AttributeMap::new())],
            batch.into_iter().collect::<Vec<_>>()
        );

        let batch = vec![("amet", "dolor", [("sed", 2)])]
            .collect_edges()
            .unwrap();

        assert_eq!(
            vec![(
                "amet".into(),
                "dolor".into(),
                AttributeMap::from([("sed".into(), 2.into())]),
            )],
            batch.into_iter().collect::<Vec<_>>()
        );

        let batch = Vec::<(NodeIndex, NodeIndex, AttributeMap)>::new()
            .collect_edges()
            .unwrap();

        assert!(batch.is_empty());
    }
}
