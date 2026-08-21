use super::{
    AttributeMap,
    datatypes::{AttributeName, NodeIndex, Value},
};

#[derive(Debug, Clone, Default)]
pub struct NodeBatch {
    elements: Vec<(NodeIndex, AttributeMap)>,
}

impl NodeBatch {
    #[must_use]
    pub const fn from_tuples(elements: Vec<(NodeIndex, AttributeMap)>) -> Self {
        Self { elements }
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.elements.len()
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&NodeIndex, &AttributeMap)> {
        self.elements
            .iter()
            .map(|(node_index, attributes)| (node_index, attributes))
    }

    pub fn attribute_values<'a>(
        &'a self,
        attribute_name: &'a AttributeName,
    ) -> impl Iterator<Item = (&'a NodeIndex, &'a Value)> {
        self.elements
            .iter()
            .filter_map(move |(node_index, attributes)| {
                attributes
                    .get(attribute_name)
                    .map(|value| (node_index, value))
            })
    }
}

impl From<Vec<(NodeIndex, AttributeMap)>> for NodeBatch {
    fn from(elements: Vec<(NodeIndex, AttributeMap)>) -> Self {
        Self::from_tuples(elements)
    }
}

impl FromIterator<(NodeIndex, AttributeMap)> for NodeBatch {
    fn from_iter<I: IntoIterator<Item = (NodeIndex, AttributeMap)>>(iterator: I) -> Self {
        Self::from_tuples(iterator.into_iter().collect())
    }
}

impl IntoIterator for NodeBatch {
    type IntoIter = std::vec::IntoIter<(NodeIndex, AttributeMap)>;
    type Item = (NodeIndex, AttributeMap);

    fn into_iter(self) -> Self::IntoIter {
        self.elements.into_iter()
    }
}

#[derive(Debug, Clone, Default)]
pub struct EdgeBatch {
    elements: Vec<(NodeIndex, NodeIndex, AttributeMap)>,
}

impl EdgeBatch {
    #[must_use]
    pub const fn from_tuples(elements: Vec<(NodeIndex, NodeIndex, AttributeMap)>) -> Self {
        Self { elements }
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.elements.len()
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&NodeIndex, &NodeIndex, &AttributeMap)> {
        self.elements
            .iter()
            .map(|(source_node_index, target_node_index, attributes)| {
                (source_node_index, target_node_index, attributes)
            })
    }

    pub fn attribute_values<'a>(
        &'a self,
        attribute_name: &'a AttributeName,
    ) -> impl Iterator<Item = (&'a NodeIndex, &'a NodeIndex, &'a Value)> {
        self.elements.iter().filter_map(
            move |(source_node_index, target_node_index, attributes)| {
                attributes
                    .get(attribute_name)
                    .map(|value| (source_node_index, target_node_index, value))
            },
        )
    }
}

impl From<Vec<(NodeIndex, NodeIndex, AttributeMap)>> for EdgeBatch {
    fn from(elements: Vec<(NodeIndex, NodeIndex, AttributeMap)>) -> Self {
        Self::from_tuples(elements)
    }
}

impl FromIterator<(NodeIndex, NodeIndex, AttributeMap)> for EdgeBatch {
    fn from_iter<I: IntoIterator<Item = (NodeIndex, NodeIndex, AttributeMap)>>(
        iterator: I,
    ) -> Self {
        Self::from_tuples(iterator.into_iter().collect())
    }
}

impl IntoIterator for EdgeBatch {
    type IntoIter = std::vec::IntoIter<(NodeIndex, NodeIndex, AttributeMap)>;
    type Item = (NodeIndex, NodeIndex, AttributeMap);

    fn into_iter(self) -> Self::IntoIter {
        self.elements.into_iter()
    }
}

#[cfg(test)]
mod test {
    use super::{EdgeBatch, NodeBatch};
    use crate::graphrecord::{
        AttributeMap,
        datatypes::{AttributeName, NodeIndex},
    };

    fn create_node_batch() -> NodeBatch {
        NodeBatch::from_tuples(vec![
            (
                "lorem".into(),
                AttributeMap::from([("sed".into(), 1.into())]),
            ),
            ("ipsum".into(), AttributeMap::new()),
            (
                "dolor".into(),
                AttributeMap::from([("sed".into(), 3.into())]),
            ),
        ])
    }

    fn create_edge_batch() -> EdgeBatch {
        EdgeBatch::from_tuples(vec![
            (
                "lorem".into(),
                "ipsum".into(),
                AttributeMap::from([("sed".into(), 1.into())]),
            ),
            ("ipsum".into(), "dolor".into(), AttributeMap::new()),
        ])
    }

    #[test]
    fn test_node_batch_from_tuples() {
        let batch = create_node_batch();

        assert_eq!(
            vec![
                (
                    "lorem".into(),
                    AttributeMap::from([("sed".into(), 1.into())]),
                ),
                ("ipsum".into(), AttributeMap::new()),
                (
                    "dolor".into(),
                    AttributeMap::from([("sed".into(), 3.into())]),
                ),
            ],
            batch.into_iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_node_batch_len() {
        assert_eq!(3, create_node_batch().len());
        assert_eq!(0, NodeBatch::default().len());
    }

    #[test]
    fn test_node_batch_is_empty() {
        assert!(!create_node_batch().is_empty());
        assert!(NodeBatch::default().is_empty());
    }

    #[test]
    fn test_node_batch_iter() {
        let batch = create_node_batch();

        let node_indices: Vec<_> = batch
            .iter()
            .map(|(node_index, _)| node_index.clone())
            .collect();

        let expected_node_indices: Vec<NodeIndex> =
            vec!["lorem".into(), "ipsum".into(), "dolor".into()];

        assert_eq!(expected_node_indices, node_indices);
    }

    #[test]
    fn test_node_batch_attribute_values() {
        let batch = create_node_batch();

        let values: Vec<_> = batch
            .attribute_values(&AttributeName::from("sed"))
            .map(|(node_index, value)| (node_index.clone(), value.clone()))
            .collect();

        assert_eq!(
            vec![("lorem".into(), 1.into()), ("dolor".into(), 3.into()),],
            values
        );
    }

    #[test]
    fn test_node_batch_from() {
        let batch = NodeBatch::from(vec![("lorem".into(), AttributeMap::new())]);

        assert_eq!(
            vec![("lorem".into(), AttributeMap::new())],
            batch.into_iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_node_batch_from_iter() {
        let batch: NodeBatch = vec![("lorem".into(), AttributeMap::new())]
            .into_iter()
            .collect();

        assert_eq!(
            vec![("lorem".into(), AttributeMap::new())],
            batch.into_iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_edge_batch_from_tuples() {
        let batch = create_edge_batch();

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
    }

    #[test]
    fn test_edge_batch_len() {
        assert_eq!(2, create_edge_batch().len());
        assert_eq!(0, EdgeBatch::default().len());
    }

    #[test]
    fn test_edge_batch_is_empty() {
        assert!(!create_edge_batch().is_empty());
        assert!(EdgeBatch::default().is_empty());
    }

    #[test]
    fn test_edge_batch_iter() {
        let batch = create_edge_batch();

        let endpoints: Vec<_> = batch
            .iter()
            .map(|(source_node_index, target_node_index, _)| {
                (source_node_index.clone(), target_node_index.clone())
            })
            .collect();

        let expected_endpoints: Vec<_> = vec![
            ("lorem".into(), "ipsum".into()),
            ("ipsum".into(), "dolor".into()),
        ];

        assert_eq!(expected_endpoints, endpoints);
    }

    #[test]
    fn test_edge_batch_attribute_values() {
        let batch = create_edge_batch();

        let values: Vec<_> = batch
            .attribute_values(&AttributeName::from("sed"))
            .map(|(source_node_index, target_node_index, value)| {
                (
                    source_node_index.clone(),
                    target_node_index.clone(),
                    value.clone(),
                )
            })
            .collect();

        assert_eq!(vec![("lorem".into(), "ipsum".into(), 1.into())], values);
    }

    #[test]
    fn test_edge_batch_from() {
        let batch = EdgeBatch::from(vec![("lorem".into(), "ipsum".into(), AttributeMap::new())]);

        assert_eq!(
            vec![("lorem".into(), "ipsum".into(), AttributeMap::new())],
            batch.into_iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_edge_batch_from_iter() {
        let batch: EdgeBatch = vec![("lorem".into(), "ipsum".into(), AttributeMap::new())]
            .into_iter()
            .collect();

        assert_eq!(
            vec![("lorem".into(), "ipsum".into(), AttributeMap::new())],
            batch.into_iter().collect::<Vec<_>>()
        );
    }
}
