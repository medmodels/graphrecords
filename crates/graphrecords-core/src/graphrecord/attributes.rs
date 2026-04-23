use crate::{
    GraphRecord,
    errors::{GraphRecordError, GraphRecordResult},
    graphrecord::intern_table::{NodeHandle, NodeIndexKind},
    graphrecord::lookup::{AsAttributeName, AsLookup},
    prelude::{Attributes, EdgeIndex, GraphRecordValue, Group, SchemaType},
};

pub struct NodeAttributesMut<'a> {
    node_handle: NodeHandle,
    graphrecord: &'a mut GraphRecord,
}

impl<'a> NodeAttributesMut<'a> {
    pub(crate) fn new(
        node: impl AsLookup<NodeIndexKind>,
        graphrecord: &'a mut GraphRecord,
    ) -> GraphRecordResult<Self> {
        let node_handle = node.resolve(graphrecord)?;

        if !graphrecord.graph.contains_node(node_handle) {
            let node_index = graphrecord.graph.node_index_table.resolve(node_handle);
            return Err(GraphRecordError::IndexError(format!(
                "Cannot find node with index {node_index}",
            )));
        }

        Ok(Self {
            node_handle,
            graphrecord,
        })
    }

    fn get_groups(&self) -> Vec<Group> {
        let node_handle = self.node_handle;
        self.graphrecord
            .group_mapping
            .groups_of_node(node_handle)
            .map(|group_handle| {
                self.graphrecord
                    .graph
                    .group_name_table
                    .resolve(group_handle)
                    .clone()
            })
            .collect()
    }

    fn current_attributes(&self) -> Attributes {
        self.graphrecord
            .graph
            .node_attributes(self.node_handle)
            .expect("node must exist.")
            .to_owned_attributes()
    }

    fn handle_schema(
        &mut self,
        attributes: &Attributes,
        groups: &[Group],
    ) -> GraphRecordResult<()> {
        let node_index = self
            .graphrecord
            .graph
            .node_index_table
            .resolve(self.node_handle)
            .clone();
        let schema = &mut self.graphrecord.schema;

        match schema.schema_type() {
            SchemaType::Inferred => {
                if groups.is_empty() {
                    schema.update_node(attributes, None, false);
                } else {
                    for group in groups {
                        schema.update_node(attributes, Some(group), false);
                    }
                }
            }
            SchemaType::Provided => {
                if groups.is_empty() {
                    schema.validate_node(&node_index, attributes, None)?;
                } else {
                    for group in groups {
                        schema.validate_node(&node_index, attributes, Some(group))?;
                    }
                }
            }
        }

        Ok(())
    }

    fn store_attributes(&mut self, attributes: Attributes) {
        self.graphrecord
            .graph
            .replace_node_attributes(self.node_handle, attributes)
            .expect("node must exist.");
    }

    pub fn replace_attributes(&mut self, attributes: Attributes) -> GraphRecordResult<()> {
        let groups = self.get_groups();
        self.handle_schema(&attributes, &groups)?;
        self.store_attributes(attributes);
        Ok(())
    }

    pub fn update_attribute(
        &mut self,
        attribute: impl AsAttributeName,
        value: GraphRecordValue,
    ) -> GraphRecordResult<()> {
        let attribute_name = attribute.as_attribute_name(self.graphrecord)?.clone();
        let groups = self.get_groups();

        let mut attributes = self.current_attributes();
        attributes
            .entry(attribute_name)
            .and_modify(|v| *v = value.clone())
            .or_insert(value);

        self.handle_schema(&attributes, &groups)?;
        self.store_attributes(attributes);
        Ok(())
    }

    pub fn remove_attribute(
        &mut self,
        attribute: impl AsAttributeName,
    ) -> GraphRecordResult<GraphRecordValue> {
        let attribute_name = attribute.as_attribute_name(self.graphrecord)?.clone();
        let groups = self.get_groups();

        let mut attributes = self.current_attributes();
        let removed_value = attributes.remove(&attribute_name);

        let Some(removed_value) = removed_value else {
            let node_index = self
                .graphrecord
                .graph
                .node_index_table
                .resolve(self.node_handle)
                .clone();
            return Err(GraphRecordError::KeyError(format!(
                "Attribute {attribute_name} does not exist on node {node_index}",
            )));
        };

        self.handle_schema(&attributes, &groups)?;
        self.store_attributes(attributes);
        Ok(removed_value)
    }
}

pub struct EdgeAttributesMut<'a> {
    edge_index: &'a EdgeIndex,
    graphrecord: &'a mut GraphRecord,
}

impl<'a> EdgeAttributesMut<'a> {
    pub(crate) fn new(
        edge_index: &'a EdgeIndex,
        graphrecord: &'a mut GraphRecord,
    ) -> GraphRecordResult<Self> {
        if !graphrecord.contains_edge(edge_index) {
            return Err(GraphRecordError::IndexError(format!(
                "Cannot find edge with index {edge_index}",
            )));
        }

        Ok(Self {
            edge_index,
            graphrecord,
        })
    }

    fn get_groups(&self) -> Vec<Group> {
        self.graphrecord
            .groups_of_edge(self.edge_index)
            .expect("edge must exist.")
            .cloned()
            .collect()
    }

    fn current_attributes(&self) -> Attributes {
        self.graphrecord
            .edge_attributes(self.edge_index)
            .expect("edge must exist.")
            .to_owned_attributes()
    }

    fn handle_schema(
        &mut self,
        attributes: &Attributes,
        groups: &[Group],
    ) -> GraphRecordResult<()> {
        let schema = &mut self.graphrecord.schema;

        match schema.schema_type() {
            SchemaType::Inferred => {
                if groups.is_empty() {
                    schema.update_edge(attributes, None, false);
                } else {
                    for group in groups {
                        schema.update_edge(attributes, Some(group), false);
                    }
                }
            }
            SchemaType::Provided => {
                if groups.is_empty() {
                    schema.validate_edge(self.edge_index, attributes, None)?;
                } else {
                    for group in groups {
                        schema.validate_edge(self.edge_index, attributes, Some(group))?;
                    }
                }
            }
        }

        Ok(())
    }

    fn store_attributes(&mut self, attributes: Attributes) {
        self.graphrecord
            .graph
            .replace_edge_attributes(self.edge_index, attributes)
            .expect("edge must exist.");
    }

    pub fn replace_attributes(&mut self, attributes: Attributes) -> GraphRecordResult<()> {
        let groups = self.get_groups();
        self.handle_schema(&attributes, &groups)?;
        self.store_attributes(attributes);
        Ok(())
    }

    pub fn update_attribute(
        &mut self,
        attribute: impl AsAttributeName,
        value: GraphRecordValue,
    ) -> GraphRecordResult<()> {
        let attribute_name = attribute.as_attribute_name(self.graphrecord)?.clone();
        let groups = self.get_groups();

        let mut attributes = self.current_attributes();
        attributes
            .entry(attribute_name)
            .and_modify(|v| *v = value.clone())
            .or_insert(value);

        self.handle_schema(&attributes, &groups)?;
        self.store_attributes(attributes);
        Ok(())
    }

    pub fn remove_attribute(
        &mut self,
        attribute: impl AsAttributeName,
    ) -> GraphRecordResult<GraphRecordValue> {
        let attribute_name = attribute.as_attribute_name(self.graphrecord)?.clone();
        let groups = self.get_groups();

        let mut attributes = self.current_attributes();
        let removed_value = attributes.remove(&attribute_name);

        let Some(removed_value) = removed_value else {
            return Err(GraphRecordError::KeyError(format!(
                "Attribute {} does not exist on edge {}",
                attribute_name, self.edge_index
            )));
        };

        self.handle_schema(&attributes, &groups)?;
        self.store_attributes(attributes);
        Ok(removed_value)
    }
}
