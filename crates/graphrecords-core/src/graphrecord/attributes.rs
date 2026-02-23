use crate::{
    GraphRecord,
    errors::{GraphRecordError, GraphRecordResult},
    prelude::{
        Attributes, EdgeIndex, GraphRecordAttribute, GraphRecordValue, Group, NodeIndex, SchemaType,
    },
};

macro_rules! impl_attributes_mut {
    (
        $struct_name:ident,
        $index_type:ty,
        $index_field:ident,
        $entity:literal,
        $contains_fn:ident,
        $groups_of_fn:ident,
        $get_attributes_fn:ident,
        $get_attributes_mut_fn:ident,
        $schema_update_fn:ident,
        $schema_validate_fn:ident
    ) => {
        pub struct $struct_name<'a> {
            $index_field: &'a $index_type,
            graphrecord: &'a mut GraphRecord,
        }

        impl<'a> $struct_name<'a> {
            pub(crate) fn new(
                $index_field: &'a $index_type,
                graphrecord: &'a mut GraphRecord,
            ) -> GraphRecordResult<Self> {
                if !graphrecord.$contains_fn($index_field) {
                    return Err(GraphRecordError::IndexError(format!(
                        concat!("Cannot find ", $entity, " with index {}"),
                        $index_field
                    )));
                }

                Ok(Self {
                    $index_field,
                    graphrecord,
                })
            }

            fn get_groups(&self) -> Vec<Group> {
                self.graphrecord
                    .$groups_of_fn(self.$index_field)
                    .expect(concat!($entity, " must exist."))
                    .cloned()
                    .collect()
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
                            schema.$schema_update_fn(attributes, None, false);
                        } else {
                            for group in groups {
                                schema.$schema_update_fn(attributes, Some(group), false);
                            }
                        }
                    }
                    SchemaType::Provided => {
                        if groups.is_empty() {
                            schema.$schema_validate_fn(self.$index_field, attributes, None)?;
                        } else {
                            for group in groups {
                                schema.$schema_validate_fn(
                                    self.$index_field,
                                    attributes,
                                    Some(group),
                                )?;
                            }
                        }
                    }
                }

                Ok(())
            }

            fn set_attributes(&mut self, attributes: Attributes) {
                *self
                    .graphrecord
                    .graph
                    .$get_attributes_mut_fn(self.$index_field)
                    .expect(concat!($entity, " must exist.")) = attributes;
            }

            pub fn replace_attributes(&mut self, attributes: Attributes) -> GraphRecordResult<()> {
                let groups = self.get_groups();
                self.handle_schema(&attributes, &groups)?;
                self.set_attributes(attributes);
                Ok(())
            }

            pub fn update_attribute(
                &mut self,
                attribute: &GraphRecordAttribute,
                value: GraphRecordValue,
            ) -> GraphRecordResult<()> {
                let groups = self.get_groups();

                let mut attributes = self
                    .graphrecord
                    .$get_attributes_fn(self.$index_field)
                    .expect(concat!($entity, " must exist."))
                    .clone();
                attributes
                    .entry(attribute.clone())
                    .and_modify(|v| *v = value.clone())
                    .or_insert(value);

                self.handle_schema(&attributes, &groups)?;
                self.set_attributes(attributes);
                Ok(())
            }

            pub fn remove_attribute(
                &mut self,
                attribute: &GraphRecordAttribute,
            ) -> GraphRecordResult<GraphRecordValue> {
                let groups = self.get_groups();

                let mut attributes = self
                    .graphrecord
                    .$get_attributes_fn(self.$index_field)
                    .expect(concat!($entity, " must exist."))
                    .clone();
                let removed_value = attributes.remove(attribute);

                let Some(removed_value) = removed_value else {
                    return Err(GraphRecordError::KeyError(format!(
                        concat!("Attribute {} does not exist on ", $entity, " {}"),
                        attribute, self.$index_field
                    )));
                };

                self.handle_schema(&attributes, &groups)?;
                self.set_attributes(attributes);
                Ok(removed_value)
            }
        }
    };
}

impl_attributes_mut!(
    NodeAttributesMut,
    NodeIndex,
    node_index,
    "node",
    contains_node,
    groups_of_node,
    node_attributes,
    node_attributes_mut,
    update_node,
    validate_node
);

impl_attributes_mut!(
    EdgeAttributesMut,
    EdgeIndex,
    edge_index,
    "edge",
    contains_edge,
    groups_of_edge,
    edge_attributes,
    edge_attributes_mut,
    update_edge,
    validate_edge
);
