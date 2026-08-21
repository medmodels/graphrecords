use super::{AttributeMap, AttributeName, EdgeIndex, GraphRecord, GroupIndex, NodeIndex};
use crate::{errors::SchemaError, graphrecord::datatypes::DataType};
use graphrecords_utils::aliases::GrHashMap;
#[cfg(any(feature = "serde", feature = "io"))]
use serde::{Deserialize, Serialize};
use std::{
    borrow::Borrow,
    collections::{HashMap, hash_map::Entry},
    ops::Deref,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(any(feature = "serde", feature = "io"), derive(Serialize, Deserialize))]
pub enum AttributeType {
    Categorical,
    Continuous,
    Temporal,
    Unstructured,
}

impl AttributeType {
    #[must_use]
    pub fn infer(data_type: &DataType) -> Self {
        match data_type {
            DataType::String | DataType::Null | DataType::Any => Self::Unstructured,
            DataType::Int | DataType::Float => Self::Continuous,
            DataType::Bool => Self::Categorical,
            DataType::DateTime | DataType::Duration => Self::Temporal,
            DataType::Union((first_data_type, second_data_type)) => {
                Self::infer(first_data_type).merge(Self::infer(second_data_type))
            }
            DataType::Option(data_type) => Self::infer(data_type),
        }
    }

    const fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::Categorical, Self::Unstructured) | (Self::Unstructured, Self::Categorical) => {
                Self::Unstructured
            }
            (Self::Categorical, _) | (_, Self::Categorical) => Self::Categorical,
            (Self::Continuous, Self::Continuous) => Self::Continuous,
            (Self::Temporal, Self::Temporal) => Self::Temporal,
            _ => Self::Unstructured,
        }
    }
}

impl DataType {
    fn into_optional(self) -> Self {
        match self {
            Self::Option(_) | Self::Any => self,
            data_type => Self::Option(Box::new(data_type)),
        }
    }

    fn merge(&self, other: &Self) -> Self {
        if self.accepts(other) {
            self.clone()
        } else {
            match (self, other) {
                (Self::Null, _) => Self::Option(Box::new(other.clone())),
                (_, Self::Null) => Self::Option(Box::new(self.clone())),
                (_, Self::Any) => Self::Any,
                (Self::Option(option1), Self::Option(option2)) => {
                    Self::Option(Box::new(option1.merge(option2)))
                }
                (Self::Option(option), _) => Self::Option(Box::new(option.merge(other))),
                (_, Self::Option(option)) => Self::Option(Box::new(self.merge(option))),
                _ => Self::Union((Box::new(self.clone()), Box::new(other.clone()))),
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(any(feature = "serde", feature = "io"), derive(Serialize, Deserialize))]
pub struct AttributeDataType {
    data_type: DataType,
    attribute_type: AttributeType,
}

impl AttributeDataType {
    fn validate(data_type: &DataType, attribute_type: AttributeType) -> Result<(), SchemaError> {
        match (attribute_type, data_type) {
            (AttributeType::Categorical | AttributeType::Unstructured, _)
            | (AttributeType::Continuous, DataType::Int | DataType::Float | DataType::Null)
            | (AttributeType::Temporal, DataType::DateTime | DataType::Duration | DataType::Null) => {
                Ok(())
            }

            (_, DataType::Option(option)) => Self::validate(option, attribute_type),
            (_, DataType::Union((first_data_type, second_data_type))) => {
                Self::validate(first_data_type, attribute_type)?;
                Self::validate(second_data_type, attribute_type)
            }

            (AttributeType::Continuous, _) => Err(SchemaError::ContinuousAttributeNotNumeric),

            (AttributeType::Temporal, _) => Err(SchemaError::TemporalAttributeNotTemporal),
        }
    }

    pub fn new(data_type: DataType, attribute_type: AttributeType) -> Result<Self, SchemaError> {
        Self::validate(&data_type, attribute_type)?;

        Ok(Self {
            data_type,
            attribute_type,
        })
    }

    #[must_use]
    pub const fn data_type(&self) -> &DataType {
        &self.data_type
    }

    #[must_use]
    pub const fn attribute_type(&self) -> &AttributeType {
        &self.attribute_type
    }

    fn merge(&mut self, other: &Self) {
        match (self.data_type.clone(), other.data_type.clone()) {
            (DataType::Null, _) => {
                self.data_type = self.data_type.merge(&other.data_type);
                self.attribute_type = other.attribute_type;
            }
            (_, DataType::Null) => {
                self.data_type = self.data_type.merge(&other.data_type);
            }
            _ => {
                self.data_type = self.data_type.merge(&other.data_type);
                self.attribute_type = self.attribute_type.merge(other.attribute_type);
            }
        }
    }
}

impl From<DataType> for AttributeDataType {
    fn from(value: DataType) -> Self {
        let attribute_type = AttributeType::infer(&value);

        Self {
            data_type: value,
            attribute_type,
        }
    }
}

impl From<(DataType, AttributeType)> for AttributeDataType {
    fn from(value: (DataType, AttributeType)) -> Self {
        Self {
            data_type: value.0,
            attribute_type: value.1,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum AttributeSchemaKind<'a> {
    Node(&'a NodeIndex),
    Edge(&'a EdgeIndex),
}

impl AttributeSchemaKind<'_> {
    fn attribute_missing_error(
        &self,
        attribute: &AttributeName,
        data_type: &DataType,
    ) -> SchemaError {
        match self {
            Self::Node(node_index) => SchemaError::NodeAttributeMissing {
                node_index: (*node_index).clone(),
                attribute: attribute.clone(),
                data_type: data_type.clone(),
            },
            Self::Edge(edge_index) => SchemaError::EdgeAttributeMissing {
                edge_index: **edge_index,
                attribute: attribute.clone(),
                data_type: data_type.clone(),
            },
        }
    }

    fn data_type_mismatch_error(
        &self,
        attribute: &AttributeName,
        data_type: &DataType,
        expected_data_type: &DataType,
    ) -> SchemaError {
        match self {
            Self::Node(node_index) => SchemaError::NodeAttributeDataTypeMismatch {
                node_index: (*node_index).clone(),
                attribute: attribute.clone(),
                data_type: data_type.clone(),
                expected_data_type: expected_data_type.clone(),
            },
            Self::Edge(edge_index) => SchemaError::EdgeAttributeDataTypeMismatch {
                edge_index: **edge_index,
                attribute: attribute.clone(),
                data_type: data_type.clone(),
                expected_data_type: expected_data_type.clone(),
            },
        }
    }

    fn attributes_not_in_schema_error(&self, attributes: Vec<AttributeName>) -> SchemaError {
        match self {
            Self::Node(node_index) => SchemaError::NodeAttributesNotInSchema {
                node_index: (*node_index).clone(),
                attributes,
            },
            Self::Edge(edge_index) => SchemaError::EdgeAttributesNotInSchema {
                edge_index: **edge_index,
                attributes,
            },
        }
    }
}

type AttributeSchemaMapping = HashMap<AttributeName, AttributeDataType>;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[cfg_attr(any(feature = "serde", feature = "io"), derive(Serialize, Deserialize))]
pub struct AttributeSchema(AttributeSchemaMapping);

impl Deref for AttributeSchema {
    type Target = AttributeSchemaMapping;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> From<T> for AttributeSchema
where
    T: Into<AttributeSchemaMapping>,
{
    fn from(value: T) -> Self {
        Self(value.into())
    }
}

impl AttributeSchema {
    #[must_use]
    pub const fn new(mapping: HashMap<AttributeName, AttributeDataType>) -> Self {
        Self(mapping)
    }

    fn validate(
        &self,
        attributes: &AttributeMap,
        kind: &AttributeSchemaKind,
    ) -> Result<(), SchemaError> {
        let mut matched_count = 0;
        let mut attributes_not_in_schema = Vec::new();

        for (key, value) in attributes {
            match self.0.get(key) {
                Some(schema) => {
                    let data_type = DataType::from(value);

                    if !schema.data_type.accepts(&data_type) {
                        return Err(kind.data_type_mismatch_error(
                            key,
                            &data_type,
                            &schema.data_type,
                        ));
                    }

                    matched_count += 1;
                }
                None => {
                    attributes_not_in_schema.push(key.clone());
                }
            }
        }

        if matched_count < self.0.len() {
            for (key, schema) in &self.0 {
                if !attributes.contains_key(key) && !matches!(schema.data_type, DataType::Option(_))
                {
                    return Err(kind.attribute_missing_error(key, &schema.data_type));
                }
            }
        }

        if !attributes_not_in_schema.is_empty() {
            return Err(kind.attributes_not_in_schema_error(attributes_not_in_schema));
        }

        Ok(())
    }

    fn update(&mut self, attributes: &AttributeMap, population_was_empty: bool) {
        for (attribute, data_type) in &mut self.0 {
            if !attributes.contains_key(attribute) {
                data_type.data_type = data_type.data_type.clone().into_optional();
            }
        }

        for (attribute, value) in attributes {
            let data_type = DataType::from(value);
            let attribute_type = AttributeType::infer(&data_type);

            let mut attribute_data_type = AttributeDataType::new(data_type, attribute_type)
                .expect("Inferred attribute type must be valid.");

            match self.0.entry(attribute.clone()) {
                Entry::Occupied(entry) => {
                    entry.into_mut().merge(&attribute_data_type);
                }
                Entry::Vacant(entry) => {
                    if !population_was_empty {
                        attribute_data_type.data_type =
                            attribute_data_type.data_type.clone().into_optional();
                    }

                    entry.insert(attribute_data_type);
                }
            }
        }
    }

    #[must_use]
    pub fn infer(attribute_maps: impl IntoIterator<Item = impl Borrow<AttributeMap>>) -> Self {
        let mut schema = Self::default();

        let mut population_was_empty = true;

        for attributes in attribute_maps {
            schema.update(attributes.borrow(), population_was_empty);

            population_was_empty = false;
        }

        schema
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[cfg_attr(any(feature = "serde", feature = "io"), derive(Serialize, Deserialize))]
pub struct GroupSchema {
    nodes: AttributeSchema,
    edges: AttributeSchema,
}

impl GroupSchema {
    #[must_use]
    pub const fn new(nodes: AttributeSchema, edges: AttributeSchema) -> Self {
        Self { nodes, edges }
    }

    #[must_use]
    pub const fn nodes(&self) -> &AttributeSchema {
        &self.nodes
    }

    #[must_use]
    pub const fn edges(&self) -> &AttributeSchema {
        &self.edges
    }

    pub fn validate_node(
        &self,
        node_index: &NodeIndex,
        attributes: &AttributeMap,
    ) -> Result<(), SchemaError> {
        self.nodes
            .validate(attributes, &AttributeSchemaKind::Node(node_index))
    }

    pub fn validate_edge(
        &self,
        edge_index: &EdgeIndex,
        attributes: &AttributeMap,
    ) -> Result<(), SchemaError> {
        self.edges
            .validate(attributes, &AttributeSchemaKind::Edge(edge_index))
    }

    #[must_use]
    pub fn infer(
        node_attribute_maps: impl IntoIterator<Item = impl Borrow<AttributeMap>>,
        edge_attribute_maps: impl IntoIterator<Item = impl Borrow<AttributeMap>>,
    ) -> Self {
        Self {
            nodes: AttributeSchema::infer(node_attribute_maps),
            edges: AttributeSchema::infer(edge_attribute_maps),
        }
    }

    pub(crate) fn update_node(&mut self, attributes: &AttributeMap, population_was_empty: bool) {
        self.nodes.update(attributes, population_was_empty);
    }

    pub(crate) fn update_edge(&mut self, attributes: &AttributeMap, population_was_empty: bool) {
        self.edges.update(attributes, population_was_empty);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[cfg_attr(any(feature = "serde", feature = "io"), derive(Serialize, Deserialize))]
pub enum SchemaType {
    #[default]
    Inferred,
    Provided,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[cfg_attr(any(feature = "serde", feature = "io"), derive(Serialize, Deserialize))]
pub struct Schema {
    groups: HashMap<GroupIndex, GroupSchema>,
    ungrouped: GroupSchema,
    r#type: SchemaType,
}

impl Schema {
    #[must_use]
    pub const fn new_inferred(
        groups: HashMap<GroupIndex, GroupSchema>,
        ungrouped: GroupSchema,
    ) -> Self {
        Self {
            groups,
            ungrouped,
            r#type: SchemaType::Inferred,
        }
    }

    #[must_use]
    pub const fn new_provided(
        groups: HashMap<GroupIndex, GroupSchema>,
        ungrouped: GroupSchema,
    ) -> Self {
        Self {
            groups,
            ungrouped,
            r#type: SchemaType::Provided,
        }
    }

    #[must_use]
    #[expect(clippy::missing_panics_doc, reason = "infallible")]
    pub fn infer(graphrecord: &GraphRecord) -> Self {
        let state = graphrecord.state();

        let mut group_node_attribute_maps: GrHashMap<_, Vec<_>> = state
            .group_addresses()
            .map(|group_address| {
                state
                    .group_index(group_address)
                    .cloned()
                    .expect("Group must exist.")
            })
            .map(|group_index| (group_index, Vec::new()))
            .collect();
        let mut group_edge_attribute_maps: GrHashMap<_, Vec<_>> = group_node_attribute_maps
            .keys()
            .cloned()
            .map(|group_index| (group_index, Vec::new()))
            .collect();

        let mut ungrouped_node_attribute_maps = Vec::new();
        let mut ungrouped_edge_attribute_maps = Vec::new();

        for node_address in state.node_addresses() {
            let attributes = state.node_attribute_map(node_address);
            let group_indices: Vec<_> = state
                .node_memberships(node_address)
                .map(|group_address| {
                    state
                        .group_index(group_address)
                        .cloned()
                        .expect("Group must exist.")
                })
                .collect();

            if group_indices.is_empty() {
                ungrouped_node_attribute_maps.push(attributes);
                continue;
            }

            for group_index in group_indices {
                group_node_attribute_maps
                    .get_mut(&group_index)
                    .expect("Group must exist.")
                    .push(attributes.clone());
            }
        }

        for edge_address in state.edge_addresses() {
            let attributes = state.edge_attribute_map(edge_address);
            let group_indices: Vec<_> = state
                .edge_memberships(edge_address)
                .map(|group_address| {
                    state
                        .group_index(group_address)
                        .cloned()
                        .expect("Group must exist.")
                })
                .collect();

            if group_indices.is_empty() {
                ungrouped_edge_attribute_maps.push(attributes);
                continue;
            }

            for group_index in group_indices {
                group_edge_attribute_maps
                    .get_mut(&group_index)
                    .expect("Group must exist.")
                    .push(attributes.clone());
            }
        }

        let group_schemas =
            group_node_attribute_maps
                .into_iter()
                .map(|(group_index, node_attribute_maps)| {
                    let edge_attribute_maps = group_edge_attribute_maps
                        .remove(&group_index)
                        .expect("Group must exist.");

                    (
                        group_index,
                        GroupSchema::infer(&node_attribute_maps, &edge_attribute_maps),
                    )
                });

        let ungrouped_schema = GroupSchema::infer(
            &ungrouped_node_attribute_maps,
            &ungrouped_edge_attribute_maps,
        );

        Self {
            groups: group_schemas.collect(),
            ungrouped: ungrouped_schema,
            r#type: SchemaType::Inferred,
        }
    }

    #[must_use]
    pub const fn groups(&self) -> &HashMap<GroupIndex, GroupSchema> {
        &self.groups
    }

    pub fn group(&self, group_index: &GroupIndex) -> Result<&GroupSchema, SchemaError> {
        self.groups
            .get(group_index)
            .ok_or_else(|| SchemaError::GroupNotInSchema {
                group_index: group_index.clone(),
            })
    }

    #[must_use]
    pub const fn ungrouped(&self) -> &GroupSchema {
        &self.ungrouped
    }

    #[must_use]
    pub const fn schema_type(&self) -> &SchemaType {
        &self.r#type
    }

    pub fn validate_node(
        &self,
        node_index: &NodeIndex,
        attributes: &AttributeMap,
        group_index: Option<&GroupIndex>,
    ) -> Result<(), SchemaError> {
        match group_index {
            Some(group_index) => {
                let schema =
                    self.groups
                        .get(group_index)
                        .ok_or_else(|| SchemaError::GroupNotInSchema {
                            group_index: group_index.clone(),
                        })?;

                schema.validate_node(node_index, attributes)
            }
            None => self.ungrouped.validate_node(node_index, attributes),
        }
    }

    pub fn validate_edge(
        &self,
        edge_index: &EdgeIndex,
        attributes: &AttributeMap,
        group_index: Option<&GroupIndex>,
    ) -> Result<(), SchemaError> {
        match group_index {
            Some(group_index) => {
                let schema =
                    self.groups
                        .get(group_index)
                        .ok_or_else(|| SchemaError::GroupNotInSchema {
                            group_index: group_index.clone(),
                        })?;

                schema.validate_edge(edge_index, attributes)
            }
            None => self.ungrouped.validate_edge(edge_index, attributes),
        }
    }

    pub(crate) fn update_node(
        &mut self,
        attributes: &AttributeMap,
        group_index: Option<&GroupIndex>,
        population_was_empty: bool,
    ) {
        match group_index {
            Some(group_index) => {
                self.groups
                    .entry(group_index.clone())
                    .or_default()
                    .update_node(attributes, population_was_empty);
            }
            None => self.ungrouped.update_node(attributes, population_was_empty),
        }
    }

    pub(crate) fn update_edge(
        &mut self,
        attributes: &AttributeMap,
        group_index: Option<&GroupIndex>,
        population_was_empty: bool,
    ) {
        match group_index {
            Some(group_index) => {
                self.groups
                    .entry(group_index.clone())
                    .or_default()
                    .update_edge(attributes, population_was_empty);
            }
            None => self.ungrouped.update_edge(attributes, population_was_empty),
        }
    }

    pub fn set_node_attribute(
        &mut self,
        attribute_name: &AttributeName,
        data_type: DataType,
        attribute_type: AttributeType,
        group_index: Option<&GroupIndex>,
    ) -> Result<(), SchemaError> {
        let attribute_data_type = AttributeDataType::new(data_type, attribute_type)?;

        match group_index {
            Some(group_index) => {
                let group_schema = self.groups.entry(group_index.clone()).or_default();
                group_schema
                    .nodes
                    .0
                    .insert(attribute_name.clone(), attribute_data_type);
            }
            None => {
                self.ungrouped
                    .nodes
                    .0
                    .insert(attribute_name.clone(), attribute_data_type);
            }
        }

        Ok(())
    }

    pub fn set_edge_attribute(
        &mut self,
        attribute_name: &AttributeName,
        data_type: DataType,
        attribute_type: AttributeType,
        group_index: Option<&GroupIndex>,
    ) -> Result<(), SchemaError> {
        let attribute_data_type = AttributeDataType::new(data_type, attribute_type)?;

        match group_index {
            Some(group_index) => {
                let group_schema = self.groups.entry(group_index.clone()).or_default();
                group_schema
                    .edges
                    .0
                    .insert(attribute_name.clone(), attribute_data_type);
            }
            None => {
                self.ungrouped
                    .edges
                    .0
                    .insert(attribute_name.clone(), attribute_data_type);
            }
        }

        Ok(())
    }

    pub fn update_node_attribute(
        &mut self,
        attribute_name: &AttributeName,
        data_type: DataType,
        attribute_type: AttributeType,
        group_index: Option<&GroupIndex>,
    ) -> Result<(), SchemaError> {
        let attribute_data_type = AttributeDataType::new(data_type, attribute_type)?;

        match group_index {
            Some(group_index) => {
                let group_schema = self.groups.entry(group_index.clone()).or_default();
                group_schema
                    .nodes
                    .0
                    .entry(attribute_name.clone())
                    .and_modify(|value| value.merge(&attribute_data_type))
                    .or_insert(attribute_data_type);
            }
            None => {
                self.ungrouped
                    .nodes
                    .0
                    .entry(attribute_name.clone())
                    .and_modify(|value| value.merge(&attribute_data_type))
                    .or_insert(attribute_data_type);
            }
        }

        Ok(())
    }

    pub fn update_edge_attribute(
        &mut self,
        attribute_name: &AttributeName,
        data_type: DataType,
        attribute_type: AttributeType,
        group_index: Option<&GroupIndex>,
    ) -> Result<(), SchemaError> {
        let attribute_data_type = AttributeDataType::new(data_type, attribute_type)?;

        match group_index {
            Some(group_index) => {
                let group_schema = self.groups.entry(group_index.clone()).or_default();
                group_schema
                    .edges
                    .0
                    .entry(attribute_name.clone())
                    .and_modify(|value| value.merge(&attribute_data_type))
                    .or_insert(attribute_data_type);
            }
            None => {
                self.ungrouped
                    .edges
                    .0
                    .entry(attribute_name.clone())
                    .and_modify(|value| value.merge(&attribute_data_type))
                    .or_insert(attribute_data_type);
            }
        }

        Ok(())
    }

    pub fn remove_node_attribute(
        &mut self,
        attribute_name: &AttributeName,
        group_index: Option<&GroupIndex>,
    ) {
        match group_index {
            Some(group_index) => {
                if let Some(group_schema) = self.groups.get_mut(group_index) {
                    group_schema.nodes.0.remove(attribute_name);
                }
            }
            None => {
                self.ungrouped.nodes.0.remove(attribute_name);
            }
        }
    }

    pub fn remove_edge_attribute(
        &mut self,
        attribute_name: &AttributeName,
        group_index: Option<&GroupIndex>,
    ) {
        match group_index {
            Some(group_index) => {
                if let Some(group_schema) = self.groups.get_mut(group_index) {
                    group_schema.edges.0.remove(attribute_name);
                }
            }
            None => {
                self.ungrouped.edges.0.remove(attribute_name);
            }
        }
    }

    pub fn add_group(
        &mut self,
        group_index: GroupIndex,
        schema: GroupSchema,
    ) -> Result<(), SchemaError> {
        if self.groups.contains_key(&group_index) {
            return Err(SchemaError::GroupAlreadyInSchema { group_index });
        }

        self.groups.insert(group_index, schema);

        Ok(())
    }

    pub fn remove_group(&mut self, group_index: &GroupIndex) {
        self.groups.remove(group_index);
    }

    pub const fn freeze(&mut self) {
        self.r#type = SchemaType::Provided;
    }

    pub const fn unfreeze(&mut self) {
        self.r#type = SchemaType::Inferred;
    }
}

#[cfg(test)]
mod test {
    use super::{AttributeDataType, GroupSchema, SchemaType};
    use crate::{
        GraphRecord,
        graphrecord::{
            AttributeMap, EdgeIndex, Schema, Value,
            datatypes::DataType,
            schema::{AttributeSchema, AttributeSchemaKind, AttributeType},
        },
    };
    use std::collections::HashMap;

    #[test]
    fn test_attribute_type_infer() {
        assert_eq!(
            AttributeType::Unstructured,
            AttributeType::infer(&DataType::String)
        );
        assert_eq!(
            AttributeType::Continuous,
            AttributeType::infer(&DataType::Int)
        );
        assert_eq!(
            AttributeType::Continuous,
            AttributeType::infer(&DataType::Float)
        );
        assert_eq!(
            AttributeType::Categorical,
            AttributeType::infer(&DataType::Bool)
        );
        assert_eq!(
            AttributeType::Temporal,
            AttributeType::infer(&DataType::DateTime)
        );
        assert_eq!(
            AttributeType::Temporal,
            AttributeType::infer(&DataType::Duration)
        );
        assert_eq!(
            AttributeType::Unstructured,
            AttributeType::infer(&DataType::Null)
        );
        assert_eq!(
            AttributeType::Unstructured,
            AttributeType::infer(&DataType::Any)
        );
        assert_eq!(
            AttributeType::Continuous,
            AttributeType::infer(&DataType::Union((
                Box::new(DataType::Int),
                Box::new(DataType::Float)
            )))
        );
        assert_eq!(
            AttributeType::Continuous,
            AttributeType::infer(&DataType::Option(Box::new(DataType::Int)))
        );
    }

    #[test]
    fn test_attribute_type_merge() {
        assert_eq!(
            AttributeType::Unstructured,
            AttributeType::Categorical.merge(AttributeType::Unstructured)
        );
        assert_eq!(
            AttributeType::Unstructured,
            AttributeType::Unstructured.merge(AttributeType::Categorical)
        );

        assert_eq!(
            AttributeType::Categorical,
            AttributeType::Categorical.merge(AttributeType::Categorical)
        );
        assert_eq!(
            AttributeType::Categorical,
            AttributeType::Categorical.merge(AttributeType::Continuous)
        );
        assert_eq!(
            AttributeType::Categorical,
            AttributeType::Categorical.merge(AttributeType::Temporal)
        );

        assert_eq!(
            AttributeType::Categorical,
            AttributeType::Continuous.merge(AttributeType::Categorical)
        );
        assert_eq!(
            AttributeType::Categorical,
            AttributeType::Temporal.merge(AttributeType::Categorical)
        );

        assert_eq!(
            AttributeType::Continuous,
            AttributeType::Continuous.merge(AttributeType::Continuous)
        );

        assert_eq!(
            AttributeType::Temporal,
            AttributeType::Temporal.merge(AttributeType::Temporal)
        );

        assert_eq!(
            AttributeType::Unstructured,
            AttributeType::Continuous.merge(AttributeType::Temporal)
        );
        assert_eq!(
            AttributeType::Unstructured,
            AttributeType::Continuous.merge(AttributeType::Unstructured)
        );

        assert_eq!(
            AttributeType::Unstructured,
            AttributeType::Temporal.merge(AttributeType::Continuous)
        );
        assert_eq!(
            AttributeType::Unstructured,
            AttributeType::Temporal.merge(AttributeType::Unstructured)
        );

        assert_eq!(
            AttributeType::Unstructured,
            AttributeType::Unstructured.merge(AttributeType::Continuous)
        );
        assert_eq!(
            AttributeType::Unstructured,
            AttributeType::Unstructured.merge(AttributeType::Temporal)
        );
        assert_eq!(
            AttributeType::Unstructured,
            AttributeType::Unstructured.merge(AttributeType::Unstructured)
        );
    }

    #[test]
    fn test_data_type_merge() {
        assert_eq!(DataType::Int, DataType::Int.merge(&DataType::Int));
        assert_eq!(
            DataType::Union((Box::new(DataType::Int), Box::new(DataType::Float))),
            DataType::Int.merge(&DataType::Float)
        );
        assert_eq!(
            DataType::Option(Box::new(DataType::Int)),
            DataType::Int.merge(&DataType::Null)
        );
        assert_eq!(
            DataType::Option(Box::new(DataType::Int)),
            DataType::Null.merge(&DataType::Int)
        );
        assert_eq!(DataType::Null, DataType::Null.merge(&DataType::Null));
        assert_eq!(DataType::Any, DataType::Int.merge(&DataType::Any));
        assert_eq!(DataType::Any, DataType::Any.merge(&DataType::Int));
        assert_eq!(
            DataType::Option(Box::new(DataType::Union((
                Box::new(DataType::Int),
                Box::new(DataType::String)
            )))),
            DataType::Option(Box::new(DataType::Int)).merge(&DataType::String)
        );
        assert_eq!(
            DataType::Option(Box::new(DataType::Int)),
            DataType::Int.merge(&DataType::Option(Box::new(DataType::Int)))
        );
        assert_eq!(
            DataType::Option(Box::new(DataType::Union((
                Box::new(DataType::Int),
                Box::new(DataType::String)
            )))),
            DataType::Option(Box::new(DataType::Int))
                .merge(&DataType::Option(Box::new(DataType::String)))
        );
    }

    #[test]
    fn test_attribute_data_type_new() {
        assert!(AttributeDataType::new(DataType::String, AttributeType::Categorical).is_ok());
        assert!(AttributeDataType::new(DataType::String, AttributeType::Continuous).is_err());
        assert!(AttributeDataType::new(DataType::String, AttributeType::Temporal).is_err());
        assert!(AttributeDataType::new(DataType::String, AttributeType::Unstructured).is_ok());

        assert!(AttributeDataType::new(DataType::Int, AttributeType::Categorical).is_ok());
        assert!(AttributeDataType::new(DataType::Int, AttributeType::Continuous).is_ok());
        assert!(AttributeDataType::new(DataType::Int, AttributeType::Temporal).is_err());
        assert!(AttributeDataType::new(DataType::Int, AttributeType::Unstructured).is_ok());

        assert!(AttributeDataType::new(DataType::Float, AttributeType::Categorical).is_ok());
        assert!(AttributeDataType::new(DataType::Float, AttributeType::Continuous).is_ok());
        assert!(AttributeDataType::new(DataType::Float, AttributeType::Temporal).is_err());
        assert!(AttributeDataType::new(DataType::Float, AttributeType::Unstructured).is_ok());

        assert!(AttributeDataType::new(DataType::Bool, AttributeType::Categorical).is_ok());
        assert!(AttributeDataType::new(DataType::Bool, AttributeType::Continuous).is_err());
        assert!(AttributeDataType::new(DataType::Bool, AttributeType::Temporal).is_err());
        assert!(AttributeDataType::new(DataType::Bool, AttributeType::Unstructured).is_ok());

        assert!(AttributeDataType::new(DataType::DateTime, AttributeType::Categorical).is_ok());
        assert!(AttributeDataType::new(DataType::DateTime, AttributeType::Continuous).is_err());
        assert!(AttributeDataType::new(DataType::DateTime, AttributeType::Temporal).is_ok());
        assert!(AttributeDataType::new(DataType::DateTime, AttributeType::Unstructured).is_ok());

        assert!(AttributeDataType::new(DataType::Duration, AttributeType::Categorical).is_ok());
        assert!(AttributeDataType::new(DataType::Duration, AttributeType::Continuous).is_err());
        assert!(AttributeDataType::new(DataType::Duration, AttributeType::Temporal).is_ok());
        assert!(AttributeDataType::new(DataType::Duration, AttributeType::Unstructured).is_ok());

        assert!(AttributeDataType::new(DataType::Null, AttributeType::Categorical).is_ok());
        assert!(AttributeDataType::new(DataType::Null, AttributeType::Continuous).is_ok());
        assert!(AttributeDataType::new(DataType::Null, AttributeType::Temporal).is_ok());
        assert!(AttributeDataType::new(DataType::Null, AttributeType::Unstructured).is_ok());

        assert!(AttributeDataType::new(DataType::Any, AttributeType::Categorical).is_ok());
        assert!(AttributeDataType::new(DataType::Any, AttributeType::Continuous).is_err());
        assert!(AttributeDataType::new(DataType::Any, AttributeType::Temporal).is_err());
        assert!(AttributeDataType::new(DataType::Any, AttributeType::Unstructured).is_ok());

        assert!(
            AttributeDataType::new(
                DataType::Option(Box::new(DataType::Int)),
                AttributeType::Categorical
            )
            .is_ok()
        );
        assert!(
            AttributeDataType::new(
                DataType::Option(Box::new(DataType::Int)),
                AttributeType::Continuous
            )
            .is_ok()
        );
        assert!(
            AttributeDataType::new(
                DataType::Option(Box::new(DataType::Int)),
                AttributeType::Temporal
            )
            .is_err()
        );
        assert!(
            AttributeDataType::new(
                DataType::Option(Box::new(DataType::Int)),
                AttributeType::Unstructured
            )
            .is_ok()
        );

        assert!(
            AttributeDataType::new(
                DataType::Union((Box::new(DataType::Int), Box::new(DataType::Float))),
                AttributeType::Categorical
            )
            .is_ok()
        );
        assert!(
            AttributeDataType::new(
                DataType::Union((Box::new(DataType::Int), Box::new(DataType::Float))),
                AttributeType::Continuous
            )
            .is_ok()
        );
        assert!(
            AttributeDataType::new(
                DataType::Union((Box::new(DataType::Int), Box::new(DataType::Float))),
                AttributeType::Temporal
            )
            .is_err()
        );
        assert!(
            AttributeDataType::new(
                DataType::Union((Box::new(DataType::Int), Box::new(DataType::Float))),
                AttributeType::Unstructured
            )
            .is_ok()
        );
    }

    #[test]
    fn test_attribute_data_type_data_type() {
        let attribute_data_type =
            AttributeDataType::new(DataType::Int, AttributeType::Categorical).unwrap();

        assert_eq!(&DataType::Int, attribute_data_type.data_type());
    }

    #[test]
    fn test_attribute_data_type_attribute_type() {
        let attribute_data_type =
            AttributeDataType::new(DataType::Int, AttributeType::Categorical).unwrap();

        assert_eq!(
            &AttributeType::Categorical,
            attribute_data_type.attribute_type()
        );
    }

    #[test]
    fn test_attribute_data_type_merge() {
        let mut attribute_data_type =
            AttributeDataType::new(DataType::Int, AttributeType::Categorical).unwrap();

        attribute_data_type
            .merge(&AttributeDataType::new(DataType::Float, AttributeType::Continuous).unwrap());

        assert_eq!(
            &DataType::Union((Box::new(DataType::Int), Box::new(DataType::Float))),
            attribute_data_type.data_type()
        );
        assert_eq!(
            &AttributeType::Categorical,
            attribute_data_type.attribute_type()
        );
    }

    #[test]
    fn test_attribute_data_type_from_data_type() {
        let attribute_data_type: AttributeDataType = DataType::Int.into();

        assert_eq!(&DataType::Int, attribute_data_type.data_type());
        assert_eq!(
            &AttributeType::Continuous,
            attribute_data_type.attribute_type()
        );
    }

    #[test]
    fn test_attribute_data_type_from_tuple() {
        let attribute_data_type: AttributeDataType =
            (DataType::Int, AttributeType::Categorical).into();

        assert_eq!(&DataType::Int, attribute_data_type.data_type());
        assert_eq!(
            &AttributeType::Categorical,
            attribute_data_type.attribute_type()
        );
    }

    #[test]
    fn test_attribute_schema_deref() {
        let schema = AttributeSchema::new(
            vec![
                (
                    "lorem".into(),
                    AttributeDataType::new(DataType::Int, AttributeType::Categorical).unwrap(),
                ),
                (
                    "ipsum".into(),
                    AttributeDataType::new(DataType::Float, AttributeType::Continuous).unwrap(),
                ),
            ]
            .into_iter()
            .collect(),
        );

        assert_eq!(
            &DataType::Int,
            schema.get(&"lorem".into()).unwrap().data_type()
        );
        assert_eq!(
            &DataType::Float,
            schema.get(&"ipsum".into()).unwrap().data_type()
        );
    }

    #[test]
    fn test_attribute_schema_validate() {
        let attribute_schema = AttributeSchema::new(
            vec![
                (
                    "lorem".into(),
                    AttributeDataType::new(DataType::Int, AttributeType::Categorical).unwrap(),
                ),
                (
                    "ipsum".into(),
                    AttributeDataType::new(DataType::Float, AttributeType::Continuous).unwrap(),
                ),
            ]
            .into_iter()
            .collect(),
        );

        let attributes: AttributeMap =
            vec![("lorem".into(), 0.into()), ("ipsum".into(), 0.0.into())]
                .into_iter()
                .collect();

        assert!(
            attribute_schema
                .validate(&attributes, &AttributeSchemaKind::Node(&0.into()))
                .is_ok()
        );

        let attributes: AttributeMap =
            vec![("lorem".into(), 0.0.into()), ("ipsum".into(), 0.into())]
                .into_iter()
                .collect();

        assert!(
            attribute_schema
                .validate(&attributes, &AttributeSchemaKind::Node(&0.into()))
                .is_err_and(|error| {
                    matches!(
                        error,
                        crate::errors::SchemaError::NodeAttributeDataTypeMismatch { .. }
                    )
                })
        );

        let attributes: AttributeMap = vec![
            ("lorem".into(), 0.into()),
            ("ipsum".into(), 0.0.into()),
            ("sit".into(), 0.0.into()),
        ]
        .into_iter()
        .collect();

        assert!(
            attribute_schema
                .validate(&attributes, &AttributeSchemaKind::Node(&0.into()))
                .is_err_and(|error| {
                    matches!(
                        error,
                        crate::errors::SchemaError::NodeAttributesNotInSchema { .. }
                    )
                })
        );

        let attribute_schema = AttributeSchema::new(
            vec![(
                "lorem".into(),
                AttributeDataType::new(DataType::Int, AttributeType::Categorical).unwrap(),
            )]
            .into_iter()
            .collect(),
        );
        let attributes = AttributeMap::new();

        assert!(
            attribute_schema
                .validate(&attributes, &AttributeSchemaKind::Node(&0.into()))
                .is_err_and(|error| {
                    matches!(
                        error,
                        crate::errors::SchemaError::NodeAttributeMissing { .. }
                    )
                })
        );
    }

    #[test]
    fn test_attribute_schema_update() {
        let mut schema = AttributeSchema::default();
        let attributes: AttributeMap =
            vec![("lorem".into(), 0.into()), ("ipsum".into(), "amet".into())]
                .into_iter()
                .collect();

        schema.update(&attributes, true);

        assert_eq!(2, schema.0.len());
        assert_eq!(
            &DataType::Int,
            schema.0.get(&"lorem".into()).unwrap().data_type()
        );
        assert_eq!(
            &DataType::String,
            schema.0.get(&"ipsum".into()).unwrap().data_type()
        );

        let new_attributes: AttributeMap =
            vec![("lorem".into(), 0.5.into()), ("sit".into(), true.into())]
                .into_iter()
                .collect();

        schema.update(&new_attributes, false);

        assert_eq!(3, schema.0.len());
        assert_eq!(
            &DataType::Union((Box::new(DataType::Int), Box::new(DataType::Float))),
            schema.0.get(&"lorem".into()).unwrap().data_type()
        );
        assert_eq!(
            &DataType::Option(Box::new(DataType::String)),
            schema.0.get(&"ipsum".into()).unwrap().data_type()
        );
        assert_eq!(
            &DataType::Option(Box::new(DataType::Bool)),
            schema.0.get(&"sit".into()).unwrap().data_type()
        );
    }

    #[test]
    fn test_attribute_schema_infer() {
        let attributes1: AttributeMap =
            vec![("lorem".into(), 0.into()), ("ipsum".into(), "amet".into())]
                .into_iter()
                .collect();

        let attributes2: AttributeMap =
            vec![("lorem".into(), 1.into()), ("sit".into(), true.into())]
                .into_iter()
                .collect();

        let schema = AttributeSchema::infer(vec![&attributes1, &attributes2]);

        assert_eq!(3, schema.0.len());
        assert_eq!(
            &DataType::Int,
            schema.0.get(&"lorem".into()).unwrap().data_type()
        );
        assert_eq!(
            &DataType::Option(Box::new(DataType::String)),
            schema.0.get(&"ipsum".into()).unwrap().data_type()
        );
        assert_eq!(
            &DataType::Option(Box::new(DataType::Bool)),
            schema.0.get(&"sit".into()).unwrap().data_type()
        );

        let attributes1: AttributeMap = vec![("lorem".into(), Value::Null)].into_iter().collect();
        let attributes2 = AttributeMap::new();

        let schema = AttributeSchema::infer(vec![&attributes1, &attributes2]);

        assert_eq!(
            &DataType::Option(Box::new(DataType::Null)),
            schema.0.get(&"lorem".into()).unwrap().data_type()
        );

        let attributes1 = AttributeMap::new();
        let attributes2: AttributeMap = vec![("lorem".into(), Value::Null)].into_iter().collect();

        let schema = AttributeSchema::infer(vec![&attributes1, &attributes2]);

        assert_eq!(
            &DataType::Option(Box::new(DataType::Null)),
            schema.0.get(&"lorem".into()).unwrap().data_type()
        );

        let attributes1: AttributeMap = vec![("lorem".into(), Value::Null)].into_iter().collect();
        let attributes2: AttributeMap = vec![("lorem".into(), 5.into())].into_iter().collect();

        let schema = AttributeSchema::infer(vec![&attributes1, &attributes2]);

        assert_eq!(
            &DataType::Option(Box::new(DataType::Int)),
            schema.0.get(&"lorem".into()).unwrap().data_type()
        );

        let attributes1: AttributeMap = vec![("lorem".into(), 5.into())].into_iter().collect();
        let attributes2: AttributeMap = vec![("lorem".into(), Value::Null)].into_iter().collect();

        let schema = AttributeSchema::infer(vec![&attributes1, &attributes2]);

        assert_eq!(
            &DataType::Option(Box::new(DataType::Int)),
            schema.0.get(&"lorem".into()).unwrap().data_type()
        );
    }

    #[test]
    fn test_group_schema_nodes() {
        let nodes = AttributeSchema::new(
            vec![
                (
                    "lorem".into(),
                    AttributeDataType::new(DataType::Int, AttributeType::Categorical).unwrap(),
                ),
                (
                    "ipsum".into(),
                    AttributeDataType::new(DataType::Float, AttributeType::Continuous).unwrap(),
                ),
            ]
            .into_iter()
            .collect(),
        );

        let group_schema = GroupSchema::new(nodes.clone(), AttributeSchema::default());

        assert_eq!(&nodes, group_schema.nodes());
    }

    #[test]
    fn test_group_schema_edges() {
        let edges = AttributeSchema::new(
            vec![
                (
                    "lorem".into(),
                    AttributeDataType::new(DataType::Int, AttributeType::Categorical).unwrap(),
                ),
                (
                    "ipsum".into(),
                    AttributeDataType::new(DataType::Float, AttributeType::Continuous).unwrap(),
                ),
            ]
            .into_iter()
            .collect(),
        );

        let group_schema = GroupSchema::new(AttributeSchema::default(), edges.clone());

        assert_eq!(&edges, group_schema.edges());
    }

    #[test]
    fn test_group_schema_validate_node() {
        let nodes = AttributeSchema::new(
            vec![
                (
                    "lorem".into(),
                    AttributeDataType::new(DataType::Int, AttributeType::Categorical).unwrap(),
                ),
                (
                    "ipsum".into(),
                    AttributeDataType::new(DataType::Float, AttributeType::Continuous).unwrap(),
                ),
            ]
            .into_iter()
            .collect(),
        );

        let group_schema = GroupSchema::new(nodes, AttributeSchema::default());

        let attributes: AttributeMap =
            vec![("lorem".into(), 0.into()), ("ipsum".into(), 0.0.into())]
                .into_iter()
                .collect();

        assert!(group_schema.validate_node(&0.into(), &attributes).is_ok());

        let attributes: AttributeMap =
            vec![("lorem".into(), 0.0.into()), ("ipsum".into(), 0.into())]
                .into_iter()
                .collect();

        assert!(
            group_schema
                .validate_node(&0.into(), &attributes)
                .is_err_and(|error| {
                    matches!(
                        error,
                        crate::errors::SchemaError::NodeAttributeDataTypeMismatch { .. }
                    )
                })
        );
    }

    #[test]
    fn test_group_schema_validate_edge() {
        let edges = AttributeSchema::new(
            vec![
                (
                    "lorem".into(),
                    AttributeDataType::new(DataType::Int, AttributeType::Categorical).unwrap(),
                ),
                (
                    "ipsum".into(),
                    AttributeDataType::new(DataType::Float, AttributeType::Continuous).unwrap(),
                ),
            ]
            .into_iter()
            .collect(),
        );

        let group_schema = GroupSchema::new(AttributeSchema::default(), edges);
        let edge_index = EdgeIndex::new(0, 0);

        let attributes: AttributeMap =
            vec![("lorem".into(), 0.into()), ("ipsum".into(), 0.0.into())]
                .into_iter()
                .collect();

        assert!(group_schema.validate_edge(&edge_index, &attributes).is_ok());

        let attributes: AttributeMap =
            vec![("lorem".into(), 0.0.into()), ("ipsum".into(), 0.into())]
                .into_iter()
                .collect();

        assert!(
            group_schema
                .validate_edge(&edge_index, &attributes)
                .is_err_and(|error| {
                    matches!(
                        error,
                        crate::errors::SchemaError::EdgeAttributeDataTypeMismatch { .. }
                    )
                })
        );
    }

    #[test]
    fn test_group_schema_infer() {
        let node_attributes1: AttributeMap = vec![
            ("lorem".into(), 0.into()),
            ("ipsum".into(), "adipiscing".into()),
        ]
        .into_iter()
        .collect();

        let node_attributes2: AttributeMap =
            vec![("lorem".into(), 1.into()), ("sit".into(), true.into())]
                .into_iter()
                .collect();

        let edge_attributes: AttributeMap = vec![
            ("amet".into(), 0.5.into()),
            ("consectetur".into(), "elit".into()),
        ]
        .into_iter()
        .collect();

        let group_schema = GroupSchema::infer(
            vec![&node_attributes1, &node_attributes2],
            vec![&edge_attributes],
        );

        assert_eq!(3, group_schema.nodes().len());
        assert_eq!(2, group_schema.edges().len());

        assert_eq!(
            &DataType::Int,
            group_schema
                .nodes()
                .get(&"lorem".into())
                .unwrap()
                .data_type()
        );
        assert_eq!(
            &DataType::Option(Box::new(DataType::String)),
            group_schema
                .nodes()
                .get(&"ipsum".into())
                .unwrap()
                .data_type()
        );
        assert_eq!(
            &DataType::Option(Box::new(DataType::Bool)),
            group_schema.nodes().get(&"sit".into()).unwrap().data_type()
        );

        assert_eq!(
            &DataType::Float,
            group_schema
                .edges()
                .get(&"amet".into())
                .unwrap()
                .data_type()
        );
        assert_eq!(
            &DataType::String,
            group_schema
                .edges()
                .get(&"consectetur".into())
                .unwrap()
                .data_type()
        );
    }

    #[test]
    fn test_group_schema_update_node() {
        let mut group_schema = GroupSchema::default();
        let attributes =
            AttributeMap::from([("lorem".into(), 0.into()), ("ipsum".into(), 0.0.into())]);

        group_schema.update_node(&attributes, true);

        assert_eq!(2, group_schema.nodes().len());
        assert_eq!(
            &DataType::Int,
            group_schema
                .nodes()
                .get(&"lorem".into())
                .unwrap()
                .data_type()
        );
        assert_eq!(
            &DataType::Float,
            group_schema
                .nodes()
                .get(&"ipsum".into())
                .unwrap()
                .data_type()
        );
    }

    #[test]
    fn test_group_schema_update_edge() {
        let mut group_schema = GroupSchema::default();
        let attributes = AttributeMap::from([
            ("lorem".into(), true.into()),
            ("ipsum".into(), "sit".into()),
        ]);

        group_schema.update_edge(&attributes, true);

        assert_eq!(2, group_schema.edges().len());
        assert_eq!(
            &DataType::Bool,
            group_schema
                .edges()
                .get(&"lorem".into())
                .unwrap()
                .data_type()
        );
        assert_eq!(
            &DataType::String,
            group_schema
                .edges()
                .get(&"ipsum".into())
                .unwrap()
                .data_type()
        );
    }

    #[test]
    fn test_schema_new_inferred() {
        let group_indices: HashMap<_, _> = vec![("dolor".into(), GroupSchema::default())]
            .into_iter()
            .collect();

        let schema = Schema::new_inferred(group_indices, GroupSchema::default());

        assert_eq!(&SchemaType::Inferred, schema.schema_type());
        assert_eq!(1, schema.groups().len());
        assert!(schema.groups().contains_key(&"dolor".into()));
        assert_eq!(&GroupSchema::default(), schema.ungrouped());
    }

    #[test]
    fn test_schema_new_provided() {
        let group_indices: HashMap<_, _> = vec![("dolor".into(), GroupSchema::default())]
            .into_iter()
            .collect();

        let schema = Schema::new_provided(group_indices, GroupSchema::default());

        assert_eq!(&SchemaType::Provided, schema.schema_type());
        assert_eq!(1, schema.groups().len());
        assert!(schema.groups().contains_key(&"dolor".into()));
        assert_eq!(&GroupSchema::default(), schema.ungrouped());
    }

    #[test]
    fn test_schema_infer() {
        let graphrecord = GraphRecord::new()
            .add_node(0, AttributeMap::from([("lorem".into(), 0.into())]))
            .unwrap()
            .add_node(1, AttributeMap::from([("ipsum".into(), 0.0.into())]))
            .unwrap()
            .add_edge(0, 1, AttributeMap::from([("sit".into(), true.into())]))
            .unwrap();

        let schema = Schema::infer(&graphrecord);

        assert_eq!(2, schema.ungrouped().nodes().len());
        assert_eq!(1, schema.ungrouped().edges().len());

        let edge_index = graphrecord.edge_indices().next().unwrap();

        let graphrecord = graphrecord
            .add_group("dolor")
            .unwrap()
            .add_nodes_to_group(vec![0, 1], "dolor")
            .unwrap()
            .add_edges_to_group(vec![edge_index], "dolor")
            .unwrap();

        let schema = Schema::infer(&graphrecord);

        assert_eq!(1, schema.groups().len());
        assert_eq!(2, schema.group(&"dolor".into()).unwrap().nodes().len());
        assert_eq!(1, schema.group(&"dolor".into()).unwrap().edges().len());
    }

    #[test]
    fn test_schema_groups() {
        let schema = Schema::new_inferred(
            vec![("dolor".into(), GroupSchema::default())]
                .into_iter()
                .collect(),
            GroupSchema::default(),
        );
        assert_eq!(1, schema.groups().len());
        assert!(schema.groups().contains_key(&"dolor".into()));
    }

    #[test]
    fn test_schema_group() {
        let schema = Schema::new_inferred(
            vec![("dolor".into(), GroupSchema::default())]
                .into_iter()
                .collect(),
            GroupSchema::default(),
        );
        assert!(schema.group(&"dolor".into()).is_ok());
        assert!(schema.group(&"missing".into()).is_err());
    }

    #[test]
    fn test_schema_ungrouped() {
        let ungrouped = GroupSchema::new(
            AttributeSchema::new(
                vec![(
                    "lorem".into(),
                    AttributeDataType::new(DataType::Int, AttributeType::Categorical).unwrap(),
                )]
                .into_iter()
                .collect(),
            ),
            AttributeSchema::default(),
        );

        let schema = Schema::new_inferred(HashMap::new(), ungrouped.clone());

        assert_eq!(&ungrouped, schema.ungrouped());
    }

    #[test]
    fn test_schema_schema_type() {
        let schema = Schema::new_inferred(HashMap::new(), GroupSchema::default());
        assert_eq!(&SchemaType::Inferred, schema.schema_type());
    }

    #[test]
    fn test_schema_validate_node() {
        let mut schema = Schema::new_inferred(
            HashMap::new(),
            GroupSchema::new(AttributeSchema::default(), AttributeSchema::default()),
        );
        schema
            .set_node_attribute(
                &"lorem".into(),
                DataType::Int,
                AttributeType::Continuous,
                None,
            )
            .unwrap();

        let attributes = AttributeMap::from([("lorem".into(), 0.into())]);
        assert!(schema.validate_node(&0.into(), &attributes, None).is_ok());

        let invalid_attributes = AttributeMap::from([("lorem".into(), "ipsum".into())]);
        assert!(
            schema
                .validate_node(&0.into(), &invalid_attributes, None)
                .is_err()
        );
    }

    #[test]
    fn test_schema_validate_edge() {
        let mut schema = Schema::new_inferred(
            HashMap::new(),
            GroupSchema::new(AttributeSchema::default(), AttributeSchema::default()),
        );
        schema
            .set_edge_attribute(
                &"lorem".into(),
                DataType::Bool,
                AttributeType::Categorical,
                None,
            )
            .unwrap();

        let edge_index = EdgeIndex::new(0, 0);

        let attributes = AttributeMap::from([("lorem".into(), true.into())]);
        assert!(schema.validate_edge(&edge_index, &attributes, None).is_ok());

        let invalid_attributes = AttributeMap::from([("lorem".into(), 0.into())]);
        assert!(
            schema
                .validate_edge(&edge_index, &invalid_attributes, None)
                .is_err()
        );
    }

    #[test]
    fn test_schema_update_node() {
        let mut schema = Schema::new_inferred(
            HashMap::new(),
            GroupSchema::new(AttributeSchema::default(), AttributeSchema::default()),
        );
        let attributes =
            AttributeMap::from([("lorem".into(), 0.into()), ("ipsum".into(), 0.0.into())]);

        schema.update_node(&attributes, None, true);

        assert_eq!(2, schema.ungrouped().nodes().len());
        assert_eq!(
            &DataType::Int,
            schema
                .ungrouped()
                .nodes()
                .get(&"lorem".into())
                .unwrap()
                .data_type()
        );
        assert_eq!(
            &DataType::Float,
            schema
                .ungrouped()
                .nodes()
                .get(&"ipsum".into())
                .unwrap()
                .data_type()
        );
    }

    #[test]
    fn test_schema_update_edge() {
        let mut schema = Schema::new_inferred(
            HashMap::new(),
            GroupSchema::new(AttributeSchema::default(), AttributeSchema::default()),
        );
        let attributes = AttributeMap::from([
            ("lorem".into(), true.into()),
            ("ipsum".into(), "sit".into()),
        ]);

        schema.update_edge(&attributes, None, true);

        assert_eq!(2, schema.ungrouped().edges().len());
        assert_eq!(
            &DataType::Bool,
            schema
                .ungrouped()
                .edges()
                .get(&"lorem".into())
                .unwrap()
                .data_type()
        );
        assert_eq!(
            &DataType::String,
            schema
                .ungrouped()
                .edges()
                .get(&"ipsum".into())
                .unwrap()
                .data_type()
        );
    }

    #[test]
    fn test_schema_set_node_attribute() {
        let mut schema = Schema::new_inferred(HashMap::new(), GroupSchema::default());
        assert!(
            schema
                .set_node_attribute(
                    &"lorem".into(),
                    DataType::Int,
                    AttributeType::Continuous,
                    None
                )
                .is_ok()
        );
        assert_eq!(
            &DataType::Int,
            schema
                .ungrouped()
                .nodes()
                .get(&"lorem".into())
                .unwrap()
                .data_type()
        );
        assert!(
            schema
                .set_node_attribute(
                    &"lorem".into(),
                    DataType::Float,
                    AttributeType::Continuous,
                    None
                )
                .is_ok()
        );
        assert_eq!(
            &DataType::Float,
            schema
                .ungrouped()
                .nodes()
                .get(&"lorem".into())
                .unwrap()
                .data_type()
        );

        assert!(
            schema
                .set_node_attribute(
                    &"lorem".into(),
                    DataType::Float,
                    AttributeType::Continuous,
                    Some(&"dolor".into())
                )
                .is_ok()
        );
        assert_eq!(
            &DataType::Float,
            schema
                .group(&"dolor".into())
                .unwrap()
                .nodes()
                .get(&"lorem".into())
                .unwrap()
                .data_type()
        );
    }

    #[test]
    fn test_schema_set_edge_attribute() {
        let mut schema = Schema::new_inferred(HashMap::new(), GroupSchema::default());
        assert!(
            schema
                .set_edge_attribute(
                    &"lorem".into(),
                    DataType::Bool,
                    AttributeType::Categorical,
                    None
                )
                .is_ok()
        );
        assert_eq!(
            &DataType::Bool,
            schema
                .ungrouped()
                .edges()
                .get(&"lorem".into())
                .unwrap()
                .data_type()
        );
        assert!(
            schema
                .set_edge_attribute(
                    &"lorem".into(),
                    DataType::Float,
                    AttributeType::Continuous,
                    None
                )
                .is_ok()
        );
        assert_eq!(
            &DataType::Float,
            schema
                .ungrouped()
                .edges()
                .get(&"lorem".into())
                .unwrap()
                .data_type()
        );

        assert!(
            schema
                .set_edge_attribute(
                    &"lorem".into(),
                    DataType::Float,
                    AttributeType::Continuous,
                    Some(&"dolor".into())
                )
                .is_ok()
        );
        assert_eq!(
            &DataType::Float,
            schema
                .group(&"dolor".into())
                .unwrap()
                .edges()
                .get(&"lorem".into())
                .unwrap()
                .data_type()
        );
    }

    #[test]
    fn test_schema_update_node_attribute() {
        let mut schema = Schema::new_inferred(HashMap::new(), GroupSchema::default());
        schema
            .set_node_attribute(
                &"lorem".into(),
                DataType::Int,
                AttributeType::Continuous,
                None,
            )
            .unwrap();
        assert!(
            schema
                .update_node_attribute(
                    &"lorem".into(),
                    DataType::Float,
                    AttributeType::Continuous,
                    None
                )
                .is_ok()
        );
        assert_eq!(
            &DataType::Union((Box::new(DataType::Int), Box::new(DataType::Float))),
            schema
                .ungrouped()
                .nodes()
                .get(&"lorem".into())
                .unwrap()
                .data_type()
        );

        schema
            .set_node_attribute(
                &"lorem".into(),
                DataType::Int,
                AttributeType::Continuous,
                Some(&"dolor".into()),
            )
            .unwrap();
        assert!(
            schema
                .update_node_attribute(
                    &"lorem".into(),
                    DataType::Float,
                    AttributeType::Continuous,
                    Some(&"dolor".into())
                )
                .is_ok()
        );
        assert_eq!(
            &DataType::Union((Box::new(DataType::Int), Box::new(DataType::Float))),
            schema
                .group(&"dolor".into())
                .unwrap()
                .nodes()
                .get(&"lorem".into())
                .unwrap()
                .data_type()
        );
    }

    #[test]
    fn test_schema_update_edge_attribute() {
        let mut schema = Schema::new_inferred(HashMap::new(), GroupSchema::default());
        schema
            .set_edge_attribute(
                &"lorem".into(),
                DataType::Bool,
                AttributeType::Categorical,
                None,
            )
            .unwrap();
        assert!(
            schema
                .update_edge_attribute(
                    &"lorem".into(),
                    DataType::String,
                    AttributeType::Unstructured,
                    None
                )
                .is_ok()
        );
        assert_eq!(
            &DataType::Union((Box::new(DataType::Bool), Box::new(DataType::String))),
            schema
                .ungrouped()
                .edges()
                .get(&"lorem".into())
                .unwrap()
                .data_type()
        );

        schema
            .set_edge_attribute(
                &"lorem".into(),
                DataType::Bool,
                AttributeType::Categorical,
                Some(&"dolor".into()),
            )
            .unwrap();
        assert!(
            schema
                .update_edge_attribute(
                    &"lorem".into(),
                    DataType::String,
                    AttributeType::Unstructured,
                    Some(&"dolor".into())
                )
                .is_ok()
        );
        assert_eq!(
            &DataType::Union((Box::new(DataType::Bool), Box::new(DataType::String))),
            schema
                .group(&"dolor".into())
                .unwrap()
                .edges()
                .get(&"lorem".into())
                .unwrap()
                .data_type()
        );
    }

    #[test]
    fn test_schema_remove_node_attribute() {
        let mut schema = Schema::new_inferred(HashMap::new(), GroupSchema::default());
        schema
            .set_node_attribute(
                &"lorem".into(),
                DataType::Int,
                AttributeType::Continuous,
                None,
            )
            .unwrap();
        schema.remove_node_attribute(&"lorem".into(), None);
        assert!(!schema.ungrouped().nodes().contains_key(&"lorem".into()));

        schema
            .set_node_attribute(
                &"lorem".into(),
                DataType::Int,
                AttributeType::Continuous,
                Some(&"dolor".into()),
            )
            .unwrap();
        schema.remove_node_attribute(&"lorem".into(), Some(&"dolor".into()));
        assert!(
            !schema
                .group(&"dolor".into())
                .unwrap()
                .nodes()
                .contains_key(&"lorem".into())
        );
    }

    #[test]
    fn test_schema_remove_edge_attribute() {
        let mut schema = Schema::new_inferred(HashMap::new(), GroupSchema::default());
        schema
            .set_edge_attribute(
                &"lorem".into(),
                DataType::Bool,
                AttributeType::Categorical,
                None,
            )
            .unwrap();
        schema.remove_edge_attribute(&"lorem".into(), None);
        assert!(!schema.ungrouped().edges().contains_key(&"lorem".into()));

        schema
            .set_edge_attribute(
                &"lorem".into(),
                DataType::Bool,
                AttributeType::Categorical,
                Some(&"dolor".into()),
            )
            .unwrap();
        schema.remove_edge_attribute(&"lorem".into(), Some(&"dolor".into()));
        assert!(
            !schema
                .group(&"dolor".into())
                .unwrap()
                .edges()
                .contains_key(&"lorem".into())
        );
    }

    #[test]
    fn test_schema_add_group() {
        let attribute_schema = AttributeSchema::new(
            vec![
                (
                    "lorem".into(),
                    AttributeDataType::new(DataType::Int, AttributeType::Categorical).unwrap(),
                ),
                (
                    "ipsum".into(),
                    AttributeDataType::new(DataType::Float, AttributeType::Continuous).unwrap(),
                ),
            ]
            .into_iter()
            .collect(),
        );

        let mut schema = Schema::new_inferred(HashMap::new(), GroupSchema::default());
        schema
            .add_group(
                "dolor".into(),
                GroupSchema::new(attribute_schema.clone(), AttributeSchema::default()),
            )
            .unwrap();
        assert_eq!(
            attribute_schema,
            schema.group(&"dolor".into()).unwrap().nodes
        );

        assert!(
            schema
                .add_group("dolor".into(), GroupSchema::default())
                .is_err_and(|error| {
                    matches!(
                        error,
                        crate::errors::SchemaError::GroupAlreadyInSchema { .. }
                    )
                })
        );
    }

    #[test]
    fn test_schema_remove_group() {
        let mut schema = Schema::new_inferred(
            vec![("dolor".into(), GroupSchema::default())]
                .into_iter()
                .collect(),
            GroupSchema::default(),
        );
        schema.remove_group(&"dolor".into());
        assert!(!schema.groups().contains_key(&"dolor".into()));
    }

    #[test]
    fn test_schema_freeze() {
        let mut schema = Schema::new_inferred(HashMap::new(), GroupSchema::default());
        assert_eq!(&SchemaType::Inferred, schema.schema_type());

        schema.freeze();
        assert_eq!(&SchemaType::Provided, schema.schema_type());
    }

    #[test]
    fn test_schema_unfreeze() {
        let mut schema = Schema::new_provided(HashMap::new(), GroupSchema::default());
        assert_eq!(&SchemaType::Provided, schema.schema_type());

        schema.unfreeze();
        assert_eq!(&SchemaType::Inferred, schema.schema_type());
    }
}
