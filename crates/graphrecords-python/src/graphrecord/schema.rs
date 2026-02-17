use super::{
    PyAttributes, PyGraphRecord, PyGroup, PyNodeIndex,
    attribute::PyGraphRecordAttribute,
    datatype::PyDataType,
    errors::PyGraphRecordError,
    traits::{DeepFrom, DeepInto},
};
use graphrecords_core::{
    errors::GraphError,
    graphrecord::{
        EdgeIndex, Group,
        schema::{AttributeDataType, AttributeType, GroupSchema, Schema, SchemaType},
    },
};
use parking_lot::RwLock;
use pyo3::prelude::*;
use std::collections::HashMap;

#[pyclass(frozen, eq, eq_int)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PyAttributeType {
    Categorical = 0,
    Continuous = 1,
    Temporal = 2,
    Unstructured = 3,
}

impl From<AttributeType> for PyAttributeType {
    fn from(value: AttributeType) -> Self {
        match value {
            AttributeType::Categorical => Self::Categorical,
            AttributeType::Continuous => Self::Continuous,
            AttributeType::Temporal => Self::Temporal,
            AttributeType::Unstructured => Self::Unstructured,
        }
    }
}

impl From<PyAttributeType> for AttributeType {
    fn from(value: PyAttributeType) -> Self {
        match value {
            PyAttributeType::Categorical => Self::Categorical,
            PyAttributeType::Continuous => Self::Continuous,
            PyAttributeType::Temporal => Self::Temporal,
            PyAttributeType::Unstructured => Self::Unstructured,
        }
    }
}

#[pymethods]
impl PyAttributeType {
    #[staticmethod]
    pub fn infer(data_type: PyDataType) -> Self {
        AttributeType::infer(&data_type.into()).into()
    }
}

#[pyclass(frozen)]
#[derive(Debug, Clone)]
pub struct PyAttributeDataType {
    data_type: PyDataType,
    attribute_type: PyAttributeType,
}

impl From<AttributeDataType> for PyAttributeDataType {
    fn from(value: AttributeDataType) -> Self {
        Self {
            data_type: value.data_type().clone().into(),
            attribute_type: (*value.attribute_type()).into(),
        }
    }
}

impl TryFrom<PyAttributeDataType> for AttributeDataType {
    type Error = GraphError;

    fn try_from(value: PyAttributeDataType) -> Result<Self, Self::Error> {
        Self::new(value.data_type.into(), value.attribute_type.into())
    }
}

impl DeepFrom<AttributeDataType> for PyAttributeDataType {
    fn deep_from(value: AttributeDataType) -> Self {
        value.into()
    }
}

#[pymethods]
impl PyAttributeDataType {
    #[new]
    #[pyo3(signature = (data_type, attribute_type))]
    pub const fn new(data_type: PyDataType, attribute_type: PyAttributeType) -> Self {
        Self {
            data_type,
            attribute_type,
        }
    }

    #[getter]
    pub fn data_type(&self) -> PyDataType {
        self.data_type.clone()
    }

    #[getter]
    pub fn attribute_type(&self) -> PyAttributeType {
        self.attribute_type.clone()
    }
}

#[pyclass(frozen)]
#[repr(transparent)]
#[derive(Debug, Clone)]
pub struct PyGroupSchema(GroupSchema);

impl From<GroupSchema> for PyGroupSchema {
    fn from(value: GroupSchema) -> Self {
        Self(value)
    }
}

impl From<PyGroupSchema> for GroupSchema {
    fn from(value: PyGroupSchema) -> Self {
        value.0
    }
}

impl DeepFrom<GroupSchema> for PyGroupSchema {
    fn deep_from(value: GroupSchema) -> Self {
        value.into()
    }
}

impl DeepFrom<PyGroupSchema> for GroupSchema {
    fn deep_from(value: PyGroupSchema) -> Self {
        value.into()
    }
}

#[pymethods]
impl PyGroupSchema {
    #[new]
    pub fn new(
        nodes: HashMap<PyGraphRecordAttribute, PyAttributeDataType>,
        edges: HashMap<PyGraphRecordAttribute, PyAttributeDataType>,
    ) -> PyResult<Self> {
        let nodes = nodes
            .into_iter()
            .map(|(k, v)| Ok((k.into(), v.try_into()?)))
            .collect::<Result<HashMap<_, _>, GraphError>>()
            .map_err(PyGraphRecordError::from)?
            .into();
        let edges = edges
            .into_iter()
            .map(|(k, v)| Ok((k.into(), v.try_into()?)))
            .collect::<Result<HashMap<_, _>, GraphError>>()
            .map_err(PyGraphRecordError::from)?
            .into();

        Ok(Self(GroupSchema::new(nodes, edges)))
    }

    #[getter]
    pub fn nodes(&self) -> HashMap<PyGraphRecordAttribute, PyAttributeDataType> {
        self.0.nodes().clone().deep_into()
    }

    #[getter]
    pub fn edges(&self) -> HashMap<PyGraphRecordAttribute, PyAttributeDataType> {
        self.0.edges().clone().deep_into()
    }

    pub fn validate_node(&self, index: PyNodeIndex, attributes: PyAttributes) -> PyResult<()> {
        Ok(self
            .0
            .validate_node(&index.into(), &attributes.deep_into())
            .map_err(PyGraphRecordError::from)?)
    }

    pub fn validate_edge(&self, index: EdgeIndex, attributes: PyAttributes) -> PyResult<()> {
        Ok(self
            .0
            .validate_edge(&index, &attributes.deep_into())
            .map_err(PyGraphRecordError::from)?)
    }
}

#[pyclass(frozen, eq, eq_int)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PySchemaType {
    Provided = 0,
    Inferred = 1,
}

impl From<SchemaType> for PySchemaType {
    fn from(value: SchemaType) -> Self {
        match value {
            SchemaType::Provided => Self::Provided,
            SchemaType::Inferred => Self::Inferred,
        }
    }
}

impl From<PySchemaType> for SchemaType {
    fn from(value: PySchemaType) -> Self {
        match value {
            PySchemaType::Provided => Self::Provided,
            PySchemaType::Inferred => Self::Inferred,
        }
    }
}

#[pyclass(frozen)]
#[repr(transparent)]
#[derive(Debug)]
pub struct PySchema(RwLock<Schema>);

impl From<Schema> for PySchema {
    fn from(value: Schema) -> Self {
        Self(RwLock::new(value))
    }
}

impl From<PySchema> for Schema {
    fn from(value: PySchema) -> Self {
        value.0.into_inner()
    }
}

impl Clone for PySchema {
    fn clone(&self) -> Self {
        Self(RwLock::new(self.0.read().clone()))
    }
}

#[pymethods]
impl PySchema {
    #[new]
    #[pyo3(signature = (groups, ungrouped, schema_type=PySchemaType::Provided))]
    pub fn new(
        groups: HashMap<PyGroup, PyGroupSchema>,
        ungrouped: PyGroupSchema,
        schema_type: PySchemaType,
    ) -> Self {
        match schema_type {
            PySchemaType::Provided => {
                Schema::new_provided(groups.deep_into(), ungrouped.deep_into()).into()
            }
            PySchemaType::Inferred => {
                Schema::new_inferred(groups.deep_into(), ungrouped.deep_into()).into()
            }
        }
    }

    #[staticmethod]
    pub fn infer(graphrecord: Bound<'_, PyGraphRecord>) -> PyResult<Self> {
        let graphrecord = graphrecord.get();

        Ok(Schema::infer(&*graphrecord.inner()?).into())
    }

    #[getter]
    pub fn groups(&self) -> Vec<PyGroup> {
        self.0
            .read()
            .groups()
            .keys()
            .cloned()
            .collect::<Vec<Group>>()
            .deep_into()
    }

    pub fn group(&self, group: PyGroup) -> PyResult<PyGroupSchema> {
        Ok(self
            .0
            .read()
            .group(&group.into())
            .map(|g| g.clone().into())
            .map_err(PyGraphRecordError::from)?)
    }

    #[getter]
    pub fn ungrouped(&self) -> PyGroupSchema {
        self.0.read().ungrouped().clone().into()
    }

    #[getter]
    pub fn schema_type(&self) -> PySchemaType {
        self.0.read().schema_type().clone().into()
    }

    #[pyo3(signature = (index, attributes, group=None))]
    pub fn validate_node(
        &self,
        index: PyNodeIndex,
        attributes: PyAttributes,
        group: Option<PyGroup>,
    ) -> PyResult<()> {
        Ok(self
            .0
            .read()
            .validate_node(
                &index.into(),
                &attributes.deep_into(),
                group.map(std::convert::Into::into).as_ref(),
            )
            .map_err(PyGraphRecordError::from)?)
    }

    #[pyo3(signature = (index, attributes, group=None))]
    pub fn validate_edge(
        &self,
        index: EdgeIndex,
        attributes: PyAttributes,
        group: Option<PyGroup>,
    ) -> PyResult<()> {
        Ok(self
            .0
            .read()
            .validate_edge(
                &index,
                &attributes.deep_into(),
                group.map(std::convert::Into::into).as_ref(),
            )
            .map_err(PyGraphRecordError::from)?)
    }

    #[pyo3(signature = (attribute, data_type, attribute_type, group=None))]
    pub fn set_node_attribute(
        &self,
        attribute: PyGraphRecordAttribute,
        data_type: PyDataType,
        attribute_type: PyAttributeType,
        group: Option<PyGroup>,
    ) -> PyResult<()> {
        Ok(self
            .0
            .write()
            .set_node_attribute(
                &attribute.into(),
                data_type.into(),
                attribute_type.into(),
                group.map(std::convert::Into::into).as_ref(),
            )
            .map_err(PyGraphRecordError::from)?)
    }

    #[pyo3(signature = (attribute, data_type, attribute_type, group=None))]
    pub fn set_edge_attribute(
        &self,
        attribute: PyGraphRecordAttribute,
        data_type: PyDataType,
        attribute_type: PyAttributeType,
        group: Option<PyGroup>,
    ) -> PyResult<()> {
        Ok(self
            .0
            .write()
            .set_edge_attribute(
                &attribute.into(),
                data_type.into(),
                attribute_type.into(),
                group.map(std::convert::Into::into).as_ref(),
            )
            .map_err(PyGraphRecordError::from)?)
    }

    #[pyo3(signature = (attribute, data_type, attribute_type, group=None))]
    pub fn update_node_attribute(
        &self,
        attribute: PyGraphRecordAttribute,
        data_type: PyDataType,
        attribute_type: PyAttributeType,
        group: Option<PyGroup>,
    ) -> PyResult<()> {
        Ok(self
            .0
            .write()
            .update_node_attribute(
                &attribute.into(),
                data_type.into(),
                attribute_type.into(),
                group.map(std::convert::Into::into).as_ref(),
            )
            .map_err(PyGraphRecordError::from)?)
    }

    #[pyo3(signature = (attribute, data_type, attribute_type, group=None))]
    pub fn update_edge_attribute(
        &self,
        attribute: PyGraphRecordAttribute,
        data_type: PyDataType,
        attribute_type: PyAttributeType,
        group: Option<PyGroup>,
    ) -> PyResult<()> {
        Ok(self
            .0
            .write()
            .update_edge_attribute(
                &attribute.into(),
                data_type.into(),
                attribute_type.into(),
                group.map(std::convert::Into::into).as_ref(),
            )
            .map_err(PyGraphRecordError::from)?)
    }

    #[pyo3(signature = (attribute, group=None))]
    pub fn remove_node_attribute(&self, attribute: PyGraphRecordAttribute, group: Option<PyGroup>) {
        self.0.write().remove_node_attribute(
            &attribute.into(),
            group.map(std::convert::Into::into).as_ref(),
        );
    }

    #[pyo3(signature = (attribute, group=None))]
    pub fn remove_edge_attribute(&self, attribute: PyGraphRecordAttribute, group: Option<PyGroup>) {
        self.0.write().remove_edge_attribute(
            &attribute.into(),
            group.map(std::convert::Into::into).as_ref(),
        );
    }

    pub fn add_group(&self, group: PyGroup, schema: PyGroupSchema) -> PyResult<()> {
        Ok(self
            .0
            .write()
            .add_group(group.into(), schema.into())
            .map_err(PyGraphRecordError::from)?)
    }

    pub fn remove_group(&self, group: PyGroup) {
        self.0.write().remove_group(&group.into());
    }

    pub fn freeze(&self) {
        self.0.write().freeze();
    }

    pub fn unfreeze(&self) {
        self.0.write().unfreeze();
    }
}
