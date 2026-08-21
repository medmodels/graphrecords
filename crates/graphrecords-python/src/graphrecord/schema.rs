use super::{
    PyAttributeName, PyAttributes, PyGraphRecord, PyGroupIndex, PyNodeIndex,
    datatype::PyDataType,
    edge_index::PyEdgeIndex,
    errors::PyGraphRecordError,
    traits::{DeepFrom, DeepInto},
};
use graphrecords_core::{
    errors::SchemaError,
    graphrecord::{
        AttributeName,
        datatypes::DataType,
        schema::{
            AttributeDataType, AttributeSchema, AttributeType, GroupSchema, Schema, SchemaType,
        },
    },
};
use pyo3::{
    exceptions::PyTypeError,
    prelude::*,
    types::{PyBytes, PyBytesMethods, PyTuple},
};
use std::{collections::HashMap, hash::BuildHasher};

#[pyclass(frozen, eq, eq_int, hash, module = "graphrecords._graphrecords.schema")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

#[pyclass(frozen, module = "graphrecords._graphrecords.schema")]
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
    type Error = SchemaError;

    fn try_from(value: PyAttributeDataType) -> Result<Self, Self::Error> {
        Self::new(value.data_type.into(), value.attribute_type.into())
    }
}

impl DeepFrom<AttributeDataType> for PyAttributeDataType {
    fn deep_from(value: AttributeDataType) -> Self {
        value.into()
    }
}

impl<H: BuildHasher + Default> DeepFrom<&AttributeSchema>
    for HashMap<PyAttributeName, PyAttributeDataType, H>
{
    fn deep_from(value: &AttributeSchema) -> Self {
        let mapping: &HashMap<AttributeName, AttributeDataType> = value;

        mapping.deep_into()
    }
}

#[pymethods]
impl PyAttributeDataType {
    #[new]
    #[pyo3(signature = (data_type, attribute_type=None))]
    pub fn new(data_type: PyDataType, attribute_type: Option<PyAttributeType>) -> PyResult<Self> {
        let data_type = DataType::from(data_type);

        let attribute_data_type = match attribute_type {
            Some(attribute_type) => AttributeDataType::new(data_type, attribute_type.into())
                .map_err(PyGraphRecordError::from)?,
            None => AttributeDataType::from(data_type),
        };

        Ok(attribute_data_type.into())
    }

    #[getter]
    pub fn data_type(&self) -> PyDataType {
        self.data_type.clone()
    }

    #[getter]
    pub const fn attribute_type(&self) -> PyAttributeType {
        self.attribute_type
    }

    #[staticmethod]
    pub fn _from_bytes(data: &Bound<'_, PyBytes>) -> PyResult<Self> {
        let attribute_data_type: AttributeDataType = bincode::deserialize(data.as_bytes())
            .map_err(|_| {
                PyGraphRecordError::Conversion(
                    "Failed to deserialize AttributeDataType".to_string(),
                )
            })?;

        Ok(attribute_data_type.into())
    }

    pub fn __reduce__<'py>(&self, py: Python<'py>) -> PyResult<(Py<PyAny>, Bound<'py, PyTuple>)> {
        let attribute_data_type = AttributeDataType::from((
            DataType::from(self.data_type.clone()),
            AttributeType::from(self.attribute_type),
        ));
        let bytes = bincode::serialize(&attribute_data_type).map_err(|_| {
            PyGraphRecordError::Conversion("Failed to serialize AttributeDataType".to_string())
        })?;
        let constructor = py.get_type::<Self>().getattr("_from_bytes")?.unbind();
        let arguments = (PyBytes::new(py, &bytes),).into_pyobject(py)?;

        Ok((constructor, arguments))
    }
}

#[pyclass(frozen, eq, module = "graphrecords._graphrecords.schema")]
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq)]
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
        nodes: HashMap<PyAttributeName, PyAttributeDataType>,
        edges: HashMap<PyAttributeName, PyAttributeDataType>,
    ) -> PyResult<Self> {
        let nodes = nodes
            .into_iter()
            .map(|(attribute_name, attribute_data_type)| {
                Ok((attribute_name.into(), attribute_data_type.try_into()?))
            })
            .collect::<Result<HashMap<_, _>, SchemaError>>()
            .map_err(PyGraphRecordError::from)?
            .into();
        let edges = edges
            .into_iter()
            .map(|(attribute_name, attribute_data_type)| {
                Ok((attribute_name.into(), attribute_data_type.try_into()?))
            })
            .collect::<Result<HashMap<_, _>, SchemaError>>()
            .map_err(PyGraphRecordError::from)?
            .into();

        Ok(Self(GroupSchema::new(nodes, edges)))
    }

    #[getter]
    pub fn nodes(&self) -> HashMap<PyAttributeName, PyAttributeDataType> {
        self.0.nodes().deep_into()
    }

    #[getter]
    pub fn edges(&self) -> HashMap<PyAttributeName, PyAttributeDataType> {
        self.0.edges().deep_into()
    }

    pub fn validate_node(&self, node_index: PyNodeIndex, attributes: PyAttributes) -> PyResult<()> {
        Ok(self
            .0
            .validate_node(&node_index.into(), &attributes.deep_into())
            .map_err(PyGraphRecordError::from)?)
    }

    pub fn validate_edge(&self, edge_index: PyEdgeIndex, attributes: PyAttributes) -> PyResult<()> {
        Ok(self
            .0
            .validate_edge(&edge_index, &attributes.deep_into())
            .map_err(PyGraphRecordError::from)?)
    }

    #[staticmethod]
    pub fn _from_bytes(data: &Bound<'_, PyBytes>) -> PyResult<Self> {
        let group_schema: GroupSchema = bincode::deserialize(data.as_bytes()).map_err(|_| {
            PyGraphRecordError::Conversion("Failed to deserialize GroupSchema".to_string())
        })?;

        Ok(Self(group_schema))
    }

    pub fn __reduce__<'py>(&self, py: Python<'py>) -> PyResult<(Py<PyAny>, Bound<'py, PyTuple>)> {
        let bytes = bincode::serialize(&self.0).map_err(|_| {
            PyGraphRecordError::Conversion("Failed to serialize GroupSchema".to_string())
        })?;
        let constructor = py.get_type::<Self>().getattr("_from_bytes")?.unbind();
        let arguments = (PyBytes::new(py, &bytes),).into_pyobject(py)?;

        Ok((constructor, arguments))
    }

    pub fn __hash__(&self) -> PyResult<isize> {
        Err(PyTypeError::new_err("unhashable type: 'GroupSchema'"))
    }

    pub fn __repr__(&self) -> String {
        format!("{:?}", self.0)
    }
}

#[pyclass(frozen, eq, eq_int, hash, module = "graphrecords._graphrecords.schema")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

#[pyclass(frozen, eq, module = "graphrecords._graphrecords.schema")]
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PySchema(Schema);

impl From<Schema> for PySchema {
    fn from(value: Schema) -> Self {
        Self(value)
    }
}

impl From<PySchema> for Schema {
    fn from(value: PySchema) -> Self {
        value.0
    }
}

#[pymethods]
impl PySchema {
    #[new]
    #[pyo3(signature = (groups, ungrouped, schema_type=PySchemaType::Provided))]
    pub fn new(
        groups: HashMap<PyGroupIndex, PyGroupSchema>,
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
    pub fn infer(graphrecord: &PyGraphRecord) -> Self {
        Schema::infer(graphrecord.record()).into()
    }

    #[getter]
    pub fn groups(&self) -> HashMap<PyGroupIndex, PyGroupSchema> {
        self.0.groups().deep_into()
    }

    pub fn group(&self, group_index: PyGroupIndex) -> PyResult<PyGroupSchema> {
        Ok(self
            .0
            .group(&group_index.into())
            .map(|group_schema| group_schema.clone().into())
            .map_err(PyGraphRecordError::from)?)
    }

    #[getter]
    pub fn ungrouped(&self) -> PyGroupSchema {
        self.0.ungrouped().clone().into()
    }

    #[getter]
    pub fn schema_type(&self) -> PySchemaType {
        self.0.schema_type().clone().into()
    }

    #[pyo3(signature = (node_index, attributes, group_index=None))]
    pub fn validate_node(
        &self,
        node_index: PyNodeIndex,
        attributes: PyAttributes,
        group_index: Option<PyGroupIndex>,
    ) -> PyResult<()> {
        Ok(self
            .0
            .validate_node(
                &node_index.into(),
                &attributes.deep_into(),
                group_index.map(Into::into).as_ref(),
            )
            .map_err(PyGraphRecordError::from)?)
    }

    #[pyo3(signature = (edge_index, attributes, group_index=None))]
    pub fn validate_edge(
        &self,
        edge_index: PyEdgeIndex,
        attributes: PyAttributes,
        group_index: Option<PyGroupIndex>,
    ) -> PyResult<()> {
        Ok(self
            .0
            .validate_edge(
                &edge_index,
                &attributes.deep_into(),
                group_index.map(Into::into).as_ref(),
            )
            .map_err(PyGraphRecordError::from)?)
    }

    #[pyo3(signature = (attribute_name, data_type, attribute_type, group_index=None))]
    pub fn set_node_attribute(
        &self,
        attribute_name: PyAttributeName,
        data_type: PyDataType,
        attribute_type: PyAttributeType,
        group_index: Option<PyGroupIndex>,
    ) -> PyResult<Self> {
        let mut schema = self.0.clone();

        schema
            .set_node_attribute(
                &attribute_name.into(),
                data_type.into(),
                attribute_type.into(),
                group_index.map(Into::into).as_ref(),
            )
            .map_err(PyGraphRecordError::from)?;

        Ok(schema.into())
    }

    #[pyo3(signature = (attribute_name, data_type, attribute_type, group_index=None))]
    pub fn set_edge_attribute(
        &self,
        attribute_name: PyAttributeName,
        data_type: PyDataType,
        attribute_type: PyAttributeType,
        group_index: Option<PyGroupIndex>,
    ) -> PyResult<Self> {
        let mut schema = self.0.clone();

        schema
            .set_edge_attribute(
                &attribute_name.into(),
                data_type.into(),
                attribute_type.into(),
                group_index.map(Into::into).as_ref(),
            )
            .map_err(PyGraphRecordError::from)?;

        Ok(schema.into())
    }

    #[pyo3(signature = (attribute_name, data_type, attribute_type, group_index=None))]
    pub fn update_node_attribute(
        &self,
        attribute_name: PyAttributeName,
        data_type: PyDataType,
        attribute_type: PyAttributeType,
        group_index: Option<PyGroupIndex>,
    ) -> PyResult<Self> {
        let mut schema = self.0.clone();

        schema
            .update_node_attribute(
                &attribute_name.into(),
                data_type.into(),
                attribute_type.into(),
                group_index.map(Into::into).as_ref(),
            )
            .map_err(PyGraphRecordError::from)?;

        Ok(schema.into())
    }

    #[pyo3(signature = (attribute_name, data_type, attribute_type, group_index=None))]
    pub fn update_edge_attribute(
        &self,
        attribute_name: PyAttributeName,
        data_type: PyDataType,
        attribute_type: PyAttributeType,
        group_index: Option<PyGroupIndex>,
    ) -> PyResult<Self> {
        let mut schema = self.0.clone();

        schema
            .update_edge_attribute(
                &attribute_name.into(),
                data_type.into(),
                attribute_type.into(),
                group_index.map(Into::into).as_ref(),
            )
            .map_err(PyGraphRecordError::from)?;

        Ok(schema.into())
    }

    #[pyo3(signature = (attribute_name, group_index=None))]
    pub fn remove_node_attribute(
        &self,
        attribute_name: PyAttributeName,
        group_index: Option<PyGroupIndex>,
    ) -> Self {
        let mut schema = self.0.clone();

        schema.remove_node_attribute(&attribute_name.into(), group_index.map(Into::into).as_ref());

        schema.into()
    }

    #[pyo3(signature = (attribute_name, group_index=None))]
    pub fn remove_edge_attribute(
        &self,
        attribute_name: PyAttributeName,
        group_index: Option<PyGroupIndex>,
    ) -> Self {
        let mut schema = self.0.clone();

        schema.remove_edge_attribute(&attribute_name.into(), group_index.map(Into::into).as_ref());

        schema.into()
    }

    pub fn add_group(
        &self,
        group_index: PyGroupIndex,
        group_schema: PyGroupSchema,
    ) -> PyResult<Self> {
        let mut schema = self.0.clone();

        schema
            .add_group(group_index.into(), group_schema.into())
            .map_err(PyGraphRecordError::from)?;

        Ok(schema.into())
    }

    pub fn remove_group(&self, group_index: PyGroupIndex) -> Self {
        let mut schema = self.0.clone();

        schema.remove_group(&group_index.into());

        schema.into()
    }

    pub fn freeze(&self) -> Self {
        let mut schema = self.0.clone();

        schema.freeze();

        schema.into()
    }

    pub fn unfreeze(&self) -> Self {
        let mut schema = self.0.clone();

        schema.unfreeze();

        schema.into()
    }

    #[staticmethod]
    pub fn _from_bytes(data: &Bound<'_, PyBytes>) -> PyResult<Self> {
        let schema: Schema = bincode::deserialize(data.as_bytes()).map_err(|_| {
            PyGraphRecordError::Conversion("Failed to deserialize Schema".to_string())
        })?;

        Ok(Self(schema))
    }

    pub fn __reduce__<'py>(&self, py: Python<'py>) -> PyResult<(Py<PyAny>, Bound<'py, PyTuple>)> {
        let bytes = bincode::serialize(&self.0).map_err(|_| {
            PyGraphRecordError::Conversion("Failed to serialize Schema".to_string())
        })?;
        let constructor = py.get_type::<Self>().getattr("_from_bytes")?.unbind();
        let arguments = (PyBytes::new(py, &bytes),).into_pyobject(py)?;

        Ok((constructor, arguments))
    }

    pub fn __hash__(&self) -> PyResult<isize> {
        Err(PyTypeError::new_err("unhashable type: 'Schema'"))
    }

    pub fn __repr__(&self) -> String {
        format!("{:?}", self.0)
    }
}
