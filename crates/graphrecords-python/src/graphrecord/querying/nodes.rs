use super::{
    PyGraphRecordAttributeCardinalityWrapper, PyGroupCardinalityWrapper,
    attributes::PyNodeAttributesTreeOperand, edges::PyEdgeOperand,
};
use crate::graphrecord::{
    PyNodeIndex,
    attribute::PyGraphRecordAttribute,
    errors::PyGraphRecordError,
    querying::{
        attributes::PyNodeAttributesTreeGroupOperand,
        edges::PyEdgeGroupOperand,
        values::{PyNodeMultipleValuesWithIndexGroupOperand, PyNodeMultipleValuesWithIndexOperand},
    },
};
use graphrecords_core::{
    errors::GraphRecordError,
    graphrecord::{
        NodeIndex,
        querying::{
            DeepClone,
            group_by::GroupOperand,
            nodes::{
                self, EdgeDirection, NodeIndexComparisonOperand, NodeIndexOperand,
                NodeIndicesComparisonOperand, NodeIndicesOperand, NodeOperand,
            },
            wrapper::Wrapper,
        },
    },
};
use pyo3::{
    Borrowed, Bound, FromPyObject, PyAny, PyErr, PyResult, pyclass, pymethods,
    types::{PyAnyMethods, PyFunction},
};
use std::ops::Deref;

#[pyclass(frozen, eq, eq_int)]
#[derive(Clone, PartialEq, Eq)]
pub enum PyEdgeDirection {
    Incoming = 0,
    Outgoing = 1,
    Both = 2,
}

impl From<EdgeDirection> for PyEdgeDirection {
    fn from(value: EdgeDirection) -> Self {
        match value {
            EdgeDirection::Incoming => Self::Incoming,
            EdgeDirection::Outgoing => Self::Outgoing,
            EdgeDirection::Both => Self::Both,
        }
    }
}

impl From<PyEdgeDirection> for EdgeDirection {
    fn from(value: PyEdgeDirection) -> Self {
        match value {
            PyEdgeDirection::Incoming => Self::Incoming,
            PyEdgeDirection::Outgoing => Self::Outgoing,
            PyEdgeDirection::Both => Self::Both,
        }
    }
}

#[pyclass(frozen)]
#[derive(Clone)]
pub enum NodeOperandGroupDiscriminator {
    Attribute(PyGraphRecordAttribute),
}

impl From<NodeOperandGroupDiscriminator> for nodes::NodeOperandGroupDiscriminator {
    fn from(value: NodeOperandGroupDiscriminator) -> Self {
        match value {
            NodeOperandGroupDiscriminator::Attribute(attribute) => {
                Self::Attribute(attribute.into())
            }
        }
    }
}

#[pyclass(frozen)]
#[repr(transparent)]
pub struct PyNodeOperand(Wrapper<NodeOperand>);

impl From<Wrapper<NodeOperand>> for PyNodeOperand {
    fn from(operand: Wrapper<NodeOperand>) -> Self {
        Self(operand)
    }
}

impl From<PyNodeOperand> for Wrapper<NodeOperand> {
    fn from(operand: PyNodeOperand) -> Self {
        operand.0
    }
}

#[pymethods]
impl PyNodeOperand {
    pub fn attribute(
        &self,
        attribute: PyGraphRecordAttribute,
    ) -> PyNodeMultipleValuesWithIndexOperand {
        self.0.attribute(attribute).into()
    }

    pub fn attributes(&self) -> PyNodeAttributesTreeOperand {
        self.0.attributes().into()
    }

    pub fn index(&self) -> PyNodeIndicesOperand {
        self.0.index().into()
    }

    pub fn in_group(&self, group: PyGroupCardinalityWrapper) {
        self.0.in_group(group);
    }

    pub fn has_attribute(&self, attribute: PyGraphRecordAttributeCardinalityWrapper) {
        self.0.has_attribute(attribute);
    }

    pub fn edges(&self, direction: PyEdgeDirection) -> PyEdgeOperand {
        self.0.edges(direction.into()).into()
    }

    pub fn neighbors(&self, direction: PyEdgeDirection) -> Self {
        self.0.neighbors(direction.into()).into()
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn either_or(&self, either: &Bound<'_, PyFunction>, or: &Bound<'_, PyFunction>) {
        self.0.either_or(
            |operand| {
                either
                    .call1((Self::from(operand.clone()),))
                    .expect("Call must succeed");
            },
            |operand| {
                or.call1((Self::from(operand.clone()),))
                    .expect("Call must succeed");
            },
        );
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn exclude(&self, query: &Bound<'_, PyFunction>) {
        self.0.exclude(|operand| {
            query
                .call1((Self::from(operand.clone()),))
                .expect("Call must succeed");
        });
    }

    pub fn group_by(&self, discriminator: NodeOperandGroupDiscriminator) -> PyNodeGroupOperand {
        self.0.group_by(discriminator.into()).into()
    }

    pub fn deep_clone(&self) -> Self {
        self.0.deep_clone().into()
    }
}

#[pyclass(frozen)]
#[repr(transparent)]
pub struct PyNodeGroupOperand(Wrapper<GroupOperand<NodeOperand>>);

impl From<Wrapper<GroupOperand<NodeOperand>>> for PyNodeGroupOperand {
    fn from(operand: Wrapper<GroupOperand<NodeOperand>>) -> Self {
        Self(operand)
    }
}

impl From<PyNodeGroupOperand> for Wrapper<GroupOperand<NodeOperand>> {
    fn from(operand: PyNodeGroupOperand) -> Self {
        operand.0
    }
}

#[pymethods]
impl PyNodeGroupOperand {
    pub fn attribute(
        &self,
        attribute: PyGraphRecordAttribute,
    ) -> PyNodeMultipleValuesWithIndexGroupOperand {
        self.0.attribute(attribute).into()
    }

    pub fn attributes(&self) -> PyNodeAttributesTreeGroupOperand {
        self.0.attributes().into()
    }

    pub fn index(&self) -> PyNodeIndicesGroupOperand {
        self.0.index().into()
    }

    pub fn in_group(&self, group: PyGroupCardinalityWrapper) {
        self.0.in_group(group);
    }

    pub fn has_attribute(&self, attribute: PyGraphRecordAttributeCardinalityWrapper) {
        self.0.has_attribute(attribute);
    }

    pub fn edges(&self, direction: PyEdgeDirection) -> PyEdgeGroupOperand {
        self.0.edges(direction.into()).into()
    }

    pub fn neighbors(&self, direction: PyEdgeDirection) -> Self {
        self.0.neighbors(direction.into()).into()
    }
    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn either_or(&self, either: &Bound<'_, PyFunction>, or: &Bound<'_, PyFunction>) {
        self.0.either_or(
            |operand| {
                either
                    .call1((PyNodeOperand::from(operand.clone()),))
                    .expect("Call must succeed");
            },
            |operand| {
                or.call1((PyNodeOperand::from(operand.clone()),))
                    .expect("Call must succeed");
            },
        );
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn exclude(&self, query: &Bound<'_, PyFunction>) {
        self.0.exclude(|operand| {
            query
                .call1((PyNodeOperand::from(operand.clone()),))
                .expect("Call must succeed");
        });
    }

    pub fn deep_clone(&self) -> Self {
        self.0.deep_clone().into()
    }
}

#[repr(transparent)]
pub struct PyNodeIndexComparisonOperand(NodeIndexComparisonOperand);

impl From<NodeIndexComparisonOperand> for PyNodeIndexComparisonOperand {
    fn from(operand: NodeIndexComparisonOperand) -> Self {
        Self(operand)
    }
}

impl From<PyNodeIndexComparisonOperand> for NodeIndexComparisonOperand {
    fn from(operand: PyNodeIndexComparisonOperand) -> Self {
        operand.0
    }
}

impl FromPyObject<'_, '_> for PyNodeIndexComparisonOperand {
    type Error = PyErr;

    fn extract(ob: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        match ob.extract::<PyNodeIndex>() {
            Ok(index) => Ok(NodeIndexComparisonOperand::Index(NodeIndex::from(index)).into()),
            _ => match ob.extract::<PyNodeIndexOperand>() {
                Ok(operand) => Ok(Self(operand.0.into())),
                _ => Err(
                    PyGraphRecordError::from(GraphRecordError::ConversionError(format!(
                        "Failed to convert {} into NodeIndex or NodeIndexOperand",
                        ob.to_owned()
                    )))
                    .into(),
                ),
            },
        }
    }
}

#[repr(transparent)]
pub struct PyNodeIndicesComparisonOperand(NodeIndicesComparisonOperand);

impl From<NodeIndicesComparisonOperand> for PyNodeIndicesComparisonOperand {
    fn from(operand: NodeIndicesComparisonOperand) -> Self {
        Self(operand)
    }
}

impl From<PyNodeIndicesComparisonOperand> for NodeIndicesComparisonOperand {
    fn from(operand: PyNodeIndicesComparisonOperand) -> Self {
        operand.0
    }
}

impl FromPyObject<'_, '_> for PyNodeIndicesComparisonOperand {
    type Error = PyErr;

    fn extract(ob: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        match ob.extract::<Vec<PyNodeIndex>>() {
            Ok(indices) => Ok(NodeIndicesComparisonOperand::Indices(
                indices.into_iter().map(NodeIndex::from).collect(),
            )
            .into()),
            _ => match ob.extract::<PyNodeIndicesOperand>() {
                Ok(operand) => Ok(Self(operand.0.into())),
                _ => Err(
                    PyGraphRecordError::from(GraphRecordError::ConversionError(format!(
                        "Failed to convert {} into List[NodeIndex] or NodeIndicesOperand",
                        ob.to_owned()
                    )))
                    .into(),
                ),
            },
        }
    }
}

#[pyclass(frozen)]
#[repr(transparent)]
#[derive(Clone)]
pub struct PyNodeIndicesOperand(Wrapper<NodeIndicesOperand>);

impl From<Wrapper<NodeIndicesOperand>> for PyNodeIndicesOperand {
    fn from(operand: Wrapper<NodeIndicesOperand>) -> Self {
        Self(operand)
    }
}

impl From<PyNodeIndicesOperand> for Wrapper<NodeIndicesOperand> {
    fn from(operand: PyNodeIndicesOperand) -> Self {
        operand.0
    }
}

impl Deref for PyNodeIndicesOperand {
    type Target = Wrapper<NodeIndicesOperand>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[pymethods]
impl PyNodeIndicesOperand {
    pub fn max(&self) -> PyNodeIndexOperand {
        self.0.max().into()
    }

    pub fn min(&self) -> PyNodeIndexOperand {
        self.0.min().into()
    }

    pub fn count(&self) -> PyNodeIndexOperand {
        self.0.count().into()
    }

    pub fn sum(&self) -> PyNodeIndexOperand {
        self.0.sum().into()
    }

    pub fn random(&self) -> PyNodeIndexOperand {
        self.0.random().into()
    }

    pub fn greater_than(&self, index: PyNodeIndexComparisonOperand) {
        self.0.greater_than(index);
    }

    pub fn greater_than_or_equal_to(&self, index: PyNodeIndexComparisonOperand) {
        self.0.greater_than_or_equal_to(index);
    }

    pub fn less_than(&self, index: PyNodeIndexComparisonOperand) {
        self.0.less_than(index);
    }

    pub fn less_than_or_equal_to(&self, index: PyNodeIndexComparisonOperand) {
        self.0.less_than_or_equal_to(index);
    }

    pub fn equal_to(&self, index: PyNodeIndexComparisonOperand) {
        self.0.equal_to(index);
    }

    pub fn not_equal_to(&self, index: PyNodeIndexComparisonOperand) {
        self.0.not_equal_to(index);
    }

    pub fn starts_with(&self, index: PyNodeIndexComparisonOperand) {
        self.0.starts_with(index);
    }

    pub fn ends_with(&self, index: PyNodeIndexComparisonOperand) {
        self.0.ends_with(index);
    }

    pub fn contains(&self, index: PyNodeIndexComparisonOperand) {
        self.0.contains(index);
    }

    pub fn is_in(&self, indices: PyNodeIndicesComparisonOperand) {
        self.0.is_in(indices);
    }

    pub fn is_not_in(&self, indices: PyNodeIndicesComparisonOperand) {
        self.0.is_not_in(indices);
    }

    pub fn add(&self, index: PyNodeIndexComparisonOperand) {
        self.0.add(index);
    }

    pub fn sub(&self, index: PyNodeIndexComparisonOperand) {
        self.0.sub(index);
    }

    pub fn mul(&self, index: PyNodeIndexComparisonOperand) {
        self.0.mul(index);
    }

    pub fn pow(&self, index: PyNodeIndexComparisonOperand) {
        self.0.pow(index);
    }

    pub fn r#mod(&self, index: PyNodeIndexComparisonOperand) {
        self.0.r#mod(index);
    }

    pub fn abs(&self) {
        self.0.abs();
    }

    pub fn trim(&self) {
        self.0.trim();
    }

    pub fn trim_start(&self) {
        self.0.trim_start();
    }

    pub fn trim_end(&self) {
        self.0.trim_end();
    }

    pub fn lowercase(&self) {
        self.0.lowercase();
    }

    pub fn uppercase(&self) {
        self.0.uppercase();
    }

    pub fn slice(&self, start: usize, end: usize) {
        self.0.slice(start, end);
    }

    pub fn is_string(&self) {
        self.0.is_string();
    }

    pub fn is_int(&self) {
        self.0.is_int();
    }

    pub fn is_max(&self) {
        self.0.is_max();
    }

    pub fn is_min(&self) {
        self.0.is_min();
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn either_or(&self, either: &Bound<'_, PyFunction>, or: &Bound<'_, PyFunction>) {
        self.0.either_or(
            |operand| {
                either
                    .call1((Self::from(operand.clone()),))
                    .expect("Call must succeed");
            },
            |operand| {
                or.call1((Self::from(operand.clone()),))
                    .expect("Call must succeed");
            },
        );
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn exclude(&self, query: &Bound<'_, PyFunction>) {
        self.0.exclude(|operand| {
            query
                .call1((Self::from(operand.clone()),))
                .expect("Call must succeed");
        });
    }

    pub fn deep_clone(&self) -> Self {
        self.0.deep_clone().into()
    }
}

#[pyclass(frozen)]
#[repr(transparent)]
#[derive(Clone)]
pub struct PyNodeIndicesGroupOperand(Wrapper<GroupOperand<NodeIndicesOperand>>);

impl From<Wrapper<GroupOperand<NodeIndicesOperand>>> for PyNodeIndicesGroupOperand {
    fn from(operand: Wrapper<GroupOperand<NodeIndicesOperand>>) -> Self {
        Self(operand)
    }
}

impl From<PyNodeIndicesGroupOperand> for Wrapper<GroupOperand<NodeIndicesOperand>> {
    fn from(operand: PyNodeIndicesGroupOperand) -> Self {
        operand.0
    }
}

impl Deref for PyNodeIndicesGroupOperand {
    type Target = Wrapper<GroupOperand<NodeIndicesOperand>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[pymethods]
impl PyNodeIndicesGroupOperand {
    pub fn max(&self) -> PyNodeIndexGroupOperand {
        self.0.max().into()
    }

    pub fn min(&self) -> PyNodeIndexGroupOperand {
        self.0.min().into()
    }

    pub fn count(&self) -> PyNodeIndexGroupOperand {
        self.0.count().into()
    }

    pub fn sum(&self) -> PyNodeIndexGroupOperand {
        self.0.sum().into()
    }

    pub fn random(&self) -> PyNodeIndexGroupOperand {
        self.0.random().into()
    }

    pub fn greater_than(&self, index: PyNodeIndexComparisonOperand) {
        self.0.greater_than(index);
    }

    pub fn greater_than_or_equal_to(&self, index: PyNodeIndexComparisonOperand) {
        self.0.greater_than_or_equal_to(index);
    }

    pub fn less_than(&self, index: PyNodeIndexComparisonOperand) {
        self.0.less_than(index);
    }

    pub fn less_than_or_equal_to(&self, index: PyNodeIndexComparisonOperand) {
        self.0.less_than_or_equal_to(index);
    }

    pub fn equal_to(&self, index: PyNodeIndexComparisonOperand) {
        self.0.equal_to(index);
    }

    pub fn not_equal_to(&self, index: PyNodeIndexComparisonOperand) {
        self.0.not_equal_to(index);
    }

    pub fn starts_with(&self, index: PyNodeIndexComparisonOperand) {
        self.0.starts_with(index);
    }

    pub fn ends_with(&self, index: PyNodeIndexComparisonOperand) {
        self.0.ends_with(index);
    }

    pub fn contains(&self, index: PyNodeIndexComparisonOperand) {
        self.0.contains(index);
    }

    pub fn is_in(&self, indices: PyNodeIndicesComparisonOperand) {
        self.0.is_in(indices);
    }

    pub fn is_not_in(&self, indices: PyNodeIndicesComparisonOperand) {
        self.0.is_not_in(indices);
    }

    pub fn add(&self, index: PyNodeIndexComparisonOperand) {
        self.0.add(index);
    }

    pub fn sub(&self, index: PyNodeIndexComparisonOperand) {
        self.0.sub(index);
    }

    pub fn mul(&self, index: PyNodeIndexComparisonOperand) {
        self.0.mul(index);
    }

    pub fn pow(&self, index: PyNodeIndexComparisonOperand) {
        self.0.pow(index);
    }

    pub fn r#mod(&self, index: PyNodeIndexComparisonOperand) {
        self.0.r#mod(index);
    }

    pub fn abs(&self) {
        self.0.abs();
    }

    pub fn trim(&self) {
        self.0.trim();
    }

    pub fn trim_start(&self) {
        self.0.trim_start();
    }

    pub fn trim_end(&self) {
        self.0.trim_end();
    }

    pub fn lowercase(&self) {
        self.0.lowercase();
    }

    pub fn uppercase(&self) {
        self.0.uppercase();
    }

    pub fn slice(&self, start: usize, end: usize) {
        self.0.slice(start, end);
    }

    pub fn is_string(&self) {
        self.0.is_string();
    }

    pub fn is_int(&self) {
        self.0.is_int();
    }

    pub fn is_max(&self) {
        self.0.is_max();
    }

    pub fn is_min(&self) {
        self.0.is_min();
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn either_or(&self, either: &Bound<'_, PyFunction>, or: &Bound<'_, PyFunction>) {
        self.0.either_or(
            |operand| {
                either
                    .call1((PyNodeIndicesOperand::from(operand.clone()),))
                    .expect("Call must succeed");
            },
            |operand| {
                or.call1((PyNodeIndicesOperand::from(operand.clone()),))
                    .expect("Call must succeed");
            },
        );
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn exclude(&self, query: &Bound<'_, PyFunction>) {
        self.0.exclude(|operand| {
            query
                .call1((PyNodeIndicesOperand::from(operand.clone()),))
                .expect("Call must succeed");
        });
    }

    pub fn ungroup(&self) -> PyNodeIndicesOperand {
        self.0.ungroup().into()
    }

    pub fn deep_clone(&self) -> Self {
        self.0.deep_clone().into()
    }
}

#[pyclass(frozen)]
#[repr(transparent)]
#[derive(Clone)]
pub struct PyNodeIndexOperand(Wrapper<NodeIndexOperand>);

impl From<Wrapper<NodeIndexOperand>> for PyNodeIndexOperand {
    fn from(operand: Wrapper<NodeIndexOperand>) -> Self {
        Self(operand)
    }
}

impl From<PyNodeIndexOperand> for Wrapper<NodeIndexOperand> {
    fn from(operand: PyNodeIndexOperand) -> Self {
        operand.0
    }
}

impl Deref for PyNodeIndexOperand {
    type Target = Wrapper<NodeIndexOperand>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[pymethods]
impl PyNodeIndexOperand {
    pub fn greater_than(&self, index: PyNodeIndexComparisonOperand) {
        self.0.greater_than(index);
    }

    pub fn greater_than_or_equal_to(&self, index: PyNodeIndexComparisonOperand) {
        self.0.greater_than_or_equal_to(index);
    }

    pub fn less_than(&self, index: PyNodeIndexComparisonOperand) {
        self.0.less_than(index);
    }

    pub fn less_than_or_equal_to(&self, index: PyNodeIndexComparisonOperand) {
        self.0.less_than_or_equal_to(index);
    }

    pub fn equal_to(&self, index: PyNodeIndexComparisonOperand) {
        self.0.equal_to(index);
    }

    pub fn not_equal_to(&self, index: PyNodeIndexComparisonOperand) {
        self.0.not_equal_to(index);
    }

    pub fn starts_with(&self, index: PyNodeIndexComparisonOperand) {
        self.0.starts_with(index);
    }

    pub fn ends_with(&self, index: PyNodeIndexComparisonOperand) {
        self.0.ends_with(index);
    }

    pub fn contains(&self, index: PyNodeIndexComparisonOperand) {
        self.0.contains(index);
    }

    pub fn is_in(&self, indices: PyNodeIndicesComparisonOperand) {
        self.0.is_in(indices);
    }

    pub fn is_not_in(&self, indices: PyNodeIndicesComparisonOperand) {
        self.0.is_not_in(indices);
    }

    pub fn add(&self, index: PyNodeIndexComparisonOperand) {
        self.0.add(index);
    }

    pub fn sub(&self, index: PyNodeIndexComparisonOperand) {
        self.0.sub(index);
    }

    pub fn mul(&self, index: PyNodeIndexComparisonOperand) {
        self.0.mul(index);
    }

    pub fn pow(&self, index: PyNodeIndexComparisonOperand) {
        self.0.pow(index);
    }

    pub fn r#mod(&self, index: PyNodeIndexComparisonOperand) {
        self.0.r#mod(index);
    }

    pub fn abs(&self) {
        self.0.abs();
    }

    pub fn trim(&self) {
        self.0.trim();
    }

    pub fn trim_start(&self) {
        self.0.trim_start();
    }

    pub fn trim_end(&self) {
        self.0.trim_end();
    }

    pub fn lowercase(&self) {
        self.0.lowercase();
    }

    pub fn uppercase(&self) {
        self.0.uppercase();
    }

    pub fn slice(&self, start: usize, end: usize) {
        self.0.slice(start, end);
    }

    pub fn is_string(&self) {
        self.0.is_string();
    }

    pub fn is_int(&self) {
        self.0.is_int();
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn either_or(&self, either: &Bound<'_, PyFunction>, or: &Bound<'_, PyFunction>) {
        self.0.either_or(
            |operand| {
                either
                    .call1((Self::from(operand.clone()),))
                    .expect("Call must succeed");
            },
            |operand| {
                or.call1((Self::from(operand.clone()),))
                    .expect("Call must succeed");
            },
        );
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn exclude(&self, query: &Bound<'_, PyFunction>) {
        self.0.exclude(|operand| {
            query
                .call1((Self::from(operand.clone()),))
                .expect("Call must succeed");
        });
    }

    pub fn deep_clone(&self) -> Self {
        self.0.deep_clone().into()
    }
}

#[pyclass(frozen)]
#[repr(transparent)]
#[derive(Clone)]
pub struct PyNodeIndexGroupOperand(Wrapper<GroupOperand<NodeIndexOperand>>);

impl From<Wrapper<GroupOperand<NodeIndexOperand>>> for PyNodeIndexGroupOperand {
    fn from(operand: Wrapper<GroupOperand<NodeIndexOperand>>) -> Self {
        Self(operand)
    }
}

impl From<PyNodeIndexGroupOperand> for Wrapper<GroupOperand<NodeIndexOperand>> {
    fn from(operand: PyNodeIndexGroupOperand) -> Self {
        operand.0
    }
}

impl Deref for PyNodeIndexGroupOperand {
    type Target = Wrapper<GroupOperand<NodeIndexOperand>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[pymethods]
impl PyNodeIndexGroupOperand {
    pub fn greater_than(&self, index: PyNodeIndexComparisonOperand) {
        self.0.greater_than(index);
    }

    pub fn greater_than_or_equal_to(&self, index: PyNodeIndexComparisonOperand) {
        self.0.greater_than_or_equal_to(index);
    }

    pub fn less_than(&self, index: PyNodeIndexComparisonOperand) {
        self.0.less_than(index);
    }

    pub fn less_than_or_equal_to(&self, index: PyNodeIndexComparisonOperand) {
        self.0.less_than_or_equal_to(index);
    }

    pub fn equal_to(&self, index: PyNodeIndexComparisonOperand) {
        self.0.equal_to(index);
    }

    pub fn not_equal_to(&self, index: PyNodeIndexComparisonOperand) {
        self.0.not_equal_to(index);
    }

    pub fn starts_with(&self, index: PyNodeIndexComparisonOperand) {
        self.0.starts_with(index);
    }

    pub fn ends_with(&self, index: PyNodeIndexComparisonOperand) {
        self.0.ends_with(index);
    }

    pub fn contains(&self, index: PyNodeIndexComparisonOperand) {
        self.0.contains(index);
    }

    pub fn is_in(&self, indices: PyNodeIndicesComparisonOperand) {
        self.0.is_in(indices);
    }

    pub fn is_not_in(&self, indices: PyNodeIndicesComparisonOperand) {
        self.0.is_not_in(indices);
    }

    pub fn add(&self, index: PyNodeIndexComparisonOperand) {
        self.0.add(index);
    }

    pub fn sub(&self, index: PyNodeIndexComparisonOperand) {
        self.0.sub(index);
    }

    pub fn mul(&self, index: PyNodeIndexComparisonOperand) {
        self.0.mul(index);
    }

    pub fn pow(&self, index: PyNodeIndexComparisonOperand) {
        self.0.pow(index);
    }

    pub fn r#mod(&self, index: PyNodeIndexComparisonOperand) {
        self.0.r#mod(index);
    }

    pub fn abs(&self) {
        self.0.abs();
    }

    pub fn trim(&self) {
        self.0.trim();
    }

    pub fn trim_start(&self) {
        self.0.trim_start();
    }

    pub fn trim_end(&self) {
        self.0.trim_end();
    }

    pub fn lowercase(&self) {
        self.0.lowercase();
    }

    pub fn uppercase(&self) {
        self.0.uppercase();
    }

    pub fn slice(&self, start: usize, end: usize) {
        self.0.slice(start, end);
    }

    pub fn is_string(&self) {
        self.0.is_string();
    }

    pub fn is_int(&self) {
        self.0.is_int();
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn either_or(&self, either: &Bound<'_, PyFunction>, or: &Bound<'_, PyFunction>) {
        self.0.either_or(
            |operand| {
                either
                    .call1((PyNodeIndexOperand::from(operand.clone()),))
                    .expect("Call must succeed");
            },
            |operand| {
                or.call1((PyNodeIndexOperand::from(operand.clone()),))
                    .expect("Call must succeed");
            },
        );
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn exclude(&self, query: &Bound<'_, PyFunction>) {
        self.0.exclude(|operand| {
            query
                .call1((PyNodeIndexOperand::from(operand.clone()),))
                .expect("Call must succeed");
        });
    }

    pub fn ungroup(&self) -> PyNodeIndicesOperand {
        self.0.ungroup().into()
    }

    pub fn deep_clone(&self) -> Self {
        self.0.deep_clone().into()
    }
}
