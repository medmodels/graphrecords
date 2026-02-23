use super::{
    PyGraphRecordAttributeCardinalityWrapper, PyGroupCardinalityWrapper,
    attributes::PyEdgeAttributesTreeOperand, nodes::PyNodeOperand,
};
use crate::graphrecord::{
    attribute::PyGraphRecordAttribute,
    errors::PyGraphRecordError,
    querying::{
        attributes::PyEdgeAttributesTreeGroupOperand,
        nodes::PyNodeGroupOperand,
        values::{PyEdgeMultipleValuesWithIndexGroupOperand, PyEdgeMultipleValuesWithIndexOperand},
    },
};
use graphrecords_core::{
    errors::GraphRecordError,
    graphrecord::{
        EdgeIndex,
        querying::{
            DeepClone,
            edges::{
                self, EdgeIndexComparisonOperand, EdgeIndexOperand, EdgeIndicesComparisonOperand,
                EdgeIndicesOperand, EdgeOperand,
            },
            group_by::GroupOperand,
            wrapper::Wrapper,
        },
    },
};
use pyo3::{
    Borrowed, Bound, FromPyObject, PyAny, PyErr, PyResult, pyclass, pymethods,
    types::{PyAnyMethods, PyFunction},
};
use std::ops::Deref;

#[pyclass(frozen)]
#[derive(Clone)]
pub enum EdgeOperandGroupDiscriminator {
    SourceNode(),
    TargetNode(),
    Parallel(),
    Attribute(PyGraphRecordAttribute),
}

impl From<EdgeOperandGroupDiscriminator> for edges::EdgeOperandGroupDiscriminator {
    fn from(discriminator: EdgeOperandGroupDiscriminator) -> Self {
        match discriminator {
            EdgeOperandGroupDiscriminator::SourceNode() => Self::SourceNode,
            EdgeOperandGroupDiscriminator::TargetNode() => Self::TargetNode,
            EdgeOperandGroupDiscriminator::Parallel() => Self::Parallel,
            EdgeOperandGroupDiscriminator::Attribute(attribute) => {
                Self::Attribute(attribute.into())
            }
        }
    }
}

#[pyclass(frozen)]
#[repr(transparent)]
pub struct PyEdgeOperand(Wrapper<EdgeOperand>);

impl From<Wrapper<EdgeOperand>> for PyEdgeOperand {
    fn from(operand: Wrapper<EdgeOperand>) -> Self {
        Self(operand)
    }
}

impl From<PyEdgeOperand> for Wrapper<EdgeOperand> {
    fn from(operand: PyEdgeOperand) -> Self {
        operand.0
    }
}

#[pymethods]
impl PyEdgeOperand {
    pub fn attribute(
        &self,
        attribute: PyGraphRecordAttribute,
    ) -> PyEdgeMultipleValuesWithIndexOperand {
        self.0.attribute(attribute).into()
    }

    pub fn attributes(&self) -> PyEdgeAttributesTreeOperand {
        self.0.attributes().into()
    }

    pub fn index(&self) -> PyEdgeIndicesOperand {
        self.0.index().into()
    }

    pub fn in_group(&self, group: PyGroupCardinalityWrapper) {
        self.0.in_group(group);
    }

    pub fn has_attribute(&self, attribute: PyGraphRecordAttributeCardinalityWrapper) {
        self.0.has_attribute(attribute);
    }

    pub fn source_node(&self) -> PyNodeOperand {
        self.0.source_node().into()
    }

    pub fn target_node(&self) -> PyNodeOperand {
        self.0.target_node().into()
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

    pub fn group_by(&self, discriminator: EdgeOperandGroupDiscriminator) -> PyEdgeGroupOperand {
        self.0.group_by(discriminator.into()).into()
    }

    pub fn deep_clone(&self) -> Self {
        self.0.deep_clone().into()
    }
}

#[pyclass(frozen)]
#[repr(transparent)]
pub struct PyEdgeGroupOperand(Wrapper<GroupOperand<EdgeOperand>>);

impl From<Wrapper<GroupOperand<EdgeOperand>>> for PyEdgeGroupOperand {
    fn from(operand: Wrapper<GroupOperand<EdgeOperand>>) -> Self {
        Self(operand)
    }
}

impl From<PyEdgeGroupOperand> for Wrapper<GroupOperand<EdgeOperand>> {
    fn from(operand: PyEdgeGroupOperand) -> Self {
        operand.0
    }
}

#[pymethods]
impl PyEdgeGroupOperand {
    pub fn attribute(
        &self,
        attribute: PyGraphRecordAttribute,
    ) -> PyEdgeMultipleValuesWithIndexGroupOperand {
        self.0.attribute(attribute).into()
    }

    pub fn attributes(&self) -> PyEdgeAttributesTreeGroupOperand {
        self.0.attributes().into()
    }

    pub fn index(&self) -> PyEdgeIndicesGroupOperand {
        self.0.index().into()
    }

    pub fn in_group(&self, group: PyGroupCardinalityWrapper) {
        self.0.in_group(group);
    }

    pub fn has_attribute(&self, attribute: PyGraphRecordAttributeCardinalityWrapper) {
        self.0.has_attribute(attribute);
    }

    pub fn source_node(&self) -> PyNodeGroupOperand {
        self.0.source_node().into()
    }

    pub fn target_node(&self) -> PyNodeGroupOperand {
        self.0.target_node().into()
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn either_or(&self, either: &Bound<'_, PyFunction>, or: &Bound<'_, PyFunction>) {
        self.0.either_or(
            |operand| {
                either
                    .call1((PyEdgeOperand::from(operand.clone()),))
                    .expect("Call must succeed");
            },
            |operand| {
                or.call1((PyEdgeOperand::from(operand.clone()),))
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
                .call1((PyEdgeOperand::from(operand.clone()),))
                .expect("Call must succeed");
        });
    }

    pub fn deep_clone(&self) -> Self {
        self.0.deep_clone().into()
    }
}

#[repr(transparent)]
pub struct PyEdgeIndexComparisonOperand(EdgeIndexComparisonOperand);

impl From<EdgeIndexComparisonOperand> for PyEdgeIndexComparisonOperand {
    fn from(operand: EdgeIndexComparisonOperand) -> Self {
        Self(operand)
    }
}

impl From<PyEdgeIndexComparisonOperand> for EdgeIndexComparisonOperand {
    fn from(operand: PyEdgeIndexComparisonOperand) -> Self {
        operand.0
    }
}

impl FromPyObject<'_, '_> for PyEdgeIndexComparisonOperand {
    type Error = PyErr;

    fn extract(ob: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        match ob.extract::<EdgeIndex>() {
            Ok(index) => Ok(EdgeIndexComparisonOperand::Index(index).into()),
            _ => match ob.extract::<PyEdgeIndexOperand>() {
                Ok(operand) => Ok(Self(operand.0.into())),
                _ => Err(
                    PyGraphRecordError::from(GraphRecordError::ConversionError(format!(
                        "Failed to convert {} into EdgeIndex or EdgeIndexOperand",
                        ob.to_owned()
                    )))
                    .into(),
                ),
            },
        }
    }
}

#[repr(transparent)]
pub struct PyEdgeIndicesComparisonOperand(EdgeIndicesComparisonOperand);

impl From<EdgeIndicesComparisonOperand> for PyEdgeIndicesComparisonOperand {
    fn from(operand: EdgeIndicesComparisonOperand) -> Self {
        Self(operand)
    }
}

impl From<PyEdgeIndicesComparisonOperand> for EdgeIndicesComparisonOperand {
    fn from(operand: PyEdgeIndicesComparisonOperand) -> Self {
        operand.0
    }
}

impl FromPyObject<'_, '_> for PyEdgeIndicesComparisonOperand {
    type Error = PyErr;

    fn extract(ob: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        match ob.extract::<Vec<EdgeIndex>>() {
            Ok(indices) => Ok(EdgeIndicesComparisonOperand::from(indices).into()),
            _ => match ob.extract::<PyEdgeIndicesOperand>() {
                Ok(operand) => Ok(Self(operand.0.into())),
                _ => Err(
                    PyGraphRecordError::from(GraphRecordError::ConversionError(format!(
                        "Failed to convert {} into List[EdgeIndex] or EdgeIndicesOperand",
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
pub struct PyEdgeIndicesOperand(Wrapper<EdgeIndicesOperand>);

impl From<Wrapper<EdgeIndicesOperand>> for PyEdgeIndicesOperand {
    fn from(operand: Wrapper<EdgeIndicesOperand>) -> Self {
        Self(operand)
    }
}

impl From<PyEdgeIndicesOperand> for Wrapper<EdgeIndicesOperand> {
    fn from(operand: PyEdgeIndicesOperand) -> Self {
        operand.0
    }
}

impl Deref for PyEdgeIndicesOperand {
    type Target = Wrapper<EdgeIndicesOperand>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[pymethods]
impl PyEdgeIndicesOperand {
    pub fn max(&self) -> PyEdgeIndexOperand {
        self.0.max().into()
    }

    pub fn min(&self) -> PyEdgeIndexOperand {
        self.0.min().into()
    }

    pub fn count(&self) -> PyEdgeIndexOperand {
        self.0.count().into()
    }

    pub fn sum(&self) -> PyEdgeIndexOperand {
        self.0.sum().into()
    }

    pub fn random(&self) -> PyEdgeIndexOperand {
        self.0.random().into()
    }

    pub fn greater_than(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.greater_than(index);
    }

    pub fn greater_than_or_equal_to(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.greater_than_or_equal_to(index);
    }

    pub fn less_than(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.less_than(index);
    }

    pub fn less_than_or_equal_to(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.less_than_or_equal_to(index);
    }

    pub fn equal_to(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.equal_to(index);
    }

    pub fn not_equal_to(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.not_equal_to(index);
    }

    pub fn starts_with(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.starts_with(index);
    }

    pub fn ends_with(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.ends_with(index);
    }

    pub fn contains(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.contains(index);
    }

    pub fn is_in(&self, indices: PyEdgeIndicesComparisonOperand) {
        self.0.is_in(indices);
    }

    pub fn is_not_in(&self, indices: PyEdgeIndicesComparisonOperand) {
        self.0.is_not_in(indices);
    }

    pub fn add(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.add(index);
    }

    pub fn sub(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.sub(index);
    }

    pub fn mul(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.mul(index);
    }

    pub fn pow(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.pow(index);
    }

    pub fn r#mod(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.r#mod(index);
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
pub struct PyEdgeIndicesGroupOperand(Wrapper<GroupOperand<EdgeIndicesOperand>>);

impl From<Wrapper<GroupOperand<EdgeIndicesOperand>>> for PyEdgeIndicesGroupOperand {
    fn from(operand: Wrapper<GroupOperand<EdgeIndicesOperand>>) -> Self {
        Self(operand)
    }
}

impl From<PyEdgeIndicesGroupOperand> for Wrapper<GroupOperand<EdgeIndicesOperand>> {
    fn from(operand: PyEdgeIndicesGroupOperand) -> Self {
        operand.0
    }
}

impl Deref for PyEdgeIndicesGroupOperand {
    type Target = Wrapper<GroupOperand<EdgeIndicesOperand>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[pymethods]
impl PyEdgeIndicesGroupOperand {
    pub fn max(&self) -> PyEdgeIndexGroupOperand {
        self.0.max().into()
    }

    pub fn min(&self) -> PyEdgeIndexGroupOperand {
        self.0.min().into()
    }

    pub fn count(&self) -> PyEdgeIndexGroupOperand {
        self.0.count().into()
    }

    pub fn sum(&self) -> PyEdgeIndexGroupOperand {
        self.0.sum().into()
    }

    pub fn random(&self) -> PyEdgeIndexGroupOperand {
        self.0.random().into()
    }

    pub fn greater_than(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.greater_than(index);
    }

    pub fn greater_than_or_equal_to(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.greater_than_or_equal_to(index);
    }

    pub fn less_than(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.less_than(index);
    }

    pub fn less_than_or_equal_to(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.less_than_or_equal_to(index);
    }

    pub fn equal_to(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.equal_to(index);
    }

    pub fn not_equal_to(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.not_equal_to(index);
    }

    pub fn starts_with(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.starts_with(index);
    }

    pub fn ends_with(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.ends_with(index);
    }

    pub fn contains(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.contains(index);
    }

    pub fn is_in(&self, indices: PyEdgeIndicesComparisonOperand) {
        self.0.is_in(indices);
    }

    pub fn is_not_in(&self, indices: PyEdgeIndicesComparisonOperand) {
        self.0.is_not_in(indices);
    }

    pub fn add(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.add(index);
    }

    pub fn sub(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.sub(index);
    }

    pub fn mul(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.mul(index);
    }

    pub fn pow(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.pow(index);
    }

    pub fn r#mod(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.r#mod(index);
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
                    .call1((PyEdgeIndicesOperand::from(operand.clone()),))
                    .expect("Call must succeed");
            },
            |operand| {
                or.call1((PyEdgeIndicesOperand::from(operand.clone()),))
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
                .call1((PyEdgeIndicesOperand::from(operand.clone()),))
                .expect("Call must succeed");
        });
    }

    pub fn ungroup(&self) -> PyEdgeIndicesOperand {
        self.0.ungroup().into()
    }

    pub fn deep_clone(&self) -> Self {
        self.0.deep_clone().into()
    }
}

#[pyclass(frozen)]
#[repr(transparent)]
#[derive(Clone)]
pub struct PyEdgeIndexOperand(Wrapper<EdgeIndexOperand>);

impl From<Wrapper<EdgeIndexOperand>> for PyEdgeIndexOperand {
    fn from(operand: Wrapper<EdgeIndexOperand>) -> Self {
        Self(operand)
    }
}

impl From<PyEdgeIndexOperand> for Wrapper<EdgeIndexOperand> {
    fn from(operand: PyEdgeIndexOperand) -> Self {
        operand.0
    }
}

impl Deref for PyEdgeIndexOperand {
    type Target = Wrapper<EdgeIndexOperand>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[pymethods]
impl PyEdgeIndexOperand {
    pub fn greater_than(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.greater_than(index);
    }

    pub fn greater_than_or_equal_to(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.greater_than_or_equal_to(index);
    }

    pub fn less_than(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.less_than(index);
    }

    pub fn less_than_or_equal_to(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.less_than_or_equal_to(index);
    }

    pub fn equal_to(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.equal_to(index);
    }

    pub fn not_equal_to(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.not_equal_to(index);
    }

    pub fn starts_with(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.starts_with(index);
    }

    pub fn ends_with(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.ends_with(index);
    }

    pub fn contains(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.contains(index);
    }

    pub fn is_in(&self, indices: PyEdgeIndicesComparisonOperand) {
        self.0.is_in(indices);
    }

    pub fn is_not_in(&self, indices: PyEdgeIndicesComparisonOperand) {
        self.0.is_not_in(indices);
    }

    pub fn add(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.add(index);
    }

    pub fn sub(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.sub(index);
    }

    pub fn mul(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.mul(index);
    }

    pub fn pow(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.pow(index);
    }

    pub fn r#mod(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.r#mod(index);
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
pub struct PyEdgeIndexGroupOperand(Wrapper<GroupOperand<EdgeIndexOperand>>);

impl From<Wrapper<GroupOperand<EdgeIndexOperand>>> for PyEdgeIndexGroupOperand {
    fn from(operand: Wrapper<GroupOperand<EdgeIndexOperand>>) -> Self {
        Self(operand)
    }
}

impl From<PyEdgeIndexGroupOperand> for Wrapper<GroupOperand<EdgeIndexOperand>> {
    fn from(operand: PyEdgeIndexGroupOperand) -> Self {
        operand.0
    }
}

impl Deref for PyEdgeIndexGroupOperand {
    type Target = Wrapper<GroupOperand<EdgeIndexOperand>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[pymethods]
impl PyEdgeIndexGroupOperand {
    pub fn greater_than(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.greater_than(index);
    }

    pub fn greater_than_or_equal_to(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.greater_than_or_equal_to(index);
    }

    pub fn less_than(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.less_than(index);
    }

    pub fn less_than_or_equal_to(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.less_than_or_equal_to(index);
    }

    pub fn equal_to(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.equal_to(index);
    }

    pub fn not_equal_to(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.not_equal_to(index);
    }

    pub fn starts_with(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.starts_with(index);
    }

    pub fn ends_with(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.ends_with(index);
    }

    pub fn contains(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.contains(index);
    }

    pub fn is_in(&self, indices: PyEdgeIndicesComparisonOperand) {
        self.0.is_in(indices);
    }

    pub fn is_not_in(&self, indices: PyEdgeIndicesComparisonOperand) {
        self.0.is_not_in(indices);
    }

    pub fn add(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.add(index);
    }

    pub fn sub(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.sub(index);
    }

    pub fn mul(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.mul(index);
    }

    pub fn pow(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.pow(index);
    }

    pub fn r#mod(&self, index: PyEdgeIndexComparisonOperand) {
        self.0.r#mod(index);
    }

    /// # Panics
    ///
    /// Panics if the python typing was not followed.
    pub fn either_or(&self, either: &Bound<'_, PyFunction>, or: &Bound<'_, PyFunction>) {
        self.0.either_or(
            |operand| {
                either
                    .call1((PyEdgeIndexOperand::from(operand.clone()),))
                    .expect("Call must succeed");
            },
            |operand| {
                or.call1((PyEdgeIndexOperand::from(operand.clone()),))
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
                .call1((PyEdgeIndexOperand::from(operand.clone()),))
                .expect("Call must succeed");
        });
    }

    pub fn ungroup(&self) -> PyEdgeIndicesOperand {
        self.0.ungroup().into()
    }

    pub fn deep_clone(&self) -> Self {
        self.0.deep_clone().into()
    }
}
