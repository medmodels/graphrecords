use graphrecords_core::{graphrecord::GroupIndex, prelude::AttributeName};

pub trait Attribute {
    type Output;

    fn attribute(&self, attribute: impl Into<AttributeName>) -> Self::Output;
}

pub trait Attributes {
    type Output;

    fn attributes(&self) -> Self::Output;
}

pub trait HasAttribute {
    type Output;

    fn has_attribute(&self, attribute: impl Into<AttributeName>) -> Self::Output;
}

pub trait InGroup {
    type Output;

    fn in_group(&self, group_index: impl Into<GroupIndex>) -> Self::Output;
}

pub trait Filter<M> {
    type Output;

    fn filter(&self, mask: M) -> Self::Output;
}
