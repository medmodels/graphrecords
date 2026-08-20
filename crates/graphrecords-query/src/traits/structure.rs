use graphrecords_core::{graphrecord::Group, prelude::AttributeName};

pub trait Attribute {
    type Output;

    fn attribute(&self, attribute: AttributeName) -> Self::Output;
}

pub trait Attributes {
    type Output;

    fn attributes(&self) -> Self::Output;
}

pub trait HasAttribute {
    type Output;

    fn has_attribute(&self, attribute: AttributeName) -> Self::Output;
}

pub trait InGroup {
    type Output;

    fn in_group(&self, group: Group) -> Self::Output;
}

pub trait Filter<M> {
    type Output;

    fn filter(&self, mask: M) -> Self::Output;
}
