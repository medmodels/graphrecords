use graphrecords_core::{graphrecord::Group, prelude::GraphRecordAttribute};

pub trait Attribute {
    type ReturnOperand;

    fn attribute(&self, attribute: GraphRecordAttribute) -> Self::ReturnOperand;
}

pub trait Attributes {
    type ReturnOperand;

    fn attributes(&self) -> Self::ReturnOperand;
}

pub trait HasAttribute {
    type ReturnOperand;

    fn has_attribute(&self, attribute: GraphRecordAttribute) -> Self::ReturnOperand;
}

pub trait InGroup {
    type ReturnOperand;

    fn in_group(&self, group: Group) -> Self::ReturnOperand;
}

pub trait Filter<M> {
    type ReturnOperand;

    fn filter(&self, mask: M) -> Self::ReturnOperand;
}
