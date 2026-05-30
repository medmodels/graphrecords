use graphrecords_core::{graphrecord::Group, prelude::GraphRecordAttribute};

pub trait Attribute {
    type ReturnOperand;

    fn attribute(&self, attribute: GraphRecordAttribute) -> Self::ReturnOperand;
}

pub trait InGroup {
    type ReturnOperand;

    fn in_group(&self, group: Group) -> Self::ReturnOperand;
}

pub trait Where {
    type MaskOperand;
    type ReturnOperand;

    fn r#where(&self, predicate: Self::MaskOperand) -> Self::ReturnOperand;
}
