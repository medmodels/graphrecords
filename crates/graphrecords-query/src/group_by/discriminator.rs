use graphrecords_core::graphrecord::{GraphRecordAttribute, GraphRecordValue};

pub trait Discriminator {
    type Key<'a>;
}

pub struct AttributeDiscriminator {
    pub attribute: GraphRecordAttribute,
}

impl Discriminator for AttributeDiscriminator {
    type Key<'a> = Option<&'a GraphRecordValue>;
}
