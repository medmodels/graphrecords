use graphrecords_core::graphrecord::{GraphRecordAttribute, GraphRecordValue};
use std::fmt::{self, Display, Formatter};

pub trait Discriminator: 'static {
    type Key<'a>;
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct AttributeDiscriminator {
    pub attribute: GraphRecordAttribute,
}

impl Display for AttributeDiscriminator {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "attribute={}", self.attribute)
    }
}

impl Discriminator for AttributeDiscriminator {
    type Key<'a> = Option<&'a GraphRecordValue>;
}
