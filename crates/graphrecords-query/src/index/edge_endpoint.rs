use crate::{IndexDomain, QueryResult};
use graphrecords_core::GraphRecord;
use std::fmt::{self, Display, Formatter};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum EdgeEndpointRole {
    Source,
    Target,
}

impl Display for EdgeEndpointRole {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source => formatter.write_str("source"),
            Self::Target => formatter.write_str("target"),
        }
    }
}

impl IndexDomain for EdgeEndpointRole {
    type Address = Self;
    type Index<'a> = Self;
    type Owned = Self;

    fn index<'a>(_graphrecord: &'a GraphRecord, address: &Self::Address) -> Self::Index<'a> {
        *address
    }

    fn own_index(index: &Self::Index<'_>) -> Self::Owned {
        *index
    }

    fn borrow_index(owned: &Self::Owned) -> Self::Index<'_> {
        *owned
    }

    fn resolve(
        _graphrecord: &GraphRecord,
        owned: &Self::Owned,
        _label: &'static str,
    ) -> QueryResult<Self::Address> {
        Ok(*owned)
    }
}
