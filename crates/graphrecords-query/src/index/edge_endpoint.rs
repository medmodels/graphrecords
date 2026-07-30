use crate::{IndexDomain, QueryResult, index::GroupKey};
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
    type Index<'a> = Self;
    type Owned = Self;

    fn to_owned(index: &Self::Index<'_>) -> Self::Owned {
        *index
    }

    fn from_owned(owned: &Self::Owned) -> Self::Index<'_> {
        *owned
    }
}

impl GroupKey for EdgeEndpointRole {
    fn resolve_key<'a>(
        _label: &'static str,
        _graphrecord: &'a GraphRecord,
        key: &Self::Owned,
    ) -> QueryResult<Self::Index<'a>> {
        Ok(*key)
    }
}
