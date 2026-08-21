use std::fmt::{Display, Formatter, Result as FmtResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EdgeDirection {
    Incoming,
    Outgoing,
    Both,
}

impl Display for EdgeDirection {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Incoming => f.write_str("incoming"),
            Self::Outgoing => f.write_str("outgoing"),
            Self::Both => f.write_str("both"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::EdgeDirection;

    #[test]
    fn test_display() {
        assert_eq!("incoming", EdgeDirection::Incoming.to_string());
        assert_eq!("outgoing", EdgeDirection::Outgoing.to_string());
        assert_eq!("both", EdgeDirection::Both.to_string());
    }
}
