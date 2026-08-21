use std::fmt::{Display, Formatter, Result as FmtResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OnConflict {
    Raise,
    KeepSelf,
    KeepOther,
}

impl Display for OnConflict {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Raise => f.write_str("raise"),
            Self::KeepSelf => f.write_str("keep_self"),
            Self::KeepOther => f.write_str("keep_other"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::OnConflict;

    #[test]
    fn test_display() {
        assert_eq!("raise", OnConflict::Raise.to_string());
        assert_eq!("keep_self", OnConflict::KeepSelf.to_string());
        assert_eq!("keep_other", OnConflict::KeepOther.to_string());
    }
}
