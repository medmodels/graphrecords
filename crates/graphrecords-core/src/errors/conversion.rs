use crate::graphrecord::Value;
use std::{
    error::Error,
    fmt::{Display, Formatter, Result as FmtResult},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConversionError {
    ValueToIdentifier { value: Value },
}

impl Error for ConversionError {}

impl Display for ConversionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::ValueToIdentifier { value } => {
                write!(f, "Cannot convert `{value}` into `Identifier`")
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::ConversionError;
    use crate::graphrecord::Value;

    #[test]
    fn test_display_values() {
        assert_eq!(
            "Cannot convert `true` into `Identifier`",
            ConversionError::ValueToIdentifier {
                value: Value::Bool(true)
            }
            .to_string()
        );
    }
}
