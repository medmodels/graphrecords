mod conversion;
mod graphrecord;
#[cfg(feature = "io")]
mod io;
mod schema;

pub use conversion::ConversionError;
pub use graphrecord::{GraphRecordError, ValueOperation};
#[cfg(feature = "io")]
pub use io::IoError;
pub use schema::SchemaError;

pub type GraphRecordResult<T> = Result<T, GraphRecordError>;

#[cfg(test)]
mod test {
    #[cfg(feature = "io")]
    use super::IoError;
    use super::{ConversionError, GraphRecordError, SchemaError};
    use crate::graphrecord::Value;

    #[test]
    fn test_from_schema_error() {
        assert!(matches!(
            GraphRecordError::from(SchemaError::GroupNotInSchema {
                group_index: "test".into()
            }),
            GraphRecordError::Schema(schema_error)
                if schema_error == SchemaError::GroupNotInSchema {
                    group_index: "test".into()
                }
        ));
    }

    #[test]
    fn test_from_conversion_error() {
        assert!(matches!(
            GraphRecordError::from(ConversionError::ValueToIdentifier {
                value: Value::Bool(true)
            }),
            GraphRecordError::Conversion(conversion_error)
                if conversion_error == ConversionError::ValueToIdentifier {
                    value: Value::Bool(true)
                }
        ));
    }

    #[cfg(feature = "io")]
    #[test]
    fn test_from_io_error() {
        assert!(matches!(
            GraphRecordError::from(IoError::CorruptedFile {
                path: "path".to_string()
            }),
            GraphRecordError::Io(io_error)
                if io_error == IoError::CorruptedFile {
                    path: "path".to_string()
                }
        ));
    }
}
