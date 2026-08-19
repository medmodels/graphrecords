mod conversion;
mod graphrecord;
mod io;
mod schema;

pub use conversion::ConversionError;
pub use graphrecord::{GraphRecordError, ValueOperation};
pub use io::IoError;
pub use schema::SchemaError;

pub type GraphRecordResult<T> = Result<T, GraphRecordError>;

#[cfg(test)]
mod test {
    use super::{ConversionError, GraphRecordError, IoError, SchemaError};
    use crate::graphrecord::Value;

    #[test]
    fn test_from_schema_error() {
        assert_eq!(
            GraphRecordError::Schema(SchemaError::GroupNotInSchema {
                group: "test".into()
            }),
            GraphRecordError::from(SchemaError::GroupNotInSchema {
                group: "test".into()
            })
        );
    }

    #[test]
    fn test_from_conversion_error() {
        assert_eq!(
            GraphRecordError::Conversion(ConversionError::ValueToIdentifier {
                value: Value::Bool(true)
            }),
            GraphRecordError::from(ConversionError::ValueToIdentifier {
                value: Value::Bool(true)
            })
        );
    }

    #[test]
    fn test_from_io_error() {
        assert_eq!(
            GraphRecordError::Io(IoError::CorruptedFile {
                path: "path".to_string()
            }),
            GraphRecordError::from(IoError::CorruptedFile {
                path: "path".to_string()
            })
        );
    }
}
