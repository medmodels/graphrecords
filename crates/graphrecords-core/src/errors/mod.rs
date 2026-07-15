mod conversion;
mod graphrecord;
mod schema;

pub use conversion::ConversionError;
pub use graphrecord::{GraphRecordError, ValueOperation};
pub use schema::SchemaError;

pub type GraphRecordResult<T> = Result<T, GraphRecordError>;

#[cfg(test)]
mod test {
    use super::{GraphRecordError, SchemaError};

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
}
