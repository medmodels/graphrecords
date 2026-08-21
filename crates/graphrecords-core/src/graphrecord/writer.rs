use super::GraphRecord;
use crate::errors::GraphRecordResult;

pub trait Writer {
    type Output;

    fn write(self, graphrecord: &GraphRecord) -> GraphRecordResult<Self::Output>;
}

impl GraphRecord {
    pub fn export<W: Writer>(&self, writer: W) -> GraphRecordResult<W::Output> {
        writer.write(self)
    }
}

#[cfg(test)]
mod test {
    use super::Writer;
    use crate::{
        errors::{GraphRecordError, GraphRecordResult},
        graphrecord::{AttributeMap, GraphRecord},
    };
    use std::{
        error::Error,
        fmt::{Display, Formatter, Result as FmtResult},
        sync::Arc,
    };

    #[derive(Debug)]
    struct LoremError;

    impl Display for LoremError {
        fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
            write!(f, "lorem ipsum")
        }
    }

    impl Error for LoremError {}

    struct CountingWriter;

    impl Writer for CountingWriter {
        type Output = (usize, usize);

        fn write(self, graphrecord: &GraphRecord) -> GraphRecordResult<Self::Output> {
            Ok((graphrecord.node_count(), graphrecord.edge_count()))
        }
    }

    struct FailingWriter;

    impl Writer for FailingWriter {
        type Output = ();

        fn write(self, _graphrecord: &GraphRecord) -> GraphRecordResult<Self::Output> {
            Err(GraphRecordError::WriterFailure {
                cause: Arc::new(LoremError),
            })
        }
    }

    fn create_graphrecord_with_one_edge() -> GraphRecord {
        GraphRecord::new()
            .add_node("lorem", AttributeMap::new())
            .unwrap()
            .add_node("ipsum", AttributeMap::new())
            .unwrap()
            .add_edge("lorem", "ipsum", AttributeMap::new())
            .unwrap()
    }

    #[test]
    fn test_export() {
        let graphrecord = create_graphrecord_with_one_edge();

        let export = graphrecord.export(CountingWriter).unwrap();

        assert_eq!((2, 1), export);
    }

    #[test]
    fn test_invalid_export() {
        let graphrecord = GraphRecord::new();

        assert!(
            graphrecord
                .export(FailingWriter)
                .is_err_and(|error| matches!(
                    error,
                    GraphRecordError::WriterFailure { cause }
                        if cause.downcast_ref::<LoremError>().is_some()
                ))
        );
    }
}
