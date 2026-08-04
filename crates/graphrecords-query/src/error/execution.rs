use crate::Diagnostic;
use std::{
    error::Error,
    fmt::{self, Display, Formatter},
};

#[derive(Debug)]
pub struct EvaluationCacheGraphRecordMismatch;

impl Display for EvaluationCacheGraphRecordMismatch {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("the evaluation cache belongs to a different graphrecord")
    }
}

impl Error for EvaluationCacheGraphRecordMismatch {}

impl Diagnostic for EvaluationCacheGraphRecordMismatch {
    fn name() -> &'static str {
        "EvaluationCacheGraphRecordMismatch"
    }
}
