use crate::{
    Diagnostic, ErrorGroup, FailureKind,
    error::{
        argument::ArgumentMissing,
        grouping::{MissingGroupAggregate, MissingGroupBucket},
        structure::{MissingAttribute, MissingTraversedAttribute},
    },
};
use graphrecords_core::graphrecord::NodeIndex;

pub struct AbsenceErrors;

impl ErrorGroup for AbsenceErrors {
    fn name() -> &'static str {
        "AbsenceErrors"
    }

    fn contains(kind: &FailureKind) -> bool {
        kind.is::<ArgumentMissing>()
            || kind.is::<MissingAttribute>()
            || kind.name() == MissingTraversedAttribute::<NodeIndex>::name()
            || kind.is::<MissingGroupAggregate>()
            || kind.is::<MissingGroupBucket>()
    }
}
