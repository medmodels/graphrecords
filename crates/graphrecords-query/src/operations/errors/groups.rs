use crate::{
    Diagnostic, ErrorGroup, FailureKind,
    operations::{
        ArgumentAbsent, MissingAttribute, MissingGroupAggregate, MissingTraversedAttribute,
    },
};
use graphrecords_core::graphrecord::NodeIndex;

pub struct AbsenceErrors;

impl ErrorGroup for AbsenceErrors {
    fn name() -> &'static str {
        "AbsenceErrors"
    }

    fn contains(kind: &FailureKind) -> bool {
        kind.is::<ArgumentAbsent>()
            || kind.is::<MissingAttribute>()
            || kind.name() == MissingTraversedAttribute::<NodeIndex>::name()
            || kind.is::<MissingGroupAggregate>()
    }
}
