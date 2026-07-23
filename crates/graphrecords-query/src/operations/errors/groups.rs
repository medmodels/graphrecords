use crate::{
    ErrorGroup, FailureKind,
    operations::{
        ArgumentAbsent, MissingAttribute, MissingGroupAggregate, MissingTraversedAttribute,
    },
};
use graphrecords_core::graphrecord::{EdgeIndex, NodeIndex};

pub struct AbsenceErrors;

impl ErrorGroup for AbsenceErrors {
    fn name() -> &'static str {
        "AbsenceErrors"
    }

    fn contains(kind: &FailureKind) -> bool {
        kind.is::<ArgumentAbsent>()
            || kind.is::<MissingAttribute>()
            || kind.is::<MissingTraversedAttribute<NodeIndex>>()
            || kind.is::<MissingTraversedAttribute<EdgeIndex>>()
            || kind.is::<MissingGroupAggregate>()
    }
}
