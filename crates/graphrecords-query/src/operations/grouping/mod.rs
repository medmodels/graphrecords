mod broadcast;
mod group_by;
mod having;
mod keys;
mod ungroup;
mod ungroup_keyed;

use crate::Diagnostic;
pub use broadcast::BroadcastOperation;
pub use group_by::{GroupByOperation, GroupKey, KeyOperand};
pub use having::HavingOperation;
pub use keys::KeysOperation;
use std::{
    error::Error,
    fmt::{self, Display, Formatter},
};
pub use ungroup::UngroupOperation;
pub use ungroup_keyed::UngroupKeyedOperation;

#[derive(Debug)]
pub struct MissingGroupAggregate;

impl Display for MissingGroupAggregate {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("no aggregate value for the element's group")
    }
}

impl Error for MissingGroupAggregate {}

impl Diagnostic for MissingGroupAggregate {
    fn name() -> &'static str {
        "MissingGroupAggregate"
    }

    fn help(&self) -> Option<String> {
        Some(
            "ensure every group produces a value or handle the gap with `on_error(...)`"
                .to_string(),
        )
    }
}
