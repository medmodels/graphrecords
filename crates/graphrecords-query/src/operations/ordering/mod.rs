mod first;
mod last;
mod sort;
mod sort_by;
mod unorder;

pub use first::FirstOperation;
pub use last::LastOperation;
pub use sort::SortOperation;
pub use sort_by::SortByOperation;
use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
};
pub use unorder::UnorderOperation;

#[derive(Debug)]
pub struct IncomparableIndices<V: Display, I: Display> {
    pub value: V,
    pub first: I,
    pub second: I,
}

impl<V: Display, I: Display> Display for IncomparableIndices<V, I> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "cannot order elements sharing value `{}`: their indices `{}` and `{}` are not comparable",
            self.value, self.first, self.second
        )
    }
}

impl<V: Display + Debug, I: Display + Debug> Error for IncomparableIndices<V, I> {}
