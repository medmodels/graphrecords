mod drop;
mod raise;
mod replace;

use crate::{Operand, traits::OnError};
pub use drop::Drop;
pub use raise::Raise;
pub use replace::Replace;

pub trait ErrorPolicy<I: Operand>: Clone + 'static {
    type Output: Operand;

    fn build(&self, input: I) -> Self::Output;
}

impl<O: Operand> OnError for O {
    fn on_error<A: ErrorPolicy<Self>>(&self, action: A) -> A::Output {
        action.build(self.clone())
    }
}
