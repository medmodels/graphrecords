use crate::{Operand, operations::ErrorPolicy};

pub trait OnError: Operand {
    fn on_error<A: ErrorPolicy<Self>>(&self, action: A) -> A::Output;
}
