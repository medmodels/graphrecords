mod arity;
mod emission;
mod shape;
mod transition;

pub use arity::{Arity, BoxedIterator, Definite, Multiple, OrderState, Ordered, Single, Unordered};
pub use emission::{Dropping, ElementEmission, Expanding, Preserving, Retention};
pub use shape::{Bare, ElementShape, Indexed, Return, ReturnShape};
pub use transition::{
    BarePipeline, ElementTransition, IndexedExpansionPipeline, IndexedToBarePipeline,
    IndexedValuePipeline, Pipeline,
};
