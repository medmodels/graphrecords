mod groups;
mod inspection;
mod on_error;

pub use groups::AbsenceErrors;
pub use inspection::{
    ErrorKindNameOperation, ErrorKindOperation, ErrorsOperation, HasErrorCauseOperation,
    InErrorGroupOperation, IsErrorKindOperation,
};
pub use on_error::{
    Drop, ErrorPolicy, ErrorPolicyIn, ErrorPolicyOf, ErrorPolicyWithCause, Raise, RaiseWhen,
    Replace,
};
