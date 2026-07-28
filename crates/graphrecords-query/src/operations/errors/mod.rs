mod groups;
mod inspection;
mod on_error;

pub use groups::AbsenceErrors;
pub use inspection::{
    ErrorKindNameOperation, ErrorKindOperation, ErrorsOperation, HasErrorCauseOperation,
    InErrorGroupOperation, IsErrorKindOperation,
};
pub use on_error::{
    Drop, DropErrorsIn, DropErrorsOf, DropErrorsWithCause, ErrorPolicy, ErrorPolicyIn,
    ErrorPolicyOf, ErrorPolicyWithCause, Raise, RaiseErrorsIn, RaiseErrorsOf, RaiseErrorsWithCause,
    RaiseWhen, RaiseWhenErrorsIn, RaiseWhenErrorsOf, RaiseWhenErrorsWithCause, Replace,
    ReplaceErrorsIn, ReplaceErrorsOf, ReplaceErrorsWithCause,
};
