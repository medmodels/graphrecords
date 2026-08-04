mod inspection;
mod on_error;

use crate::registry::OperationManifest;
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

pub(super) fn operation_manifests() -> Vec<OperationManifest> {
    vec![
        inspection::errors::operation_manifest(),
        inspection::kind::operation_manifest(),
        inspection::name::operation_manifest(),
        on_error::drop::operation_manifest(),
        on_error::raise::operation_manifest(),
        on_error::raise::raise_when::operation_manifest(),
        on_error::replace::operation_manifest(),
    ]
}
