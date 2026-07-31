mod is_bool;
mod is_datetime;
mod is_duration;
mod is_float;
mod is_int;
mod is_null;
mod is_string;

use crate::registry::OperationManifest;
pub use is_bool::IsBoolOperation;
pub use is_datetime::IsDateTimeOperation;
pub use is_duration::IsDurationOperation;
pub use is_float::IsFloatOperation;
pub use is_int::IsIntOperation;
pub use is_null::IsNullOperation;
pub use is_string::IsStringOperation;

pub(super) fn operation_manifests() -> Vec<OperationManifest> {
    vec![
        is_bool::operation_manifest(),
        is_datetime::operation_manifest(),
        is_duration::operation_manifest(),
        is_float::operation_manifest(),
        is_int::operation_manifest(),
        is_null::operation_manifest(),
        is_string::operation_manifest(),
    ]
}
