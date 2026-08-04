mod all;
mod any;
mod count;
mod maximum;
mod mean;
mod median;
mod minimum;
mod mode;
mod product;
mod random;
mod standard_deviation;
mod sum;
mod unique_count;
mod variance;

use crate::registry::OperationManifest;
pub use all::AllOperation;
pub use any::AnyOperation;
pub use count::CountOperation;
pub use maximum::MaximumOperation;
pub use mean::MeanOperation;
pub use median::MedianOperation;
pub use minimum::MinimumOperation;
pub use mode::ModeOperation;
pub use product::ProductOperation;
pub use random::RandomOperation;
pub use standard_deviation::StandardDeviationOperation;
pub use sum::SumOperation;
pub use unique_count::UniqueCountOperation;
pub use variance::VarianceOperation;

pub(super) fn operation_manifests() -> Vec<OperationManifest> {
    vec![
        all::operation_manifest(),
        any::operation_manifest(),
        count::operation_manifest(),
        maximum::operation_manifest(),
        mean::operation_manifest(),
        median::operation_manifest(),
        minimum::operation_manifest(),
        mode::operation_manifest(),
        product::operation_manifest(),
        random::operation_manifest(),
        standard_deviation::operation_manifest(),
        sum::operation_manifest(),
        unique_count::operation_manifest(),
        variance::operation_manifest(),
    ]
}
