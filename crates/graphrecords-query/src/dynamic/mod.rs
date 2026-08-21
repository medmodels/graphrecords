pub(crate) mod argument;
pub(crate) mod dispatch;
pub(crate) mod expression;
mod index;
mod optimizer;
pub(crate) mod payload;
pub(crate) mod projection;
mod selection;
pub(crate) mod stream;
mod value;

pub(crate) use crate::element::{Dropping, Preserving};
pub(crate) use argument::{DynArgumentBuilder, DynSetLiteral, Keyable};
pub use argument::{
    DynArgumentLane, DynArgumentSource, DynCastTarget, DynInvokeArgument, DynValueTarget,
};
pub(crate) use dispatch::{
    DynApplier, DynEntityDomain, DynGroupedOperationContext, DynLaneKind, OperationCapture,
    apply_group_operation, apply_grouped_operation, apply_lane_operation, entity_domain,
    innermost_lane_kind, invoke_argument_source, invoke_attribute, invoke_direction, invoke_group,
    invoke_lane, invoke_position,
};
pub(crate) use expression::{
    DynArityHandle, DynGroupHandle, DynHandle, DynLaneHandle, DynLaneState, IntoDynArityHandle,
    IntoDynExpression, IntoDynLaneHandle,
};
pub use expression::{DynExplanation, DynExpression, edges, groups, nodes};
pub(crate) use graphrecords_core::graphrecord::{EdgeIndex, GroupIndex, NodeIndex};
pub use index::{
    DynExpandedAddress, DynExpandedOwned, DynExpandedView, DynIndex, DynIndexAddress,
    DynIndexOwned, DynIndexView,
};
pub use optimizer::register_dyn_builtins;
pub use payload::{
    DynArityContainer, DynTerminal, DynTerminalBucket, DynTerminalKeyFailure, DynTerminalLane,
    DynTerminalPartition, DynTerminalPartitionParts,
};
pub(crate) use payload::{DynPayload, DynYield};
pub(crate) use projection::{
    DynElementOperation, DynExpansionOperation, DynExpressionProjection, DynGroupOperation,
    DynLaneOperation, DynPayloadOutput, DynShapeProjection,
};
pub(crate) use stream::{DynArity, DynStreamShape};
pub use stream::{DynArityStream, DynStream};
pub use value::{
    DynCachedValue, DynEntityRef, DynEntityReference, DynEntityReferenceKind, DynEquivalenceKey,
    DynValue, DynValueView,
};
