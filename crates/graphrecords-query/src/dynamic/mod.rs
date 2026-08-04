pub(crate) mod argument;
pub(crate) mod dispatch;
mod index;
mod manifest;
pub(crate) mod operand;
mod optimizer;
pub(crate) mod payload;
pub(crate) mod projection;
mod retarget;
pub(crate) mod stream;
mod value;

pub(crate) use crate::element::{Dropping, Preserving};
pub(crate) use argument::{DynArgumentBuilder, DynSetLiteral, Keyable};
pub use argument::{DynArgumentSource, DynCastTarget, DynInvokeArgument};
pub(crate) use dispatch::{
    DynApplier, DynEntityDomain, DynGroupedOperationContext, DynLaneKind, OperationCapture,
    apply_group_operation, apply_grouped_operation, apply_lane_operation, entity_domain,
    innermost_lane_kind, invoke_argument_source, invoke_attribute, invoke_direction, invoke_group,
    invoke_operand, invoke_position,
};
pub(crate) use graphrecords_core::graphrecord::{EdgeIndex, NodeIndex};
pub use index::{DynExpandedOwned, DynExpandedRef, DynIndex, DynIndexOwned, DynIndexRef};
pub(crate) use manifest::{
    operation_dynamic_alias, operation_dynamic_aliases, operation_dynamic_argument_type,
    operation_dynamic_argument_value, operation_dynamic_arguments, operation_dynamic_capture,
    operation_dynamic_element_apply, operation_dynamic_element_build,
    operation_dynamic_element_entity, operation_dynamic_entity_dispatch,
    operation_dynamic_expansion_apply, operation_dynamic_field,
    operation_dynamic_group_all_arities, operation_dynamic_group_apply,
    operation_dynamic_group_build, operation_dynamic_lane_apply, operation_dynamic_lane_build,
    operation_dynamic_lane_entity, operation_dynamic_lane_function,
    operation_dynamic_receiver_dispatch, operation_dynamic_selected_argument,
    operation_dynamic_set_arity, operation_dynamic_set_build, operation_dynamic_shape_apply,
    operation_dynamic_via_dispatch, operation_element_applier, operation_group_applier,
    operation_lane_applier,
};
pub(crate) use operand::{
    DynArityHandle, DynGroupHandle, DynHandle, DynLaneHandle, DynLaneState, IntoDynArityHandle,
    IntoDynLaneHandle, IntoDynOperand, TRANSITION,
};
pub use operand::{DynExplanation, DynOperand, query_edges, query_nodes};
pub use optimizer::register_dyn_builtins;
pub(crate) use payload::{DynPayload, DynYield};
pub use payload::{DynTerminal, DynTerminalArity, DynTerminalLane};
pub(crate) use projection::{
    DynElementOperation, DynExpansionOperation, DynGroupOperation, DynLaneOperation,
    DynOperandProjection, DynPayloadOutput, DynShapeProjection,
};
pub use retarget::DynValueTarget;
pub(crate) use stream::{DynArity, DynStreamShape};
pub use stream::{DynArityStream, DynStream};
pub use value::{DynEntityReference, DynEquivalenceKey, DynValue};
