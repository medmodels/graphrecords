mod capability;
mod descriptor;
mod emission;
pub(crate) mod manifest;
mod pattern;
mod template;

pub use capability::{CapabilityIdentifier, CapabilityRegistry};
pub use descriptor::{
    ArgumentDescriptor, ArgumentMissingPolicy, ArgumentValueSource, ArityDescriptor,
    DomainDescriptor, IndexDescriptor, LaneShapeDescriptor, OperandDescriptor, OrderDescriptor,
    RetentionDescriptor, ValueArgumentDescriptor, ValueDescriptor, ValueRole,
};
pub(crate) use emission::OutArityTable;
pub use emission::{EmissionKind, EmissionSpec};
pub use manifest::OperationRegistry;
pub(crate) use manifest::{
    AbsoluteCapability, AddCapability, ArgumentWitness, ArityWitness, BareValueCapability,
    CastBoolCapability, CastDateTimeCapability, CastDurationCapability, CastFloatCapability,
    CastIntCapability, CastStringCapability, CeilCapability, ClipCapability, CubeRootCapability,
    DivideCapability, ElementShapeWitness, EntityAttributesWitness, EntityWitness,
    EnumerableArityWitness, EqualityCapability, EquivalenceCapability, ExponentialCapability,
    FloorCapability, GroupKeyWitness, GroupMemberWitness, GroupingCapability, IndexWitness,
    IndicesInGroupWitness, IntCapability, KindTestCapability, LogarithmCapability,
    MedianCapability, ModeCapability, ModuloCapability, MultiplyCapability, NegateCapability,
    OperationManifest, OperationManifestEntry, OrderingCapability, PowerCapability,
    RoundCapability, ScalarCapability, ScalarKindTestCapability, SetSourceWitness, SignCapability,
    SortableCapability, SortableIndexWitness, SquareRootCapability, StringCapability,
    SubtractCapability, ValueDomainCapability, ValueDomainOnly, ValueWitness, describe,
    manifest_entry_alias, manifest_entry_aliases, manifest_entry_argument_pattern,
    manifest_entry_set_argument_pattern, manifest_witness_alias, manifest_witness_argument_alias,
    manifest_witness_set_argument_alias, operation_element_entry, operation_element_method,
    operation_element_witness, operation_group_entry, operation_group_witness,
    operation_lane_entry, operation_lane_witness, operation_manifest, operation_manifest_name,
    operation_policy_method, operation_value_capability_marker, operation_value_capability_witness,
};
pub use pattern::{
    AlignmentDescriptor, ArgumentPattern, ArityPattern, Bindings, CapabilitySet, IndexPattern,
    OrderPattern, ShapePattern, StatePattern, ValuePattern, VariableIdentifier,
};
pub use template::{
    ArityDescriptorTemplate, IndexDescriptorTemplate, LaneShapeDescriptorTemplate,
    OperandDescriptorTemplate, OrderDescriptorTemplate, ValueDescriptorTemplate,
};
