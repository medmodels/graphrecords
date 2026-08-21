mod capability;
mod descriptor;
mod emission;
pub(crate) mod manifest;
mod pattern;
mod template;

pub use capability::{CapabilityIdentifier, CapabilityRegistry};
pub use descriptor::{
    ArgumentDescriptor, ArgumentMissingPolicy, ArgumentValueSource, ArityDescriptor,
    DomainDescriptor, ExpressionDescriptor, IndexDescriptor, LaneShapeDescriptor, OrderDescriptor,
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
    FloorCapability, GroupKeyWitness, GroupMemberWitness, GroupMembershipWitness,
    GroupingCapability, IndexWitness, KindTestCapability, LogarithmCapability, MedianCapability,
    ModeCapability, ModuloCapability, MultiplyCapability, NegateCapability, OperationManifest,
    OperationManifestEntry, OrderingCapability, PowerCapability, RoundCapability, ScalarCapability,
    ScalarKindTestCapability, SetSourceWitness, SignCapability, SortableCapability,
    SquareRootCapability, StringCapability, SubtractCapability, TransitionAttributeNameCapability,
    TransitionAttributeNameIndexCapability, TransitionBoolIndexCapability,
    TransitionFailureKindIndexCapability, TransitionFailureKindValueCapability,
    TransitionGroupIndexCapability, TransitionMaskCapability, TransitionNodeIndexCapability,
    TransitionPositionalIndexCapability, TransitionScalarCapability,
    TransitionValueIndexCapability, ValueDomainCapability, ValueDomainOnly, ValueWitness, describe,
    operation_manifest,
};
pub use pattern::{
    AlignmentDescriptor, ArgumentPattern, ArityPattern, Bindings, CapabilitySet, ExpressionPattern,
    IndexPattern, OrderPattern, RetentionPattern, ShapePattern, ValuePattern, VariableIdentifier,
};
pub use template::{
    ArityDescriptorTemplate, ExpressionDescriptorTemplate, IndexDescriptorTemplate,
    LaneShapeDescriptorTemplate, OrderDescriptorTemplate, ValueDescriptorTemplate,
};
