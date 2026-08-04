use super::{
    DynIndex, DynOperand, DynValue, OperationCapture, TRANSITION, operation_dynamic_element_apply,
};
use crate::{
    AttributeName, Bare, FailureKind, FailureKindValue, IndexValue, Indexed, Mask, Positional,
    QueryResult, Scalar, Transition,
    operations::TransitionOperation,
    registry::{ArgumentDescriptor, IndexDescriptor, LaneShapeDescriptor, ValueDescriptor},
};
use graphrecords_core::graphrecord::{EdgeIndex, GraphRecordValue, NodeIndex};

macro_rules! dynamic_value_targets {
    ($($variant:ident : $value:ty = $descriptor:expr),+ $(,)?) => {
        #[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
        pub enum DynValueTarget {
            $($variant),+
        }

        impl DynValueTarget {
            fn descriptor(self) -> ValueDescriptor {
                match self {
                    $(Self::$variant => $descriptor),+
                }
            }

            pub(crate) fn argument_descriptor(self) -> ArgumentDescriptor {
                match self {
                    $(Self::$variant => ArgumentDescriptor::selector::<$value>()),+
                }
            }
        }

        fn source_target(descriptor: &ValueDescriptor) -> Option<DynValueTarget> {
            $(
                if descriptor == &$descriptor {
                    return Some(DynValueTarget::$variant);
                }
            )+

            None
        }
    };
}

dynamic_value_targets!(
    Value: Scalar = ValueDescriptor::value::<Scalar>(),
    ValueIndex: IndexValue<GraphRecordValue> =
        ValueDescriptor::index(IndexDescriptor::domain::<GraphRecordValue>()),
    AttributeName: AttributeName = ValueDescriptor::value::<AttributeName>(),
    AttributeNameIndex: IndexValue<AttributeName> =
        ValueDescriptor::index(IndexDescriptor::domain::<AttributeName>()),
    NodeIndex: IndexValue<NodeIndex> =
        ValueDescriptor::index(IndexDescriptor::domain::<NodeIndex>()),
    EdgeIndex: IndexValue<EdgeIndex> =
        ValueDescriptor::index(IndexDescriptor::domain::<EdgeIndex>()),
    PositionalIndex: IndexValue<Positional> =
        ValueDescriptor::index(IndexDescriptor::domain::<Positional>()),
    BoolIndex: IndexValue<bool> = ValueDescriptor::index(IndexDescriptor::domain::<bool>()),
    Mask: Mask = ValueDescriptor::value::<Mask>(),
    FailureKind: FailureKindValue = ValueDescriptor::value::<FailureKindValue>(),
    FailureKindIndex: IndexValue<FailureKind> =
        ValueDescriptor::index(IndexDescriptor::domain::<FailureKind>()),
);

macro_rules! apply_retarget {
    ($input:expr, $output:expr, $source:ty, $target:ty) => {{
        let capture = OperationCapture::<TransitionOperation<$target>>::capture();
        let operation = capture.transition::<$target>().operation();
        let apply = || -> DynOperand {
            match $input.descriptor().lane_shape() {
                LaneShapeDescriptor::Indexed { .. } => {
                    operation_dynamic_element_apply!(
                        operation,
                        $input,
                        $output,
                        TransitionOperation<$target>,
                        Indexed<DynIndex, $source>,
                        Indexed<DynIndex, $target>
                    )
                }
                LaneShapeDescriptor::Bare { .. } => {
                    operation_dynamic_element_apply!(
                        operation,
                        $input,
                        $output,
                        TransitionOperation<$target>,
                        Bare<$source>,
                        Bare<$target>
                    )
                }
            }
        };

        apply()
    }};
}

impl DynOperand {
    pub fn retarget(&self, target: DynValueTarget) -> QueryResult<Self> {
        let source = source_target(self.descriptor().lane_shape().value());
        let output = self.descriptor().with_lane_value(target.descriptor());

        let operand = match (source, target) {
            (Some(DynValueTarget::Value), DynValueTarget::ValueIndex) => {
                apply_retarget!(self, output, Scalar, IndexValue<GraphRecordValue>)
            }
            (Some(DynValueTarget::Value), DynValueTarget::AttributeName) => {
                apply_retarget!(self, output, Scalar, AttributeName)
            }
            (Some(DynValueTarget::Value), DynValueTarget::NodeIndex) => {
                apply_retarget!(self, output, Scalar, IndexValue<NodeIndex>)
            }
            (Some(DynValueTarget::Value), DynValueTarget::AttributeNameIndex) => {
                apply_retarget!(self, output, Scalar, IndexValue<AttributeName>)
            }
            (Some(DynValueTarget::Value), DynValueTarget::Mask) => {
                apply_retarget!(self, output, Scalar, Mask)
            }
            (Some(DynValueTarget::Value), DynValueTarget::BoolIndex) => {
                apply_retarget!(self, output, Scalar, IndexValue<bool>)
            }
            (Some(DynValueTarget::Value), DynValueTarget::EdgeIndex) => {
                apply_retarget!(self, output, Scalar, IndexValue<EdgeIndex>)
            }
            (Some(DynValueTarget::Value), DynValueTarget::PositionalIndex) => {
                apply_retarget!(self, output, Scalar, IndexValue<Positional>)
            }
            (Some(DynValueTarget::ValueIndex), DynValueTarget::Value) => {
                apply_retarget!(self, output, IndexValue<GraphRecordValue>, Scalar)
            }
            (Some(DynValueTarget::ValueIndex), DynValueTarget::AttributeName) => {
                apply_retarget!(self, output, IndexValue<GraphRecordValue>, AttributeName)
            }
            (Some(DynValueTarget::ValueIndex), DynValueTarget::NodeIndex) => {
                apply_retarget!(
                    self,
                    output,
                    IndexValue<GraphRecordValue>,
                    IndexValue<NodeIndex>
                )
            }
            (Some(DynValueTarget::ValueIndex), DynValueTarget::AttributeNameIndex) => {
                apply_retarget!(
                    self,
                    output,
                    IndexValue<GraphRecordValue>,
                    IndexValue<AttributeName>
                )
            }
            (Some(DynValueTarget::ValueIndex), DynValueTarget::Mask) => {
                apply_retarget!(self, output, IndexValue<GraphRecordValue>, Mask)
            }
            (Some(DynValueTarget::ValueIndex), DynValueTarget::BoolIndex) => {
                apply_retarget!(self, output, IndexValue<GraphRecordValue>, IndexValue<bool>)
            }
            (Some(DynValueTarget::ValueIndex), DynValueTarget::EdgeIndex) => {
                apply_retarget!(
                    self,
                    output,
                    IndexValue<GraphRecordValue>,
                    IndexValue<EdgeIndex>
                )
            }
            (Some(DynValueTarget::ValueIndex), DynValueTarget::PositionalIndex) => {
                apply_retarget!(
                    self,
                    output,
                    IndexValue<GraphRecordValue>,
                    IndexValue<Positional>
                )
            }
            (Some(DynValueTarget::AttributeName), DynValueTarget::Value) => {
                apply_retarget!(self, output, AttributeName, Scalar)
            }
            (Some(DynValueTarget::AttributeName), DynValueTarget::ValueIndex) => {
                apply_retarget!(self, output, AttributeName, IndexValue<GraphRecordValue>)
            }
            (Some(DynValueTarget::AttributeName), DynValueTarget::NodeIndex) => {
                apply_retarget!(self, output, AttributeName, IndexValue<NodeIndex>)
            }
            (Some(DynValueTarget::AttributeName), DynValueTarget::AttributeNameIndex) => {
                apply_retarget!(self, output, AttributeName, IndexValue<AttributeName>)
            }
            (Some(DynValueTarget::AttributeName), DynValueTarget::EdgeIndex) => {
                apply_retarget!(self, output, AttributeName, IndexValue<EdgeIndex>)
            }
            (Some(DynValueTarget::AttributeName), DynValueTarget::PositionalIndex) => {
                apply_retarget!(self, output, AttributeName, IndexValue<Positional>)
            }
            (Some(DynValueTarget::NodeIndex), DynValueTarget::Value) => {
                apply_retarget!(self, output, IndexValue<NodeIndex>, Scalar)
            }
            (Some(DynValueTarget::NodeIndex), DynValueTarget::ValueIndex) => {
                apply_retarget!(
                    self,
                    output,
                    IndexValue<NodeIndex>,
                    IndexValue<GraphRecordValue>
                )
            }
            (Some(DynValueTarget::NodeIndex), DynValueTarget::AttributeName) => {
                apply_retarget!(self, output, IndexValue<NodeIndex>, AttributeName)
            }
            (Some(DynValueTarget::NodeIndex), DynValueTarget::AttributeNameIndex) => {
                apply_retarget!(
                    self,
                    output,
                    IndexValue<NodeIndex>,
                    IndexValue<AttributeName>
                )
            }
            (Some(DynValueTarget::NodeIndex), DynValueTarget::EdgeIndex) => {
                apply_retarget!(self, output, IndexValue<NodeIndex>, IndexValue<EdgeIndex>)
            }
            (Some(DynValueTarget::NodeIndex), DynValueTarget::PositionalIndex) => {
                apply_retarget!(self, output, IndexValue<NodeIndex>, IndexValue<Positional>)
            }
            (Some(DynValueTarget::AttributeNameIndex), DynValueTarget::Value) => {
                apply_retarget!(self, output, IndexValue<AttributeName>, Scalar)
            }
            (Some(DynValueTarget::AttributeNameIndex), DynValueTarget::ValueIndex) => {
                apply_retarget!(
                    self,
                    output,
                    IndexValue<AttributeName>,
                    IndexValue<GraphRecordValue>
                )
            }
            (Some(DynValueTarget::AttributeNameIndex), DynValueTarget::AttributeName) => {
                apply_retarget!(self, output, IndexValue<AttributeName>, AttributeName)
            }
            (Some(DynValueTarget::AttributeNameIndex), DynValueTarget::NodeIndex) => {
                apply_retarget!(
                    self,
                    output,
                    IndexValue<AttributeName>,
                    IndexValue<NodeIndex>
                )
            }
            (Some(DynValueTarget::AttributeNameIndex), DynValueTarget::EdgeIndex) => {
                apply_retarget!(
                    self,
                    output,
                    IndexValue<AttributeName>,
                    IndexValue<EdgeIndex>
                )
            }
            (Some(DynValueTarget::AttributeNameIndex), DynValueTarget::PositionalIndex) => {
                apply_retarget!(
                    self,
                    output,
                    IndexValue<AttributeName>,
                    IndexValue<Positional>
                )
            }
            (Some(DynValueTarget::Mask), DynValueTarget::Value) => {
                apply_retarget!(self, output, Mask, Scalar)
            }
            (Some(DynValueTarget::Mask), DynValueTarget::ValueIndex) => {
                apply_retarget!(self, output, Mask, IndexValue<GraphRecordValue>)
            }
            (Some(DynValueTarget::Mask), DynValueTarget::BoolIndex) => {
                apply_retarget!(self, output, Mask, IndexValue<bool>)
            }
            (Some(DynValueTarget::BoolIndex), DynValueTarget::Value) => {
                apply_retarget!(self, output, IndexValue<bool>, Scalar)
            }
            (Some(DynValueTarget::BoolIndex), DynValueTarget::ValueIndex) => {
                apply_retarget!(self, output, IndexValue<bool>, IndexValue<GraphRecordValue>)
            }
            (Some(DynValueTarget::BoolIndex), DynValueTarget::Mask) => {
                apply_retarget!(self, output, IndexValue<bool>, Mask)
            }
            (Some(DynValueTarget::EdgeIndex), DynValueTarget::Value) => {
                apply_retarget!(self, output, IndexValue<EdgeIndex>, Scalar)
            }
            (Some(DynValueTarget::EdgeIndex), DynValueTarget::ValueIndex) => {
                apply_retarget!(
                    self,
                    output,
                    IndexValue<EdgeIndex>,
                    IndexValue<GraphRecordValue>
                )
            }
            (Some(DynValueTarget::EdgeIndex), DynValueTarget::AttributeName) => {
                apply_retarget!(self, output, IndexValue<EdgeIndex>, AttributeName)
            }
            (Some(DynValueTarget::EdgeIndex), DynValueTarget::NodeIndex) => {
                apply_retarget!(self, output, IndexValue<EdgeIndex>, IndexValue<NodeIndex>)
            }
            (Some(DynValueTarget::EdgeIndex), DynValueTarget::AttributeNameIndex) => {
                apply_retarget!(
                    self,
                    output,
                    IndexValue<EdgeIndex>,
                    IndexValue<AttributeName>
                )
            }
            (Some(DynValueTarget::EdgeIndex), DynValueTarget::PositionalIndex) => {
                apply_retarget!(self, output, IndexValue<EdgeIndex>, IndexValue<Positional>)
            }
            (Some(DynValueTarget::PositionalIndex), DynValueTarget::Value) => {
                apply_retarget!(self, output, IndexValue<Positional>, Scalar)
            }
            (Some(DynValueTarget::PositionalIndex), DynValueTarget::ValueIndex) => {
                apply_retarget!(
                    self,
                    output,
                    IndexValue<Positional>,
                    IndexValue<GraphRecordValue>
                )
            }
            (Some(DynValueTarget::PositionalIndex), DynValueTarget::AttributeName) => {
                apply_retarget!(self, output, IndexValue<Positional>, AttributeName)
            }
            (Some(DynValueTarget::PositionalIndex), DynValueTarget::NodeIndex) => {
                apply_retarget!(self, output, IndexValue<Positional>, IndexValue<NodeIndex>)
            }
            (Some(DynValueTarget::PositionalIndex), DynValueTarget::AttributeNameIndex) => {
                apply_retarget!(
                    self,
                    output,
                    IndexValue<Positional>,
                    IndexValue<AttributeName>
                )
            }
            (Some(DynValueTarget::PositionalIndex), DynValueTarget::EdgeIndex) => {
                apply_retarget!(self, output, IndexValue<Positional>, IndexValue<EdgeIndex>)
            }
            (Some(DynValueTarget::FailureKind), DynValueTarget::FailureKindIndex) => {
                apply_retarget!(self, output, FailureKindValue, IndexValue<FailureKind>)
            }
            (Some(DynValueTarget::FailureKindIndex), DynValueTarget::FailureKind) => {
                apply_retarget!(self, output, IndexValue<FailureKind>, FailureKindValue)
            }
            _ => return self.inapplicable(TRANSITION, vec![target.argument_descriptor()]),
        };

        Ok(operand)
    }

    pub(crate) fn erase_mask_lane(&self) -> Self {
        let capture = OperationCapture::<TransitionOperation<DynValue>>::capture();
        let operation = capture.transition::<DynValue>().operation();
        let output = self
            .descriptor()
            .with_lane_value(ValueDescriptor::index(IndexDescriptor::domain::<bool>()));

        operation_dynamic_element_apply!(
            operation,
            self,
            output,
            TransitionOperation<DynValue>,
            Indexed<DynIndex, Mask>,
            Indexed<DynIndex, DynValue>
        )
    }
}
