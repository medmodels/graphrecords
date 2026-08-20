macro_rules! operation_dynamic_shape_apply {
    ($operation_value:expr, $input:expr, $output:expr, $dynamic_shape:ty) => {{
        let operation = $operation_value;
        let output = $output;

        let $crate::dynamic::DynHandle::Lane(handle) = &$input.handle else {
            return match $input.descriptor().lane_arity() {
                $crate::registry::ArityDescriptor::Multiple {
                    order: $crate::registry::OrderDescriptor::Ordered,
                } => $crate::dynamic::apply_grouped_operation::<
                    $dynamic_shape,
                    $crate::Multiple<$crate::Ordered>,
                    _,
                >($input, operation, output),
                $crate::registry::ArityDescriptor::Multiple {
                    order: $crate::registry::OrderDescriptor::Unordered,
                } => $crate::dynamic::apply_grouped_operation::<
                    $dynamic_shape,
                    $crate::Multiple<$crate::Unordered>,
                    _,
                >($input, operation, output),
                $crate::registry::ArityDescriptor::Single => {
                    $crate::dynamic::apply_grouped_operation::<$dynamic_shape, $crate::Single, _>(
                        $input, operation, output,
                    )
                }
                $crate::registry::ArityDescriptor::Definite => {
                    $crate::dynamic::apply_grouped_operation::<$dynamic_shape, $crate::Definite, _>(
                        $input, operation, output,
                    )
                }
            };
        };

        let handles = <$dynamic_shape as $crate::dynamic::DynLaneState>::handles(handle);

        match handles {
            $crate::dynamic::DynArityHandle::MultipleOrdered(_) => {
                $crate::dynamic::apply_lane_operation::<
                    $dynamic_shape,
                    $crate::Multiple<$crate::Ordered>,
                    _,
                >(handles, operation, output)
            }
            $crate::dynamic::DynArityHandle::MultipleUnordered(_) => {
                $crate::dynamic::apply_lane_operation::<
                    $dynamic_shape,
                    $crate::Multiple<$crate::Unordered>,
                    _,
                >(handles, operation, output)
            }
            $crate::dynamic::DynArityHandle::Single(_) => {
                $crate::dynamic::apply_lane_operation::<$dynamic_shape, $crate::Single, _>(
                    handles, operation, output,
                )
            }
            $crate::dynamic::DynArityHandle::Definite(_) => {
                $crate::dynamic::apply_lane_operation::<$dynamic_shape, $crate::Definite, _>(
                    handles, operation, output,
                )
            }
        }
    }};
}

macro_rules! operation_dynamic_element_apply {
    (
        $operation_value:ident, $input:expr, $output:expr, $operation:ty, $shape:ty, $out_shape:ty
    ) => {{
        type Emission = <$operation as $crate::operations::ElementKernel<$shape>>::Emission;
        type DynamicOperation =
            $crate::dynamic::DynElementOperation<$operation, $shape, $out_shape, Emission>;
        type DynamicShape = <$shape as $crate::dynamic::DynShapeProjection>::Dynamic;

        $crate::dynamic::operation_dynamic_shape_apply!(
            DynamicOperation::new($operation_value),
            $input,
            $output,
            DynamicShape
        )
    }};
}

pub(crate) use operation_dynamic_element_apply;
pub(crate) use operation_dynamic_shape_apply;
