macro_rules! operation_dynamic_alias {
    ($order:ty, $entity:ty, $shape:ty, $arity:ty, $name:ident,IndexDomain) => {
        type $name = $crate::dynamic::DynIndex;
    };
    ($order:ty, $entity:ty, $shape:ty, $arity:ty, $name:ident,EntityDomain) => {
        type $name = $entity;
    };
    ($order:ty, $entity:ty, $shape:ty, $arity:ty, $name:ident,EntityAttributes) => {
        type $name = $entity;
    };
    ($order:ty, $entity:ty, $shape:ty, $arity:ty, $name:ident,IndicesInGroup) => {
        type $name = $entity;
    };
    ($order:ty, $entity:ty, $shape:ty, $arity:ty, $name:ident,EnsureSortable) => {
        type $name = $crate::dynamic::DynIndex;
    };
    ($order:ty, $entity:ty, $shape:ty, $arity:ty, $name:ident,ElementShape) => {
        type $name = $shape;
    };
    ($order:ty, $entity:ty, $shape:ty, $arity:ty, $name:ident,OrderState) => {
        type $name = $order;
    };
    ($order:ty, $entity:ty, $shape:ty, $arity:ty, $name:ident,Arity) => {
        type $name = $arity;
    };
    ($order:ty, $entity:ty, $shape:ty, $arity:ty, $name:ident,Lane) => {
        type $name = $crate::dynamic::DynPayload;
    };
    (
        $order:ty,
        $entity:ty,
        $shape:ty,
        $arity:ty,
        $name:ident,
        $bound:ident
        $(< $target:ident >)?
        $(+ $additional:ident)*
    ) => {
        type $name = <$shape as $crate::ElementShape>::ValueDomain;
    };
}

macro_rules! operation_dynamic_receiver_dispatch {
    (
        $input:expr,
        [$build:ident] ($($arguments:tt)*),
        [$(($name:ident, $bound:ident $(<$target:ident>)? $(+ $additional:ident)*))*]
    ) => {
        $crate::dynamic::operation_dynamic_receiver_dispatch!(
            @scan
            $input,
            [$build] ($($arguments)*),
            [$(($name, $bound $(<$target>)? $(+ $additional)*))*]
        )
    };
    (
        @scan
        $input:expr,
        [$build:ident] ($($arguments:tt)*),
        [($name:ident, ValueDomain $(<$target:ident>)? $(+ $additional:ident)*) $($remaining:tt)*]
    ) => {
        $crate::dynamic::operation_dynamic_receiver_dispatch!(
            @with_mask_or_unit $input, [$build] ($($arguments)*)
        )
    };
    (
        @scan
        $input:expr,
        [$build:ident] ($($arguments:tt)*),
        [($name:ident, BareValueDomain $(<$target:ident>)? $(+ $additional:ident)*) $($remaining:tt)*]
    ) => {
        $crate::dynamic::operation_dynamic_receiver_dispatch!(
            @with_mask $input, [$build] ($($arguments)*)
        )
    };
    (
        @scan
        $input:expr,
        [$build:ident] ($($arguments:tt)*),
        [($name:ident, GroupingValue $(<$target:ident>)? $(+ $additional:ident)*) $($remaining:tt)*]
    ) => {
        $crate::dynamic::operation_dynamic_receiver_dispatch!(
            @with_mask $input, [$build] ($($arguments)*)
        )
    };
    (
        @scan
        $input:expr,
        [$build:ident] ($($arguments:tt)*),
        [($name:ident, ValueEquality $(<$target:ident>)? $(+ $additional:ident)*) $($remaining:tt)*]
    ) => {
        $crate::dynamic::operation_dynamic_receiver_dispatch!(
            @with_mask $input, [$build] ($($arguments)*)
        )
    };
    (
        @scan
        $input:expr,
        [$build:ident] ($($arguments:tt)*),
        [($name:ident, ValueEquivalence $(<$target:ident>)? $(+ $additional:ident)*) $($remaining:tt)*]
    ) => {
        $crate::dynamic::operation_dynamic_receiver_dispatch!(
            @with_mask $input, [$build] ($($arguments)*)
        )
    };
    (
        @scan
        $input:expr,
        [$build:ident] ($($arguments:tt)*),
        [($name:ident, ValueMode $(<$target:ident>)? $(+ $additional:ident)*) $($remaining:tt)*]
    ) => {
        $crate::dynamic::operation_dynamic_receiver_dispatch!(
            @with_mask $input, [$build] ($($arguments)*)
        )
    };
    (
        @scan
        $input:expr,
        [$build:ident] ($($arguments:tt)*),
        [($name:ident, $bound:ident $(<$target:ident>)? $(+ $additional:ident)*) $($remaining:tt)*]
    ) => {
        $crate::dynamic::operation_dynamic_receiver_dispatch!(
            @scan $input, [$build] ($($arguments)*), [$($remaining)*]
        )
    };
    (@scan $input:expr, [$build:ident] ($($arguments:tt)*), []) => {
        $crate::dynamic::$build!($crate::dynamic::DynValue, $($arguments)*)
    };
    (@with_mask $input:expr, [$build:ident] ($($arguments:tt)*)) => {
        match $crate::dynamic::innermost_lane_kind($input.descriptor()) {
            $crate::dynamic::DynLaneKind::IndexedMask
            | $crate::dynamic::DynLaneKind::BareMask => {
                $crate::dynamic::$build!($crate::Mask, $($arguments)*)
            }
            _ => $crate::dynamic::$build!($crate::dynamic::DynValue, $($arguments)*),
        }
    };
    (@with_mask_or_unit $input:expr, [$build:ident] ($($arguments:tt)*)) => {
        match $crate::dynamic::innermost_lane_kind($input.descriptor()) {
            $crate::dynamic::DynLaneKind::IndexedMask
            | $crate::dynamic::DynLaneKind::BareMask => {
                $crate::dynamic::$build!($crate::Mask, $($arguments)*)
            }
            $crate::dynamic::DynLaneKind::IndexedUnit => {
                $crate::dynamic::$build!($crate::Unit, $($arguments)*)
            }
            _ => $crate::dynamic::$build!($crate::dynamic::DynValue, $($arguments)*),
        }
    };
}

macro_rules! operation_dynamic_aliases {
    (
        $order:ty,
        $entity:ty,
        $shape:ty,
        $arity:ty;
        [$(($name:ident, $bound:ident $(<$target:ident>)? $(+ $additional:ident)*))*]
    ) => {
        $(
            $crate::dynamic::operation_dynamic_alias!(
                $order,
                $entity,
                $shape,
                $arity,
                $name,
                $bound $(<$target>)? $(+ $additional)*
            );
        )*

        let _ = std::marker::PhantomData::<fn() -> ($($name,)*)>;
    };
    (
        $order:ty,
        $entity:ty,
        $shape:ty,
        $arity:ty;
        $(
            $name:ident : $bound:ident $(<$target:ident>)? $(+ $additional:ident)*
        ),* $(,)?
    ) => {
        $(
            $crate::dynamic::operation_dynamic_alias!(
                $order,
                $entity,
                $shape,
                $arity,
                $name,
                $bound $(<$target>)? $(+ $additional)*
            );
        )*

        let _ = std::marker::PhantomData::<fn() -> ($($name,)*)>;
    };
}

macro_rules! operation_dynamic_argument_type {
    ($alignment:ty, $value:ty, $retention:ty) => {
        $crate::operations::Argument<
            $alignment,
            <$value as $crate::dynamic::DynArgumentBuilder<$alignment, $retention>>::Dynamic,
            $retention,
        >
    };
    ($alignment:ty, $retention:ty) => {
        $crate::operations::Argument<
            $alignment,
            $crate::dynamic::DynValue,
            $retention,
        >
    };
}

macro_rules! operation_dynamic_argument_value {
    ($source:expr, $alignment:ty, $value:ty, $retention:ty) => {
        <$value as $crate::dynamic::DynArgumentBuilder<$alignment, $retention>>::build($source)
    };
    ($source:expr, $alignment:ty, $retention:ty) => {{
        type DynamicValue = $crate::dynamic::DynValue;

        <DynamicValue as $crate::dynamic::DynArgumentBuilder<$alignment, $retention>>::build(
            $source,
        )
    }};
}

macro_rules! operation_dynamic_capture {
    (
        $operation:ty,
        $method:ident,
        $arguments:expr; policy[$policy:path]; selector[]; receiver[]; fields[]; values[]
    ) => {{
        let _ = $arguments;
        let capture = $crate::dynamic::OperationCapture::<$operation>::capture();
        let captured = capture.$method($policy);
        captured.operation()
    }};
    (
        $operation:ty,
        $method:ident,
        $arguments:expr; policy[$policy:path = $type:ident:: $function:ident($argument:ident)]; selector[]; receiver[]; fields[]; values[$first:ident]
    ) => {{
        let capture = $crate::dynamic::OperationCapture::<$operation>::capture();
        let captured = capture.$method($type::$function($first));
        captured.operation()
    }};
    (
        $operation:ty,
        $method:ident,
        $arguments:expr; policy[$policy:path = $receiver:ident. $function:ident($argument:ident)]; selector[]; receiver[]; fields[]; values[$first:ident]
    ) => {{
        let capture = $crate::dynamic::OperationCapture::<$operation>::capture();
        let captured = capture.$method($receiver.$function($first));
        captured.operation()
    }};
    (
        $operation:ty,
        $method:ident,
        $arguments:expr; policy[]; selector[$selector:ident]; receiver[]; fields[]; values[]
    ) => {{
        let _ = $arguments;
        let capture = $crate::dynamic::OperationCapture::<$operation>::capture();
        let captured = capture.$method($selector);
        captured.operation()
    }};
    (
        $operation:ty,
        $method:ident,
        $arguments:expr; policy[]; selector[]; receiver[$receiver:ident]; fields[]; values[$first:ident]
    ) => {{
        let capture = $crate::dynamic::OperationCapture::<$operation>::capture();
        let captured = $first.$method(&capture);
        captured.operation()
    }};
    (
        $operation:ty,
        $method:ident,
        $arguments:expr; policy[]; selector[]; receiver[]; fields[]; values[]
    ) => {{
        let _ = $arguments;
        let capture = $crate::dynamic::OperationCapture::<$operation>::capture();
        let captured = capture.$method();
        captured.operation()
    }};
    (
        $operation:ty,
        $method:ident,
        $arguments:expr; policy[]; selector[]; receiver[]; fields[]; values[$first:ident]
    ) => {{
        let capture = $crate::dynamic::OperationCapture::<$operation>::capture();
        let captured = capture.$method($first);
        captured.operation()
    }};
    (
        $operation:ty,
        $method:ident,
        $arguments:expr; policy[]; selector[]; receiver[]; fields[]; values[$first:ident, $second:ident]
    ) => {{
        let capture = $crate::dynamic::OperationCapture::<$operation>::capture();
        let captured = capture.$method($first, $second);
        captured.operation()
    }};
    (
        $operation:ty,
        $method:ident,
        $arguments:expr; policy[]; selector[]; receiver[]; fields[$field:ident : $field_type:ident]; values[]
    ) => {{
        let capture = $crate::dynamic::OperationCapture::<$operation>::capture();
        let field = $crate::dynamic::operation_dynamic_field!($arguments, 0, $field_type);
        let captured = capture.$method(field);
        captured.operation()
    }};
    (
        $operation:ty,
        $method:ident,
        $arguments:expr; policy[]; selector[]; receiver[]; fields[$first_field:ident : $first_type:ident, $second_field:ident : $second_type:ident]; values[]
    ) => {{
        let capture = $crate::dynamic::OperationCapture::<$operation>::capture();
        let first_field = $crate::dynamic::operation_dynamic_field!($arguments, 0, $first_type);
        let second_field = $crate::dynamic::operation_dynamic_field!($arguments, 1, $second_type);
        let captured = capture.$method(first_field, second_field);
        captured.operation()
    }};
}

macro_rules! operation_dynamic_field {
    ($arguments:expr, $position:expr,GraphRecordAttribute) => {
        $crate::dynamic::invoke_attribute($arguments, $position)
    };
    ($arguments:expr, $position:expr,Group) => {
        $crate::dynamic::invoke_group($arguments, $position)
    };
    ($arguments:expr, $position:expr,EdgeDirection) => {
        $crate::dynamic::invoke_direction($arguments, $position)
    };
    ($arguments:expr, $position:expr,usize) => {
        $crate::dynamic::invoke_position($arguments, $position)
    };
}

macro_rules! operation_dynamic_selected_argument {
    (
        $operation:ty,
        $method:ident,
        $input:expr,
        $arguments:expr,
        $output:expr,
        [$apply:path] ($($apply_arguments:tt)*),
        policy[$($policy:tt)*],
        selector[$($selector:ident)?],
        receiver[$($receiver:ident)?],
        fields[$($field:ident : $field_type:ident),*],
        argument[$name:ident, $alignment:ty],
        value[$($value:ident)?],
        masked[$unmasked:tt]
    ) => {{
        let source = $crate::dynamic::invoke_argument_source($arguments, 0);

        if source.is_mask() {
            $(type $value = $crate::Mask;)?

            $crate::dynamic::operation_dynamic_selected_argument!(
                $operation,
                $method,
                $input,
                $arguments,
                $output,
                [$apply] ($($apply_arguments)*),
                policy[$($policy)*],
                selector[$($selector)?],
                receiver[$($receiver)?],
                fields[$($field : $field_type),*],
                argument[$name, $alignment, $crate::Mask],
                direct
            )
        } else {
            $(type $value = $crate::dynamic::DynValue;)?

            $crate::dynamic::operation_dynamic_selected_argument!(
                $operation,
                $method,
                $input,
                $arguments,
                $output,
                [$apply] ($($apply_arguments)*),
                policy[$($policy)*],
                selector[$($selector)?],
                receiver[$($receiver)?],
                fields[$($field : $field_type),*],
                argument[$name, $alignment, $crate::dynamic::DynValue],
                $unmasked
            )
        }
    }};
    (
        $operation:ty,
        $method:ident,
        $input:expr,
        $arguments:expr,
        $output:expr,
        [$apply:path] ($($apply_arguments:tt)*),
        policy[$($policy:tt)*],
        selector[$($selector:ident)?],
        receiver[$($receiver:ident)?],
        fields[$($field:ident : $field_type:ident),*],
        argument[$name:ident, $alignment:ty, $value:ty],
        direct
    ) => {{
        let source = $crate::dynamic::invoke_argument_source($arguments, 0);

        if source.is_dropping() {
            type $name = $crate::dynamic::operation_dynamic_argument_type!(
                $alignment,
                $value,
                $crate::dynamic::Dropping
            );

            let first_argument = $crate::dynamic::operation_dynamic_argument_value!(
                source,
                $alignment,
                $value,
                $crate::dynamic::Dropping
            );
            let operation = $crate::dynamic::operation_dynamic_capture!(
                $operation,
                $method,
                $arguments;
                policy[$($policy)*];
                selector[$($selector)?];
                receiver[$($receiver)?];
                fields[$($field : $field_type),*];
                values[first_argument]
            );

            $apply!(operation, $input, $output, $($apply_arguments)*)
        } else {
            type $name = $crate::dynamic::operation_dynamic_argument_type!(
                $alignment,
                $value,
                $crate::dynamic::Preserving
            );

            let first_argument = $crate::dynamic::operation_dynamic_argument_value!(
                source,
                $alignment,
                $value,
                $crate::dynamic::Preserving
            );
            let operation = $crate::dynamic::operation_dynamic_capture!(
                $operation,
                $method,
                $arguments;
                policy[$($policy)*];
                selector[$($selector)?];
                receiver[$($receiver)?];
                fields[$($field : $field_type),*];
                values[first_argument]
            );

            $apply!(operation, $input, $output, $($apply_arguments)*)
        }
    }};
    (
        $operation:ty,
        $method:ident,
        $input:expr,
        $arguments:expr,
        $output:expr,
        [$apply:path] ($($apply_arguments:tt)*),
        policy[$($policy:tt)*],
        selector[$($selector:ident)?],
        receiver[$($receiver:ident)?],
        fields[$($field:ident : $field_type:ident),*],
        argument[$name:ident, $alignment:ty, $value:ty],
        keyable
    ) => {{
        let source = $crate::dynamic::invoke_argument_source($arguments, 0);

        if source.is_dropping() {
            type DynamicArgument = $crate::dynamic::operation_dynamic_argument_type!(
                $alignment,
                $value,
                $crate::dynamic::Dropping
            );
            type $name = $crate::dynamic::Keyable<DynamicArgument>;

            let argument = $crate::dynamic::operation_dynamic_argument_value!(
                source,
                $alignment,
                $value,
                $crate::dynamic::Dropping
            );
            let first_argument = $crate::dynamic::Keyable::new(argument);
            let operation = $crate::dynamic::operation_dynamic_capture!(
                $operation,
                $method,
                $arguments;
                policy[$($policy)*];
                selector[$($selector)?];
                receiver[$($receiver)?];
                fields[$($field : $field_type),*];
                values[first_argument]
            );

            $apply!(operation, $input, $output, $($apply_arguments)*)
        } else {
            type DynamicArgument = $crate::dynamic::operation_dynamic_argument_type!(
                $alignment,
                $value,
                $crate::dynamic::Preserving
            );
            type $name = $crate::dynamic::Keyable<DynamicArgument>;

            let argument = $crate::dynamic::operation_dynamic_argument_value!(
                source,
                $alignment,
                $value,
                $crate::dynamic::Preserving
            );
            let first_argument = $crate::dynamic::Keyable::new(argument);
            let operation = $crate::dynamic::operation_dynamic_capture!(
                $operation,
                $method,
                $arguments;
                policy[$($policy)*];
                selector[$($selector)?];
                receiver[$($receiver)?];
                fields[$($field : $field_type),*];
                values[first_argument]
            );

            $apply!(operation, $input, $output, $($apply_arguments)*)
        }
    }};
}

macro_rules! operation_dynamic_arguments {
    (
        $operation:ty,
        $method:ident,
        $input:expr,
        $arguments:expr,
        $output:expr,
        [$apply:path] ($($apply_arguments:tt)*),
        policy[$($policy:tt)*],
        selector[$($selector:ident)?],
        receiver[$($receiver:ident)?],
        fields[$($field:ident : $field_type:ident),*],
        arguments[]
    ) => {{
        let operation = $crate::dynamic::operation_dynamic_capture!(
            $operation,
            $method,
            $arguments;
            policy[$($policy)*];
            selector[$($selector)?];
            receiver[$($receiver)?];
            fields[$($field : $field_type),*];
            values[]
        );
        $apply!(operation, $input, $output, $($apply_arguments)*)
    }};
    (
        $operation:ty,
        $method:ident,
        $input:expr,
        $arguments:expr,
        $output:expr,
        [$apply:path] ($($apply_arguments:tt)*),
        policy[$($policy:tt)*],
        selector[$($selector:ident)?],
        receiver[$($receiver:ident)?],
        fields[$($field:ident : $field_type:ident),*],
        arguments[($name:ident, $alignment:ty, $value:ty; $retention:ty)]
    ) => {{
        type $name = $crate::dynamic::operation_dynamic_argument_type!(
            $alignment, $value, $retention
        );

        let source = $crate::dynamic::invoke_argument_source($arguments, 0);
        let first_argument = $crate::dynamic::operation_dynamic_argument_value!(
            source, $alignment, $value, $retention
        );
        let operation = $crate::dynamic::operation_dynamic_capture!(
            $operation,
            $method,
            $arguments;
            policy[$($policy)*];
            selector[$($selector)?];
            receiver[$($receiver)?];
            fields[$($field : $field_type),*];
            values[first_argument]
        );
        $apply!(operation, $input, $output, $($apply_arguments)*)
    }};
    (
        $operation:ty,
        sort_by,
        $input:expr,
        $arguments:expr,
        $output:expr,
        [$apply:path] ($($apply_arguments:tt)*),
        policy[$($policy:tt)*],
        selector[$($selector:ident)?],
        receiver[$($receiver:ident)?],
        fields[$($field:ident : $field_type:ident),*],
        arguments[($name:ident, $alignment:ty)]
    ) => {
        $crate::dynamic::operation_dynamic_selected_argument!(
            $operation,
            sort_by,
            $input,
            $arguments,
            $output,
            [$apply] ($($apply_arguments)*),
            policy[$($policy)*],
            selector[$($selector)?],
            receiver[$($receiver)?],
            fields[$($field : $field_type),*],
            argument[$name, $alignment],
            value[],
            masked[direct]
        )
    };
    (
        $operation:ty,
        $method:ident,
        $input:expr,
        $arguments:expr,
        $output:expr,
        [$apply:path] ($($apply_arguments:tt)*),
        policy[$($policy:tt)*],
        selector[$($selector:ident)?],
        receiver[$($receiver:ident)?],
        fields[$($field:ident : $field_type:ident),*],
        arguments[($name:ident, $alignment:ty $(, $value:ty)? $(; $retention:ty)?)]
    ) => {{
        let source = $crate::dynamic::invoke_argument_source($arguments, 0);
        if source.is_dropping() {
            type $name = $crate::dynamic::operation_dynamic_argument_type!(
                $alignment $(, $value)?,
                $crate::dynamic::Dropping
            );
            let first_argument = $crate::dynamic::operation_dynamic_argument_value!(
                source,
                $alignment $(, $value)?,
                $crate::dynamic::Dropping
            );
            let operation = $crate::dynamic::operation_dynamic_capture!(
                $operation,
                $method,
                $arguments;
                policy[$($policy)*];
                selector[$($selector)?];
                receiver[$($receiver)?];
                fields[$($field : $field_type),*];
                values[first_argument]
            );
            $apply!(operation, $input, $output, $($apply_arguments)*)
        } else {
            type $name = $crate::dynamic::operation_dynamic_argument_type!(
                $alignment $(, $value)?,
                $crate::dynamic::Preserving
            );
            let first_argument = $crate::dynamic::operation_dynamic_argument_value!(
                source,
                $alignment $(, $value)?,
                $crate::dynamic::Preserving
            );
            let operation = $crate::dynamic::operation_dynamic_capture!(
                $operation,
                $method,
                $arguments;
                policy[$($policy)*];
                selector[$($selector)?];
                receiver[$($receiver)?];
                fields[$($field : $field_type),*];
                values[first_argument]
            );
            $apply!(operation, $input, $output, $($apply_arguments)*)
        }
    }};
    (
        $operation:ty,
        $method:ident,
        $input:expr,
        $arguments:expr,
        $output:expr,
        [$apply:path] ($($apply_arguments:tt)*),
        policy[$($policy:tt)*],
        selector[$($selector:ident)?],
        receiver[$($receiver:ident)?],
        fields[$($field:ident : $field_type:ident),*],
        arguments[
            ($first_name:ident, $first_alignment:ty $(, $first_value:ty)?),
            ($second_name:ident, $second_alignment:ty $(, $second_value:ty)?)
        ]
    ) => {{
        let first_source = $crate::dynamic::invoke_argument_source($arguments, 0);
        let second_source = $crate::dynamic::invoke_argument_source($arguments, 1);
        match (first_source.is_dropping(), second_source.is_dropping()) {
            (false, false) => {
                type $first_name = $crate::dynamic::operation_dynamic_argument_type!(
                    $first_alignment $(, $first_value)?,
                    $crate::dynamic::Preserving
                );
                type $second_name = $crate::dynamic::operation_dynamic_argument_type!(
                    $second_alignment $(, $second_value)?,
                    $crate::dynamic::Preserving
                );
                let first_argument = $crate::dynamic::operation_dynamic_argument_value!(
                    first_source,
                    $first_alignment $(, $first_value)?,
                    $crate::dynamic::Preserving
                );
                let second_argument = $crate::dynamic::operation_dynamic_argument_value!(
                    second_source,
                    $second_alignment $(, $second_value)?,
                    $crate::dynamic::Preserving
                );
                let operation = $crate::dynamic::operation_dynamic_capture!(
                    $operation,
                    $method,
                    $arguments;
                    policy[];
                    selector[$($selector)?];
                    receiver[$($receiver)?];
                    fields[$($field : $field_type),*];
                    values[first_argument, second_argument]
                );
                $apply!(operation, $input, $output, $($apply_arguments)*)
            }
            (false, true) => {
                type $first_name = $crate::dynamic::operation_dynamic_argument_type!(
                    $first_alignment $(, $first_value)?,
                    $crate::dynamic::Preserving
                );
                type $second_name = $crate::dynamic::operation_dynamic_argument_type!(
                    $second_alignment $(, $second_value)?,
                    $crate::dynamic::Dropping
                );
                let first_argument = $crate::dynamic::operation_dynamic_argument_value!(
                    first_source,
                    $first_alignment $(, $first_value)?,
                    $crate::dynamic::Preserving
                );
                let second_argument = $crate::dynamic::operation_dynamic_argument_value!(
                    second_source,
                    $second_alignment $(, $second_value)?,
                    $crate::dynamic::Dropping
                );
                let operation = $crate::dynamic::operation_dynamic_capture!(
                    $operation,
                    $method,
                    $arguments;
                    policy[];
                    selector[$($selector)?];
                    receiver[$($receiver)?];
                    fields[$($field : $field_type),*];
                    values[first_argument, second_argument]
                );
                $apply!(operation, $input, $output, $($apply_arguments)*)
            }
            (true, false) => {
                type $first_name = $crate::dynamic::operation_dynamic_argument_type!(
                    $first_alignment $(, $first_value)?,
                    $crate::dynamic::Dropping
                );
                type $second_name = $crate::dynamic::operation_dynamic_argument_type!(
                    $second_alignment $(, $second_value)?,
                    $crate::dynamic::Preserving
                );
                let first_argument = $crate::dynamic::operation_dynamic_argument_value!(
                    first_source,
                    $first_alignment $(, $first_value)?,
                    $crate::dynamic::Dropping
                );
                let second_argument = $crate::dynamic::operation_dynamic_argument_value!(
                    second_source,
                    $second_alignment $(, $second_value)?,
                    $crate::dynamic::Preserving
                );
                let operation = $crate::dynamic::operation_dynamic_capture!(
                    $operation,
                    $method,
                    $arguments;
                    policy[];
                    selector[$($selector)?];
                    receiver[$($receiver)?];
                    fields[$($field : $field_type),*];
                    values[first_argument, second_argument]
                );
                $apply!(operation, $input, $output, $($apply_arguments)*)
            }
            (true, true) => {
                type $first_name = $crate::dynamic::operation_dynamic_argument_type!(
                    $first_alignment $(, $first_value)?,
                    $crate::dynamic::Dropping
                );
                type $second_name = $crate::dynamic::operation_dynamic_argument_type!(
                    $second_alignment $(, $second_value)?,
                    $crate::dynamic::Dropping
                );
                let first_argument = $crate::dynamic::operation_dynamic_argument_value!(
                    first_source,
                    $first_alignment $(, $first_value)?,
                    $crate::dynamic::Dropping
                );
                let second_argument = $crate::dynamic::operation_dynamic_argument_value!(
                    second_source,
                    $second_alignment $(, $second_value)?,
                    $crate::dynamic::Dropping
                );
                let operation = $crate::dynamic::operation_dynamic_capture!(
                    $operation,
                    $method,
                    $arguments;
                    policy[];
                    selector[$($selector)?];
                    receiver[$($receiver)?];
                    fields[$($field : $field_type),*];
                    values[first_argument, second_argument]
                );
                $apply!(operation, $input, $output, $($apply_arguments)*)
            }
        }
    }};
}

macro_rules! operation_dynamic_entity_dispatch {
    (
        $input:expr,
        [$build:ident] ($($arguments:tt)*),
        [$(($name:ident, $bound:ident $(<$target:ident>)? $(+ $additional:ident)*))*]
    ) => {
        $crate::dynamic::operation_dynamic_entity_dispatch!(
            @scan
            $input,
            [$build] ($($arguments)*),
            [$(($name, $bound $(<$target>)? $(+ $additional)*))*],
            [$(($name, $bound $(<$target>)? $(+ $additional)*))*]
        )
    };
    (
        @scan
        $input:expr,
        [$build:ident] ($($arguments:tt)*),
        [$($all:tt)*],
        [($name:ident, EntityDomain $(<$target:ident>)? $(+ $additional:ident)*) $($remaining:tt)*]
    ) => {
        $crate::dynamic::operation_dynamic_entity_dispatch!(
            @entity $input, [$build] ($($arguments)*), [$($all)*]
        )
    };
    (
        @scan
        $input:expr,
        [$build:ident] ($($arguments:tt)*),
        [$($all:tt)*],
        [($name:ident, EntityAttributes $(<$target:ident>)? $(+ $additional:ident)*) $($remaining:tt)*]
    ) => {
        $crate::dynamic::operation_dynamic_entity_dispatch!(
            @entity $input, [$build] ($($arguments)*), [$($all)*]
        )
    };
    (
        @scan
        $input:expr,
        [$build:ident] ($($arguments:tt)*),
        [$($all:tt)*],
        [($name:ident, IndicesInGroup $(<$target:ident>)? $(+ $additional:ident)*) $($remaining:tt)*]
    ) => {
        $crate::dynamic::operation_dynamic_entity_dispatch!(
            @entity $input, [$build] ($($arguments)*), [$($all)*]
        )
    };
    (
        @scan
        $input:expr,
        [$build:ident] ($($arguments:tt)*),
        [$($all:tt)*],
        [($name:ident, $bound:ident $(<$target:ident>)? $(+ $additional:ident)*) $($remaining:tt)*]
    ) => {
        $crate::dynamic::operation_dynamic_entity_dispatch!(
            @scan
            $input,
            [$build] ($($arguments)*),
            [$($all)*],
            [$($remaining)*]
        )
    };
    (
        @scan
        $input:expr,
        [$build:ident] ($($arguments:tt)*),
        [$($all:tt)*],
        []
    ) => {
        $crate::dynamic::$build!(
            $crate::dynamic::DynIndex,
            $($arguments)*,
            [$($all)*]
        )
    };
    (@entity $input:expr, [$build:ident] ($($arguments:tt)*), [$($all:tt)*]) => {
        match $crate::dynamic::entity_domain($input) {
            $crate::dynamic::DynEntityDomain::Node => {
                $crate::dynamic::$build!(
                    $crate::dynamic::NodeIndex,
                    $($arguments)*,
                    [$($all)*]
                )
            }
            $crate::dynamic::DynEntityDomain::Edge => {
                $crate::dynamic::$build!(
                    $crate::dynamic::EdgeIndex,
                    $($arguments)*,
                    [$($all)*]
                )
            }
        }
    };
}

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

macro_rules! operation_dynamic_expansion_apply {
    (
        $operation_value:ident,
        $input:expr,
        $output:expr,
        $operation:ty,
        $index:ty,
        $value:ty,
        $child:ty,
        $out_value:ty,
        $order:ty
    ) => {{
        type DynamicOperation = $crate::dynamic::DynExpansionOperation<
            $operation,
            $index,
            $value,
            $child,
            $out_value,
            $order,
        >;
        type Shape = $crate::Indexed<$index, $value>;
        type DynamicShape = <Shape as $crate::dynamic::DynShapeProjection>::Dynamic;

        $crate::dynamic::operation_dynamic_shape_apply!(
            DynamicOperation::new($operation_value),
            $input,
            $output,
            DynamicShape
        )
    }};
}

macro_rules! operation_dynamic_element_build {
    (
        $dynamic_value:ty,
        $entity:ty,
        $operation:ty,
        expand_to,
        $input:expr,
        $arguments:expr,
        $output:expr,
        policy[$($policy:tt)*],
        selector[],
        receiver[$receiver:ident],
        fields[],
        arguments[($argument:ident, $alignment:ty, $argument_value:ty)],
        normal[$shape:ty, $out_shape:ty],
        [
            ($parent:ident, IndexDomain)
            ($child:ident, IndexDomain)
            ($value:ident, ValueDomain)
            ($out_value:ident, ValueDomain)
        ]
    ) => {{
        type $parent = $crate::dynamic::DynIndex;
        type $child = $crate::dynamic::DynIndex;
        type $value = $dynamic_value;

        $crate::dynamic::operation_dynamic_selected_argument!(
            $operation,
            expand_to,
            $input,
            $arguments,
            $output,
            [$crate::dynamic::operation_dynamic_element_apply] (
                $operation,
                $shape,
                $out_shape
            ),
            policy[$($policy)*],
            selector[],
            receiver[$receiver],
            fields[],
            argument[$argument, $alignment],
            value[$out_value],
            masked[direct]
        )
    }};
    (
        $dynamic_value:ty,
        $entity:ty,
        $operation:ty,
        $method:ident,
        $input:expr,
        $arguments:expr,
        $output:expr,
        policy[$($policy:tt)*],
        selector[$($selector:ident)?],
        receiver[$($receiver:ident)?],
        fields[$($field:ident : $field_type:ident),*],
        arguments[$(($argument:ident, $alignment:ty $(, $argument_value:ty)? $(; $retention:ty)?)),*],
        normal[$shape:ty, $out_shape:ty],
        [$($parameters:tt)*]
    ) => {{
        $crate::dynamic::operation_dynamic_aliases!(
            $crate::Unordered,
            $entity,
            $crate::Indexed<$crate::dynamic::DynIndex, $dynamic_value>,
            $crate::Multiple<$crate::Unordered>;
            [$($parameters)*]
        );
        $crate::dynamic::operation_dynamic_arguments!(
            $operation,
            $method,
            $input,
            $arguments,
            $output,
            [$crate::dynamic::operation_dynamic_element_apply] (
                $operation,
                $shape,
                $out_shape
            ),
            policy[$($policy)*],
            selector[$($selector)?],
            receiver[$($receiver)?],
            fields[$($field : $field_type),*],
            arguments[$(($argument, $alignment $(, $argument_value)? $(; $retention)?)),*]
        )
    }};
    (
        $dynamic_value:ty,
        $entity:ty,
        $operation:ty,
        $method:ident,
        $input:expr,
        $arguments:expr,
        $output:expr,
        policy[$($policy:tt)*],
        selector[$($selector:ident)?],
        receiver[$($receiver:ident)?],
        fields[$($field:ident : $field_type:ident),*],
        arguments[$(($argument:ident, $alignment:ty $(, $argument_value:ty)? $(; $retention:ty)?)),*],
        expansion[$index:ty, $value:ty, $child:ty, $out_value:ty, $order:ty],
        [$($parameters:tt)*]
    ) => {{
        $crate::dynamic::operation_dynamic_aliases!(
            $crate::Unordered,
            $entity,
            $crate::Indexed<$crate::dynamic::DynIndex, $dynamic_value>,
            $crate::Multiple<$crate::Unordered>;
            [$($parameters)*]
        );
        $crate::dynamic::operation_dynamic_arguments!(
            $operation,
            $method,
            $input,
            $arguments,
            $output,
            [$crate::dynamic::operation_dynamic_expansion_apply] (
                $operation,
                $index,
                $value,
                $child,
                $out_value,
                $order
            ),
            policy[$($policy)*],
            selector[$($selector)?],
            receiver[$($receiver)?],
            fields[$($field : $field_type),*],
            arguments[$(($argument, $alignment $(, $argument_value)? $(; $retention)?)),*]
        )
    }};
}

macro_rules! operation_dynamic_set_arity {
    (
        $operation:ty,
        $method:ident,
        $input:expr,
        $arguments:expr,
        $output:expr,
        argument[$name:ident, $set_shape:ty],
        $handles:expr,
        input[$shape:ty],
        output[$out_shape:ty]
    ) => {
        match $handles {
            $crate::dynamic::DynArityHandle::MultipleOrdered(handle) => {
                type $name =
                    $crate::operands::OperandHandle<$set_shape, $crate::Multiple<$crate::Ordered>>;

                let set = handle.clone();
                let operation = $crate::dynamic::operation_dynamic_capture!(
                    $operation,
                    $method,
                    $arguments;
                    policy[];
                    selector[];
                    receiver[];
                    fields[];
                    values[set]
                );

                $crate::dynamic::operation_dynamic_element_apply!(
                    operation, $input, $output, $operation, $shape, $out_shape
                )
            }
            $crate::dynamic::DynArityHandle::MultipleUnordered(handle) => {
                type $name =
                    $crate::operands::OperandHandle<$set_shape, $crate::Multiple<$crate::Unordered>>;

                let set = handle.clone();
                let operation = $crate::dynamic::operation_dynamic_capture!(
                    $operation,
                    $method,
                    $arguments;
                    policy[];
                    selector[];
                    receiver[];
                    fields[];
                    values[set]
                );

                $crate::dynamic::operation_dynamic_element_apply!(
                    operation, $input, $output, $operation, $shape, $out_shape
                )
            }
            $crate::dynamic::DynArityHandle::Single(handle) => {
                type $name = $crate::operands::OperandHandle<$set_shape, $crate::Single>;

                let set = handle.clone();
                let operation = $crate::dynamic::operation_dynamic_capture!(
                    $operation,
                    $method,
                    $arguments;
                    policy[];
                    selector[];
                    receiver[];
                    fields[];
                    values[set]
                );

                $crate::dynamic::operation_dynamic_element_apply!(
                    operation, $input, $output, $operation, $shape, $out_shape
                )
            }
            $crate::dynamic::DynArityHandle::Definite(handle) => {
                type $name = $crate::operands::OperandHandle<$set_shape, $crate::Definite>;

                let set = handle.clone();
                let operation = $crate::dynamic::operation_dynamic_capture!(
                    $operation,
                    $method,
                    $arguments;
                    policy[];
                    selector[];
                    receiver[];
                    fields[];
                    values[set]
                );

                $crate::dynamic::operation_dynamic_element_apply!(
                    operation, $input, $output, $operation, $shape, $out_shape
                )
            }
        }
    };
}

macro_rules! operation_dynamic_set_build {
    (
        $dynamic_value:ty,
        $operation:ty,
        $method:ident,
        $input:expr,
        $arguments:expr,
        $output:expr,
        argument[$name:ident],
        input[$shape:ty],
        output[$out_shape:ty],
        [$($parameters:tt)*]
    ) => {{
        $crate::dynamic::operation_dynamic_aliases!(
            $crate::Unordered,
            $crate::dynamic::DynIndex,
            $crate::Indexed<$crate::dynamic::DynIndex, $dynamic_value>,
            $crate::Multiple<$crate::Unordered>;
            [$($parameters)*]
        );

        let source = $crate::dynamic::invoke_argument_source($arguments, 0);

        if source.is_literal_set() {
            type $name = Vec<<$dynamic_value as $crate::dynamic::DynSetLiteral>::Element>;

            let set = <$dynamic_value as $crate::dynamic::DynSetLiteral>::literal(source);
            let operation = $crate::dynamic::operation_dynamic_capture!(
                $operation,
                $method,
                $arguments;
                policy[];
                selector[];
                receiver[];
                fields[];
                values[set]
            );

            return $crate::dynamic::operation_dynamic_element_apply!(
                operation, $input, $output, $operation, $shape, $out_shape
            );
        }

        let set = source.as_operand();
        let $crate::dynamic::DynHandle::Lane(lane) = &set.handle else {
            panic!("registry admitted a grouped operand where a dynamic set source is required")
        };

        match lane {
            $crate::dynamic::DynLaneHandle::BareValue(_)
            | $crate::dynamic::DynLaneHandle::BareMask(_) => {
                $crate::dynamic::operation_dynamic_set_arity!(
                    $operation,
                    $method,
                    $input,
                    $arguments,
                    $output,
                    argument[$name, $crate::Bare<$dynamic_value>],
                    <$crate::Bare<$dynamic_value> as $crate::dynamic::DynLaneState>::handles(lane),
                    input[$shape],
                    output[$out_shape]
                )
            }
            _ => {
                $crate::dynamic::operation_dynamic_set_arity!(
                    $operation,
                    $method,
                    $input,
                    $arguments,
                    $output,
                    argument[$name, $crate::Indexed<$crate::dynamic::DynIndex, $dynamic_value>],
                    <$crate::Indexed<$crate::dynamic::DynIndex, $dynamic_value> as
                        $crate::dynamic::DynLaneState>::handles(lane),
                    input[$shape],
                    output[$out_shape]
                )
            }
        }
    }};
}

macro_rules! operation_dynamic_element_entity {
    (
        $entity:ty,
        $operation:ty,
        $method:ident,
        $input:expr,
        $arguments:expr,
        $output:expr,
        policy[$($policy:tt)*],
        selector[$($selector:ident)?],
        receiver[$($receiver:ident)?],
        fields[$($field:ident : $field_type:ident),*],
        arguments[$(($argument:ident, $alignment:ty $(, $argument_value:ty)? $(; $retention:ty)?)),*],
        $kind:ident[$($kind_arguments:tt)*],
        [$($parameters:tt)*]
    ) => {
        $crate::dynamic::operation_dynamic_receiver_dispatch!(
            $input,
            [operation_dynamic_element_build] (
                $entity,
                $operation,
                $method,
                $input,
                $arguments,
                $output,
                policy[$($policy)*],
                selector[$($selector)?],
                receiver[$($receiver)?],
                fields[$($field : $field_type),*],
                arguments[$(($argument, $alignment $(, $argument_value)? $(; $retention)?)),*],
                $kind[$($kind_arguments)*],
                [$($parameters)*]
            ),
            [$($parameters)*]
        )
    };
}

macro_rules! operation_element_applier {
    (
        $operation:ty,
        $method:ident,
        policy[$($policy:tt)*],
        {
            parameters: < $($name:ident : $bound:ident $(<$target:ident>)? $(+ $additional:ident)*),* $(,)? >;
            argument: $set_argument:ident : SetSource<$set_value:ty>;
            input: $shape:ty;
            output: $out_shape:ty;
            emission: $emission:ty;
        }
    ) => {{
        fn apply(
            input: &$crate::dynamic::DynOperand,
            arguments: &[$crate::dynamic::DynInvokeArgument],
            output: $crate::registry::OperandDescriptor,
        ) -> $crate::dynamic::DynOperand {
            $crate::dynamic::operation_dynamic_receiver_dispatch!(
                input,
                [operation_dynamic_set_build] (
                    $operation,
                    $method,
                    input,
                    arguments,
                    output,
                    argument[$set_argument],
                    input[$shape],
                    output[$out_shape],
                    [$(($name, $bound $(<$target>)? $(+ $additional)*))*]
                ),
                [$(($name, $bound $(<$target>)? $(+ $additional)*))*]
            )
        }
        apply
    }};
    (
        $operation:ty,
        $method:ident,
        policy[$($policy:tt)*],
        {
            parameters: < $($name:ident : $bound:ident $(<$target:ident>)? $(+ $additional:ident)*),* $(,)? >;
            $(selector: $selector:ident;)?
            $(field: $field:ident : $field_type:ident;)*
            $(
                argument: $argument:ident : ArgumentSource<
                    $alignment:ty $(, $argument_value:ty)? $(, Retention = $retention:ty)?
                >
                $(where $argument_owner:ident::ValueDomain : $capability:ident)?;
            )*
            $(receiver: $receiver:ident;)?
            input: Indexed<$index:ty, $value:ty>;
            output: Indexed<ExpandedIndex<$parent:ty, $child:ty>, $out_value:ty>;
            emission: Expanding<$order:ty>;
            $(where $where_owner:ident::Owned : $where_first:ident $(+ $where_more:ident)*;)?
        }
    ) => {{
        fn apply(
            input: &$crate::dynamic::DynOperand,
            arguments: &[$crate::dynamic::DynInvokeArgument],
            output: $crate::registry::OperandDescriptor,
        ) -> $crate::dynamic::DynOperand {
            $crate::dynamic::operation_dynamic_entity_dispatch!(
                input,
                [operation_dynamic_element_entity] (
                    $operation,
                    $method,
                    input,
                    arguments,
                    output,
                    policy[$($policy)*],
                    selector[$($selector)?],
                    receiver[$($receiver)?],
                    fields[$($field : $field_type),*],
                    arguments[$(($argument, $alignment $(, $argument_value)? $(; $retention)?)),*],
                    expansion[$index, $value, $child, $out_value, $order]
                ),
                [$(($name, $bound $(<$target>)? $(+ $additional)*))*]
            )
        }
        apply
    }};
    (
        $operation:ty,
        $method:ident,
        policy[$($policy:tt)*],
        {
            parameters: < $($name:ident : $bound:ident $(<$target:ident>)? $(+ $additional:ident)*),* $(,)? >;
            $(selector: $selector:ident;)?
            $(field: $field:ident : $field_type:ident;)*
            $(
                argument: $argument:ident : ArgumentSource<
                    $alignment:ty $(, $argument_value:ty)? $(, Retention = $retention:ty)?
                >
                $(where $argument_owner:ident::ValueDomain : $capability:ident)?;
            )*
            $(receiver: $receiver:ident;)?
            input: $shape:ty;
            output: $out_shape:ty;
            emission: $emission:ty;
            $(where $where_owner:ident::Owned : $where_first:ident $(+ $where_more:ident)*;)?
        }
    ) => {{
        fn apply(
            input: &$crate::dynamic::DynOperand,
            arguments: &[$crate::dynamic::DynInvokeArgument],
            output: $crate::registry::OperandDescriptor,
        ) -> $crate::dynamic::DynOperand {
            $crate::dynamic::operation_dynamic_entity_dispatch!(
                input,
                [operation_dynamic_element_entity] (
                    $operation,
                    $method,
                    input,
                    arguments,
                    output,
                    policy[$($policy)*],
                    selector[$($selector)?],
                    receiver[$($receiver)?],
                    fields[$($field : $field_type),*],
                    arguments[$(($argument, $alignment $(, $argument_value)? $(; $retention)?)),*],
                    normal[$shape, $out_shape]
                ),
                [$(($name, $bound $(<$target>)? $(+ $additional)*))*]
            )
        }
        apply
    }};
}

macro_rules! operation_dynamic_lane_apply {
    (
        $operation_value:ident,
        $input:expr,
        $output:expr,
        $operation:ty,
        $shape:ty,
        $arity:ty,
        $out_operand:ty
    ) => {{
        type DynamicOperation =
            $crate::dynamic::DynLaneOperation<$operation, $shape, $arity, $out_operand>;
        type DynamicShape = <$shape as $crate::dynamic::DynShapeProjection>::Dynamic;

        let operation = DynamicOperation::new($operation_value);

        let $crate::dynamic::DynHandle::Lane(handle) = &$input.handle else {
            return $crate::dynamic::apply_grouped_operation::<DynamicShape, $arity, _>(
                $input, operation, $output,
            );
        };

        let handles = <DynamicShape as $crate::dynamic::DynLaneState>::handles(handle);

        $crate::dynamic::apply_lane_operation::<DynamicShape, $arity, _>(
            handles, operation, $output,
        )
    }};
}

macro_rules! operation_dynamic_lane_build {
    (
        $dynamic_value:ty,
        $entity:ty,
        $order:ty,
        $arity:ty,
        $operation:ty,
        group_by,
        $input:expr,
        $arguments:expr,
        $output:expr,
        policy[$($policy:tt)*],
        fields[],
        arguments[($argument:ident, $alignment:ty, $argument_value:ty)],
        input[$shape:ty],
        output[$out_operand:ty],
        [
            ($index:ident, IndexDomain)
            ($value:ident, ValueDomain)
            ($key:ident, GroupingValue)
            $(($input_order:ident, OrderState))?
        ]
    ) => {{
        type $index = $crate::dynamic::DynIndex;
        type $value = $dynamic_value;
        $(type $input_order = $order;)?

        $crate::dynamic::operation_dynamic_selected_argument!(
            $operation,
            group_by,
            $input,
            $arguments,
            $output,
            [$crate::dynamic::operation_dynamic_lane_apply] (
                $operation,
                $shape,
                $arity,
                $out_operand
            ),
            policy[$($policy)*],
            selector[],
            receiver[],
            fields[],
            argument[$argument, $alignment],
            value[$key],
            masked[keyable]
        )
    }};
    (
        $dynamic_value:ty,
        $entity:ty,
        $order:ty,
        $arity:ty,
        $operation:ty,
        $method:ident,
        $input:expr,
        $arguments:expr,
        $output:expr,
        policy[$($policy:tt)*],
        fields[$($field:ident : $field_type:ident),*],
        arguments[$(($argument:ident, $alignment:ty $(, $argument_value:ty)? $(; $retention:ty)?)),*],
        input[$shape:ty],
        output[$out_operand:ty],
        [$($parameters:tt)*]
    ) => {{
        $crate::dynamic::operation_dynamic_aliases!(
            $order,
            $entity,
            $crate::Indexed<$crate::dynamic::DynIndex, $dynamic_value>,
            $arity;
            [$($parameters)*]
        );
        $crate::dynamic::operation_dynamic_arguments!(
            $operation,
            $method,
            $input,
            $arguments,
            $output,
            [$crate::dynamic::operation_dynamic_lane_apply] (
                $operation,
                $shape,
                $arity,
                $out_operand
            ),
            policy[$($policy)*],
            selector[],
            receiver[],
            fields[$($field : $field_type),*],
            arguments[$(($argument, $alignment $(, $argument_value)? $(; $retention)?)),*]
        )
    }};
}

macro_rules! operation_dynamic_lane_entity {
    (
        $entity:ty,
        $order:ty,
        $arity:ty,
        $operation:ty,
        $method:ident,
        $input:expr,
        $arguments:expr,
        $output:expr,
        policy[$($policy:tt)*],
        fields[$($field:ident : $field_type:ident),*],
        arguments[$(($argument:ident, $alignment:ty $(, $argument_value:ty)? $(; $retention:ty)?)),*],
        input[$shape:ty],
        output[$out_operand:ty],
        [$($parameters:tt)*]
    ) => {
        $crate::dynamic::operation_dynamic_receiver_dispatch!(
            $input,
            [operation_dynamic_lane_build] (
                $entity,
                $order,
                $arity,
                $operation,
                $method,
                $input,
                $arguments,
                $output,
                policy[$($policy)*],
                fields[$($field : $field_type),*],
                arguments[$(($argument, $alignment $(, $argument_value)? $(; $retention)?)),*],
                input[$shape],
                output[$out_operand],
                [$($parameters)*]
            ),
            [$($parameters)*]
        )
    };
}

macro_rules! operation_dynamic_lane_function {
    (
        $operation:ty,
        $method:ident,
        policy[$($policy:tt)*],
        $order:ty,
        $arity:ty,
        {
            parameters: < $($name:ident : $bound:ident $(<$target:ident>)? $(+ $additional:ident)*),* $(,)? >;
            $(field: $field:ident : $field_type:ident;)*
            $(
                argument: $argument:ident : ArgumentSource<
                    $alignment:ty $(, $argument_value:ty)? $(, Retention = $retention:ty)?
                >
                $(where $argument_owner:ident::ValueDomain : $capability:ident)?;
            )*
            input: ($shape:ty, $input_arity:ty);
            output: $out_operand:ty;
            $(where $where_owner:ident::Owned : $where_first:ident $(+ $where_more:ident)*;)?
        }
    ) => {{
        fn apply(
            input: &$crate::dynamic::DynOperand,
            arguments: &[$crate::dynamic::DynInvokeArgument],
            output: $crate::registry::OperandDescriptor,
        ) -> $crate::dynamic::DynOperand {
            $crate::dynamic::operation_dynamic_entity_dispatch!(
                input,
                [operation_dynamic_lane_entity] (
                    $order,
                    $arity,
                    $operation,
                    $method,
                    input,
                    arguments,
                    output,
                    policy[$($policy)*],
                    fields[$($field : $field_type),*],
                    arguments[$(($argument, $alignment $(, $argument_value)? $(; $retention)?)),*],
                    input[$shape],
                    output[$out_operand]
                ),
                [$(($name, $bound $(<$target>)? $(+ $additional)*))*]
            )
        }
        apply
    }};
}

macro_rules! operation_lane_applier {
    (
        $operation:ty,
        $method:ident,
        policy[$($policy:tt)*],
        {
            parameters: < $($name:ident : $bound:ident $(<$target:ident>)? $(+ $additional:ident)*),* $(,)? >;
            $(field: $field:ident : $field_type:ident;)*
            $(
                argument: $argument:ident : ArgumentSource<
                    $alignment:ty $(, $argument_value:ty)? $(, Retention = $retention:ty)?
                >
                $(where $argument_owner:ident::ValueDomain : $capability:ident)?;
            )*
            input: ($shape:ty, Multiple<Ordered>);
            output: $out_operand:ty;
            $(where $where_owner:ident::Owned : $where_first:ident $(+ $where_more:ident)*;)?
        }
    ) => {
        $crate::dynamic::operation_dynamic_lane_function!(
            $operation,
            $method,
            policy[$($policy)*],
            $crate::Ordered,
            $crate::Multiple<$crate::Ordered>,
            {
                parameters: < $($name : $bound $(<$target>)? $(+ $additional)*),* >;
                $(field: $field : $field_type;)*
                $(
                    argument: $argument : ArgumentSource<
                    $alignment $(, $argument_value)? $(, Retention = $retention)?
                >
                    $(where $argument_owner::ValueDomain : $capability)?;
                )*
                input: ($shape, Multiple<Ordered>);
                output: $out_operand;
                $(where $where_owner::Owned : $where_first $(+ $where_more)*;)?
            }
        )
    };
    (
        $operation:ty,
        $method:ident,
        policy[$($policy:tt)*],
        {
            parameters: < $($name:ident : $bound:ident $(<$target:ident>)? $(+ $additional:ident)*),* $(,)? >;
            $(field: $field:ident : $field_type:ident;)*
            $(
                argument: $argument:ident : ArgumentSource<
                    $alignment:ty $(, $argument_value:ty)? $(, Retention = $retention:ty)?
                >
                $(where $argument_owner:ident::ValueDomain : $capability:ident)?;
            )*
            input: ($shape:ty, Multiple<Unordered>);
            output: $out_operand:ty;
            $(where $where_owner:ident::Owned : $where_first:ident $(+ $where_more:ident)*;)?
        }
    ) => {
        $crate::dynamic::operation_dynamic_lane_function!(
            $operation,
            $method,
            policy[$($policy)*],
            $crate::Unordered,
            $crate::Multiple<$crate::Unordered>,
            {
                parameters: < $($name : $bound $(<$target>)? $(+ $additional)*),* >;
                $(field: $field : $field_type;)*
                $(
                    argument: $argument : ArgumentSource<
                    $alignment $(, $argument_value)? $(, Retention = $retention)?
                >
                    $(where $argument_owner::ValueDomain : $capability)?;
                )*
                input: ($shape, Multiple<Unordered>);
                output: $out_operand;
                $(where $where_owner::Owned : $where_first $(+ $where_more)*;)?
            }
        )
    };
    (
        $operation:ty,
        $method:ident,
        policy[$($policy:tt)*],
        {
            parameters: < $($name:ident : $bound:ident $(<$target:ident>)? $(+ $additional:ident)*),* $(,)? >;
            $(field: $field:ident : $field_type:ident;)*
            $(
                argument: $argument:ident : ArgumentSource<
                    $alignment:ty $(, $argument_value:ty)? $(, Retention = $retention:ty)?
                >
                $(where $argument_owner:ident::ValueDomain : $capability:ident)?;
            )*
            input: ($shape:ty, Multiple<$order:ident>);
            output: $out_operand:ty;
            $(where $where_owner:ident::Owned : $where_first:ident $(+ $where_more:ident)*;)?
        }
    ) => {{
        fn apply(
            input: &$crate::dynamic::DynOperand,
            arguments: &[$crate::dynamic::DynInvokeArgument],
            output: $crate::registry::OperandDescriptor,
        ) -> $crate::dynamic::DynOperand {
            match input.descriptor().lane_arity() {
                $crate::registry::ArityDescriptor::Multiple {
                    order: $crate::registry::OrderDescriptor::Ordered,
                } => $crate::dynamic::operation_dynamic_entity_dispatch!(
                    input,
                    [operation_dynamic_lane_entity] (
                        $crate::Ordered,
                        $crate::Multiple<$crate::Ordered>,
                        $operation,
                        $method,
                        input,
                        arguments,
                        output,
                        policy[$($policy)*],
                        fields[$($field : $field_type),*],
                        arguments[$(($argument, $alignment $(, $argument_value)? $(; $retention)?)),*],
                        input[$shape],
                        output[$out_operand]
                    ),
                    [$(($name, $bound $(<$target>)? $(+ $additional)*))*]
                ),
                $crate::registry::ArityDescriptor::Multiple {
                    order: $crate::registry::OrderDescriptor::Unordered,
                } => $crate::dynamic::operation_dynamic_entity_dispatch!(
                    input,
                    [operation_dynamic_lane_entity] (
                        $crate::Unordered,
                        $crate::Multiple<$crate::Unordered>,
                        $operation,
                        $method,
                        input,
                        arguments,
                        output,
                        policy[$($policy)*],
                        fields[$($field : $field_type),*],
                        arguments[$(($argument, $alignment $(, $argument_value)? $(; $retention)?)),*],
                        input[$shape],
                        output[$out_operand]
                    ),
                    [$(($name, $bound $(<$target>)? $(+ $additional)*))*]
                ),
                _ => panic!("registry selected a multiple-lane operation for a different arity"),
            }
        }
        apply
    }};
    (
        $operation:ty,
        $method:ident,
        policy[$($policy:tt)*],
        {
            parameters: < $($name:ident : $bound:ident $(<$target:ident>)? $(+ $additional:ident)*),* $(,)? >;
            $(field: $field:ident : $field_type:ident;)*
            $(
                argument: $argument:ident : ArgumentSource<
                    $alignment:ty $(, $argument_value:ty)? $(, Retention = $retention:ty)?
                >
                $(where $argument_owner:ident::ValueDomain : $capability:ident)?;
            )*
            input: ($shape:ty, Single);
            output: $out_operand:ty;
            $(where $where_owner:ident::Owned : $where_first:ident $(+ $where_more:ident)*;)?
        }
    ) => {
        $crate::dynamic::operation_dynamic_lane_function!(
            $operation,
            $method,
            policy[$($policy)*],
            $crate::Unordered,
            $crate::Single,
            {
                parameters: < $($name : $bound $(<$target>)? $(+ $additional)*),* >;
                $(field: $field : $field_type;)*
                $(
                    argument: $argument : ArgumentSource<
                    $alignment $(, $argument_value)? $(, Retention = $retention)?
                >
                    $(where $argument_owner::ValueDomain : $capability)?;
                )*
                input: ($shape, Single);
                output: $out_operand;
                $(where $where_owner::Owned : $where_first $(+ $where_more)*;)?
            }
        )
    };
    (
        $operation:ty,
        $method:ident,
        policy[$($policy:tt)*],
        {
            parameters: < $($name:ident : $bound:ident $(<$target:ident>)? $(+ $additional:ident)*),* $(,)? >;
            $(field: $field:ident : $field_type:ident;)*
            $(
                argument: $argument:ident : ArgumentSource<
                    $alignment:ty $(, $argument_value:ty)? $(, Retention = $retention:ty)?
                >
                $(where $argument_owner:ident::ValueDomain : $capability:ident)?;
            )*
            input: ($shape:ty, Definite);
            output: $out_operand:ty;
            $(where $where_owner:ident::Owned : $where_first:ident $(+ $where_more:ident)*;)?
        }
    ) => {
        $crate::dynamic::operation_dynamic_lane_function!(
            $operation,
            $method,
            policy[$($policy)*],
            $crate::Unordered,
            $crate::Definite,
            {
                parameters: < $($name : $bound $(<$target>)? $(+ $additional)*),* >;
                $(field: $field : $field_type;)*
                $(
                    argument: $argument : ArgumentSource<
                    $alignment $(, $argument_value)? $(, Retention = $retention)?
                >
                    $(where $argument_owner::ValueDomain : $capability)?;
                )*
                input: ($shape, Definite);
                output: $out_operand;
                $(where $where_owner::Owned : $where_first $(+ $where_more)*;)?
            }
        )
    };
}

macro_rules! operation_dynamic_group_apply {
    ($operation_value:ident, $input:expr, $output:expr, $operation:ty,payload) => {{ $crate::dynamic::apply_group_operation($input, $operation_value, $output) }};
    (
        $operation_value:ident, $input:expr, $output:expr, $operation:ty,lane[$shape:ty, $arity:ty]
    ) => {{
        type DynamicOperation = $crate::dynamic::DynGroupOperation<$operation, $shape, $arity>;

        let operation = DynamicOperation::new($operation_value);

        $crate::dynamic::apply_group_operation($input, operation, $output)
    }};
}

macro_rules! operation_dynamic_group_build {
    (
        $operation:ty,
        $method:ident,
        $input:expr,
        $arguments:expr,
        $output:expr,
        policy[$($policy:tt)*],
        payload,
        arguments[$(($argument:ident, $alignment:ty $(, $argument_value:ty)? $(; $retention:ty)?)),*]
    ) => {
        $crate::dynamic::operation_dynamic_arguments!(
            $operation,
            $method,
            $input,
            $arguments,
            $output,
            [$crate::dynamic::operation_dynamic_group_apply] ($operation, payload),
            policy[$($policy)*],
            selector[],
            receiver[],
            fields[],
            arguments[$(($argument, $alignment $(, $argument_value)? $(; $retention)?)),*]
        )
    };
    (
        $operation:ty,
        $method:ident,
        $input:expr,
        $arguments:expr,
        $output:expr,
        policy[$($policy:tt)*],
        lane[$shape:ty, $arity:ty],
        arguments[$(($argument:ident, $alignment:ty $(, $argument_value:ty)? $(; $retention:ty)?)),*]
    ) => {
        $crate::dynamic::operation_dynamic_arguments!(
            $operation,
            $method,
            $input,
            $arguments,
            $output,
            [$crate::dynamic::operation_dynamic_group_apply] (
                $operation,
                lane[$shape, $arity]
            ),
            policy[$($policy)*],
            selector[],
            receiver[],
            fields[],
            arguments[$(($argument, $alignment $(, $argument_value)? $(; $retention)?)),*]
        )
    };
}

macro_rules! operation_dynamic_group_all_arities {
    (
        $shape:ty,
        $operation:ty,
        $method:ident,
        $input:expr,
        $arguments:expr,
        $output:expr,
        policy[$($policy:tt)*],
        $member:ident,
        $key:ident,
        $shape_name:ident,
        $arity_name:ident,
        arguments[$(($argument:ident, $alignment:ty $(, $argument_value:ty)? $(; $retention:ty)?)),*]
    ) => {{
        type $member = $crate::dynamic::DynIndex;
        type $key = $crate::dynamic::DynIndex;
        type $shape_name = $shape;

        let _ = std::marker::PhantomData::<fn() -> ($member, $key)>;

        match $input.descriptor().lane_arity() {
            $crate::registry::ArityDescriptor::Multiple {
                order: $crate::registry::OrderDescriptor::Ordered,
            } => {
                type $arity_name = $crate::Multiple<$crate::Ordered>;

                $crate::dynamic::operation_dynamic_group_build!(
                    $operation,
                    $method,
                    $input,
                    $arguments,
                    $output,
                    policy[$($policy)*],
                    lane[$shape_name, $arity_name],
                    arguments[$(($argument, $alignment $(, $argument_value)? $(; $retention)?)),*]
                )
            }
            $crate::registry::ArityDescriptor::Multiple {
                order: $crate::registry::OrderDescriptor::Unordered,
            } => {
                type $arity_name = $crate::Multiple<$crate::Unordered>;

                $crate::dynamic::operation_dynamic_group_build!(
                    $operation,
                    $method,
                    $input,
                    $arguments,
                    $output,
                    policy[$($policy)*],
                    lane[$shape_name, $arity_name],
                    arguments[$(($argument, $alignment $(, $argument_value)? $(; $retention)?)),*]
                )
            }
            $crate::registry::ArityDescriptor::Single => {
                type $arity_name = $crate::Single;

                $crate::dynamic::operation_dynamic_group_build!(
                    $operation,
                    $method,
                    $input,
                    $arguments,
                    $output,
                    policy[$($policy)*],
                    lane[$shape_name, $arity_name],
                    arguments[$(($argument, $alignment $(, $argument_value)? $(; $retention)?)),*]
                )
            }
            $crate::registry::ArityDescriptor::Definite => {
                type $arity_name = $crate::Definite;

                $crate::dynamic::operation_dynamic_group_build!(
                    $operation,
                    $method,
                    $input,
                    $arguments,
                    $output,
                    policy[$($policy)*],
                    lane[$shape_name, $arity_name],
                    arguments[$(($argument, $alignment $(, $argument_value)? $(; $retention)?)),*]
                )
            }
        }
    }};
}

macro_rules! operation_dynamic_via_dispatch {
    (
        $operation:ty,
        $method:ident,
        $input:expr,
        $via:expr,
        $output:expr,argument[$argument:ident],arity[$via_arity:ident],via_value[$via_value:ty],payload[$payload_shape:ty, $payload_arity:ty]
    ) => {{
        type ViaShape = $crate::Indexed<$crate::dynamic::DynIndex, $via_value>;

        let $crate::dynamic::DynHandle::Lane(lane) = &$via.handle else {
            panic!("registry admitted a grouped operand where a dynamic via lane is required")
        };

        match <ViaShape as $crate::dynamic::DynLaneState>::handles(lane) {
            $crate::dynamic::DynArityHandle::MultipleOrdered(handle) => {
                type $via_arity = $crate::Multiple<$crate::Ordered>;
                type $argument = $crate::operands::OperandHandle<ViaShape, $via_arity>;

                let via_source = handle.clone();
                let capture = $crate::dynamic::OperationCapture::<$operation>::capture();
                let operation = capture.$method(via_source).operation();
                let operation = $crate::dynamic::DynGroupOperation::<
                    $operation,
                    $payload_shape,
                    $payload_arity,
                >::new(operation);

                $crate::dynamic::apply_group_operation($input, operation, $output)
            }
            $crate::dynamic::DynArityHandle::MultipleUnordered(handle) => {
                type $via_arity = $crate::Multiple<$crate::Unordered>;
                type $argument = $crate::operands::OperandHandle<ViaShape, $via_arity>;

                let via_source = handle.clone();
                let capture = $crate::dynamic::OperationCapture::<$operation>::capture();
                let operation = capture.$method(via_source).operation();
                let operation = $crate::dynamic::DynGroupOperation::<
                    $operation,
                    $payload_shape,
                    $payload_arity,
                >::new(operation);

                $crate::dynamic::apply_group_operation($input, operation, $output)
            }
            $crate::dynamic::DynArityHandle::Single(handle) => {
                type $via_arity = $crate::Single;
                type $argument = $crate::operands::OperandHandle<ViaShape, $via_arity>;

                let via_source = handle.clone();
                let capture = $crate::dynamic::OperationCapture::<$operation>::capture();
                let operation = capture.$method(via_source).operation();
                let operation = $crate::dynamic::DynGroupOperation::<
                    $operation,
                    $payload_shape,
                    $payload_arity,
                >::new(operation);

                $crate::dynamic::apply_group_operation($input, operation, $output)
            }
            $crate::dynamic::DynArityHandle::Definite(handle) => {
                type $via_arity = $crate::Definite;
                type $argument = $crate::operands::OperandHandle<ViaShape, $via_arity>;

                let via_source = handle.clone();
                let capture = $crate::dynamic::OperationCapture::<$operation>::capture();
                let operation = capture.$method(via_source).operation();
                let operation = $crate::dynamic::DynGroupOperation::<
                    $operation,
                    $payload_shape,
                    $payload_arity,
                >::new(operation);

                $crate::dynamic::apply_group_operation($input, operation, $output)
            }
        }
    }};
}

macro_rules! operation_group_applier {
    (
        $operation:ty,
        $method:ident,
        policy[$policy:path],
        {
            group: < $member:ident : IndexDomain, $key:ident : GroupKey $(,)? >;
            parameters: < $payload:ident : Lane $(,)? >;
            input: $payload_input:ident;
            output: $out_operand:ty;
        }
    ) => {{
        fn apply(
            input: &$crate::dynamic::DynOperand,
            arguments: &[$crate::dynamic::DynInvokeArgument],
            output: $crate::registry::OperandDescriptor,
        ) -> $crate::dynamic::DynOperand {
            let _ = arguments;
            let capture = $crate::dynamic::OperationCapture::<$operation>::capture();
            let operation = capture.$method($policy).operation();

            $crate::dynamic::apply_group_operation(input, operation, output)
        }

        apply
    }};
    (
        $operation:ty,
        $method:ident,
        policy[$($policy:tt)*],
        {
            group: < $member:ident : IndexDomain, $key:ident : GroupKey $(,)? >;
            parameters: < $($name:ident : $bound:ident $(<$target:ident>)? $(+ $additional:ident)*),* $(,)? >;
            argument: $argument:ident :
                IndexedElementSource<Indexed<$via_index:ident, $via_value:ident>, $via_arity:ident>;
            input: OperandHandle<Indexed<$payload_index:ident, $payload_value:ident>, $payload_arity:ident>;
            output: $out_operand:ty;
        }
    ) => {{
        fn apply(
            input: &$crate::dynamic::DynOperand,
            arguments: &[$crate::dynamic::DynInvokeArgument],
            output: $crate::registry::OperandDescriptor,
        ) -> $crate::dynamic::DynOperand {
            type $member = $crate::dynamic::DynIndex;
            type $key = $crate::dynamic::DynIndex;
            type $via_index = $crate::dynamic::DynIndex;
            type $payload_index = $crate::dynamic::DynIndex;

            let _ = std::marker::PhantomData::<
                fn() -> ($member, $key, $via_index, $payload_index),
            >;

            let via = $crate::dynamic::invoke_operand(arguments, 0);
            let transitioned;
            let via = if matches!(
                $crate::dynamic::innermost_lane_kind(via.descriptor()),
                $crate::dynamic::DynLaneKind::IndexedMask
            ) {
                transitioned = via.erase_mask_lane();
                &transitioned
            } else {
                via
            };

            match (
                $crate::dynamic::innermost_lane_kind(input.descriptor()),
                $crate::dynamic::innermost_lane_kind(via.descriptor()),
            ) {
                ($crate::dynamic::DynLaneKind::IndexedMask | $crate::dynamic::DynLaneKind::BareMask, _) => {
                    type $payload_value = $crate::Mask;
                    type $via_value = $crate::dynamic::DynValue;
                    $crate::dynamic::operation_dynamic_via_dispatch!(
                        $operation, $method, input, via, output,
                        argument[$argument], arity[$via_arity], via_value[$via_value],
                        payload[$crate::Indexed<$payload_index, $payload_value>, $payload_arity]
                    )
                }
                (_, _) => {
                    type $payload_value = $crate::dynamic::DynValue;
                    type $via_value = $crate::dynamic::DynValue;
                    $crate::dynamic::operation_dynamic_via_dispatch!(
                        $operation, $method, input, via, output,
                        argument[$argument], arity[$via_arity], via_value[$via_value],
                        payload[$crate::Indexed<$payload_index, $payload_value>, $payload_arity]
                    )
                }
            }
        }

        apply
    }};
    (
        $operation:ty,
        $method:ident,
        policy[$($policy:tt)*],
        {
            group: < $member:ident : IndexDomain, $key:ident : GroupKey $(,)? >;
            parameters: < $($name:ident : $bound:ident $(<$target:ident>)? $(+ $additional:ident)*),* $(,)? >;
            argument: $argument:ident :
                IndexedElementSource<Indexed<$via_index:ident, $via_value:ident>, $via_arity:ident>;
            input: OperandHandle<Bare<$payload_value:ident>, $payload_arity:ident>;
            output: $out_operand:ty;
        }
    ) => {{
        fn apply(
            input: &$crate::dynamic::DynOperand,
            arguments: &[$crate::dynamic::DynInvokeArgument],
            output: $crate::registry::OperandDescriptor,
        ) -> $crate::dynamic::DynOperand {
            type $member = $crate::dynamic::DynIndex;
            type $key = $crate::dynamic::DynIndex;
            type $via_index = $crate::dynamic::DynIndex;

            let _ = std::marker::PhantomData::<fn() -> ($member, $key, $via_index)>;

            let via = $crate::dynamic::invoke_operand(arguments, 0);
            let transitioned;
            let via = if matches!(
                $crate::dynamic::innermost_lane_kind(via.descriptor()),
                $crate::dynamic::DynLaneKind::IndexedMask
            ) {
                transitioned = via.erase_mask_lane();
                &transitioned
            } else {
                via
            };

            match (
                $crate::dynamic::innermost_lane_kind(input.descriptor()),
                $crate::dynamic::innermost_lane_kind(via.descriptor()),
            ) {
                ($crate::dynamic::DynLaneKind::IndexedMask | $crate::dynamic::DynLaneKind::BareMask, _) => {
                    type $payload_value = $crate::Mask;
                    type $via_value = $crate::dynamic::DynValue;
                    $crate::dynamic::operation_dynamic_via_dispatch!(
                        $operation, $method, input, via, output,
                        argument[$argument], arity[$via_arity], via_value[$via_value],
                        payload[$crate::Bare<$payload_value>, $payload_arity]
                    )
                }
                (_, _) => {
                    type $payload_value = $crate::dynamic::DynValue;
                    type $via_value = $crate::dynamic::DynValue;
                    $crate::dynamic::operation_dynamic_via_dispatch!(
                        $operation, $method, input, via, output,
                        argument[$argument], arity[$via_arity], via_value[$via_value],
                        payload[$crate::Bare<$payload_value>, $payload_arity]
                    )
                }
            }
        }

        apply
    }};
    (
        $operation:ty,
        $method:ident,
        policy[$($policy:tt)*],
        {
            group: < $member:ident : IndexDomain, $key:ident : GroupKey $(,)? >;
            parameters: < $payload:ident : Lane $(,)? >;
            $(
                argument: $argument:ident : ArgumentSource<
                    $alignment:ty $(, $argument_value:ty)? $(, Retention = $retention:ty)?
                >
                $(where $argument_owner:ident::ValueDomain : $capability:ident)?;
            )*
            input: $payload_input:ident;
            output: $out_operand:ty;
            $(where $where_owner:ident::Owned : $where_first:ident $(+ $where_more:ident)*;)?
        }
    ) => {{
        fn apply(
            input: &$crate::dynamic::DynOperand,
            arguments: &[$crate::dynamic::DynInvokeArgument],
            output: $crate::registry::OperandDescriptor,
        ) -> $crate::dynamic::DynOperand {
            type $member = $crate::dynamic::DynIndex;
            type $key = $crate::dynamic::DynIndex;
            type $payload = $crate::dynamic::DynPayload;

            let _ = std::marker::PhantomData::<fn() -> ($member, $key, $payload)>;

            $crate::dynamic::operation_dynamic_group_build!(
                $operation,
                $method,
                input,
                arguments,
                output,
                policy[$($policy)*],
                payload,
                arguments[$(($argument, $alignment $(, $argument_value)? $(; $retention)?)),*]
            )
        }

        apply
    }};
    (
        $operation:ty,
        $method:ident,
        policy[$($policy:tt)*],
        {
            group: < $member:ident : IndexDomain, $key:ident : GroupKey $(,)? >;
            parameters: < $index:ident : IndexDomain, $value:ident : ValueDomain $(,)? >;
            $(
                argument: $argument:ident : ArgumentSource<
                    $alignment:ty $(, $argument_value:ty)? $(, Retention = $retention:ty)?
                >
                $(where $argument_owner:ident::ValueDomain : $capability:ident)?;
            )*
            input: OperandHandle<Indexed<$input_index:ident, $input_value:ident>, $arity:ty>;
            output: $out_operand:ty;
            $(where $where_owner:ident::Owned : $where_first:ident $(+ $where_more:ident)*;)?
        }
    ) => {{
        fn apply(
            input: &$crate::dynamic::DynOperand,
            arguments: &[$crate::dynamic::DynInvokeArgument],
            output: $crate::registry::OperandDescriptor,
        ) -> $crate::dynamic::DynOperand {
            type $member = $crate::dynamic::DynIndex;
            type $key = $crate::dynamic::DynIndex;
            type $index = $crate::dynamic::DynIndex;

            let _ = std::marker::PhantomData::<fn() -> ($member, $key, $index)>;

            match $crate::dynamic::innermost_lane_kind(input.descriptor()) {
                $crate::dynamic::DynLaneKind::IndexedValue => {
                    type $value = $crate::dynamic::DynValue;

                    $crate::dynamic::operation_dynamic_group_build!(
                        $operation,
                        $method,
                        input,
                        arguments,
                        output,
                        policy[$($policy)*],
                        lane[$crate::Indexed<$index, $value>, $arity],
                        arguments[$(($argument, $alignment $(, $argument_value)? $(; $retention)?)),*]
                    )
                }
                $crate::dynamic::DynLaneKind::IndexedMask => {
                    type $value = $crate::Mask;

                    $crate::dynamic::operation_dynamic_group_build!(
                        $operation,
                        $method,
                        input,
                        arguments,
                        output,
                        policy[$($policy)*],
                        lane[$crate::Indexed<$index, $value>, $arity],
                        arguments[$(($argument, $alignment $(, $argument_value)? $(; $retention)?)),*]
                    )
                }
                $crate::dynamic::DynLaneKind::IndexedUnit => {
                    type $value = $crate::Unit;

                    $crate::dynamic::operation_dynamic_group_build!(
                        $operation,
                        $method,
                        input,
                        arguments,
                        output,
                        policy[$($policy)*],
                        lane[$crate::Indexed<$index, $value>, $arity],
                        arguments[$(($argument, $alignment $(, $argument_value)? $(; $retention)?)),*]
                    )
                }
                _ => panic!("registry selected an indexed group kernel for a different lane shape"),
            }
        }

        apply
    }};
    (
        $operation:ty,
        $method:ident,
        policy[$($policy:tt)*],
        {
            group: < $member:ident : IndexDomain, $key:ident : GroupKey $(,)? >;
            parameters: < $value:ident : BareValueDomain $(,)? >;
            $(
                argument: $argument:ident : ArgumentSource<
                    $alignment:ty $(, $argument_value:ty)? $(, Retention = $retention:ty)?
                >
                $(where $argument_owner:ident::ValueDomain : $capability:ident)?;
            )*
            input: OperandHandle<Bare<$input_value:ident>, $arity:ty>;
            output: $out_operand:ty;
            $(where $where_owner:ident::Owned : $where_first:ident $(+ $where_more:ident)*;)?
        }
    ) => {{
        fn apply(
            input: &$crate::dynamic::DynOperand,
            arguments: &[$crate::dynamic::DynInvokeArgument],
            output: $crate::registry::OperandDescriptor,
        ) -> $crate::dynamic::DynOperand {
            type $member = $crate::dynamic::DynIndex;
            type $key = $crate::dynamic::DynIndex;

            let _ = std::marker::PhantomData::<fn() -> ($member, $key)>;

            match $crate::dynamic::innermost_lane_kind(input.descriptor()) {
                $crate::dynamic::DynLaneKind::BareValue => {
                    type $value = $crate::dynamic::DynValue;

                    $crate::dynamic::operation_dynamic_group_build!(
                        $operation,
                        $method,
                        input,
                        arguments,
                        output,
                        policy[$($policy)*],
                        lane[$crate::Bare<$value>, $arity],
                        arguments[$(($argument, $alignment $(, $argument_value)? $(; $retention)?)),*]
                    )
                }
                $crate::dynamic::DynLaneKind::BareMask => {
                    type $value = $crate::Mask;

                    $crate::dynamic::operation_dynamic_group_build!(
                        $operation,
                        $method,
                        input,
                        arguments,
                        output,
                        policy[$($policy)*],
                        lane[$crate::Bare<$value>, $arity],
                        arguments[$(($argument, $alignment $(, $argument_value)? $(; $retention)?)),*]
                    )
                }
                _ => panic!("registry selected a bare group kernel for a different lane shape"),
            }
        }

        apply
    }};
    (
        $operation:ty,
        $method:ident,
        policy[$($policy:tt)*],
        {
            group: < $member:ident : IndexDomain, $key:ident : GroupKey $(,)? >;
            parameters: < $index:ident : IndexDomain, $value:ident : ValueDomain, $order:ident : OrderState $(,)? >;
            $(
                argument: $argument:ident : ArgumentSource<
                    $alignment:ty $(, $argument_value:ty)? $(, Retention = $retention:ty)?
                >
                $(where $argument_owner:ident::ValueDomain : $capability:ident)?;
            )*
            input: OperandHandle<Indexed<$input_index:ident, $input_value:ident>, Multiple<$input_order:ident>>;
            output: $out_operand:ty;
            $(where $where_owner:ident::Owned : $where_first:ident $(+ $where_more:ident)*;)?
        }
    ) => {{
        fn apply(
            input: &$crate::dynamic::DynOperand,
            arguments: &[$crate::dynamic::DynInvokeArgument],
            output: $crate::registry::OperandDescriptor,
        ) -> $crate::dynamic::DynOperand {
            type $member = $crate::dynamic::DynIndex;
            type $key = $crate::dynamic::DynIndex;
            type $index = $crate::dynamic::DynIndex;

            let _ = std::marker::PhantomData::<fn() -> ($member, $key, $index)>;

            match (
                $crate::dynamic::innermost_lane_kind(input.descriptor()),
                input.descriptor().lane_arity(),
            ) {
                (
                    $crate::dynamic::DynLaneKind::IndexedValue,
                    $crate::registry::ArityDescriptor::Multiple {
                        order: $crate::registry::OrderDescriptor::Ordered,
                    },
                ) => {
                    type $value = $crate::dynamic::DynValue;
                    type $order = $crate::Ordered;

                    $crate::dynamic::operation_dynamic_group_build!(
                        $operation,
                        $method,
                        input,
                        arguments,
                        output,
                        policy[$($policy)*],
                        lane[$crate::Indexed<$index, $value>, $crate::Multiple<$order>],
                        arguments[$(($argument, $alignment $(, $argument_value)? $(; $retention)?)),*]
                    )
                }
                (
                    $crate::dynamic::DynLaneKind::IndexedValue,
                    $crate::registry::ArityDescriptor::Multiple {
                        order: $crate::registry::OrderDescriptor::Unordered,
                    },
                ) => {
                    type $value = $crate::dynamic::DynValue;
                    type $order = $crate::Unordered;

                    $crate::dynamic::operation_dynamic_group_build!(
                        $operation,
                        $method,
                        input,
                        arguments,
                        output,
                        policy[$($policy)*],
                        lane[$crate::Indexed<$index, $value>, $crate::Multiple<$order>],
                        arguments[$(($argument, $alignment $(, $argument_value)? $(; $retention)?)),*]
                    )
                }
                (
                    $crate::dynamic::DynLaneKind::IndexedMask,
                    $crate::registry::ArityDescriptor::Multiple {
                        order: $crate::registry::OrderDescriptor::Ordered,
                    },
                ) => {
                    type $value = $crate::Mask;
                    type $order = $crate::Ordered;

                    $crate::dynamic::operation_dynamic_group_build!(
                        $operation,
                        $method,
                        input,
                        arguments,
                        output,
                        policy[$($policy)*],
                        lane[$crate::Indexed<$index, $value>, $crate::Multiple<$order>],
                        arguments[$(($argument, $alignment $(, $argument_value)? $(; $retention)?)),*]
                    )
                }
                (
                    $crate::dynamic::DynLaneKind::IndexedMask,
                    $crate::registry::ArityDescriptor::Multiple {
                        order: $crate::registry::OrderDescriptor::Unordered,
                    },
                ) => {
                    type $value = $crate::Mask;
                    type $order = $crate::Unordered;

                    $crate::dynamic::operation_dynamic_group_build!(
                        $operation,
                        $method,
                        input,
                        arguments,
                        output,
                        policy[$($policy)*],
                        lane[$crate::Indexed<$index, $value>, $crate::Multiple<$order>],
                        arguments[$(($argument, $alignment $(, $argument_value)? $(; $retention)?)),*]
                    )
                }
                (
                    $crate::dynamic::DynLaneKind::IndexedUnit,
                    $crate::registry::ArityDescriptor::Multiple {
                        order: $crate::registry::OrderDescriptor::Ordered,
                    },
                ) => {
                    type $value = $crate::Unit;
                    type $order = $crate::Ordered;

                    $crate::dynamic::operation_dynamic_group_build!(
                        $operation,
                        $method,
                        input,
                        arguments,
                        output,
                        policy[$($policy)*],
                        lane[$crate::Indexed<$index, $value>, $crate::Multiple<$order>],
                        arguments[$(($argument, $alignment $(, $argument_value)? $(; $retention)?)),*]
                    )
                }
                (
                    $crate::dynamic::DynLaneKind::IndexedUnit,
                    $crate::registry::ArityDescriptor::Multiple {
                        order: $crate::registry::OrderDescriptor::Unordered,
                    },
                ) => {
                    type $value = $crate::Unit;
                    type $order = $crate::Unordered;

                    $crate::dynamic::operation_dynamic_group_build!(
                        $operation,
                        $method,
                        input,
                        arguments,
                        output,
                        policy[$($policy)*],
                        lane[$crate::Indexed<$index, $value>, $crate::Multiple<$order>],
                        arguments[$(($argument, $alignment $(, $argument_value)? $(; $retention)?)),*]
                    )
                }
                _ => panic!("registry selected an indexed multiple group kernel for a different lane state"),
            }
        }

        apply
    }};
    (
        $operation:ty,
        $method:ident,
        policy[$($policy:tt)*],
        {
            group: < $member:ident : IndexDomain, $key:ident : GroupKey $(,)? >;
            parameters: < $value:ident : BareValueDomain, $order:ident : OrderState $(,)? >;
            $(
                argument: $argument:ident : ArgumentSource<
                    $alignment:ty $(, $argument_value:ty)? $(, Retention = $retention:ty)?
                >
                $(where $argument_owner:ident::ValueDomain : $capability:ident)?;
            )*
            input: OperandHandle<Bare<$input_value:ident>, Multiple<$input_order:ident>>;
            output: $out_operand:ty;
            $(where $where_owner:ident::Owned : $where_first:ident $(+ $where_more:ident)*;)?
        }
    ) => {{
        fn apply(
            input: &$crate::dynamic::DynOperand,
            arguments: &[$crate::dynamic::DynInvokeArgument],
            output: $crate::registry::OperandDescriptor,
        ) -> $crate::dynamic::DynOperand {
            type $member = $crate::dynamic::DynIndex;
            type $key = $crate::dynamic::DynIndex;

            let _ = std::marker::PhantomData::<fn() -> ($member, $key)>;

            match (
                $crate::dynamic::innermost_lane_kind(input.descriptor()),
                input.descriptor().lane_arity(),
            ) {
                (
                    $crate::dynamic::DynLaneKind::BareValue,
                    $crate::registry::ArityDescriptor::Multiple {
                        order: $crate::registry::OrderDescriptor::Ordered,
                    },
                ) => {
                    type $value = $crate::dynamic::DynValue;
                    type $order = $crate::Ordered;

                    $crate::dynamic::operation_dynamic_group_build!(
                        $operation,
                        $method,
                        input,
                        arguments,
                        output,
                        policy[$($policy)*],
                        lane[$crate::Bare<$value>, $crate::Multiple<$order>],
                        arguments[$(($argument, $alignment $(, $argument_value)? $(; $retention)?)),*]
                    )
                }
                (
                    $crate::dynamic::DynLaneKind::BareValue,
                    $crate::registry::ArityDescriptor::Multiple {
                        order: $crate::registry::OrderDescriptor::Unordered,
                    },
                ) => {
                    type $value = $crate::dynamic::DynValue;
                    type $order = $crate::Unordered;

                    $crate::dynamic::operation_dynamic_group_build!(
                        $operation,
                        $method,
                        input,
                        arguments,
                        output,
                        policy[$($policy)*],
                        lane[$crate::Bare<$value>, $crate::Multiple<$order>],
                        arguments[$(($argument, $alignment $(, $argument_value)? $(; $retention)?)),*]
                    )
                }
                (
                    $crate::dynamic::DynLaneKind::BareMask,
                    $crate::registry::ArityDescriptor::Multiple {
                        order: $crate::registry::OrderDescriptor::Ordered,
                    },
                ) => {
                    type $value = $crate::Mask;
                    type $order = $crate::Ordered;

                    $crate::dynamic::operation_dynamic_group_build!(
                        $operation,
                        $method,
                        input,
                        arguments,
                        output,
                        policy[$($policy)*],
                        lane[$crate::Bare<$value>, $crate::Multiple<$order>],
                        arguments[$(($argument, $alignment $(, $argument_value)? $(; $retention)?)),*]
                    )
                }
                (
                    $crate::dynamic::DynLaneKind::BareMask,
                    $crate::registry::ArityDescriptor::Multiple {
                        order: $crate::registry::OrderDescriptor::Unordered,
                    },
                ) => {
                    type $value = $crate::Mask;
                    type $order = $crate::Unordered;

                    $crate::dynamic::operation_dynamic_group_build!(
                        $operation,
                        $method,
                        input,
                        arguments,
                        output,
                        policy[$($policy)*],
                        lane[$crate::Bare<$value>, $crate::Multiple<$order>],
                        arguments[$(($argument, $alignment $(, $argument_value)? $(; $retention)?)),*]
                    )
                }
                _ => panic!("registry selected a bare multiple group kernel for a different lane state"),
            }
        }

        apply
    }};
    (
        $operation:ty,
        $method:ident,
        policy[$($policy:tt)*],
        {
            group: < $member:ident : IndexDomain, $key:ident : GroupKey $(,)? >;
            parameters: < $shape:ident : ElementShape, $arity:ident : Arity $(,)? >;
            $(
                argument: $argument:ident : ArgumentSource<
                    $alignment:ty $(, $argument_value:ty)? $(, Retention = $retention:ty)?
                >
                $(where $argument_owner:ident::ValueDomain : $capability:ident)?;
            )*
            input: OperandHandle<$input_shape:ident, $input_arity:ident>;
            output: $out_operand:ty;
            $(where $where_owner:ident::Owned : $where_first:ident $(+ $where_more:ident)*;)?
        }
    ) => {{
        fn apply(
            input: &$crate::dynamic::DynOperand,
            arguments: &[$crate::dynamic::DynInvokeArgument],
            output: $crate::registry::OperandDescriptor,
        ) -> $crate::dynamic::DynOperand {
            match $crate::dynamic::innermost_lane_kind(input.descriptor()) {
                $crate::dynamic::DynLaneKind::IndexedValue => {
                    $crate::dynamic::operation_dynamic_group_all_arities!(
                        $crate::Indexed<$crate::dynamic::DynIndex, $crate::dynamic::DynValue>,
                        $operation,
                        $method,
                        input,
                        arguments,
                        output,
                        policy[$($policy)*],
                        $member,
                        $key,
                        $shape,
                        $arity,
                        arguments[$(($argument, $alignment $(, $argument_value)? $(; $retention)?)),*]
                    )
                }
                $crate::dynamic::DynLaneKind::IndexedMask => {
                    $crate::dynamic::operation_dynamic_group_all_arities!(
                        $crate::Indexed<$crate::dynamic::DynIndex, $crate::Mask>,
                        $operation,
                        $method,
                        input,
                        arguments,
                        output,
                        policy[$($policy)*],
                        $member,
                        $key,
                        $shape,
                        $arity,
                        arguments[$(($argument, $alignment $(, $argument_value)? $(; $retention)?)),*]
                    )
                }
                $crate::dynamic::DynLaneKind::IndexedUnit => {
                    $crate::dynamic::operation_dynamic_group_all_arities!(
                        $crate::Indexed<$crate::dynamic::DynIndex, $crate::Unit>,
                        $operation,
                        $method,
                        input,
                        arguments,
                        output,
                        policy[$($policy)*],
                        $member,
                        $key,
                        $shape,
                        $arity,
                        arguments[$(($argument, $alignment $(, $argument_value)? $(; $retention)?)),*]
                    )
                }
                $crate::dynamic::DynLaneKind::BareValue => {
                    $crate::dynamic::operation_dynamic_group_all_arities!(
                        $crate::Bare<$crate::dynamic::DynValue>,
                        $operation,
                        $method,
                        input,
                        arguments,
                        output,
                        policy[$($policy)*],
                        $member,
                        $key,
                        $shape,
                        $arity,
                        arguments[$(($argument, $alignment $(, $argument_value)? $(; $retention)?)),*]
                    )
                }
                $crate::dynamic::DynLaneKind::BareMask => {
                    $crate::dynamic::operation_dynamic_group_all_arities!(
                        $crate::Bare<$crate::Mask>,
                        $operation,
                        $method,
                        input,
                        arguments,
                        output,
                        policy[$($policy)*],
                        $member,
                        $key,
                        $shape,
                        $arity,
                        arguments[$(($argument, $alignment $(, $argument_value)? $(; $retention)?)),*]
                    )
                }
            }
        }

        apply
    }};
}

pub(crate) use operation_dynamic_alias;
pub(crate) use operation_dynamic_aliases;
pub(crate) use operation_dynamic_argument_type;
pub(crate) use operation_dynamic_argument_value;
pub(crate) use operation_dynamic_arguments;
pub(crate) use operation_dynamic_capture;
pub(crate) use operation_dynamic_element_apply;
pub(crate) use operation_dynamic_element_build;
pub(crate) use operation_dynamic_element_entity;
pub(crate) use operation_dynamic_entity_dispatch;
pub(crate) use operation_dynamic_expansion_apply;
pub(crate) use operation_dynamic_field;
pub(crate) use operation_dynamic_group_all_arities;
pub(crate) use operation_dynamic_group_apply;
pub(crate) use operation_dynamic_group_build;
pub(crate) use operation_dynamic_lane_apply;
pub(crate) use operation_dynamic_lane_build;
pub(crate) use operation_dynamic_lane_entity;
pub(crate) use operation_dynamic_lane_function;
pub(crate) use operation_dynamic_receiver_dispatch;
pub(crate) use operation_dynamic_selected_argument;
pub(crate) use operation_dynamic_set_arity;
pub(crate) use operation_dynamic_set_build;
pub(crate) use operation_dynamic_shape_apply;
pub(crate) use operation_dynamic_via_dispatch;
pub(crate) use operation_element_applier;
pub(crate) use operation_group_applier;
pub(crate) use operation_lane_applier;
