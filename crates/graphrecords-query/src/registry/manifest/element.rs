macro_rules! operation_element_witness {
    (
        $operation:ty,
        $trait:ident $(<$($trait_argument:ty),+ $(,)?>)?,
        $method:ident,
        policy[$($policy:tt)*],
    ) => {};
    (
        $operation:ty,
        $trait:ident $(<$($trait_argument:ty),+ $(,)?>)?,
        $method:ident,
        policy[$($policy:tt)*],
        $kernel:tt
        $($remaining:tt)*
    ) => {
        $crate::registry::operation_element_witness!(
            @kernel
            $operation,
            $trait $(<$($trait_argument),+>)?,
            $method,
            policy[$($policy)*],
            $kernel
        );
        $crate::registry::operation_element_witness!(
            $operation,
            $trait $(<$($trait_argument),+>)?,
            $method,
            policy[$($policy)*],
            $($remaining)*
        );
    };
    (
        @kernel
        $operation:ty,
        $trait:ident $(<$($trait_argument:ty),+ $(,)?>)?,
        $method:ident,
        policy[$($policy:tt)*], {
            parameters: < $($name:ident : $bound:ident $(<$target:ident>)? $(+ $additional:ident)*),* $(,)? >;
            argument: $argument:ident : SetSource<$set_value:ty>;
            input: $shape:ty;
            output: $out_shape:ty;
            emission: $emission:ty;
        }
    ) => {{
        {
            $($crate::registry::manifest_witness_alias!($crate::Ordered, $name, $bound $(<$target>)? $(+ $additional)*);)*
            $crate::registry::manifest_witness_set_argument_alias!($argument, $set_value);

            const fn verify_scope()
            where
                $operation: $crate::operations::Operation<Scope = $crate::operations::Element>,
            {
            }

            const fn verify_kernel()
            where
                $operation: $crate::operations::ElementKernel<
                    $shape,
                    OutShape = $out_shape,
                    Emission = $emission,
                >,
            {
            }

            verify_scope();
            verify_kernel();
            $crate::registry::operation_element_method!(
                $trait $(<$($trait_argument),+>)?, $method, $shape, policy[$($policy)*],
            );
        }
        {
            $($crate::registry::manifest_witness_alias!($crate::Unordered, $name, $bound $(<$target>)? $(+ $additional)*);)*
            $crate::registry::manifest_witness_set_argument_alias!($argument, $set_value);

            const fn verify_kernel()
            where
                $operation: $crate::operations::ElementKernel<
                    $shape,
                    OutShape = $out_shape,
                    Emission = $emission,
                >,
            {
            }

            verify_kernel();
        }
    }};
    (
        @kernel
        $operation:ty,
        $trait:ident $(<$($trait_argument:ty),+ $(,)?>)?,
        $method:ident,
        policy[$($policy:tt)*], {
            parameters: < $($name:ident : $bound:ident $(<$target:ident>)? $(+ $additional:ident)*),* $(,)? >;
            $(selector: $selector:ty;)?
            $(field: $field:ident : $field_type:ty;)*
            $(
                argument: $argument:ident : ArgumentSource<$alignment:ty $(, $value:ty)?>
                $(where $argument_owner:ident::ValueDomain : $capability:ident)?;
            )*
            $(receiver: $receiver:ident;)?
            input: $shape:ty;
            output: $out_shape:ty;
            emission: $emission:ty;
            $(where $where_owner:ident::Owned : $where_first:ident $(+ $where_more:ident)*;)?
        }
    ) => {{
        {
            $($crate::registry::manifest_witness_alias!($crate::Ordered, $name, $bound $(<$target>)? $(+ $additional)*);)*
            $(
                $crate::registry::manifest_witness_argument_alias!(
                    $argument, $alignment $(, $value)? $(; $capability)?
                );
            )*

            const fn verify_scope()
            where
                $operation: $crate::operations::Operation<Scope = $crate::operations::Element>,
            {
            }

            const fn verify_kernel()
            where
                $operation: $crate::operations::ElementKernel<
                    $shape,
                    OutShape = $out_shape,
                    Emission = $emission,
                >,
                $(<$where_owner as $crate::ValueDomain>::Owned: $where_first $(+ $where_more)*,)?
            {
            }

            verify_scope();
            verify_kernel();
            $crate::registry::operation_element_method!(
                $trait $(<$($trait_argument),+>)?, $method, $shape, policy[$($policy)*],
                $($receiver)?
            );
        }
        {
            $($crate::registry::manifest_witness_alias!($crate::Unordered, $name, $bound $(<$target>)? $(+ $additional)*);)*
            $(
                $crate::registry::manifest_witness_argument_alias!(
                    $argument, $alignment $(, $value)? $(; $capability)?
                );
            )*

            const fn verify_kernel()
            where
                $operation: $crate::operations::ElementKernel<
                    $shape,
                    OutShape = $out_shape,
                    Emission = $emission,
                >,
                $(<$where_owner as $crate::ValueDomain>::Owned: $where_first $(+ $where_more)*,)?
            {
            }

            verify_kernel();
        }
    }};
}

macro_rules! operation_element_method {
    (
        $trait:ident $(<$($trait_argument:ty),+ $(,)?>)?,
        $method:ident,
        $shape:ty,
        policy[$($policy:tt)*],
    ) => {
        $crate::registry::operation_policy_method!(
            $trait $(<$($trait_argument),+>)?,
            $method,
            policy[$($policy)*],
            $crate::operands::OperandHandle<$shape, $crate::Multiple<$crate::Ordered>>
        );
    };
    ($trait:ident, $method:ident, $shape:ty, policy[], $receiver:ident) => {
        const fn verify_method<
            O: $trait<
                $crate::operands::OperandHandle<$shape, $crate::Multiple<$crate::Ordered>>,
            >,
        >() {
            let _ = O::$method;
        }

        verify_method::<$receiver>();
    };
}

macro_rules! operation_element_entry {
    (
        $operation:ty,
        $method:ident,
        policy[$($policy:tt)*],
        $dynamic_kernel:tt,
        {
            parameters: < $($name:ident : $bound:ident $(<$target:ident>)? $(+ $additional:ident)*),* $(,)? >;
            argument: $argument:ident : SetSource<$set_value:ty>;
            input: $shape:ty;
            output: $out_shape:ty;
            emission: $emission:ty;
        }
    ) => {{
        $crate::registry::manifest_entry_aliases!(
            (0)
            $($name : $bound $(<$target>)? $(+ $additional)*,)*
        );

        $crate::registry::OperationManifestEntry::element::<$shape, $out_shape>(
            vec![$crate::registry::manifest_entry_set_argument_pattern!($set_value)],
            <$emission as $crate::registry::describe::DescribeEmission>::emission_spec(),
            #[cfg(feature = "dynamic")]
            $crate::dynamic::operation_element_applier!(
                $operation,
                $method,
                policy[$($policy)*],
                $dynamic_kernel
            ),
        )
    }};
    (
        $operation:ty,
        $method:ident,
        policy[$($policy:tt)*],
        $dynamic_kernel:tt,
        {
            parameters: < $($name:ident : $bound:ident $(<$target:ident>)? $(+ $additional:ident)*),* $(,)? >;
            $(selector: $selector:ident;)?
            $(field: $field:ident : $field_type:ident;)*
            $(
                argument: $argument:ident : ArgumentSource<$alignment:ty $(, $value:ty)?>
                $(where $argument_owner:ident::ValueDomain : $capability:ident)?;
            )*
            $(receiver: $receiver:ident;)?
            input: $shape:ty;
            output: $out_shape:ty;
            emission: $emission:ty;
            $(where $where_owner:ident::Owned : $where_first:ident $(+ $where_more:ident)*;)?
        }
    ) => {{
        $crate::registry::manifest_entry_aliases!(
            (0)
            $($name : $bound $(<$target>)? $(+ $additional)*,)*
        );

        $crate::registry::OperationManifestEntry::element::<$shape, $out_shape>(
            vec![
                $($crate::registry::ArgumentPattern::selector::<$selector>(),)?
                $($crate::registry::ArgumentPattern::field::<$field_type>(),)*
                $(
                    $crate::registry::manifest_entry_argument_pattern!(
                        $alignment $(, $value)? $(; $capability)?
                    ),
                )*
            ],
            <$emission as $crate::registry::describe::DescribeEmission>::emission_spec(),
            #[cfg(feature = "dynamic")]
            $crate::dynamic::operation_element_applier!(
                $operation,
                $method,
                policy[$($policy)*],
                $dynamic_kernel
            ),
        )
    }};
}

pub(crate) use operation_element_entry;
pub(crate) use operation_element_method;
pub(crate) use operation_element_witness;
