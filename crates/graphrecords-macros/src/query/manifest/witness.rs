use super::model::{Kernel, Manifest, Parameter, Scope, ValueArgument};
use proc_macro2::TokenStream;
use quote::quote;
use syn::{Error, Ident, Path, Result, Type};

pub fn witness(manifest: &Manifest, query: &Path) -> Result<TokenStream> {
    let kernels = manifest
        .kernels
        .iter()
        .map(|kernel| kernel_witness(manifest, kernel, query))
        .collect::<Result<Vec<_>>>()?;

    Ok(quote! {
        const _: () = {
            #(#kernels)*
            ;
        };
    })
}

fn kernel_witness(manifest: &Manifest, kernel: &Kernel, query: &Path) -> Result<TokenStream> {
    let ordered = kernel_side(manifest, kernel, query, true)?;
    let unordered = kernel_side(manifest, kernel, query, false)?;

    Ok(quote! {
        {
            { #ordered }
            { #unordered }
        };
    })
}

fn kernel_side(
    manifest: &Manifest,
    kernel: &Kernel,
    query: &Path,
    ordered: bool,
) -> Result<TokenStream> {
    let order = if ordered {
        quote!(#query::Ordered)
    } else {
        quote!(#query::Unordered)
    };

    let mut statements = TokenStream::new();

    if let Some(group) = &kernel.group {
        let member = &group.member;
        let key = &group.key;
        statements.extend(quote! {
            type #member = #query::registry::GroupMemberWitness;
            type #key = #query::registry::GroupKeyWitness;
        });
    }

    for parameter in &kernel.parameters {
        statements.extend(parameter_alias(query, &order, parameter)?);
    }

    statements.extend(argument_aliases(manifest, kernel, query)?);

    let operation = &manifest.operation;
    if ordered {
        let scope = match manifest.scope {
            Scope::Element => quote!(Element),
            Scope::Lane => quote!(Lane),
            Scope::Group => quote!(Group),
        };
        statements.extend(quote! {
            const fn verify_scope()
            where
                #operation: #query::operations::Operation<Scope = #query::operations::#scope>,
            {
            }
        });
    }

    statements.extend(verify_kernel(manifest, kernel, query)?);

    if ordered {
        statements.extend(quote! {
            verify_scope();
            verify_kernel();
        });
        statements.extend(verify_method(manifest, kernel, query)?);
    } else {
        statements.extend(quote! {
            verify_kernel();
        });
    }

    Ok(statements)
}

fn argument_aliases(manifest: &Manifest, kernel: &Kernel, query: &Path) -> Result<TokenStream> {
    if let Some(set) = kernel.set_argument() {
        if manifest.scope != Scope::Element {
            return Err(Error::new(
                set.name.span(),
                "a set argument requires element scope",
            ));
        }
        let name = &set.name;
        let value = &set.value;

        return Ok(quote! {
            type #name = #query::registry::SetSourceWitness<#value>;
        });
    }

    if let Some(via) = kernel.via_argument() {
        if manifest.scope != Scope::Group {
            return Err(Error::new(
                via.name.span(),
                "a via argument requires group scope",
            ));
        }
        let name = &via.name;
        let index = &via.index;
        let value = &via.value;
        let arity = &via.arity;

        return Ok(quote! {
            type #name = #query::expressions::ExpressionHandle<
                #query::Indexed<#index, #value>,
                #arity,
            >;
        });
    }

    if let Some(retained) = kernel.retention_argument() {
        if manifest.scope != Scope::Lane {
            return Err(Error::new(
                retained.name.span(),
                "a retention argument requires lane scope",
            ));
        }

        return argument_alias(query, retained);
    }

    let mut aliases = TokenStream::new();
    for argument in kernel.value_arguments()? {
        if argument.retention.is_some() {
            return Err(Error::new(
                argument.name.span(),
                "a retention argument must be the kernel's only argument",
            ));
        }
        aliases.extend(argument_alias(query, argument)?);
    }

    Ok(aliases)
}

fn argument_alias(query: &Path, argument: &ValueArgument) -> Result<TokenStream> {
    let name = &argument.name;
    let alignment = &argument.alignment;

    let witness = match (&argument.value, &argument.retention, &argument.capability) {
        (Some(value), Some(retention), None) => {
            quote!(#query::registry::ArgumentWitness<#alignment, #value, #retention>)
        }
        (Some(value), None, None) => {
            quote!(#query::registry::ArgumentWitness<#alignment, #value>)
        }
        (None, None, Some(capability)) => {
            let capability = capability_witness(
                query,
                capability,
                None,
                &quote!(#query::registry::ValueDomainOnly),
            )?;
            quote!(#query::registry::ArgumentWitness<#alignment, #capability>)
        }
        (None, None, None) => quote! {
            #query::registry::ArgumentWitness<
                #alignment,
                #query::registry::ValueWitness<
                    #query::registry::ValueDomainCapability,
                    #query::registry::ValueDomainOnly,
                >,
            >
        },
        _ => {
            return Err(Error::new(
                name.span(),
                "an argument must not combine a value with a capability",
            ));
        }
    };

    Ok(quote! {
        type #name = #witness;
    })
}

fn verify_kernel(manifest: &Manifest, kernel: &Kernel, query: &Path) -> Result<TokenStream> {
    let operation = &manifest.operation;
    let owned = kernel.where_owned.as_ref().map(|constraint| {
        let owner = &constraint.owner;
        let bounds = &constraint.bounds;
        quote!(<#owner as #query::ValueDomain>::Owned: #(#bounds)+*,)
    });

    let bound = match manifest.scope {
        Scope::Lane => {
            let super::model::KernelInput::Lane { shape, arity } = &kernel.input else {
                return Err(Error::new_spanned(
                    &kernel.output,
                    "a lane kernel input must pair a shape with an arity",
                ));
            };
            let output = &kernel.output;
            quote!(#query::operations::LaneKernel<#shape, #arity, Output = #output>)
        }
        Scope::Element => {
            let shape = kernel.shape();
            let out_shape = &kernel.output;
            let Some(emission) = &kernel.emission else {
                return Err(Error::new_spanned(
                    &kernel.output,
                    "an element kernel must declare an emission",
                ));
            };
            quote! {
                #query::operations::ElementKernel<
                    #shape,
                    OutShape = #out_shape,
                    Emission = #emission,
                >
            }
        }
        Scope::Group => {
            let Some(group) = &kernel.group else {
                return Err(Error::new_spanned(
                    &kernel.output,
                    "a group kernel must declare its group domains",
                ));
            };
            let member = &group.member;
            let key = &group.key;
            let payload = kernel.shape();
            let output = &kernel.output;
            quote! {
                #query::operations::GroupKernel<
                    #member,
                    #key,
                    #payload,
                    Output = #output,
                >
            }
        }
    };

    Ok(quote! {
        const fn verify_kernel()
        where
            #operation: #bound,
            #owned
        {
        }
    })
}

fn verify_method(manifest: &Manifest, kernel: &Kernel, query: &Path) -> Result<TokenStream> {
    match manifest.scope {
        Scope::Lane => {
            let super::model::KernelInput::Lane { shape, arity } = &kernel.input else {
                return Err(Error::new_spanned(
                    &kernel.output,
                    "a lane kernel input must pair a shape with an arity",
                ));
            };
            let receiver = quote!(#query::expressions::ExpressionHandle<#shape, #arity>);
            policy_method(manifest, query, None, &receiver)
        }
        Scope::Element => {
            let shape = kernel.shape();
            if let Some(receiver) = &kernel.receiver {
                if manifest.policy.is_some() || manifest.trait_arguments.is_some() {
                    return Err(Error::new(
                        receiver.span(),
                        "a receiver method must use a bare trait without a policy",
                    ));
                }
                let method_trait = &manifest.method_trait;
                let method = &manifest.method;

                return Ok(quote! {
                    const fn verify_method<
                        E: #method_trait<
                            #query::expressions::ExpressionHandle<#shape, #query::Multiple<#query::Ordered>>,
                        >,
                    >() {
                        let _ = E::#method;
                    }

                    verify_method::<#receiver>();
                    ;
                });
            }
            let receiver = quote! {
                #query::expressions::ExpressionHandle<#shape, #query::Multiple<#query::Ordered>>
            };
            policy_method(manifest, query, kernel.selector.as_ref(), &receiver)
        }
        Scope::Group => {
            let Some(group) = &kernel.group else {
                return Err(Error::new_spanned(
                    &kernel.output,
                    "a group kernel must declare its group domains",
                ));
            };
            let member = &group.member;
            let key = &group.key;
            let payload = kernel.shape();
            let receiver = quote!(#query::expressions::GroupedExpression<#member, #key, #payload>);
            policy_method(manifest, query, None, &receiver)
        }
    }
}

fn policy_method(
    manifest: &Manifest,
    query: &Path,
    target: Option<&Path>,
    receiver: &TokenStream,
) -> Result<TokenStream> {
    let method = &manifest.method;

    if let Some(policy) = &manifest.policy {
        let policy = &policy.path;
        let trait_name = manifest.method_trait.to_string();
        let policy_bound = match (trait_name.as_str(), &manifest.trait_arguments) {
            ("OnError", None) => {
                quote!(#query::operations::ErrorPolicy<<E as OnError>::Expression>)
            }
            ("OnBucketError", None) => {
                quote!(#query::operations::BucketErrorPolicy<<E as OnBucketError>::Expression>)
            }
            ("OnKeyError", None) => {
                quote!(#query::operations::KeyErrorPolicy<<E as OnKeyError>::Expression>)
            }
            _ => {
                return Err(Error::new(
                    manifest.method_trait.span(),
                    "a policy requires an error handling trait",
                ));
            }
        };
        let method_trait = &manifest.method_trait;

        return Ok(quote! {
            const fn verify_method<E: #method_trait>()
            where
                #policy: #policy_bound,
            {
                let _ = E::#method::<#policy>;
            }

            verify_method::<#receiver>();
            ;
        });
    }

    if manifest.method_trait == "Transition"
        && manifest.trait_arguments.is_none()
        && let Some(target) = target
    {
        return Ok(quote! {
            const fn verify_method<E>()
            where
                E: Transition,
                <E as Transition>::Expression:
                    #query::operations::Apply<#query::operations::TransitionOperation<#target>>,
            {
                let _ = E::#method::<#target>;
            }

            verify_method::<#receiver>();
            ;
        });
    }

    let method_trait = &manifest.method_trait;
    let trait_arguments = manifest
        .trait_arguments
        .as_ref()
        .map(|arguments| quote!(<#(#arguments),*>));

    Ok(quote! {
        const fn verify_method<E: #method_trait #trait_arguments>() {
            let _ = E::#method;
        }

        verify_method::<#receiver>();
        ;
    })
}

fn parameter_alias(
    query: &Path,
    order: &TokenStream,
    parameter: &Parameter,
) -> Result<TokenStream> {
    let name = &parameter.name;
    let bound = parameter.bound.to_string();
    let additional: Vec<_> = parameter.additional.iter().map(Ident::to_string).collect();
    let additional: Vec<_> = additional.iter().map(String::as_str).collect();

    let witness = match (bound.as_str(), &parameter.target, &additional[..]) {
        ("IndexDomain", None, []) => quote!(#query::registry::IndexWitness),
        ("EntityDomain", None, []) => quote!(#query::registry::EntityWitness),
        ("EntityAttributes", None, []) => quote!(#query::registry::EntityAttributesWitness),
        ("GroupMembership", None, []) => quote!(#query::registry::GroupMembershipWitness),
        ("GroupMember", None, []) => quote!(#query::registry::GroupMemberWitness),
        ("GroupKey", None, []) => quote!(#query::registry::GroupKeyWitness),
        ("ElementShape", None, []) => quote!(#query::registry::ElementShapeWitness),
        ("OrderState", None, []) => quote!(#order),
        ("Arity", None, []) => quote!(#query::registry::ArityWitness),
        ("EnumerableArity", None, []) => quote!(#query::registry::EnumerableArityWitness),
        ("Lane", None, []) => quote! {
            #query::expressions::ExpressionHandle<
                #query::Bare<
                    #query::registry::ValueWitness<
                        #query::registry::ValueDomainCapability,
                        #query::registry::BareValueCapability,
                    >,
                >,
                #query::Multiple<#order>,
            >
        },
        ("ValueDomain", None, []) => quote! {
            #query::registry::ValueWitness<
                #query::registry::ValueDomainCapability,
                #query::registry::ValueDomainOnly,
            >
        },
        ("BareValueDomain", None, []) => quote! {
            #query::registry::ValueWitness<
                #query::registry::ValueDomainCapability,
                #query::registry::BareValueCapability,
            >
        },
        ("ValueGrouping", Some(_), []) => quote! {
            #query::registry::ValueWitness<
                #query::registry::GroupingCapability,
                #query::registry::ValueDomainOnly,
            >
        },
        (_, Some(target), ["BareValueDomain"]) => capability_witness(
            query,
            &parameter.bound,
            Some(target),
            &quote!(#query::registry::BareValueCapability),
        )?,
        (_, Some(target), []) => capability_witness(
            query,
            &parameter.bound,
            Some(target),
            &quote!(#query::registry::ValueDomainOnly),
        )?,
        (_, None, [_, "BareValueDomain"]) => {
            let first = capability_marker(query, &parameter.bound, None)?;
            let second = capability_marker(query, &parameter.additional[0], None)?;
            quote! {
                #query::registry::ValueWitness<
                    (#first, #second),
                    #query::registry::BareValueCapability,
                >
            }
        }
        (_, None, ["BareValueDomain"]) => capability_witness(
            query,
            &parameter.bound,
            None,
            &quote!(#query::registry::BareValueCapability),
        )?,
        (_, None, [_]) => {
            let first = capability_marker(query, &parameter.bound, None)?;
            let second = capability_marker(query, &parameter.additional[0], None)?;
            quote! {
                #query::registry::ValueWitness<
                    (#first, #second),
                    #query::registry::ValueDomainOnly,
                >
            }
        }
        (_, None, []) => capability_witness(
            query,
            &parameter.bound,
            None,
            &quote!(#query::registry::ValueDomainOnly),
        )?,
        _ => {
            return Err(Error::new(
                name.span(),
                "the parameter bounds have no witness form",
            ));
        }
    };

    Ok(quote! {
        type #name = #witness;
    })
}

fn capability_witness(
    query: &Path,
    capability: &Ident,
    target: Option<&Type>,
    shape: &TokenStream,
) -> Result<TokenStream> {
    let marker = capability_marker(query, capability, target)?;

    Ok(quote!(#query::registry::ValueWitness<#marker, #shape>))
}

pub fn capability_marker(
    query: &Path,
    capability: &Ident,
    target: Option<&Type>,
) -> Result<TokenStream> {
    let target = target.map(|target| quote!(#target).to_string().replace(' ', ""));

    let marker = match (capability.to_string().as_str(), target.as_deref()) {
        ("ValueAbsolute", None) => "AbsoluteCapability",
        ("ValueAdd", None) => "AddCapability",
        ("ValueCast", Some("Bool")) => "CastBoolCapability",
        ("ValueCast", Some("DateTime")) => "CastDateTimeCapability",
        ("ValueCast", Some("Duration")) => "CastDurationCapability",
        ("ValueCast", Some("Float")) => "CastFloatCapability",
        ("ValueCast", Some("Int")) => "CastIntCapability",
        ("ValueCast", Some("String")) => "CastStringCapability",
        ("ValueCeil", None) => "CeilCapability",
        ("ValueClip", None) => "ClipCapability",
        ("ValueCubeRoot", None) => "CubeRootCapability",
        ("ValueDivide", None) => "DivideCapability",
        ("ValueEquality", None) => "EqualityCapability",
        ("ValueEquivalence", None) => "EquivalenceCapability",
        ("ValueExponential", None) => "ExponentialCapability",
        ("ValueFloor", None) => "FloorCapability",
        ("ValueGrouping", None) => "GroupingCapability",
        ("ValueInt", None) => "IntCapability",
        ("ValueKindTest", None) => "KindTestCapability",
        ("ValueLogarithm", None) => "LogarithmCapability",
        ("ValueMedian", None) => "MedianCapability",
        ("ValueMode", None) => "ModeCapability",
        ("ValueModulo", None) => "ModuloCapability",
        ("ValueMultiply", None) => "MultiplyCapability",
        ("ValueNegate", None) => "NegateCapability",
        ("ValueOrdering", None) => "OrderingCapability",
        ("ValuePower", None) => "PowerCapability",
        ("ValueRound", None) => "RoundCapability",
        ("ValueScalar", None) => "ScalarCapability",
        ("ValueScalarKindTest", None) => "ScalarKindTestCapability",
        ("ValueSign", None) => "SignCapability",
        ("EnsureSortable", None) => "SortableCapability",
        ("ValueSquareRoot", None) => "SquareRootCapability",
        ("ValueString", None) => "StringCapability",
        ("ValueSubtract", None) => "SubtractCapability",
        ("ValueTransition", Some("AttributeName")) => "TransitionAttributeNameCapability",
        ("ValueTransition", Some("(IndexValue<AttributeName>)")) => {
            "TransitionAttributeNameIndexCapability"
        }
        ("ValueTransition", Some("(IndexValue<bool>)")) => "TransitionBoolIndexCapability",
        ("ValueTransition", Some("(IndexValue<FailureKind>)")) => {
            "TransitionFailureKindIndexCapability"
        }
        ("ValueTransition", Some("FailureKindValue")) => "TransitionFailureKindValueCapability",
        ("ValueTransition", Some("(IndexValue<Group>)")) => "TransitionGroupIndexCapability",
        ("ValueTransition", Some("Mask")) => "TransitionMaskCapability",
        ("ValueTransition", Some("(IndexValue<NodeIndex>)")) => "TransitionNodeIndexCapability",
        ("ValueTransition", Some("(IndexValue<Positional>)")) => {
            "TransitionPositionalIndexCapability"
        }
        ("ValueTransition", Some("Scalar")) => "TransitionScalarCapability",
        ("ValueTransition", Some("(IndexValue<Value>)")) => "TransitionValueIndexCapability",
        _ => {
            return Err(Error::new(
                capability.span(),
                "the capability has no registered marker",
            ));
        }
    };
    let marker = Ident::new(marker, capability.span());

    Ok(quote!(#query::registry::#marker))
}
