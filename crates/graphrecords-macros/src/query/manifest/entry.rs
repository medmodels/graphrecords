use super::{
    applier,
    model::{Kernel, KernelInput, Manifest, Scope, ValueArgument},
    witness::capability_marker,
};
use proc_macro2::TokenStream;
use quote::quote;
use syn::{Error, Ident, LitStr, Path, Result, Type};

pub fn manifest_function(manifest: &Manifest, query: &Path) -> Result<TokenStream> {
    let name = manifest
        .registry_name
        .clone()
        .unwrap_or_else(|| LitStr::new(&manifest.method.to_string(), manifest.method.span()));
    let entries = manifest
        .kernels
        .iter()
        .map(|kernel| entry(manifest, kernel, query))
        .collect::<Result<Vec<_>>>()?;

    Ok(quote! {
        pub fn operation_manifest() -> #query::registry::OperationManifest {
            #query::registry::OperationManifest::new(
                #name,
                vec![#(#entries),*],
            )
        }
    })
}

fn entry(manifest: &Manifest, kernel: &Kernel, query: &Path) -> Result<TokenStream> {
    let aliases = entry_aliases(kernel, query)?;
    let row = entry_row(manifest, kernel, query)?;

    Ok(quote! {
        {
            #aliases
            ;
            #row
        }
    })
}

fn entry_aliases(kernel: &Kernel, query: &Path) -> Result<TokenStream> {
    let mut aliases = TokenStream::new();
    let mut position = quote!(0);

    if let Some(group) = &kernel.group {
        let member_bound = Ident::new("GroupMember", group.member.span());
        aliases.extend(entry_alias(
            query,
            &position,
            &group.member,
            &member_bound,
            None,
            &[],
        )?);
        position.extend(quote!(+ 1));

        let key_bound = Ident::new("GroupKey", group.key.span());
        aliases.extend(entry_alias(
            query,
            &position,
            &group.key,
            &key_bound,
            None,
            &[],
        )?);
        position.extend(quote!(+ 1));
    }

    for parameter in &kernel.parameters {
        aliases.extend(entry_alias(
            query,
            &position,
            &parameter.name,
            &parameter.bound,
            parameter.target.as_ref(),
            &parameter.additional,
        )?);
        position.extend(quote!(+ 1));
    }

    Ok(aliases)
}

fn entry_row(manifest: &Manifest, kernel: &Kernel, query: &Path) -> Result<TokenStream> {
    let applier = applier::applier(manifest, kernel, query)?;

    match manifest.scope {
        Scope::Lane => {
            let KernelInput::Lane { shape, arity } = &kernel.input else {
                return Err(Error::new_spanned(
                    &kernel.output,
                    "a lane kernel input must pair a shape with an arity",
                ));
            };
            let output = &kernel.output;
            let field_types = kernel.fields.iter().map(|field| &field.field_type);
            let patterns = kernel
                .value_arguments()?
                .into_iter()
                .map(|argument| argument_pattern(query, argument))
                .collect::<Result<Vec<_>>>()?;

            Ok(quote! {
                #query::registry::OperationManifestEntry::lane::<#shape, #arity, #output>(
                    vec![
                        #(#query::registry::ArgumentPattern::field::<#field_types>(),)*
                        #(#patterns,)*
                    ],
                    #[cfg(feature = "dynamic")]
                    #applier,
                )
            })
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
            let arguments = if let Some(set) = kernel.set_argument() {
                let value = &set.value;
                quote! {
                    vec![#query::registry::ArgumentPattern::Set(
                        <#value as #query::registry::describe::DescribeValue>::value_pattern(),
                    )]
                }
            } else {
                let selector = kernel.selector.as_ref().map(
                    |selector| quote!(#query::registry::ArgumentPattern::selector::<#selector>(),),
                );
                let field_types = kernel.fields.iter().map(|field| &field.field_type);
                let patterns = kernel
                    .value_arguments()?
                    .into_iter()
                    .map(|argument| argument_pattern(query, argument))
                    .collect::<Result<Vec<_>>>()?;
                quote! {
                    vec![
                        #selector
                        #(#query::registry::ArgumentPattern::field::<#field_types>(),)*
                        #(#patterns,)*
                    ]
                }
            };

            Ok(quote! {
                #query::registry::OperationManifestEntry::element::<#shape, #out_shape>(
                    #arguments,
                    <#emission as #query::registry::describe::DescribeEmission>::emission_spec(),
                    #[cfg(feature = "dynamic")]
                    #applier,
                )
            })
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
            let arguments = if let Some(via) = kernel.via_argument() {
                let index = &via.index;
                let value = &via.value;
                let arity = &via.arity;
                quote! {
                    vec![
                        #query::registry::ArgumentPattern::Expression(
                            <#query::expressions::ExpressionHandle<
                                #query::Indexed<#index, #value>,
                                #arity,
                            > as #query::registry::describe::DescribeExpression>::expression_pattern(),
                        ),
                    ]
                }
            } else {
                let patterns = kernel
                    .value_arguments()?
                    .into_iter()
                    .map(|argument| argument_pattern(query, argument))
                    .collect::<Result<Vec<_>>>()?;
                quote! {
                    vec![
                        #(#patterns,)*
                    ]
                }
            };

            Ok(quote! {
                #query::registry::OperationManifestEntry::group::<#member, #key, #payload, #output>(
                    #arguments,
                    #[cfg(feature = "dynamic")]
                    #applier,
                )
            })
        }
    }
}

fn argument_pattern(query: &Path, argument: &ValueArgument) -> Result<TokenStream> {
    let alignment = &argument.alignment;
    let alignment = quote! {
        <#alignment as #query::registry::describe::DescribeAlignment>::alignment_descriptor()
    };

    match (&argument.value, &argument.retention, &argument.capability) {
        (Some(value), Some(retention), None) => Ok(quote! {
            #query::registry::ArgumentPattern::Value {
                value: <#value as #query::registry::describe::DescribeValue>::value_pattern(),
                alignment: #alignment,
                retention: <#retention as
                    #query::registry::describe::DescribeRetention>::retention_pattern(),
            }
        }),
        (Some(value), None, None) => Ok(quote! {
            #query::registry::ArgumentPattern::Value {
                value: <#value as #query::registry::describe::DescribeValue>::value_pattern(),
                alignment: #alignment,
                retention: #query::registry::RetentionPattern::Any,
            }
        }),
        (None, None, Some(capability)) => {
            let marker = capability_marker(query, capability, None)?;
            Ok(quote! {
                #query::registry::ArgumentPattern::Value {
                    value: <#marker as
                        #query::registry::describe::CapabilityMarkers>::argument_value_pattern(),
                    alignment: #alignment,
                    retention: #query::registry::RetentionPattern::Any,
                }
            })
        }
        (None, None, None) => Ok(quote! {
            #query::registry::ArgumentPattern::Value {
                value: <#query::registry::describe::RegisteredOnly as
                    #query::registry::describe::CapabilityMarkers>::argument_value_pattern(),
                alignment: #alignment,
                retention: #query::registry::RetentionPattern::Any,
            }
        }),
        _ => Err(Error::new(
            argument.name.span(),
            "an argument must not combine a value with a capability",
        )),
    }
}

fn entry_alias(
    query: &Path,
    position: &TokenStream,
    name: &Ident,
    bound: &Ident,
    target: Option<&Type>,
    additional: &[Ident],
) -> Result<TokenStream> {
    let additional_names: Vec<_> = additional.iter().map(Ident::to_string).collect();
    let additional_names: Vec<_> = additional_names.iter().map(String::as_str).collect();

    let variable = match (bound.to_string().as_str(), target, &additional_names[..]) {
        ("IndexDomain" | "GroupMember", None, []) => {
            quote!(#query::registry::describe::IndexPatternVariable<{ #position }>)
        }
        ("EntityIndexDomain", None, []) => {
            quote!(#query::registry::describe::EntityPatternVariable<{ #position }>)
        }
        ("EntityAttributes", None, []) => {
            quote!(#query::registry::describe::EntityAttributesPatternVariable<{ #position }>)
        }
        ("GroupMembership", None, []) => {
            quote!(#query::registry::describe::GroupMembershipPatternVariable<{ #position }>)
        }
        ("GroupKey", None, []) => {
            quote!(#query::registry::describe::GroupKeyPatternVariable<{ #position }>)
        }
        ("ElementShape", None, []) => {
            quote!(#query::registry::describe::ShapePatternVariable<{ #position }>)
        }
        ("OrderState", None, []) => {
            quote!(#query::registry::describe::OrderPatternVariable<{ #position }>)
        }
        ("Arity" | "EnumerableArity", None, []) => {
            quote!(#query::registry::describe::ArityPatternVariable<{ #position }>)
        }
        ("Lane", None, []) => {
            quote!(#query::registry::describe::LanePatternVariable<{ #position }>)
        }
        ("ValueDomain", None, []) => quote! {
            #query::registry::describe::ValuePatternVariable<
                { #position },
                #query::registry::describe::RegisteredOnly,
            >
        },
        ("BareValueDomain", None, []) => quote! {
            #query::registry::describe::ValuePatternVariable<
                { #position },
                #query::registry::BareValueCapability,
            >
        },
        ("ValueGrouping", Some(target), []) => quote! {
            #query::registry::describe::GroupingValuePatternVariable<{ #position }, #target>
        },
        (_, Some(target), ["BareValueDomain"]) => {
            let marker = capability_marker(query, bound, Some(target))?;
            quote! {
                #query::registry::describe::ValuePatternVariable<
                    { #position },
                    (#marker, #query::registry::BareValueCapability),
                >
            }
        }
        (_, Some(target), []) => {
            let marker = capability_marker(query, bound, Some(target))?;
            quote! {
                #query::registry::describe::ValuePatternVariable<{ #position }, #marker>
            }
        }
        (_, None, [_, "BareValueDomain"]) => {
            let first = capability_marker(query, bound, None)?;
            let second = capability_marker(query, &additional[0], None)?;
            quote! {
                #query::registry::describe::ValuePatternVariable<
                    { #position },
                    ((#first, #second), #query::registry::BareValueCapability),
                >
            }
        }
        (_, None, ["BareValueDomain"]) => {
            let marker = capability_marker(query, bound, None)?;
            quote! {
                #query::registry::describe::ValuePatternVariable<
                    { #position },
                    (#marker, #query::registry::BareValueCapability),
                >
            }
        }
        (_, None, [_]) => {
            let first = capability_marker(query, bound, None)?;
            let second = capability_marker(query, &additional[0], None)?;
            quote! {
                #query::registry::describe::ValuePatternVariable<
                    { #position },
                    (#first, #second),
                >
            }
        }
        (_, None, []) => {
            let marker = capability_marker(query, bound, None)?;
            quote! {
                #query::registry::describe::ValuePatternVariable<{ #position }, #marker>
            }
        }
        _ => {
            return Err(Error::new(
                name.span(),
                "the parameter bounds have no pattern form",
            ));
        }
    };

    Ok(quote! {
        type #name = #variable;
    })
}
