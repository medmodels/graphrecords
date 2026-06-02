use crate::{crate_path, has_flag};
use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Error, Index, Result};

pub fn expand(input: &DeriveInput) -> Result<TokenStream> {
    let name = &input.ident;

    let (impl_generics, type_generics, where_clause) = input.generics.split_for_impl();

    let crate_path = crate_path(&input.attrs, "operand", |_| Ok(false))?;

    let Data::Struct(data) = &input.data else {
        return Err(Error::new_spanned(
            input,
            "Operand can only be derived for structs",
        ));
    };

    let mut context = None;

    for (index, field) in data.fields.iter().enumerate() {
        if !has_flag(field, "operand", "context")? {
            continue;
        }

        if context.is_some() {
            return Err(Error::new_spanned(
                field,
                "only one field may be marked `#[operand(context)]`",
            ));
        }

        context = Some((index, field));
    }

    let Some((index, field)) = context else {
        return Err(Error::new_spanned(
            input,
            "`#[derive(Operand)]` requires one field marked `#[operand(context)]`",
        ));
    };

    let field_type = &field.ty;

    let (accessor, from_context) = if let Some(identifier) = &field.ident {
        (
            quote!(#identifier),
            quote! {
                fn from_context(#identifier: ::std::sync::Arc<Self::Context>) -> Self {
                    Self { #identifier }
                }
            },
        )
    } else {
        let index = Index::from(index);
        (
            quote!(#index),
            quote! {
                fn from_context(context: ::std::sync::Arc<Self::Context>) -> Self {
                    Self(context)
                }
            },
        )
    };

    Ok(quote! {
        impl #impl_generics #crate_path::Operand for #name #type_generics #where_clause {
            type Context = <#field_type as ::core::ops::Deref>::Target;

            fn context(&self) -> &Self::Context {
                self.#accessor.as_ref()
            }

            fn as_plan_node(&self) -> &dyn #crate_path::optimizer::PlanNode {
                self.#accessor.as_ref()
            }

            #from_context
        }
    })
}
