use super::{PlanModel, with_bounds};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{DeriveInput, Result};

pub fn expand(input: &DeriveInput) -> Result<TokenStream> {
    let PlanModel {
        ident,
        generics,
        crate_path,
        arguments,
        argument_types,
        payload,
        ..
    } = PlanModel::parse(input)?;

    let generics = with_bounds(
        &generics,
        &argument_types,
        &quote!(#crate_path::optimizer::PlanIdentity),
    );

    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();

    let payload_eq = payload.clone();
    let payload_hash = payload;
    let arguments_eq = arguments.clone();
    let arguments_hash = arguments;

    let hasher = format_ident!("__Hasher");

    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics #crate_path::optimizer::PlanIdentity for #ident #type_generics #where_clause {
            fn identity_eq(&self, other: &Self) -> bool {
                #(
                    if self.#payload_eq != other.#payload_eq {
                        return false;
                    }
                )*

                #(
                    if !#crate_path::optimizer::PlanIdentity::identity_eq(
                        &self.#arguments_eq,
                        &other.#arguments_eq,
                    ) {
                        return false;
                    }
                )*

                true
            }

            fn identity_hash<#hasher: ::core::hash::Hasher>(&self, state: &mut #hasher) {
                #( ::core::hash::Hash::hash(&self.#payload_hash, state); )*
                #(
                    #crate_path::optimizer::PlanIdentity::identity_hash(
                        &self.#arguments_hash,
                        state,
                    );
                )*
            }
        }
    })
}
