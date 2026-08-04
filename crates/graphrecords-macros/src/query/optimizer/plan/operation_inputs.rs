use super::{PlanModel, with_bounds};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{DeriveInput, Result};

pub fn expand(input: &DeriveInput) -> Result<TokenStream> {
    let PlanModel {
        ident,
        generics,
        crate_path,
        inputs,
        input_types,
        arguments,
        argument_types,
        ..
    } = PlanModel::parse(input)?;

    let generics = with_bounds(
        &generics,
        &argument_types,
        &quote!(
            #crate_path::optimizer::PlanIdentity
                + #crate_path::optimizer::PlanInputs
                + 'static
        ),
    );

    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();

    let primary = format_ident!("__Primary");

    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics #crate_path::optimizer::OperationInputs for #ident #type_generics #where_clause {
            type Inputs<'__inputs, #primary: '__inputs> = (
                &'__inputs #primary,
                #( &'__inputs #input_types, )*
                #( &'__inputs #argument_types, )*
            );

            fn inputs<'__inputs, #primary: '__inputs>(
                &'__inputs self,
                primary: &'__inputs #primary,
            ) -> Self::Inputs<'__inputs, #primary> {
                ( primary, #( &self.#inputs, )* #( &self.#arguments, )* )
            }
        }
    })
}
