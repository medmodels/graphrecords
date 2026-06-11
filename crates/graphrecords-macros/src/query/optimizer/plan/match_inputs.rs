use super::PlanModel;
use proc_macro2::TokenStream;
use quote::quote;
use syn::{DeriveInput, Result};

pub fn expand(input: &DeriveInput) -> Result<TokenStream> {
    let PlanModel {
        ident,
        generics,
        crate_path,
        inputs,
        input_types,
        ..
    } = PlanModel::parse(input)?;

    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();

    let body = if inputs.is_empty() {
        quote!()
    } else {
        quote!( ( #( &self.#inputs, )* ) )
    };

    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics #crate_path::optimizer::MatchInputs for #ident #type_generics #where_clause {
            type Inputs<'__inputs> = ( #( &'__inputs #input_types, )* );

            fn inputs(&self) -> Self::Inputs<'_> {
                #body
            }
        }
    })
}
